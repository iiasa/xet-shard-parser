//! Server-side shard verification.
//!
//! Every chunk hash used here comes from data the server trusts: chunk lists the server
//! built itself when a xorb was uploaded, the CAS info of shards already in the local
//! index, or the footer of the xorb object stored in S3. Nothing is taken from the
//! uploaded shard on trust.

use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Seek, SeekFrom};

use mdb_shard::merklehash::{file_hash, MerkleHash};
use mdb_shard::metadata_shard::chunk_verification::range_hash_from_chunks;
use mdb_shard::metadata_shard::file_structs::MDBFileInfo;
use mdb_shard::metadata_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::metadata_shard::xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry};
use mdb_shard::metadata_shard::ShardFileManager;
use mdb_shard::metadata_shard::{MDBShardFileFooter, MDBShardFileHeader, MDBShardInfo};
use mdb_shard::xorb_object::XorbObject;

use crate::s3_client::{create_s3_client, s3_download_object, s3_download_range, MAX_CONCURRENCY, MAX_S3_ATTEMPTS};

/// A trusted chunk list: (chunk hash, unpacked size) per chunk, in xorb order.
pub type ChunkList = Vec<(MerkleHash, u32)>;

/// Size of one packed chunk-list entry: 32-byte hash + u32 unpacked size (little endian).
pub const PACKED_ENTRY_SIZE: usize = 36;

/// Reasons returned to Python. `XORB_NOT_FOUND` is the one a stale client cache causes.
pub const XORB_NOT_FOUND: &str = "xorb_not_found";
pub const MALFORMED_SHARD: &str = "malformed_shard";
pub const CAS_INFO_MISMATCH: &str = "cas_info_mismatch";
pub const BAD_RANGE: &str = "bad_range";
pub const MISSING_VERIFICATION: &str = "missing_verification";
pub const BAD_VERIFICATION_HASH: &str = "bad_verification_hash";
pub const BAD_FILE_HASH: &str = "bad_file_hash";
pub const INTERNAL_ERROR: &str = "internal_error";

/// A failed check: a short reason code and the xorb or file hash it is about.
pub type Failure = (String, String);

fn fail(reason: &str, hash: impl ToString) -> Failure {
    (reason.to_string(), hash.to_string())
}

pub fn pack_chunk_list(chunks: &ChunkList) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunks.len() * PACKED_ENTRY_SIZE);
    for (h, size) in chunks {
        out.extend_from_slice(h.as_bytes());
        out.extend_from_slice(&size.to_le_bytes());
    }
    out
}

pub fn unpack_chunk_list(bytes: &[u8]) -> Option<ChunkList> {
    if bytes.len() % PACKED_ENTRY_SIZE != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / PACKED_ENTRY_SIZE);
    for entry in bytes.chunks_exact(PACKED_ENTRY_SIZE) {
        let mut h = [0u8; 32];
        h.copy_from_slice(&entry[0..32]);
        let size = u32::from_le_bytes(entry[32..36].try_into().ok()?);
        out.push((MerkleHash::from(h), size));
    }
    Some(out)
}

/// Reads the chunk list from a xorb object's footer. `bytes` may be the whole xorb or any
/// tail of it that contains the full footer (for example the last 1 MiB from S3).
pub fn chunk_list_from_xorb_footer(bytes: &[u8]) -> Option<ChunkList> {
    let xorb = XorbObject::deserialize(&mut Cursor::new(bytes)).ok()?;
    let info = xorb.info;
    if info.chunk_hashes.len() != info.unpacked_chunk_offsets.len() {
        return None;
    }
    let mut out = Vec::with_capacity(info.chunk_hashes.len());
    let mut prev_end: u32 = 0;
    for (h, end) in info.chunk_hashes.iter().zip(info.unpacked_chunk_offsets.iter()) {
        let size = end.checked_sub(prev_end)?;
        out.push((*h, size));
        prev_end = *end;
    }
    Some(out)
}

fn chunk_list_from_xorb_info(info: &MDBXorbInfo) -> ChunkList {
    info.chunks.iter().map(|c: &XorbChunkSequenceEntry| (c.chunk_hash, c.unpacked_segment_bytes)).collect()
}

/// Compares a client-provided CAS info block with the server's chunk list.
/// Offsets in CAS info are cumulative unpacked positions (see xet-core `RawXorbData::from_chunks`).
fn check_cas_info(info: &MDBXorbInfo, trusted: &ChunkList) -> bool {
    if info.metadata.num_entries as usize != trusted.len() || info.chunks.len() != trusted.len() {
        return false;
    }
    let mut pos: u64 = 0;
    for (entry, (h, size)) in info.chunks.iter().zip(trusted.iter()) {
        if entry.chunk_hash != *h || entry.unpacked_segment_bytes != *size || entry.chunk_byte_range_start as u64 != pos {
            return false;
        }
        pos += *size as u64;
    }
    info.metadata.num_bytes_in_xorb as u64 == pos
}

/// Checks every term of one file and recomputes its hash.
fn check_file(file: &MDBFileInfo, trusted: &HashMap<MerkleHash, ChunkList>) -> Result<(), Failure> {
    let file_hash_hex = file.metadata.file_hash.hex();

    if file.verification.len() != file.segments.len() {
        return Err(fail(MISSING_VERIFICATION, &file_hash_hex));
    }

    let mut all_chunks: Vec<(MerkleHash, u64)> = Vec::new();
    for (seg, ver) in file.segments.iter().zip(file.verification.iter()) {
        let Some(chunks) = trusted.get(&seg.xorb_hash) else {
            return Err(fail(XORB_NOT_FOUND, seg.xorb_hash.hex()));
        };
        let (start, end) = (seg.chunk_index_start as usize, seg.chunk_index_end as usize);
        if start >= end || end > chunks.len() {
            return Err(fail(BAD_RANGE, seg.xorb_hash.hex()));
        }
        let range = &chunks[start..end];
        let range_bytes: u64 = range.iter().map(|(_, s)| *s as u64).sum();
        if range_bytes != seg.unpacked_segment_bytes as u64 {
            return Err(fail(BAD_RANGE, seg.xorb_hash.hex()));
        }
        let hashes: Vec<MerkleHash> = range.iter().map(|(h, _)| *h).collect();
        if range_hash_from_chunks(&hashes) != ver.range_hash {
            return Err(fail(BAD_VERIFICATION_HASH, seg.xorb_hash.hex()));
        }
        all_chunks.extend(range.iter().map(|(h, s)| (*h, *s as u64)));
    }

    if file_hash(&all_chunks) != file.metadata.file_hash {
        return Err(fail(BAD_FILE_HASH, &file_hash_hex));
    }
    Ok(())
}

/// Reads one xorb's CAS info from a shard file, using the shard's own xorb lookup table.
fn find_xorb_in_shard<R: std::io::Read + Seek>(
    shard: &MDBShardInfo,
    reader: &mut R,
    xorb_hash: &MerkleHash,
) -> Option<MDBXorbInfo> {
    let mut idx = [0u32; 8];
    let n = shard.get_xorb_info_index_by_hash(reader, xorb_hash, &mut idx).ok()?;
    for &i in &idx[..n] {
        // Lookup values are row indices into the xorb info section; every row is one
        // 48-byte header or chunk entry.
        let pos = shard.metadata.xorb_info_offset + (i as u64) * (std::mem::size_of::<XorbChunkSequenceEntry>() as u64);
        if reader.seek(SeekFrom::Start(pos)).is_err() {
            continue;
        }
        if let Ok(Some(info)) = MDBXorbInfo::deserialize(reader) {
            if info.metadata.xorb_hash == *xorb_hash {
                return Some(info);
            }
        }
    }
    None
}

/// Looks up chunk lists for `wanted` xorbs in the shards of the local index.
/// Opens each shard file once and searches its own xorb lookup table for all wanted xorbs.
async fn lookup_in_local_index(
    sfm: &ShardFileManager,
    wanted: &HashSet<MerkleHash>,
) -> Result<HashMap<MerkleHash, ChunkList>, String> {
    let mut found: HashMap<MerkleHash, ChunkList> = HashMap::new();
    if wanted.is_empty() {
        return Ok(found);
    }
    let shards = sfm.registered_shard_list().await.map_err(|e| format!("shard list: {e:?}"))?;

    for shard_file in shards {
        if found.len() == wanted.len() {
            break;
        }
        let Ok(mut reader) = shard_file.get_reader() else { continue };

        for xorb_hash in wanted {
            if found.contains_key(xorb_hash) {
                continue;
            }
            if let Some(info) = find_xorb_in_shard(&shard_file.shard, &mut reader, xorb_hash) {
                found.insert(*xorb_hash, chunk_list_from_xorb_info(&info));
            }
        }
    }
    Ok(found)
}

/// Reads chunk lists from the footers of xorbs in S3. Xorbs not in S3 are left out.
async fn lookup_in_s3(wanted: Vec<MerkleHash>) -> Result<HashMap<MerkleHash, ChunkList>, String> {
    let mut found = HashMap::new();
    if wanted.is_empty() {
        return Ok(found);
    }
    let (client, bucket) = create_s3_client().map_err(|e| format!("S3 client: {e}"))?;

    use futures::stream::{iter, StreamExt};
    let results = iter(wanted.into_iter().map(|h| {
        let client = client.clone();
        let bucket = bucket.clone();
        async move {
            let key = format!("xorbs/default/{}", h.hex());
            let res = s3_download_range(&client, &bucket, &key, "bytes=-1048576", MAX_S3_ATTEMPTS, false).await;
            (h, res)
        }
    }))
    .buffer_unordered(MAX_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    for (h, res) in results {
        if let Some(bytes) = res? {
            if let Some(list) = chunk_list_from_xorb_footer(&bytes) {
                found.insert(h, list);
            }
        }
    }
    Ok(found)
}

/// Verifies an uploaded shard. Returns `Ok(())` if every check passes, otherwise the first failure.
///
/// `server_chunks` holds the chunk lists the server saved when the xorbs of this upload
/// arrived (from Redis). Missing entries are read from S3 footers.
pub async fn verify_shard(
    sfm: &ShardFileManager,
    shard_bytes: &[u8],
    server_chunks: HashMap<MerkleHash, ChunkList>,
) -> Result<(), Failure> {
    let shard = MDBMinimalShard::from_reader(&mut Cursor::new(shard_bytes), true, true)
        .map_err(|_| fail(MALFORMED_SHARD, ""))?;

    let mut cas_infos = Vec::with_capacity(shard.num_xorb());
    for i in 0..shard.num_xorb() {
        let view = shard.xorb(i).ok_or_else(|| fail(MALFORMED_SHARD, ""))?;
        cas_infos.push(MDBXorbInfo::from(view));
    }
    let mut files = Vec::with_capacity(shard.num_files());
    for i in 0..shard.num_files() {
        let view = shard.file(i).ok_or_else(|| fail(MALFORMED_SHARD, ""))?;
        files.push(MDBFileInfo::from(view));
    }

    let mut trusted: HashMap<MerkleHash, ChunkList> = HashMap::new();

    // 1. Xorbs new in this shard: compare CAS info with the server-built chunk list.
    let mut cas_missing = Vec::new();
    for info in &cas_infos {
        match server_chunks.get(&info.metadata.xorb_hash) {
            Some(list) => {
                trusted.insert(info.metadata.xorb_hash, list.clone());
            },
            None => cas_missing.push(info.metadata.xorb_hash),
        }
    }
    let from_s3 = lookup_in_s3(cas_missing).await.map_err(|e| fail(INTERNAL_ERROR, e))?;
    trusted.extend(from_s3);

    for info in &cas_infos {
        let h = info.metadata.xorb_hash;
        let Some(list) = trusted.get(&h) else {
            return Err(fail(XORB_NOT_FOUND, h.hex()));
        };
        if !check_cas_info(info, list) {
            return Err(fail(CAS_INFO_MISMATCH, h.hex()));
        }
    }

    // 2. Older xorbs referenced by file terms: local index first, then S3 footers.
    let old: HashSet<MerkleHash> = files
        .iter()
        .flat_map(|f| f.segments.iter().map(|s| s.xorb_hash))
        .filter(|h| !trusted.contains_key(h))
        .collect();
    let from_index = lookup_in_local_index(sfm, &old).await.map_err(|e| fail(INTERNAL_ERROR, e))?;
    let still_missing: Vec<MerkleHash> = old.iter().filter(|h| !from_index.contains_key(h)).copied().collect();
    trusted.extend(from_index);
    let from_s3 = lookup_in_s3(still_missing).await.map_err(|e| fail(INTERNAL_ERROR, e))?;
    trusted.extend(from_s3);

    // 3. Every term and every file hash.
    for file in &files {
        check_file(file, &trusted)?;
    }
    Ok(())
}

/// Result of `deep_audit_shards`.
#[derive(Default)]
pub struct DeepAuditReport {
    pub shards_checked: usize,
    /// File entries of live files checked (a file can have entries in several shards).
    pub entries_checked: usize,
    /// (file hash hex, reason, shard hash hex): entries that don't describe their file exactly.
    pub bad_entries: Vec<(String, String, String)>,
    pub blocks_checked: usize,
    /// (xorb hash hex, shard hash hex): CAS info blocks whose chunk hashes or sizes differ from
    /// the xorb's footer. Uploads that reuse chunks rely on these, so this is a real problem.
    pub blocks_wrong_chunks: Vec<(String, String)>,
    /// Blocks right in chunks and sizes but with positions written the old GC way. Harmless.
    pub blocks_old_positions: usize,
    /// Blocks for xorbs that are not in S3.
    pub blocks_xorb_missing: usize,
}

/// Read-only check of every registered shard against the xorb footers in S3:
/// - every file entry of a live file must describe its file exactly (`check_file_layout`:
///   chunk ranges, sizes, verification hashes if present, file hash). All entries are checked,
///   not just the first one found for a file;
/// - every CAS info block must list the same chunks and sizes as its xorb's footer.
pub async fn deep_audit_shards(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    shards: &[(String, std::path::PathBuf)],
    live: &HashSet<MerkleHash>,
) -> Result<DeepAuditReport, String> {
    use futures::stream::{iter, StreamExt};
    let mut report = DeepAuditReport::default();
    let mut chunk_lists: HashMap<MerkleHash, Option<ChunkList>> = HashMap::new();

    for (n, (shard_hash, path)) in shards.iter().enumerate() {
        let bytes = std::fs::read(path).map_err(|e| format!("Cannot read shard {shard_hash}: {e}"))?;
        let shard = MDBMinimalShard::from_reader(&mut Cursor::new(&bytes), true, true)
            .map_err(|e| format!("Cannot parse shard {shard_hash}: {e:?}"))?;
        let mut files = Vec::new();
        for i in 0..shard.num_files() {
            let view = shard.file(i).ok_or_else(|| format!("Cannot read file entry {i} of {shard_hash}"))?;
            if live.contains(&view.file_hash()) {
                files.push(MDBFileInfo::from(view));
            }
        }
        let mut cas_infos = Vec::with_capacity(shard.num_xorb());
        for i in 0..shard.num_xorb() {
            let view = shard.xorb(i).ok_or_else(|| format!("Cannot read CAS info block {i} of {shard_hash}"))?;
            cas_infos.push(MDBXorbInfo::from(view));
        }

        if chunk_lists.len() > GC_CHECK_FOOTER_CACHE {
            chunk_lists.clear();
        }
        let wanted: HashSet<MerkleHash> = files.iter()
            .flat_map(|f| f.segments.iter().map(|s| s.xorb_hash))
            .chain(cas_infos.iter().map(|c| c.metadata.xorb_hash))
            .filter(|h| !chunk_lists.contains_key(h))
            .collect();
        // Each footer is parsed as soon as it arrives and the 1 MiB download dropped, so at most
        // MAX_CONCURRENCY downloads are in memory at once, however many xorbs the shard uses.
        let fetched = iter(wanted.into_iter().map(|h| async move {
            let key = format!("xorbs/default/{}", h.hex());
            let res = s3_download_range(client, bucket, &key, "bytes=-1048576", MAX_S3_ATTEMPTS, false).await
                .map(|footer| footer.and_then(|f| chunk_list_from_xorb_footer(&f)));
            (h, res)
        }))
        .buffer_unordered(MAX_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        for (h, res) in fetched {
            chunk_lists.insert(h, res?);
        }
        let known: HashMap<MerkleHash, ChunkList> = chunk_lists.iter()
            .filter_map(|(h, l)| l.as_ref().map(|l| (*h, l.clone())))
            .collect();

        for info in &cas_infos {
            report.blocks_checked += 1;
            let h = info.metadata.xorb_hash;
            match known.get(&h) {
                None => report.blocks_xorb_missing += 1,
                Some(list) => {
                    let same_chunks = info.chunks.len() == list.len()
                        && info.chunks.iter().zip(list.iter()).all(|(c, (lh, ls))| c.chunk_hash == *lh && c.unpacked_segment_bytes == *ls);
                    if !same_chunks {
                        report.blocks_wrong_chunks.push((h.hex(), shard_hash.clone()));
                    } else if !check_cas_info(info, list) {
                        report.blocks_old_positions += 1;
                    }
                }
            }
        }
        for file in &files {
            report.entries_checked += 1;
            if let Err((reason, detail)) = check_file_layout(file, &known) {
                report.bad_entries.push((file.metadata.file_hash.hex(), format!("{reason} ({detail})"), shard_hash.clone()));
            }
        }
        report.shards_checked += 1;
        if (n + 1) % 100 == 0 {
            eprintln!("[Deep audit] {}/{} shards, {} entries checked, {} bad", n + 1, shards.len(), report.entries_checked, report.bad_entries.len());
        }
    }
    Ok(report)
}

/// Returns a copy of `shard_bytes` with the footer's `shard_key_expiry` set to `expiry_unix`.
/// Shards without a footer are returned unchanged.
pub fn with_dedup_expiry(shard_bytes: &[u8], expiry_unix: u64) -> Result<Vec<u8>, String> {
    let header = MDBShardFileHeader::deserialize(&mut Cursor::new(shard_bytes)).map_err(|e| format!("header: {e:?}"))?;
    let footer_size = header.footer_size as usize;
    if footer_size == 0 || footer_size > shard_bytes.len() {
        return Ok(shard_bytes.to_vec());
    }
    let footer_start = shard_bytes.len() - footer_size;
    let mut footer =
        MDBShardFileFooter::deserialize(&mut Cursor::new(&shard_bytes[footer_start..])).map_err(|e| format!("footer: {e:?}"))?;
    if footer.footer_offset as usize != footer_start {
        return Err(format!("footer offset {} does not match position {}", footer.footer_offset, footer_start));
    }
    footer.shard_key_expiry = expiry_unix;

    let mut out = Vec::with_capacity(shard_bytes.len());
    out.extend_from_slice(&shard_bytes[..footer_start]);
    footer.serialize(&mut out).map_err(|e| format!("serialize footer: {e:?}"))?;
    if out.len() != shard_bytes.len() {
        return Err("footer size changed".to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mdb_shard::metadata_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, FileVerificationEntry};
    use mdb_shard::metadata_shard::shard_in_memory::MDBInMemoryShard;
    use mdb_shard::xorb_object::{reconstruct_xorb_with_footer, Chunk, RawXorbData, SerializedXorbObject};

    /// Builds a xorb the way the xet-core client does, then the footer the way our
    /// upload_xorb does, and returns (client CAS info, server chunk list).
    fn client_xorb() -> (RawXorbData, ChunkList) {
        // Compressible data, so packed and unpacked offsets differ.
        let chunks: Vec<Chunk> = (0..5u8).map(|i| Chunk::new(Bytes::from(vec![i; 3000 + i as usize * 100]))).collect();
        let raw = RawXorbData::from_chunks(&chunks, vec![0]);
        let serialized = SerializedXorbObject::from_xorb(raw.clone(), true, "lz4", 16).unwrap();
        let mut with_footer = Vec::new();
        reconstruct_xorb_with_footer(&mut with_footer, &serialized.serialized_data).unwrap();
        let list = chunk_list_from_xorb_footer(&with_footer).unwrap();
        (raw, list)
    }

    fn file_over(raw: &RawXorbData, start: usize, end: usize) -> MDBFileInfo {
        let info = &raw.xorb_info;
        let range = &info.chunks[start..end];
        let hashes: Vec<MerkleHash> = range.iter().map(|c| c.chunk_hash).collect();
        let sizes: Vec<(MerkleHash, u64)> = range.iter().map(|c| (c.chunk_hash, c.unpacked_segment_bytes as u64)).collect();
        MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash(&sizes), 1, true, false),
            segments: vec![FileDataSequenceEntry::from_xorb_entries(&info.metadata, range, start, end)],
            verification: vec![FileVerificationEntry::new(range_hash_from_chunks(&hashes))],
            metadata_ext: None,
        }
    }

    #[test]
    fn cas_info_from_client_matches_server_footer() {
        let (raw, list) = client_xorb();
        assert!(check_cas_info(&raw.xorb_info, &list));

        let mut bad = raw.xorb_info.clone();
        bad.chunks[2].unpacked_segment_bytes += 1;
        assert!(!check_cas_info(&bad, &list));
    }

    #[test]
    fn file_terms_are_checked() {
        let (raw, list) = client_xorb();
        let mut trusted = HashMap::new();
        trusted.insert(raw.hash(), list);

        let good = file_over(&raw, 1, 4);
        assert!(check_file(&good, &trusted).is_ok());

        let mut wrong_hash = good.clone();
        wrong_hash.metadata.file_hash = MerkleHash::default();
        assert_eq!(check_file(&wrong_hash, &trusted).unwrap_err().0, BAD_FILE_HASH);

        let mut wrong_range = good.clone();
        wrong_range.segments[0].chunk_index_end = 9;
        assert_eq!(check_file(&wrong_range, &trusted).unwrap_err().0, BAD_RANGE);

        let mut wrong_ver = good.clone();
        wrong_ver.verification[0] = FileVerificationEntry::new(MerkleHash::default());
        assert_eq!(check_file(&wrong_ver, &trusted).unwrap_err().0, BAD_VERIFICATION_HASH);

        let mut no_ver = good.clone();
        no_ver.verification.clear();
        assert_eq!(check_file(&no_ver, &trusted).unwrap_err().0, MISSING_VERIFICATION);

        assert_eq!(check_file(&good, &HashMap::new()).unwrap_err().0, XORB_NOT_FOUND);
    }

    #[test]
    fn packed_chunk_list_round_trips() {
        let (_, list) = client_xorb();
        assert_eq!(unpack_chunk_list(&pack_chunk_list(&list)).unwrap(), list);
    }

    #[test]
    fn dedup_expiry_rewrites_only_the_footer() {
        let (raw, _) = client_xorb();
        let mut shard = MDBInMemoryShard::default();
        shard.add_xorb_block(raw.xorb_info.clone()).unwrap();
        shard.add_file_reconstruction_info(file_over(&raw, 0, 5)).unwrap();
        let bytes = shard.to_bytes().unwrap();

        let out = with_dedup_expiry(&bytes, 1_700_000_000).unwrap();
        assert_eq!(out.len(), bytes.len());
        let header = MDBShardFileHeader::deserialize(&mut Cursor::new(&out)).unwrap();
        let start = out.len() - header.footer_size as usize;
        assert_eq!(out[..start], bytes[..start]);
        let footer = MDBShardFileFooter::deserialize(&mut Cursor::new(&out[start..])).unwrap();
        assert_eq!(footer.shard_key_expiry, 1_700_000_000);
    }
    #[test]
    fn finds_xorb_in_a_shard_with_several_xorbs() {
        let mut shard = MDBInMemoryShard::default();
        let mut raws = Vec::new();
        for seed in 0..4u8 {
            let chunks: Vec<Chunk> =
                (0..3 + seed).map(|i| Chunk::new(Bytes::from(vec![seed.wrapping_mul(31).wrapping_add(i); 500]))).collect();
            let raw = RawXorbData::from_chunks(&chunks, vec![0]);
            shard.add_xorb_block(raw.xorb_info.clone()).unwrap();
            raws.push(raw);
        }
        let bytes = shard.to_bytes().unwrap();
        let mut reader = Cursor::new(&bytes);
        let info = MDBShardInfo::load_from_reader(&mut reader).unwrap();
        for raw in &raws {
            let found = find_xorb_in_shard(&info, &mut reader, &raw.hash()).unwrap();
            assert_eq!(found.chunks, raw.xorb_info.chunks);
        }
        assert!(find_xorb_in_shard(&info, &mut reader, &MerkleHash::default()).is_none());
    }
}


#[cfg(test)]
mod real_shard_tests {
    use super::*;

    /// Runs the CAS info, term and file-hash checks on real shards from a directory.
    /// Chunk lists for every xorb come from the CAS info of all shards in the directory.
    /// Run with: XET_REAL_SHARDS=/path/to/shards cargo test real_shards -- --ignored --nocapture
    #[test]
    #[ignore]
    fn real_shards() {
        let dir = std::env::var("XET_REAL_SHARDS").expect("set XET_REAL_SHARDS");
        let mut shards = Vec::new();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().map(|e| e == "mdb").unwrap_or(false) {
                shards.push((path.clone(), std::fs::read(&path).unwrap()));
            }
        }
        let mut trusted: HashMap<MerkleHash, ChunkList> = HashMap::new();
        let mut parsed = Vec::new();
        for (path, bytes) in &shards {
            let shard = MDBMinimalShard::from_reader(&mut Cursor::new(bytes), true, true).unwrap();
            let infos: Vec<MDBXorbInfo> = (0..shard.num_xorb()).map(|i| MDBXorbInfo::from(shard.xorb(i).unwrap())).collect();
            let files: Vec<MDBFileInfo> = (0..shard.num_files()).map(|i| MDBFileInfo::from(shard.file(i).unwrap())).collect();
            for info in &infos {
                trusted.insert(info.metadata.xorb_hash, chunk_list_from_xorb_info(info));
            }
            parsed.push((path.clone(), infos, files));
        }

        let (mut cas_ok, mut cas_bad, mut files_ok) = (0, 0, 0);
        let mut failures: HashMap<String, usize> = HashMap::new();
        for (path, infos, files) in &parsed {
            for info in infos {
                if check_cas_info(info, &trusted[&info.metadata.xorb_hash]) {
                    cas_ok += 1;
                } else {
                    cas_bad += 1;
                    if cas_bad <= 3 {
                        eprintln!("CAS offsets not unpacked-cumulative in {:?} xorb {}", path.file_name().unwrap(), info.metadata.xorb_hash.hex());
                    }
                }
            }
            for file in files {
                match check_file(file, &trusted) {
                    Ok(()) => files_ok += 1,
                    Err((reason, _)) => *failures.entry(reason).or_default() += 1,
                }
            }
        }
        eprintln!("shards={} cas_ok={cas_ok} cas_bad={cas_bad} files_ok={files_ok} failures={failures:?}", shards.len());
    }
}


/// Checks that a file entry describes its file correctly, given the real chunk lists of the
/// xorbs it points at: every segment's chunk range lies inside its xorb, has the recorded size,
/// matches the segment's verification hash (when the entry has verification hashes; entries
/// uploaded before verification existed have none), and the chunks of all segments together
/// give the file's hash.
fn check_file_layout(file: &MDBFileInfo, trusted: &HashMap<MerkleHash, ChunkList>) -> Result<(), Failure> {
    let has_verification = file.verification.len() == file.segments.len();
    let mut all_chunks: Vec<(MerkleHash, u64)> = Vec::new();
    for (i, seg) in file.segments.iter().enumerate() {
        let Some(chunks) = trusted.get(&seg.xorb_hash) else {
            return Err(fail(XORB_NOT_FOUND, seg.xorb_hash.hex()));
        };
        let (start, end) = (seg.chunk_index_start as usize, seg.chunk_index_end as usize);
        if start >= end || end > chunks.len() {
            return Err(fail(BAD_RANGE, seg.xorb_hash.hex()));
        }
        let range = &chunks[start..end];
        if range.iter().map(|(_, s)| *s as u64).sum::<u64>() != seg.unpacked_segment_bytes as u64 {
            return Err(fail(BAD_RANGE, seg.xorb_hash.hex()));
        }
        if has_verification {
            let hashes: Vec<MerkleHash> = range.iter().map(|(h, _)| *h).collect();
            if range_hash_from_chunks(&hashes) != file.verification[i].range_hash {
                return Err(fail(BAD_VERIFICATION_HASH, seg.xorb_hash.hex()));
            }
        }
        all_chunks.extend(range.iter().map(|(h, s)| (*h, *s as u64)));
    }
    if file_hash(&all_chunks) != file.metadata.file_hash {
        return Err(fail(BAD_FILE_HASH, file.metadata.file_hash.hex()));
    }
    Ok(())
}

/// Reason for a file entry in a shard that survives GC but points at a xorb commit deletes.
pub const XORB_TO_BE_DELETED: &str = "points_at_xorb_to_be_deleted";

/// Reason for a new xorb whose name is not the xorb hash recorded in its own footer.
pub const XORB_NAME_MISMATCH: &str = "xorb_name_mismatch";

/// Footer chunk lists kept in memory at once by `check_gc_shard_entries`; the cache is
/// cleared past this, so memory stays bounded however many xorbs the new shards use.
const GC_CHECK_FOOTER_CACHE: usize = 2048;

/// Checks the shards a GC run wrote (`gc_consolidated/shards/`) against the footers of the
/// xorbs they point at (new xorbs from `gc_consolidated/xorbs/`, others from `xorbs/default/`):
/// - every file entry must describe its file exactly (ranges, sizes, verification and file hashes);
/// - every CAS info block must match its xorb's footer (chunks, sizes, unpacked positions);
/// - every new xorb must be named by the xorb hash in its own footer;
/// - no file entry may point at a xorb in `old_xorbs` (deleted at commit).
///
/// Adds the hash of every xorb with a CAS info block in the new shards to `blocks_seen`.
///
/// Returns (file entries checked, failures as (file or xorb hash hex, reason)).
pub async fn check_gc_shard_entries(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    new_shards: &[String],
    new_xorbs: &HashSet<String>,
    old_xorbs: &HashSet<String>,
    blocks_seen: &mut HashSet<String>,
) -> Result<(usize, Vec<(String, String)>), String> {
    use futures::stream::{iter, StreamExt};
    let mut chunk_lists: HashMap<MerkleHash, ChunkList> = HashMap::new();
    let mut names_checked: HashSet<MerkleHash> = HashSet::new();
    let mut checked = 0;
    let mut failures = Vec::new();

    for shard_hash in new_shards {
        let key = format!("gc_consolidated/shards/{shard_hash}.mdb");
        let bytes = s3_download_object(client, bucket, &key, MAX_S3_ATTEMPTS, true).await?
            .ok_or_else(|| format!("New shard {key} not found"))?;
        let shard = MDBMinimalShard::from_reader(&mut Cursor::new(&bytes), true, true)
            .map_err(|e| format!("Cannot parse new shard {key}: {e:?}"))?;
        let mut files = Vec::with_capacity(shard.num_files());
        for i in 0..shard.num_files() {
            let view = shard.file(i).ok_or_else(|| format!("Cannot read file entry {i} of {key}"))?;
            files.push(MDBFileInfo::from(view));
        }
        let mut cas_infos = Vec::with_capacity(shard.num_xorb());
        for i in 0..shard.num_xorb() {
            let view = shard.xorb(i).ok_or_else(|| format!("Cannot read CAS info block {i} of {key}"))?;
            cas_infos.push(MDBXorbInfo::from(view));
        }

        if chunk_lists.len() > GC_CHECK_FOOTER_CACHE {
            chunk_lists.clear();
        }
        let wanted: HashSet<MerkleHash> = files.iter()
            .flat_map(|f| f.segments.iter().map(|s| s.xorb_hash))
            .chain(cas_infos.iter().map(|c| c.metadata.xorb_hash))
            .filter(|h| !chunk_lists.contains_key(h))
            .collect();
        let fetched = iter(wanted.into_iter().map(|h| {
            let hex = h.hex();
            let is_new = new_xorbs.contains(&hex);
            let key = if is_new { format!("gc_consolidated/xorbs/{hex}") } else { format!("xorbs/default/{hex}") };
            // Parsed as soon as it arrives, keeping only (hash in the footer, chunk list): at most
            // MAX_CONCURRENCY 1 MiB downloads are in memory at once.
            async move {
                let res = s3_download_range(client, bucket, &key, "bytes=-1048576", MAX_S3_ATTEMPTS, false).await
                    .map(|footer| footer.and_then(|f| {
                        let xorb = XorbObject::deserialize(&mut Cursor::new(&f)).ok()?;
                        Some((xorb.info.xorb_hash, chunk_list_from_xorb_footer(&f)))
                    }));
                (h, is_new, res)
            }
        }))
        .buffer_unordered(MAX_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        for (h, is_new, res) in fetched {
            let Some((footer_hash, list)) = res? else { continue };
            if is_new && names_checked.insert(h) && footer_hash != h {
                failures.push((h.hex(), format!("{XORB_NAME_MISMATCH} (footer says {})", footer_hash.hex())));
            }
            if let Some(list) = list {
                chunk_lists.insert(h, list);
            }
        }

        for info in &cas_infos {
            let h = info.metadata.xorb_hash;
            blocks_seen.insert(h.hex());
            match chunk_lists.get(&h) {
                None => failures.push((h.hex(), format!("{XORB_NOT_FOUND} (CAS info block) in shard {shard_hash}"))),
                Some(list) if !check_cas_info(info, list) => {
                    failures.push((h.hex(), format!("{CAS_INFO_MISMATCH} in shard {shard_hash}")))
                }
                Some(_) => {}
            }
        }
        for file in &files {
            checked += 1;
            if let Some(seg) = file.segments.iter().find(|s| old_xorbs.contains(&s.xorb_hash.hex())) {
                failures.push((file.metadata.file_hash.hex(), format!(
                    "{XORB_TO_BE_DELETED} ({}) in shard {shard_hash}", seg.xorb_hash.hex())));
                continue;
            }
            if let Err((reason, detail)) = check_file_layout(file, &chunk_lists) {
                failures.push((file.metadata.file_hash.hex(), format!("{reason} ({detail}) in shard {shard_hash}")));
            }
        }
    }
    Ok((checked, failures))
}
