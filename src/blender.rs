use pyo3::prelude::*;
use redb::ReadableTableMetadata;
use std::collections::HashSet;
use std::io::Cursor;
use tokio::runtime::Runtime;
use std::sync::Arc;

use crate::s3_client::{
    create_s3_client,
    s3_download_object,
    s3_download_range,
    s3_check_exists,
    s3_put_object,
    s3_copy_object,
    s3_copy_object_verified,
    s3_delete_object,
    s3_list_objects_v2,
    s3_upload_file_reqwest_http1,
    MAX_S3_ATTEMPTS,
    MAX_CONCURRENCY,
};

use mdb_shard::merklehash::{MerkleHash, compute_data_hash};
use mdb_shard::metadata_shard::shard_in_memory::MDBInMemoryShard;
use mdb_shard::metadata_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::metadata_shard::file_structs::MDBFileInfo;
use mdb_shard::metadata_shard::xorb_structs::{MDBXorbInfo, XorbChunkSequenceHeader, XorbChunkSequenceEntry};
use mdb_shard::xorb_object::{reconstruct_xorb_with_footer, XorbObject};
use redb::{Database, ReadableTable, ReadableDatabase, TableDefinition};

pub const GC_LIVE_FILES_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_files");
pub const GC_LIVE_CHUNKS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_chunks");
pub const GC_LIVE_XORBS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_xorbs");
pub const GC_SPARSE_XORBS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_sparse_xorbs");
pub const GC_XORB_CHUNKS_TABLE: redb::TableDefinition<&[u8; 32], &[u8]> = redb::TableDefinition::new("gc_xorb_chunks");

use mdb_shard::metadata_shard::shard_file_reconstructor::FileReconstructor;

// New Transaction Lock Tables
pub const TXN_META_TABLE: TableDefinition<&str, &str> = TableDefinition::new("txn_meta");
pub const TXN_OLD_XORBS_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_old_xorbs");
pub const TXN_OLD_SHARDS_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_old_shards");
pub const TXN_NEW_XORBS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("txn_new_xorbs");
pub const TXN_NEW_SHARDS_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_new_shards");
/// Where each repacked chunk went: (old xorb hash, old chunk index) -> (new xorb hash, new chunk index).
/// Keyed by position, not chunk hash, so the same chunk repacked from two xorbs keeps both positions.
pub const TXN_CHUNK_MAP_TABLE: TableDefinition<&[u8; 36], &[u8; 36]> = TableDefinition::new("txn_chunk_position_map");
/// File entries in the new shards that failed the layout check: file hash (hex) -> reason.
pub const TXN_BAD_FILE_ENTRIES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("txn_bad_file_entries");
/// xet-core's per-xorb chunk limit (MAX_XORB_CHUNKS); repacked xorbs stay within it.
const MAX_NEW_XORB_CHUNKS: usize = 8 * 1024;
pub const TXN_MISSING_FILES_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_missing_files");
pub const TXN_MISSING_POST_FILES_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_missing_post_files");
pub const TXN_UNIQUE_XORBS_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_unique_xorbs");
pub const TXN_MISSING_XORBS_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_missing_xorbs");
fn parse_xorb_footer_data(bytes: &[u8]) -> Option<(Vec<MerkleHash>, Vec<u32>, Vec<u32>)> {
    let mut reader = Cursor::new(bytes);
    let xorb_obj = XorbObject::deserialize(&mut reader).ok()?;
    let info = xorb_obj.info;
    Some((info.chunk_hashes, info.chunk_boundary_offsets, info.unpacked_chunk_offsets))
}

pub const TXN_KEPT_XORBS_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_kept_xorbs");

/// Seconds a committed transaction's bin/ contents are kept, so a commit can still be reverted.
const BIN_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

fn io_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string())
}

fn value_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
}

/// Lists every object under `prefix`, page by page, calling `f` for each one.
async fn for_each_listed_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    mut f: impl FnMut(&aws_sdk_s3::types::Object),
) -> Result<(), String> {
    let mut token: Option<String> = None;
    loop {
        let resp = s3_list_objects_v2(client, bucket, prefix, token.as_deref(), MAX_S3_ATTEMPTS).await?;
        for obj in resp.contents() {
            f(obj);
        }
        token = resp.next_continuation_token().map(String::from);
        if token.is_none() {
            return Ok(());
        }
    }
}

/// Lists every object under `prefix` and returns the key suffixes (the xorb hashes).
/// One LIST request covers 1000 keys, so this replaces a HEAD per xorb for the
/// common case where the xorb exists. Callers still HEAD anything not listed.
fn list_key_suffixes(rt: &Runtime, client: &aws_sdk_s3::Client, bucket: &str, prefix: &str) -> PyResult<HashSet<String>> {
    let mut keys = HashSet::new();
    rt.block_on(for_each_listed_object(client, bucket, prefix, |obj| {
        if let Some(suffix) = obj.key().and_then(|key| key.strip_prefix(prefix)) {
            keys.insert(suffix.to_string());
        }
    })).map_err(io_err)?;
    Ok(keys)
}

/// Lists `xorbs/default/` page by page and returns the xorbs whose LastModified is at or after `cutoff_unix`.
pub fn _list_young_xorbs(cutoff_unix: u64) -> PyResult<HashSet<[u8; 32]>> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = create_s3_client()?;
    let mut young = HashSet::new();
    rt.block_on(for_each_listed_object(&client, &bucket, "xorbs/default/", |obj| {
        let (Some(key), Some(modified)) = (obj.key(), obj.last_modified()) else { return };
        if modified.secs() < 0 || (modified.secs() as u64) < cutoff_unix {
            return;
        }
        if let Ok(h) = MerkleHash::from_hex(key.rsplit('/').next().unwrap_or("")) {
            young.insert(h.into());
        }
    })).map_err(io_err)?;
    Ok(young)
}

/// Removes xorbs last written at or after `cutoff_unix` from the GC's sparse set, so start
/// never removes or repacks them. Returns how many were removed.
pub fn _drop_young_from_sparse(gc_db: &Database, cutoff_unix: u64) -> PyResult<usize> {
    let young = _list_young_xorbs(cutoff_unix)?;
    let write_txn = gc_db.begin_write().map_err(io_err)?;
    let mut removed = 0;
    {
        let mut sparse = write_txn.open_table(GC_SPARSE_XORBS_TABLE).map_err(io_err)?;
        for h in &young {
            if sparse.remove(h).map_err(io_err)?.is_some() {
                removed += 1;
            }
        }
    }
    write_txn.commit().map_err(io_err)?;
    Ok(removed)
}

/// Downloads the transaction lock file to `txn_path`.
fn download_lock(rt: &Runtime, client: &aws_sdk_s3::Client, bucket: &str, txn_path: &str) -> PyResult<()> {
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let data = s3_download_object(client, bucket, key, MAX_S3_ATTEMPTS, true).await?
            .ok_or_else(|| format!("Lock file {} not found", key))?;
        std::fs::write(txn_path, data).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(io_err)
}

fn upload_lock(rt: &Runtime, client: &aws_sdk_s3::Client, bucket: &str, txn_path: &str) -> PyResult<()> {
    rt.block_on(async {
        s3_upload_file_reqwest_http1(client, bucket, "gc/active_transaction.redb", txn_path, MAX_S3_ATTEMPTS).await
            .map_err(|e| format!("Failed to upload lock: {e}"))
    }).map_err(io_err)?;
    let _ = std::fs::remove_file(txn_path);
    Ok(())
}

/// Returns the LastModified time (unix seconds) of an object, or None if it doesn't exist.
async fn s3_last_modified(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Result<Option<u64>, String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match client.head_object().bucket(bucket).key(key).send().await {
            Ok(resp) => return Ok(resp.last_modified().map(|t| t.secs().max(0) as u64)),
            Err(e) => {
                if let Some(service_err) = e.as_service_error() {
                    if service_err.is_not_found() {
                        return Ok(None);
                    }
                }
                if attempts >= MAX_S3_ATTEMPTS {
                    return Err(format!("HEAD {key} failed after {attempts} attempts: {e:?}"));
                }
                tokio::time::sleep(crate::s3_client::calculate_backoff(attempts)).await;
            }
        }
    }
}

/// Attempts per move before the step stops with an error (each copy also retries internally).
const MOVE_ATTEMPTS: u32 = 3;

#[derive(Debug, PartialEq)]
enum MoveOutcome {
    Moved,
    /// The source is gone and the destination exists: an earlier, interrupted run moved it.
    AlreadyMoved,
    /// Neither exists, even after re-checking.
    NothingToMove,
}

/// Moves an object within the bucket: copy, confirm the copy exists, then delete the source.
///
/// The source is only deleted after a HEAD confirms the destination, so a copy that failed or
/// was wrongly reported as "source not found" can never lose data. Retries a few times, then
/// returns an error with the source kept, so the step stops and can be run again.
async fn s3_move_object(client: &aws_sdk_s3::Client, bucket: &str, src_key: &str, dest_key: &str) -> Result<MoveOutcome, String> {
    for attempt in 1..=MOVE_ATTEMPTS {
        if s3_last_modified(client, bucket, src_key).await?.is_none() {
            if s3_last_modified(client, bucket, dest_key).await?.is_some() {
                return Ok(MoveOutcome::AlreadyMoved);
            }
            if attempt < MOVE_ATTEMPTS {
                // Re-check in case the "not found" was misreported.
                tokio::time::sleep(crate::s3_client::calculate_backoff(attempt)).await;
                continue;
            }
            return Ok(MoveOutcome::NothingToMove);
        }

        let copy_source = format!("{bucket}/{src_key}");
        let copied = s3_copy_object(client, bucket, &copy_source, dest_key, MAX_S3_ATTEMPTS, false).await;
        if copied.is_ok() && s3_last_modified(client, bucket, dest_key).await?.is_some() {
            s3_delete_object(client, bucket, src_key, MAX_S3_ATTEMPTS).await
                .map_err(|e| format!("Copied {src_key} to {dest_key} but could not delete the source: {e}"))?;
            return Ok(MoveOutcome::Moved);
        }
        eprintln!("[GC Move] {src_key} -> {dest_key} not confirmed (attempt {attempt}/{MOVE_ATTEMPTS}): {copied:?}");
        tokio::time::sleep(crate::s3_client::calculate_backoff(attempt)).await;
    }
    Err(format!("Could not move {src_key} to {dest_key} after {MOVE_ATTEMPTS} attempts; the source was kept. Run the step again."))
}

/// Runs `f` on every item with up to MAX_CONCURRENCY in flight. Stops starting new items
/// after the first error (items already running finish) and returns that error.
async fn for_each_concurrent<I, T, F, Fut>(items: I, f: F) -> Result<(), String>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    use futures::stream::{iter, TryStreamExt};
    iter(items.into_iter().map(Ok::<T, String>))
        .try_for_each_concurrent(MAX_CONCURRENCY, f)
        .await
}

/// Finds where a segment of an old xorb (chunks `start..end`) went during repacking. Returns the
/// new xorb and start index only if every chunk moved, in order, to consecutive positions of
/// the same new xorb.
/// Rewrites a CAS info block's chunk positions as cumulative unpacked bytes, the way xet-core
/// writes them. Blocks from uploads are unchanged; blocks written by earlier GC runs (which used
/// packed positions) are corrected when GC copies them into a new shard.
fn with_unpacked_positions(mut info: MDBXorbInfo) -> MDBXorbInfo {
    let mut pos: u32 = 0;
    for c in &mut info.chunks {
        c.chunk_byte_range_start = pos;
        pos = pos.saturating_add(c.unpacked_segment_bytes);
    }
    info.metadata.num_bytes_in_xorb = pos;
    info
}

fn remap_segment(
    chunk_map: &impl ReadableTable<&'static [u8; 36], &'static [u8; 36]>,
    old_xorb: &[u8; 32],
    start: u32,
    end: u32,
) -> Option<(MerkleHash, u32)> {
    let new_pos = |idx: u32| -> Option<([u8; 32], u32)> {
        let mut key = [0u8; 36];
        key[0..32].copy_from_slice(old_xorb);
        key[32..36].copy_from_slice(&idx.to_le_bytes());
        let val = chunk_map.get(&key).ok()??;
        let v = val.value();
        Some((<[u8; 32]>::try_from(&v[0..32]).ok()?, u32::from_le_bytes(v[32..36].try_into().ok()?)))
    };
    let (new_xorb, new_start) = new_pos(start)?;
    for k in 1..end.saturating_sub(start) {
        if new_pos(start + k)? != (new_xorb, new_start + k) {
            return None;
        }
    }
    Some((MerkleHash::from(new_xorb), new_start))
}

/// Files looked up per batch in the GC verify scans.
const VERIFY_LOOKUP_BATCH: usize = 1000;

fn get_current_rss_mb() -> f64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<f64>() {
                        return kb / 1024.0;
                    }
                }
            }
        }
    }
    0.0
}

use mdb_shard::metadata_shard::{ChunkIndexSizeEnforcement, ShardFileManager};

/// `drop_entries`: (file hash, shard hash hex) of file entries to leave out, for example the
/// wrong entries `audit --deep` found. Their shards are rewritten even if nothing else in them
/// changes.
pub fn _consolidate_metadata(
    sfm: Arc<ShardFileManager>,
    gc_db: Arc<std::sync::RwLock<Option<Database>>>,
    started_at: u64,
    drop_entries: &HashSet<([u8; 32], String)>,
) -> PyResult<()> {
    let drop_shards: HashSet<&String> = drop_entries.iter().map(|(_, s)| s).collect();
    let mut dropped_entries = 0usize;
    let mut dropped_live_entries = 0usize;
    
    let gc_db_lock = gc_db.read().unwrap();
    let db = match &*gc_db_lock {
        Some(d) => d,
        None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized")),
    };
    
    let read_txn = db.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
    let live_chunks = read_txn.open_table(GC_LIVE_CHUNKS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let live_files = read_txn.open_table(GC_LIVE_FILES_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let live_xorbs = read_txn.open_table(GC_LIVE_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let xorb_chunks = read_txn.open_table(GC_XORB_CHUNKS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;

    let rt = Runtime::new().unwrap();

    // 0. S3 Network Setup
    let (client, bucket) = create_s3_client()?;

    // Bin-Packing State
    let max_xorb_size = 64 * 1024 * 1024; // 64 MB target
    // Buffer State
    let mut xorb_new_chunks: Vec<u8> = Vec::new();
    let mut new_entries: Vec<XorbChunkSequenceEntry> = Vec::new();
    // (old xorb, old index) of each chunk in the buffer, in buffer order.
    let mut current_old_positions: Vec<[u8; 36]> = Vec::new();
    let mut current_uncompressed_size: u32 = 0;
    // New xorbs that turned out identical to a xorb already in xorbs/default/ (see flush_xorb_buffer).
    let mut reused_xorbs: std::collections::HashMap<MerkleHash, MDBXorbInfo> = std::collections::HashMap::new();

    // 1. Transaction Lock Database (opened at start to stream incrementally)
    let txn_path = "/tmp/active_transaction.redb";
    let _ = std::fs::remove_file(txn_path); // ensure clean
    let txn_db = Database::create(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB error: {e}")))?;
    let write_txn = txn_db.begin_write().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn error: {e}")))?;
    
    let mut meta_table = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    meta_table.insert("status", "consolidated").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert error: {e}")))?;
    meta_table.insert("timestamp", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs().to_string().as_str()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert error: {e}")))?;
    // When start began (before the liveness snapshot). Commit keeps any old xorb written after this.
    meta_table.insert("started_at", started_at.to_string().as_str()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert error: {e}")))?;
    
    let mut old_xorbs_table = write_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let mut new_xorbs_table = write_txn.open_table(TXN_NEW_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let mut old_shards_table = write_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let mut new_shards_table = write_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let mut chunk_map_table = write_txn.open_table(TXN_CHUNK_MAP_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;

    let sparse_xorbs = read_txn.open_table(GC_SPARSE_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;

    // A closure to flush the current buffer to S3
    let flush_xorb_buffer = |chunks: &mut Vec<u8>, entries: &mut Vec<XorbChunkSequenceEntry>, old_positions: &mut Vec<[u8; 36]>, unp_size: &mut u32, map_table: &mut redb::Table<&[u8; 36], &[u8; 36]>, xorbs_table: &mut redb::Table<&str, &[u8]>, reused: &mut std::collections::HashMap<MerkleHash, MDBXorbInfo>| -> PyResult<()> {
        if chunks.is_empty() { return Ok(()); }
        
        let mut xorb_new_full = Vec::new();
        // xet-core hashes every chunk from the data and returns the standard xorb hash (the one
        // it also writes into the footer); the new xorb is named by it, like uploaded xorbs.
        let (_, new_xorb_hash) = reconstruct_xorb_with_footer(&mut xorb_new_full, chunks)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Footer error: {e:?}")))?;
        let new_xorb_hash_str = new_xorb_hash.hex();
        
        for (i, old_pos) in old_positions.iter().enumerate() {
            let mut val = [0u8; 36];
            val[0..32].copy_from_slice(new_xorb_hash.as_bytes());
            val[32..36].copy_from_slice(&(i as u32).to_le_bytes());
            map_table.insert(old_pos, &val).unwrap();
        }

        let xorb_info = MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(new_xorb_hash, entries.len() as u32, *unp_size),
            chunks: entries.clone(),
        };

        // The standard name depends only on the chunks, so a new xorb can come out identical to a
        // xorb that already exists (for example one repacked without dropping any chunk). Then
        // that xorb is reused as is: nothing is uploaded, and it is kept off the delete list below.
        let existing_key = format!("xorbs/default/{new_xorb_hash_str}");
        let new_xorb_key: [u8; 32] = new_xorb_hash.into();
        let known = xorb_chunks.get(&new_xorb_key).ok().flatten().is_some();
        if known && rt.block_on(s3_check_exists(&client, &bucket, &existing_key, MAX_S3_ATTEMPTS)).map_err(io_err)? {
            reused.insert(new_xorb_hash, xorb_info);
            chunks.clear();
            entries.clear();
            old_positions.clear();
            *unp_size = 0;
            return Ok(());
        }

        let mut serialized_bytes = Vec::new();
        xorb_info.serialize(&mut serialized_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialize error: {e:?}")))?;
        xorbs_table.insert(new_xorb_hash_str.as_str(), serialized_bytes.as_slice()).unwrap();

        // Stream XORB natively
        rt.block_on(async {
            let key = format!("gc_consolidated/xorbs/{}", new_xorb_hash_str);
            s3_put_object(&client, &bucket, &key, xorb_new_full.clone(), MAX_S3_ATTEMPTS).await
                .map_err(|e| format!("Failed to put {}: {:?}", key, e))?;
            Ok::<_, String>(())
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;

        // Reset buffer
        chunks.clear();
        entries.clear();
        old_positions.clear();
        *unp_size = 0;
        Ok(())
    };

    // 2. Process Sparse XORBs Incrementally with Parallel Fetching and HTTP Range
    let total_sparse = match sparse_xorbs.len() {
        Ok(len) => len as usize,
        Err(_) => 0,
    };
    eprintln!("[GC] Found {} sparse XORBs to consolidate (RSS: {:.1} MB).", total_sparse, get_current_rss_mb());

    let batch_size = 50;
    let mut chunk = Vec::with_capacity(batch_size);
    let mut batch_idx = 0;
    let total_batches = if total_sparse > 0 { (total_sparse + batch_size - 1) / batch_size } else { 1 };

    let mut sparse_iter = sparse_xorbs.iter().unwrap();
    let mut done = false;
    while !done {
        chunk.clear();
        for _ in 0..batch_size {
            if let Some(item) = sparse_iter.next() {
                let (k, _) = item.unwrap();
                chunk.push(*k.value());
            } else {
                done = true;
                break;
            }
        }
        if chunk.is_empty() {
            break;
        }
        batch_idx += 1;
        eprintln!("[GC] Processing sparse XORB batch {}/{} (RSS: {:.1} MB)...", batch_idx, total_batches, get_current_rss_mb());
        
        // 2a. Separate 0%-utilized (dead) XORBs from live sparse XORBs, and fetch footers only for live sparse XORBs
        let mut live_sparse_chunk = Vec::with_capacity(chunk.len());
        for hash_bytes in &chunk {
            if live_xorbs.get(hash_bytes).unwrap().is_some() {
                live_sparse_chunk.push(*hash_bytes);
            } else {
                // 0% utilized XORB: record in old_xorbs_table for stage/revert/prune, skip S3 download
                let xorb_hash_str = MerkleHash::from(*hash_bytes).hex();
                old_xorbs_table.insert(xorb_hash_str.as_str(), ()).unwrap();
            }
        }

        let footer_results = rt.block_on(async {
            use futures::stream::{StreamExt, iter};
            let futures_iter = live_sparse_chunk.iter().map(|hash_bytes| {
                let client = client.clone();
                let bucket = bucket.clone();
                let hash_bytes = *hash_bytes;
                async move {
                    let xorb_hash = MerkleHash::from(hash_bytes);
                    let xorb_hash_str = xorb_hash.hex();
                    let key = format!("xorbs/default/{}", xorb_hash_str);
                    // A missing xorb is not an error here: nothing is repacked from it, the files
                    // using it are left out of the new shards, and verification then reports them
                    // (status verification_failed). Network errors still stop the run.
                    let footer_bytes_result = s3_download_range(&client, &bucket, &key, "bytes=-1048576", MAX_S3_ATTEMPTS, false).await;
                    (hash_bytes, footer_bytes_result)
                }
            });
            iter(futures_iter).buffer_unordered(batch_size).collect::<Vec<_>>().await
        });
        
        // 2b. Identify Live Chunks and coalesce ranges
        struct RangeReq {
            start: usize,
            end: usize,
            /// (old xorb, old index) of each chunk.
            old_positions: Vec<[u8; 36]>,
            hashes: Vec<MerkleHash>,
            unp_lens: Vec<u32>,
            chunk_lens: Vec<usize>,
        }
        
        let mut xorbs_to_fetch = Vec::new();
        
        for (hash_bytes, footer_bytes_result) in footer_results {
            let footer_bytes_opt = footer_bytes_result.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
            
            let xorb_hash = MerkleHash::from(hash_bytes);
            let xorb_hash_str = xorb_hash.hex();
            old_xorbs_table.insert(xorb_hash_str.as_str(), ()).unwrap();
            
            if footer_bytes_opt.is_none() {
                eprintln!("[GC] XORB {xorb_hash_str} is missing from S3; files using it will be reported by verification.");
            }
            if let Some(footer_bytes) = footer_bytes_opt {
                if let Some((hashes, boundaries, unpacked)) = parse_xorb_footer_data(&footer_bytes) {
                    let mut current_range: Option<RangeReq> = None;
                    let mut ranges = Vec::new();
                    
                    for i in 0..hashes.len() {
                        let h = hashes[i];
                        let h_bytes: [u8; 32] = h.into();
                        
                        let mut layout_key = [0u8; 36];
                        layout_key[0..32].copy_from_slice(&hash_bytes);
                        layout_key[32..36].copy_from_slice(&(i as u32).to_le_bytes());
                        
                        let is_live = live_chunks.get(&h_bytes).unwrap().is_some();
                        if is_live {
                            let start = if i == 0 { 0 } else { boundaries[i - 1] as usize };
                            let end = boundaries[i] as usize;
                            let len = end - start;
                            let unp_len = if i == 0 { unpacked[0] } else { unpacked[i] - unpacked[i - 1] };
                            
                            if let Some(ref mut req) = current_range {
                                if req.end == start { // contiguous
                                    req.end = end;
                                    req.old_positions.push(layout_key);
                                    req.hashes.push(h);
                                    req.unp_lens.push(unp_len);
                                    req.chunk_lens.push(len);
                                } else {
                                    ranges.push(current_range.take().unwrap());
                                    current_range = Some(RangeReq { start, end, old_positions: vec![layout_key], hashes: vec![h], unp_lens: vec![unp_len], chunk_lens: vec![len] });
                                }
                            } else {
                                current_range = Some(RangeReq { start, end, old_positions: vec![layout_key], hashes: vec![h], unp_lens: vec![unp_len], chunk_lens: vec![len] });
                            }
                        }
                    }
                    if let Some(req) = current_range {
                        ranges.push(req);
                    }
                    if !ranges.is_empty() {
                        xorbs_to_fetch.push((hash_bytes, ranges));
                    }
                }
            }
        }
        
        // 2c. Fetch Live Ranges Concurrently
        let mut range_futures = Vec::new();
        for (hash_bytes, ranges) in xorbs_to_fetch {
            let xorb_hash = MerkleHash::from(hash_bytes);
            let xorb_hash_str = xorb_hash.hex();
            for req in ranges {
                let client = client.clone();
                let bucket = bucket.clone();
                let key = format!("xorbs/default/{}", xorb_hash_str);
                let range_header = format!("bytes={}-{}", req.start, req.end - 1);
                
                range_futures.push(async move {
                    let bytes_result = s3_download_range(&client, &bucket, &key, &range_header, MAX_S3_ATTEMPTS, true).await;
                    (req, bytes_result)
                });
            }
        }
        
        let fetched_ranges = rt.block_on(async {
            use futures::stream::{StreamExt, iter};
            iter(range_futures).buffer_unordered(batch_size).collect::<Vec<_>>().await
        });
        
        // 2d. Process Fetched Ranges
        // A range is a run of consecutive live chunks of one old xorb, so every file segment that
        // pointed into it lies inside one range. Ranges are never split across new xorbs: that
        // keeps each segment contiguous in a single new xorb, which the shard rewrite relies on.
        for (req, bytes_result) in fetched_ranges {
            let bytes = bytes_result.map_err(io_err)?
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyIOError, _>("Range download returned no data"))?;
            if bytes.len() != req.end - req.start {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Range download returned {} bytes, expected {}", bytes.len(), req.end - req.start)));
            }

            let range_unpacked: u64 = req.unp_lens.iter().map(|l| *l as u64).sum();
            let would_overflow = current_uncompressed_size as u64 + range_unpacked > max_xorb_size as u64
                || current_old_positions.len() + req.hashes.len() > MAX_NEW_XORB_CHUNKS;
            if !current_old_positions.is_empty() && would_overflow {
                flush_xorb_buffer(&mut xorb_new_chunks, &mut new_entries, &mut current_old_positions, &mut current_uncompressed_size, &mut chunk_map_table, &mut new_xorbs_table, &mut reused_xorbs)?;
            }

            let mut offset = 0;
            for j in 0..req.hashes.len() {
                let len = req.chunk_lens[j];
                xorb_new_chunks.extend_from_slice(&bytes[offset..offset + len]);
                current_old_positions.push(req.old_positions[j]);
                // Chunk positions in CAS info are cumulative unpacked bytes, as xet-core writes them.
                new_entries.push(XorbChunkSequenceEntry::new(req.hashes[j], req.unp_lens[j], current_uncompressed_size));
                current_uncompressed_size += req.unp_lens[j];
                offset += len;
            }

            if current_uncompressed_size >= max_xorb_size || current_old_positions.len() >= MAX_NEW_XORB_CHUNKS {
                flush_xorb_buffer(&mut xorb_new_chunks, &mut new_entries, &mut current_old_positions, &mut current_uncompressed_size, &mut chunk_map_table, &mut new_xorbs_table, &mut reused_xorbs)?;
            }
        }
    }
    
    // Flush any remaining partial XORB
    flush_xorb_buffer(&mut xorb_new_chunks, &mut new_entries, &mut current_old_positions, &mut current_uncompressed_size, &mut chunk_map_table, &mut new_xorbs_table, &mut reused_xorbs)?;

    // 3. Process Old Shards Incrementally and Stream into New Shards
    let mut current_shard = MDBInMemoryShard::default();
    let mut added_xorbs: HashSet<MerkleHash> = HashSet::new();
    let max_shard_size: u64 = 64 * 1024 * 1024; // 64 MB target
    let mut unwritten_files = 0;
    // Keys under xorbs/default/, listed once on first need (see "still in use" below).
    let mut stored_xorbs: Option<HashSet<String>> = None;
    let mut blocks_kept_without_files = 0usize;

    // Shards are named by the hash of their bytes, so a rewrite that changes nothing comes out
    // with the name of the shard it replaces (for example a shard an earlier, reverted GC run
    // wrote). Uploading it as "new" and moving the "old" one to bin/ would remove that one object
    // from shards/. Such a shard is kept as it is instead (see the end of this function).
    let registered_shards: HashSet<String> = rt.block_on(sfm.registered_shard_list())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?
        .iter().map(|s| s.shard_hash.hex()).collect();
    let mut unchanged_shards: HashSet<String> = HashSet::new();
    let flush_shard = |shard_mem: &mut MDBInMemoryShard, new_shards_table: &mut redb::Table<&str, ()>, added_xorbs: &mut HashSet<MerkleHash>, unchanged: &mut HashSet<String>| -> PyResult<()> {
        let shard_bytes = shard_mem.to_bytes()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialize error: {e:?}")))?;
        
        let new_shard_hash = compute_data_hash(&shard_bytes);
        let new_shard_hash_str = new_shard_hash.hex();
        if registered_shards.contains(&new_shard_hash_str) {
            unchanged.insert(new_shard_hash_str);
            *shard_mem = MDBInMemoryShard::default();
            added_xorbs.clear();
            return Ok(());
        }
        
        rt.block_on(async {
            let key = format!("gc_consolidated/shards/{}.mdb", new_shard_hash_str);
            s3_put_object(&client, &bucket, &key, shard_bytes.clone(), MAX_S3_ATTEMPTS).await
                .map_err(|e| format!("Failed to put {}: {:?}", key, e))?;
            Ok::<_, String>(())
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
        
        new_shards_table.insert(new_shard_hash_str.as_str(), ()).unwrap();
        
        *shard_mem = MDBInMemoryShard::default();
        added_xorbs.clear();
        Ok(())
    };
    
    let shards = rt.block_on(async {
        sfm.registered_shard_list().await
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;
    
    for shard_file in shards {
        let shard_hash_str = shard_file.shard_hash.hex();
        let shard_path = shard_file.path.to_string_lossy().into_owned();
        
        let mut file = match std::fs::File::open(&shard_path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        
        if let Ok(minimal_shard) = MDBMinimalShard::from_reader(&mut file, true, true) {
            let mut has_sparse = false;

            for i in 0..minimal_shard.num_files() {
                if let Some(file_view) = minimal_shard.file(i) {
                    let file_info = MDBFileInfo::from(file_view);
                    for seg in &file_info.segments {
                        let seg_hash_bytes: [u8; 32] = seg.xorb_hash.into();
                        if sparse_xorbs.get(&seg_hash_bytes).unwrap().is_some() {
                            has_sparse = true;
                        }
                        // A file entry pointing at a xorb no shard lists and S3 doesn't have.
                        // Comes from uploads made with a stale or broken client cache before
                        // shard verification existed (verification now rejects them). Rewrite the
                        // shard: the entry is dropped unless a live file still uses that xorb.
                        // GC_XORB_CHUNKS_TABLE already reflects S3 (see init_gc), so this is a
                        // local table lookup per entry, no extra S3 request.
                        if xorb_chunks.get(&seg_hash_bytes).unwrap().is_none() {
                            if !has_sparse {
                                eprintln!("[GC] Shard {shard_hash_str} references missing XORB {}; rewriting it.", seg.xorb_hash.hex());
                            }
                            has_sparse = true;
                        }
                    }
                }
            }
            for i in 0..minimal_shard.num_xorb() {
                if let Some(xorb_view) = minimal_shard.xorb(i) {
                    let x_bytes: [u8; 32] = xorb_view.xorb_hash().into();
                    if sparse_xorbs.get(&x_bytes).unwrap().is_some() {
                        has_sparse = true;
                    }
                }
            }
            
            if drop_shards.contains(&shard_hash_str) {
                has_sparse = true;
            }
            if !has_sparse {
                continue;
            }
            old_shards_table.insert(shard_hash_str.as_str(), ()).unwrap();
            
            // Note: we don't need to rebuild old_xorb_layouts from the shard because we already 
            // populated layout_table from the S3 fetch.
            
            let mut existing_xorb_blocks: std::collections::HashMap<MerkleHash, MDBXorbInfo> = std::collections::HashMap::new();
            for i in 0..minimal_shard.num_xorb() {
                if let Some(xorb_view) = minimal_shard.xorb(i) {
                    let x_hash = xorb_view.xorb_hash();
                    let x_bytes: [u8; 32] = x_hash.into();
                    if sparse_xorbs.get(&x_bytes).unwrap().is_some() {
                        let x_hash_str = x_hash.hex();
                        old_xorbs_table.insert(x_hash_str.as_str(), ()).unwrap();
                    } else if live_xorbs.get(&x_bytes).unwrap().is_some() {
                        existing_xorb_blocks.insert(x_hash, with_unpacked_positions(MDBXorbInfo::from(xorb_view)));
                    }
                }
            }

            for i in 0..minimal_shard.num_files() {
                if let Some(file_view) = minimal_shard.file(i) {
                    let fh_bytes: [u8; 32] = file_view.file_hash().into();
                    let is_live = live_files.get(&fh_bytes).unwrap().is_some();
                    if !drop_shards.is_empty() && drop_entries.contains(&(fh_bytes, shard_hash_str.clone())) {
                        dropped_entries += 1;
                        if is_live {
                            // Still in use: verification reports the file if no other entry is left.
                            dropped_live_entries += 1;
                            eprintln!("[GC] Dropping entry of file {} in shard {shard_hash_str}, but the file is still in use.", file_view.file_hash().hex());
                        }
                        continue;
                    }
                    if is_live {
                        let mut file_info = MDBFileInfo::from(file_view);
                        let mut valid = true;
                        
                        for seg in &mut file_info.segments {
                            let seg_hash_bytes: [u8; 32] = seg.xorb_hash.into();
                            let is_sparse = sparse_xorbs.get(&seg_hash_bytes).unwrap().is_some();
                            if is_sparse {
                                // Every chunk of the segment must have moved to consecutive
                                // positions of one new xorb; otherwise the entry can't be remapped
                                // (it is left out and verification reports the file).
                                match remap_segment(&chunk_map_table, &seg_hash_bytes, seg.chunk_index_start, seg.chunk_index_end) {
                                    Some((new_xorb_hash, new_start_idx)) => {
                                        let length = seg.chunk_index_end - seg.chunk_index_start;
                                        seg.xorb_hash = new_xorb_hash;
                                        seg.chunk_index_start = new_start_idx;
                                        seg.chunk_index_end = new_start_idx + length;
                                    },
                                    None => {
                                        valid = false;
                                        break;
                                    },
                                }
                            } else if live_xorbs.get(&seg_hash_bytes).unwrap().is_none() {
                                valid = false;
                                break;
                            }
                        }
                        if valid {
                            for seg in &file_info.segments {
                                if !added_xorbs.contains(&seg.xorb_hash) {
                                    let x_str = seg.xorb_hash.hex();
                                    if let Some(xorb_info) = reused_xorbs.get(&seg.xorb_hash) {
                                        current_shard.add_xorb_block(xorb_info.clone())
                                            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Shard error: {e:?}")))?;
                                        added_xorbs.insert(seg.xorb_hash);
                                    } else if let Some(xorb_val) = new_xorbs_table.get(x_str.as_str()).unwrap() {
                                        let bytes = xorb_val.value();
                                        if let Ok(Some(xorb_info)) = MDBXorbInfo::deserialize(&mut std::io::Cursor::new(bytes)) {
                                            current_shard.add_xorb_block(xorb_info)
                                                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Shard error: {e:?}")))?;
                                            added_xorbs.insert(seg.xorb_hash);
                                        }
                                    } else if let Some(xorb_info) = existing_xorb_blocks.get(&seg.xorb_hash) {
                                        current_shard.add_xorb_block(xorb_info.clone())
                                            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Shard error: {e:?}")))?;
                                        added_xorbs.insert(seg.xorb_hash);
                                    } else {
                                        let seg_xorb_bytes: [u8; 32] = seg.xorb_hash.into();
                                        if let Ok(Some(xorb_val)) = xorb_chunks.get(&seg_xorb_bytes) {
                                            let bytes = xorb_val.value();
                                            let total_entries = bytes.len() / 40;
                                            let mut entries = Vec::with_capacity(total_entries);
                                            let mut offset_acc: u32 = 0;
                                            for idx in 0..total_entries {
                                                let base = idx * 40;
                                                let mut ch_bytes = [0u8; 32];
                                                ch_bytes.copy_from_slice(&bytes[base..base + 32]);
                                                let unp_len = u32::from_le_bytes(bytes[base + 36..base + 40].try_into().unwrap());
                                                entries.push(XorbChunkSequenceEntry::new(MerkleHash::from(ch_bytes), unp_len, offset_acc));
                                                offset_acc = offset_acc.saturating_add(unp_len);
                                            }
                                            let xorb_info = MDBXorbInfo {
                                                metadata: XorbChunkSequenceHeader::new(seg.xorb_hash, entries.len() as u32, offset_acc),
                                                chunks: entries,
                                            };
                                            current_shard.add_xorb_block(xorb_info)
                                                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Shard error: {e:?}")))?;
                                            added_xorbs.insert(seg.xorb_hash);
                                        }
                                    }
                                }
                            }

                            current_shard.add_file_reconstruction_info(file_info)
                                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Shard error: {e:?}")))?;
                            unwritten_files += 1;
                            
                            if current_shard.shard_file_size() >= max_shard_size {
                                flush_shard(&mut current_shard, &mut new_shards_table, &mut added_xorbs, &mut unchanged_shards)?;
                                unwritten_files = 0;
                            }
                        }
                    }
                }
            }
            // Keep the blocks of this shard's xorbs that stay in use even when none of the files
            // written above uses them (their files are in other shards, or were deleted). Uploads
            // only describe the xorbs they created, so this shard may be the only one with such a
            // block; without it the dedup chunk lookup (/v1/chunks) could no longer find the xorb.
            // Only xorbs that are really in S3: shards can list xorbs whose object is gone.
            let mut still_used: Vec<(&MerkleHash, &MDBXorbInfo)> = existing_xorb_blocks.iter()
                .filter(|(h, _)| !added_xorbs.contains(*h))
                .collect();
            if !still_used.is_empty() && stored_xorbs.is_none() {
                stored_xorbs = Some(list_key_suffixes(&rt, &client, &bucket, "xorbs/default/")?);
            }
            if let Some(stored) = &stored_xorbs {
                still_used.retain(|(h, _)| stored.contains(&h.hex()));
            }
            still_used.sort_by_key(|(h, _)| <[u8; 32]>::from(**h));
            for (h, info) in still_used {
                current_shard.add_xorb_block(info.clone())
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Shard error: {e:?}")))?;
                added_xorbs.insert(*h);
                blocks_kept_without_files += 1;
            }
            if current_shard.shard_file_size() >= max_shard_size {
                flush_shard(&mut current_shard, &mut new_shards_table, &mut added_xorbs, &mut unchanged_shards)?;
                unwritten_files = 0;
            }
        }
    }

    if unwritten_files > 0 || current_shard.num_xorb_entries() > 0 {
        flush_shard(&mut current_shard, &mut new_shards_table, &mut added_xorbs, &mut unchanged_shards)?;
    }
    eprintln!("[GC] Carried {blocks_kept_without_files} info blocks of xorbs still in use whose own rewritten shard has no file using them.");
    
    if !drop_entries.is_empty() {
        eprintln!("[GC] Dropped {dropped_entries} of {} requested file entries ({dropped_live_entries} of files still in use).", drop_entries.len());
    }

    // A rewrite identical to an existing shard: that shard stays in shards/ as it is.
    for h in &unchanged_shards {
        old_shards_table.remove(h.as_str()).map_err(io_err)?;
    }
    if !unchanged_shards.is_empty() {
        eprintln!("[GC] {} rewritten shards are identical to existing shards; keeping those as they are.", unchanged_shards.len());
    }

    // A reused xorb holds live chunks again: it must never be deleted, even if it was sparse.
    for h in reused_xorbs.keys() {
        old_xorbs_table.remove(h.hex().as_str()).map_err(io_err)?;
    }
    if !reused_xorbs.is_empty() {
        eprintln!("[GC] {} repacked xorbs are identical to existing xorbs; reusing those instead of uploading copies.", reused_xorbs.len());
    }

    // 4. Update the active transaction metadata status to "consolidated"Lock
    drop(old_xorbs_table);
    drop(new_xorbs_table);
    drop(old_shards_table);
    drop(new_shards_table);
    drop(chunk_map_table);
    drop(meta_table);
    
    write_txn.commit().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit txn error: {e}")))?;
    drop(txn_db);

    // Stream the final REDB lock file up to S3
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        s3_upload_file_reqwest_http1(&client, &bucket, key, txn_path, MAX_S3_ATTEMPTS).await
            .map_err(|e| format!("Failed to put {}: {:?}", key, e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let _ = std::fs::remove_file(txn_path);

    Ok(())
}

pub fn _stage_gc_transaction() -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = create_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let data = s3_download_object(&client, &bucket, key, MAX_S3_ATTEMPTS, true).await?
            .ok_or_else(|| format!("Lock file {} not found", key))?;
        std::fs::write(txn_path, data).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let db = Database::open(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB err: {e}")))?;
    let write_txn = db.begin_write().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn err: {e}")))?;
    
    {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let status = meta.get("status").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        if status.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot stage: status is missing"));
        }
        let status_val = status.unwrap().value().to_string();
        // Only a transaction whose verification found no problem at all is published: no file
        // missing after GC (including files already broken before it, since GC drops entries
        // that point at missing xorbs) and no wrong entry in the new shards.
        if status_val != "verified" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Cannot stage: status is '{status_val}', it must be 'verified'. Fix the reported problems and run GC start again, or revert.")));
        }
        // Never publish shards whose file entries point at the wrong chunks.
        let bad_entries: usize = meta.get("bad_file_entries").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?
            .and_then(|v| v.value().parse().ok()).unwrap_or(0);
        if bad_entries > 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Cannot stage: verification found {bad_entries} file entries in the new shards that point at the wrong chunks. Run GC start again.")));
        }
        
        let new_xorbs_table = write_txn.open_table(TXN_NEW_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let new_shards_table = write_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let old_shards_table = write_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;

        let new_xorbs: Vec<String> = new_xorbs_table.iter().unwrap().map(|item| item.unwrap().0.value().to_string()).collect();
        let new_shards: Vec<String> = new_shards_table.iter().unwrap().map(|item| item.unwrap().0.value().to_string()).collect();
        let old_shards: Vec<String> = old_shards_table.iter().unwrap().map(|item| item.unwrap().0.value().to_string()).collect();

        // The three phases stay in order (new shards reference new xorbs; old shards go only
        // once their replacements are in place). Items within a phase run concurrently.
        rt.block_on(async {
            // Copy XORBs
            eprintln!("[GC Stage] Copying {} new XORBs...", new_xorbs.len());
            for_each_concurrent(new_xorbs, |hash| {
                let (client, bucket) = (&client, &bucket);
                async move {
                    let src = format!("gc_consolidated/xorbs/{}", hash);
                    let dest = format!("xorbs/default/{}", hash);
                    s3_copy_object_verified(client, bucket, &src, &dest, MAX_S3_ATTEMPTS).await
                        .map_err(|e| format!("Copy failed for {}: {:?}", hash, e))
                }
            }).await?;

            // Copy Shards
            eprintln!("[GC Stage] Copying {} new shards...", new_shards.len());
            for_each_concurrent(new_shards, |hash| {
                let (client, bucket) = (&client, &bucket);
                async move {
                    let src = format!("gc_consolidated/shards/{}.mdb", hash);
                    let dest = format!("shards/{}.mdb", hash);
                    s3_copy_object_verified(client, bucket, &src, &dest, MAX_S3_ATTEMPTS).await
                        .map_err(|e| format!("Copy failed for {}: {:?}", hash, e))?;

                    // Delete any existing tombstone for this new shard (resurrection)
                    let tombstone_key = format!("shards/tombstones/{}.revoked", hash);
                    let _ = s3_delete_object(client, bucket, &tombstone_key, MAX_S3_ATTEMPTS).await;
                    Ok(())
                }
            }).await?;

            // Tombstone and move old shards
            eprintln!("[GC Stage] Moving {} old shards to bin...", old_shards.len());
            for_each_concurrent(old_shards, |hash_str| {
                let (client, bucket) = (&client, &bucket);
                async move {
                    // Move old shard to bin
                    s3_move_object(client, bucket, &format!("shards/{hash_str}.mdb"), &format!("bin/shards/{hash_str}.mdb")).await?;

                    // Create tombstone
                    let key = format!("shards/tombstones/{}.revoked", hash_str);
                    s3_put_object(client, bucket, &key, Vec::new(), MAX_S3_ATTEMPTS).await
                        .map(|_| ())
                        .map_err(|e| format!("Failed to put tombstone {}: {:?}", key, e))
                }
            }).await?;

            // Old XORBs stay in xorbs/default/ until commit: clients may still hold dedup
            // answers that point at them, and commit re-checks whether they came back into use.
            Ok::<_, String>(())
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;

        meta.insert("status", "staged").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        meta.insert("staged_at", unix_now().to_string().as_str()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    }
    
    write_txn.commit().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit err: {e}")))?;
    drop(db);
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        s3_upload_file_reqwest_http1(&client, &bucket, key, txn_path, MAX_S3_ATTEMPTS).await
            .map_err(|e| format!("Failed to upload staged lock: {e}"))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let _ = std::fs::remove_file(txn_path);
    Ok(())
}

fn _verify_pre_gc_transaction(
    sfm: Arc<ShardFileManager>,
    gc_db: Arc<std::sync::RwLock<Option<Database>>>,
    client: &aws_sdk_s3::Client,
    bucket: &str,
    write_txn: &redb::WriteTransaction,
    rt: &Runtime,
) -> PyResult<usize> {
    eprintln!("[GC Verify PRE] Phase 2: Scanning live files from GC_LIVE_FILES_TABLE... (Current RSS: {} MB)", get_current_rss_mb());
    let mut missing_count = 0;
    let mut files_scanned = 0;
    
    let mut missing_table = write_txn.open_table(TXN_MISSING_FILES_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    let mut unique_xorbs_table = write_txn.open_table(TXN_UNIQUE_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    
    let gc_db_lock = gc_db.read().unwrap();
    if let Some(ref gcdb) = *gc_db_lock {
        let read_txn = gcdb.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn error: {e}")))?;
        if let Ok(files_table) = read_txn.open_table(GC_LIVE_FILES_TABLE) {
            // Look files up in parallel in batches; `buffered` keeps table order.
            let mut file_iter = files_table.iter().unwrap();
            loop {
                let chunk: Vec<[u8; 32]> = file_iter.by_ref().take(VERIFY_LOOKUP_BATCH).map(|item| {
                    let (file_hash_bytes, _) = item.unwrap();
                    let mut h = [0u8; 32];
                    h.copy_from_slice(file_hash_bytes.value());
                    h
                }).collect();
                if chunk.is_empty() {
                    break;
                }
                let lookups = rt.block_on(async {
                    futures::stream::StreamExt::collect::<Vec<_>>(
                        futures::stream::StreamExt::buffered(
                            futures::stream::iter(chunk.iter().map(|h| {
                                // Lookups scan shards synchronously: spawn them onto the
                                // runtime's worker threads so they run in parallel.
                                let index = sfm.clone();
                                let mh = MerkleHash::from(*h);
                                let task = rt.spawn(async move { index.get_file_reconstruction_info(&mh).await });
                                async move { task.await.expect("shard lookup task panicked") }
                            })),
                            MAX_CONCURRENCY,
                        )
                    ).await
                });
                for (h, res) in chunk.into_iter().zip(lookups) {
                    files_scanned += 1;
                    if files_scanned % 10000 == 0 {
                        eprintln!("[GC Verify PRE] Phase 2: Scanned {} files (RSS: {} MB)", files_scanned, get_current_rss_mb());
                    }
                
                    let file_hash_hex = MerkleHash::from(h).hex();
                
                    let mut xorb_deps = Vec::new();
                    let mut found = true;
                
                
                    match res {
                        Ok(Some((info, _))) => {
                            for segment in info.segments {
                                xorb_deps.push(segment.xorb_hash.hex());
                            }
                        },
                        _ => {
                            found = false;
                        }
                    }
                
                    if !found {
                        missing_table.insert(file_hash_hex.as_str(), ()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
                        missing_count += 1;
                        continue;
                    }
                
                    // Validate XORBs - removed old_xorbs_table check to avoid false positives for repacked files
                    for xh in &xorb_deps {
                        unique_xorbs_table.insert(xh.as_str(), ()).unwrap();
                    }
                }
            }
        }
    }
    
    // 3. Concurrently verify all required XORBs actually exist physically
    eprintln!("[GC Verify PRE] Phase 3: Verifying unique XORBs on S3 in batches of 100... (Current RSS: {} MB)", get_current_rss_mb());
    use futures::stream::{StreamExt, iter};
    let mut missing_xorbs_table = write_txn.open_table(TXN_MISSING_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    
    let mut batch = Vec::with_capacity(100);
    let mut process_batch = |b: &mut Vec<String>| -> PyResult<()> {
        let results = rt.block_on(async {
            let futures_iter = b.iter().map(|xorb_hash_hex| {
                let client = client.clone();
                let bucket = bucket.to_string();
                let xh = xorb_hash_hex.clone();
                async move {
                    let key_live = format!("xorbs/default/{}", xh);
                    let live = s3_check_exists(&client, &bucket, &key_live, MAX_S3_ATTEMPTS).await?;
                    Ok((xh, live))
                }
            });
            
            iter(futures_iter)
                .buffer_unordered(MAX_CONCURRENCY)
                .collect::<Vec<Result<(String, bool), String>>>()
                .await
        });
        
        for item in results {
            let (xh, exists) = item.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
            if !exists {
                eprintln!("[GC Verify PRE] Phase 3: Missing XORB detected: {}", xh);
                missing_xorbs_table.insert(xh.as_str(), ()).unwrap();
            }
        }
        b.clear();
        Ok(())
    };

    let listed_live = list_key_suffixes(rt, client, bucket, "xorbs/default/")?;
    eprintln!("[GC Verify PRE] Phase 3: Listed {} XORBs on S3; checking unlisted ones individually", listed_live.len());

    let mut xorbs_scanned = 0;
    for item in unique_xorbs_table.iter().unwrap() {
        let (k, _) = item.unwrap();
        let xh = k.value().to_string();
        if listed_live.contains(&xh) {
            continue;
        }
        batch.push(xh);
        if batch.len() >= 100 {
            process_batch(&mut batch)?;
            xorbs_scanned += 100;
            if xorbs_scanned % 1000 == 0 {
                eprintln!("[GC Verify PRE] Phase 3: Checked {} unlisted XORBs individually (RSS: {} MB)", xorbs_scanned, get_current_rss_mb());
            }
        }
    }
    if !batch.is_empty() {
        process_batch(&mut batch)?;
    }
    
    // 4. Second pass to stream files and check for missing XORB intersections
    eprintln!("[GC Verify PRE] Phase 4: Resolving missing files for missing XORBs... (Current RSS: {} MB)", get_current_rss_mb());
    let mut missing_xorbs_empty = true;
    for _ in missing_xorbs_table.iter().unwrap() {
        missing_xorbs_empty = false;
        break;
    }

    if !missing_xorbs_empty {
        if let Some(ref gcdb) = *gc_db_lock {
            let read_txn = gcdb.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn error: {e}")))?;
            if let Ok(files_table) = read_txn.open_table(GC_LIVE_FILES_TABLE) {
                for item in files_table.iter().unwrap() {
                    let (file_hash_bytes, _) = item.unwrap();
                    let mut h = [0u8; 32];
                    h.copy_from_slice(file_hash_bytes.value());
                    let file_hash_hex = MerkleHash::from(h).hex();
                    
                    if missing_table.get(file_hash_hex.as_str()).unwrap().is_some() {
                        continue;
                    }
                    
                    let mut xorb_deps = Vec::new();
                    let res = rt.block_on(async { sfm.get_file_reconstruction_info(&MerkleHash::from_hex(&file_hash_hex).unwrap()).await });
                    if let Ok(Some((info, _))) = res {
                        for segment in info.segments {
                            xorb_deps.push(segment.xorb_hash.hex());
                        }
                    }
                    
                    let mut has_missing = false;
                    for xh in xorb_deps {
                        if missing_xorbs_table.get(xh.as_str()).unwrap().is_some() {
                            has_missing = true;
                            break;
                        }
                    }
                    if has_missing {
                        missing_table.insert(file_hash_hex.as_str(), ()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
                        missing_count += 1;
                    }
                }
            }
        }
    }

    Ok(missing_count)
}

fn _verify_post_gc_transaction(
    sfm: Arc<ShardFileManager>,
    gc_db: Arc<std::sync::RwLock<Option<Database>>>,
    client: &aws_sdk_s3::Client,
    bucket: &str,
    write_txn: &redb::WriteTransaction,
    rt: &Runtime,
) -> PyResult<usize> {
    eprintln!("[GC Verify POST] Phase 1: Preparing temp ShardFileManager...");
    
    // Extract new shards and old shards from txn DB
    let mut new_shards = Vec::new();
    if let Ok(shards_table) = write_txn.open_table(TXN_NEW_SHARDS_TABLE) {
        for item in shards_table.iter().unwrap() {
            let (hash_key, _) = item.unwrap();
            new_shards.push(hash_key.value().to_string());
        }
    }
    let mut old_shards = std::collections::HashSet::new();
    if let Ok(old_shards_table) = write_txn.open_table(TXN_OLD_SHARDS_TABLE) {
        for item in old_shards_table.iter().unwrap() {
            let (hash_key, _) = item.unwrap();
            old_shards.insert(hash_key.value().to_string());
        }
    }

    let temp_dir_path = std::env::temp_dir().join(format!("gc_verify_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&temp_dir_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Tempdir err: {e}")))?;
    
    let ctx = xet_runtime::core::context::XetContext::default()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Context err: {e:?}")))?;
    
    let verify_sfm = Arc::new(rt.block_on(async {
        ShardFileManager::new_in_cache_directory(&ctx, temp_dir_path.to_string_lossy().to_string()).await
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("SFM init err: {e:?}")))?);

    let active_shards = rt.block_on(async { sfm.registered_shard_list().await })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("SFM list err: {e:?}")))?;

    let mut paths_to_register = Vec::new();
    for shard in active_shards {
        if !old_shards.contains(&shard.shard_hash.hex()) {
            paths_to_register.push(shard.path.clone());
        }
    }

    eprintln!("[GC Verify POST] Phase 1: Registering {} existing shards to temp SFM", paths_to_register.len());
    rt.block_on(async {
        verify_sfm.register_shards_by_path(&paths_to_register, ChunkIndexSizeEnforcement::StopAtCap).await
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Register err: {e:?}")))?;

    eprintln!("[GC Verify POST] Phase 1: Downloading and registering {} new shards", new_shards.len());
    for ns in &new_shards {
        let key = format!("gc_consolidated/shards/{}.mdb", ns);
        let data = rt.block_on(async { s3_download_object(client, bucket, &key, MAX_S3_ATTEMPTS, true).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Missing new shard: {}", key)))?;
        
        let path = temp_dir_path.join(format!("{}.mdb", ns));
        std::fs::write(&path, &data).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write err: {e}")))?;
        rt.block_on(async { verify_sfm.register_shards_by_path(&[path], ChunkIndexSizeEnforcement::StopAtCap).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Register new err: {e:?}")))?;
    }

    eprintln!("[GC Verify POST] Phase 2: Scanning live files from GC_LIVE_FILES_TABLE...");
    let mut missing_count = 0;
    let mut files_scanned = 0;
    let mut missing_table = write_txn.open_table(TXN_MISSING_POST_FILES_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    let mut post_xorbs_table = write_txn.open_table(redb::TableDefinition::<&str, ()>::new("txn_unique_post_xorbs")).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    let old_xorbs_table = write_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    
    let gc_db_lock = gc_db.read().unwrap();
    if let Some(ref gcdb) = *gc_db_lock {
        let read_txn = gcdb.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn error: {e}")))?;
        if let Ok(files_table) = read_txn.open_table(GC_LIVE_FILES_TABLE) {
            // Look files up in parallel in batches; `buffered` keeps table order.
            let mut file_iter = files_table.iter().unwrap();
            loop {
                let chunk: Vec<[u8; 32]> = file_iter.by_ref().take(VERIFY_LOOKUP_BATCH).map(|item| {
                    let (file_hash_bytes, _) = item.unwrap();
                    let mut h = [0u8; 32];
                    h.copy_from_slice(file_hash_bytes.value());
                    h
                }).collect();
                if chunk.is_empty() {
                    break;
                }
                let lookups = rt.block_on(async {
                    futures::stream::StreamExt::collect::<Vec<_>>(
                        futures::stream::StreamExt::buffered(
                            futures::stream::iter(chunk.iter().map(|h| {
                                // Lookups scan shards synchronously: spawn them onto the
                                // runtime's worker threads so they run in parallel.
                                let index = verify_sfm.clone();
                                let mh = MerkleHash::from(*h);
                                let task = rt.spawn(async move { index.get_file_reconstruction_info(&mh).await });
                                async move { task.await.expect("shard lookup task panicked") }
                            })),
                            MAX_CONCURRENCY,
                        )
                    ).await
                });
                for (h, res) in chunk.into_iter().zip(lookups) {
                    files_scanned += 1;
                    if files_scanned % 10000 == 0 {
                        eprintln!("[GC Verify POST] Phase 2: Scanned {} files", files_scanned);
                    }
                
                    let file_hash_hex = MerkleHash::from(h).hex();
                
                    let mut found_old_xorb = false;
                    match res {
                        Ok(Some((info, _))) => {
                            for segment in info.segments {
                                let xh = segment.xorb_hash.hex();
                                if old_xorbs_table.get(xh.as_str()).unwrap().is_some() {
                                    found_old_xorb = true;
                                    break;
                                }
                                post_xorbs_table.insert(xh.as_str(), ()).unwrap();
                            }
                        },
                        _ => {
                            missing_table.insert(file_hash_hex.as_str(), ()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
                            missing_count += 1;
                            continue;
                        }
                    }
                
                    if found_old_xorb {
                        missing_table.insert(file_hash_hex.as_str(), ()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
                        missing_count += 1;
                    }
                }
            }
        }
    }

    eprintln!("[GC Verify POST] Phase 3: Verifying unique XORBs on S3 in batches of 100...");
    use futures::stream::{StreamExt, iter};
    let mut missing_xorbs_table = write_txn.open_table(redb::TableDefinition::<&str, ()>::new("txn_missing_post_xorbs")).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    
    let mut new_xorbs = std::collections::HashSet::new();
    if let Ok(new_xorbs_table) = write_txn.open_table(TXN_NEW_XORBS_TABLE) {
        for item in new_xorbs_table.iter().unwrap() {
            let (hash_key, _) = item.unwrap();
            new_xorbs.insert(hash_key.value().to_string());
        }
    }

    let mut batch = Vec::with_capacity(100);
    let mut process_batch = |b: &mut Vec<String>| -> PyResult<()> {
        let results = rt.block_on(async {
            let futures_iter = b.iter().map(|xorb_hash_hex| {
                let client = client.clone();
                let bucket = bucket.to_string();
                let xh = xorb_hash_hex.clone();
                let is_new = new_xorbs.contains(&xh);
                async move {
                    let key = if is_new {
                        format!("gc_consolidated/xorbs/{}", xh)
                    } else {
                        format!("xorbs/default/{}", xh)
                    };
                    let exists = s3_check_exists(&client, &bucket, &key, MAX_S3_ATTEMPTS).await?;
                    Ok((xh, exists))
                }
            });
            iter(futures_iter)
                .buffer_unordered(MAX_CONCURRENCY)
                .collect::<Vec<Result<(String, bool), String>>>()
                .await
        });
        
        for item in results {
            let (xh, exists) = item.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
            if !exists {
                missing_xorbs_table.insert(xh.as_str(), ()).unwrap();
            }
        }
        b.clear();
        Ok(())
    };

    let listed_live = list_key_suffixes(rt, client, bucket, "xorbs/default/")?;
    let listed_new = list_key_suffixes(rt, client, bucket, "gc_consolidated/xorbs/")?;
    eprintln!("[GC Verify POST] Phase 3: Listed {} live and {} consolidated XORBs on S3; checking unlisted ones individually", listed_live.len(), listed_new.len());

    let mut xorbs_scanned = 0;
    for item in post_xorbs_table.iter().unwrap() {
        let (k, _) = item.unwrap();
        let xh = k.value().to_string();
        let listed = if new_xorbs.contains(&xh) { &listed_new } else { &listed_live };
        if listed.contains(&xh) {
            continue;
        }
        batch.push(xh);
        if batch.len() >= 100 {
            process_batch(&mut batch)?;
            xorbs_scanned += 100;
            if xorbs_scanned % 1000 == 0 {
                eprintln!("[GC Verify POST] Phase 3: Checked {} unlisted XORBs individually (RSS: {} MB)", xorbs_scanned, get_current_rss_mb());
            }
        }
    }
    if !batch.is_empty() {
        process_batch(&mut batch)?;
    }
    
    eprintln!("[GC Verify POST] Phase 4: Resolving missing files for missing XORBs...");
    let mut missing_xorbs_empty = true;
    for _ in missing_xorbs_table.iter().unwrap() {
        missing_xorbs_empty = false;
        break;
    }

    if !missing_xorbs_empty {
        if let Some(ref gcdb) = *gc_db_lock {
            let read_txn = gcdb.begin_read().unwrap();
            if let Ok(files_table) = read_txn.open_table(GC_LIVE_FILES_TABLE) {
                for item in files_table.iter().unwrap() {
                    let (file_hash_bytes, _) = item.unwrap();
                    let mut h = [0u8; 32];
                    h.copy_from_slice(file_hash_bytes.value());
                    let file_hash_hex = MerkleHash::from(h).hex();
                    
                    if missing_table.get(file_hash_hex.as_str()).unwrap().is_some() {
                        continue;
                    }
                    
                    let mut xorb_deps = Vec::new();
                    let res = rt.block_on(async { verify_sfm.get_file_reconstruction_info(&MerkleHash::from_hex(&file_hash_hex).unwrap()).await });
                    if let Ok(Some((info, _))) = res {
                        for segment in info.segments {
                            xorb_deps.push(segment.xorb_hash.hex());
                        }
                    }
                    
                    let mut has_missing = false;
                    for xh in xorb_deps {
                        if missing_xorbs_table.get(xh.as_str()).unwrap().is_some() {
                            has_missing = true;
                            break;
                        }
                    }
                    if has_missing {
                        missing_table.insert(file_hash_hex.as_str(), ()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
                        missing_count += 1;
                    }
                }
            }
        }
    }

    drop(verify_sfm);
    let _ = std::fs::remove_dir_all(&temp_dir_path);

    Ok(missing_count)
}

pub fn _verify_gc_transaction(
    sfm: Arc<ShardFileManager>,
    gc_db: Arc<std::sync::RwLock<Option<Database>>>,
) -> PyResult<usize> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = create_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let data = s3_download_object(&client, &bucket, key, MAX_S3_ATTEMPTS, true).await?
            .ok_or_else(|| format!("Lock file {} not found", key))?;
        std::fs::write(txn_path, data).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let db = Database::open(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB err: {e}")))?;
    let write_txn = db.begin_write().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn err: {e}")))?;
    
    {
        let meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let status_str = {
            let status = meta.get("status").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
            status.unwrap().value().to_string()
        };
        
        if status_str != "staged" && status_str != "consolidated" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot verify: status is not staged or consolidated"));
        }
    }
    
    // Pass 1: PRE-GC
    let missing_pre_gc = _verify_pre_gc_transaction(sfm.clone(), gc_db.clone(), &client, &bucket, &write_txn, &rt)?;
    eprintln!("[GC Verify] PRE-GC Missing files: {}", missing_pre_gc);

    // Pass 2: POST-GC
    let missing_post_gc = _verify_post_gc_transaction(sfm.clone(), gc_db.clone(), &client, &bucket, &write_txn, &rt)?;
    eprintln!("[GC Verify] POST-GC Missing files: {}", missing_post_gc);

    // Pass 3: every file entry in the new shards must describe its file correctly in the xorbs
    // it points at. Passes 1-2 only check that those xorbs exist.
    let (new_shards, new_xorbs) = {
        let shards_table = write_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(io_err)?;
        let xorbs_table = write_txn.open_table(TXN_NEW_XORBS_TABLE).map_err(io_err)?;
        let shards: Vec<String> = shards_table.iter().map_err(io_err)?
            .map(|item| item.map(|(k, _)| k.value().to_string())).collect::<Result<_, _>>().map_err(io_err)?;
        let xorbs: HashSet<String> = xorbs_table.iter().map_err(io_err)?
            .map(|item| item.map(|(k, _)| k.value().to_string())).collect::<Result<_, _>>().map_err(io_err)?;
        (shards, xorbs)
    };
    let (old_shards, old_xorbs) = {
        let shards_table = write_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(io_err)?;
        let xorbs_table = write_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(io_err)?;
        let shards: HashSet<String> = shards_table.iter().map_err(io_err)?
            .map(|item| item.map(|(k, _)| k.value().to_string())).collect::<Result<_, _>>().map_err(io_err)?;
        let xorbs: HashSet<String> = xorbs_table.iter().map_err(io_err)?
            .map(|item| item.map(|(k, _)| k.value().to_string())).collect::<Result<_, _>>().map_err(io_err)?;
        (shards, xorbs)
    };
    let mut blocks_before: HashSet<String> = HashSet::new();
    let mut blocks_after: HashSet<String> = HashSet::new();
    let (entries_checked, mut bad_entries) = rt.block_on(crate::verify::check_gc_shard_entries(&client, &bucket, &new_shards, &new_xorbs, &old_xorbs, &mut blocks_after))
        .map_err(io_err)?;
    // Shards GC leaves in place must not point at xorbs commit deletes either. True by
    // construction (any shard mentioning such a xorb is rewritten); checked here on the local
    // shard files, every file entry, no network.
    let untouched_checked = check_untouched_shards(&rt, &sfm, &old_shards, &old_xorbs, &mut bad_entries, &mut blocks_before, &mut blocks_after)?;
    eprintln!("[GC Verify] Untouched shards: {untouched_checked} checked for entries pointing at xorbs that commit deletes");

    // Every xorb that stays in use and had a CAS info block in a replaced shard must still have
    // one after GC: without it, uploads can no longer reuse its chunks (wasted storage, not
    // broken files). Xorbs deleted at commit, or missing from S3, are rightly left out.
    {
        let missing_xorbs: HashSet<String> = {
            let t = write_txn.open_table(redb::TableDefinition::<&str, ()>::new("txn_missing_post_xorbs")).map_err(io_err)?;
            t.iter().map_err(io_err)?.map(|item| item.map(|(k, _)| k.value().to_string())).collect::<Result<_, _>>().map_err(io_err)?
        };
        let gc_db_lock = gc_db.read().unwrap();
        let gcdb = gc_db_lock.as_ref().ok_or_else(|| io_err("GC DB not initialized"))?;
        let gc_read = gcdb.begin_read().map_err(io_err)?;
        let live_xorbs = gc_read.open_table(GC_LIVE_XORBS_TABLE).map_err(io_err)?;
        let mut lost = 0usize;
        for x in &blocks_before {
            if old_xorbs.contains(x) || missing_xorbs.contains(x) || blocks_after.contains(x) {
                continue;
            }
            let Ok(h) = MerkleHash::from_hex(x) else { continue };
            let key: [u8; 32] = h.into();
            if live_xorbs.get(&key).map_err(io_err)?.is_some() {
                lost += 1;
                bad_entries.push((x.clone(), "lost_info_block (xorb still in use has no CAS info block in any shard after GC)".to_string()));
            }
        }
        eprintln!("[GC Verify] CAS info blocks: {} xorbs had one in a replaced shard; {lost} in use would be left without one", blocks_before.len());
    }
    eprintln!("[GC Verify] New shards: {} file entries checked, {} problems (wrong file layout, CAS info or xorb name)", entries_checked, bad_entries.len());
    {
        let mut bad_table = write_txn.open_table(TXN_BAD_FILE_ENTRIES_TABLE).map_err(io_err)?;
        for (file_hash, reason) in &bad_entries {
            eprintln!("[GC Verify] Problem in new shards for {file_hash}: {reason}");
            bad_table.insert(file_hash.as_str(), reason.as_str()).map_err(io_err)?;
        }
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(io_err)?;
        meta.insert("bad_file_entries", bad_entries.len().to_string().as_str()).map_err(io_err)?;
    }
    // Files missing after GC that were not missing before: damage GC itself would cause.
    let newly_missing: Vec<String> = {
        let pre = write_txn.open_table(TXN_MISSING_FILES_TABLE).map_err(io_err)?;
        let post = write_txn.open_table(TXN_MISSING_POST_FILES_TABLE).map_err(io_err)?;
        let mut out = Vec::new();
        for item in post.iter().map_err(io_err)? {
            let (k, _) = item.map_err(io_err)?;
            if pre.get(k.value()).map_err(io_err)?.is_none() {
                out.push(k.value().to_string());
            }
        }
        out
    };
    for f in newly_missing.iter().take(100) {
        eprintln!("[GC Verify] File missing after GC that was fine before: {f}");
    }
    {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(io_err)?;
        meta.insert("newly_missing_files", newly_missing.len().to_string().as_str()).map_err(io_err)?;
    }
    eprintln!("[GC Verify] Missing after GC: {missing_post_gc} ({} already missing before GC, {} newly missing)",
        missing_post_gc.saturating_sub(newly_missing.len()), newly_missing.len());
    let problems = missing_post_gc + bad_entries.len();

    eprintln!("[GC Verify] Phase 5: Committing verification results (missing files post-gc: {}, bad entries: {})", missing_post_gc, bad_entries.len());
    if problems == 0 {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        meta.insert("status", "verified").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    } else {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        meta.insert("status", "failed").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    }
    
    write_txn.commit().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit err: {e}")))?;
    drop(db);
    
    eprintln!("[GC Verify] Phase 6: Uploading active_transaction.redb to S3 (RSS: {} MB)...", get_current_rss_mb());
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        s3_upload_file_reqwest_http1(&client, &bucket, key, txn_path, MAX_S3_ATTEMPTS).await
            .map_err(|e| format!("Failed to upload verified lock: {e}"))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let _ = std::fs::remove_file(txn_path);
    Ok(problems)
}

/// Checks every file entry of the registered shards GC leaves in place (not in `old_shards`) for
/// segments pointing at a xorb in `old_xorbs`. Adds each one to `bad` and returns the number of
/// shards checked.
///
/// Also collects which xorbs have a CAS info block: in the shards being replaced
/// (`blocks_before`) and in the shards that stay (`blocks_after`).
fn check_untouched_shards(
    rt: &Runtime,
    sfm: &ShardFileManager,
    old_shards: &HashSet<String>,
    old_xorbs: &HashSet<String>,
    bad: &mut Vec<(String, String)>,
    blocks_before: &mut HashSet<String>,
    blocks_after: &mut HashSet<String>,
) -> PyResult<usize> {
    let shards = rt.block_on(sfm.registered_shard_list())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;
    let mut checked = 0;
    for shard_file in shards {
        let shard_hash = shard_file.shard_hash.hex();
        let mut reader = shard_file.get_reader().map_err(io_err)?;
        let replaced = old_shards.contains(&shard_hash);
        let shard = MDBMinimalShard::from_reader(&mut reader, !replaced, true)
            .map_err(|e| io_err(format!("Cannot parse shard {shard_hash}: {e:?}")))?;
        let blocks = if replaced { &mut *blocks_before } else { &mut *blocks_after };
        for i in 0..shard.num_xorb() {
            if let Some(view) = shard.xorb(i) {
                blocks.insert(view.xorb_hash().hex());
            }
        }
        if replaced {
            continue;
        }
        checked += 1;
        for i in 0..shard.num_files() {
            let Some(view) = shard.file(i) else { continue };
            let info = MDBFileInfo::from(view);
            if let Some(seg) = info.segments.iter().find(|s| old_xorbs.contains(&s.xorb_hash.hex())) {
                bad.push((info.metadata.file_hash.hex(), format!(
                    "points_at_xorb_to_be_deleted ({}) in untouched shard {shard_hash}", seg.xorb_hash.hex())));
            }
        }
    }
    Ok(checked)
}

/// Commits a staged transaction: moves the old xorbs that are still unused to bin/xorbs/.
///
/// Refuses until `min_wait_secs` after stage unless `skip_wait`. Uses the live xorb set in
/// `gc_db`, which the caller must have just rebuilt from the current database. An old xorb is
/// kept if a live file uses it, or if it was written again after start (a client re-uploaded it).
/// Returns (moved, kept) xorb hashes.
pub fn _commit_gc_transaction(
    gc_db: Arc<std::sync::RwLock<Option<Database>>>,
    min_wait_secs: u64,
    skip_wait: bool,
) -> PyResult<(Vec<String>, Vec<String>)> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = create_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    download_lock(&rt, &client, &bucket, txn_path)?;

    let db = Database::open(txn_path).map_err(|e| io_err(format!("DB err: {e}")))?;
    let write_txn = db.begin_write().map_err(io_err)?;
    let moved: Vec<String>;
    let mut kept = Vec::new();
    {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(io_err)?;
        let get = |meta: &redb::Table<&str, &str>, k: &str| -> Option<String> {
            meta.get(k).ok().flatten().map(|v| v.value().to_string())
        };
        let status_val = get(&meta, "status").unwrap_or_default();
        if status_val != "staged" {
            return Err(value_err(format!("Cannot commit: status is '{status_val}', must be 'staged'. Run stage first.")));
        }
        let staged_at: u64 = get(&meta, "staged_at").and_then(|v| v.parse().ok())
            .ok_or_else(|| value_err("Cannot commit: transaction has no staged_at (staged by an older version). Revert it and run GC again."))?;
        let started_at: u64 = get(&meta, "started_at").and_then(|v| v.parse().ok())
            .or_else(|| get(&meta, "timestamp").and_then(|v| v.parse().ok()))
            .unwrap_or(staged_at);

        let now = unix_now();
        let ready_at = staged_at + min_wait_secs;
        if !skip_wait && now < ready_at {
            let hours = (ready_at - now + 3599) / 3600;
            return Err(value_err(format!(
                "Cannot commit yet: wait until {ready_at} (unix), about {hours} more hours, so dedup answers that offered the old xorbs have expired."
            )));
        }

        let gc_db_lock = gc_db.read().unwrap();
        let gcdb = gc_db_lock.as_ref().ok_or_else(|| value_err("GC DB not initialized: run the liveness check before commit"))?;
        let gc_read = gcdb.begin_read().map_err(io_err)?;
        let live_xorbs = gc_read.open_table(GC_LIVE_XORBS_TABLE).map_err(io_err)?;

        let old_xorbs_table = write_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(io_err)?;
        let mut candidates = Vec::new();
        for item in old_xorbs_table.iter().map_err(io_err)? {
            let (k, _) = item.map_err(io_err)?;
            let hash_str = k.value().to_string();
            let Ok(h) = MerkleHash::from_hex(&hash_str) else { continue };
            let h_bytes: [u8; 32] = h.into();
            if live_xorbs.get(&h_bytes).map_err(io_err)?.is_some() {
                eprintln!("[GC Commit] Keeping {hash_str}: used by a live file.");
                kept.push(hash_str);
            } else {
                candidates.push(hash_str);
            }
        }
        drop(old_xorbs_table);

        let results: Vec<(String, Result<Option<u64>, String>)> = rt.block_on(async {
            use futures::stream::{iter, StreamExt};
            iter(candidates.into_iter().map(|hash_str| {
                let client = client.clone();
                let bucket = bucket.clone();
                async move {
                    let res = s3_last_modified(&client, &bucket, &format!("xorbs/default/{hash_str}")).await;
                    (hash_str, res)
                }
            }))
            .buffer_unordered(MAX_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
        });

        let mut to_move = Vec::new();
        for (hash_str, res) in results {
            match res.map_err(io_err)? {
                None => {}, // already gone (e.g. moved by an earlier, interrupted commit)
                Some(modified) if modified >= started_at => {
                    eprintln!("[GC Commit] Keeping {hash_str}: written again after start.");
                    kept.push(hash_str);
                },
                Some(_) => to_move.push(hash_str),
            }
        }

        eprintln!("[GC Commit] Moving {} old XORBs to bin...", to_move.len());
        rt.block_on(for_each_concurrent(&to_move, |hash_str| {
            let (client, bucket) = (&client, &bucket);
            async move {
                s3_move_object(client, bucket, &format!("xorbs/default/{hash_str}"), &format!("bin/xorbs/{hash_str}")).await
                    .map(|_| ())
            }
        })).map_err(io_err)?;
        moved = to_move;

        let mut kept_table = write_txn.open_table(TXN_KEPT_XORBS_TABLE).map_err(io_err)?;
        for h in &kept {
            kept_table.insert(h.as_str(), ()).map_err(io_err)?;
        }
        meta.insert("status", "committed").map_err(io_err)?;
        meta.insert("committed_at", now.to_string().as_str()).map_err(io_err)?;
    }

    write_txn.commit().map_err(io_err)?;
    drop(db);
    upload_lock(&rt, &client, &bucket, txn_path)?;
    Ok((moved, kept))
}

/// Reverts a staged or committed transaction by adding the old shards (and, after commit, the
/// moved old xorbs) back. Never deletes: new shards and xorbs stay, because uploads since
/// stage may dedup against them. Old and new are both valid; the next GC removes the duplicate.
pub fn _revert_gc_transaction() -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = create_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    download_lock(&rt, &client, &bucket, txn_path)?;

    let db = Database::open(txn_path).map_err(|e| io_err(format!("DB err: {e}")))?;
    let write_txn = db.begin_write().map_err(io_err)?;
    {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(io_err)?;
        let status_val = meta.get("status").map_err(io_err)?.map(|s| s.value().to_string()).unwrap_or_default();
        match status_val.as_str() {
            "staged" | "committed" => {},
            "consolidated" | "verified" | "verification_failed" | "failed" => {
                return Err(value_err("Cannot revert: transaction has not been staged yet. Nothing to revert."));
            },
            other => return Err(value_err(format!("Cannot revert: status is '{other}', must be 'staged' or 'committed'"))),
        }

        let old_shards_table = write_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(io_err)?;
        let old_xorbs_table = write_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(io_err)?;

        rt.block_on(async {
            // Old shards back from bin, and their tombstones removed (resurrection).
            for item in old_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();

                // Any failure stops the revert before it is marked "reverted", so prune keeps bin/.
                let outcome = s3_move_object(&client, &bucket, &format!("bin/shards/{hash_str}.mdb"), &format!("shards/{hash_str}.mdb")).await?;
                if outcome == MoveOutcome::NothingToMove {
                    eprintln!("[GC Revert] Old shard {hash_str} is in neither bin/shards/ nor shards/.");
                }
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                s3_delete_object(&client, &bucket, &key, MAX_S3_ATTEMPTS).await?;
            }

            // Old xorbs that commit moved go back too. Done whatever the status says: a commit
            // interrupted before it saved "committed" may already have moved some of them.
            // Xorbs that were never moved are still in xorbs/default/ (AlreadyMoved: nothing to do).
            for item in old_xorbs_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                let outcome = s3_move_object(&client, &bucket, &format!("bin/xorbs/{hash_str}"), &format!("xorbs/default/{hash_str}")).await?;
                if outcome == MoveOutcome::NothingToMove {
                    eprintln!("[GC Revert] Old xorb {hash_str} is in neither bin/xorbs/ nor xorbs/default/.");
                }
            }
            Ok::<_, String>(())
        }).map_err(io_err)?;

        meta.insert("status", "reverted").map_err(io_err)?;
        meta.insert("reverted_at", unix_now().to_string().as_str()).map_err(io_err)?;
    }

    write_txn.commit().map_err(io_err)?;
    drop(db);
    upload_lock(&rt, &client, &bucket, txn_path)
}

/// Reads the lock file's status and committed_at, if a lock exists.
async fn read_lock_state(client: &aws_sdk_s3::Client, bucket: &str, txn_path: &str) -> Result<Option<(String, Option<u64>)>, String> {
    let Some(data) = s3_download_object(client, bucket, "gc/active_transaction.redb", MAX_S3_ATTEMPTS, false).await? else {
        return Ok(None);
    };
    std::fs::write(txn_path, &data).map_err(|e| format!("Failed to save lock: {e}"))?;
    let db = Database::open(txn_path).map_err(|e| format!("DB err: {e}"))?;
    let read_txn = db.begin_read().map_err(|e| format!("Read txn err: {e}"))?;
    let meta = read_txn.open_table(TXN_META_TABLE).map_err(|e| format!("Table err: {e}"))?;
    let status = meta.get("status").map_err(|e| format!("{e}"))?.map(|v| v.value().to_string()).unwrap_or_default();
    let committed_at = meta.get("committed_at").map_err(|e| format!("{e}"))?.and_then(|v| v.value().parse().ok());
    Ok(Some((status, committed_at)))
}

/// Deletes GC leftovers.
///
/// bin/ and gc_consolidated/ are only emptied when no transaction needs them: none exists, it
/// was reverted or failed, or it was committed at least 7 days ago (until then a committed GC
/// can still be reverted). A transaction that is started or staged keeps everything.
pub fn _prune_garbage() -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = create_s3_client()?;
    
    rt.block_on(async {
        let txn_path = "/tmp/active_transaction.redb";
        let lock_key = "gc/active_transaction.redb";
        let now = unix_now();

        let state = read_lock_state(&client, &bucket, txn_path).await?;
        let (bin_free, delete_lock) = match &state {
            None => (true, false),
            Some((status, committed_at)) => match status.as_str() {
                "committed" => {
                    let old_enough = committed_at.map(|t| now >= t + BIN_RETENTION_SECS).unwrap_or(false);
                    (old_enough, old_enough)
                },
                "reverted" | "failed" | "skipped" => (true, true),
                _ => (false, false), // consolidated, verified, verification_failed, staged
            },
        };
        if !bin_free {
            eprintln!("[GC Prune] Keeping bin/ and gc_consolidated/: transaction state {:?} still needs them.", state);
        }

        if bin_free {
            for prefix in &["bin/", "gc_consolidated/"] {
                let mut continuation_token: Option<String> = None;
                loop {
                    let resp = match s3_list_objects_v2(&client, &bucket, prefix, continuation_token.as_deref(), MAX_S3_ATTEMPTS).await {
                        Ok(r) => r,
                        Err(_) => break,
                    };
                    for obj in resp.contents() {
                        if let Some(key) = obj.key() {
                            let _ = s3_delete_object(&client, &bucket, key, MAX_S3_ATTEMPTS).await;
                        }
                    }
                    continuation_token = resp.next_continuation_token().map(String::from);
                    if continuation_token.is_none() {
                        break;
                    }
                }
            }
        }
        
        // Delete tombstones older than 7 days
        let now = now as i64;
        let seven_days = 7 * 24 * 60 * 60;
        
        let mut continuation_token: Option<String> = None;
        loop {
            let resp = match s3_list_objects_v2(&client, &bucket, "shards/tombstones/", continuation_token.as_deref(), MAX_S3_ATTEMPTS).await {
                Ok(r) => r,
                Err(_) => break,
            };
            
            for obj in resp.contents() {
                if let Some(last_modified) = obj.last_modified() {
                    if now - last_modified.secs() > seven_days {
                        if let Some(key) = obj.key() {
                            if key.ends_with(".revoked") {
                                let file_name = key.split('/').last().unwrap();
                                let hash_str = file_name.strip_suffix(".revoked").unwrap();
                                let shard_key = format!("shards/{}.mdb", hash_str);
                                let _ = s3_delete_object(&client, &bucket, &shard_key, MAX_S3_ATTEMPTS).await;
                            }
                            let _ = s3_delete_object(&client, &bucket, key, MAX_S3_ATTEMPTS).await;
                        }
                    }
                }
            }
            
            continuation_token = resp.next_continuation_token().map(String::from);
            if continuation_token.is_none() {
                break;
            }
        }
        
        if delete_lock {
            let _ = s3_delete_object(&client, &bucket, lock_key, MAX_S3_ATTEMPTS).await;
        }
        let _ = std::fs::remove_file(txn_path);

        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    Ok(())
}

pub fn _get_gc_transaction_info(py: Python<'_>) -> PyResult<Py<pyo3::types::PyDict>> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = create_s3_client()?;
    let txn_path = format!("/tmp/active_transaction_explore_{}.redb", std::process::id());
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let data = s3_download_object(&client, &bucket, key, MAX_S3_ATTEMPTS, false).await?
            .ok_or_else(|| format!("Lock file {} not found", key))?;
        std::fs::write(&txn_path, data).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let db = Database::open(&txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB err: {e}")))?;
    let read_txn = db.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn err: {e}")))?;
    
    let dict = pyo3::types::PyDict::new(py);
    
    if let Ok(meta_table) = read_txn.open_table(TXN_META_TABLE) {
        if let Ok(Some(status)) = meta_table.get("status") {
            dict.set_item("status", status.value())?;
        }
        if let Ok(Some(timestamp)) = meta_table.get("timestamp") {
            dict.set_item("timestamp", timestamp.value())?;
        }
        for key in ["started_at", "staged_at", "committed_at", "reverted_at", "bad_file_entries", "newly_missing_files"] {
            if let Ok(Some(v)) = meta_table.get(key) {
                dict.set_item(key, v.value())?;
            }
        }
    }
    
    let new_shards = pyo3::types::PyList::empty(py);
    if let Ok(table) = read_txn.open_table(TXN_NEW_SHARDS_TABLE) {
        if let Ok(iter) = table.iter() {
            for item in iter {
                let (k, _) = item.unwrap();
                new_shards.append(k.value())?;
            }
        }
    }
    dict.set_item("new_shards", new_shards)?;

    let old_shards = pyo3::types::PyList::empty(py);
    if let Ok(table) = read_txn.open_table(TXN_OLD_SHARDS_TABLE) {
        if let Ok(iter) = table.iter() {
            for item in iter {
                let (k, _) = item.unwrap();
                old_shards.append(k.value())?;
            }
        }
    }
    dict.set_item("old_shards", old_shards)?;

    let new_xorbs = pyo3::types::PyList::empty(py);
    if let Ok(table) = read_txn.open_table(TXN_NEW_XORBS_TABLE) {
        if let Ok(iter) = table.iter() {
            for item in iter {
                let (k, _) = item.unwrap();
                new_xorbs.append(k.value())?;
            }
        }
    }
    dict.set_item("new_xorbs", new_xorbs)?;

    let old_xorbs = pyo3::types::PyList::empty(py);
    if let Ok(table) = read_txn.open_table(TXN_OLD_XORBS_TABLE) {
        if let Ok(iter) = table.iter() {
            for item in iter {
                let (k, _) = item.unwrap();
                old_xorbs.append(k.value())?;
            }
        }
    }
    dict.set_item("old_xorbs", old_xorbs)?;

    let missing_files = pyo3::types::PyList::empty(py);
    if let Ok(table) = read_txn.open_table(TXN_MISSING_FILES_TABLE) {
        if let Ok(iter) = table.iter() {
            for item in iter {
                let (k, _) = item.unwrap();
                missing_files.append(k.value())?;
            }
        }
    }
    dict.set_item("missing_files", missing_files)?;

    let missing_post_files = pyo3::types::PyList::empty(py);
    if let Ok(table) = read_txn.open_table(TXN_MISSING_POST_FILES_TABLE) {
        if let Ok(iter) = table.iter() {
            for item in iter {
                let (k, _) = item.unwrap();
                missing_post_files.append(k.value())?;
            }
        }
    }
    dict.set_item("missing_post_files", missing_post_files)?;

    drop(read_txn);
    drop(db);
    let _ = std::fs::remove_file(&txn_path);

    Ok(dict.into())
}


#[cfg(test)]
mod tests {
    use super::*;

    fn pos(xorb: u8, idx: u32) -> [u8; 36] {
        let mut k = [0u8; 36];
        k[0..32].copy_from_slice(&[xorb; 32]);
        k[32..36].copy_from_slice(&idx.to_le_bytes());
        k
    }

    /// Builds a chunk position map from (old xorb, old idx, new xorb, new idx) rows and runs
    /// `remap_segment` for chunks `start..end` of old xorb `old`.
    fn remap(rows: &[(u8, u32, u8, u32)], old: u8, start: u32, end: u32) -> Option<([u8; 32], u32)> {
        let db = redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TXN_CHUNK_MAP_TABLE).unwrap();
            for &(ox, oi, nx, ni) in rows {
                table.insert(&pos(ox, oi), &pos(nx, ni)).unwrap();
            }
        }
        txn.commit().unwrap();
        let read = db.begin_read().unwrap();
        let table = read.open_table(TXN_CHUNK_MAP_TABLE).unwrap();
        remap_segment(&table, &[old; 32], start, end).map(|(h, s)| (<[u8; 32]>::from(h), s))
    }

    #[test]
    fn remaps_a_segment_that_moved_in_one_piece() {
        // Old xorb 1, chunks 3..6 moved to new xorb 9 at positions 10..13.
        let rows = [(1, 3, 9, 10), (1, 4, 9, 11), (1, 5, 9, 12)];
        assert_eq!(remap(&rows, 1, 3, 6), Some(([9; 32], 10)));
    }

    #[test]
    fn refuses_a_segment_split_across_new_xorbs() {
        // Chunk 5 landed at the start of the next new xorb.
        let rows = [(1, 3, 9, 10), (1, 4, 9, 11), (1, 5, 8, 0)];
        assert_eq!(remap(&rows, 1, 3, 6), None);
    }

    #[test]
    fn refuses_a_segment_whose_chunks_are_not_consecutive() {
        let rows = [(1, 3, 9, 10), (1, 4, 9, 11), (1, 5, 9, 20)];
        assert_eq!(remap(&rows, 1, 3, 6), None);
    }

    #[test]
    fn copied_cas_info_gets_unpacked_positions() {
        use mdb_shard::metadata_shard::xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader};
        // A block as an earlier GC wrote it: positions are packed (compressed) byte offsets.
        let h = |b: u8| MerkleHash::from([b; 32]);
        let info = MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(h(9), 3, 130),
            chunks: vec![
                XorbChunkSequenceEntry::new(h(1), 100u32, 0u32),
                XorbChunkSequenceEntry::new(h(2), 200u32, 50u32),
                XorbChunkSequenceEntry::new(h(3), 300u32, 90u32),
            ],
        };
        let fixed = with_unpacked_positions(info);
        let starts: Vec<u32> = fixed.chunks.iter().map(|c| c.chunk_byte_range_start).collect();
        assert_eq!(starts, vec![0, 100, 300]);
        assert_eq!(fixed.metadata.num_bytes_in_xorb, 600);
        // Hashes, sizes and the xorb hash are untouched.
        assert_eq!(fixed.metadata.xorb_hash, h(9));
        assert_eq!(fixed.chunks[2].chunk_hash, h(3));
        assert_eq!(fixed.chunks[2].unpacked_segment_bytes, 300);
        // Already-correct blocks (from uploads) stay the same.
        assert_eq!(with_unpacked_positions(fixed.clone()), fixed);
    }

    #[test]
    fn new_xorb_is_named_by_its_footer_hash() {
        use mdb_shard::xorb_object::{serialize_chunk, CompressionScheme, XorbObject};
        let mut raw = Vec::new();
        serialize_chunk(&[7u8; 5000], &mut raw, CompressionScheme::LZ4).unwrap();
        serialize_chunk(&(0..6000u32).map(|i| (i % 251) as u8).collect::<Vec<_>>(), &mut raw, CompressionScheme::LZ4).unwrap();
        let mut full = Vec::new();
        let (_, name) = reconstruct_xorb_with_footer(&mut full, &raw).unwrap();
        let footer = XorbObject::deserialize(&mut std::io::Cursor::new(&full)).unwrap().info.xorb_hash;
        assert_eq!(name, footer);
        assert!(XorbObject::validate_xorb_object(&mut std::io::Cursor::new(&full), &name).unwrap().is_some());
    }

    #[test]
    fn refuses_a_segment_with_a_chunk_that_was_not_repacked() {
        let rows = [(1, 3, 9, 10), (1, 5, 9, 12)];
        assert_eq!(remap(&rows, 1, 3, 6), None);
    }

    #[test]
    fn uses_positions_of_the_right_old_xorb() {
        // The same chunks were repacked from two old xorbs to different places; each old xorb's
        // segment must map to its own copy.
        let rows = [(1, 0, 9, 0), (1, 1, 9, 1), (2, 0, 9, 50), (2, 1, 9, 51)];
        assert_eq!(remap(&rows, 1, 0, 2), Some(([9; 32], 0)));
        assert_eq!(remap(&rows, 2, 0, 2), Some(([9; 32], 50)));
    }
}
