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
pub const TXN_CHUNK_MAP_TABLE: TableDefinition<&[u8; 32], &[u8; 36]> = TableDefinition::new("txn_chunk_map");
pub const TXN_XORB_LAYOUT_TABLE: TableDefinition<&[u8; 36], &[u8; 32]> = TableDefinition::new("txn_xorb_layout");
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

use mdb_shard::metadata_shard::ShardFileManager;

pub fn _consolidate_metadata(
    sfm: Arc<ShardFileManager>,
    gc_db: Arc<std::sync::RwLock<Option<Database>>>,
) -> PyResult<()> {
    
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
    let mut current_offset: u32 = 0;
    let mut current_xorb_chunk_hashes: Vec<MerkleHash> = Vec::new();
    let mut current_uncompressed_size: u32 = 0;

    // 1. Transaction Lock Database (opened at start to stream incrementally)
    let txn_path = "/tmp/active_transaction.redb";
    let _ = std::fs::remove_file(txn_path); // ensure clean
    let txn_db = Database::create(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB error: {e}")))?;
    let write_txn = txn_db.begin_write().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn error: {e}")))?;
    
    let mut meta_table = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    meta_table.insert("status", "consolidated").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert error: {e}")))?;
    meta_table.insert("timestamp", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs().to_string().as_str()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert error: {e}")))?;
    
    let mut old_xorbs_table = write_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let mut new_xorbs_table = write_txn.open_table(TXN_NEW_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let mut old_shards_table = write_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let mut new_shards_table = write_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let mut chunk_map_table = write_txn.open_table(TXN_CHUNK_MAP_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
    let mut layout_table = write_txn.open_table(TXN_XORB_LAYOUT_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;

    let sparse_xorbs = read_txn.open_table(GC_SPARSE_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;

    // A closure to flush the current buffer to S3
    let flush_xorb_buffer = |chunks: &mut Vec<u8>, entries: &mut Vec<XorbChunkSequenceEntry>, hashes: &mut Vec<MerkleHash>, offset: &mut u32, unp_size: &mut u32, map_table: &mut redb::Table<&[u8; 32], &[u8; 36]>, xorbs_table: &mut redb::Table<&str, &[u8]>| -> PyResult<()> {
        if chunks.is_empty() { return Ok(()); }
        
        let mut xorb_new_full = Vec::new();
        reconstruct_xorb_with_footer(&mut xorb_new_full, chunks)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Footer error: {e:?}")))?;
            
        let new_xorb_hash = compute_data_hash(&xorb_new_full);
        let new_xorb_hash_str = new_xorb_hash.hex();
        
        for (i, h) in hashes.iter().enumerate() {
            let mut val = [0u8; 36];
            val[0..32].copy_from_slice(new_xorb_hash.as_bytes());
            val[32..36].copy_from_slice(&(i as u32).to_le_bytes());
            let h_bytes: [u8; 32] = (*h).into();
            map_table.insert(&h_bytes, &val).unwrap();
        }

        let xorb_info = MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(new_xorb_hash, entries.len() as u32, *offset),
            chunks: entries.clone(),
        };
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
        hashes.clear();
        *offset = 0;
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
                    let footer_bytes_result = s3_download_range(&client, &bucket, &key, "bytes=-1048576", MAX_S3_ATTEMPTS, true).await;
                    (hash_bytes, footer_bytes_result)
                }
            });
            iter(futures_iter).buffer_unordered(batch_size).collect::<Vec<_>>().await
        });
        
        // 2b. Identify Live Chunks and coalesce ranges
        struct RangeReq {
            start: usize,
            end: usize,
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
                        layout_table.insert(&layout_key, &h_bytes).unwrap();
                        
                        let is_live = live_chunks.get(&h_bytes).unwrap().is_some();
                        if is_live {
                            let start = if i == 0 { 0 } else { boundaries[i - 1] as usize };
                            let end = boundaries[i] as usize;
                            let len = end - start;
                            let unp_len = if i == 0 { unpacked[0] } else { unpacked[i] - unpacked[i - 1] };
                            
                            if let Some(ref mut req) = current_range {
                                if req.end == start { // contiguous
                                    req.end = end;
                                    req.hashes.push(h);
                                    req.unp_lens.push(unp_len);
                                    req.chunk_lens.push(len);
                                } else {
                                    ranges.push(current_range.take().unwrap());
                                    current_range = Some(RangeReq { start, end, hashes: vec![h], unp_lens: vec![unp_len], chunk_lens: vec![len] });
                                }
                            } else {
                                current_range = Some(RangeReq { start, end, hashes: vec![h], unp_lens: vec![unp_len], chunk_lens: vec![len] });
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
        for (req, bytes_result) in fetched_ranges {
            let bytes_opt = bytes_result.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
            if let Some(bytes) = bytes_opt {
                let mut offset = 0;
                for j in 0..req.hashes.len() {
                    let h = req.hashes[j];
                    let unp_len = req.unp_lens[j];
                    let len = req.chunk_lens[j];
                    
                    if offset + len <= bytes.len() {
                        xorb_new_chunks.extend_from_slice(&bytes[offset..offset+len]);
                        current_xorb_chunk_hashes.push(h);
                        new_entries.push(XorbChunkSequenceEntry::new(h, unp_len, current_offset));
                        current_offset += len as u32;
                        current_uncompressed_size += unp_len;
                        offset += len;
                        
                        if current_uncompressed_size >= max_xorb_size {
                            flush_xorb_buffer(&mut xorb_new_chunks, &mut new_entries, &mut current_xorb_chunk_hashes, &mut current_offset, &mut current_uncompressed_size, &mut chunk_map_table, &mut new_xorbs_table)?;
                        }
                    }
                }
            }
        }
    }
    
    // Flush any remaining partial XORB
    flush_xorb_buffer(&mut xorb_new_chunks, &mut new_entries, &mut current_xorb_chunk_hashes, &mut current_offset, &mut current_uncompressed_size, &mut chunk_map_table, &mut new_xorbs_table)?;

    // 3. Process Old Shards Incrementally and Stream into New Shards
    let mut current_shard = MDBInMemoryShard::default();
    let mut added_xorbs: HashSet<MerkleHash> = HashSet::new();
    let max_shard_size: u64 = 64 * 1024 * 1024; // 64 MB target
    let mut unwritten_files = 0;

    let flush_shard = |shard_mem: &mut MDBInMemoryShard, new_shards_table: &mut redb::Table<&str, ()>, added_xorbs: &mut HashSet<MerkleHash>| -> PyResult<()> {
        let shard_bytes = shard_mem.to_bytes()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialize error: {e:?}")))?;
        
        let new_shard_hash = compute_data_hash(&shard_bytes);
        let new_shard_hash_str = new_shard_hash.hex();
        
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
                        existing_xorb_blocks.insert(x_hash, MDBXorbInfo::from(xorb_view));
                    }
                }
            }

            for i in 0..minimal_shard.num_files() {
                if let Some(file_view) = minimal_shard.file(i) {
                    let fh_bytes: [u8; 32] = file_view.file_hash().into();
                    if live_files.get(&fh_bytes).unwrap().is_some() {
                        let mut file_info = MDBFileInfo::from(file_view);
                        let mut valid = true;
                        
                        for seg in &mut file_info.segments {
                            let seg_hash_bytes: [u8; 32] = seg.xorb_hash.into();
                            let is_sparse = sparse_xorbs.get(&seg_hash_bytes).unwrap().is_some();
                            if is_sparse {
                                let mut layout_key = [0u8; 36];
                                layout_key[0..32].copy_from_slice(&seg_hash_bytes);
                                layout_key[32..36].copy_from_slice(&seg.chunk_index_start.to_le_bytes());
                                
                                if let Some(chunk_hash_val) = layout_table.get(&layout_key).unwrap() {
                                    let first_chunk_bytes: [u8; 32] = *chunk_hash_val.value();
                                    
                                    if let Some(chunk_val) = chunk_map_table.get(&first_chunk_bytes).unwrap() {
                                        let val = chunk_val.value();
                                        let new_xorb_hash = MerkleHash::from(<[u8; 32]>::try_from(&val[0..32]).unwrap());
                                        let new_start_idx = u32::from_le_bytes(val[32..36].try_into().unwrap());
                                        
                                        seg.xorb_hash = new_xorb_hash;
                                        let length = seg.chunk_index_end - seg.chunk_index_start;
                                        seg.chunk_index_start = new_start_idx;
                                        seg.chunk_index_end = new_start_idx + length;
                                    } else {
                                        valid = false;
                                        break;
                                    }
                                } else {
                                    valid = false;
                                    break;
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
                                    if let Some(xorb_val) = new_xorbs_table.get(x_str.as_str()).unwrap() {
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
                                                let packed_len = u32::from_le_bytes(bytes[base + 32..base + 36].try_into().unwrap());
                                                let unp_len = u32::from_le_bytes(bytes[base + 36..base + 40].try_into().unwrap());
                                                entries.push(XorbChunkSequenceEntry::new(MerkleHash::from(ch_bytes), unp_len, offset_acc));
                                                offset_acc = offset_acc.saturating_add(packed_len);
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
                                flush_shard(&mut current_shard, &mut new_shards_table, &mut added_xorbs)?;
                                unwritten_files = 0;
                            }
                        }
                    }
                }
            }
        }
    }

    if unwritten_files > 0 || current_shard.num_xorb_entries() > 0 {
        flush_shard(&mut current_shard, &mut new_shards_table, &mut added_xorbs)?;
    }
    
    // 4. Update the active transaction metadata status to "consolidated"Lock
    drop(old_xorbs_table);
    drop(new_xorbs_table);
    drop(old_shards_table);
    drop(new_shards_table);
    drop(chunk_map_table);
    drop(meta_table);
    drop(layout_table);
    
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
        if status_val != "consolidated" && status_val != "verified" && status_val != "verification_failed" && status_val != "failed" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot stage: status must be consolidated, verified, verification_failed, or failed"));
        }
        
        let new_xorbs_table = write_txn.open_table(TXN_NEW_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let new_shards_table = write_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let old_shards_table = write_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let old_xorbs_table = write_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        
        rt.block_on(async {
            // Copy XORBs
            for item in new_xorbs_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash = hash_key.value();
                let src = format!("{}/gc_consolidated/xorbs/{}", bucket, hash);
                let dest = format!("xorbs/default/{}", hash);
                s3_copy_object(&client, &bucket, &src, &dest, MAX_S3_ATTEMPTS, false).await
                    .map_err(|e| format!("Copy failed for {}: {:?}", hash, e))?;
            }
            // Copy Shards
            for item in new_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash = hash_key.value();
                let src = format!("{}/gc_consolidated/shards/{}.mdb", bucket, hash);
                let dest = format!("shards/{}.mdb", hash);
                s3_copy_object(&client, &bucket, &src, &dest, MAX_S3_ATTEMPTS, false).await
                    .map_err(|e| format!("Copy failed for {}: {:?}", hash, e))?;
                
                // Delete any existing tombstone for this new shard (resurrection)
                let tombstone_key = format!("shards/tombstones/{}.revoked", hash);
                let _ = s3_delete_object(&client, &bucket, &tombstone_key, MAX_S3_ATTEMPTS).await;
            }
            
            // Tombstone and move old shards
            for item in old_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                
                // Move old shard to bin
                let src = format!("{}/shards/{}.mdb", bucket, hash_str);
                let dest = format!("bin/shards/{}.mdb", hash_str);
                s3_copy_object(&client, &bucket, &src, &dest, MAX_S3_ATTEMPTS, true).await
                    .map_err(|e| format!("Copy failed for {}: {:?}", hash_str, e))?;
                let key_to_del = format!("shards/{}.mdb", hash_str);
                let _ = s3_delete_object(&client, &bucket, &key_to_del, MAX_S3_ATTEMPTS).await;

                // Create tombstone
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                s3_put_object(&client, &bucket, &key, Vec::new(), MAX_S3_ATTEMPTS).await
                    .map_err(|e| format!("Failed to put tombstone {}: {:?}", key, e))?;
            }
            
            // Move old XORBs
            for item in old_xorbs_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                
                let src = format!("{}/xorbs/default/{}", bucket, hash_str);
                let dest = format!("bin/xorbs/{}", hash_str);
                s3_copy_object(&client, &bucket, &src, &dest, MAX_S3_ATTEMPTS, true).await
                    .map_err(|e| format!("Copy failed for {}: {:?}", hash_str, e))?;
                let key_to_del = format!("xorbs/default/{}", hash_str);
                let _ = s3_delete_object(&client, &bucket, &key_to_del, MAX_S3_ATTEMPTS).await;
            }
            Ok::<_, String>(())
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
        
        meta.insert("status", "staged").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
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
    let verify_sfm = sfm.clone();
    eprintln!("[GC Verify PRE] Phase 2: Scanning live files from GC_LIVE_FILES_TABLE... (Current RSS: {} MB)", get_current_rss_mb());
    let mut missing_count = 0;
    let mut files_scanned = 0;
    
    let mut missing_table = write_txn.open_table(TXN_MISSING_FILES_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    let mut unique_xorbs_table = write_txn.open_table(TXN_UNIQUE_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    
    let gc_db_lock = gc_db.read().unwrap();
    if let Some(ref gcdb) = *gc_db_lock {
        let read_txn = gcdb.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn error: {e}")))?;
        if let Ok(files_table) = read_txn.open_table(GC_LIVE_FILES_TABLE) {
            for item in files_table.iter().unwrap() {
                files_scanned += 1;
                if files_scanned % 10000 == 0 {
                    eprintln!("[GC Verify PRE] Phase 2: Scanned {} files (RSS: {} MB)", files_scanned, get_current_rss_mb());
                }
                
                let (file_hash_bytes, _) = item.unwrap();
                let mut h = [0u8; 32];
                h.copy_from_slice(file_hash_bytes.value());
                let file_hash_hex = MerkleHash::from(h).hex();
                
                let mut xorb_deps = Vec::new();
                let mut found = true;
                
                let res = rt.block_on(async { verify_sfm.get_file_reconstruction_info(&MerkleHash::from_hex(&file_hash_hex).unwrap()).await });
                
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

    let mut xorbs_scanned = 0;
    for item in unique_xorbs_table.iter().unwrap() {
        let (k, _) = item.unwrap();
        batch.push(k.value().to_string());
        if batch.len() >= 100 {
            process_batch(&mut batch)?;
            xorbs_scanned += 100;
            if xorbs_scanned % 1000 == 0 {
                eprintln!("[GC Verify PRE] Phase 3: Verified {} XORBs (RSS: {} MB)", xorbs_scanned, get_current_rss_mb());
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
        verify_sfm.register_shards_by_path(&paths_to_register).await
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Register err: {e:?}")))?;

    eprintln!("[GC Verify POST] Phase 1: Downloading and registering {} new shards", new_shards.len());
    for ns in &new_shards {
        let key = format!("gc_consolidated/shards/{}.mdb", ns);
        let data = rt.block_on(async { s3_download_object(client, bucket, &key, MAX_S3_ATTEMPTS, true).await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Missing new shard: {}", key)))?;
        
        let path = temp_dir_path.join(format!("{}.mdb", ns));
        std::fs::write(&path, &data).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write err: {e}")))?;
        rt.block_on(async { verify_sfm.register_shards_by_path(&[path]).await })
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
            for item in files_table.iter().unwrap() {
                files_scanned += 1;
                if files_scanned % 10000 == 0 {
                    eprintln!("[GC Verify POST] Phase 2: Scanned {} files", files_scanned);
                }
                
                let (file_hash_bytes, _) = item.unwrap();
                let mut h = [0u8; 32];
                h.copy_from_slice(file_hash_bytes.value());
                let file_hash_hex = MerkleHash::from(h).hex();
                
                let res = rt.block_on(async { verify_sfm.get_file_reconstruction_info(&MerkleHash::from_hex(&file_hash_hex).unwrap()).await });
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

    let mut xorbs_scanned = 0;
    for item in post_xorbs_table.iter().unwrap() {
        let (k, _) = item.unwrap();
        batch.push(k.value().to_string());
        if batch.len() >= 100 {
            process_batch(&mut batch)?;
            xorbs_scanned += 100;
            if xorbs_scanned % 1000 == 0 {
                eprintln!("[GC Verify POST] Phase 3: Verified {} XORBs (RSS: {} MB)", xorbs_scanned, get_current_rss_mb());
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

    eprintln!("[GC Verify] Phase 5: Committing verification results (missing files post-gc: {})", missing_post_gc);
    if missing_post_gc == 0 {
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
    Ok(missing_post_gc)
}

pub fn _commit_gc_transaction() -> PyResult<()> {
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
        let status_val = status.map(|s| s.value().to_string()).unwrap_or_default();
        if status_val != "staged" && status_val != "verified" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot commit: status is not staged or verified"));
        }
        
        // Tombstones for old_shards are already created during stage_gc, no need to create them here
        
        meta.insert("status", "committed").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    }
    
    write_txn.commit().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit err: {e}")))?;
    drop(db);
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        s3_upload_file_reqwest_http1(&client, &bucket, key, txn_path, MAX_S3_ATTEMPTS).await
            .map_err(|e| format!("Failed to upload committed lock: {e}"))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let _ = std::fs::remove_file(txn_path);
    Ok(())
}

pub fn _revert_gc_transaction() -> PyResult<()> {
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
        let status_val = status.map(|s| s.value().to_string()).unwrap_or_default();
        if status_val == "consolidated" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot revert: transaction has not been staged yet. Nothing to revert."));
        } else if status_val != "staged" && status_val != "verified" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot revert: status is not staged or verified"));
        }
        
        let new_shards_table = write_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let old_shards_table = write_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let new_xorbs_table = write_txn.open_table(TXN_NEW_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let old_xorbs_table = write_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        
        rt.block_on(async {
            for item in new_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                
                let active_key = format!("shards/{}.mdb", hash_str);
                let _ = s3_delete_object(&client, &bucket, &active_key, MAX_S3_ATTEMPTS).await;
                
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                s3_put_object(&client, &bucket, &key, Vec::new(), MAX_S3_ATTEMPTS).await
                    .map_err(|e| format!("Failed to put tombstone {}: {:?}", key, e))?;
            }
            
            for item in new_xorbs_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                let active_key = format!("xorbs/default/{}", hash_str);
                let _ = s3_delete_object(&client, &bucket, &active_key, MAX_S3_ATTEMPTS).await;
            }
            
            // Delete tombstones for old shards (resurrection) and move back from bin
            for item in old_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                let _ = s3_delete_object(&client, &bucket, &key, MAX_S3_ATTEMPTS).await;
                
                let src = format!("{}/bin/shards/{}.mdb", bucket, hash_str);
                let dest = format!("shards/{}.mdb", hash_str);
                if let Ok(_) = s3_copy_object(&client, &bucket, &src, &dest, MAX_S3_ATTEMPTS, true).await {
                    let bin_key = format!("bin/shards/{}.mdb", hash_str);
                    let _ = s3_delete_object(&client, &bucket, &bin_key, MAX_S3_ATTEMPTS).await;
                }
            }

            for item in old_xorbs_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                
                let src = format!("{}/bin/xorbs/{}", bucket, hash_str);
                let dest = format!("xorbs/default/{}", hash_str);
                if let Ok(_) = s3_copy_object(&client, &bucket, &src, &dest, MAX_S3_ATTEMPTS, true).await {
                    let bin_key = format!("bin/xorbs/{}", hash_str);
                    let _ = s3_delete_object(&client, &bucket, &bin_key, MAX_S3_ATTEMPTS).await;
                }
            }
            Ok::<_, String>(())
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
        
        meta.insert("status", "reverted").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    }
    
    write_txn.commit().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit err: {e}")))?;
    drop(db);
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        s3_upload_file_reqwest_http1(&client, &bucket, key, txn_path, MAX_S3_ATTEMPTS).await
            .map_err(|e| format!("Failed to upload reverted lock: {e}"))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let _ = std::fs::remove_file(txn_path);
    Ok(())
}

pub fn _prune_garbage() -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = create_s3_client()?;
    
    rt.block_on(async {
        // Delete everything in bin/ and gc_consolidated/ immediately
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
        
        // Delete tombstones older than 7 days
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
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
        
        // Delete lock file on s3 and local path if it is not staged
        let txn_path = "/tmp/active_transaction.redb";
        let lock_key = "gc/active_transaction.redb";
        
        let mut should_delete = false;
        if let Some(data) = s3_download_object(&client, &bucket, lock_key, MAX_S3_ATTEMPTS, false).await? {
            if let Ok(_) = std::fs::write(txn_path, &data) {
                if let Ok(db) = Database::open(txn_path) {
                    if let Ok(read_txn) = db.begin_read() {
                        if let Ok(meta_table) = read_txn.open_table(TXN_META_TABLE) {
                            if let Ok(Some(status)) = meta_table.get("status") {
                                let status_val = status.value().to_string();
                                if status_val != "staged" {
                                    should_delete = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if should_delete {
            let _ = s3_delete_object(&client, &bucket, lock_key, MAX_S3_ATTEMPTS).await;
            let _ = std::fs::remove_file(txn_path);
        }

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

