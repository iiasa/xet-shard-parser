use pyo3::prelude::*;
use std::collections::{HashSet, HashMap};
use std::io::Cursor;
use reqwest::Client as ReqwestClient;
use tokio::runtime::Runtime;
use std::sync::Arc;
use futures::future::join_all;
use aws_sdk_s3::Client;
use aws_config::Region;
use aws_sdk_s3::config::Credentials;

use mdb_shard::merklehash::{MerkleHash, compute_data_hash};
use mdb_shard::metadata_shard::shard_in_memory::MDBInMemoryShard;
use mdb_shard::metadata_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::metadata_shard::file_structs::MDBFileInfo;
use mdb_shard::metadata_shard::xorb_structs::{MDBXorbInfo, XorbChunkSequenceHeader, XorbChunkSequenceEntry};
use mdb_shard::xorb_object::{reconstruct_xorb_with_footer, XorbObject};
use redb::{Database, ReadableTable, ReadableDatabase, TableDefinition};

pub const GC_LIVE_FILES_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_files");
pub const GC_LIVE_CHUNKS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_chunks");
pub const GC_SPARSE_XORBS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_sparse_xorbs");

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
pub const TXN_UNIQUE_XORBS_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_unique_xorbs");
pub const TXN_MISSING_XORBS_TABLE: TableDefinition<&str, ()> = TableDefinition::new("txn_missing_xorbs");
fn parse_xorb_footer_data(bytes: &[u8]) -> Option<(Vec<MerkleHash>, Vec<u32>, Vec<u32>)> {
    let mut reader = Cursor::new(bytes);
    let xorb_obj = XorbObject::deserialize(&mut reader).ok()?;
    let info = xorb_obj.info;
    Some((info.chunk_hashes, info.chunk_boundary_offsets, info.unpacked_chunk_offsets))
}

use mdb_shard::metadata_shard::ShardFileManager;

pub fn _consolidate_metadata(
    _py: Python<'_>,
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

    let rt = Runtime::new().unwrap();

    // 0. S3 Network Setup
    let (client, bucket) = _setup_s3_client()?;

    // Bin-Packing State
    let max_xorb_size = 64 * 1024 * 1024; // 64 MB target
    // Buffer State

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
    let mut flush_xorb_buffer = |chunks: &mut Vec<u8>, entries: &mut Vec<XorbChunkSequenceEntry>, hashes: &mut Vec<MerkleHash>, offset: &mut u32, unp_size: &mut u32, map_table: &mut redb::Table<&[u8; 32], &[u8; 36]>, xorbs_table: &mut redb::Table<&str, &[u8]>| -> PyResult<()> {
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
            client.put_object()
                .bucket(&bucket)
                .key(&key)
                .body(aws_sdk_s3::primitives::ByteStream::from(xorb_new_full))
                .send().await
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

    // 2. Process Sparse XORBs Incrementally
    for item in sparse_xorbs.iter().unwrap() {
        let (k, _) = item.unwrap();
        let hash_bytes = k.value();
        let xorb_hash = MerkleHash::from(*hash_bytes);
        let xorb_hash_str = xorb_hash.hex();
        
        old_xorbs_table.insert(xorb_hash_str.as_str(), ()).unwrap();

        let xorb_bytes_opt = rt.block_on(async {
            let key = format!("xorbs/default/{}", xorb_hash_str);
            download_with_retry(&client, &bucket, &key, 5).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;

        let xorb_bytes = match xorb_bytes_opt {
            Some(b) => b,
            None => continue,
        };

        if let Some((hashes, boundaries, unpacked)) = parse_xorb_footer_data(&xorb_bytes) {
            for i in 0..hashes.len() {
                let h = hashes[i];
                let h_bytes: [u8; 32] = h.into();
                
                let mut layout_key = [0u8; 36];
                layout_key[0..32].copy_from_slice(hash_bytes);
                layout_key[32..36].copy_from_slice(&(i as u32).to_le_bytes());
                layout_table.insert(&layout_key, &h_bytes).unwrap();
                
                let is_live = live_chunks.get(&h_bytes).unwrap().is_some();
                if is_live {
                    let start = if i == 0 { 0 } else { boundaries[i - 1] as usize };
                    let end = boundaries[i] as usize;
                    let len = (end - start) as u32;
                    let unp_len = if i == 0 { unpacked[0] } else { unpacked[i] - unpacked[i - 1] };
                    
                    xorb_new_chunks.extend_from_slice(&xorb_bytes[start..end]);
                    current_xorb_chunk_hashes.push(h);
                    new_entries.push(XorbChunkSequenceEntry::new(h, unp_len, current_offset));
                    current_offset += len;
                    current_uncompressed_size += unp_len;
                    
                    if current_uncompressed_size >= max_xorb_size {
                        flush_xorb_buffer(&mut xorb_new_chunks, &mut new_entries, &mut current_xorb_chunk_hashes, &mut current_offset, &mut current_uncompressed_size, &mut chunk_map_table, &mut new_xorbs_table)?;
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

    let mut flush_shard = |shard_mem: &mut MDBInMemoryShard, new_shards_table: &mut redb::Table<&str, ()>, added_xorbs: &mut HashSet<MerkleHash>| -> PyResult<()> {
        let shard_bytes = shard_mem.to_bytes()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialize error: {e:?}")))?;
        
        let new_shard_hash = compute_data_hash(&shard_bytes);
        let new_shard_hash_str = new_shard_hash.hex();
        
        rt.block_on(async {
            let key = format!("gc_consolidated/shards/{}.mdb", new_shard_hash_str);
            client.put_object()
                .bucket(&bucket)
                .key(&key)
                .body(aws_sdk_s3::primitives::ByteStream::from(shard_bytes))
                .send().await
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
            
            if !has_sparse {
                continue;
            }
            old_shards_table.insert(shard_hash_str.as_str(), ()).unwrap();
            
            // Note: we don't need to rebuild old_xorb_layouts from the shard because we already 
            // populated layout_table from the S3 fetch.
            
            for i in 0..minimal_shard.num_xorb() {
                if let Some(xorb_view) = minimal_shard.xorb(i) {
                    let x_hash = xorb_view.xorb_hash();
                    let x_bytes: [u8; 32] = x_hash.into();
                    if sparse_xorbs.get(&x_bytes).unwrap().is_some() {
                        let x_hash_str = x_hash.hex();
                        old_xorbs_table.insert(x_hash_str.as_str(), ()).unwrap();
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
                                }
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
        let body = aws_sdk_s3::primitives::ByteStream::from_path(std::path::Path::new(txn_path)).await
            .map_err(|e| format!("Failed to read lock file: {:?}", e))?;
        client.put_object()
            .bucket(&bucket)
            .key(key)
            .body(body)
            .send().await
            .map_err(|e| format!("Failed to put active_transaction.redb: {:?}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let _ = std::fs::remove_file(txn_path);

    Ok(())
}

fn _setup_s3_client() -> PyResult<(Client, String)> {
    let endpoint = std::env::var("XET_CAS_S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let access_key = std::env::var("XET_CAS_S3_API_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = std::env::var("XET_CAS_S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let region = std::env::var("XET_CAS_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let bucket = std::env::var("XET_CAS_S3_BUCKET_NAME").unwrap_or_else(|_| "xet-cas".to_string());

    let credentials = Credentials::new(access_key, secret_key, None, None, "xet_janitor");
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .credentials_provider(credentials)
        .region(aws_config::Region::new(region))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .build();

    Ok((Client::from_conf(config), bucket))
}

async fn download_with_retry(client: &Client, bucket: &str, key: &str, max_attempts: u32) -> Result<Option<Vec<u8>>, String> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match client.get_object().bucket(bucket).key(key).send().await {
            Ok(resp) => {
                match resp.body.collect().await {
                    Ok(data) => return Ok(Some(data.into_bytes().to_vec())),
                    Err(e) => {
                        if attempts >= max_attempts {
                            return Err(format!("Failed to collect {}: {:?}", key, e));
                        }
                        tokio::time::sleep(std::time::Duration::from_millis((500 * attempts) as u64)).await;
                    }
                }
            }
            Err(e) => {
                let err_str = format!("{:?}", e);
                if err_str.contains("NoSuchKey") || err_str.contains("NotFound") {
                    return Ok(None);
                }
                if attempts >= max_attempts {
                    return Err(format!("Failed to get {}: {}", key, err_str));
                }
                tokio::time::sleep(std::time::Duration::from_millis((500 * attempts) as u64)).await;
            }
        }
    }
}

pub fn _stage_gc_transaction(_py: Python<'_>) -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let data = download_with_retry(&client, &bucket, key, 5).await?
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
                client.copy_object().copy_source(&src).bucket(&bucket).key(&dest).send().await
                    .map_err(|e| format!("Copy failed for {}: {:?}", hash, e))?;
            }
            // Copy Shards
            for item in new_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash = hash_key.value();
                let src = format!("{}/gc_consolidated/shards/{}.mdb", bucket, hash);
                let dest = format!("shards/{}.mdb", hash);
                client.copy_object().copy_source(&src).bucket(&bucket).key(&dest).send().await
                    .map_err(|e| format!("Copy failed for {}: {:?}", hash, e))?;
                
                // Delete any existing tombstone for this new shard (resurrection)
                let tombstone_key = format!("shards/tombstones/{}.revoked", hash);
                let _ = client.delete_object().bucket(&bucket).key(&tombstone_key).send().await;
            }
            
            // Tombstone and move old shards
            for item in old_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                
                // Move old shard to bin
                let src = format!("{}/shards/{}.mdb", bucket, hash_str);
                let dest = format!("bin/shards/{}.mdb", hash_str);
                if let Err(e) = client.copy_object().copy_source(&src).bucket(&bucket).key(&dest).send().await {
                    // Ignore 404s if it was already moved
                    if !format!("{:?}", e).contains("NotFound") && !format!("{:?}", e).contains("NoSuchKey") {
                        return Err(format!("Copy failed for {}: {:?}", hash_str, e));
                    }
                } else {
                    let key_to_del = format!("shards/{}.mdb", hash_str);
                    let _ = client.delete_object().bucket(&bucket).key(&key_to_del).send().await;
                }

                // Create tombstone
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                client.put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(aws_sdk_s3::primitives::ByteStream::from(Vec::new()))
                    .send().await
                    .map_err(|e| format!("Failed to put tombstone {}: {:?}", key, e))?;
            }
            
            // Move old XORBs
            for item in old_xorbs_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                
                let src = format!("{}/xorbs/default/{}", bucket, hash_str);
                let dest = format!("bin/xorbs/{}", hash_str);
                if let Err(e) = client.copy_object().copy_source(&src).bucket(&bucket).key(&dest).send().await {
                    if !format!("{:?}", e).contains("NotFound") && !format!("{:?}", e).contains("NoSuchKey") {
                        return Err(format!("Copy failed for {}: {:?}", hash_str, e));
                    }
                } else {
                    let key_to_del = format!("xorbs/default/{}", hash_str);
                    let _ = client.delete_object().bucket(&bucket).key(&key_to_del).send().await;
                }
            }
            Ok::<_, String>(())
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
        
        meta.insert("status", "staged").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    }
    
    write_txn.commit().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit err: {e}")))?;
    drop(db);
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let body = aws_sdk_s3::primitives::ByteStream::from_path(std::path::Path::new(txn_path)).await.unwrap();
        client.put_object().bucket(&bucket).key(key).body(body).send().await.unwrap();
        Ok::<_, String>(())
    }).unwrap();
    
    let _ = std::fs::remove_file(txn_path);
    Ok(())
}

pub fn _verify_gc_transaction(
    _py: Python<'_>,
    sfm: Arc<ShardFileManager>,
    gc_db: Arc<std::sync::RwLock<Option<Database>>>,
) -> PyResult<usize> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let data = download_with_retry(&client, &bucket, key, 5).await?
            .ok_or_else(|| format!("Lock file {} not found", key))?;
        std::fs::write(txn_path, data).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let db = Database::open(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB err: {e}")))?;
    let write_txn = db.begin_write().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn err: {e}")))?;
    
    {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let status_str = {
            let status = meta.get("status").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
            status.unwrap().value().to_string()
        };
        
        if status_str != "staged" && status_str != "consolidated" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot verify: status is not staged or consolidated"));
        }
    }
    
    // Extract new shards into memory
    let mut new_shards = Vec::new();
    
    if let Ok(shards_table) = write_txn.open_table(TXN_NEW_SHARDS_TABLE) {
        for item in shards_table.iter().unwrap() {
            let (hash_key, _) = item.unwrap();
            new_shards.push(hash_key.value().to_string());
        }
    }
    if let Ok(old_shards_table) = write_txn.open_table(TXN_OLD_SHARDS_TABLE) {
        for item in old_shards_table.iter().unwrap() {
            let (hash_key, _) = item.unwrap();
            let hash_hex = hash_key.value().to_string();
            
            // Prune old shards from sfm so it is forced to use surviving shards
            let shard_path = sfm.shard_directory().join(format!("{}.mdb", hash_hex));
            let _ = std::fs::remove_file(shard_path);
        }
    }
    
    // 1. Download staging shards, cryptographically verify, and save to SFM disk
    let mut validation_err = None;
    rt.block_on(async {
        for shard_hash in &new_shards {
            let key = format!("gc_consolidated/shards/{}.mdb", shard_hash);
            match download_with_retry(&client, &bucket, &key, 5).await {
                Ok(Some(bytes)) => {
                    // Hybrid Check: Strictly verify the Merkle/CRC bytes
                    if let Err(e) = MDBMinimalShard::from_reader(&mut std::io::Cursor::new(&bytes), true, true) {
                        validation_err = Some(format!("Cryptographic validation failed for {}: {:?}", shard_hash, e));
                        break;
                    }
                    
                    // Save pristine shard directly to the local sfm cache disk
                    let shard_path = sfm.shard_directory().join(format!("{}.mdb", shard_hash));
                    if let Err(e) = std::fs::write(&shard_path, &bytes) {
                        validation_err = Some(format!("Failed to write verified shard {} to disk: {:?}", shard_hash, e));
                        break;
                    }
                }
                Ok(None) => {
                    validation_err = Some(format!("Failed to download body for {}", shard_hash));
                    break;
                }
                Err(e) => {
                    validation_err = Some(format!("Failed to fetch staged shard {}: {:?}", shard_hash, e));
                    break;
                }
            }
        }
        
        if validation_err.is_none() {
            // Load the newly saved, verified shards into the sfm index natively, alongside surviving shards
            let _ = sfm.refresh_shard_dir(false, 0).await;
        }
    });

    if let Some(err_msg) = validation_err {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(err_msg));
    }

    // Create a fresh isolated ShardFileManager instance from disk to guarantee zero in-memory stale handles
    let verify_sfm = rt.block_on(async {
        let ctx = xet_runtime::core::context::XetContext::default()
            .map_err(|e| format!("Failed to create context: {:?}", e))?;
        ShardFileManager::new_in_cache_directory(&ctx, sfm.shard_directory()).await
            .map_err(|e| format!("Failed to create verification ShardFileManager: {:?}", e))
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;

    // 2. Gather unique XORB dependencies for all live files (Pass 1)
    let mut missing_count = 0;
    
    let mut missing_table = write_txn.open_table(TXN_MISSING_FILES_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    let mut unique_xorbs_table = write_txn.open_table(TXN_UNIQUE_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    let old_xorbs_table = write_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    
    let gc_db_lock = gc_db.read().unwrap();
    if let Some(ref gcdb) = *gc_db_lock {
        let read_txn = gcdb.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn error: {e}")))?;
        if let Ok(files_table) = read_txn.open_table(GC_LIVE_FILES_TABLE) {
            for item in files_table.iter().unwrap() {
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
                
                // Validate XORBs
                for xh in &xorb_deps {
                    if old_xorbs_table.get(xh.as_str()).unwrap().is_some() {
                        // Dangling pointer to deleted tombstone!
                        missing_table.insert(file_hash_hex.as_str(), ()).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
                        missing_count += 1;
                        break;
                    }
                    unique_xorbs_table.insert(xh.as_str(), ()).unwrap();
                }
            }
        }
    }
    
    // 3. Concurrently verify all required XORBs actually exist physically
    use futures::stream::{StreamExt, iter};
    let mut missing_xorbs_table = write_txn.open_table(TXN_MISSING_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    
    let mut batch = Vec::with_capacity(100);
    let mut process_batch = |b: &mut Vec<String>| {
        let results = rt.block_on(async {
            let futures_iter = b.iter().map(|xorb_hash_hex| {
                let client = client.clone();
                let bucket = bucket.clone();
                let xh = xorb_hash_hex.clone();
                async move {
                    let key_staged = format!("gc_consolidated/xorbs/{}", xh);
                    let key_live = format!("xorbs/default/{}", xh);
                    
                    if client.head_object().bucket(&bucket).key(&key_staged).send().await.is_ok() {
                        return (xh, true);
                    }
                    if client.head_object().bucket(&bucket).key(&key_live).send().await.is_ok() {
                        return (xh, true);
                    }
                    (xh, false)
                }
            });
            
            iter(futures_iter)
                .buffer_unordered(100)
                .collect::<Vec<(String, bool)>>()
                .await
        });
        
        for (xh, exists) in results {
            if !exists {
                missing_xorbs_table.insert(xh.as_str(), ()).unwrap();
            }
        }
        b.clear();
    };

    for item in unique_xorbs_table.iter().unwrap() {
        let (k, _) = item.unwrap();
        batch.push(k.value().to_string());
        if batch.len() >= 100 {
            process_batch(&mut batch);
        }
    }
    if !batch.is_empty() {
        process_batch(&mut batch);
    }
    
    // 4. Second pass to stream files and check for missing XORB intersections
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

    if missing_count == 0 {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        meta.insert("status", "verified").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    } else {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        meta.insert("status", "failed").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    }
    
    drop(missing_table);
    drop(unique_xorbs_table);
    drop(old_xorbs_table);
    drop(missing_xorbs_table);

    write_txn.commit().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit err: {e}")))?;
    drop(db);
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let body = aws_sdk_s3::primitives::ByteStream::from_path(std::path::Path::new(txn_path)).await.unwrap();
        client.put_object().bucket(&bucket).key(key).body(body).send().await.unwrap();
        Ok::<_, String>(())
    }).unwrap();
    
    let _ = std::fs::remove_file(txn_path);
    Ok(missing_count)
}

pub fn _commit_gc_transaction(_py: Python<'_>) -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let data = download_with_retry(&client, &bucket, key, 5).await?
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
        let body = aws_sdk_s3::primitives::ByteStream::from_path(std::path::Path::new(txn_path)).await.unwrap();
        client.put_object().bucket(&bucket).key(key).body(body).send().await.unwrap();
        Ok::<_, String>(())
    }).unwrap();
    
    let _ = std::fs::remove_file(txn_path);
    Ok(())
}

pub fn _revert_gc_transaction(_py: Python<'_>) -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let data = download_with_retry(&client, &bucket, key, 5).await?
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
                let _ = client.delete_object().bucket(&bucket).key(&active_key).send().await;
                
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                client.put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(aws_sdk_s3::primitives::ByteStream::from(Vec::new()))
                    .send().await
                    .map_err(|e| format!("Failed to put tombstone {}: {:?}", key, e))?;
            }
            
            for item in new_xorbs_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                let active_key = format!("xorbs/default/{}", hash_str);
                let _ = client.delete_object().bucket(&bucket).key(&active_key).send().await;
            }
            
            // Delete tombstones for old shards (resurrection) and move back from bin
            for item in old_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                let _ = client.delete_object().bucket(&bucket).key(&key).send().await;
                
                let src = format!("{}/bin/shards/{}.mdb", bucket, hash_str);
                let dest = format!("shards/{}.mdb", hash_str);
                if let Ok(_) = client.copy_object().copy_source(&src).bucket(&bucket).key(&dest).send().await {
                    let bin_key = format!("bin/shards/{}.mdb", hash_str);
                    let _ = client.delete_object().bucket(&bucket).key(&bin_key).send().await;
                }
            }

            for item in old_xorbs_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                
                let src = format!("{}/bin/xorbs/{}", bucket, hash_str);
                let dest = format!("xorbs/default/{}", hash_str);
                if let Ok(_) = client.copy_object().copy_source(&src).bucket(&bucket).key(&dest).send().await {
                    let bin_key = format!("bin/xorbs/{}", hash_str);
                    let _ = client.delete_object().bucket(&bucket).key(&bin_key).send().await;
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
        let body = aws_sdk_s3::primitives::ByteStream::from_path(std::path::Path::new(txn_path)).await.unwrap();
        client.put_object().bucket(&bucket).key(key).body(body).send().await.unwrap();
        Ok::<_, String>(())
    }).unwrap();
    
    let _ = std::fs::remove_file(txn_path);
    Ok(())
}

pub fn _prune_garbage(_py: Python<'_>) -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    
    rt.block_on(async {
        // Delete everything in bin/ and gc_consolidated/ immediately
        for prefix in &["bin/", "gc_consolidated/"] {
            let mut continuation_token = None;
            loop {
                let mut req = client.list_objects_v2().bucket(&bucket).prefix(*prefix);
                if let Some(token) = continuation_token {
                    req = req.continuation_token(token);
                }
                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                
                for obj in resp.contents() {
                    if let Some(key) = obj.key() {
                        let _ = client.delete_object().bucket(&bucket).key(key).send().await;
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
        
        let mut continuation_token = None;
        loop {
            let mut req = client.list_objects_v2().bucket(&bucket).prefix("shards/tombstones/");
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp = match req.send().await {
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
                                let _ = client.delete_object().bucket(&bucket).key(&shard_key).send().await;
                            }
                            let _ = client.delete_object().bucket(&bucket).key(key).send().await;
                        }
                    }
                }
            }
            
            continuation_token = resp.next_continuation_token().map(String::from);
            if continuation_token.is_none() {
                break;
            }
        }
        
        // Delete lock file if it exists and is committed or reverted
        let txn_path = "/tmp/active_transaction.redb";
        let lock_key = "gc/active_transaction.redb";
        
        if let Ok(Some(data)) = download_with_retry(&client, &bucket, lock_key, 5).await {
            if let Ok(_) = std::fs::write(txn_path, data) {
                if let Ok(db) = Database::open(txn_path) {
                    if let Ok(read_txn) = db.begin_read() {
                        if let Ok(meta_table) = read_txn.open_table(TXN_META_TABLE) {
                            if let Ok(Some(status)) = meta_table.get("status") {
                                let status_val = status.value().to_string();
                                if status_val == "committed" || status_val == "reverted" {
                                    let _ = client.delete_object().bucket(&bucket).key(lock_key).send().await;
                                }
                            }
                        }
                    }
                }
            }
        }
        let _ = std::fs::remove_file(txn_path);

        Ok::<_, String>(())
    }).unwrap();
    
    Ok(())
}

pub fn _get_gc_transaction_info(py: Python<'_>) -> PyResult<Py<pyo3::types::PyDict>> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    let txn_path = format!("/tmp/active_transaction_explore_{}.redb", std::process::id());
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let data = download_with_retry(&client, &bucket, key, 5).await?
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

    drop(read_txn);
    drop(db);
    let _ = std::fs::remove_file(&txn_path);

    Ok(dict.into())
}

