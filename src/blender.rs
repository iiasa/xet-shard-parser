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
            match client.get_object().bucket(&bucket).key(&key).send().await {
                Ok(resp) => {
                    let data = resp.body.collect().await
                        .map_err(|e| format!("Failed to collect {}: {:?}", key, e))?;
                    Ok::<Option<Vec<u8>>, String>(Some(data.into_bytes().to_vec()))
                },
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    println!("GC DEBUG: S3 fetch failed with error: {}", err_str);
                    if err_str.contains("NoSuchKey") || err_str.contains("NotFound") {
                        Ok::<Option<Vec<u8>>, String>(None)
                    } else {
                        Err(format!("Failed to get {}: {}", key, err_str))
                    }
                }
            }
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;

        let xorb_bytes = match xorb_bytes_opt {
            Some(b) => b,
            None => {
                println!("GC DEBUG: SKIP! XORB {} not found in S3!", xorb_hash_str);
                continue;
            }
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
                println!("GC DEBUG: Checking chunk {} in XORB {} -> is_live={}", h.hex(), xorb_hash_str, is_live);
                
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

            println!("GC DEBUG: Shard has {} files", minimal_shard.num_files());
            for i in 0..minimal_shard.num_files() {
                if let Some(file_view) = minimal_shard.file(i) {
                    let fh_bytes: [u8; 32] = file_view.file_hash().into();
                    println!("GC DEBUG: Checking file {}", file_view.file_hash().hex());
                    if live_files.get(&fh_bytes).unwrap().is_some() {
                        println!("GC DEBUG: File {} is live!", file_view.file_hash().hex());
                        let mut file_info = MDBFileInfo::from(file_view);
                        let mut valid = true;
                        
                        for seg in &mut file_info.segments {
                        println!("GC DEBUG: File {} has segment with xorb {}", file_view.file_hash().hex(), seg.xorb_hash.hex());
                        let seg_hash_bytes: [u8; 32] = seg.xorb_hash.into();
                        let is_sparse = sparse_xorbs.get(&seg_hash_bytes).unwrap().is_some();
                        println!("GC DEBUG: Is segment sparse? {}", is_sparse);
                        if is_sparse {
                                let mut layout_key = [0u8; 36];
                                layout_key[0..32].copy_from_slice(&seg_hash_bytes);
                                layout_key[32..36].copy_from_slice(&seg.chunk_index_start.to_le_bytes());
                                
                                if let Some(chunk_hash_val) = layout_table.get(&layout_key).unwrap() {
                                    let first_chunk_bytes: [u8; 32] = *chunk_hash_val.value();
                                    let first_chunk_hash = MerkleHash::from(first_chunk_bytes);
                                    
                                    if let Some(chunk_val) = chunk_map_table.get(&first_chunk_bytes).unwrap() {
                                        let val = chunk_val.value();
                                        let new_xorb_hash = MerkleHash::from(<[u8; 32]>::try_from(&val[0..32]).unwrap());
                                        let new_start_idx = u32::from_le_bytes(val[32..36].try_into().unwrap());
                                        
                                        println!("GC DEBUG: Translating segment chunk {} -> new XORB {} idx {}", first_chunk_hash.hex(), new_xorb_hash.hex(), new_start_idx);
                                        
                                        seg.xorb_hash = new_xorb_hash;
                                        let length = seg.chunk_index_end - seg.chunk_index_start;
                                        seg.chunk_index_start = new_start_idx;
                                        seg.chunk_index_end = new_start_idx + length;
                                    } else {
                                        println!("GC DEBUG: VALID=FALSE! Chunk {} not found in map for XORB {}", first_chunk_hash.hex(), seg.xorb_hash.hex());
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

pub fn _stage_gc_transaction(_py: Python<'_>) -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let resp = client.get_object().bucket(&bucket).key(key).send().await
            .map_err(|e| format!("Failed to get lock: {:?}", e))?;
        let data = resp.body.collect().await.map_err(|e| format!("Failed to read lock: {:?}", e))?;
        std::fs::write(txn_path, data.into_bytes()).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let db = Database::create(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB err: {e}")))?;
    let write_txn = db.begin_write().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn err: {e}")))?;
    
    {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let status = meta.get("status").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        if status.is_none() || status.unwrap().value() != "consolidated" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot stage: status is not consolidated"));
        }
        
        let new_xorbs_table = write_txn.open_table(TXN_NEW_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let new_shards_table = write_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let old_shards_table = write_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        
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
            
            // Tombstone old shards
            for item in old_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                client.put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(aws_sdk_s3::primitives::ByteStream::from(Vec::new()))
                    .send().await
                    .map_err(|e| format!("Failed to put tombstone {}: {:?}", key, e))?;
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
) -> PyResult<Vec<String>> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let resp = client.get_object().bucket(&bucket).key(key).send().await
            .map_err(|e| format!("Failed to get lock: {:?}", e))?;
        let data = resp.body.collect().await.map_err(|e| format!("Failed to read lock: {:?}", e))?;
        std::fs::write(txn_path, data.into_bytes()).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let db = Database::create(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB err: {e}")))?;
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
    
    // Integrity Verification (Pass 1 and 2)
    let mut missing_files = Vec::new();
    let gc_db_lock = gc_db.read().unwrap();
    if let Some(ref gcdb) = *gc_db_lock {
        let read_txn = gcdb.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn error: {e}")))?;
        if let Ok(files_table) = read_txn.open_table(GC_LIVE_FILES_TABLE) {
            
            // 1. Gather all file dependencies
            let mut file_dependencies = std::collections::HashMap::new();
            let mut unique_xorbs = std::collections::HashSet::new();
            
            for item in files_table.iter().unwrap() {
                let (file_hash_bytes, _) = item.unwrap();
                let file_hash_val = file_hash_bytes.value();
                let mut h = [0u8; 32];
                h.copy_from_slice(file_hash_val);
                let file_hash = MerkleHash::from(h);
                let file_hash_hex = file_hash.hex();
                
                let res = rt.block_on(async { sfm.get_file_reconstruction_info(&file_hash).await });
                match res {
                    Ok(Some((info, _))) => {
                        let mut xorb_deps = Vec::new();
                        for segment in info.segments {
                            let xh = segment.xorb_hash.hex();
                            unique_xorbs.insert(xh.clone());
                            xorb_deps.push(xh);
                        }
                        file_dependencies.insert(file_hash_hex, xorb_deps);
                    },
                    _ => {
                        missing_files.push(file_hash_hex);
                    }
                }
            }
            
            // 2. Concurrently verify all unique XORBs
            use futures::stream::{StreamExt, iter};
            let mut missing_xorbs = std::collections::HashSet::new();
            
            let verification_results = rt.block_on(async {
                let futures_iter = unique_xorbs.into_iter().map(|xorb_hash_hex| {
                    let client = client.clone();
                    let bucket = bucket.clone();
                    async move {
                        let key_live = format!("xorbs/default/{}", xorb_hash_hex);
                        let key_staged = format!("gc_consolidated/xorbs/{}", xorb_hash_hex);
                        
                        let s3_res_live = client.head_object().bucket(&bucket).key(&key_live).send().await;
                        if s3_res_live.is_ok() {
                            return (xorb_hash_hex, true);
                        }
                        
                        let s3_res_staged = client.head_object().bucket(&bucket).key(&key_staged).send().await;
                        if s3_res_staged.is_ok() {
                            return (xorb_hash_hex, true);
                        }
                        
                        (xorb_hash_hex, false)
                    }
                });
                
                iter(futures_iter)
                    .buffer_unordered(100)
                    .collect::<Vec<(String, bool)>>()
                    .await
            });
            
            for (xh, exists) in verification_results {
                if !exists {
                    missing_xorbs.insert(xh);
                }
            }
            
            // 3. Mark files missing if they depend on missing XORBs
            for (file_hash_hex, xorb_deps) in file_dependencies {
                if xorb_deps.iter().any(|xh| missing_xorbs.contains(xh)) {
                    missing_files.push(file_hash_hex);
                }
            }
        }
    }

    if missing_files.is_empty() {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        meta.insert("status", "verified").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
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
    Ok(missing_files)
}

pub fn _commit_gc_transaction(_py: Python<'_>) -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let resp = client.get_object().bucket(&bucket).key(key).send().await
            .map_err(|e| format!("Failed to get lock: {:?}", e))?;
        let data = resp.body.collect().await.map_err(|e| format!("Failed to read lock: {:?}", e))?;
        std::fs::write(txn_path, data.into_bytes()).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let db = Database::create(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB err: {e}")))?;
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
        let resp = client.get_object().bucket(&bucket).key(key).send().await
            .map_err(|e| format!("Failed to get lock: {:?}", e))?;
        let data = resp.body.collect().await.map_err(|e| format!("Failed to read lock: {:?}", e))?;
        std::fs::write(txn_path, data.into_bytes()).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let db = Database::create(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB err: {e}")))?;
    let write_txn = db.begin_write().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn err: {e}")))?;
    
    {
        let mut meta = write_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let status = meta.get("status").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let status_val = status.map(|s| s.value().to_string()).unwrap_or_default();
        if status_val != "consolidated" && status_val != "staged" && status_val != "verified" {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot revert: status is not consolidated, staged or verified"));
        }
        
        let new_shards_table = write_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        let old_shards_table = write_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        
        rt.block_on(async {
            for item in new_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                client.put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(aws_sdk_s3::primitives::ByteStream::from(Vec::new()))
                    .send().await
                    .map_err(|e| format!("Failed to put tombstone {}: {:?}", key, e))?;
            }
            
            // Delete tombstones for old shards (resurrection)
            for item in old_shards_table.iter().unwrap() {
                let (hash_key, _) = item.unwrap();
                let hash_str = hash_key.value();
                let key = format!("shards/tombstones/{}.revoked", hash_str);
                let _ = client.delete_object().bucket(&bucket).key(&key).send().await;
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

pub fn _sweep_garbage(_py: Python<'_>) -> PyResult<()> {
    let rt = Runtime::new().unwrap();
    let (client, bucket) = _setup_s3_client()?;
    let txn_path = "/tmp/active_transaction.redb";
    
    rt.block_on(async {
        let key = "gc/active_transaction.redb";
        let resp = client.get_object().bucket(&bucket).key(key).send().await
            .map_err(|e| format!("Failed to get lock: {:?}", e))?;
        let data = resp.body.collect().await.map_err(|e| format!("Failed to read lock: {:?}", e))?;
        std::fs::write(txn_path, data.into_bytes()).map_err(|e| format!("Failed to save lock: {}", e))?;
        Ok::<_, String>(())
    }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
    
    let db = Database::create(txn_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("DB err: {e}")))?;
    let read_txn = db.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn err: {e}")))?;
    
    let meta = read_txn.open_table(TXN_META_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    let status_val = meta.get("status").map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    
    if status_val.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("No status found in transaction"));
    }
    
    let status = status_val.unwrap().value().to_string();
    
    let is_commit = if status == "committed" { true } else if status == "reverted" { false } else {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Cannot sweep: status is {}", status)));
    };
    
    // We need to delete old_xorbs and old_shards if committed, OR new_xorbs and new_shards if reverted.
    let mut xorbs_to_delete: Vec<String> = Vec::new();
    if is_commit {
        let old_xorbs = read_txn.open_table(TXN_OLD_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        for item in old_xorbs.iter().unwrap() { xorbs_to_delete.push(item.unwrap().0.value().to_string()); }
    } else {
        let new_xorbs = read_txn.open_table(TXN_NEW_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        for item in new_xorbs.iter().unwrap() { xorbs_to_delete.push(item.unwrap().0.value().to_string()); }
    }

    let mut shards_to_delete: Vec<String> = Vec::new();
    if is_commit {
        let old_shards = read_txn.open_table(TXN_OLD_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        for item in old_shards.iter().unwrap() { shards_to_delete.push(item.unwrap().0.value().to_string()); }
    } else {
        let new_shards = read_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
        for item in new_shards.iter().unwrap() { shards_to_delete.push(item.unwrap().0.value().to_string()); }
    }

    let mut new_xorbs_to_delete: Vec<String> = Vec::new();
    let new_xorbs_table = read_txn.open_table(TXN_NEW_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    for item in new_xorbs_table.iter().unwrap() { new_xorbs_to_delete.push(item.unwrap().0.value().to_string()); }

    let mut new_shards_to_delete: Vec<String> = Vec::new();
    let new_shards_table = read_txn.open_table(TXN_NEW_SHARDS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table err: {e}")))?;
    for item in new_shards_table.iter().unwrap() { new_shards_to_delete.push(item.unwrap().0.value().to_string()); }
    
    rt.block_on(async {
        // Delete XORBs
        for hash_str in &xorbs_to_delete {
            let key = format!("xorbs/default/{}", hash_str);
            client.delete_object().bucket(&bucket).key(&key).send().await
                .map_err(|e| format!("Failed to delete XORB {}: {:?}", key, e))?;
        }
        
        // Delete Shards (Leave tombstones so HydrationTask can discover them)
        for hash_str in &shards_to_delete {
            let key = format!("shards/{}.mdb", hash_str);
            client.delete_object().bucket(&bucket).key(&key).send().await
                .map_err(|e| format!("Failed to delete shard {}: {:?}", key, e))?;
        }
        
        // ALWAYS Delete Staging versions from gc_consolidated/
        for hash_str in &new_xorbs_to_delete {
            let key = format!("gc_consolidated/xorbs/{}", hash_str);
            client.delete_object().bucket(&bucket).key(&key).send().await
                .map_err(|e| format!("Failed to delete staging XORB {}: {:?}", key, e))?;
        }
        for hash_str in &new_shards_to_delete {
            let key = format!("gc_consolidated/shards/{}.mdb", hash_str);
            client.delete_object().bucket(&bucket).key(&key).send().await
                .map_err(|e| format!("Failed to delete staging shard {}: {:?}", key, e))?;
        }
        
        // Finally, delete the lock file itself
        client.delete_object().bucket(&bucket).key("gc/active_transaction.redb").send().await
            .map_err(|e| format!("Failed to delete transaction lock: {:?}", e))?;
        
        // As a final cleanup step, delete any tombstones older than 7 days
        let mut continuation_token = None;
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        let seven_days = 7 * 24 * 60 * 60;
        
        loop {
            let mut req = client.list_objects_v2().bucket(&bucket).prefix("shards/tombstones/");
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(_) => break, // If listing fails, we just skip tombstone cleanup for this run
            };
            
            for obj in resp.contents() {
                if let Some(last_modified) = obj.last_modified() {
                    if now - last_modified.secs() > seven_days {
                        if let Some(key) = obj.key() {
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
        
        Ok::<_, String>(())
    }).unwrap();
    
    drop(db);
    let _ = std::fs::remove_file(txn_path);
    Ok(())
}


