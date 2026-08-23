use std::io::{Cursor, Seek};
use std::sync::Arc;

use mdb_shard::metadata_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::metadata_shard::set_operations::shard_set_union;
use mdb_shard::metadata_shard::{MDBShardInfo, MDBShardFileHeader, MDBShardFileFooter};
use mdb_shard::metadata_shard::ShardFileManager;

use mdb_shard::metadata_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::metadata_shard::xorb_structs::{MDBXorbInfo, XorbChunkSequenceHeader, XorbChunkSequenceEntry};
use mdb_shard::merklehash::{MerkleHash, compute_data_hash};
use mdb_shard::xorb_object::{reconstruct_xorb_with_footer, XorbObject};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata};

mod blender;
use std::mem::size_of;
use std::mem::swap;
use std::io::Write;
use futures::{StreamExt, TryStreamExt};
use reqwest::Client;

const GLOBAL_DEDUP_TABLE: redb::TableDefinition<&[u8; 32], &[u8; 32]> = redb::TableDefinition::new("global_dedup");
const GC_LIVE_CHUNKS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_chunks");
const GC_PRIMARY_XORB_TABLE: redb::TableDefinition<&[u8; 32], &[u8; 32]> = redb::TableDefinition::new("gc_primary_xorb");
const GC_XORB_UTILIZATION_TABLE: redb::TableDefinition<&[u8; 32], &str> = redb::TableDefinition::new("gc_xorb_utilization");
const GC_SPARSE_XORBS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_sparse_xorbs");
const GC_LIVE_FILES_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_files");

fn parse_xorb_footer_data(bytes: &[u8]) -> Option<(Vec<MerkleHash>, Vec<u32>, Vec<u32>)> {
    let mut reader = Cursor::new(bytes);
    let xorb_obj = XorbObject::deserialize(&mut reader).ok()?;
    let info = xorb_obj.info;
    Some((info.chunk_hashes, info.chunk_boundary_offsets, info.unpacked_chunk_offsets))
}

#[pyclass]
pub struct ShardIndex {
    sfm: Arc<ShardFileManager>,
    rt: tokio::runtime::Runtime,
    db: Arc<redb::Database>,
    client: Client,
    gc_db: Arc<std::sync::RwLock<Option<redb::Database>>>,
}

#[pymethods]
impl ShardIndex {
    #[new]
    pub fn new(cache_dir: String, db_path: String, max_cache_size: Option<u64>) -> PyResult<Self> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create tokio runtime: {e}")))?;
        
        let ctx = xet_runtime::core::context::XetContext::default()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to initialize XetContext: {e:?}")))?;

        let sfm = rt.block_on(async {
            ShardFileManager::new_in_cache_directory(&ctx, cache_dir).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to init ShardFileManager: {e:?}")))?;

        let db = redb::Database::create(db_path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open redb: {e}")))?;

        let client = Client::builder()
            .danger_accept_invalid_certs(true)
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to build client: {e}")))?;

        let index = ShardIndex {
            sfm,
            rt,
            db: Arc::new(db),
            client,
            gc_db: Arc::new(std::sync::RwLock::new(None)),
        };

        // Trigger an initial refresh with the size limit
        index.refresh(max_cache_size)?;

        Ok(index)
    }

    pub fn get_all_shard_xorbs(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let shards = self.rt.block_on(async {
            self.sfm.registered_shard_list().await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;

        let dict = PyDict::new(py);

        for shard_file in shards {
            let mut reader = match shard_file.get_reader() {
                Ok(r) => r,
                Err(_) => continue,
            };
            
            let m_shard = match MDBMinimalShard::from_reader(&mut reader, false, true) {
                Ok(s) => s,
                Err(_) => continue,
            };
            
            let xorbs_list = PyList::empty(py);
            for i in 0..m_shard.num_xorb() {
                if let Some(xiv) = m_shard.xorb(i) {
                    let h_hex = xiv.xorb_hash().hex();
                    let _ = xorbs_list.append(h_hex);
                }
            }
            let _ = dict.set_item(shard_file.shard_hash.hex(), xorbs_list);
        }

        Ok(dict.into())
    }

    #[pyo3(signature = (file_hash_hex))]
    pub fn get_file_size(&self, py: Python<'_>, file_hash_hex: &str) -> PyResult<Option<u64>> {
        let h = MerkleHash::from_hex(file_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        
        let res = self.rt.block_on(async {
            self.sfm.get_file_reconstruction_info(&h).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Query failed: {e:?}")))?;
        
        if let Some((file_info, _)) = res {
            Ok(Some(file_info.file_size() as u64))
        } else {
            Ok(None)
        }
    }

    #[pyo3(signature = (xorb_hashes))]
    pub fn get_shards_for_xorbs(&self, py: Python<'_>, xorb_hashes: Vec<String>) -> PyResult<std::collections::HashSet<String>> {
        let mut target_xorbs: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();
        for hex in xorb_hashes {
            if let Ok(mh) = MerkleHash::from_hex(&hex) {
                target_xorbs.insert(mh.into());
            }
        }
        
        let shards = self.rt.block_on(async {
            self.sfm.registered_shard_list().await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;

        let mut matching_shards = std::collections::HashSet::new();

        for shard_file in shards {
            let mut reader = match shard_file.get_reader() {
                Ok(r) => r,
                Err(_) => continue,
            };
            
            let m_shard = match MDBMinimalShard::from_reader(&mut reader, false, true) {
                Ok(s) => s,
                Err(_) => continue,
            };
            
            for i in 0..m_shard.num_xorb() {
                if let Some(xiv) = m_shard.xorb(i) {
                    let h: [u8; 32] = xiv.xorb_hash().into();
                    if target_xorbs.contains(&h) {
                        matching_shards.insert(shard_file.shard_hash.hex());
                        break;
                    }
                }
            }
        }

        Ok(matching_shards)
    }



    #[pyo3(signature = ())]
    pub fn start_gc_transaction(&self, py: Python<'_>) -> PyResult<bool> {
        let gc_db_lock = self.gc_db.read().unwrap();
        if gc_db_lock.is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized"));
        }
        let db = gc_db_lock.as_ref().unwrap();
        let read_txn = db.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
        let sparse_xorbs = read_txn.open_table(blender::GC_SPARSE_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
        
        if sparse_xorbs.is_empty().unwrap_or(true) {
            return Ok(false);
        }
        
        drop(read_txn);
        drop(gc_db_lock);
        
        blender::_consolidate_metadata(py, self.sfm.clone(), self.gc_db.clone())?;
        
        Ok(true)
    }

    #[pyo3(signature = ())]
    pub fn stage_gc_transaction(&self, py: Python<'_>) -> PyResult<bool> {
        blender::_stage_gc_transaction(py)?;
        Ok(true)
    }

    #[pyo3(signature = ())]
    pub fn verify_gc_transaction(&self, py: Python<'_>) -> PyResult<usize> {
        let missing = blender::_verify_gc_transaction(py, self.sfm.clone(), self.gc_db.clone())?;
        Ok(missing)
    }

    #[pyo3(signature = ())]
    pub fn commit_gc_transaction(&self, py: Python<'_>) -> PyResult<bool> {
        blender::_commit_gc_transaction(py)?;
        Ok(true)
    }

    #[pyo3(signature = ())]
    pub fn revert_gc_transaction(&self, py: Python<'_>) -> PyResult<bool> {
        blender::_revert_gc_transaction(py)?;
        Ok(true)
    }

    #[pyo3(signature = ())]
    pub fn prune_garbage(&self, py: Python<'_>) -> PyResult<bool> {
        blender::_prune_garbage(py)?;
        Ok(true)
    }

    pub fn prune_shard(&self, shard_hash_hex: &str) -> PyResult<()> {
        let h = MerkleHash::from_hex(shard_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        let write_txn = self.db.begin_write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn failed: {e}")))?;
        {
            let mut table = match write_txn.open_table(GLOBAL_DEDUP_TABLE) {
                Ok(t) => t,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(e) => return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}"))),
            };
            
            let mut to_delete = Vec::new();
            for entry in table.iter().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))? {
                let (k, v) = entry.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
                if v.value() == &h_bytes {
                    to_delete.push(*k.value());
                }
            }

            for k in to_delete {
                table.remove(&k)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Remove failed: {e}")))?;
            }
        }
        write_txn.commit()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit failed: {e}")))?;

        Ok(())
    }

    pub fn register_shard(&self, shard_bytes: &[u8], shard_hash_hex: Option<String>) -> PyResult<()> {
        // 1. Register with ShardFileManager (persists .sib to disk and indexes in memory)
        self.rt.block_on(async {
            self.sfm.import_shard_from_bytes(shard_bytes).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to import shard: {e:?}")))?;

        // 2. Index global deduplication chunks in redb
        let mut cursor = Cursor::new(shard_bytes);
        let shard = MDBMinimalShard::from_reader(&mut cursor, true, true)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Parse error: {e:?}")))?;

        let shard_hash = if let Some(h_hex) = shard_hash_hex {
            MerkleHash::from_hex(&h_hex)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid shard hash hex: {e:?}")))?
        } else {
            compute_data_hash(shard_bytes)
        };
        let shard_hash_bytes: [u8; 32] = shard_hash.into();

        let write_txn = self.db.begin_write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn failed: {e}")))?;
        {
            let mut table = write_txn.open_table(GLOBAL_DEDUP_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
            
            for chunk_hash in shard.global_dedup_eligible_chunks() {
                let chunk_hash_bytes: [u8; 32] = chunk_hash.into();
                table.insert(&chunk_hash_bytes, &shard_hash_bytes)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert failed: {e}")))?;
            }
        }
        write_txn.commit()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit failed: {e}")))?;

        Ok(())
    }

    #[pyo3(signature = (file_hash_hex, start_byte=None, end_byte=None, footers=None, coalesce=None))]
    pub fn calculate_reconstruction(
        &self,
        py: Python<'_>,
        file_hash_hex: &str,
        start_byte: Option<u64>,
        end_byte: Option<u64>,
        footers: Option<&PyDict>,
        coalesce: Option<bool>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let mut xorb_footers = std::collections::HashMap::new();
        
        if let Some(footers) = footers {
            let mut raw_map = std::collections::HashMap::new();
            for (k, v) in footers.iter() {
                raw_map.insert(k.extract::<String>()?, v.extract::<&[u8]>()?);
            }
            
            for (xh_hex, bytes) in raw_map {
                let hash = MerkleHash::from_hex(&xh_hex)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
                let footer = parse_xorb_footer_data(bytes);
                
                xorb_footers.insert(hash, footer);
            }
        }

        self.calculate_reconstruction_internal(py, file_hash_hex, start_byte, end_byte, xorb_footers, coalesce.unwrap_or(true))
    }

    #[pyo3(signature = (file_hash_hex, start_byte, end_byte, xorb_urls, coalesce=None))]
    pub fn calculate_reconstruction_with_urls(
        &self,
        py: Python<'_>,
        file_hash_hex: &str,
        start_byte: Option<u64>,
        end_byte: Option<u64>,
        xorb_urls: &PyDict,
        coalesce: Option<bool>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let h = MerkleHash::from_hex(file_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;

        let res = self.rt.block_on(async {
            self.sfm.get_file_reconstruction_info(&h).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Query failed: {e:?}")))?;

        let (file_info, _) = match res {
            Some(r) => r,
            None => return Ok(None),
        };

        // Determine which XORBs are needed for the requested range
        let total_file_size = file_info.file_size();
        let file_range_start = start_byte.unwrap_or(0);
        let file_range_end = end_byte.unwrap_or(total_file_size).min(total_file_size);

        let mut cumulative_bytes = 0u64;
        let mut needed_xorbs = std::collections::HashSet::new();
        for segment in &file_info.segments {
            let n = segment.unpacked_segment_bytes as u64;
            if cumulative_bytes + n > file_range_start && cumulative_bytes <= file_range_end {
                needed_xorbs.insert(segment.xorb_hash);
            }
            cumulative_bytes += n;
        }

        println!("[Rust Cas Debug] needed_xorbs count: {}", needed_xorbs.len());
        let keys = xorb_urls.keys();
        println!("[Rust Cas Debug] xorb_urls keys: {:?}", keys);

        // Concurrenty fetch footers for required XORBs
        let mut xorb_footers = std::collections::HashMap::new();
        let mut fetch_tasks = Vec::new();

        for xh in needed_xorbs {
            let xh_hex = xh.hex();
            let mut found = false;
            println!("[Rust Cas Debug] Looking up xh_hex: {}", xh_hex);
            if let Ok(Some(url_obj)) = xorb_urls.get_item(&xh_hex) {
                if let Ok(url) = url_obj.extract::<String>() {
                    println!("[Rust Cas Debug] Found URL: {}", url);
                    fetch_tasks.push((xh, url));
                    found = true;
                } else {
                    println!("[Rust Cas Debug] Failed to extract URL string");
                }
            } else {
                println!("[Rust Cas Debug] xorb_urls does not contain {}", xh_hex);
            }
            if !found {
                xorb_footers.insert(xh, None);
            }
        }

        if !fetch_tasks.is_empty() {
            let footers_res = self.rt.block_on(async {
                let client = self.client.clone();

                let results: Vec<(MerkleHash, Option<Vec<u8>>)> = futures::stream::iter(fetch_tasks)
                    .map(|(xh, url)| {
                        let client = client.clone();
                        async move {
                            // Fetch last 64KB for footer
                            let resp = client.get(&url)
                                .header("Range", "bytes=-1048576")
                                .send()
                                .await;
                            
                            match resp {
                                Ok(r) if r.status().is_success() || r.status() == reqwest::StatusCode::PARTIAL_CONTENT => {
                                    let bytes = r.bytes().await.ok().map(|b| b.to_vec());
                                    (xh, bytes)
                                }
                                Ok(r) => {
                                    eprintln!("[Rust Cas Debug] Fetch footer failed for {}: status={}", url, r.status());
                                    (xh, None)
                                }
                                Err(e) => {
                                    eprintln!("[Rust Cas Debug] Fetch footer request failed for {}: error={}", url, e);
                                    (xh, None)
                                }
                            }
                        }
                    })
                    .buffer_unordered(10)
                    .collect()
                    .await;
                
                Ok::<Vec<(MerkleHash, Option<Vec<u8>>)>, String>(results)
            }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

            for (xh, bytes_opt) in footers_res {
                let footer = bytes_opt.and_then(|bytes| parse_xorb_footer_data(&bytes));
                xorb_footers.insert(xh, footer);
            }
        }

        self.calculate_reconstruction_internal(py, file_hash_hex, start_byte, end_byte, xorb_footers, coalesce.unwrap_or(true))
    }

    pub fn get_chunk_shard(&self, chunk_hash_hex: &str) -> PyResult<Option<String>> {
        let h = MerkleHash::from_hex(chunk_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        let read_txn = self.db.begin_read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
        let table = match read_txn.open_table(GLOBAL_DEDUP_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}"))),
        };
        
        let res = table.get(&h_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Get failed: {e}")))?;

        if let Some(shard_hash_bytes) = res {
            let shard_hash = MerkleHash::from(*shard_hash_bytes.value());
            return Ok(Some(shard_hash.hex()));
        }

        Ok(None)
    }

    pub fn get_xorb_layout(&self, py: Python<'_>, xorb_hash_hex: &str) -> PyResult<Option<Py<PyList>>> {
        let h = MerkleHash::from_hex(xorb_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;

        let shards = self.rt.block_on(async {
            self.sfm.registered_shard_list().await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;

        for shard_file in shards {
            let mut reader = match shard_file.get_reader() {
                Ok(r) => r,
                Err(_) => continue,
            };
            
            let mut dest_indices = [0u32; 8];
            if let Ok(num_indices) = shard_file.shard.get_xorb_info_index_by_hash(&mut reader, &h, &mut dest_indices) {
                if num_indices > 0 {
                    reader.rewind().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
                    let m_shard = MDBMinimalShard::from_reader(&mut reader, false, true)
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Parse error: {e:?}")))?;
                    
                    for i in 0..m_shard.num_xorb() {
                        if let Some(xiv) = m_shard.xorb(i) {
                            if xiv.xorb_hash() == h {
                                let list = PyList::empty(py);
                                for j in 0..xiv.num_entries() {
                                    let chunk = xiv.chunk(j);
                                    let entry = PyList::empty(py);
                                    entry.append(chunk.chunk_hash.hex())?;
                                    entry.append(chunk.chunk_byte_range_start)?;
                                    entry.append(chunk.unpacked_segment_bytes)?;
                                    list.append(entry)?;
                                }
                                return Ok(Some(list.into()));
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn refresh(&self, max_cache_size: Option<u64>) -> PyResult<()> {
        let prune_size = max_cache_size.unwrap_or(0);
        self.rt.block_on(async {
            self.sfm.refresh_shard_dir(false, prune_size).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Refresh failed: {e:?}")))?;
        Ok(())
    }

    #[pyo3(signature = (tasks))]
    pub fn reconstruct_file_parallel(
        &self,
        py: Python<'_>,
        tasks: &PyList,
    ) -> PyResult<PyObject> {
        // tasks is a list of (url, byte_start, byte_end, unpacked_size)
        let mut fetch_tasks = Vec::new();
        let mut total_size = 0;

        for item in tasks.iter() {
            let task_tuple: (String, u64, u64, u32) = item.extract()?;
            total_size += task_tuple.3 as usize;
            fetch_tasks.push(task_tuple);
        }

        if fetch_tasks.is_empty() {
            return Ok(PyBytes::new(py, &[]).into());
        }

        let mut output_buffer = vec![0u8; total_size];

        self.rt.block_on(async {
            let client = self.client.clone();
            
            let mut results: Vec<Result<Vec<u8>, String>> = Vec::new();
            if !fetch_tasks.is_empty() {
                results = futures::stream::iter(fetch_tasks)
                    .map(|(url, b_start, b_end, unpacked_size)| {
                        let client = client.clone();
                        async move {
                            // Fetch the exact byte range
                            let resp = client.get(&url)
                                .header("Range", format!("bytes={}-{}", b_start, b_end))
                                .send()
                                .await
                                .map_err(|e| format!("Fetch failed for {}: {}", url, e))?;
                            
                            let mut decompressed = Vec::new();
                            let mut writer = std::io::Cursor::new(&mut decompressed);
                            
                            let mut stream_reader = resp.bytes_stream()
                                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                                .into_async_read();
                                
                            let mut total_unpacked = 0u32;
                            while total_unpacked < unpacked_size {
                                let (_, unpacked_len) = mdb_shard::xorb_object::deserialize_async::deserialize_chunk_to_writer(&mut stream_reader, &mut writer).await
                                    .map_err(|e| format!("Decompression failed: {:?}", e))?;
                                total_unpacked += unpacked_len;
                            }
                            
                            Ok::<Vec<u8>, String>(decompressed)
                        }
                    })
                    .buffered(16) // Limit concurrency to 16 requests per batch
                    .collect::<Vec<_>>()
                    .await;
            }
            
            let mut final_offset = 0;
            for res in results {
                match res {
                    Ok(data) => {
                        let len = data.len().min(total_size - final_offset);
                        if len > 0 {
                            output_buffer[final_offset..final_offset + len].copy_from_slice(&data[..len]);
                            final_offset += len;
                        }
                    }
                    Err(e) => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
                }
            }
            
            Ok::<(), PyErr>(())
        })?;

        let bytes = PyBytes::new(py, &output_buffer);
        Ok(bytes.into())
    }

    #[pyo3(signature = (gc_db_path))]
    pub fn init_gc(&self, gc_db_path: &str) -> PyResult<()> {
        let db = redb::Database::create(gc_db_path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to create GC DB: {e}")))?;
        
        let write_txn = db.begin_write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("GC DB Write txn failed: {e}")))?;
        {
            let _ = write_txn.open_table(GC_LIVE_CHUNKS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open GC_LIVE_CHUNKS_TABLE: {e}")))?;
            let _ = write_txn.open_table(GC_PRIMARY_XORB_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open GC_PRIMARY_XORB_TABLE: {e}")))?;
            let _ = write_txn.open_table(GC_XORB_UTILIZATION_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open GC_XORB_UTILIZATION_TABLE: {e}")))?;
        }
        write_txn.commit()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("GC DB Commit failed: {e}")))?;

        *self.gc_db.write().unwrap() = Some(db);
        Ok(())
    }

    #[pyo3(signature = ())]
    pub fn cleanup_gc(&self) -> PyResult<()> {
        *self.gc_db.write().unwrap() = None;
        Ok(())
    }

    #[pyo3(signature = (live_file_hashes))]
    pub fn build_live_chunks_list(&self, py: Python<'_>, live_file_hashes: Vec<String>) -> PyResult<()> {
        let gc_db_lock = self.gc_db.read().unwrap();
        let gc_db = match &*gc_db_lock {
            Some(db) => db,
            None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
        };

        // Insert live files into GC_LIVE_FILES_TABLE
        let write_txn = gc_db.begin_write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn failed: {e}")))?;
        {
            let mut files_table = write_txn.open_table(GC_LIVE_FILES_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
            for file_hash_hex in &live_file_hashes {
                if let Ok(h) = MerkleHash::from_hex(file_hash_hex) {
                    let h_bytes: [u8; 32] = h.into();
                    files_table.insert(&h_bytes, &())
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert failed: {e}")))?;
                }
            }
        }
        write_txn.commit()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit failed: {e}")))?;

        for file_hash_hex in live_file_hashes {
            let h = MerkleHash::from_hex(&file_hash_hex)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;

            let res = self.rt.block_on(async {
                self.sfm.get_file_reconstruction_info(&h).await
            }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Query failed for {}: {:?}", file_hash_hex, e)))?;

            let (file_info, _) = match res {
                Some(r) => r,
                None => continue,
            };

            let mut chunks_to_insert: Vec<([u8; 32], [u8; 32])> = Vec::new();

            for segment in &file_info.segments {
                let xorb_hash_hex = segment.xorb_hash.hex();
                let layout_opt = self.get_xorb_layout(py, &xorb_hash_hex)?;
                
                if let Some(layout) = layout_opt {
                    let layout_ref = layout.as_ref(py);
                    let start_idx = segment.chunk_index_start as usize;
                    let end_idx = segment.chunk_index_end as usize;

                    for idx in start_idx..end_idx {
                        if idx < layout_ref.len() {
                            if let Ok(chunk_entry) = layout_ref.get_item(idx) {
                                if let Ok(chunk_list) = chunk_entry.downcast::<PyList>() {
                                    if let Ok(chunk_hash_str) = chunk_list.get_item(0).unwrap().extract::<String>() {
                                        if let Ok(chunk_h) = MerkleHash::from_hex(&chunk_hash_str) {
                                            chunks_to_insert.push((chunk_h.into(), segment.xorb_hash.into()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if !chunks_to_insert.is_empty() {
                let write_txn = gc_db.begin_write()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn failed: {e}")))?;
                {
                    let mut live_table = write_txn.open_table(GC_LIVE_CHUNKS_TABLE)
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
                    let mut primary_table = write_txn.open_table(GC_PRIMARY_XORB_TABLE)
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;

                    for (chunk_h, xorb_h) in chunks_to_insert {
                        live_table.insert(&chunk_h, &())
                            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert failed: {e}")))?;
                        
                        if primary_table.get(&chunk_h).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Get failed: {e}")))?.is_none() {
                            primary_table.insert(&chunk_h, &xorb_h)
                                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert primary failed: {e}")))?;
                        }
                    }
                }
                write_txn.commit()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit failed: {e}")))?;
            }
        }
        Ok(())
    }

    #[pyo3(signature = (chunk_hash_hex))]
    pub fn is_chunk_live(&self, chunk_hash_hex: &str) -> PyResult<bool> {
        let gc_db_lock = self.gc_db.read().unwrap();
        let gc_db = match &*gc_db_lock {
            Some(db) => db,
            None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
        };

        let h = MerkleHash::from_hex(chunk_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        let read_txn = gc_db.begin_read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
        let table = read_txn.open_table(GC_LIVE_CHUNKS_TABLE)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
        
        let res = table.get(&h_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Get failed: {e}")))?;

        Ok(res.is_some())
    }

    #[pyo3(signature = (chunk_hash_hex))]
    pub fn get_primary_xorb(&self, chunk_hash_hex: &str) -> PyResult<Option<String>> {
        let gc_db_lock = self.gc_db.read().unwrap();
        let gc_db = match &*gc_db_lock {
            Some(db) => db,
            None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
        };

        let h = MerkleHash::from_hex(chunk_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        let read_txn = gc_db.begin_read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
        let table = read_txn.open_table(GC_PRIMARY_XORB_TABLE)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
        
        let res = table.get(&h_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Get failed: {e}")))?;

        if let Some(xorb_hash_bytes) = res {
            let xorb_hash = MerkleHash::from(*xorb_hash_bytes.value());
            return Ok(Some(xorb_hash.hex()));
        }
        Ok(None)
    }

    #[pyo3(signature = (sparse_threshold=30.0))]
    pub fn run_global_utilization_analysis(&self, py: Python<'_>, sparse_threshold: f64) -> PyResult<()> {
        let gc_db_lock = self.gc_db.read().unwrap();
        let gc_db = match &*gc_db_lock {
            Some(db) => db,
            None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
        };

        let shards = self.rt.block_on(async {
            self.sfm.registered_shard_list().await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;

        // Collect all XORBs and their layout
        let mut xorb_layouts: std::collections::HashMap<MerkleHash, Vec<(MerkleHash, u32)>> = std::collections::HashMap::new();

        for shard_file in shards {
            let mut reader = match shard_file.get_reader() {
                Ok(r) => r,
                Err(_) => continue,
            };
            
            let m_shard = match MDBMinimalShard::from_reader(&mut reader, false, true) {
                Ok(s) => s,
                Err(_) => continue,
            };
            
            for i in 0..m_shard.num_xorb() {
                if let Some(xiv) = m_shard.xorb(i) {
                    let h = xiv.xorb_hash();
                    if !xorb_layouts.contains_key(&h) {
                        let mut layout = Vec::new();
                        let num_entries = xiv.num_entries();
                        for j in 0..num_entries {
                            let chunk = xiv.chunk(j);
                            
                            // Calculate exact physical packed length using boundaries
                            let packed_length = if j + 1 < num_entries {
                                let next_chunk = xiv.chunk(j + 1);
                                next_chunk.chunk_byte_range_start.saturating_sub(chunk.chunk_byte_range_start)
                            } else {
                                // Fundamental limitation of the Shard Index: The physical size of the final 
                                // chunk is never stored. We must use `unpacked_segment_bytes` as the safe 
                                // upper-bound estimate, which is identical to how xet-core internally fakes it.
                                chunk.unpacked_segment_bytes
                            };
                            
                            layout.push((chunk.chunk_hash, packed_length));
                        }
                        xorb_layouts.insert(h, layout);
                    }
                }
            }
        }

        let write_txn = gc_db.begin_write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn failed: {e}")))?;
        {
            let mut util_table = write_txn.open_table(GC_XORB_UTILIZATION_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
            let mut sparse_table = write_txn.open_table(GC_SPARSE_XORBS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Sparse table open failed: {e}")))?;
            let live_chunks_table = write_txn.open_table(GC_LIVE_CHUNKS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Live chunks table open failed: {e}")))?;

            for (xorb_hash, chunks) in xorb_layouts {
                let mut total_bytes: u64 = 0;
                let mut live_bytes: u64 = 0;
                let mut dead_chunks = Vec::new();

                for (chunk_h, packed_bytes) in chunks {
                    let packed_bytes_u64 = packed_bytes as u64;
                    total_bytes += packed_bytes_u64;
                    
                    let chunk_h_bytes: [u8; 32] = chunk_h.into();
                    let is_live = live_chunks_table.get(&chunk_h_bytes)
                        .unwrap_or(None)
                        .is_some();
                        
                    if is_live {
                        live_bytes += packed_bytes_u64;
                    } else {
                        dead_chunks.push(chunk_h.hex());
                    }
                }

                let utilization = if total_bytes == 0 { 0.0 } else { (live_bytes as f64 / total_bytes as f64) * 100.0 };
                
                // Construct a simple JSON string to save in redb
                let dead_chunks_json = dead_chunks.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",");
                let json_str = format!("{{\"utilization\":{},\"live_bytes\":{},\"total_bytes\":{},\"dead_chunks\":[{}]}}",
                    utilization, live_bytes, total_bytes, dead_chunks_json);
                    
                let xorb_h_bytes: [u8; 32] = xorb_hash.into();
                util_table.insert(&xorb_h_bytes, json_str.as_str())
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert failed: {e}")))?;
                    
                if utilization <= sparse_threshold {
                    sparse_table.insert(&xorb_h_bytes, ())
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert sparse failed: {e}")))?;
                }
            }
        }
        write_txn.commit()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit failed: {e}")))?;

        Ok(())
    }

    #[pyo3(signature = (xorb_hash_hex))]
    pub fn get_xorb_utilization(&self, xorb_hash_hex: &str) -> PyResult<Option<String>> {
        let gc_db_lock = self.gc_db.read().unwrap();
        let gc_db = match &*gc_db_lock {
            Some(db) => db,
            None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
        };

        let h = MerkleHash::from_hex(xorb_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        let read_txn = gc_db.begin_read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
        let table = match read_txn.open_table(GC_XORB_UTILIZATION_TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}"))),
        };
        
        let res = table.get(&h_bytes)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Get failed: {e}")))?;

        if let Some(value) = res {
            let json_str = value.value().to_string();
            return Ok(Some(json_str));
        }
        Ok(None)
    }

    #[pyo3(signature = ())]
    pub fn get_sparse_xorbs(&self, _py: Python<'_>) -> PyResult<Vec<String>> {
        let db_lock = self.gc_db.read().unwrap();
        let db = match &*db_lock {
            Some(d) => d,
            None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized")),
        };

        let read_txn = db.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
        let table = match read_txn.open_table(GC_SPARSE_XORBS_TABLE) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };

        let mut sparse_xorbs = Vec::new();
        for item in table.iter().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table iter failed: {e}")))? {
            let (key, _) = item.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table read failed: {e}")))?;
            sparse_xorbs.push(MerkleHash::from(*key.value()).hex());
        }

        Ok(sparse_xorbs)
    }
}

impl ShardIndex {
    fn calculate_reconstruction_internal(
        &self,
        py: Python<'_>,
        file_hash_hex: &str,
        start_byte: Option<u64>,
        end_byte: Option<u64>,
        xorb_footers: std::collections::HashMap<MerkleHash, Option<(Vec<MerkleHash>, Vec<u32>, Vec<u32>)>>,
        coalesce: bool,
    ) -> PyResult<Option<Py<PyDict>>> {
        let h = MerkleHash::from_hex(file_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;

        let res = self.rt.block_on(async {
            self.sfm.get_file_reconstruction_info(&h).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Query failed: {e:?}")))?;

        let (file_info, _) = match res {
            Some(r) => r,
            None => return Ok(None),
        };

        let total_file_size = file_info.file_size();
        let file_range_start = start_byte.unwrap_or(0);
        let file_range_end = end_byte.unwrap_or(total_file_size).min(total_file_size);

        if file_range_start >= total_file_size {
            if total_file_size == 0 && file_range_start == 0 {
                let dict = PyDict::new(py);
                dict.set_item("offset_into_first_range", 0)?;
                dict.set_item("terms", PyList::empty(py))?;
                dict.set_item("fetch_info", PyDict::new(py))?;
                return Ok(Some(dict.into()));
            }
            return Ok(None);
        }

        #[derive(Clone)]
        struct FetchInfoIntermediate {
            chunk_range_start: u32,
            chunk_range_end: u32,
            byte_range_start: u64,
            byte_range_end: u64,
        }

        let mut fetch_info_map: std::collections::HashMap<MerkleHash, Vec<FetchInfoIntermediate>> = std::collections::HashMap::new();

        let mut cumulative_bytes = 0u64;
        let mut first_found = false;
        let mut first_chunk_byte_start = 0u64;
        let mut terms = Vec::new();

        for segment in &file_info.segments {
            let seg_unpacked_len = segment.unpacked_segment_bytes as u64;
            
            // Intersection check: segment covers [cumulative_bytes, cumulative_bytes + seg_unpacked_len)
            if cumulative_bytes + seg_unpacked_len > file_range_start && cumulative_bytes < file_range_end {
                
                if !first_found {
                    first_chunk_byte_start = cumulative_bytes;
                    first_found = true;
                }

                let xorb_footer_opt = xorb_footers.get(&segment.xorb_hash).and_then(|f| f.as_ref());
                
                let chunk_range_start = segment.chunk_index_start;
                let chunk_range_end = segment.chunk_index_end;
                
                let mut unpacked_len = segment.unpacked_segment_bytes;
                
                if let Some((_f_hashes, f_boundaries, f_unpacked)) = xorb_footer_opt {
                    let start_unpacked = if chunk_range_start == 0 { 0 } else { f_unpacked[(chunk_range_start - 1) as usize] };
                    let end_unpacked = if chunk_range_end == 0 { 0 } else { f_unpacked[(chunk_range_end - 1) as usize] };
                    unpacked_len = (end_unpacked - start_unpacked) as u32;

                    let start_byte = if chunk_range_start == 0 { 0 } else { f_boundaries[(chunk_range_start - 1) as usize] };
                    let end_byte = if chunk_range_end == 0 { 0 } else { f_boundaries[(chunk_range_end - 1) as usize] };

                    fetch_info_map
                        .entry(segment.xorb_hash)
                        .or_default()
                        .push(FetchInfoIntermediate {
                            chunk_range_start,
                            chunk_range_end,
                            byte_range_start: start_byte as u64,
                            byte_range_end: end_byte as u64,
                        });
                }

                let term_dict = PyDict::new(py);
                term_dict.set_item("hash", segment.xorb_hash.hex())?;
                term_dict.set_item("unpacked_length", unpacked_len)?;
                let range_dict = PyDict::new(py);
                range_dict.set_item("start", chunk_range_start)?;
                range_dict.set_item("end", chunk_range_end)?;
                term_dict.set_item("range", range_dict)?;
                terms.push(term_dict);
            }
            
            cumulative_bytes += seg_unpacked_len;
        }

        let py_terms = PyList::empty(py);
        for term in terms {
            py_terms.append(term)?;
        }

        let py_fetch_info = PyDict::new(py);
        for (hash, mut fi_vec) in fetch_info_map {
            if fi_vec.is_empty() {
                continue;
            }
            
            let final_entries = if coalesce {
                // Sort by chunk range start
                fi_vec.sort_by_key(|fi| fi.chunk_range_start);
                
                // Coalesce adjacent/overlapping entries
                let mut coalesced: Vec<FetchInfoIntermediate> = Vec::new();
                for fi in fi_vec {
                    if coalesced.is_empty() {
                        coalesced.push(fi);
                    } else {
                        let last = coalesced.last_mut().unwrap();
                        if last.chunk_range_end >= fi.chunk_range_start {
                            last.chunk_range_end = last.chunk_range_end.max(fi.chunk_range_end);
                            last.byte_range_end = last.byte_range_end.max(fi.byte_range_end);
                        } else {
                            coalesced.push(fi);
                        }
                    }
                }
                coalesced
            } else {
                fi_vec
            };

            let list = PyList::empty(py);
            for fi in final_entries {
                let fi_dict = PyDict::new(py);
                let cr_dict = PyDict::new(py);
                cr_dict.set_item("start", fi.chunk_range_start)?;
                cr_dict.set_item("end", fi.chunk_range_end)?;
                fi_dict.set_item("range", cr_dict)?;
                
                let ur_dict = PyDict::new(py);
                ur_dict.set_item("start", fi.byte_range_start)?;
                ur_dict.set_item("end", fi.byte_range_end.saturating_sub(1))?; // HttpRange is inclusive end
                fi_dict.set_item("url_range", ur_dict)?;
                
                fi_dict.set_item("url", format!("s3://{}/xorbs/{}", "BUCKET", hash.hex()))?;
                list.append(fi_dict)?;
            }
            py_fetch_info.set_item(hash.hex(), list)?;
        }

        let result = PyDict::new(py);
        result.set_item("offset_into_first_range", file_range_start - first_chunk_byte_start)?;
        result.set_item("terms", py_terms)?;
        result.set_item("fetch_info", py_fetch_info)?;

        Ok(Some(result.into()))
    }
}


#[pyfunction]
#[allow(unsafe_op_in_unsafe_fn)]
pub fn merge_shards(
    py: Python<'_>,
    shard_list: Vec<Vec<u8>>,
    target_max_size: u64,
) -> PyResult<Py<PyList>> {
    if shard_list.is_empty() {
        return Ok(PyList::empty(py).into_py(py));
    }

    use mdb_shard::metadata_shard::shard_in_memory::MDBInMemoryShard;

    let dest_shards = PyList::empty(py);
    let mut current_shard = MDBInMemoryShard::default();

    for data in shard_list {
        let parsed_shard = bytes_to_in_memory_shard(&data)?;

        if current_shard.is_empty() {
            current_shard = parsed_shard;
        } else {
            let candidate = current_shard.union(&parsed_shard)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Union error: {e:?}")))?;

            if candidate.shard_file_size() <= target_max_size {
                current_shard = candidate;
            } else {
                let out_bytes = current_shard.to_bytes()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialize error: {e:?}")))?;
                dest_shards.append(PyBytes::new(py, &out_bytes))?;
                current_shard = parsed_shard;
            }
        }
    }

    if !current_shard.is_empty() {
        let out_bytes = current_shard.to_bytes()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialize error: {e:?}")))?;
        dest_shards.append(PyBytes::new(py, &out_bytes))?;
    }

    Ok(dest_shards.into_py(py))
}

#[pyfunction]
pub fn add_footer_to_xorb(py: Python<'_>, xorb_bytes: &[u8]) -> PyResult<PyObject> {
    let mut output = Vec::new();
    match reconstruct_xorb_with_footer(&mut output, xorb_bytes) {
        Ok(_) => {
            let bytes = pyo3::types::PyBytes::new(py, &output);
            Ok(bytes.into())
        },
        Err(e) => {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to reconstruct xorb with footer: {:?}", e)))
        }
    }
}

fn bytes_to_in_memory_shard(shard_bytes: &[u8]) -> PyResult<mdb_shard::metadata_shard::shard_in_memory::MDBInMemoryShard> {
    use mdb_shard::metadata_shard::shard_in_memory::MDBInMemoryShard;
    use mdb_shard::metadata_shard::file_structs::MDBFileInfo;
    use mdb_shard::metadata_shard::xorb_structs::MDBXorbInfo;

    let mut cursor = Cursor::new(shard_bytes);
    let minimal_shard = MDBMinimalShard::from_reader(&mut cursor, true, true)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to parse minimal shard: {e:?}")))?;
    
    let mut in_memory_shard = MDBInMemoryShard::default();
    for i in 0..minimal_shard.num_files() {
        if let Some(file_view) = minimal_shard.file(i) {
            in_memory_shard.add_file_reconstruction_info(MDBFileInfo::from(file_view))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to add file info: {e:?}")))?;
        }
    }
    for i in 0..minimal_shard.num_xorb() {
        if let Some(xorb_view) = minimal_shard.xorb(i) {
            in_memory_shard.add_xorb_block(MDBXorbInfo::from(xorb_view))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to add xorb info: {e:?}")))?;
        }
    }
    
    Ok(in_memory_shard)
}

#[pyfunction]
pub fn compute_shard_hash(shard_bytes: &[u8]) -> String {
    compute_data_hash(shard_bytes).hex()
}

#[pyfunction]
pub fn reconstruct_shard(py: Python<'_>, shard_bytes: &[u8]) -> PyResult<PyObject> {
    let header = MDBShardFileHeader::deserialize(&mut Cursor::new(shard_bytes))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to parse header: {e:?}")))?;

    if header.footer_size == 0 {
        let in_memory_shard = bytes_to_in_memory_shard(shard_bytes)?;
        let reconstructed = in_memory_shard.to_bytes()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to serialize in-memory shard: {e:?}")))?;
        let bytes = pyo3::types::PyBytes::new(py, &reconstructed);
        Ok(bytes.into())
    } else {
        let bytes = pyo3::types::PyBytes::new(py, shard_bytes);
        Ok(bytes.into())
    }
}

#[pyfunction]
pub fn get_gc_transaction_info(py: Python<'_>) -> PyResult<Py<PyDict>> {
    blender::_get_gc_transaction_info(py)
}

#[pymodule]
fn xet_shard_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ShardIndex>()?;
    m.add_function(wrap_pyfunction!(compute_shard_hash, m)?)?;
    m.add_function(wrap_pyfunction!(merge_shards, m)?)?;
    m.add_function(wrap_pyfunction!(add_footer_to_xorb, m)?)?;
    m.add_function(wrap_pyfunction!(reconstruct_shard, m)?)?;
    m.add_function(wrap_pyfunction!(get_gc_transaction_info, m)?)?;

    Ok(())
}

