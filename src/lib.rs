use std::io::{Cursor, Seek};
use std::sync::Arc;

use mdb_shard::metadata_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::metadata_shard::set_operations::shard_set_union;
use mdb_shard::metadata_shard::{MDBShardInfo, MDBShardFileHeader, MDBShardFileFooter};
use mdb_shard::metadata_shard::ShardFileManager;

use mdb_shard::metadata_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::merklehash::{MerkleHash, compute_data_hash};
use mdb_shard::xorb_object::{reconstruct_xorb_with_footer, XorbObject};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::mem::size_of;
use std::mem::swap;
use std::io::Write;
use futures::{StreamExt, TryStreamExt};
use reqwest::Client;

use redb::ReadableTable;
const GLOBAL_DEDUP_TABLE: redb::TableDefinition<&[u8; 32], &[u8; 32]> = redb::TableDefinition::new("global_dedup");

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
}

#[pymethods]
impl ShardIndex {
    #[new]
    pub fn new(cache_dir: String, db_path: String, max_cache_size: Option<u64>) -> PyResult<Self> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create tokio runtime: {e}")))?;
        
        let sfm = rt.block_on(async {
            ShardFileManager::new_in_cache_directory(cache_dir).await
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
        };

        // Trigger an initial refresh with the size limit
        index.refresh(max_cache_size)?;

        Ok(index)
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

    #[pyo3(signature = (file_hash_hex, start_byte=None, end_byte=None, footers=None))]
    pub fn calculate_reconstruction(
        &self,
        py: Python<'_>,
        file_hash_hex: &str,
        start_byte: Option<u64>,
        end_byte: Option<u64>,
        footers: Option<&PyDict>,
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

        self.calculate_reconstruction_internal(py, file_hash_hex, start_byte, end_byte, xorb_footers)
    }

    #[pyo3(signature = (file_hash_hex, start_byte, end_byte, xorb_urls))]
    pub fn calculate_reconstruction_with_urls(
        &self,
        py: Python<'_>,
        file_hash_hex: &str,
        start_byte: Option<u64>,
        end_byte: Option<u64>,
        xorb_urls: &PyDict,
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

        self.calculate_reconstruction_internal(py, file_hash_hex, start_byte, end_byte, xorb_footers)
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
            let mut reader = shard_file.get_reader()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open shard reader: {e:?}")))?;
            
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
}

impl ShardIndex {
    fn calculate_reconstruction_internal(
        &self,
        py: Python<'_>,
        file_hash_hex: &str,
        start_byte: Option<u64>,
        end_byte: Option<u64>,
        xorb_footers: std::collections::HashMap<MerkleHash, Option<(Vec<MerkleHash>, Vec<u32>, Vec<u32>)>>,
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
        for (hash, fi_vec) in fetch_info_map {
            let list = PyList::empty(py);
            for fi in fi_vec {
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

    let mut cur_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut next_data = Vec::<u8>::with_capacity(target_max_size as usize);
    let mut out_data = Vec::<u8>::with_capacity(target_max_size as usize);

    let dest_shards = PyList::empty(py);
    let mut cur_si = MDBShardInfo::default();

    for data in shard_list {
        next_data = data;
        let mut cursor = Cursor::new(&next_data);
        
        // Use MDBShardInfo::load_from_reader to get header and footer metadata.
        // This is required for shard_set_union.
        let shard_info = MDBShardInfo::load_from_reader(&mut cursor)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Parse error: {e:?}")))?;

        if cur_data.is_empty() {
            // Starting from scratch with the first shard
            swap(&mut cur_data, &mut next_data);
            cur_si = shard_info;
        } else if cur_data.len() + next_data.len() - (size_of::<MDBShardFileHeader>() + size_of::<MDBShardFileFooter>())
            <= target_max_size as usize
        {
            // We have enough size capacity to merge this one in.
            out_data.clear();
            cur_si = shard_set_union(
                &cur_si,
                &mut Cursor::new(&cur_data),
                &shard_info,
                &mut Cursor::new(&next_data),
                &mut out_data,
            ).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Merge error: {e:?}")))?;

            // Now swap out the destination data with the current data.
            swap(&mut out_data, &mut cur_data);
        } else {
            // Current buffer is full or would be too large; "flush" it and start new.
            dest_shards.append(PyBytes::new(py, &cur_data))?;

            // Move the loaded data into the current buffer.
            swap(&mut cur_data, &mut next_data);
            cur_si = shard_info;
        }
    }

    // If there is any left over at the end, flush that as well.
    if !cur_data.is_empty() {
        dest_shards.append(PyBytes::new(py, &cur_data))?;
    }

    Ok(dest_shards.into_py(py))
}

#[pyfunction]
pub fn add_footer_to_xorb(py: Python<'_>, xorb_bytes: Vec<u8>) -> PyResult<PyObject> {
    let mut output = Vec::new();
    match reconstruct_xorb_with_footer(&mut output, &xorb_bytes) {
        Ok(_) => {
            let bytes = pyo3::types::PyBytes::new(py, &output);
            Ok(bytes.into())
        },
        Err(e) => {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to reconstruct xorb with footer: {:?}", e)))
        }
    }
}

#[pymodule]
fn xet_shard_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ShardIndex>()?;
    m.add_function(wrap_pyfunction!(merge_shards, m)?)?;
    m.add_function(wrap_pyfunction!(add_footer_to_xorb, m)?)?;
    Ok(())
}
