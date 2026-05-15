use std::io::{Cursor, Seek};
use std::collections::HashSet;
use std::sync::Arc;

use mdb_shard::metadata_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::metadata_shard::set_operations::shard_set_union;
use mdb_shard::metadata_shard::{MDBShardInfo, MDBShardFileHeader, MDBShardFileFooter};
use mdb_shard::metadata_shard::ShardFileManager;

use mdb_shard::metadata_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::merklehash::{MerkleHash, file_hash, compute_data_hash};
use mdb_shard::xorb_object::{reconstruct_xorb_with_footer, error::XorbObjectError};
use xet_data::deduplication::Chunker;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::mem::size_of;
use std::mem::swap;

use redb::ReadableTable;
const GLOBAL_DEDUP_TABLE: redb::TableDefinition<&[u8; 32], &[u8; 32]> = redb::TableDefinition::new("global_dedup");


#[pyclass]
pub struct ShardIndex {
    sfm: Arc<ShardFileManager>,
    rt: tokio::runtime::Runtime,
    db: Arc<redb::Database>,
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

        let index = ShardIndex {
            sfm,
            rt,
            db: Arc::new(db),
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
        let h = MerkleHash::from_hex(file_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;

        let res = self.rt.block_on(async {
            self.sfm.get_file_reconstruction_info(&h).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Query failed: {e:?}")))?;

        let (file_info, _) = match res {
            Some(r) => r,
            None => return Ok(None),
        };

        // Parse footers
        let mut xorb_footers = std::collections::HashMap::new();
        
        let footers = footers.unwrap_or_else(|| PyDict::new(py));
        for (k, v) in footers.iter() {
            let hash_hex: String = k.extract()?;
            let bytes: &[u8] = v.extract()?;
            
            let hash = MerkleHash::from_hex(&hash_hex)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid footer hex {}: {:?}", hash_hex, e)))?;

            let mut cursor = Cursor::new(bytes);
            use mdb_shard::xorb_object::XorbObjectInfoV1;
            let footer = match XorbObjectInfoV1::deserialize_only_boundaries_section(&mut cursor) {
                Ok((f, _)) => Some(f),
                Err(_) => {
                    // Footer might be missing or corrupted (e.g. WASM upload). 
                    // We will fall back to full file scanning.
                    None
                }
            };
            
            xorb_footers.insert(hash, footer);
        }

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

        let mut s_idx = 0;
        let mut cumulative_bytes = 0u64;
        let mut first_chunk_byte_start = 0u64;

        loop {
            if s_idx >= file_info.segments.len() {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid range"));
            }

            let n = file_info.segments[s_idx].unpacked_segment_bytes as u64;
            if cumulative_bytes + n > file_range_start {
                first_chunk_byte_start = cumulative_bytes;
                break;
            } else {
                cumulative_bytes += n;
                s_idx += 1;
            }
        }

        let mut terms = Vec::new();

        #[derive(Clone)]
        struct FetchInfoIntermediate {
            chunk_range_start: u32,
            chunk_range_end: u32,
            byte_range_start: u64,
            byte_range_end: u64,
        }

        let mut fetch_info_map: std::collections::HashMap<MerkleHash, Vec<FetchInfoIntermediate>> = std::collections::HashMap::new();

        while s_idx < file_info.segments.len() && cumulative_bytes < file_range_end {
            let mut segment = file_info.segments[s_idx].clone();
            let mut chunk_range_start = segment.chunk_index_start;
            let mut chunk_range_end = segment.chunk_index_end;

            let xorb_footer_opt = xorb_footers.get(&segment.xorb_hash).cloned().flatten();

            let has_footer = xorb_footer_opt.is_some();

            let get_chunk_length = |idx: u32| -> PyResult<u32> {
                if let Some(ref footer) = xorb_footer_opt {
                    if idx == 0 {
                        Ok(footer.unpacked_chunk_offsets[0])
                    } else if (idx as usize) < footer.unpacked_chunk_offsets.len() {
                        Ok(footer.unpacked_chunk_offsets[idx as usize] - footer.unpacked_chunk_offsets[(idx - 1) as usize])
                    } else {
                        Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Chunk index out of bounds in unpacked_chunk_offsets"))
                    }
                } else {
                    // Without footer, we can't prune individual chunks. 
                    // This branch shouldn't be hit because we don't prune if footer is missing.
                    Ok(0)
                }
            };

            if has_footer {
                if cumulative_bytes < file_range_start {
                    while chunk_range_start < chunk_range_end {
                        let next_chunk_size = get_chunk_length(chunk_range_start)? as u64;
                        if cumulative_bytes + next_chunk_size <= file_range_start {
                            cumulative_bytes += next_chunk_size;
                            first_chunk_byte_start += next_chunk_size;
                            segment.unpacked_segment_bytes -= next_chunk_size as u32;
                            chunk_range_start += 1;
                        } else {
                            break;
                        }
                    }
                }

                if cumulative_bytes + segment.unpacked_segment_bytes as u64 > file_range_end {
                    while chunk_range_end > chunk_range_start {
                        let last_chunk_size = get_chunk_length(chunk_range_end - 1)? as u64;
                        if cumulative_bytes + (segment.unpacked_segment_bytes - last_chunk_size as u32) as u64 >= file_range_end {
                            chunk_range_end -= 1;
                            segment.unpacked_segment_bytes -= last_chunk_size as u32;
                        } else {
                            break;
                        }
                    }
                }
            }

            if let Some(ref footer) = xorb_footer_opt {
                let start_byte = if chunk_range_start == 0 { 0 } else { footer.chunk_boundary_offsets[(chunk_range_start - 1) as usize] };
                let end_byte = if chunk_range_end == 0 { 0 } else { footer.chunk_boundary_offsets[(chunk_range_end - 1) as usize] };

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
            term_dict.set_item("unpacked_length", segment.unpacked_segment_bytes)?;
            let range_dict = PyDict::new(py);
            range_dict.set_item("start", chunk_range_start)?;
            range_dict.set_item("end", chunk_range_end)?;
            term_dict.set_item("range", range_dict)?;
            terms.push(term_dict);

            cumulative_bytes += segment.unpacked_segment_bytes as u64;
            s_idx += 1;
        }

        let py_terms = PyList::empty(py);
        for term in terms {
            py_terms.append(term)?;
        }

        let py_fetch_info = PyDict::new(py);
        for (hash, mut fi_vec) in fetch_info_map {
            fi_vec.sort_by_key(|fi| fi.chunk_range_start);
            
            let merged_list = PyList::empty(py);
            let mut idx = 0;
            while idx < fi_vec.len() {
                let mut new_fi = fi_vec[idx].clone();
                while idx + 1 < fi_vec.len() {
                    let next_fi = &fi_vec[idx + 1];
                    if next_fi.chunk_range_start <= new_fi.chunk_range_end {
                        new_fi.chunk_range_end = next_fi.chunk_range_end.max(new_fi.chunk_range_end);
                        new_fi.byte_range_end = next_fi.byte_range_end.max(new_fi.byte_range_end);
                        idx += 1;
                    } else {
                        break;
                    }
                }
                
                let fi_dict = PyDict::new(py);
                let cr_dict = PyDict::new(py);
                cr_dict.set_item("start", new_fi.chunk_range_start)?;
                cr_dict.set_item("end", new_fi.chunk_range_end)?;
                fi_dict.set_item("range", cr_dict)?;
                
                let ur_dict = PyDict::new(py);
                ur_dict.set_item("start", new_fi.byte_range_start)?;
                ur_dict.set_item("end", new_fi.byte_range_end.saturating_sub(1))?; // HttpRange is inclusive end
                fi_dict.set_item("url_range", ur_dict)?;
                
                fi_dict.set_item("url", format!("s3://{}/xorbs/{}", "BUCKET", hash.hex()))?;
                merged_list.append(fi_dict)?;
                idx += 1;
            }
            py_fetch_info.set_item(hash.hex(), merged_list)?;
        }

        let result = PyDict::new(py);
        result.set_item("offset_into_first_range", file_range_start - first_chunk_byte_start)?;
        result.set_item("terms", py_terms)?;
        result.set_item("fetch_info", py_fetch_info)?;

        Ok(Some(result.into()))
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
}

#[pyfunction]
#[allow(unsafe_op_in_unsafe_fn)]
pub fn extract_shard_metadata(
    py: Python<'_>,
    shard_bytes: &[u8],
) -> PyResult<Py<PyDict>> {
    // Parse shard (files AND XORB info)
    let mut cursor = Cursor::new(shard_bytes);
    let shard = MDBMinimalShard::from_reader(&mut cursor, true, true)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Parse error: {e:?}")))?;

    let root_dict = PyDict::new(py);

    // 1. Files Index
    let file_list = PyList::empty(py);
    for i in 0..shard.num_files() {
        if let Some(fiv) = shard.file(i) {
            let file_dict = PyDict::new(py);
            let h: MerkleHash = fiv.file_hash();
            file_dict.set_item("file_hash", h.hex())?;
            
            let segments = PyList::empty(py);
            for j in 0..fiv.num_entries() {
                let seg = fiv.entry(j);
                let seg_dict = PyDict::new(py);
                let xh: MerkleHash = seg.xorb_hash;
                seg_dict.set_item("h", xh.hex())?;
                seg_dict.set_item("s", seg.chunk_index_start)?;
                seg_dict.set_item("e", seg.chunk_index_end)?;
                seg_dict.set_item("l", seg.unpacked_segment_bytes)?;
                segments.append(seg_dict)?;
            }
            file_dict.set_item("segments", segments)?;
            file_list.append(file_dict)?;
        }
    }
    root_dict.set_item("files", file_list)?;

    // 2. XORB Index & 3. Global Dedup Chunks
    let xorb_list = PyList::empty(py);
    let mut eligible_chunks = HashSet::new();

    for i in 0..shard.num_xorb() {
        if let Some(xiv) = shard.xorb(i) {
            let xorb_dict = PyDict::new(py);
            let xh: MerkleHash = xiv.xorb_hash();
            xorb_dict.set_item("xorb_hash", xh.hex())?;
            
            let layout = PyList::empty(py);
            for j in 0..xiv.num_entries() {
                let chunk = xiv.chunk(j);
                
                // Add to XORB layout: [hash, offset, length]
                let entry = PyList::empty(py);
                let ch: MerkleHash = chunk.chunk_hash;
                entry.append(ch.hex())?;
                entry.append(chunk.chunk_byte_range_start)?;
                entry.append(chunk.unpacked_segment_bytes)?;
                layout.append(entry)?;

                // Collect global dedup eligible chunks
                if chunk.is_global_dedup_eligible() {
                    let ch: MerkleHash = chunk.chunk_hash;
                    eligible_chunks.insert(ch);
                }
            }
            xorb_dict.set_item("chunk_layout", layout)?;
            xorb_list.append(xorb_dict)?;
        }
    }
    root_dict.set_item("xorbs", xorb_list)?;

    // 4. Flattened Eligible Chunks
    let chunk_list = PyList::empty(py);
    for hash in eligible_chunks {
        let ch: MerkleHash = hash;
        chunk_list.append(ch.hex())?;
    }
    root_dict.set_item("eligible_chunks", chunk_list)?;

    Ok(root_dict.into_py(py))
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
#[allow(unsafe_op_in_unsafe_fn)]
pub fn create_shard(
    py: Python<'_>,
    xorb_hash_hex: &str,
    total_size: u32,
    chunk_layout: Vec<(&str, u32, u32)>, 
) -> PyResult<Py<PyBytes>> {
    use mdb_shard::merklehash::MerkleHash;
    use mdb_shard::metadata_shard::xorb_structs::{MDBXorbInfo, XorbChunkSequenceHeader, XorbChunkSequenceEntry};
    use mdb_shard::metadata_shard::shard_in_memory::MDBInMemoryShard;

    let xorb_hash = MerkleHash::from_hex(xorb_hash_hex)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Bad XORB Hash: {:?}", e)))?;

    let mut chunks = Vec::with_capacity(chunk_layout.len());
    for (c_hash_hex, offset, length) in chunk_layout {
        let chunk_hash = MerkleHash::from_hex(c_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Bad Chunk Hash: {:?}", e)))?;
        let entry = XorbChunkSequenceEntry::new(chunk_hash, length, offset);
        chunks.push(entry);
    }

    let header = XorbChunkSequenceHeader::new(xorb_hash, chunks.len() as u32, total_size);
    let xorb_info = std::sync::Arc::new(MDBXorbInfo { metadata: header, chunks });

    let mut shard = MDBInMemoryShard::default();
    shard.add_xorb_block(xorb_info)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to add XORB: {:?}", e)))?;
    
    let shard_bytes = shard.to_bytes()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to serialize shard: {:?}", e)))?;

    Ok(PyBytes::new(py, &shard_bytes).into_py(py))
}

#[pyfunction]
#[allow(unsafe_op_in_unsafe_fn)]
pub fn calculate_file_hash(data: &[u8]) -> PyResult<String> {
    let mut chunker = Chunker::default();
    let chunks = chunker.next_block(data, true);
    
    let mut chunk_hashes = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        chunk_hashes.push((chunk.hash, chunk.data.len() as u64));
    }
    
    let hash = file_hash(chunk_hashes.as_slice());
    Ok(hash.hex())
}

#[pymodule]
fn xet_shard_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ShardIndex>()?;
    m.add_function(wrap_pyfunction!(extract_shard_metadata, m)?)?;
    m.add_function(wrap_pyfunction!(merge_shards, m)?)?;
    m.add_function(wrap_pyfunction!(create_shard, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_file_hash, m)?)?;
    m.add_function(wrap_pyfunction!(add_footer_to_xorb, m)?)?;
    Ok(())
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
