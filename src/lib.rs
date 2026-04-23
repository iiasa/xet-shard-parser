use std::io::{Cursor, Read, Seek, SeekFrom};
use std::collections::HashSet;
use std::sync::Arc;

use mdb_shard::metadata_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::metadata_shard::set_operations::shard_set_union;
use mdb_shard::metadata_shard::{MDBShardInfo, MDBShardFileHeader, MDBShardFileFooter};
use mdb_shard::metadata_shard::ShardFileManager;
use mdb_shard::metadata_shard::file_structs::MDBFileInfo;
use mdb_shard::metadata_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::merklehash::{MerkleHash, file_hash, compute_data_hash};
use xet_data::deduplication::Chunker;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyType};
use std::mem::size_of;
use std::mem::swap;

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
            let mut table = write_txn.open_table(GLOBAL_DEDUP_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
            
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

    pub fn register_shard(&self, shard_bytes: &[u8]) -> PyResult<()> {
        // 1. Register with ShardFileManager (persists .sib to disk and indexes in memory)
        self.rt.block_on(async {
            self.sfm.import_shard_from_bytes(shard_bytes).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to import shard: {e:?}")))?;

        // 2. Index global deduplication chunks in redb
        let mut cursor = Cursor::new(shard_bytes);
        let shard = MDBMinimalShard::from_reader(&mut cursor, false, true)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Parse error: {e:?}")))?;

        let shard_hash = compute_data_hash(shard_bytes);
        let shard_hash_bytes: [u8; 32] = shard_hash.into();

        let write_txn = self.db.begin_write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn failed: {e}")))?;
        {
            let mut table = write_txn.open_table(GLOBAL_DEDUP_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
            
            for i in 0..shard.num_xorb() {
                if let Some(xiv) = shard.xorb(i) {
                    for j in 0..xiv.num_entries() {
                        let chunk = xiv.chunk(j);
                        if chunk.is_global_dedup_eligible() {
                            let chunk_hash_bytes: [u8; 32] = chunk.chunk_hash.into();
                            table.insert(&chunk_hash_bytes, &shard_hash_bytes)
                                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Insert failed: {e}")))?;
                        }
                    }
                }
            }
        }
        write_txn.commit()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit failed: {e}")))?;

        Ok(())
    }

    pub fn get_reconstruction(&self, py: Python<'_>, file_hash_hex: &str) -> PyResult<Option<Py<PyDict>>> {
        let h = MerkleHash::from_hex(file_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;

        let res = self.rt.block_on(async {
            self.sfm.get_file_reconstruction_info(&h).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Query failed: {e:?}")))?;

        if let Some((file_info, _shard_hash)) = res {
            let dict = PyDict::new(py);
            let segments = PyList::empty(py);
            for seg in file_info.segments {
                let seg_dict = PyDict::new(py);
                seg_dict.set_item("h", seg.xorb_hash.hex())?;
                seg_dict.set_item("s", seg.chunk_index_start)?;
                seg_dict.set_item("e", seg.chunk_index_end)?;
                seg_dict.set_item("l", seg.unpacked_segment_bytes)?;
                segments.append(seg_dict)?;
            }
            dict.set_item("segments", segments)?;
            return Ok(Some(dict.into()));
        }

        Ok(None)
    }

    pub fn get_chunk_shard(&self, chunk_hash_hex: &str) -> PyResult<Option<String>> {
        let h = MerkleHash::from_hex(chunk_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        let read_txn = self.db.begin_read()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
        let table = read_txn.open_table(GLOBAL_DEDUP_TABLE)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
        
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

    let mut dest_shards = PyList::empty(py);
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
    
    let hash = file_hash(&chunk_hashes);
    Ok(hash.hex())
}

#[pymodule]
fn xet_shard_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ShardIndex>()?;
    m.add_function(wrap_pyfunction!(extract_shard_metadata, m)?)?;
    m.add_function(wrap_pyfunction!(merge_shards, m)?)?;
    m.add_function(wrap_pyfunction!(create_shard, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_file_hash, m)?)?;
    Ok(())
}