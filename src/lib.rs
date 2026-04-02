use std::io::Cursor;
use std::collections::HashSet;

use mdb_shard::metadata_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::merklehash::MerkleHash;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

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
                
                // Add to XORB layout: [offset, length]
                let entry = PyList::empty(py);
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

#[pymodule]
fn xet_shard_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(extract_shard_metadata, m)?)?;
    Ok(())
}