use std::io::Cursor;

use mdb_shard::streaming_shard::MDBMinimalShard;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

#[pyfunction]
#[allow(unsafe_op_in_unsafe_fn)]
pub fn extract_file_reconstruction_info(
    py: Python<'_>,
    shard_bytes: &[u8],
) -> PyResult<Vec<(String, Py<PyDict>)>> {
    // Parse shard (files only, no CAS)
    let mut cursor = Cursor::new(shard_bytes);
    let shard = MDBMinimalShard::from_reader(&mut cursor, true, false)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Parse error: {e:?}")))?;

    let mut results = Vec::new();

    for i in 0..shard.num_files() {
        if let Some(fiv) = shard.file(i) {
            let file_hash = fiv.file_hash().to_string();

            // Dict for the file
            let dict = PyDict::new(py);
            dict.set_item("file_hash", &file_hash)?;
            dict.set_item("num_entries", fiv.num_entries())?;

            // Segments
            let segs = PyList::empty(py);
            for j in 0..fiv.num_entries() {
                let seg = fiv.entry(j);

                let seg_dict = PyDict::new(py);
                seg_dict.set_item("hash", seg.cas_hash.to_string())?;
                seg_dict.set_item("unpacked_length", seg.unpacked_segment_bytes)?;
                seg_dict.set_item("start", seg.chunk_index_start)?;
                seg_dict.set_item("end", seg.chunk_index_end)?;

                segs.append(seg_dict)?;
            }
            dict.set_item("segments", segs)?;

            results.push((file_hash, dict.into_py(py)));
        }
    }

    Ok(results)
}

#[pymodule]
fn xet_shard_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(extract_file_reconstruction_info, m)?)?;
    Ok(())
}


// In Python
// from xet_shard_parser import extract_file_reconstruction_info

// files = extract_file_reconstruction_info(open("shard.dat", "rb").read())
// print(files)

// [
//   (
//     "abcd1234...", 
//     {
//       "file_hash": "abcd1234...",
//       "num_entries": 2,
//       "segments": [
//          {"hash": "xxxx", "unpacked_length": 1024, "start": 0, "end": 1024},
//          {"hash": "yyyy", "unpacked_length": 2048, "start": 1024, "end": 3072}
//       ]
//     }
//   ),
//   ...
// ]

// reconstruction response should be :

// {
//     file_hash: file_hash.
//     terms: [
//         {
//             hash: chunk_hash,
//             unpacked_length: unpacked_segment_bytes,
//             start: range_start,
//             end: range_end
//         },
//         ...
//     ]
// }