use std::io::{Cursor, Seek};
use std::sync::Arc;

use mdb_shard::metadata_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::metadata_shard::MDBShardFileHeader;
use mdb_shard::metadata_shard::{ChunkIndexSizeEnforcement, ShardFileManager};

use mdb_shard::metadata_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::merklehash::{MerkleHash, compute_data_hash};
use mdb_shard::xorb_object::{reconstruct_xorb_with_footer, XorbObject};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

mod blender;
pub mod s3_client;
mod verify;
use futures::StreamExt;
use reqwest::Client;

use crate::s3_client::{
    reqwest_download_footer_with_retry,
    reqwest_download_and_decompress_range_with_retry,
    MAX_S3_ATTEMPTS,
    MAX_CONCURRENCY,
};

const GLOBAL_DEDUP_TABLE: redb::TableDefinition<&[u8; 32], &[u8; 32]> = redb::TableDefinition::new("global_dedup");
const GC_LIVE_CHUNKS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_chunks");
const GC_PRIMARY_XORB_TABLE: redb::TableDefinition<&[u8; 32], &[u8; 32]> = redb::TableDefinition::new("gc_primary_xorb");
const GC_XORB_UTILIZATION_TABLE: redb::TableDefinition<&[u8; 32], &str> = redb::TableDefinition::new("gc_xorb_utilization");
const GC_SPARSE_XORBS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_sparse_xorbs");
const GC_LIVE_FILES_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_files");
const GC_LIVE_XORBS_TABLE: redb::TableDefinition<&[u8; 32], ()> = redb::TableDefinition::new("gc_live_xorbs");
const GC_XORB_CHUNKS_TABLE: redb::TableDefinition<&[u8; 32], &[u8]> = redb::TableDefinition::new("gc_xorb_chunks");

fn parse_xorb_footer_data(bytes: &[u8]) -> Option<(Vec<MerkleHash>, Vec<u32>, Vec<u32>)> {
    let mut reader = Cursor::new(bytes);
    let xorb_obj = XorbObject::deserialize(&mut reader).ok()?;
    let info = xorb_obj.info;
    Some((info.chunk_hashes, info.chunk_boundary_offsets, info.unpacked_chunk_offsets))
}

/// Shard-index lookups in flight during the GC liveness check (spread over the runtime's
/// worker threads), and how many files are looked up per group.
const LIVENESS_LOOKUP_CONCURRENCY: usize = 64;
const LIVENESS_LOOKUP_GROUP: usize = 512;
/// Live chunks buffered per GC DB commit in the liveness check (~64 bytes each).
const LIVENESS_FLUSH_CHUNKS: usize = 250_000;

/// Where a file's entry can be read from, for the liveness check.
enum GcFileLoc {
    /// Every entry for this file (in one shard or several) has the same segments, so reading
    /// this shard gives the liveness check exactly what the shard manager's lookup would.
    Uniform(Arc<mdb_shard::metadata_shard::MDBShardFile>),
    /// Entries in different shards disagree: the shard manager's own order decides, so the
    /// full lookup is used.
    Conflicting,
}

/// Which shard to read each file entry from, built by `init_gc` for the liveness check.
///
/// The shard manager answers a file lookup by opening shard after shard until one has the
/// file, which costs one file open per registered shard for every file. This map turns that
/// into one open per file, without changing any result:
/// - entries whose segments agree across all shards are read from one of those shards;
/// - conflicting entries still use the shard manager's lookup;
/// - a file in no registered shard is reported as not found, which is what the shard
///   manager's search over the same shards returns (this crate never adds in-memory entries).
struct GcFileShardMap {
    /// Shards registered when the map was built; it is only used while this is unchanged.
    shard_hashes: std::collections::HashSet<[u8; 32]>,
    files: std::collections::HashMap<[u8; 32], GcFileLoc>,
}

/// Hash of the parts of a file entry the liveness check uses (and the rest of each segment).
fn segments_hash(file_info: &mdb_shard::metadata_shard::file_structs::MDBFileInfo) -> MerkleHash {
    let mut buf = Vec::with_capacity(file_info.segments.len() * 48);
    for seg in &file_info.segments {
        buf.extend_from_slice(seg.xorb_hash.as_bytes());
        buf.extend_from_slice(&seg.xorb_flags.to_le_bytes());
        buf.extend_from_slice(&seg.unpacked_segment_bytes.to_le_bytes());
        buf.extend_from_slice(&seg.chunk_index_start.to_le_bytes());
        buf.extend_from_slice(&seg.chunk_index_end.to_le_bytes());
    }
    compute_data_hash(&buf)
}

#[pyclass]
pub struct ShardIndex {
    sfm: Arc<ShardFileManager>,
    rt: tokio::runtime::Runtime,
    db: Arc<redb::Database>,
    client: Client,
    gc_db: Arc<std::sync::RwLock<Option<redb::Database>>>,
    gc_db_path: Arc<std::sync::RwLock<Option<std::path::PathBuf>>>,
    gc_file_shards: Arc<std::sync::RwLock<Option<Arc<GcFileShardMap>>>>,
}

#[pymethods]
impl ShardIndex {
    #[new]
    pub fn new(py: Python<'_>, cache_dir: String, db_path: String, max_cache_size: Option<u64>) -> PyResult<Self> {
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
            .pool_idle_timeout(std::time::Duration::from_secs(15))
            .pool_max_idle_per_host(MAX_CONCURRENCY)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to build client: {e}")))?;

        let index = ShardIndex {
            sfm,
            rt,
            db: Arc::new(db),
            client,
            gc_db: Arc::new(std::sync::RwLock::new(None)),
            gc_db_path: Arc::new(std::sync::RwLock::new(None)),
            gc_file_shards: Arc::new(std::sync::RwLock::new(None)),
        };

        // Trigger an initial refresh with the size limit
        index.refresh(py, max_cache_size)?;

        Ok(index)
    }

    pub fn get_all_shard_xorbs<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let sfm = self.sfm.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let shards = sfm.registered_shard_list().await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;

            Python::with_gil(|py| {
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
                let obj: PyObject = dict.into();
                Ok(obj)
            })
        })
    }

    #[pyo3(signature = (file_hash_hex))]
    pub fn get_file_size<'a>(&self, py: Python<'a>, file_hash_hex: String) -> PyResult<&'a PyAny> {
        let h = MerkleHash::from_hex(&file_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let sfm = self.sfm.clone();
        
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let res = sfm.get_file_reconstruction_info(&h).await;
            if let Ok(Some((file_info, _))) = res {
                Ok::<_, pyo3::PyErr>(Some(file_info.file_size() as u64))
            } else {
                Ok::<_, pyo3::PyErr>(None)
            }
        })
    }

    #[pyo3(signature = (xorb_hashes))]
    pub fn get_shards_for_xorbs<'a>(&self, py: Python<'a>, xorb_hashes: Vec<String>) -> PyResult<&'a PyAny> {
        let mut target_xorbs: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();
        for hex in xorb_hashes {
            if let Ok(mh) = MerkleHash::from_hex(&hex) {
                target_xorbs.insert(mh.into());
            }
        }
        let sfm = self.sfm.clone();
        
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let shards = sfm.registered_shard_list().await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;

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

            Ok::<_, pyo3::PyErr>(matching_shards)
        })
    }



    /// Starts a GC transaction.
    ///
    /// `started_at` is the unix time start began (before the liveness check). Xorbs whose S3
    /// LastModified is less than `min_age_days` before `started_at` are never removed or
    /// repacked by this run. `drop_entries`: (file hash hex, shard hash hex) of file entries to
    /// leave out of the rewritten shards (from `audit --deep`); their shards are always rewritten.
    #[pyo3(signature = (started_at, min_age_days=28.0, drop_entries=Vec::new()))]
    pub fn start_gc_transaction(&self, py: Python<'_>, started_at: u64, min_age_days: f64, drop_entries: Vec<(String, String)>) -> PyResult<bool> {
        let mut drop_set: std::collections::HashSet<([u8; 32], String)> = std::collections::HashSet::new();
        for (file_hex, shard_hex) in drop_entries {
            let h = MerkleHash::from_hex(&file_hex)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid file hash {file_hex}: {e:?}")))?;
            drop_set.insert((h.into(), shard_hex));
        }
        let has_sparse = py.allow_threads(|| {
            let gc_db_lock = self.gc_db.read().unwrap();
            if gc_db_lock.is_none() {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized"));
            }
            let db = gc_db_lock.as_ref().unwrap();

            // 0 (or less) turns the age filter off, e.g. for the one-time cleanup. Comparing
            // second-precision timestamps at the boundary would otherwise skip xorbs written in
            // the same second as start.
            if min_age_days > 0.0 {
                let min_age_secs = (min_age_days * 86400.0) as u64;
                let cutoff = started_at.saturating_sub(min_age_secs);
                let removed = blender::_drop_young_from_sparse(db, cutoff)?;
                eprintln!("[GC] Skipping {removed} xorbs written in the last {min_age_days} days.");
            } else {
                eprintln!("[GC] Age filter off (min_age_days = {min_age_days}).");
            }

            let read_txn = db.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
            let sparse_xorbs = read_txn.open_table(blender::GC_SPARSE_XORBS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
            
            if drop_set.is_empty() && sparse_xorbs.is_empty().unwrap_or(true) && !self.any_file_entry_references_missing_xorb(db)? {
                return Ok(false);
            }
            
            drop(read_txn);
            drop(gc_db_lock);
            
            blender::_consolidate_metadata(self.sfm.clone(), self.gc_db.clone(), started_at, &drop_set)?;
            Ok(true)
        })?;
        Ok(has_sparse)
    }

    #[pyo3(signature = ())]
    pub fn stage_gc_transaction(&self, py: Python<'_>) -> PyResult<bool> {
        py.allow_threads(|| {
            blender::_stage_gc_transaction()?;
            Ok::<_, pyo3::PyErr>(())
        })?;
        Ok(true)
    }

    #[pyo3(signature = ())]
    pub fn verify_gc_transaction(&self, py: Python<'_>) -> PyResult<usize> {
        let missing = py.allow_threads(|| {
            blender::_verify_gc_transaction(self.sfm.clone(), self.gc_db.clone())
        })?;
        Ok(missing)
    }

    /// Commits a staged transaction. Call `init_gc` + `build_live_chunks_list` (the liveness
    /// check) first, so the live xorb set reflects the current database.
    /// Returns (moved, kept) old xorb hashes.
    #[pyo3(signature = (min_wait_secs, skip_wait=false))]
    pub fn commit_gc_transaction(&self, py: Python<'_>, min_wait_secs: u64, skip_wait: bool) -> PyResult<(Vec<String>, Vec<String>)> {
        py.allow_threads(|| blender::_commit_gc_transaction(self.gc_db.clone(), min_wait_secs, skip_wait))
    }

    #[pyo3(signature = ())]
    pub fn revert_gc_transaction(&self, py: Python<'_>) -> PyResult<bool> {
        py.allow_threads(|| {
            blender::_revert_gc_transaction()?;
            Ok::<_, pyo3::PyErr>(())
        })?;
        Ok(true)
    }

    #[pyo3(signature = ())]
    pub fn prune_garbage(&self, py: Python<'_>) -> PyResult<bool> {
        py.allow_threads(|| {
            blender::_prune_garbage()?;
            Ok::<_, pyo3::PyErr>(())
        })?;
        Ok(true)
    }

    pub fn prune_shard(&self, py: Python<'_>, shard_hash_hex: &str) -> PyResult<()> {
        let h = MerkleHash::from_hex(shard_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        py.allow_threads(|| {
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
            Ok::<(), PyErr>(())
        })?;

        Ok(())
    }

    pub fn register_shard(&self, py: Python<'_>, shard_bytes: &[u8], shard_hash_hex: Option<String>) -> PyResult<()> {
        py.allow_threads(|| {
            // 1. Register with ShardFileManager (persists .sib to disk and indexes in memory)
            self.rt.block_on(async {
                self.sfm.import_shard_from_bytes(shard_bytes, ChunkIndexSizeEnforcement::StopAtCap).await
            }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to import shard: {e:?}")))?;
            Ok::<(), PyErr>(())
        })?;

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

        py.allow_threads(|| {
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
            Ok::<(), PyErr>(())
        })?;

        Ok(())
    }

    #[pyo3(signature = (file_hash_hex, start_byte=None, end_byte=None, footers=None, coalesce=None))]
    pub fn calculate_reconstruction<'a>(
        &self,
        py: Python<'a>,
        file_hash_hex: String,
        start_byte: Option<u64>,
        end_byte: Option<u64>,
        footers: Option<&PyDict>,
        coalesce: Option<bool>,
    ) -> PyResult<&'a PyAny> {
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

        let h = MerkleHash::from_hex(&file_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
            
        let sfm = self.sfm.clone();
        
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let res = sfm.get_file_reconstruction_info(&h).await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Query failed: {e:?}")))?;
            
            let (file_info, _) = match res {
                Some(r) => r,
                None => return Ok(Python::with_gil(|py| py.None().into_ref(py).to_object(py))),
            };
            
            Python::with_gil(|py| {
                Self::calculate_reconstruction_internal_sync(py, start_byte, end_byte, xorb_footers, coalesce.unwrap_or(true), file_info)
                    .map(|r| match r {
                        Some(dict) => dict.to_object(py),
                        None => py.None().into_ref(py).to_object(py),
                    })
            })
        })
    }

    #[pyo3(signature = (file_hash_hex, start_byte, end_byte, xorb_urls, coalesce=None))]
    pub fn calculate_reconstruction_with_urls<'a>(
        &self,
        py: Python<'a>,
        file_hash_hex: String,
        start_byte: Option<u64>,
        end_byte: Option<u64>,
        xorb_urls: std::collections::HashMap<String, String>,
        coalesce: Option<bool>,
    ) -> PyResult<&'a PyAny> {
        let h = MerkleHash::from_hex(&file_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let sfm = self.sfm.clone();
        let client = self.client.clone();
        let sfm2 = self.sfm.clone(); // For calculate_reconstruction_internal

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let res = sfm.get_file_reconstruction_info(&h).await;

            let (file_info, _) = match res {
                Ok(Some(r)) => r,
                _ => return Ok::<_, pyo3::PyErr>(None),
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

            // Concurrently fetch footers for required XORBs
            let mut xorb_footers = std::collections::HashMap::new();
            let mut fetch_tasks = Vec::new();

            for xh in &needed_xorbs {
                let xh_hex = xh.hex();
                if let Some(url) = xorb_urls.get(&xh_hex) {
                    fetch_tasks.push((xh.clone(), url.clone()));
                } else {
                    xorb_footers.insert(xh.clone(), None);
                }
            }

            if !fetch_tasks.is_empty() {
                let results: Vec<Result<(MerkleHash, Option<Vec<u8>>), String>> = futures::stream::iter(fetch_tasks)
                    .map(|(xh, url)| {
                        let client = client.clone();
                        async move {
                            let xh_hex = xh.hex();
                            let bytes_opt = reqwest_download_footer_with_retry(&client, &url, &xh_hex, MAX_S3_ATTEMPTS).await?;
                            Ok((xh, bytes_opt))
                        }
                    })
                    .buffer_unordered(MAX_CONCURRENCY)
                    .collect()
                    .await;

                for res in results {
                    let (xh, bytes_opt) = res.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
                    let footer = match bytes_opt {
                        Some(bytes) => parse_xorb_footer_data(&bytes),
                        None => None,
                    };
                    xorb_footers.insert(xh, footer);
                }
            }

            // Inline the internal reconstruction logic here to avoid GIL lifetime issues!
            let res2 = sfm2.get_file_reconstruction_info(&h).await;
            let (file_info2, _) = match res2 {
                Ok(Some(r)) => r,
                _ => return Ok::<_, pyo3::PyErr>(None),
            };
            
            Python::with_gil(|py| {
                // Here we just call a purely synchronous helper function without blocking!
                crate::ShardIndex::calculate_reconstruction_internal_sync(py, start_byte, end_byte, xorb_footers, coalesce.unwrap_or(true), file_info2)
            })
        })
    }

    pub fn get_chunk_shard(&self, py: Python<'_>, chunk_hash_hex: &str) -> PyResult<Option<String>> {
        let h = MerkleHash::from_hex(chunk_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        py.allow_threads(|| {
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
        })
    }

    pub fn get_xorb_layout(&self, py: Python<'_>, xorb_hash_hex: &str) -> PyResult<Option<Py<PyList>>> {
        let h = MerkleHash::from_hex(xorb_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;

        let shards = py.allow_threads(|| {
            self.rt.block_on(async {
                self.sfm.registered_shard_list().await
            })
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

    pub fn refresh(&self, py: Python<'_>, max_cache_size: Option<u64>) -> PyResult<()> {
        let prune_size = max_cache_size.unwrap_or(0);
        py.allow_threads(|| {
            self.rt.block_on(async {
                self.sfm.refresh_shard_dir(false, prune_size, ChunkIndexSizeEnforcement::StopAtCap).await
            })
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Refresh failed: {e:?}")))?;
        Ok(())
    }

    #[pyo3(signature = (tasks))]
    pub fn reconstruct_file_parallel<'a>(&self, py: Python<'a>, tasks: &PyList) -> PyResult<&'a PyAny> {
        let mut fetch_tasks: Vec<(String, u64, u64, u32)> = Vec::new();
        let mut total_size = 0usize;
        
        for item in tasks.iter() {
            let task_tuple: (String, u64, u64, u32) = item.extract()?;
            total_size += task_tuple.3 as usize;
            fetch_tasks.push(task_tuple);
        }

        if fetch_tasks.is_empty() {
            return pyo3_asyncio::tokio::future_into_py(py, async move {
                Python::with_gil(|py| {
                    let obj: PyObject = pyo3::types::PyBytes::new(py, &[]).into();
                    Ok::<_, pyo3::PyErr>(obj)
                })
            });
        }

        let client = self.client.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut results: Vec<Result<Vec<u8>, String>> = Vec::new();
            if !fetch_tasks.is_empty() {
                results = futures::stream::iter(fetch_tasks)
                    .map(|(url, b_start, b_end, unpacked_size)| {
                        let client = client.clone();
                        async move {
                            reqwest_download_and_decompress_range_with_retry(
                                &client,
                                &url,
                                b_start,
                                b_end,
                                unpacked_size,
                                MAX_S3_ATTEMPTS,
                            ).await
                        }
                    })
                    .buffered(MAX_CONCURRENCY)
                    .collect::<Vec<Result<Vec<u8>, String>>>()
                    .await;
            }

            let mut final_out = vec![0u8; total_size];
            let mut offset = 0;
            
            for res in results {
                let chunk_bytes = res.map_err(|e| pyo3::PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
                let chunk_len = chunk_bytes.len();
                final_out[offset..offset + chunk_len].copy_from_slice(&chunk_bytes);
                offset += chunk_len;
            }

            Python::with_gil(|py| {
                let obj: PyObject = pyo3::types::PyBytes::new(py, &final_out).into();
                Ok::<_, pyo3::PyErr>(obj)
            })
        })
    }

    #[pyo3(signature = (gc_db_path=None))]
    pub fn init_gc(&self, py: Python<'_>, gc_db_path: Option<String>) -> PyResult<()> {
        let path = match gc_db_path {
            Some(p) => std::path::PathBuf::from(p),
            None => self.sfm.shard_directory().join("gc_live_chunks.redb"),
        };

        // Close any prior GC database handle first and remove old file if present
        *self.gc_db.write().unwrap() = None;
        *self.gc_file_shards.write().unwrap() = None;
        let mut gc_file_map: Option<GcFileShardMap> = None;
        if path.exists() {
            let _ = std::fs::remove_file(&path);
        }

        let db = redb::Database::create(&path)
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
            let _ = write_txn.open_table(GC_SPARSE_XORBS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open GC_SPARSE_XORBS_TABLE: {e}")))?;
            let _ = write_txn.open_table(GC_LIVE_FILES_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open GC_LIVE_FILES_TABLE: {e}")))?;
            let _ = write_txn.open_table(GC_LIVE_XORBS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open GC_LIVE_XORBS_TABLE: {e}")))?;

            let mut xorb_chunks_table = write_txn.open_table(GC_XORB_CHUNKS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Failed to open GC_XORB_CHUNKS_TABLE: {e}")))?;

            let shards = py.allow_threads(|| {
                self.rt.block_on(async {
                    self.sfm.registered_shard_list().await
                })
            }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;

            let mut missing_xorb_info: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();

            for shard_file in &shards {
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
                        let xorb_h = xiv.xorb_hash();
                        let xorb_h_bytes: [u8; 32] = xorb_h.into();
                        if xorb_chunks_table.get(&xorb_h_bytes).unwrap_or(None).is_none() {
                            let num_entries = xiv.num_entries();
                            let mut chunk_bytes = Vec::with_capacity(num_entries * 40);
                            for j in 0..num_entries {
                                let chunk = xiv.chunk(j);
                                let packed_length = if j + 1 < num_entries {
                                    let next_chunk = xiv.chunk(j + 1);
                                    next_chunk.chunk_byte_range_start.saturating_sub(chunk.chunk_byte_range_start)
                                } else {
                                    chunk.unpacked_segment_bytes
                                };
                                let chunk_h_bytes: [u8; 32] = chunk.chunk_hash.into();
                                chunk_bytes.extend_from_slice(&chunk_h_bytes);
                                chunk_bytes.extend_from_slice(&packed_length.to_le_bytes());
                                chunk_bytes.extend_from_slice(&chunk.unpacked_segment_bytes.to_le_bytes());
                            }
                            let _ = xorb_chunks_table.insert(&xorb_h_bytes, chunk_bytes.as_slice());
                        }
                    }
                }
            }

            // Second pass: find any XORBs referenced by file segments whose MDBXorbInfo block was omitted in an earlier GC run.
            // The same pass records which shard holds each file entry (see GcFileShardMap).
            // While building: the shard to read and the hash of its segments, or None once two
            // entries for the file disagree.
            let mut file_shards: std::collections::HashMap<[u8; 32], Option<(Arc<mdb_shard::metadata_shard::MDBShardFile>, MerkleHash)>> =
                std::collections::HashMap::new();
            let mut file_map_complete = true;
            for shard_file in &shards {
                let mut reader = match shard_file.get_reader() {
                    Ok(r) => r,
                    Err(_) => { file_map_complete = false; continue },
                };
                let m_shard = match MDBMinimalShard::from_reader(&mut reader, true, false) {
                    Ok(s) => s,
                    Err(_) => { file_map_complete = false; continue },
                };
                for i in 0..m_shard.num_files() {
                    if let Some(file_view) = m_shard.file(i) {
                        let file_info = mdb_shard::metadata_shard::file_structs::MDBFileInfo::from(file_view);
                        let file_h: [u8; 32] = file_info.metadata.file_hash.into();
                        let seg_h = segments_hash(&file_info);
                        match file_shards.entry(file_h) {
                            std::collections::hash_map::Entry::Vacant(e) => {
                                e.insert(Some((shard_file.clone(), seg_h)));
                            },
                            std::collections::hash_map::Entry::Occupied(mut e) => {
                                let agrees = matches!(e.get(), Some((_, h)) if *h == seg_h);
                                if !agrees {
                                    e.insert(None);
                                }
                            },
                        }
                        for seg in &file_info.segments {
                            let xorb_h_bytes: [u8; 32] = seg.xorb_hash.into();
                            if xorb_chunks_table.get(&xorb_h_bytes).unwrap_or(None).is_none() {
                                missing_xorb_info.insert(xorb_h_bytes);
                            }
                        }
                    }
                }
            }

            if file_map_complete {
                let files: std::collections::HashMap<[u8; 32], GcFileLoc> = file_shards.into_iter()
                    .map(|(file_h, loc)| (file_h, match loc {
                        Some((shard, _)) => GcFileLoc::Uniform(shard),
                        None => GcFileLoc::Conflicting,
                    }))
                    .collect();
                let conflicting = files.values().filter(|v| matches!(v, GcFileLoc::Conflicting)).count();
                eprintln!("[GC Init] File-to-shard map: {} files read directly, {} with conflicting entries across shards (full lookup).", files.len() - conflicting, conflicting);
                gc_file_map = Some(GcFileShardMap {
                    shard_hashes: shards.iter().map(|s| <[u8; 32]>::from(s.shard_hash)).collect(),
                    files,
                });
            } else {
                eprintln!("[GC Init] A shard could not be read; liveness lookups will all use the full lookup.");
            }

            if !missing_xorb_info.is_empty() {
                if let Ok((s3_client, bucket)) = crate::s3_client::create_s3_client() {
                    let missing_vec: Vec<[u8; 32]> = missing_xorb_info.into_iter().collect();
                    let recovered = py.allow_threads(|| {
                        self.rt.block_on(async {
                            use futures::stream::{StreamExt, iter};
                            let futures_iter = missing_vec.into_iter().map(|xorb_h_bytes| {
                                let client = s3_client.clone();
                                let bucket = bucket.clone();
                                async move {
                                    let xorb_h = MerkleHash::from(xorb_h_bytes);
                                    let key = format!("xorbs/default/{}", xorb_h.hex());
                                    let res = crate::s3_client::s3_download_range(&client, &bucket, &key, "bytes=-1048576", MAX_S3_ATTEMPTS, false).await;
                                    (xorb_h_bytes, res)
                                }
                            });
                            iter(futures_iter).buffer_unordered(MAX_CONCURRENCY).collect::<Vec<_>>().await
                        })
                    });

                    for (xorb_h_bytes, res) in recovered {
                        let footer_opt = res.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e))?;
                        if let Some(footer_bytes) = footer_opt {
                            if let Some((hashes, boundaries, unpacked)) = parse_xorb_footer_data(&footer_bytes) {
                                let num_entries = hashes.len();
                                let mut chunk_bytes = Vec::with_capacity(num_entries * 40);
                                for j in 0..num_entries {
                                    let packed_length = if j == 0 {
                                        boundaries[0]
                                    } else {
                                        boundaries[j].saturating_sub(boundaries[j - 1])
                                    };
                                    let unpacked_segment_bytes = if j == 0 {
                                        unpacked[0]
                                    } else {
                                        unpacked[j].saturating_sub(unpacked[j - 1])
                                    };
                                    let chunk_h_bytes: [u8; 32] = hashes[j].into();
                                    chunk_bytes.extend_from_slice(&chunk_h_bytes);
                                    chunk_bytes.extend_from_slice(&packed_length.to_le_bytes());
                                    chunk_bytes.extend_from_slice(&unpacked_segment_bytes.to_le_bytes());
                                }
                                let _ = xorb_chunks_table.insert(&xorb_h_bytes, chunk_bytes.as_slice());
                            }
                        }
                    }
                }
            }
        }
        write_txn.commit()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("GC DB Commit failed: {e}")))?;

        *self.gc_db.write().unwrap() = Some(db);
        *self.gc_db_path.write().unwrap() = Some(path);
        *self.gc_file_shards.write().unwrap() = gc_file_map.map(Arc::new);
        Ok(())
    }

    #[pyo3(signature = ())]
    pub fn cleanup_gc(&self, _py: Python<'_>) -> PyResult<()> {
        *self.gc_db.write().unwrap() = None;
        *self.gc_file_shards.write().unwrap() = None;
        if let Some(path) = self.gc_db_path.write().unwrap().take() {
            if path.exists() {
                let _ = std::fs::remove_file(path);
            }
        }
        Ok(())
    }

    #[pyo3(signature = (live_file_hashes))]
    pub fn build_live_chunks_list(&self, _py: Python<'_>, live_file_hashes: Vec<String>) -> PyResult<()> {
        let gc_db_lock = self.gc_db.read().unwrap();
        let gc_db = match &*gc_db_lock {
            Some(db) => db,
            None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
        };

        // 1. Insert live files into GC_LIVE_FILES_TABLE in one transaction
        let write_txn = gc_db.begin_write()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn failed: {e}")))?;
        {
            let mut files_table = write_txn.open_table(GC_LIVE_FILES_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
            for file_hash_hex in &live_file_hashes {
                if let Ok(h) = MerkleHash::from_hex(file_hash_hex) {
                    let h_bytes: [u8; 32] = h.into();
                    let _ = files_table.insert(&h_bytes, &());
                }
            }
        }
        write_txn.commit()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit failed: {e}")))?;

        // 2. Query XORB chunks from GC_XORB_CHUNKS_TABLE and stream chunks to live tables
        let mut chunks_to_insert: Vec<([u8; 32], [u8; 32])> = Vec::with_capacity(LIVENESS_FLUSH_CHUNKS);
        let mut xorbs_to_insert: Vec<[u8; 32]> = Vec::with_capacity(1000);

        let flush_chunks = |chunks: &mut Vec<([u8; 32], [u8; 32])>, xorbs: &mut Vec<[u8; 32]>| -> PyResult<()> {
            if chunks.is_empty() && xorbs.is_empty() { return Ok(()); }
            let write_txn = gc_db.begin_write()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn failed: {e}")))?;
            {
                let mut live_table = write_txn.open_table(GC_LIVE_CHUNKS_TABLE)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
                let mut primary_table = write_txn.open_table(GC_PRIMARY_XORB_TABLE)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
                let mut live_xorbs_table = write_txn.open_table(GC_LIVE_XORBS_TABLE)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;

                // Same table contents as inserting in arrival order, with far fewer B-tree
                // operations: every live xorb once, in key order.
                let live_xorbs: std::collections::BTreeSet<[u8; 32]> = xorbs.iter().copied()
                    .chain(chunks.iter().map(|(_, xorb_h)| *xorb_h))
                    .collect();
                for xorb_h in &live_xorbs {
                    let _ = live_xorbs_table.insert(xorb_h, &());
                }
                // The sort is stable: for a chunk seen more than once, its first occurrence
                // stays first, so first-writer-wins in GC_PRIMARY_XORB_TABLE is unchanged.
                // Later occurrences would only repeat a no-op insert and a lookup that finds
                // the primary already set, so they are skipped.
                chunks.sort_by_key(|(chunk_h, _)| *chunk_h);
                let mut prev_chunk: Option<[u8; 32]> = None;
                for (chunk_h, xorb_h) in chunks.iter() {
                    if prev_chunk == Some(*chunk_h) {
                        continue;
                    }
                    prev_chunk = Some(*chunk_h);
                    let _ = live_table.insert(chunk_h, &());
                    if primary_table.get(chunk_h).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Get failed: {e}")))?.is_none() {
                        let _ = primary_table.insert(chunk_h, xorb_h);
                    }
                }
            }
            write_txn.commit()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit failed: {e}")))?;
            chunks.clear();
            xorbs.clear();
            Ok(())
        };

        {
            let read_txn = gc_db.begin_read()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
            let xorb_table = read_txn.open_table(GC_XORB_CHUNKS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;

            // Each lookup scans the shards synchronously, so it is spawned onto the runtime's
            // worker threads to run in parallel. Results are consumed in input order
            // (`buffered`, not `buffer_unordered`), so the first-writer-wins
            // GC_PRIMARY_XORB_TABLE gets the same result as the old sequential loop.
            // Groups bound how many results are held in memory at once.
            let parsed: Vec<(&String, MerkleHash)> = live_file_hashes.iter()
                .filter_map(|hex| MerkleHash::from_hex(hex).ok().map(|h| (hex, h)))
                .collect();
            // Use the file-to-shard map only if the registered shards are still the ones it was
            // built from; otherwise every file takes the full lookup.
            let file_map = self.gc_file_shards.read().unwrap().clone().filter(|m| {
                let current: std::collections::HashSet<[u8; 32]> = _py
                    .allow_threads(|| self.rt.block_on(self.sfm.registered_shard_list()))
                    .map(|v| v.iter().map(|s| <[u8; 32]>::from(s.shard_hash)).collect())
                    .unwrap_or_default();
                let unchanged = current == m.shard_hashes;
                if !unchanged {
                    eprintln!("[GC Liveness] Registered shards changed since init_gc; using the full lookup.");
                }
                unchanged
            });

            for group in parsed.chunks(LIVENESS_LOOKUP_GROUP) {
                let lookups = _py.allow_threads(|| {
                    self.rt.block_on(async {
                        futures::stream::iter(group.iter().map(|(_, h)| {
                            let index = self.sfm.clone();
                            let h = *h;
                            let h_bytes: [u8; 32] = h.into();
                            // None: full lookup. Some(None): in no shard. Some(Some(s)): read s.
                            let direct: Option<Option<Arc<mdb_shard::metadata_shard::MDBShardFile>>> = file_map.as_ref().and_then(|m| {
                                match m.files.get(&h_bytes) {
                                    None => Some(None),
                                    Some(GcFileLoc::Uniform(shard)) => Some(Some(shard.clone())),
                                    Some(GcFileLoc::Conflicting) => None,
                                }
                            });
                            self.rt.spawn(async move {
                                match direct {
                                    Some(None) => return Ok(None),
                                    Some(Some(shard)) => {
                                        // Same segments as the entry the shard manager would
                                        // return. On any surprise, fall back to its lookup.
                                        if let Ok(Some(fi)) = shard.get_file_reconstruction_info(&h) {
                                            return Ok(Some((fi, Some(shard.shard_hash))));
                                        }
                                    },
                                    None => {},
                                }
                                index.get_file_reconstruction_info(&h).await
                            })
                        }))
                        .buffered(LIVENESS_LOOKUP_CONCURRENCY)
                        .collect::<Vec<_>>()
                        .await
                    })
                });

                for ((file_hash_hex, _), res) in group.iter().zip(lookups) {
                    let res = res
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Lookup task failed for {}: {:?}", file_hash_hex, e)))?
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Query failed for {}: {:?}", file_hash_hex, e)))?;

                    let (file_info, _) = match res {
                        Some(r) => r,
                        None => continue,
                    };

                    for segment in &file_info.segments {
                        let xorb_h_bytes: [u8; 32] = segment.xorb_hash.into();
                        xorbs_to_insert.push(xorb_h_bytes);
                        if let Ok(Some(xorb_val)) = xorb_table.get(&xorb_h_bytes) {
                            let bytes = xorb_val.value();
                            let total_entries = bytes.len() / 40;
                            let start_idx = (segment.chunk_index_start as usize).min(total_entries);
                            let end_idx = (segment.chunk_index_end as usize).min(total_entries);

                            for idx in start_idx..end_idx {
                                let offset = idx * 40;
                                let mut chunk_h = [0u8; 32];
                                chunk_h.copy_from_slice(&bytes[offset..offset + 32]);
                                chunks_to_insert.push((chunk_h, xorb_h_bytes));
                            
                                if chunks_to_insert.len() >= LIVENESS_FLUSH_CHUNKS {
                                    flush_chunks(&mut chunks_to_insert, &mut xorbs_to_insert)?;
                                }
                            }
                        } else if xorbs_to_insert.len() >= 1000 {
                            flush_chunks(&mut chunks_to_insert, &mut xorbs_to_insert)?;
                        }
                    }
                }
            }
        }

        // Flush any remaining chunks
        flush_chunks(&mut chunks_to_insert, &mut xorbs_to_insert)?;

        Ok(())
    }

    #[pyo3(signature = (chunk_hash_hex))]
    pub fn is_chunk_live(&self, py: Python<'_>, chunk_hash_hex: &str) -> PyResult<bool> {
        let h = MerkleHash::from_hex(chunk_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        py.allow_threads(|| {
            let gc_db_lock = self.gc_db.read().unwrap();
            let gc_db = match &*gc_db_lock {
                Some(db) => db,
                None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
            };

            let read_txn = gc_db.begin_read()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
            let table = read_txn.open_table(GC_LIVE_CHUNKS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
            
            let res = table.get(&h_bytes)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Get failed: {e}")))?;

            Ok(res.is_some())
        })
    }

    #[pyo3(signature = (chunk_hash_hex))]
    pub fn get_primary_xorb(&self, py: Python<'_>, chunk_hash_hex: &str) -> PyResult<Option<String>> {
        let h = MerkleHash::from_hex(chunk_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        py.allow_threads(|| {
            let gc_db_lock = self.gc_db.read().unwrap();
            let gc_db = match &*gc_db_lock {
                Some(db) => db,
                None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
            };

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
        })
    }

    /// Verifies an uploaded shard against data the server trusts.
    ///
    /// `server_chunks` maps xorb hash (hex) to the packed chunk list saved at xorb upload
    /// (see `xorb_chunk_list`). Returns `None` when the shard passes, otherwise
    /// `(reason, hash_hex)` for the first failing check.
    #[pyo3(signature = (shard_bytes, server_chunks))]
    pub fn verify_shard(
        &self,
        py: Python<'_>,
        shard_bytes: &[u8],
        server_chunks: &PyDict,
    ) -> PyResult<Option<(String, String)>> {
        let mut chunks = std::collections::HashMap::new();
        for (k, v) in server_chunks.iter() {
            let hex: String = k.extract()?;
            let bytes: &[u8] = v.extract()?;
            if let (Ok(h), Some(list)) = (MerkleHash::from_hex(&hex), verify::unpack_chunk_list(bytes)) {
                chunks.insert(h, list);
            }
        }
        let shard_bytes = shard_bytes.to_vec();
        let result = py.allow_threads(|| {
            self.rt.block_on(verify::verify_shard(&self.sfm, &shard_bytes, chunks))
        });
        Ok(result.err())
    }

    /// Read-only deep audit of every registered shard (see `verify::deep_audit_shards`).
    /// `live_file_hashes`: hex hashes of the files in use. Returns a dict of counts and lists.
    #[pyo3(signature = (live_file_hashes))]
    pub fn deep_audit(&self, py: Python<'_>, live_file_hashes: Vec<String>) -> PyResult<Py<PyDict>> {
        let live: std::collections::HashSet<MerkleHash> = live_file_hashes.iter()
            .filter_map(|h| MerkleHash::from_hex(h).ok())
            .collect();
        let (client, bucket) = crate::s3_client::create_s3_client()?;
        let report = py.allow_threads(|| {
            self.rt.block_on(async {
                let shards = self.sfm.registered_shard_list().await.map_err(|e| format!("Failed to get shard list: {e:?}"))?;
                let list: Vec<(String, std::path::PathBuf)> = shards.iter().map(|s| (s.shard_hash.hex(), s.path.clone())).collect();
                verify::deep_audit_shards(&client, &bucket, &list, &live).await
            })
        }).map_err(PyErr::new::<pyo3::exceptions::PyIOError, _>)?;
        let dict = PyDict::new(py);
        dict.set_item("shards_checked", report.shards_checked)?;
        dict.set_item("entries_checked", report.entries_checked)?;
        dict.set_item("bad_entries", report.bad_entries)?;  // (file hash, reason, shard hash)
        dict.set_item("blocks_checked", report.blocks_checked)?;
        dict.set_item("blocks_wrong_chunks", report.blocks_wrong_chunks)?;
        dict.set_item("blocks_old_positions", report.blocks_old_positions)?;
        dict.set_item("blocks_xorb_missing", report.blocks_xorb_missing)?;  // (xorb hash, shard hash)
        dict.set_item("blocks_footer_unreadable", report.blocks_footer_unreadable)?;  // (xorb hash, shard hash)
        Ok(dict.into())
    }

    #[pyo3(signature = (sparse_threshold=30.0))]
    pub fn run_global_utilization_analysis(&self, _py: Python<'_>, sparse_threshold: f64) -> PyResult<()> {
        _py.allow_threads(|| {
        let gc_db_lock = self.gc_db.read().unwrap();
        let gc_db = match &*gc_db_lock {
            Some(db) => db,
            None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
        };

        let mut util_entries = Vec::with_capacity(10000);
        let mut sparse_entries = Vec::with_capacity(10000);

        let flush_utilization = |utils: &mut Vec<([u8; 32], String)>, sparses: &mut Vec<[u8; 32]>| -> PyResult<()> {
            if utils.is_empty() && sparses.is_empty() { return Ok(()); }
            let write_txn = gc_db.begin_write()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Write txn failed: {e}")))?;
            {
                let mut util_table = write_txn.open_table(GC_XORB_UTILIZATION_TABLE)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
                let mut sparse_table = write_txn.open_table(GC_SPARSE_XORBS_TABLE)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;

                for (k, v) in utils.iter() {
                    let _ = util_table.insert(k, v.as_str());
                }
                for k in sparses.iter() {
                    let _ = sparse_table.insert(k, &());
                }
            }
            write_txn.commit()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Commit failed: {e}")))?;
            utils.clear();
            sparses.clear();
            Ok(())
        };

        {
            let read_txn = gc_db.begin_read()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
            let xorb_table = read_txn.open_table(GC_XORB_CHUNKS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table open failed: {e}")))?;
            let live_chunks_table = read_txn.open_table(GC_LIVE_CHUNKS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Live chunks table open failed: {e}")))?;
            let live_xorbs_table = read_txn.open_table(GC_LIVE_XORBS_TABLE)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Live xorbs table open failed: {e}")))?;

            for item in xorb_table.iter().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Iter failed: {e}")))? {
                let (key, val) = item.map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read failed: {e}")))?;
                let xorb_h_bytes = *key.value();
                let bytes = val.value();
                let total_entries = bytes.len() / 40;

                let is_xorb_live = live_xorbs_table.get(&xorb_h_bytes).unwrap_or(None).is_some();

                let mut total_bytes: u64 = 0;
                let mut live_bytes: u64 = 0;
                let mut dead_chunks = Vec::new();

                for idx in 0..total_entries {
                    let offset = idx * 40;
                    let mut chunk_h_bytes = [0u8; 32];
                    chunk_h_bytes.copy_from_slice(&bytes[offset..offset + 32]);
                    let packed_len = u32::from_le_bytes(bytes[offset + 32..offset + 36].try_into().unwrap()) as u64;

                    total_bytes += packed_len;
                    let is_live = is_xorb_live && live_chunks_table.get(&chunk_h_bytes).unwrap_or(None).is_some();
                    if is_live {
                        live_bytes += packed_len;
                    } else {
                        dead_chunks.push(MerkleHash::from(chunk_h_bytes).hex());
                    }
                }

                let utilization = if total_bytes == 0 { 0.0 } else { (live_bytes as f64 / total_bytes as f64) * 100.0 };
                let dead_chunks_json = dead_chunks.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",");
                let json_str = format!("{{\"utilization\":{},\"live_bytes\":{},\"total_bytes\":{},\"dead_chunks\":[{}]}}",
                    utilization, live_bytes, total_bytes, dead_chunks_json);

                util_entries.push((xorb_h_bytes, json_str));
                if utilization <= sparse_threshold {
                    sparse_entries.push(xorb_h_bytes);
                }
                
                if util_entries.len() >= 10000 {
                    flush_utilization(&mut util_entries, &mut sparse_entries)?;
                }
            }
        }

        // Flush any remaining entries
        flush_utilization(&mut util_entries, &mut sparse_entries)?;

        Ok::<_, pyo3::PyErr>(())
        })?;
        Ok(())
    }

    #[pyo3(signature = (xorb_hash_hex))]
    pub fn get_xorb_utilization(&self, py: Python<'_>, xorb_hash_hex: &str) -> PyResult<Option<String>> {
        let h = MerkleHash::from_hex(xorb_hash_hex)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid hex: {e:?}")))?;
        let h_bytes: [u8; 32] = h.into();

        py.allow_threads(|| {
            let gc_db_lock = self.gc_db.read().unwrap();
            let gc_db = match &*gc_db_lock {
                Some(db) => db,
                None => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("GC DB not initialized. Call init_gc first.")),
            };

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
        })
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
    /// True if any registered shard has a file entry pointing at a xorb GC can't find (no
    /// shard lists it and S3 has no footer for it). GC start rewrites such shards, so it must
    /// run even when no xorb is up for removal.
    ///
    /// How such entries arise: an upload made with a stale or broken client cache referenced a
    /// xorb the server had no listing for, and was stored before shard verification existed.
    /// Verification now rejects these uploads (400), so new ones can't appear; this exists to
    /// clean up old ones during the one-time cleanup and any that remain afterwards.
    ///
    /// No S3 requests here: `init_gc` already filled GC_XORB_CHUNKS_TABLE from shard xorb lists
    /// and, for xorbs only file entries reference, from S3 footers (network errors abort it).
    /// So "not in the table" means "listed nowhere and not in S3".
    ///
    /// Cost: only called when nothing is up for removal (the caller's `&&` skips it otherwise);
    /// reads file entries of the local shard files, no network; stops at the first hit.
    fn any_file_entry_references_missing_xorb(&self, gc_db: &redb::Database) -> PyResult<bool> {
        let read_txn = gc_db.begin_read().map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Read txn failed: {e}")))?;
        let xorb_chunks = read_txn.open_table(GC_XORB_CHUNKS_TABLE).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Table error: {e}")))?;
        let shards = self.rt.block_on(self.sfm.registered_shard_list())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to get shard list: {e:?}")))?;
        for shard_file in shards {
            let Ok(mut reader) = shard_file.get_reader() else { continue };
            let Ok(shard) = MDBMinimalShard::from_reader(&mut reader, true, false) else { continue };
            for i in 0..shard.num_files() {
                let Some(view) = shard.file(i) else { continue };
                let info = mdb_shard::metadata_shard::file_structs::MDBFileInfo::from(view);
                for seg in &info.segments {
                    let h: [u8; 32] = seg.xorb_hash.into();
                    if xorb_chunks.get(&h).unwrap_or(None).is_none() {
                        eprintln!("[GC] Shard {} references missing XORB {}.", shard_file.shard_hash.hex(), seg.xorb_hash.hex());
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    pub fn calculate_reconstruction_internal_sync(
        py: Python<'_>,
        start_byte: Option<u64>,
        end_byte: Option<u64>,
        xorb_footers: std::collections::HashMap<MerkleHash, Option<(Vec<MerkleHash>, Vec<u32>, Vec<u32>)>>,
        coalesce: bool,
        file_info: mdb_shard::metadata_shard::file_structs::MDBFileInfo,
    ) -> PyResult<Option<Py<PyDict>>> {

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
            if cumulative_bytes + seg_unpacked_len > file_range_start && cumulative_bytes <= file_range_end {
                
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

    let dest_shards_bytes = py.allow_threads(|| {
        let mut dest = Vec::new();
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
                    dest.push(out_bytes);
                    current_shard = parsed_shard;
                }
            }
        }

        if !current_shard.is_empty() {
            let out_bytes = current_shard.to_bytes()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Serialize error: {e:?}")))?;
            dest.push(out_bytes);
        }
        Ok::<Vec<Vec<u8>>, PyErr>(dest)
    })?;

    let dest_shards = PyList::empty(py);
    for out_bytes in dest_shards_bytes {
        dest_shards.append(PyBytes::new(py, &out_bytes))?;
    }

    Ok(dest_shards.into_py(py))
}

#[pyfunction]
pub fn add_footer_to_xorb(py: Python<'_>, xorb_bytes: &[u8]) -> PyResult<PyObject> {
    let output = py.allow_threads(|| {
        let mut output = Vec::new();
        match reconstruct_xorb_with_footer(&mut output, xorb_bytes) {
            Ok(_) => Ok(output),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to reconstruct xorb with footer: {:?}", e)))
        }
    })?;
    
    let bytes = pyo3::types::PyBytes::new(py, &output);
    Ok(bytes.into())
}

/// Rebuilds the xorb footer from the chunk data and returns `(xorb_bytes, xorb_hash_hex)`.
///
/// Any existing footer (valid or not) is discarded: chunk parsing stops at the `XETBLOB`
/// footer identifier and a fresh footer is written. The returned hash is computed from the
/// chunks themselves, so callers can verify it matches the xorb's name before storing.
#[pyfunction]
pub fn add_footer_to_xorb_with_hash(py: Python<'_>, xorb_bytes: &[u8]) -> PyResult<(PyObject, String)> {
    let (output, hash) = py.allow_threads(|| {
        let mut output = Vec::new();
        match reconstruct_xorb_with_footer(&mut output, xorb_bytes) {
            Ok((_, hash)) => Ok((output, hash.hex())),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to reconstruct xorb with footer: {:?}", e)))
        }
    })?;

    let bytes = pyo3::types::PyBytes::new(py, &output);
    Ok((bytes.into(), hash))
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
pub fn compute_shard_hash(py: Python<'_>, shard_bytes: &[u8]) -> String {
    py.allow_threads(|| {
        compute_data_hash(shard_bytes).hex()
    })
}

#[pyfunction]
pub fn reconstruct_shard(py: Python<'_>, shard_bytes: &[u8]) -> PyResult<PyObject> {
    let result = py.allow_threads(|| {
        let header = MDBShardFileHeader::deserialize(&mut Cursor::new(shard_bytes))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to parse header: {e:?}")))?;

        if header.footer_size == 0 {
            let in_memory_shard = bytes_to_in_memory_shard(shard_bytes)?;
            let reconstructed = in_memory_shard.to_bytes()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Failed to serialize in-memory shard: {e:?}")))?;
            Ok::<Option<Vec<u8>>, PyErr>(Some(reconstructed))
        } else {
            Ok::<Option<Vec<u8>>, PyErr>(None)
        }
    })?;

    if let Some(reconstructed) = result {
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

/// Returns the packed chunk list (32-byte hash + u32 LE unpacked size per chunk) read from
/// the footer of a xorb object, as built by `add_footer_to_xorb_with_hash`.
#[pyfunction]
pub fn xorb_chunk_list(py: Python<'_>, xorb_bytes: &[u8]) -> PyResult<PyObject> {
    let packed = py.allow_threads(|| {
        verify::chunk_list_from_xorb_footer(xorb_bytes)
            .map(|list| verify::pack_chunk_list(&list))
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Failed to read xorb footer"))
    })?;
    Ok(PyBytes::new(py, &packed).into())
}

/// Returns the hashes (hex) of the xorbs listed in a shard's CAS info section: the xorbs new
/// in the upload that produced this shard.
#[pyfunction]
pub fn shard_xorb_hashes(py: Python<'_>, shard_bytes: &[u8]) -> PyResult<Vec<String>> {
    py.allow_threads(|| {
        let shard = MDBMinimalShard::from_reader(&mut Cursor::new(shard_bytes), false, true)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Parse error: {e:?}")))?;
        Ok((0..shard.num_xorb()).filter_map(|i| shard.xorb(i)).map(|x| x.xorb_hash().hex()).collect())
    })
}

/// Returns a copy of the shard with the footer's expiry set to `expiry_unix` (seconds).
/// Used for /v1/chunks answers; the stored shard is not changed.
#[pyfunction]
pub fn with_dedup_expiry(py: Python<'_>, shard_bytes: &[u8], expiry_unix: u64) -> PyResult<PyObject> {
    let out = py.allow_threads(|| {
        verify::with_dedup_expiry(shard_bytes, expiry_unix)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))
    })?;
    Ok(PyBytes::new(py, &out).into())
}

#[pymodule]
fn xet_shard_parser(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ShardIndex>()?;
    m.add_function(wrap_pyfunction!(compute_shard_hash, m)?)?;
    m.add_function(wrap_pyfunction!(merge_shards, m)?)?;
    m.add_function(wrap_pyfunction!(add_footer_to_xorb, m)?)?;
    m.add_function(wrap_pyfunction!(add_footer_to_xorb_with_hash, m)?)?;
    m.add_function(wrap_pyfunction!(reconstruct_shard, m)?)?;
    m.add_function(wrap_pyfunction!(get_gc_transaction_info, m)?)?;
    m.add_function(wrap_pyfunction!(xorb_chunk_list, m)?)?;
    m.add_function(wrap_pyfunction!(with_dedup_expiry, m)?)?;
    m.add_function(wrap_pyfunction!(shard_xorb_hashes, m)?)?;

    Ok(())
}

