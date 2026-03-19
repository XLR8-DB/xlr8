//! Full fetch module - GIL-free MongoDB to Parquet pipeline with parallel tokio workers

// Submodules
pub mod types;
pub mod bson_sort;
pub mod utilities;
pub mod parquet_writer;
pub mod arrow_builder;

// Re-export key types for convenience
pub use types::{
    BsonChunk, FetchResult, MemoryAwareBuffer, 
    SchemaSpec, SortSpec,
};

// Re-export functions
pub use bson_sort::{compare_bson_values, bson_type_priority, parse_sort_spec, sort_documents};
pub use utilities::{get_timestamp_range, make_date_range_filename};
pub use parquet_writer::write_parquet_file;
pub use arrow_builder::{
    build_arrow_schema_from_spec, build_record_batch, extract_field_to_array, get_nested_value,
};

use bson::{doc, Document};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

// ============================================================================
// BSON-BASED CHUNK PARSING (Phase 1 Integration)
// ============================================================================


/// Parse BSON-serialized chunks from Python
/// 
/// Python sends: bson.encode({"chunks": [{filter: {...}, chunk_idx: 0, start_ms: ..., end_ms: ...}, ...]})
/// 
/// This handles ALL MongoDB types correctly:
/// - ObjectId
/// - DateTime
/// - Regex
/// - Binary
/// - etc.
fn parse_bson_chunks(chunks_bson: &[u8]) -> PyResult<Vec<BsonChunk>> {
    // Deserialize BSON wrapper document
    let wrapper: Document = bson::from_slice(chunks_bson)
        .map_err(|e| PyValueError::new_err(format!("Failed to parse BSON chunks: {e}")))?;
    
    // Extract chunks array
    let chunks_array = wrapper.get_array("chunks")
        .map_err(|e| PyValueError::new_err(format!("Missing 'chunks' field in BSON: {e}")))?;
    
    let mut result = Vec::new();
    
    for (idx, chunk_bson) in chunks_array.iter().enumerate() {
        let chunk_doc = chunk_bson.as_document()
            .ok_or_else(|| PyValueError::new_err(format!("Chunk {} is not a document", idx)))?;
        
        // Extract filter (as BSON Document - preserve all MongoDB types)
        let filter = chunk_doc.get_document("filter")
            .map_err(|e| PyValueError::new_err(format!("Chunk {} missing 'filter': {e}", idx)))?
            .clone();
        
        // Extract metadata
        let chunk_idx = chunk_doc.get_i32("chunk_idx")
            .map_err(|e| PyValueError::new_err(format!("Chunk {} missing 'chunk_idx': {e}", idx)))?;
        
        // start_ms and end_ms are optional - partial brackets may have None for unbounded queries
        let start_ms = chunk_doc.get_i64("start_ms").ok();
        let end_ms = chunk_doc.get_i64("end_ms").ok();
        
        result.push(BsonChunk {
            filter,
            chunk_idx,
            start_ms,
            end_ms,
        });
    }
    
    Ok(result)
}

/// Fetch MongoDB data in parallel using BSON-encoded chunk filters.
/// Returns dict with total_docs, total_files, duration_secs.
#[pyfunction]
#[pyo3(signature = (mongodb_uri, db_name, collection_name, chunks_bson, schema_json, cache_dir, num_workers, batch_size, flush_trigger_mb, avg_doc_size_bytes, sort_spec_json, time_field, projection_json, row_group_size=None))]
pub fn fetch_chunks_bson(
    py: Python<'_>,
    mongodb_uri: &str,
    db_name: &str,
    collection_name: &str,
    chunks_bson: Vec<u8>,  // BSON bytes from Python
    schema_json: &str,
    cache_dir: &str,
    num_workers: usize,
    batch_size: i64,
    flush_trigger_mb: usize,
    avg_doc_size_bytes: usize,
    sort_spec_json: &str,  // Sort specification as JSON
    time_field: &str,      // Time field name for timestamp tracking
    projection_json: &str, // MongoDB projection as JSON (e.g., '{"field": 1}')
    row_group_size: Option<usize>, // Parquet row group size (None = use Arrow default)
) -> PyResult<HashMap<String, PyObject>> {
    // Parse BSON chunks (handles ObjectId, datetime, etc.)
    let chunks = parse_bson_chunks(&chunks_bson)?;
    
    // Parse schema
    // Parse schema
    let schema_spec: SchemaSpec = serde_json::from_str(schema_json)
        .map_err(|e| PyValueError::new_err(format!("Invalid schema_json: {e}")))?;
    
    // Parse projection (optional - null means no projection)
    let projection: Option<mongodb::bson::Document> = if projection_json == "null" || projection_json.is_empty() {
        None
    } else {
        Some(serde_json::from_str(projection_json)
            .map_err(|e| PyValueError::new_err(format!("Invalid projection_json: {e}")))?)
    };
    
    // Release GIL and run async
    let mongodb_uri = mongodb_uri.to_string();
    let db_name = db_name.to_string();
    let collection_name = collection_name.to_string();
    let cache_dir = cache_dir.to_string();
    
    let result = py.allow_threads(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                // Process BSON chunks directly
                
                
                let start = std::time::Instant::now();
                
                // Create MongoDB client
                let client_options = mongodb::options::ClientOptions::parse(&mongodb_uri).await
                    .map_err(|e| PyValueError::new_err(format!("Failed to parse MongoDB URI: {e}")))?;
                
                let client = mongodb::Client::with_options(client_options)
                    .map_err(|e| PyValueError::new_err(format!("Failed to create MongoDB client: {e}")))?;
                
                // Shared state
                let client = Arc::new(client);
                let schema_spec = Arc::new(schema_spec);
                let db_name = Arc::new(db_name);
                let collection_name = Arc::new(collection_name);
                let cache_dir = Arc::new(cache_dir);
                let flush_trigger_mb = Arc::new(flush_trigger_mb);
                let avg_doc_size_bytes = Arc::new(avg_doc_size_bytes);
                let row_group_size = Arc::new(row_group_size);
                
                // Parse sort_spec from JSON
                let sort_spec = parse_sort_spec(sort_spec_json);
                let sort_spec = Arc::new(sort_spec);
                let time_field = Arc::new(time_field.to_string());
                
                // =============================================================
                // OPTIMAL MODE: Pre-assigned chunks, zero runtime coordination
                // =============================================================
                // - Chunks assigned round-robin at start (no channel, no semaphore)
                // - N workers, each with its OWN persistent buffer (no mutex!)
                // - Buffers accumulate across chunks -> fewer files
                // - Maximum speed: no coordination overhead during execution
                // =============================================================
                        
                        // Pre-assign chunks round-robin to workers
                        let mut chunks_per_worker: Vec<Vec<BsonChunk>> = (0..num_workers)
                            .map(|_| Vec::new())
                            .collect();
                        
                        for (idx, chunk) in chunks.into_iter().enumerate() {
                            chunks_per_worker[idx % num_workers].push(chunk);
                        }
                        
                        let cancel_token = CancellationToken::new();
                        let mut handles = Vec::with_capacity(num_workers);
                        
                        for (_worker_id, my_chunks) in chunks_per_worker.into_iter().enumerate() {
                            let client = Arc::clone(&client);
                            let schema_spec = Arc::clone(&schema_spec);
                            let db_name = Arc::clone(&db_name);
                            let collection_name = Arc::clone(&collection_name);
                            let cache_dir = Arc::clone(&cache_dir);
                            let sort_spec = Arc::clone(&sort_spec);
                            let time_field = Arc::clone(&time_field);
                            let flush_trigger_mb = Arc::clone(&flush_trigger_mb);
                            let avg_doc_size_bytes = Arc::clone(&avg_doc_size_bytes);
                            let row_group_size = Arc::clone(&row_group_size);
                            let projection_cloned = projection.clone();
                            let _chunk_count = my_chunks.len();
                            let worker_cancel = cancel_token.clone();
                            
                            let handle = tokio::spawn(async move {
                                let _worker_start = std::time::Instant::now();
                                let db = client.database(&db_name);
                                let collection: mongodb::Collection<Document> = db.collection(&collection_name);
                                let arrow_schema = build_arrow_schema_from_spec(&schema_spec);
                                
                                // MY OWN buffer - no sharing, no mutex!
                                let mut buffer = MemoryAwareBuffer::new(*flush_trigger_mb, *avg_doc_size_bytes);
                                let mut worker_total_docs = 0usize;
                                let mut worker_file_count = 0usize;
                                
                                // Process ALL my pre-assigned chunks
                                for bson_chunk in my_chunks {
                                    // Check if another worker has failed
                                    if worker_cancel.is_cancelled() {
                                        return Err(PyValueError::new_err(
                                            "cancelled: another worker failed".to_string()
                                        ));
                                    }
                                    // Build find options with conditional projection
                                    let find_options = if let Some(ref proj) = projection_cloned {
                                        mongodb::options::FindOptions::builder()
                                            .batch_size(batch_size as u32)
                                            .projection(proj.clone())
                                            .build()
                                    } else {
                                        mongodb::options::FindOptions::builder()
                                            .batch_size(batch_size as u32)
                                            .build()
                                    };
                                    
                                    let mut cursor = match collection.find(bson_chunk.filter).with_options(find_options).await {
                                        Ok(c) => c,
                                        Err(e) => return Err(PyValueError::new_err(
                                            format!("chunk {}: find() failed: {e}", bson_chunk.chunk_idx)
                                        )),
                                    };
                                    
                                    use futures::stream::StreamExt;
                                    while let Some(result) = cursor.next().await {
                                        // Check cancellation between documents
                                        if worker_cancel.is_cancelled() {
                                            return Err(PyValueError::new_err(
                                                "cancelled: another worker failed".to_string()
                                            ));
                                        }
                                        
                                        let doc = match result {
                                            Ok(d) => d,
                                            Err(e) => return Err(PyValueError::new_err(
                                                format!("chunk {}: cursor error: {e}", bson_chunk.chunk_idx)
                                            )),
                                        };
                                        
                                        buffer.add(doc);
                                        worker_total_docs += 1;
                                        
                                        if buffer.should_flush() {
                                            let mut docs = buffer.take_docs();
                                            let _doc_count = docs.len();
                                            
                                            // Move CPU/IO work to blocking thread pool
                                            let sort_spec_clone = sort_spec.clone();
                                            let time_field_clone = time_field.clone();
                                            let cache_dir_clone = cache_dir.clone();
                                            let schema_spec_clone = schema_spec.clone();
                                            let arrow_schema_clone = arrow_schema.clone();
                                            let row_group_size_clone = row_group_size.clone();
                                            let file_count = worker_file_count;
                                            
                                            tokio::task::spawn_blocking(move || {
                                                // Sort on blocking thread
                                                if let Some(ref spec) = sort_spec_clone.as_ref() {
                                                    sort_documents(&mut docs, spec);
                                                }
                                                
                                                // Build Arrow batch on blocking thread
                                                let (start_ms, end_ms) = get_timestamp_range(&docs, &time_field_clone);
                                                let filepath = make_date_range_filename(
                                                    &cache_dir_clone,
                                                    start_ms,
                                                    end_ms,
                                                    file_count
                                                );
                                                
                                                let batch = build_record_batch(&docs, &schema_spec_clone, &arrow_schema_clone)?;
                                                write_parquet_file(&batch, &filepath, *row_group_size_clone)?;
                                                
                                                Ok::<_, PyErr>(())
                                            })
                                            .await
                                            .map_err(|e| PyValueError::new_err(format!("Blocking task error: {e}")))??;
                                            
                                            worker_file_count += 1;
                                        }
                                    }
                                }
                                
                                // Final flush at end
                                if buffer.len() > 0 {
                                    let mut docs = buffer.take_docs();
                                    
                                    // Move CPU/IO work to blocking thread pool
                                    let sort_spec_clone = sort_spec.clone();
                                    let time_field_clone = time_field.clone();
                                    let cache_dir_clone = cache_dir.clone();
                                    let schema_spec_clone = schema_spec.clone();
                                    let arrow_schema_clone = arrow_schema.clone();
                                    let row_group_size_clone = row_group_size.clone();
                                    let file_count = worker_file_count;
                                    
                                    tokio::task::spawn_blocking(move || {
                                        // Sort on blocking thread
                                        if let Some(ref spec) = sort_spec_clone.as_ref() {
                                            sort_documents(&mut docs, spec);
                                        }
                                        
                                        // Build Arrow batch on blocking thread
                                        let (start_ms, end_ms) = get_timestamp_range(&docs, &time_field_clone);
                                        let filepath = make_date_range_filename(
                                            &cache_dir_clone,
                                            start_ms,
                                            end_ms,
                                            file_count
                                        );
                                        
                                        let batch = build_record_batch(&docs, &schema_spec_clone, &arrow_schema_clone)?;
                                        write_parquet_file(&batch, &filepath, *row_group_size_clone)?;
                                        
                                        Ok::<_, PyErr>(())
                                    })
                                    .await
                                    .map_err(|e| PyValueError::new_err(format!("Blocking task error: {e}")))??;
                                    
                                    worker_file_count += 1;
                                }
                                
                                Ok::<(usize, usize), PyErr>((worker_total_docs, worker_file_count))
                            });
                            
                            handles.push(handle);
                        }
                        
                        // Await all workers - cancel remaining on first real error
                        let mut total_docs = 0;
                        let mut total_files = 0;
                        let mut worker_errors: Vec<String> = Vec::new();
                        
                        for handle in handles {
                            match handle.await {
                                Ok(Ok((docs, files))) => {
                                    total_docs += docs;
                                    total_files += files;
                                }
                                Ok(Err(e)) => {
                                    let msg = format!("{e}");
                                    // Only signal cancellation for real errors, not cancellation echoes
                                    if !msg.contains("cancelled: another worker failed") {
                                        cancel_token.cancel();
                                        worker_errors.push(msg);
                                    }
                                }
                                Err(e) => {
                                    cancel_token.cancel();
                                    worker_errors.push(format!("worker panicked: {e}"));
                                }
                            }
                        }
                        
                        if !worker_errors.is_empty() {
                            if let Err(cleanup_err) = std::fs::remove_dir_all(cache_dir.as_str()) {
                                worker_errors.push(format!("cache cleanup failed: {cleanup_err}"));
                            }
                            return Err(PyValueError::new_err(
                                format!(
                                    "Fetch failed ({} error(s)). Errors:\n{}",
                                    worker_errors.len(),
                                    worker_errors.join("\n")
                                )
                            ));
                        }
                        
                        let duration_secs = start.elapsed().as_secs_f64();
                        
                        Ok::<FetchResult, PyErr>(FetchResult {
                            total_docs,
                            total_files,
                            duration_secs,
                            stats_file: None,
                        })
            })
    })?;
    
    // Convert result to Python dict
    let total_docs = result.total_docs.to_object(py);
    let total_files = result.total_files.to_object(py);
    let duration_secs = result.duration_secs.to_object(py);
    let stats_file = result.stats_file.to_object(py);
    
    let mut dict = HashMap::new();
    dict.insert("total_docs".to_string(), total_docs);
    dict.insert("total_files".to_string(), total_files);
    dict.insert("duration_secs".to_string(), duration_secs);
    dict.insert("stats_file".to_string(), stats_file);
    
    Ok(dict)
}

/// Register full_fetch module functions
pub fn register_full_fetch_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(fetch_chunks_bson, m)?)?;  // BSON API
    Ok(())
}
