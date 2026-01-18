//! Type definitions for full_fetch module: timeline tracking, statistics, buffers, schemas.

use bson::Document;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

// ============================================================================
// TIMELINE TRACKING
// ============================================================================

/// Worker activity event for performance visualization (Gantt-chart style).
#[derive(Debug, Clone, serde::Serialize)]
pub struct TimelineEvent {
    pub worker_id: usize,
    pub timestamp_ms: u128,  // Milliseconds since epoch
    pub event_type: String,  // "fetch_start", "fetch_end", "flush_start", "flush_end"
    pub doc_count: Option<usize>,  // Number of docs fetched or flushed
    pub data_start_ms: Option<i64>,  // Start timestamp of data range (for flush)
    pub data_end_ms: Option<i64>,  // End timestamp of data range (for flush)
}

/// Thread-safe event collector for parallel worker timeline visualization.
#[derive(Debug)]
pub struct Timeline {
    events: std::sync::Mutex<Vec<TimelineEvent>>,
    start_instant: Instant,
}

impl Timeline {
    pub fn new() -> Self {
        Self {
            events: std::sync::Mutex::new(Vec::new()),
            start_instant: Instant::now(),
        }
    }
    
    pub fn record(&self, worker_id: usize, event_type: &str) {
        self.record_with_metadata(worker_id, event_type, None, None, None);
    }
    
    pub fn record_with_metadata(
        &self, 
        worker_id: usize, 
        event_type: &str,
        doc_count: Option<usize>,
        data_start_ms: Option<i64>,
        data_end_ms: Option<i64>
    ) {
        let timestamp_ms = self.start_instant.elapsed().as_millis();
        if let Ok(mut events) = self.events.lock() {
            events.push(TimelineEvent {
                worker_id,
                timestamp_ms,
                event_type: event_type.to_string(),
                doc_count,
                data_start_ms,
                data_end_ms,
            });
        }
    }
    
    pub fn get_all(&self) -> Vec<TimelineEvent> {
        self.events.lock().map(|e| e.clone()).unwrap_or_default()
    }
    
    pub fn save_to_file(&self, path: &str) -> std::io::Result<()> {
        let events = self.get_all();
        let json = serde_json::json!({
            "events": events,
            "total_events": events.len(),
        });
        std::fs::write(path, serde_json::to_string_pretty(&json)?)?;
        Ok(())
    }
}


// ============================================================================
// FLUSH STATISTICS
// ============================================================================

/// Per-file metrics collected during Parquet write for analysis.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FileStats {
    pub worker_id: usize,
    pub file_number: usize,
    pub doc_count: usize,
    pub start_date: String,  // Human-readable ISO 8601
    pub end_date: String,    // Human-readable ISO 8601
    pub time_span_hours: f64,
    pub buffer_size_mb: f64,
    pub file_path: String,
}

/// Thread-safe stats collector for all workers. Exports to file_stats.json.
#[derive(Debug)]
pub struct StatsCollector {
    stats: std::sync::Mutex<Vec<FileStats>>,
    total_files: AtomicUsize,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self {
            stats: std::sync::Mutex::new(Vec::new()),
            total_files: AtomicUsize::new(0),
        }
    }
    
    pub fn record(&self, stat: FileStats) {
        self.total_files.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut stats) = self.stats.lock() {
            stats.push(stat);
        }
    }
    
    pub fn get_all(&self) -> Vec<FileStats> {
        self.stats.lock().map(|s| s.clone()).unwrap_or_default()
    }
    
    pub fn total_files(&self) -> usize {
        self.total_files.load(Ordering::Relaxed)
    }
    
    /// Write all stats to a JSON file in the cache directory
    pub fn write_to_json(&self, cache_dir: &str) -> std::io::Result<String> {
        let stats = self.get_all();
        let json = serde_json::to_string_pretty(&stats)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        
        let filepath = format!("{}/file_stats.json", cache_dir);
        std::fs::write(&filepath, &json)?;
        Ok(filepath)
    }
}

// ============================================================================
// MEMORY-AWARE BUFFERING
// ============================================================================

/// Document buffer that triggers flush when reaching memory limit.
/// Uses 15x multiplier for heap size estimation (measured empirically).
#[derive(Debug)]
pub struct MemoryAwareBuffer {
    docs: Vec<Document>,
    max_bytes: usize,
    actual_bytes_per_doc: usize,  // Measured from first batch
    measured: bool,                // Have we done one-time measurement?
    estimated_current_bytes: usize,
}

impl MemoryAwareBuffer {
    pub fn new(max_memory_mb: usize, avg_doc_size_bytes: usize) -> Self {
        let max_bytes = max_memory_mb * 1024 * 1024;
        
        // Conservative initial estimate: 15x multiplier (measured 14.8x in tests)
        // This will be replaced by actual measurement after first 10 docs
        let initial_estimate = avg_doc_size_bytes * 15;
        
        // Memory tracking with 15x multiplier (measured 14.8x)
        
        Self {
            docs: Vec::new(),
            max_bytes,
            actual_bytes_per_doc: initial_estimate,
            measured: false,
            estimated_current_bytes: 0,
        }
    }
    
    pub fn add(&mut self, doc: Document) {
        self.docs.push(doc);
        
        // One-time measurement after first 10 docs
        if !self.measured && self.docs.len() == 10 {
            self.measure_actual_memory();
            self.measured = true;
        }
        
        self.estimated_current_bytes += self.actual_bytes_per_doc;
    }
    
    pub fn measure_actual_memory(&mut self) {
        // Measure actual memory by sampling first 10 docs
        let mut total_serialized = 0;
        for doc in &self.docs {
            if let Ok(bson_bytes) = bson::to_vec(doc) {
                total_serialized += bson_bytes.len();
            }
        }
        
        let avg_serialized = total_serialized / self.docs.len();
        
        // Use measured 15x multiplier (empirically determined from measure_doc_memory test)
        let measured_bytes_per_doc = avg_serialized * 15;
        
        // One-time measurement complete
        
        // Update our estimate
        self.actual_bytes_per_doc = measured_bytes_per_doc;
        
        // Recalculate current bytes based on actual measurement
        self.estimated_current_bytes = self.docs.len() * self.actual_bytes_per_doc;
    }
    
    pub fn approx_mb(&self) -> f64 {
        self.estimated_current_bytes as f64 / (1024.0 * 1024.0)
    }
    
    pub fn should_flush(&self) -> bool {
        self.estimated_current_bytes >= self.max_bytes
    }
    
    pub fn len(&self) -> usize {
        self.docs.len()
    }
    
    pub fn clear(&mut self) {
        self.docs.clear();
        self.estimated_current_bytes = 0;
        // Keep measured flag and actual_bytes_per_doc for next batch
    }
    
    pub fn take_docs(&mut self) -> Vec<Document> {
        self.estimated_current_bytes = 0;
        std::mem::take(&mut self.docs)
    }
}

// ============================================================================
// SORT SPECIFICATION
// ============================================================================

/// Re-exported from bson_sort module. Format: Vec<(field_name, direction)> where direction is 1 (ASC) or -1 (DESC).
pub use super::bson_sort::SortSpec;

/// Result summary returned to Python after fetch completion.
#[derive(Debug)]
pub struct FetchResult {
    pub total_docs: usize,
    pub total_files: usize,
    pub duration_secs: f64,
    pub stats_file: Option<String>,
}

/// Single field definition: name and type (e.g., "float", "list:int", "any").
#[derive(Debug, Clone, serde::Deserialize)]
pub struct FieldSpec {
    pub name: String,
    #[serde(rename = "kind")]
    pub field_type: String,
}


/// Schema from Python with time_field and column definitions.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SchemaSpec {
    pub time_field: String,
    pub fields: Vec<FieldSpec>,
}

// ============================================================================
// CHUNK DEFINITIONS
// ============================================================================

/// BSON chunk with MongoDB filter. Supports ObjectId, datetime, and complex queries.
#[derive(Debug, Clone)]
pub struct BsonChunk {
    pub filter: Document,
    pub chunk_idx: i32,
    pub start_ms: Option<i64>,  // For metadata/reporting; None for partial brackets
    pub end_ms: Option<i64>,
}
