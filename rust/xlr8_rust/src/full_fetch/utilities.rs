//! Utility functions for timestamp extraction and Parquet filename generation.

use bson::Document;

/// Extract min and max timestamps (in milliseconds) from documents.
pub fn get_timestamp_range(docs: &[Document], time_field: &str) -> (Option<i64>, Option<i64>) {
    let mut min_ts: Option<i64> = None;
    let mut max_ts: Option<i64> = None;
    
    for doc in docs {
        if let Some(bson::Bson::DateTime(dt)) = doc.get(time_field) {
            let ts = dt.timestamp_millis();
            min_ts = Some(min_ts.map_or(ts, |m| m.min(ts)));
            max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
        }
    }
    
    (min_ts, max_ts)
}

/// Generate Parquet filename with embedded timestamp range (e.g., "ts_1704326400_1704412800_part_0001.parquet").
pub fn make_date_range_filename(cache_dir: &str, min_ts: Option<i64>, max_ts: Option<i64>, file_count: usize) -> String {
    match (min_ts, max_ts) {
        (Some(min), Some(max)) => {
            // Format as ISO date-hour for compactness  
            let min_secs = min / 1000;
            let max_secs = max / 1000;
            format!(
                "{}/ts_{}_{}_part_{:04}.parquet",
                cache_dir, min_secs, max_secs, file_count
            )
        }
        _ => {
            // Fallback to simple numbering
            format!("{}/part_{:04}.parquet", cache_dir, file_count)
        }
    }
}