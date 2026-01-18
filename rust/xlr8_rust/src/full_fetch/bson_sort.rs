//! MongoDB-compliant BSON comparison for sorting.
//!
//! Provides BSON value extraction and comparison following MongoDB's type ordering rules.
//! Used for sorting Arrow RecordBatch rows and BSON documents in-memory.

use arrow::array::{Array, StructArray, Float64Array, Int32Array, Int64Array, StringArray, BooleanArray, TimestampMillisecondArray};
use bson::{Bson, Document};

/// Sort specification: Vec of (field_name, direction) where direction is 1 (ASC) or -1 (DESC)
pub type SortSpec = Vec<(String, i32)>;

/// Extract a BSON value from an Arrow StructArray (Types.Any) at a given row index.
///
/// The StructArray has 13 nullable fields - only one should be non-null per row.
/// Returns `Bson::Null` if no field is populated.
pub fn extract_bson_value(struct_array: &StructArray, row_idx: usize) -> Bson {
    // Try each field in the Any struct to find the non-null value
    // The Any struct has 13 fields: float, int32, int64, string, objectid, etc.
    
    // Check float_value
    if let Some(col) = struct_array.column_by_name("float_value") {
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            if !arr.is_null(row_idx) {
                return Bson::Double(arr.value(row_idx));
            }
        }
    }
    
    // Check int32_value
    if let Some(col) = struct_array.column_by_name("int32_value") {
        if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
            if !arr.is_null(row_idx) {
                return Bson::Int32(arr.value(row_idx));
            }
        }
    }
    
    // Check int64_value
    if let Some(col) = struct_array.column_by_name("int64_value") {
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            if !arr.is_null(row_idx) {
                return Bson::Int64(arr.value(row_idx));
            }
        }
    }
    
    // Check string_value
    if let Some(col) = struct_array.column_by_name("string_value") {
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            if !arr.is_null(row_idx) {
                return Bson::String(arr.value(row_idx).to_string());
            }
        }
    }
    
    // Check bool_value
    if let Some(col) = struct_array.column_by_name("bool_value") {
        if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
            if !arr.is_null(row_idx) {
                return Bson::Boolean(arr.value(row_idx));
            }
        }
    }
    
    // Check datetime_value
    if let Some(col) = struct_array.column_by_name("datetime_value") {
        if let Some(arr) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
            if !arr.is_null(row_idx) {
                let millis = arr.value(row_idx);
                return Bson::DateTime(bson::DateTime::from_millis(millis));
            }
        }
    }
    
    // Check null_value last
    if let Some(col) = struct_array.column_by_name("null_value") {
        if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
            if !arr.is_null(row_idx) && arr.value(row_idx) {
                return Bson::Null;
            }
        }
    }
    
    Bson::Null
}

/// Get BSON type priority for MongoDB sorting order.
/// Reference: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
pub fn bson_type_priority(value: Option<&bson::Bson>) -> i32 {
    match value {
        None => 0,  // Null/missing
        Some(bson::Bson::MinKey) => -1,
        Some(bson::Bson::Null) => 0,
        Some(bson::Bson::Double(_)) | Some(bson::Bson::Int32(_)) | 
        Some(bson::Bson::Int64(_)) | Some(bson::Bson::Decimal128(_)) => 1,  // Numbers
        Some(bson::Bson::String(_)) | Some(bson::Bson::Symbol(_)) => 2,
        Some(bson::Bson::Document(_)) => 3,
        Some(bson::Bson::Array(_)) => 4,
        Some(bson::Bson::Binary(_)) => 5,
        Some(bson::Bson::ObjectId(_)) => 6,
        Some(bson::Bson::Boolean(_)) => 7,
        Some(bson::Bson::DateTime(_)) => 8,
        Some(bson::Bson::Timestamp(_)) => 9,
        Some(bson::Bson::RegularExpression(_)) => 10,
        Some(bson::Bson::JavaScriptCode(_)) => 11,
        Some(bson::Bson::JavaScriptCodeWithScope(_)) => 12,
        Some(bson::Bson::MaxKey) => 100,
        _ => 0,
    }
}

/// Sort documents in-place using MongoDB-compatible ordering.
/// Supports multi-field sorting with independent ASC/DESC directions.
pub fn sort_documents(docs: &mut [Document], sort_spec: &SortSpec) {
    if sort_spec.is_empty() {
        return;
    }
    
    docs.sort_by(|a, b| {
        for (field, direction) in sort_spec {
            let val_a = a.get(field);
            let val_b = b.get(field);
            
            let cmp = compare_bson_values(val_a, val_b);
            
            if cmp != std::cmp::Ordering::Equal {
                return if *direction >= 0 { cmp } else { cmp.reverse() };
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Parse JSON-encoded sort specification from Python.
/// Returns None for "null", empty string, or invalid JSON.
pub fn parse_sort_spec(json: &str) -> Option<SortSpec> {
    if json == "null" || json.is_empty() {
        return None;
    }
    
    // Parse as Vec<[field, direction]>
    let parsed: Result<Vec<(String, i32)>, _> = serde_json::from_str(json);
    parsed.ok()
}

/// Compare two BSON values according to MongoDB ordering.
/// Compares by type priority first, then by value within same type.
pub fn compare_bson_values(a: Option<&bson::Bson>, b: Option<&bson::Bson>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    
    let type_a = bson_type_priority(a);
    let type_b = bson_type_priority(b);
    
    // First compare by type priority
    if type_a != type_b {
        return type_a.cmp(&type_b);
    }
    
    // Same type - compare values
    match (a, b) {
        (None, None) => Ordering::Equal,
        (Some(bson::Bson::Null), Some(bson::Bson::Null)) => Ordering::Equal,
        
        // Numbers - convert to f64 for comparison
        (Some(av), Some(bv)) if type_a == 1 => {
            let fa = match av {
                bson::Bson::Double(d) => *d,
                bson::Bson::Int32(i) => *i as f64,
                bson::Bson::Int64(i) => *i as f64,
                _ => 0.0,
            };
            let fb = match bv {
                bson::Bson::Double(d) => *d,
                bson::Bson::Int32(i) => *i as f64,
                bson::Bson::Int64(i) => *i as f64,
                _ => 0.0,
            };
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }
        
        // Strings
        (Some(bson::Bson::String(sa)), Some(bson::Bson::String(sb))) => sa.cmp(sb),
        
        // ObjectIds
        (Some(bson::Bson::ObjectId(oa)), Some(bson::Bson::ObjectId(ob))) => oa.cmp(ob),
        
        // Booleans
        (Some(bson::Bson::Boolean(ba)), Some(bson::Bson::Boolean(bb))) => ba.cmp(bb),
        
        // DateTimes
        (Some(bson::Bson::DateTime(da)), Some(bson::Bson::DateTime(db))) => {
            da.timestamp_millis().cmp(&db.timestamp_millis())
        }
        
        _ => Ordering::Equal,
    }
}