//! Schema types and Arrow schema building for XLR8.
//!
//! This module provides the schema specifications and Arrow schema generation
//! for converting MongoDB documents to Parquet format.

use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use serde::Deserialize;

/// Specification for a single field in the schema.
#[derive(Deserialize, Clone)]
pub struct FieldSpec {
    pub name: String,
    pub kind: String,
    /// Timestamp unit: "ns" (nanoseconds), "us" (microseconds), "ms" (milliseconds), "s" (seconds)
    #[serde(default)]
    pub unit: Option<String>,
    /// Timezone for timestamps (e.g., "UTC")
    #[serde(default)]
    pub tz: Option<String>,
}

/// Full schema specification matching Python's Schema class.
#[derive(Deserialize)]
pub struct SchemaSpec {
    /// Schema version (reserved for future use)
    #[serde(rename = "version")]
    pub _version: u8,
    pub time_field: String,
    /// Average document size in bytes (passed separately to fetch_chunks_bson, not used here)
    #[serde(default, rename = "avg_doc_size_bytes")]
    pub _avg_doc_size_bytes: Option<u64>,
    pub fields: Vec<FieldSpec>,
}

/// Create the Arrow struct type for Types.Any (13-field full BSON support).
/// This is the production schema used by encode_any_values_to_arrow and fetch_chunks_bson.
pub fn create_any_struct_type_full() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("float_value", DataType::Float64, true),
        Field::new("int32_value", DataType::Int32, true),
        Field::new("int64_value", DataType::Int64, true),
        Field::new("string_value", DataType::Utf8, true),
        Field::new("objectid_value", DataType::Utf8, true),
        Field::new("decimal128_value", DataType::Utf8, true),
        Field::new("regex_value", DataType::Utf8, true),
        Field::new("binary_value", DataType::Utf8, true),
        Field::new("document_value", DataType::Utf8, true),
        Field::new("array_value", DataType::Utf8, true),
        Field::new("bool_value", DataType::Boolean, true),
        Field::new(
            "datetime_value",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("null_value", DataType::Boolean, true),
    ]))
}

/// Build Arrow schema from FieldSpec list.
/// Respects unit and tz settings for timestamp fields.
pub fn build_arrow_schema(fields: &[FieldSpec]) -> ArrowSchema {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| match f.kind.as_str() {
            "timestamp" => {
                // Parse timestamp unit from Python schema
                let time_unit = match f.unit.as_deref() {
                    Some("s") => TimeUnit::Second,
                    Some("ms") => TimeUnit::Millisecond,
                    Some("us") => TimeUnit::Microsecond,
                    Some("ns") => TimeUnit::Nanosecond,
                    _ => TimeUnit::Millisecond, // Default to ms (MongoDB's standard)
                };
                // Get timezone (default to UTC if not specified)
                let tz = f.tz.clone().or_else(|| Some("UTC".to_string()));
                Field::new(
                    &f.name,
                    DataType::Timestamp(time_unit, tz.map(|s| s.into())),
                    true,
                )
            }
            "objectid" | "string" => Field::new(&f.name, DataType::Utf8, true),
            "int64" => Field::new(&f.name, DataType::Int64, true),
            "float64" => Field::new(&f.name, DataType::Float64, true),
            "bool" => Field::new(&f.name, DataType::Boolean, true),
            "any" => Field::new(&f.name, create_any_struct_type_full(), true),
            _ => Field::new(&f.name, DataType::Utf8, true),
        })
        .collect();
    ArrowSchema::new(arrow_fields)
}
