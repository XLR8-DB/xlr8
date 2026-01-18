//! Python value encoder for Arrow StructArray (Types.Any columns).
//!
//! This module provides fast encoding of Python mixed-type values into Arrow
//! StructArray format with 13 typed fields supporting all MongoDB BSON types.
//!
//! Used by xlr8.storage.reader for decoding cached Parquet data.

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    StructArray, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDateTime, PyDict, PyFloat, PyInt, PyList, PyString};
use pyo3_arrow::PyArray;
use std::sync::Arc;

/// Builder for encoding Python values to 13-field Any struct.
/// Matches Python's Types.Any() schema exactly.
pub struct AnyEncoderBuilder {
    float_values: Float64Builder,
    int32_values: Int32Builder,
    int64_values: Int64Builder,
    string_values: StringBuilder,
    objectid_values: StringBuilder,
    decimal128_values: StringBuilder,
    regex_values: StringBuilder,
    binary_values: StringBuilder,
    document_values: StringBuilder,
    array_values: StringBuilder,
    bool_values: BooleanBuilder,
    datetime_values: TimestampMillisecondBuilder,
    null_values: BooleanBuilder,
}

impl AnyEncoderBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        AnyEncoderBuilder {
            float_values: Float64Builder::with_capacity(capacity),
            int32_values: Int32Builder::with_capacity(capacity),
            int64_values: Int64Builder::with_capacity(capacity),
            string_values: StringBuilder::with_capacity(capacity, capacity * 32),
            objectid_values: StringBuilder::with_capacity(capacity, capacity * 24),
            decimal128_values: StringBuilder::with_capacity(capacity, capacity * 40),
            regex_values: StringBuilder::with_capacity(capacity, capacity * 64),
            binary_values: StringBuilder::with_capacity(capacity, capacity * 128),
            document_values: StringBuilder::with_capacity(capacity, capacity * 256),
            array_values: StringBuilder::with_capacity(capacity, capacity * 128),
            bool_values: BooleanBuilder::with_capacity(capacity),
            datetime_values: TimestampMillisecondBuilder::with_capacity(capacity),
            null_values: BooleanBuilder::with_capacity(capacity),
        }
    }

    pub fn append_null(&mut self) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_value(true);
    }

    pub fn append_float(&mut self, v: f64) {
        self.float_values.append_value(v);
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_int32(&mut self, v: i32) {
        self.float_values.append_null();
        self.int32_values.append_value(v);
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_int64(&mut self, v: i64) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_value(v);
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_string(&mut self, v: &str) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_value(v);
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_objectid(&mut self, v: &str) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_value(v);
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_decimal128(&mut self, v: &str) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_value(v);
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    #[allow(dead_code)]
    pub fn append_regex(&mut self, v: &str) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_value(v);
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_binary(&mut self, v: &str) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_value(v);
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_document(&mut self, v: &str) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_value(v);
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_array(&mut self, v: &str) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_value(v);
        self.bool_values.append_null();
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_bool(&mut self, v: bool) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_value(v);
        self.datetime_values.append_null();
        self.null_values.append_null();
    }

    pub fn append_datetime_ms(&mut self, v: i64) {
        self.float_values.append_null();
        self.int32_values.append_null();
        self.int64_values.append_null();
        self.string_values.append_null();
        self.objectid_values.append_null();
        self.decimal128_values.append_null();
        self.regex_values.append_null();
        self.binary_values.append_null();
        self.document_values.append_null();
        self.array_values.append_null();
        self.bool_values.append_null();
        self.datetime_values.append_value(v);
        self.null_values.append_null();
    }

    pub fn finish(mut self) -> ArrayRef {
        let struct_fields = Fields::from(vec![
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
        ]);

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.float_values.finish()),
            Arc::new(self.int32_values.finish()),
            Arc::new(self.int64_values.finish()),
            Arc::new(self.string_values.finish()),
            Arc::new(self.objectid_values.finish()),
            Arc::new(self.decimal128_values.finish()),
            Arc::new(self.regex_values.finish()),
            Arc::new(self.binary_values.finish()),
            Arc::new(self.document_values.finish()),
            Arc::new(self.array_values.finish()),
            Arc::new(self.bool_values.finish()),
            Arc::new(self.datetime_values.finish()),
            Arc::new(self.null_values.finish()),
        ];

        Arc::new(StructArray::new(struct_fields, arrays, None))
    }
}

/// Encode a Python list of mixed values into an Arrow StructArray.
///
/// Used for encoding Types.Any() columns from Python to Arrow format.
///
/// Input: Python list of mixed values [42.5, 100, "hello", True, None, ...]
/// Output: PyArrow StructArray with 13 typed fields
///
/// Supported Python types:
/// - None -> null_value
/// - bool -> bool_value
/// - int (fits i32) -> int32_value
/// - int (large) -> int64_value
/// - float -> float_value
/// - str -> string_value
/// - bytes -> binary_value (base64 encoded)
/// - datetime -> datetime_value (milliseconds)
/// - dict -> document_value (JSON string)
/// - list -> array_value (JSON string)
/// - ObjectId (via str(obj)) -> objectid_value
/// - Decimal128 -> decimal128_value
#[pyfunction]
pub fn encode_any_values_to_arrow(py: Python<'_>, values: Bound<'_, PyList>) -> PyResult<PyArray> {
    let n = values.len();
    let mut builder = AnyEncoderBuilder::with_capacity(n);

    for item in values.iter() {
        // Check None first
        if item.is_none() {
            builder.append_null();
            continue;
        }

        // Check bool BEFORE int (isinstance(True, int) is True in Python!)
        if let Ok(b) = item.downcast::<PyBool>() {
            builder.append_bool(b.is_true());
            continue;
        }

        // Check int
        if let Ok(i) = item.downcast::<PyInt>() {
            let val: i64 = i.extract()?;
            // Check if fits in i32
            if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                builder.append_int32(val as i32);
            } else {
                builder.append_int64(val);
            }
            continue;
        }

        // Check float
        if let Ok(f) = item.downcast::<PyFloat>() {
            let val: f64 = f.extract()?;
            builder.append_float(val);
            continue;
        }

        // Check string
        if let Ok(s) = item.downcast::<PyString>() {
            let val: &str = s.extract()?;
            builder.append_string(val);
            continue;
        }

        // Check bytes
        if let Ok(b) = item.downcast::<PyBytes>() {
            let bytes: &[u8] = b.extract()?;
            let encoded = BASE64.encode(bytes);
            builder.append_binary(&encoded);
            continue;
        }

        // Check datetime
        if let Ok(dt) = item.downcast::<PyDateTime>() {
            // Get timestamp in milliseconds
            let ts = dt.call_method0("timestamp")?;
            let secs: f64 = ts.extract()?;
            let millis = (secs * 1000.0) as i64;
            builder.append_datetime_ms(millis);
            continue;
        }

        // Check dict (encode as JSON)
        if let Ok(_d) = item.downcast::<PyDict>() {
            // Use Python's json.dumps for reliable serialization
            let json_mod = py.import_bound("json")?;
            let json_str = json_mod.call_method1("dumps", (&item,))?;
            let s: String = json_str.extract()?;
            builder.append_document(&s);
            continue;
        }

        // Check list (encode as JSON)
        if let Ok(_l) = item.downcast::<PyList>() {
            let json_mod = py.import_bound("json")?;
            let json_str = json_mod.call_method1("dumps", (&item,))?;
            let s: String = json_str.extract()?;
            builder.append_array(&s);
            continue;
        }

        // Check for ObjectId by class name
        let type_name = item.get_type().name()?;
        if type_name == "ObjectId" {
            let s = item.str()?.to_string();
            builder.append_objectid(&s);
            continue;
        }

        // Check for Decimal128 by class name
        if type_name == "Decimal128" {
            let s = item.str()?.to_string();
            builder.append_decimal128(&s);
            continue;
        }

        // Fallback: convert to string
        let s = item.str()?.to_string();
        builder.append_string(&s);
    }

    let struct_array = builder.finish();

    // Create the field for the struct array
    let struct_field = Field::new(
        "value",
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
        ])),
        true,
    );

    // Convert to PyArray
    Ok(PyArray::new(struct_array, Arc::new(struct_field)))
}
