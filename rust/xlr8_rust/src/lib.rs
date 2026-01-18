//! XLR8 Rust Backend
//!
//! High-performance native extensions for XLR8 MongoDB acceleration.
//!
//! # Exported Functions
//!
//! - `fetch_chunks_bson`: GIL-free parallel MongoDB fetching with tokio async runtime
//! - `encode_any_values_to_arrow`: Encode Python values to Arrow StructArray
//! - `decode_any_struct_arrow`: Decode Arrow StructArray to Python values
//!
//! # Architecture
//!
//! The crate is organized into modules:
//! - `schema`: Schema types and Arrow schema building
//! - `encoder`: Python value to Arrow StructArray encoding
//! - `decoder`: Arrow StructArray to Python value decoding
//! - `full_fetch`: Full Rust MongoDB client with parallel tokio workers
//!   - `bson_sort`: MongoDB-compliant BSON comparison for sorting
//!   - `arrow_builder`: Arrow RecordBatch construction from BSON documents
//!   - `parquet_writer`: Parquet file writing with ZSTD compression
//!   - `utilities`: Helper functions for timestamp ranges and file naming
//!   - `types`: Timeline tracking and statistics collection
//!
//! # Performance
//!
//! The Rust backend provides significant performance improvements through:
//! - Zero-copy access to Arrow columnar memory
//! - GIL-free execution using py.allow_threads()
//! - Tokio async runtime for parallel MongoDB queries
//! - Memory-aware buffering to minimize file count

use pyo3::prelude::*;

pub mod decoder;
pub mod encoder;
pub mod full_fetch;
pub mod schema;

/// Python module definition for _xlr8_rust
#[pymodule]
fn _xlr8_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(full_fetch::fetch_chunks_bson, m)?)?;
    m.add_function(wrap_pyfunction!(encoder::encode_any_values_to_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(decoder::decode_any_struct_arrow, m)?)?;
    Ok(())
}


