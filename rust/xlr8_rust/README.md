# xlr8_rust

Native Rust extensions for the XLR8 MongoDB acceleration library.

## Overview

This crate provides performance-critical components for XLR8, a Python library that accelerates MongoDB analytical queries through Parquet caching. The Rust backend handles:

- Parallel MongoDB fetching with GIL-free execution
- Encoding Python values to Apache Arrow format
- Decoding Arrow StructArrays back to Python values
- Writing Parquet files with ZSTD compression

## Architecture

```
src/
├── lib.rs              # PyO3 module definition and exports
├── schema.rs           # Arrow schema types and builders
├── encoder.rs          # Python to Arrow encoding (Types.Any)
├── decoder.rs          # Arrow to Python decoding (Types.Any)
└── full_fetch/
    ├── mod.rs          # Main fetch_chunks_bson function
    ├── types.rs        # Buffer, schema, and chunk definitions
    ├── bson_sort.rs    # MongoDB-compliant BSON comparison
    ├── arrow_builder.rs# RecordBatch construction from BSON
    ├── parquet_writer.rs# Parquet file writing
    └── utilities.rs    # Timestamp and filename helpers
```

## Exported Functions

The crate exports three functions to Python via PyO3:

### fetch_chunks_bson

Fetches data from MongoDB in parallel using tokio async workers, converts documents to Arrow columnar format, and writes Parquet files to disk.

```python
from xlr8.rust_backend import fetch_chunks_bson

result = fetch_chunks_bson(
    mongodb_uri="mongodb://localhost:27017",
    db_name="mydb",
    collection_name="trades",
    chunks_bson=bson_encoded_chunks,  # BSON-serialized chunk definitions
    schema_json=schema_json,          # Field names and types
    cache_dir="/path/to/cache",
    num_workers=4,
    batch_size=10000,
    flush_trigger_mb=48,
    avg_doc_size_bytes=250,
    sort_spec_json='[["timestamp", 1]]',
    time_field="timestamp",
    projection_json="null"
)
# Returns: {"total_docs": int, "total_files": int, "duration_secs": float}
```

Key features:
- Releases Python GIL during execution for true parallelism
- BSON-encoded filters preserve MongoDB types (ObjectId, DateTime)
- Memory-aware buffering with configurable flush threshold
- MongoDB-compliant sorting within each output file

### encode_any_values_to_arrow

Encodes a Python list of mixed-type values into an Arrow StructArray with 13 typed fields, supporting all MongoDB BSON types.

```python
from xlr8.rust_backend import encode_any_values_to_arrow

values = [42.5, "hello", None, True, datetime.now()]
arrow_array = encode_any_values_to_arrow(values)
```

The 13-field struct schema supports:
- Numeric: float_value, int32_value, int64_value
- String types: string_value, objectid_value, decimal128_value, regex_value
- Complex types: binary_value, document_value, array_value
- Other: bool_value, datetime_value, null_value

### decode_any_struct_arrow

Decodes an Arrow StructArray back to a Python list of native values.

```python
from xlr8.rust_backend import decode_any_struct_arrow

python_values = decode_any_struct_arrow(arrow_struct_array)
# Returns: [42.5, "hello", None, True, datetime(...)]
```

## Building

This crate is built using [maturin](https://github.com/PyO3/maturin) as part of the XLR8 package.

### Development build

```bash
cd rust/xlr8_rust
maturin develop --release
```

### Production build

```bash
maturin build --release
```

## Dependencies

Key dependencies:
- `pyo3` - Python bindings for Rust
- `pyo3-arrow` - Zero-copy Arrow interop with PyArrow
- `arrow` / `parquet` - Apache Arrow and Parquet support
- `mongodb` - Official MongoDB Rust driver
- `tokio` - Async runtime for parallel fetching
- `bson` - BSON serialization with full type support

See `Cargo.toml` for the complete dependency list.

## Integration with Python

The Rust functions are exposed to Python through the `xlr8.rust_backend` module:

```python
# src/xlr8/rust_backend.py
import _xlr8_rust as _native

fetch_chunks_bson = _native.fetch_chunks_bson
decode_any_struct_arrow = _native.decode_any_struct_arrow
encode_any_values_to_arrow = _native.encode_any_values_to_arrow
```

These functions are used internally by:
- `xlr8.execution.executor` - Calls `fetch_chunks_bson` for MongoDB data fetching
- `xlr8.storage.reader` - Uses `decode_any_struct_arrow` for Parquet reading
- `xlr8.schema.encoder` - Uses `encode_any_values_to_arrow` for value encoding

## Testing

The Rust code is tested through the Python test suite which exercises all PyO3 bindings:

```bash
# From the repository root
uv run pytest
```

## License

Apache-2.0
