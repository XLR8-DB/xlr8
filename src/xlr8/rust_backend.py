"""Python bridge for the native Rust backend.

This module provides the Rust-accelerated functions for XLR8.
The Rust backend is REQUIRED - not optional.

Why Rust?
---------
- 10-15x faster than Python for encoding/decoding operations
- GIL-free execution enables true parallelism
- Zero-copy Arrow operations
- Memory-efficient BSON processing

Key Functions:
--------------
- fetch_chunks_bson: Parallel MongoDB fetches with GIL-free execution
- encode_any_values_to_arrow: Fast encoding for Types.Any fields
- decode_any_struct_arrow: Fast decoding from Arrow structs

All functions are implemented in Rust (see rust/xlr8_rust/) and
exposed via PyO3 bindings.

Usage:
------
    from xlr8.rust_backend import encode_any_values_to_arrow

    values = [42.5, "hello", None, True]
    arrow_array = encode_any_values_to_arrow(values)
"""

import _xlr8_rust as _native  # type: ignore[import-not-found]

# GIL-FREE: BSON-based chunks (Phase 1 integration)
# Accepts BSON-serialized chunks for proper ObjectId, datetime handling
fetch_chunks_bson = _native.fetch_chunks_bson

# Fast Arrow-native decoder - takes PyArrow StructArray directly
# Operates on Arrow memory directly for ~44x speedup vs Python iteration
decode_any_struct_arrow = _native.decode_any_struct_arrow

# Fast Arrow-native encoder - takes Python list, returns PyArrow StructArray
# Operates directly in Rust for ~10x speedup vs Python iteration
encode_any_values_to_arrow = _native.encode_any_values_to_arrow
