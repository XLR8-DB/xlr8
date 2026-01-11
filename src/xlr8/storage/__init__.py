"""
Parquet storage layer for XLR8.

Provides efficient storage components for MongoDB query results:

- Buffers: Memory-aware document buffering (DocumentBuffer, ColumnarDocumentBuffer) TODO
- Writer: Streaming Parquet writer with compression and row group optimization TODO
- Reader: Batch-aware Parquet reader for DataFrame construction TODO
- Cache: Query-specific cache management with deterministic hashing
"""

from .cache import CacheManager, hash_query

__all__ = [
    "CacheManager",
    "hash_query",
]
