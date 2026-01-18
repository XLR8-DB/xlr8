"""
Parquet storage layer for XLR8.

Provides efficient storage components for MongoDB query results:

- Reader: Batch-aware Parquet reader for DataFrame construction
- Cache: Query-specific cache management with deterministic hashing
"""

from .cache import CacheManager, hash_query
from .reader import ParquetReader

__all__ = [
    "ParquetReader",
    "CacheManager",
    "hash_query",
]
