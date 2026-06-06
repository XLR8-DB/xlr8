"""
Parquet storage layer for XLR8.

Provides efficient storage components for MongoDB query results:

- Reader: Batch-aware Parquet reader for DataFrame construction
- Cache: Query-specific cache management with deterministic hashing
- CacheHandler: Handle to query existing Parquet cache with MQL filters
- CacheCursor: Cursor-like object for filtered Parquet cache queries
- mql_filter: MQL-to-DuckDB-SQL filter translator
"""

from .cache import CacheManager, hash_query
from .cache_handler import CacheCursor, CacheHandler
from .mql_filter import translate_mql_to_sql
from .reader import ParquetReader

__all__ = [
    "CacheCursor",
    "CacheHandler",
    "CacheManager",
    "ParquetReader",
    "hash_query",
    "translate_mql_to_sql",
]
