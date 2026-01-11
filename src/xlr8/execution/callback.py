"""
Partition-based callback streaming for data lake population among other use cases.

================================================================================
ARCHITECTURE - STREAM TO CALLBACK WITH PARTITIONING
================================================================================

This module implements a two-phase approach:

PHASE 1: Download to Cache (reuses existing Rust backend)
────────────────────────────────────────────────────────────────────────────────
    MongoDB -------> Rust Workers -------> Parquet Cache (on disk)

    Uses execute_parallel_stream_to_cache() - memory-safe.

PHASE 2: Partition + Parallel Callbacks
────────────────────────────────────────────────────────────────────────────────
    1. Build partition plan using DuckDB: TODO
       - Discover unique (time_bucket, partition_key) combinations
       - Create work items for each partition

    2. Execute callbacks in parallel (ThreadPoolExecutor): TODO
       - Each worker: DuckDB query -> PyArrow Table -> decode -> callback()
       - DuckDB releases GIL -> true parallelism
       - User callbacks can use non-picklable objects (boto3, etc.)

EDGE CASES HANDLED:
────────────────────────────────────────────────────────────────────────────────
    - NULL values in partition_by fields -> grouped as one partition
    - Empty partitions (no data in time bucket) -> skipped
    - Parent fields (e.g., "metadata") -> expanded to child fields like
    "metadata.source", etc.
    - Types.Any() fields -> decoded based on any_type_strategy
    - ObjectIds -> converted to strings (same as to_polars)
    - Large partitions -> DuckDB streams internally, memory-safe
    - Timezone handling -> all datetimes normalized to UTC

================================================================================
"""
