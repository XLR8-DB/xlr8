"""
Execution coordinator for parallel query execution.

================================================================================
DATA FLOW - EXECUTION ORCHESTRATION
================================================================================

This is the HEART of XLR8. It coordinates the entire parallel fetch pipeline.

EXECUTION FLOW:
────────────────────────────────────────────────────────────────────────────────

execute_parallel_stream_to_cache() is called with:
  - pymongo_collection: The MongoDB collection
  - filter_dict: The user's query
  - schema: Type definitions for encoding
  - cache_manager: Where to write Parquet files

┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 1: BUILD BRACKETS                                                      │
│ Query -> List[Bracket]                                                      │
│                                                                             │
│ Example:                                                                    │
│ {"$or": [...], "timestamp": {...}}                                          │
│           v                                                                 │
│ [Bracket(static={"sensor_id": "64a..."}, time=Jan-Jul),                  │
│  Bracket(static={"sensor_id": "64b..."}, time=Jan-Jul)]                  │
└─────────────────────────────────────────────────────────────────────────────┘
                              v
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 2: BUILD EXECUTION PLAN                                                │
│ Time range + RAM budget -> workers, batch_size, chunk_minutes               │
│                                                                             │
│ Example (6-month range, 2000MB RAM, max 10 workers):                        │
│ ExecutionPlan(                                                              │
│     worker_count=10,                                                        │
│     batch_size_docs=50000,                                                  │
│     chunk_size_minutes=1440,                                                │
│     estimated_ram_mb=1800                                                   │
│ )                                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                              v
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 3: CHUNK TIME RANGES                                                   │
│ Each bracket's time range -> multiple chunks                                │
│                                                                             │
│ Example (Bracket 1 with Jan-Jul range, 14-day chunks):                      │
│ -> Chunk 1.1: Jan 1-15 with filter {"sensor_id": "64a..."}               │
│ -> Chunk 1.2: Jan 15-29 with filter {"sensor_id": "64a..."}              │
│ -> ...                                                                      │
│ -> Chunk 1.13: Jun 17 - Jul 1                                               │
│                                                                             │
│ Total: 13 chunks x 2 brackets = 26 work items                               │
└─────────────────────────────────────────────────────────────────────────────┘
                              v
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 4: PARALLEL RUST FETCH (rust_backend.fetch_chunks_bson)                │
│ Rust backend processes all chunks concurrently in parallel workers          │
│                                                                             │
│ Worker 0: Grabs Chunk 1 -> Fetch 45K docs -> Write part_0000.parquet        │
│ Worker 1: Grabs Chunk 2 -> Fetch 52K docs -> Write part_0001.parquet        │
│ ...                                                                         │
│ Worker 9: Grabs Chunk 10 -> Fetch 38K docs -> Write part_0009.parquet       │
│                                                                             │
│ All I/O happens in Rust (GIL-free, tokio async MongoDB client)              │
│ Workers pull more chunks as they finish until queue is empty                │
└─────────────────────────────────────────────────────────────────────────────┘
                              v
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 5: RETURN STATS                                                        │
│ {                                                                           │
│     "total_docs": 500000,                                                   │
│     "total_files": 26,                                                      │
│     "duration_s": 12.5,                                                     │
│     "workers": 10                                                           │
│ }                                                                           │
└─────────────────────────────────────────────────────────────────────────────┘

================================================================================
"""

import json
import logging
import warnings
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from bson import encode as bson_encode

from xlr8.analysis import chunk_time_range
from xlr8.analysis.brackets import build_brackets_for_find
from xlr8.analysis.inspector import validate_query_for_chunking
from xlr8.execution.planner import build_execution_plan
from xlr8.schema import Schema

logger = logging.getLogger(__name__)


def execute_parallel_stream_to_cache(
    pymongo_collection,
    filter_dict: Dict[str, Any],
    schema: Schema,
    cache_manager: Any,
    *,
    projection: Optional[Dict[str, int]] = None,
    approx_document_size_bytes: int = 500,
    available_ram_gb: Optional[float] = None,
    max_workers: int = 4,
    peak_ram_limit_mb: int = 512,
    chunking_granularity: Optional[timedelta] = None,
    mongo_uri: Union[str, Callable[[], str], None] = None,
    sort_spec: Optional[List[Tuple[str, int]]] = None,
    row_group_size: Optional[int] = None,
) -> Dict[str, Any]:
    """Execute query with streaming to Parquet cache.

    Uses bracket-based chunking and memory-aware execution planning.
    Streams results directly to cache shards.

    Args:
        pymongo_collection: PyMongo collection instance
        filter_dict: MongoDB query filter
        schema: Schema for Parquet encoding
        cache_manager: CacheManager instance
        projection: MongoDB projection
        approx_document_size_bytes: Average doc size for RAM planning
        available_ram_gb: Override RAM detection
        max_workers: Maximum workers (default: 4)
        peak_ram_limit_mb: RAM budget (default: 512)
        chunking_granularity: Time granularity for chunking (e.g.,
            `timedelta(minutes=10)`). If None, uses single-worker mode without chunking.
        mongo_uri: MongoDB connection string or callable that returns one.
        sort_spec: Sort specification for pre-sorting during cache write.
            Format: `[(field, direction), ...]`
            Where direction is `1` (ASC) or `-1` (DESC).

    Returns:
        Dict with total_docs, total_files, duration_s, workers
    """

    # High-level safety: forbidden operators / nested OR
    is_valid, reason = validate_query_for_chunking(
        filter_dict,
        schema.time_field,
    )

    # NOTE: We no longer use single-worker fallback. All queries go through
    # the parallel Rust path, even if they have no time bounds (unbounded).
    # The brackets algorithm handles unbounded queries as unchunked brackets.

    if not is_valid:
        # Reject queries with forbidden operators (geospatial, $expr, etc.)
        raise ValueError(f"Query not executable: {reason}")

    # Build brackets (static_filter + timerange) like
    ok, bracket_reason, brackets, _ = build_brackets_for_find(
        filter_dict, schema.time_field
    )
    if not ok:
        warnings.warn(
            (
                f"Query not chunkable ({bracket_reason}). "
                "Falling back to single-worker mode."
            ),
            UserWarning,
            stacklevel=2,
        )
        # Fall back to single-worker, unchunked execution
        max_workers = 1
        chunking_granularity = None
        # When query is not chunkable, brackets will be empty
        # We'll execute the entire query without brackets

    if not brackets:
        # No brackets or non-chunkable query - execute full query in single worker
        if max_workers == 1 and chunking_granularity is None:
            # This is expected for non-chunkable queries
            # Create a single "bracket" with the original filter, no time chunking
            logger.info("Non-chunkable query - executing as single unchunked query")
            # We'll handle this as a special case below
            brackets = []
            start_time = None
            end_time = None
        else:
            raise ValueError("No time ranges found in chunkable query")
    else:
        # Separate full and partial brackets for planning
        full_brackets_plan = [b for b in brackets if b.timerange.is_full]

        # Derive time span ONLY from full brackets (partial ones are executed unchunked)
        if full_brackets_plan:
            los = [
                b.timerange.lo for b in full_brackets_plan if b.timerange.lo is not None
            ]
            his = [
                b.timerange.hi for b in full_brackets_plan if b.timerange.hi is not None
            ]
            start_time = min(los)
            end_time = max(his)
        else:
            # All brackets are unchunked (partial or unbounded)
            start_time = None
            end_time = None
            logger.info(
                "All brackets are unchunked (partial/unbounded) - "
                "will execute as single queries"
            )

    # Get avg doc size from schema (default to approx_document_size_bytes)
    avg_doc_size = getattr(schema, "avg_doc_size_bytes", approx_document_size_bytes)

    effective_peak_ram_mb = peak_ram_limit_mb
    if available_ram_gb is not None:
        effective_peak_ram_mb = int(available_ram_gb * 1024)

    # Count unchunked queries (partial + unbounded brackets)
    # For non-chunkable queries (empty brackets), count as 1 unchunked query
    unchunked_brackets_count = (
        sum(1 for b in brackets if not b.timerange.is_full) if brackets else 1
    )

    exec_plan = build_execution_plan(
        start_time=start_time,
        end_time=end_time,
        avg_doc_size_bytes=avg_doc_size,
        max_workers=max_workers,
        peak_ram_limit_mb=effective_peak_ram_mb,
        chunking_granularity=chunking_granularity,
        # Always pass: planner combines with time chunks.
        num_unchunked_queries=unchunked_brackets_count,
    )

    # Build chunks (opt): group brackets by time range.
    # If multiple $or branches share the same time range, combine them into one $or
    # query per chunk instead of creating separate chunks for each branch.

    # Handle non-chunkable queries (empty brackets)
    if not brackets:
        # Execute entire query as single unchunked query
        chunks = [(filter_dict, 0, None, None)]
        logger.info("Executing non-chunkable query as single chunk")
    else:
        # Separate full and unchunked brackets
        full_brackets: List = []
        unchunked_brackets: List = []

        for b in brackets:
            if b.timerange.is_full:
                full_brackets.append(b)
            else:
                unchunked_brackets.append(b)

        brackets_by_timerange: Dict[Tuple[datetime, datetime], List] = defaultdict(list)

        for b in full_brackets:
            brackets_by_timerange[(b.timerange.lo, b.timerange.hi)].append(b)

        time_chunks_per_bracket: List[
            Tuple[Dict[str, Any], int, datetime, datetime]
        ] = []
        chunk_index = 0

        # Process full brackets - chunk them
        for (lo, hi), bracket_group in brackets_by_timerange.items():
            br_chunks = chunk_time_range(
                start=lo,
                end=hi,
                chunk_size=exec_plan.chunk_size,
            )

            for c_start, c_end in br_chunks:
                # Determine if this is the last chunk - preserve original end boundary
                # operator.
                is_last_chunk = c_end == hi

                if len(bracket_group) == 1:
                    # Single bracket - simple filter
                    chunk_filter = dict(bracket_group[0].static_filter)
                    time_clause = {}

                    # Lower bound: always $gte for chunk starts
                    time_clause["$gte"] = c_start

                    # Upper bound: use original operator if last chunk, else $lt
                    if is_last_chunk and bracket_group[0].timerange.hi_inclusive:
                        time_clause["$lte"] = c_end
                    else:
                        time_clause["$lt"] = c_end

                    chunk_filter[schema.time_field] = time_clause
                else:
                    # Multiple brackets with same time range - combine with $or
                    or_branches = []
                    for b in bracket_group:
                        branch = dict(b.static_filter)
                        or_branches.append(branch)

                    time_clause = {}
                    time_clause["$gte"] = c_start

                    # Use original operator if last chunk and ANY bracket is inclusive
                    if is_last_chunk and any(
                        b.timerange.hi_inclusive for b in bracket_group
                    ):
                        time_clause["$lte"] = c_end
                    else:
                        time_clause["$lt"] = c_end

                    chunk_filter = {"$or": or_branches, schema.time_field: time_clause}

                time_chunks_per_bracket.append(
                    (chunk_filter, chunk_index, c_start, c_end)
                )
                chunk_index += 1

        # Process unchunked brackets (partial + unbounded) - execute as single queries
        for b in unchunked_brackets:
            chunk_filter = dict(b.static_filter)
            time_clause = {}
            if b.timerange.lo is not None:
                if b.timerange.lo_inclusive:
                    time_clause["$gte"] = b.timerange.lo
                else:
                    time_clause["$gt"] = b.timerange.lo
            if b.timerange.hi is not None:
                if b.timerange.hi_inclusive:
                    time_clause["$lte"] = b.timerange.hi
                else:
                    time_clause["$lt"] = b.timerange.hi
            if time_clause:
                chunk_filter[schema.time_field] = time_clause

            time_chunks_per_bracket.append(
                (chunk_filter, chunk_index, b.timerange.lo, b.timerange.hi)
            )
            chunk_index += 1
            logger.info("Added unchunked bracket as single query: %s", chunk_filter)

        chunks = time_chunks_per_bracket

    # Cap worker count to actual number of chunks - no point having idle workers
    actual_worker_count = min(exec_plan.worker_count, len(chunks))

    print("\n[Plan] Execution Plan:")
    if start_time is not None and end_time is not None:
        print(f"  - Date range: {start_time.date()} to {end_time.date()}")
        print(f"  - Total days: {(end_time - start_time).days}")
    else:
        print("  - Mode: Unchunked queries (no time range)")
    print(f"  - Chunks: {len(chunks)}")
    print(f"  - Workers: {actual_worker_count}")
    chunk_seconds = int(exec_plan.chunk_size.total_seconds())
    if chunk_seconds > 0:
        if chunk_seconds >= 86400:
            print(f"  - Chunk size: {chunk_seconds // 86400} day(s)")
        elif chunk_seconds >= 3600:
            print(f"  - Chunk size: {chunk_seconds // 3600} hour(s)")
        elif chunk_seconds >= 60:
            print(f"  - Chunk size: {chunk_seconds // 60} minute(s)")
        else:
            print(f"  - Chunk size: {chunk_seconds} second(s)")
    print(f"  - Batch size: {exec_plan.batch_size_docs:,}")
    print(f"  - Flush trigger/worker: {exec_plan.flush_trigger_mb} MB")
    print(f"  - Max Estimated RAM Usage: {exec_plan.estimated_ram_mb:,} MB")

    # =========================================================================
    # RUST BACKEND EXECUTION (Phase 2 - Full GIL-free implementation)
    # =========================================================================
    # All MongoDB fetching, BSON decoding, Arrow conversion, and Parquet writing
    # happens in Rust with NO Python GIL contention.
    #
    # Python's role: memory planning, chunking, BSON serialization, result reading
    # Rust's role: MongoDB client, async/parallel fetch, BSON->Arrow->Parquet
    # =========================================================================

    # Validate mongo_uri is provided
    if mongo_uri is None:
        raise ValueError(
            "mongo_uri is required for Rust backend execution. "
            "Pass it to XLR8Collection constructor or accelerate()."
        )

    # Resolve callable if needed
    resolved_uri = mongo_uri() if callable(mongo_uri) else mongo_uri

    # Ensure cache directory exists
    cache_manager.ensure_cache_dir()

    # Serialize chunks to BSON (handles ObjectId, datetime, etc.)
    chunks_bson = serialize_chunks_for_rust(chunks)

    logger.info(
        "Serialized %d chunks to %d BSON bytes for Rust backend",
        len(chunks),
        len(chunks_bson),
    )

    # Get MongoDB connection details
    db_name = pymongo_collection.database.name
    collection_name = pymongo_collection.name

    # Prepare schema JSON for Rust
    schema_json = json.dumps(schema.to_spec())

    print("  - Mode: RUST BACKEND (GIL-free, tokio async)")

    # Call Rust backend directly!
    from xlr8 import rust_backend

    rust_kwargs: Dict[str, Any] = {
        "mongodb_uri": resolved_uri,
        "db_name": db_name,
        "collection_name": collection_name,
        "chunks_bson": chunks_bson,
        "schema_json": schema_json,
        "cache_dir": str(cache_manager.cache_dir),
        "num_workers": actual_worker_count,
        "batch_size": exec_plan.batch_size_docs,
        "flush_trigger_mb": exec_plan.flush_trigger_mb,
        "avg_doc_size_bytes": schema.avg_doc_size_bytes,
        "sort_spec_json": json.dumps(sort_spec) if sort_spec else "null",
        "time_field": schema.time_field,
        "projection_json": json.dumps(projection) if projection else "null",
    }
    if row_group_size is not None:
        rust_kwargs["row_group_size"] = row_group_size

    result = rust_backend.fetch_chunks_bson(**rust_kwargs)

    logger.info(
        "Rust execution complete: %d docs, %d files, %.2fs",
        result["total_docs"],
        result["total_files"],
        result["duration_secs"],
    )

    # Convert Rust result format to match Python pool format
    # (for compatibility with existing result reading code)
    result["workers"] = actual_worker_count
    result["duration_s"] = result["duration_secs"]  # Add Python format key
    result["cache_dir"] = str(cache_manager.cache_dir)

    return result


# ============================================================================
# RUST BACKEND INTEGRATION (Phase 2)
# ============================================================================


def serialize_chunks_for_rust(
    chunks: Sequence[
        Tuple[Dict[str, Any], int, Optional[datetime], Optional[datetime]]
    ],
) -> bytes:
    """
    Serialize chunks to BSON bytes for Rust backend.

    This function converts Python chunks (which may contain ObjectId, datetime,
    and other BSON types) into BSON bytes that Rust can deserialize correctly.

    Args:
        chunks: List of (filter, chunk_idx, c_start, c_end) tuples from executor

    Returns:
        BSON-encoded bytes ready for Rust's fetch_chunks_bson()

    Example:
        chunks = [
            ({"metadata.instrument": "AUD_CAD", "timestamp": {...}}, 0, start, end)
        ]
        bson_bytes = serialize_chunks_for_rust(chunks)
        # Pass bson_bytes to rust_backend.fetch_chunks_bson(chunks_bson=bson_bytes)
    """
    bson_chunks = []

    for chunk_filter, chunk_idx, c_start, c_end in chunks:
        chunk_doc = {
            "filter": chunk_filter,  # Contains ObjectId, datetime, etc.
            "chunk_idx": chunk_idx,
        }
        # Handle None timestamps for partial brackets (unbounded queries)
        if c_start is not None:
            chunk_doc["start_ms"] = int(c_start.timestamp() * 1000)
        if c_end is not None:
            chunk_doc["end_ms"] = int(c_end.timestamp() * 1000)
        bson_chunks.append(chunk_doc)

    # Wrap in document (Rust expects {"chunks": [...]})
    wrapper = {"chunks": bson_chunks}

    # Encode to BSON bytes using pymongo's bson module
    return bson_encode(wrapper)


__all__ = [
    "execute_parallel_stream_to_cache",
    "serialize_chunks_for_rust",
]
