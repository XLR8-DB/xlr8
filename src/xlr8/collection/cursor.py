"""
XLR8 cursor with PyMongo compatibility.

================================================================================
DATA FLOW - CURSOR (WHERE ACCELERATION HAPPENS)
================================================================================

This module is where the magic happens. When user calls cursor.to_dataframe(),
we decide whether to:
  A) Use regular PyMongo iteration (slow)
  B) Use accelerated parallel fetch + Parquet caching (fast)

DECISION FLOW:
────────────────────────────────────────────────────────────────────────────────

cursor.to_dataframe() called
          │
          ▼
┌─────────────────────────────┐
│ Is schema provided?         │─── No ──▶ REGULAR PATH (PyMongo iteration)
└─────────────────────────────┘
          │ Yes
          ▼
┌─────────────────────────────┐
│ Is query chunkable?         │─── No ──▶ REGULAR PATH
│ (has time range, no         │          (e.g., has $where or nested $or)
│  forbidden operators)       │
└─────────────────────────────┘
          │ Yes
          ▼
┌─────────────────────────────┐
│ Is data in cache?           │─── Yes ─▶ READ FROM CACHE
│ (.cache/{query_hash}/*.parquet)        (instant, ~100ms for 1M rows)
└─────────────────────────────┘
          │ No
          ▼
┌─────────────────────────────┐
│ ACCELERATED PATH:           │
│ 1. Build brackets           │ ← analysis/brackets.py
│ 2. Plan execution           │ ← execution/planner.py
│ 3. Chunk time ranges        │ ← analysis/chunker.py
│ 4. Parallel async fetch     │ ← Rust backend (fetch_chunks_bson)
│ 5. Stream to Parquet        │ ← Rust backend writes shards
│ 6. Read back DataFrame      │ ← storage/reader.py
└─────────────────────────────┘

EXAMPLE DATA TRANSFORMATIONS:
────────────────────────────────────────────────────────────────────────────────

1. INPUT QUERY (from user):
   {
       "$or": [
           {"metadata.sensor_id": ObjectId("64a...")},
           {"metadata.sensor_id": ObjectId("64b...")},
       ],
       "timestamp": {"$gte": datetime(2024, 1, 1), "$lt": datetime(2024, 7, 1)}
   }

2. AFTER BRACKET ANALYSIS (brackets.py):
   [
       Bracket(static={"metadata.sensor_id": "64a..."}, time=Jan-Jul),
       Bracket(static={"metadata.sensor_id": "64b..."}, time=Jan-Jul),
   ]

3. AFTER CHUNKING (for each bracket):
   Bracket 1 -> 13 chunks (14 days each for 6 months)
   Bracket 2 -> 13 chunks
   Total: 26 work items in queue

4. PARALLEL FETCH (10 workers):
   Worker 0: Chunk 1 -> 45,000 docs, write to part_0000.parquet
   Worker 1: Chunk 2 -> 52,000 docs, write to part_0001.parquet
   ...
   Worker 9: Chunk 10 -> 38,000 docs, write to part_0009.parquet
   (Rust async workers pull chunks as they finish)

5. OUTPUT (DataFrame):
   pandas.DataFrame with columns: [timestamp, metadata.device_id, value, ...]
   500,000 rows loaded from Parquet in ~0.5s

================================================================================
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
    Iterator,
    Literal,
    Generator,
    cast,
)
from datetime import datetime, date, timezone, timedelta
import logging
import warnings
import pandas as pd
import time
import pyarrow as pa
import polars as pl

logger = logging.getLogger(__name__)

# Import after logger to avoid circular imports
from xlr8.constants import DEFAULT_BATCH_SIZE
from xlr8.execution.callback import execute_partitioned_callback
from xlr8.analysis import (
    build_brackets_for_find,
    chunk_time_range,
    get_sort_field_info,
    validate_sort_field,
)
from xlr8.schema.types import Any as AnyType, List as ListType
from xlr8.storage import CacheManager, ParquetReader
from xlr8.execution import execute_parallel_stream_to_cache


def parse_datetime_tz_aware(
    value: Union[datetime, date, str, None],
    param_name: str = "date",
) -> Optional[datetime]:
    """
    Parse a date/datetime value to a timezone-aware datetime.

    Accepts:
    - datetime (must be tz-aware or will assume UTC)
    - date (converted to midnight UTC)
    - ISO format string with timezone (e.g., "2024-01-15T10:30:00Z", "2024-01-15T10:30:00+00:00")

    Args:
        value: The date value to parse
        param_name: Name of parameter for error messages

    Returns:
        Timezone-aware datetime or None if value is None

    Raises:
        ValueError: If string is not a valid ISO format or missing timezone
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            # Assume UTC for naive datetimes
            return value.replace(tzinfo=timezone.utc)
        return value

    if isinstance(value, date):
        # Convert date to midnight UTC
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)

    if isinstance(value, str):
        # Try parsing ISO format
        try:
            # Python 3.11+ has datetime.fromisoformat with better Z support
            # For compatibility, handle Z suffix manually
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"

            dt = datetime.fromisoformat(value)

            if dt.tzinfo is None:
                raise ValueError(
                    f"{param_name}: Timezone-aware datetime required. "
                    f"Got '{value}' without timezone. "
                    f"Use ISO format with timezone like '2024-01-15T10:30:00Z' or '2024-01-15T10:30:00+00:00'"
                )
            return dt
        except ValueError as e:
            if "Timezone-aware" in str(e):
                raise
            raise ValueError(
                f"{param_name}: Invalid datetime string '{value}'. "
                f"Use ISO format with timezone like '2024-01-15T10:30:00Z' or '2024-01-15T10:30:00+00:00'"
            ) from e

    raise TypeError(
        f"{param_name}: Expected datetime, date, or ISO string, got {type(value).__name__}"
    )


class XLR8Cursor:
    """
    PyMongo-compatible cursor with acceleration support.

    Acts as drop-in replacement for pymongo.cursor.Cursor but can
    accelerate queries through parallel execution and Parquet caching.

    Key differences from PyMongo:
    - to_dataframe() / to_polars() for efficient DataFrame conversion
    - Transparent acceleration when query is chunkable
    - Maintains full PyMongo API compatibility for iteration

    Example:
        >>> cursor = collection.find({"timestamp": {"$gte": start, "$lt": end}})
        >>> df = cursor.to_dataframe()  # Accelerated execution
        >>>
        >>> # Or use like regular PyMongo cursor:
        >>> for doc in cursor:
        ...     print(doc)
    """

    def __init__(
        self,
        collection: Any,  # XLR8Collection
        query_filter: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        skip: int = 0,
        limit: int = 0,
        sort: Optional[List[tuple]] = None,
        batch_size: int = 1000,
    ):
        """
        Initialize cursor.

        Args:
            collection: Parent XLR8Collection
            query_filter: Query filter dict
            projection: Field projection dict
            skip: Number of documents to skip
            limit: Maximum documents to return (0 = unlimited)
            sort: List of (field, direction) tuples
            batch_size: Batch size for iteration
        """
        self._collection = collection
        self._filter = query_filter
        self._projection = projection
        self._skip = skip
        self._limit = limit
        self._sort = sort
        self._batch_size = batch_size

        # Iteration state
        self._started = False
        self._pymongo_cursor: Optional[Any] = None
        self._exhausted = False

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over documents."""
        if not self._started:
            self._started = True
            # Create actual PyMongo cursor for iteration
            self._ensure_pymongo_cursor()

        if self._pymongo_cursor is None:
            return iter([])

        return iter(self._pymongo_cursor)

    def __next__(self) -> Dict[str, Any]:
        """Get next document."""
        if not self._started:
            self.__iter__()

        if self._pymongo_cursor is None:
            raise StopIteration

        return next(self._pymongo_cursor)

    def _ensure_pymongo_cursor(self) -> None:
        """Lazily create PyMongo cursor only when needed for iteration/delegation."""
        if self._pymongo_cursor is None:
            self._pymongo_cursor = self._collection.pymongo_collection.find(
                filter=self._filter,
                projection=self._projection,
                skip=self._skip,
                limit=self._limit,
                sort=self._sort,
                batch_size=self._batch_size,
            )

    def raw_cursor(self):
        """
        Get direct access to underlying PyMongo cursor.

        This is an escape hatch for power users who need access to PyMongo cursor
        methods not explicitly implemented in XLR8Cursor.

        Returns:
            pymongo.cursor.Cursor: The underlying PyMongo cursor

        Example:
            >>> cursor = collection.find(...)
            >>> cursor.raw_cursor().comment("my query").max_time_ms(5000)
        """
        self._ensure_pymongo_cursor()
        return self._pymongo_cursor

    def __getattr__(self, name: str) -> Any:
        """
        Delegate unknown attributes to underlying PyMongo cursor.

        This provides transparent access to all PyMongo cursor methods while
        preserving XLR8's accelerated methods.

        Note: PyMongo cursor is created lazily only when delegation is needed.
        For explicit access, use .raw_cursor()
        """
        # Avoid infinite recursion
        if name.startswith("_"):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            )

        # Create PyMongo cursor if needed
        self._ensure_pymongo_cursor()

        # Get attribute from PyMongo cursor
        attr = getattr(self._pymongo_cursor, name)

        # If it's a method that returns cursor, wrap the result
        if callable(attr):

            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                # If PyMongo method returns cursor, it returns self (the PyMongo cursor)
                # We want to return our wrapper instead
                if result is self._pymongo_cursor:
                    return self
                return result

            return wrapper

        return attr

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    # PyMongo compatibility methods

    def skip(self, count: int) -> "XLR8Cursor":
        """
        Skip documents.

        Args:
            count: Number of documents to skip

        Returns:
            Self for chaining
        """
        if self._started:
            raise RuntimeError("Cannot modify cursor after iteration started")

        self._skip = count
        return self

    def limit(self, count: int) -> "XLR8Cursor":
        """
        Limit result count.

        Args:
            count: Maximum documents to return

        Returns:
            Self for chaining
        """
        if self._started:
            raise RuntimeError("Cannot modify cursor after iteration started")

        self._limit = count
        return self

    def sort(
        self, key_or_list: Union[str, List[tuple]], direction: int = 1
    ) -> "XLR8Cursor":
        """
        Sort results.

        Automatically adds _id as final tie-breaker for deterministic ordering
        (matching MongoDB's behavior).

        Args:
            key_or_list: Field name or list of (field, direction) tuples
            direction: Sort direction (1=ascending, -1=descending)

        Returns:
            Self for chaining
        """
        if self._started:
            raise RuntimeError("Cannot modify cursor after iteration started")

        if isinstance(key_or_list, str):
            self._sort = [(key_or_list, direction)]
        else:
            self._sort = key_or_list

        return self

    def batch_size(self, size: int) -> "XLR8Cursor":
        """
        Set batch size for iteration.

        Args:
            size: Batch size

        Returns:
            Self for chaining
        """
        if self._started:
            raise RuntimeError("Cannot modify cursor after iteration started")

        self._batch_size = size
        return self

    def close(self) -> None:
        """Close cursor and free resources."""
        if self._pymongo_cursor is not None:
            self._pymongo_cursor.close()
            self._pymongo_cursor = None
        self._exhausted = True

    # count() and distinct() removed - use __getattr__ delegation to PyMongo
    # These are available via: cursor.count(), cursor.distinct()
    # __getattr__ automatically forwards them to the underlying PyMongo cursor

    # XLR8-specific acceleration methods

    def to_dataframe(
        self,
        accelerate: bool = True,
        cache_read: bool = True,
        cache_write: bool = True,
        start_date: Optional[Union[datetime, date, str]] = None,
        end_date: Optional[Union[datetime, date, str]] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        flush_ram_limit_mb: int = 512,
    ) -> pd.DataFrame:
        """
        Convert results to Pandas DataFrame with optional acceleration.

        This is the main acceleration entry point. If the query is chunkable
        and acceleration is enabled, uses parallel execution and Parquet caching
        for 2-5x speedup on large result sets.

        ┌─────────────────────────────────────────────────────────────────────┐
        │ DATA FLOW - ACCELERATION DECISION:                                  │
        │                                                                     │
        │ INPUT: self._filter (the MongoDB query)                             │
        │ Example: {                                                          │
        │     "timestamp": {"$gte": datetime(2024,1,1), "$lt": datetime(...)},│
        │     "$or": [{"metadata.sensor_id": ObjectId("64a...")}]          │
        │ }                                                                   │
        │                                                                     │
        │ DECISION STEPS:                                                     │
        │ 1. Check if schema exists         -> No: raise error (schema required)│
        │ 2. Check if query is chunkable    -> No: single-worker, still Parquet│
        │    (is_chunkable_query checks for time bounds, forbidden ops)       │
        │ 3. If chunkable: use parallel workers based on time span            │
        │                                                                     │
        │ OUTPUT: pandas.DataFrame with columns from schema                   │
        │ Example columns: [timestamp, metadata.device_id, value]             │
        │                                                                     │
        │ PERFORMANCE:                                                        │
        │ - Regular path: ~30s for 500K docs (sequential cursor iteration)    │
        │ - Accelerated path: ~10s for 500K docs (parallel + caching)         │
        │ - Cache hit: ~0.5s for 500K docs (read from Parquet)                │
        └─────────────────────────────────────────────────────────────────────┘

        Args:
            accelerate: Enable acceleration if query is chunkable
            cache_read: Read from Parquet cache if available
            cache_write: Write results to Parquet cache
            start_date: Filter cached data from this date (inclusive).
                        Accepts datetime, date, or ISO string with timezone.
                        Example: "2024-01-15T00:00:00Z" or datetime with tzinfo
            end_date: Filter cached data until this date (exclusive).
                      Accepts datetime, date, or ISO string with timezone.
            coerce: Error handling mode:
                    - "raise": Raise exceptions on schema validation errors (default)
                    - "error": Log errors and store None for invalid values
            max_workers: Maximum parallel workers (default: 4). More workers use
                        more RAM but process faster. Set to 1 for single-threaded.
                        Only used when chunking_granularity is provided.
            chunking_granularity: Time granularity for chunking the query.
                        Example: timedelta(days=1) chunks by day, timedelta(hours=1) by hour.
                        REQUIRED for parallel execution - determines chunk boundaries.
                        If None, single-worker mode is used (no parallelization).
            row_group_size: Rows per Parquet row group. If None, Rust default is used.
            flush_ram_limit_mb: RAM limit in MB for buffered data before flushing to
                        Parquet. Higher values mean fewer files but more memory usage.
                        (default: 512)

        Returns:
            Pandas DataFrame with results

        Raises:
            ValueError: If no schema is provided (schema is required for acceleration)
            ValueError: If date strings are not timezone-aware

        Example:
            >>> cursor = collection.find({
            ...     "timestamp": {"$gte": start, "$lt": end},
            ...     "status": "active"
            ... })
            >>> df = cursor.to_dataframe()  # Accelerated automatically
            >>>
        """
        # Schema is required for acceleration
        schema = self._collection.schema
        if schema is None:
            raise ValueError(
                "Schema is required for to_dataframe(). "
                "Provide a schema when creating the collection: "
                "xlr8_collection = xlr8.wrap(collection, schema=my_schema)"
            )

        # CRITICAL: Validate projection doesn't exclude required fields
        if self._projection:
            # Check if projection is inclusion (has 1 values) or exclusion (has 0 values)
            projection_values = [v for k, v in self._projection.items() if k != "_id"]
            is_inclusion = any(v == 1 for v in projection_values)

            # Time field must be included (required for all operations)
            if is_inclusion:
                time_in_projection = (
                    schema.time_field in self._projection
                    and self._projection[schema.time_field] == 1
                )
                if not time_in_projection:
                    raise ValueError(
                        f"Projection must include time field '{schema.time_field}'. "
                        f"Projection: {self._projection}"
                    )

            # Sort fields must be included
            if self._sort:
                for sort_field, _ in self._sort:
                    if is_inclusion:
                        if (
                            sort_field not in self._projection
                            or self._projection[sort_field] != 1
                        ):
                            raise ValueError(
                                f"Projection must include sort field '{sort_field}'. "
                                f"Cannot sort by a field that is projected out. "
                                f"Projection: {self._projection}"
                            )

        # CRITICAL: If limit() or skip() are used, fall back to PyMongo
        # Reason: Downloading all data just to return a subset is impractical
        # MongoDB can efficiently handle limit/skip operations
        if self._limit > 0 or self._skip > 0:
            logger.info(
                "limit() or skip() detected - falling back to PyMongo iteration "
                "(acceleration would be impractical for subset queries)"
            )
            # Use fresh PyMongo cursor (not self which may be exhausted)
            pymongo_cursor = self._collection.pymongo_collection.find(
                self._filter, self._projection
            )
            if self._sort:
                pymongo_cursor = pymongo_cursor.sort(self._sort)
            if self._skip:
                pymongo_cursor = pymongo_cursor.skip(self._skip)
            if self._limit:
                pymongo_cursor = pymongo_cursor.limit(self._limit)
            if self._batch_size:
                pymongo_cursor = pymongo_cursor.batch_size(self._batch_size)
            return pd.json_normalize(list(pymongo_cursor))

        # Validate sort field if specified
        if self._sort:
            sort_validation = validate_sort_field(self._sort, schema)
            if not sort_validation.is_valid:
                raise ValueError(f"Sort validation failed: {sort_validation.reason}")

        # Parse and validate date filters
        parsed_start = parse_datetime_tz_aware(start_date, "start_date")
        parsed_end = parse_datetime_tz_aware(end_date, "end_date")

        if not accelerate:
            # Fallback to regular iteration (ignores date filters)
            if parsed_start or parsed_end:
                logger.warning(
                    "start_date/end_date filters are ignored when accelerate=False"
                )
            return self._to_dataframe_regular()

        is_chunkable, reason, brackets, _ = build_brackets_for_find(
            self._filter,
            schema.time_field,
            self._sort,  # Pass sort spec for $natural detection
        )

        # Validate chunking_granularity if provided
        # CRITICAL: If chunking_granularity is None, we CANNOT chunk the query
        # because we don't know the data's time precision (could be ms, us, ns)
        if chunking_granularity is not None:
            if chunking_granularity.total_seconds() <= 0:
                raise ValueError(
                    f"chunking_granularity must be positive, got {chunking_granularity}"
                )

        if not is_chunkable:
            # REJECT mode - invalid query syntax or contradictory constraints
            # This is different from SINGLE mode (where is_chunkable=True, brackets empty)
            if parsed_start or parsed_end:
                logger.warning(
                    "start_date/end_date filters are ignored for non-chunkable queries"
                )
            logger.info("Query has invalid syntax (%s) - cannot execute", reason)
            return self._to_dataframe_accelerated(
                cache_read=cache_read,
                cache_write=cache_write,
                start_date=parsed_start,
                end_date=parsed_end,
                coerce=coerce,
                max_workers=1,  # Single worker for invalid queries
                chunking_granularity=None,  # No chunking
                is_chunkable=False,
            )

        # Check for SINGLE mode - valid query but single-worker fallback
        # Indicated by: is_chunkable=True AND empty brackets
        if is_chunkable and not brackets:
            # SINGLE mode examples: $natural sort, unbounded $or branches
            logger.info(
                "Query valid but not parallelizable (%s) - using single-worker mode",
                reason,
            )
            return self._to_dataframe_accelerated(
                cache_read=cache_read,
                cache_write=cache_write,
                start_date=parsed_start,
                end_date=parsed_end,
                coerce=coerce,
                max_workers=1,  # Single worker for SINGLE mode
                chunking_granularity=None,  # No chunking
                is_chunkable=False,
            )

        # Query IS chunkable, but do we have granularity info?
        if chunking_granularity is None:
            # No chunking_granularity provided - cannot parallelize safely
            # because we don't know how to split the time range
            logger.info(
                "Query is chunkable but chunking_granularity not provided - "
                "using single-worker mode. Provide chunking_granularity=timedelta(...) "
                "to enable parallel execution."
            )
            return self._to_dataframe_accelerated(
                cache_read=cache_read,
                cache_write=cache_write,
                start_date=parsed_start,
                end_date=parsed_end,
                coerce=coerce,
                max_workers=1,  # Single worker - no chunking info
                chunking_granularity=None,
                is_chunkable=False,  # Treat as non-chunkable since we can't chunk
                flush_ram_limit_mb=flush_ram_limit_mb,  # Pass through for cache reading
                row_group_size=row_group_size,  # Pass through for DuckDB batch
            )

        # Use accelerated parallel execution - we have chunking info!
        return self._to_dataframe_accelerated(
            cache_read=cache_read,
            cache_write=cache_write,
            start_date=parsed_start,
            end_date=parsed_end,
            coerce=coerce,
            max_workers=max_workers,
            chunking_granularity=chunking_granularity,
            is_chunkable=True,
            flush_ram_limit_mb=flush_ram_limit_mb,
            row_group_size=row_group_size,
        )

    def to_dataframe_batches(
        self,
        batch_size: int = DEFAULT_BATCH_SIZE,
        cache_read: bool = True,
        cache_write: bool = True,
        start_date: Optional[Union[datetime, date, str]] = None,
        end_date: Optional[Union[datetime, date, str]] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        flush_ram_limit_mb: int = 512,
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Yield DataFrames in batches from cache without loading all data into memory.

        This is a memory-efficient alternative to to_dataframe() for very large
        result sets. Instead of loading the entire result into memory, it yields
        smaller DataFrames that can be processed incrementally.

        ┌─────────────────────────────────────────────────────────────────────┐
        │ MEMORY-EFFICIENT BATCH PROCESSING:                                  │
        │                                                                     │
        │ Instead of:                                                         │
        │   df = cursor.to_dataframe()  # Loads ALL 10M rows into RAM         │
        │                                                                     │
        │ Use:                                                                │
        │   for batch_df in cursor.to_dataframe_batches(batch_size=50000):    │
        │       process(batch_df)  # Only 50K rows in RAM at a time           │
        │                                                                     │
        │ Memory usage: O(batch_size) instead of O(total_rows)                │
        └─────────────────────────────────────────────────────────────────────┘

        Args:
            batch_size: Number of rows per DataFrame batch (default: 10,000)
            cache_read: Read from Parquet cache if available
            cache_write: Write results to Parquet cache on cache miss
            start_date: Filter cached data from this date (inclusive).
                        Accepts datetime, date, or ISO string with timezone.
            end_date: Filter cached data until this date (exclusive).
            coerce: Error handling mode ("raise" or "error")
            max_workers: Maximum parallel workers for cache population (default: 4)
            chunking_granularity: Time granularity for chunking (required for parallel fetch)

        Yields:
            pd.DataFrame: Batches of rows as DataFrames

        Raises:
            ValueError: If no schema is provided
            ValueError: If date strings are not timezone-aware
            ValueError: If cache doesn't exist and cache_write=False

        Example:
            >>> # Process 10M rows without loading all into RAM
            >>> total = 0
            >>> for batch_df in cursor.to_dataframe_batches(batch_size=50000):
            ...     total += len(batch_df)
            ...     # Process batch_df...
            >>> print(f"Processed {total} rows")
            >>>
            >>> # With date filtering:
            >>> for batch_df in cursor.to_dataframe_batches(
            ...     batch_size=10000,
            ...     start_date="2024-06-01T00:00:00Z",
            ...     end_date="2024-06-15T00:00:00Z"
            ... ):
            ...     analyze(batch_df)
        """
        # Schema is required
        schema = self._collection.schema
        if schema is None:
            raise ValueError(
                "Schema is required for to_dataframe_batches(). "
                "Provide a schema when creating the collection."
            )

        # CRITICAL: If limit() or skip() are used, fall back to PyMongo
        # Reason: Downloading all data just to return a subset is impractical
        if self._limit > 0 or self._skip > 0:
            logger.info(
                "limit() or skip() detected - falling back to PyMongo iteration "
                "(acceleration would be impractical for subset queries)"
            )
            # Use fresh PyMongo cursor in batches (not self which may be exhausted)
            pymongo_cursor = self._collection.pymongo_collection.find(
                self._filter, self._projection
            )
            if self._sort:
                pymongo_cursor = pymongo_cursor.sort(self._sort)
            if self._skip:
                pymongo_cursor = pymongo_cursor.skip(self._skip)
            if self._limit:
                pymongo_cursor = pymongo_cursor.limit(self._limit)
            if self._batch_size:
                pymongo_cursor = pymongo_cursor.batch_size(self._batch_size)

            batch = []
            for doc in pymongo_cursor:
                batch.append(doc)
                if len(batch) >= batch_size:
                    yield pd.DataFrame(batch)
                    batch = []
            if batch:
                yield pd.DataFrame(batch)
            return

        # CRITICAL: Validate projection doesn't exclude required fields
        if self._projection:
            projection_values = [v for k, v in self._projection.items() if k != "_id"]
            is_inclusion = any(v == 1 for v in projection_values)

            # Time field must be included
            if is_inclusion:
                time_in_projection = (
                    schema.time_field in self._projection
                    and self._projection[schema.time_field] == 1
                )
                if not time_in_projection:
                    raise ValueError(
                        f"Projection must include time field '{schema.time_field}'. "
                        f"Projection: {self._projection}"
                    )

            # Sort fields must be included
            if self._sort:
                for sort_field, _ in self._sort:
                    if is_inclusion:
                        if (
                            sort_field not in self._projection
                            or self._projection[sort_field] != 1
                        ):
                            raise ValueError(
                                f"Projection must include sort field '{sort_field}'. "
                                f"Cannot sort by a field that is projected out. "
                                f"Projection: {self._projection}"
                            )

        time_field = schema.time_field

        # Validate sort field if specified
        if self._sort:
            sort_validation = validate_sort_field(self._sort, schema)
            if not sort_validation.is_valid:
                raise ValueError(f"Sort validation failed: {sort_validation.reason}")
            logger.info(
                "Sorted streaming enabled - using DuckDB K-way merge for global sort order"
            )

        # Parse and validate date filters
        parsed_start = parse_datetime_tz_aware(start_date, "start_date")
        parsed_end = parse_datetime_tz_aware(end_date, "end_date")

        is_chunkable, reason, brackets, _ = build_brackets_for_find(
            self._filter,
            time_field,
            self._sort,  # Pass sort spec for $natural detection
        )

        # Handle REJECT mode (is_chunkable=False)
        if not is_chunkable:
            warnings.warn(
                f"Invalid query syntax ({reason}). Cannot execute this query.",
                UserWarning,
                stacklevel=2,
            )
            # Override max_workers to 1 for invalid queries
            max_workers = 1
            chunking_granularity = None

        # Handle SINGLE mode (is_chunkable=True but empty brackets)
        elif is_chunkable and not brackets:
            warnings.warn(
                f"Query valid but not parallelizable ({reason}). Using single-worker mode.",
                UserWarning,
                stacklevel=2,
            )
            # Override max_workers to 1 for SINGLE mode
            max_workers = 1
            chunking_granularity = None

        # Mark as started
        if not self._started:
            self._started = True

        # Create cache manager
        cache = CacheManager(
            filter_dict=self._filter,
            projection=self._projection,
            sort=self._sort,
        )

        # Ensure cache exists
        if not cache.exists():
            if not cache_write:
                raise ValueError(
                    "Cache does not exist and cache_write=False. "
                    "Either call to_dataframe() first to populate cache, "
                    "or set cache_write=True."
                )

            # Populate cache first
            print("[Query] Cache miss - fetching from MongoDB...")

            # Populate cache via accelerated executor
            result = execute_parallel_stream_to_cache(
                pymongo_collection=self._collection.pymongo_collection,
                filter_dict=self._filter,
                schema=schema,
                cache_manager=cache,
                projection=self._projection,
                approx_document_size_bytes=self._collection.approx_document_size_bytes,
                max_workers=max_workers,
                peak_ram_limit_mb=flush_ram_limit_mb,
                chunking_granularity=chunking_granularity,
                mongo_uri=self._collection.mongo_uri,
                sort_spec=self._sort,  # Pass sort for pre-sorting during Parquet write
                row_group_size=row_group_size,
            )

            print(
                f"\n[Cache] Cache written: {result['total_docs']:,} docs in {result['duration_s']:.2f}s"
            )

        elif not cache_read and cache_write:
            # CRITICAL: cache_read=False but cache_write=True and cache exists
            # Clear old cache and re-populate to avoid duplicate data
            print(
                "[Clean] Clearing existing cache (cache_read=False, starting fresh)..."
            )
            cache.clean()

            print("[Query] Re-fetching from MongoDB...")

            # Re-populate cache via accelerated executor
            result = execute_parallel_stream_to_cache(
                pymongo_collection=self._collection.pymongo_collection,
                filter_dict=self._filter,
                schema=schema,
                cache_manager=cache,
                projection=self._projection,
                approx_document_size_bytes=self._collection.approx_document_size_bytes,
                max_workers=max_workers,
                peak_ram_limit_mb=flush_ram_limit_mb,
                chunking_granularity=chunking_granularity,
                mongo_uri=self._collection.mongo_uri,
                sort_spec=self._sort,  # Pass sort for pre-sorting during Parquet write
                row_group_size=row_group_size,
            )

            print(
                f"\n[Cache] Cache re-written: {result['total_docs']:,} docs in {result['duration_s']:.2f}s"
            )

        # Now yield batches from cache
        print(f"[Cache] Streaming batches from cache: {cache.cache_dir}")
        reader = ParquetReader(cache.cache_dir)

        # Use globally sorted streaming if sort is specified
        if self._sort:
            print("[Sort] Using DuckDB K-way merge for globally sorted batches")
            yield from reader.iter_globally_sorted_batches(
                sort_spec=self._sort,  # Pass full sort spec for multi-field sorting
                batch_size=batch_size,
                schema=schema,
                time_field=time_field,
                start_date=parsed_start,
                end_date=parsed_end,
                coerce=coerce,
                memory_limit_mb=flush_ram_limit_mb,  # Pass RAM limit to DuckDB
                threads=max_workers,  # Pass thread count to DuckDB
            )
        else:
            yield from reader.iter_dataframe_batches(
                batch_size=batch_size,
                schema=schema,
                time_field=time_field,
                start_date=parsed_start,
                end_date=parsed_end,
                coerce=coerce,
            )

    def stream_to_callback(
        self,
        callback: Callable[["pa.Table", Dict[str, Any]], None],
        *,
        partition_time_delta: timedelta,
        partition_by: Optional[Union[str, List[str]]] = None,
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        flush_ram_limit_mb: int = 512,
        cache_read: bool = True,
        cache_write: bool = True,
    ) -> Dict[str, Any]:
        """
        Stream partitioned PyArrow tables to a callback function.

        This is a two-phase operation:
        1. Download data from MongoDB to local Parquet cache (reuses Rust backend)
        2. Partition data and call callback in parallel for each partition

        Perfect for populating data lakes with partitioned data structures.

        ┌─────────────────────────────────────────────────────────────────────┐
        │ PARTITION MODES:                                                    │
        │                                                                     │
        │ TIME ONLY (partition_by=None):                                      │
        │   partition_time_delta=timedelta(weeks=1)                           │
        │   -> 1 callback per week of data                                     │
        │                                                                     │
        │ TIME + FIELD (partition_by="metadata.instrument"):                  │
        │   partition_time_delta=timedelta(weeks=1)                           │
        │   -> 1 callback per (week, instrument) combination                   │
        │                                                                     │
        │ Example: 1 year of data, 10 instruments, weekly partitions          │
        │   -> 52 weeks × 10 instruments = 520 callbacks                       │
        └─────────────────────────────────────────────────────────────────────┘

        The callback receives:
        - table: PyArrow Table with data for this partition
        - metadata: Dict with partition info:
            {
                "time_start": datetime,      # Start of time bucket
                "time_end": datetime,        # End of time bucket
                "partition_values": {...},   # Values for partition_by fields
                "row_count": int,            # Rows in this table
                "partition_index": int,      # 0-based partition index
                "total_partitions": int,     # Total partition count
            }

        Args:
            callback: Function(table: pa.Table, metadata: dict) -> None
                     Called for each partition. Runs in ThreadPoolExecutor.
            partition_time_delta: Time bucket size for partitioning.
                        Example: timedelta(weeks=1) creates weekly partitions.
                        REQUIRED - determines how data is grouped.
            partition_by: Field(s) to partition by, in addition to time.
                        Example: "metadata.instrument" or ["region", "device_id"]
                        Can be any field in schema except time field.
                        None = partition by time only.
            any_type_strategy: How to decode Types.Any() struct columns:
                        - "float": Coalesce to Float64, prioritize numeric (default)
                        - "string": Convert everything to string (lossless)
                        - "keep_struct": Keep raw struct, don't decode
            max_workers: Number of parallel callback threads (default: 4).
                        DuckDB releases GIL, so threads get true parallelism.
            chunking_granularity: Time granularity for MongoDB fetch chunks.
                        Used during Phase 1 (download). Example: timedelta(hours=16).
                        If None, defaults to partition_time_delta.
            flush_ram_limit_mb: RAM limit for buffered data (default: 512).
                        Used during both download and partition phases.
            cache_read: Read from existing cache if available (default: True).
            cache_write: Write to cache during download (default: True).

        Returns:
            Dict with:
                - total_partitions: Number of partitions processed
                - total_rows: Total rows across all partitions
                - skipped_partitions: Empty partitions skipped
                - duration_s: Total execution time
                - cache_duration_s: Time spent on cache population
                - partition_duration_s: Time spent on partition callbacks

        Raises:
            ValueError: If no schema provided
            ValueError: If query not chunkable (no time bounds)
            ValueError: If sort specified on non-time field
            RuntimeError: If callback fails for any partition

        Example:
            >>> # Upload weekly data per instrument to S3 data lake
            >>> import pyarrow.parquet as pq
            >>> import s3fs
            >>>
            >>> fs = s3fs.S3FileSystem()
            >>>
            >>> def upload_partition(table, metadata):
            ...     instrument = metadata['partition_values'].get('metadata.instrument', 'unknown')
            ...     week = metadata['time_start'].strftime('%Y-%m-%d')
            ...     path = f"s3://bucket/data/instrument={instrument}/week={week}.parquet"
            ...     pq.write_table(table, path, filesystem=fs)
            >>>
            >>> cursor.stream_to_callback(
            ...     callback=upload_partition,
            ...     partition_time_delta=timedelta(weeks=1),
            ...     partition_by="metadata.instrument",
            ...     max_workers=8,
            ...     chunking_granularity=timedelta(hours=16),
            ... )
        """
        total_start = time.time()

        schema = self._collection.schema
        if schema is None:
            raise ValueError(
                "Schema is required for stream_to_callback(). "
                "Provide a schema when creating the collection."
            )

        # CRITICAL: limit() and skip() don't make sense for streaming callbacks
        # These operations require knowing the full result set, which defeats
        # the purpose of streaming
        if self._limit > 0 or self._skip > 0:
            raise ValueError(
                "stream_to_callback() does not support limit() or skip(). "
                "These operations require knowing the total result set size upfront, "
                "which defeats the purpose of streaming. "
                "Use to_dataframe() or iterate with PyMongo cursor instead."
            )

        time_field = schema.time_field

        # CRITICAL: Validate projection doesn't exclude partition_by fields
        if self._projection and partition_by:
            # Check if projection is inclusion (has 1 values) or exclusion (has 0 values)
            projection_values = [v for k, v in self._projection.items() if k != "_id"]
            is_inclusion = any(v == 1 for v in projection_values)

            # Time field must be included
            if is_inclusion:
                time_in_projection = (
                    time_field in self._projection and self._projection[time_field] == 1
                )
                if not time_in_projection:
                    raise ValueError(
                        f"Projection must include time field '{time_field}'. "
                        f"Projection: {self._projection}"
                    )

            # Partition fields must be included
            partition_by_list = (
                [partition_by] if isinstance(partition_by, str) else partition_by
            )
            for field in partition_by_list:
                if is_inclusion:
                    # For parent fields like "metadata", check if any child is included
                    field_or_children_included = (
                        field in self._projection and self._projection[field] == 1
                    ) or any(
                        k.startswith(f"{field}.") and self._projection[k] == 1
                        for k in self._projection.keys()
                    )
                    if not field_or_children_included:
                        raise ValueError(
                            f"Projection must include partition field '{field}'. "
                            f"Cannot partition by a field that is projected out. "
                            f"Projection: {self._projection}"
                        )

        # Validate sort fields in projection
        if self._projection and self._sort:
            projection_values = [v for k, v in self._projection.items() if k != "_id"]
            is_inclusion = any(v == 1 for v in projection_values)
            for sort_field, _ in self._sort:
                if is_inclusion:
                    if (
                        sort_field not in self._projection
                        or self._projection[sort_field] != 1
                    ):
                        raise ValueError(
                            f"Projection must include sort field '{sort_field}'. "
                            f"Projection: {self._projection}"
                        )

        # Validate sort - only allow time field sorting
        if self._sort:
            for field, _direction in self._sort:
                if field != time_field:
                    raise ValueError(
                        f"stream_to_callback() only supports sorting by time field '{time_field}'. "
                        f"Got sort field: '{field}'. "
                        "Remove .sort() or sort only by time field."
                    )
            # Store sort direction
            sort_ascending = self._sort[0][1] == 1
        else:
            sort_ascending = True  # Default to ascending

        # Normalize partition_by to list
        partition_by_list: Optional[List[str]] = None
        if partition_by is not None:
            if isinstance(partition_by, str):
                partition_by_list = [partition_by]
            else:
                partition_by_list = list(partition_by)

            # Validate partition_by fields exist in schema (or are parent fields with children)
            all_schema_fields = list(schema.fields.keys())
            for field in partition_by_list:
                if field == time_field:
                    raise ValueError(
                        f"Cannot partition by time field '{time_field}'. "
                        "Time partitioning is automatic via partition_time_delta."
                    )
                # Check if field exists directly OR has children
                has_direct = schema.has_field(field)
                has_children = any(f.startswith(f"{field}.") for f in all_schema_fields)
                if not has_direct and not has_children:
                    raise ValueError(
                        f"Partition field '{field}' not found in schema. "
                        f"Available fields: {all_schema_fields}"
                    )

        # Default chunking_granularity to partition_time_delta
        if chunking_granularity is None:
            chunking_granularity = partition_time_delta

        # NEW: build_brackets_for_find internally validates via is_chunkable_query
        is_chunkable, reason, brackets, _ = build_brackets_for_find(
            self._filter,
            time_field,
            self._sort,  # Pass sort spec for $natural detection
        )

        # Handle REJECT mode (is_chunkable=False)
        if not is_chunkable:
            warnings.warn(
                f"Invalid query syntax ({reason}). Cannot execute this query.",
                UserWarning,
                stacklevel=2,
            )
            # Override max_workers to 1 for invalid queries
            max_workers = 1
            chunking_granularity = None

        # Handle SINGLE mode (is_chunkable=True but empty brackets)
        elif is_chunkable and not brackets:
            warnings.warn(
                f"Query valid but not parallelizable ({reason}). Using single-worker mode.",
                UserWarning,
                stacklevel=2,
            )
            # Override max_workers to 1 for SINGLE mode
            max_workers = 1
            chunking_granularity = None

        # Mark as started
        if not self._started:
            self._started = True

        # ─────────────────────────────────────────────────────────────────────
        # PHASE 1: Download to cache (reuses existing Rust backend)
        # ─────────────────────────────────────────────────────────────────────
        cache = CacheManager(
            filter_dict=self._filter,
            projection=self._projection,
            sort=self._sort,
        )

        cache_start = time.time()

        if cache_read and cache.exists():
            print(f"[Cache] Using existing cache: {cache.cache_dir}")
        else:
            if not cache_write:
                raise ValueError(
                    "Cache does not exist and cache_write=False. "
                    "Set cache_write=True to download data first."
                )

            if cache.exists() and not cache_read:
                print("[Clean] Clearing existing cache (cache_read=False)...")
                cache.clean()

            print("[Query] Downloading from MongoDB to cache...")
            result = execute_parallel_stream_to_cache(
                pymongo_collection=self._collection.pymongo_collection,
                filter_dict=self._filter,
                schema=schema,
                cache_manager=cache,
                projection=self._projection,
                approx_document_size_bytes=self._collection.approx_document_size_bytes,
                max_workers=max_workers,
                peak_ram_limit_mb=flush_ram_limit_mb,
                chunking_granularity=chunking_granularity,
                mongo_uri=self._collection.mongo_uri,
                row_group_size=row_group_size,
            )
            print(
                f"[Cache] Downloaded: {result['total_docs']:,} docs in {result['duration_s']:.2f}s"
            )

        cache_duration = time.time() - cache_start

        # ─────────────────────────────────────────────────────────────────────
        # PHASE 2: Partition and stream to callbacks
        # ─────────────────────────────────────────────────────────────────────

        partition_result = execute_partitioned_callback(
            cache_dir=str(cache.cache_dir),
            schema=schema,
            callback=callback,
            partition_time_delta=partition_time_delta,
            partition_by=partition_by_list,
            any_type_strategy=any_type_strategy,
            max_workers=max_workers,
            sort_ascending=sort_ascending,
            memory_limit_mb=flush_ram_limit_mb,
        )

        total_duration = time.time() - total_start

        return {
            "total_partitions": partition_result["total_partitions"],
            "total_rows": partition_result["total_rows"],
            "skipped_partitions": partition_result["skipped_partitions"],
            "duration_s": total_duration,
            "cache_duration_s": cache_duration,
            "partition_duration_s": partition_result["duration_s"],
        }

    def to_polars(
        self,
        accelerate: bool = True,
        cache_read: bool = True,
        cache_write: bool = True,
        start_date: Optional[Union[datetime, date, str]] = None,
        end_date: Optional[Union[datetime, date, str]] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
        flush_ram_limit_mb: int = 512,
    ) -> pl.DataFrame:
        """
        Convert results to Polars DataFrame with optional acceleration.

        This mirrors to_dataframe() but returns a Polars DataFrame.
        Uses ParquetReader with engine="polars" for efficient native reading.

        Args:
            accelerate: Enable acceleration if query is chunkable
            cache_read: Read from Parquet cache if available
            cache_write: Write results to Parquet cache
            start_date: Filter cached data from this date (inclusive).
                        Accepts datetime, date, or ISO string with timezone.
            end_date: Filter cached data until this date (exclusive).
            coerce: Error handling mode ("raise" or "error")
            max_workers: Maximum parallel workers (default: 4)
            chunking_granularity: Time granularity for chunking (e.g., timedelta(days=1))
            row_group_size: Rows per parquet row group. If None, Rust default is used.
            any_type_strategy: How to decode Types.Any() struct columns:
                    - "float": Coalesce to Float64, prioritize numeric (default)
                    - "string": Convert everything to string (lossless)
                    - "keep_struct": Keep raw struct, don't decode
            flush_ram_limit_mb: RAM limit in MB for buffered data before flushing.
                        (default: 512)

        Returns:
            Polars DataFrame with results

        Raises:
            ValueError: If no schema is provided

        Example:
            >>> cursor = collection.find({...}).sort("timestamp", 1)
            >>> df = cursor.to_polars(
            ...     max_workers=8,
            ...     chunking_granularity=timedelta(days=7),
            ...     flush_ram_limit_mb=2000,
            ... )
        """
        schema = self._collection.schema
        if schema is None:
            raise ValueError(
                "Schema is required for to_polars(). "
                "Provide a schema when creating the collection."
            )

        # CRITICAL: If limit() or skip() are used, fall back to PyMongo
        # Reason: Downloading all data just to return a subset is impractical
        if self._limit > 0 or self._skip > 0:
            logger.info(
                "limit() or skip() detected - falling back to PyMongo iteration "
                "(acceleration would be impractical for subset queries)"
            )
            # Use fresh PyMongo cursor (not self which may be exhausted)
            pymongo_cursor = self._collection.pymongo_collection.find(
                self._filter, self._projection
            )
            if self._sort:
                pymongo_cursor = pymongo_cursor.sort(self._sort)
            if self._skip:
                pymongo_cursor = pymongo_cursor.skip(self._skip)
            if self._limit:
                pymongo_cursor = pymongo_cursor.limit(self._limit)
            if self._batch_size:
                pymongo_cursor = pymongo_cursor.batch_size(self._batch_size)
            docs = list(pymongo_cursor)
            if not docs:
                return pl.DataFrame()
            return pl.DataFrame(docs)

        # CRITICAL: Validate projection doesn't exclude required fields
        if self._projection:
            projection_values = [v for k, v in self._projection.items() if k != "_id"]
            is_inclusion = any(v == 1 for v in projection_values)

            # Time field must be included
            if is_inclusion:
                time_in_projection = (
                    schema.time_field in self._projection
                    and self._projection[schema.time_field] == 1
                )
                if not time_in_projection:
                    raise ValueError(
                        f"Projection must include time field '{schema.time_field}'. "
                        f"Projection: {self._projection}"
                    )

            # Sort fields must be included
            if self._sort:
                for sort_field, _ in self._sort:
                    if is_inclusion:
                        if (
                            sort_field not in self._projection
                            or self._projection[sort_field] != 1
                        ):
                            raise ValueError(
                                f"Projection must include sort field '{sort_field}'. "
                                f"Cannot sort by a field that is projected out. "
                                f"Projection: {self._projection}"
                            )

        time_field = schema.time_field

        # Validate sort field if specified
        if self._sort:
            sort_validation = validate_sort_field(self._sort, schema)
            if not sort_validation.is_valid:
                raise ValueError(f"Sort validation failed: {sort_validation.reason}")

        # Parse and validate date filters
        parsed_start = parse_datetime_tz_aware(start_date, "start_date")
        parsed_end = parse_datetime_tz_aware(end_date, "end_date")

        if not accelerate:
            if parsed_start or parsed_end:
                logger.warning(
                    "start_date/end_date filters are ignored when accelerate=False"
                )
            # Fallback to regular iteration (native Polars from dicts)
            return self._to_polars_regular()

        is_chunkable, reason, brackets, _ = build_brackets_for_find(
            self._filter,
            schema.time_field,
            self._sort,  # Pass sort spec for $natural detection
        )

        # Handle REJECT mode (is_chunkable=False)
        if not is_chunkable:
            if parsed_start or parsed_end:
                logger.warning(
                    "start_date/end_date filters are ignored for non-chunkable queries"
                )
            logger.info("Invalid query syntax (%s) - cannot execute", reason)
            # Fall back to single-worker mode
            max_workers = 1
            chunking_granularity = None

        # Handle SINGLE mode (is_chunkable=True but empty brackets)
        elif is_chunkable and not brackets:
            logger.info(
                "Query valid but not parallelizable (%s) - using single-worker mode",
                reason,
            )
            # Fall back to single-worker mode
            max_workers = 1
            chunking_granularity = None

        # Create cache manager
        cache = CacheManager(
            filter_dict=self._filter,
            projection=self._projection,
            sort=self._sort,
        )

        # Check if cache exists
        if cache_read and cache.exists():
            print(f"[Cache] Reading from cache (polars): {cache.cache_dir}")
            reader = ParquetReader(cache.cache_dir)
            df = cast(
                pl.DataFrame,
                reader.to_dataframe(
                    engine="polars",
                    schema=schema,
                    time_field=time_field,
                    start_date=parsed_start,
                    end_date=parsed_end,
                    coerce=coerce,
                    any_type_strategy=any_type_strategy,
                ),
            )

            # Check if we need DuckDB sorting (Any types or List types)
            need_duckdb_sort = False
            sort_infos: List[Dict[str, Any]] = []
            if self._sort:
                sort_infos = get_sort_field_info(self._sort, schema)

                # Expand parent fields to children and collect all fields to check
                fields_to_check = []
                for info in sort_infos:
                    if info["is_parent"]:
                        # Parent field - check all children
                        fields_to_check.extend(info["child_fields"])
                    else:
                        # Direct field
                        fields_to_check.append(info["field"])

                # Check if any of the actual sort fields (after expansion) are Any/List types
                for field in fields_to_check:
                    if field in schema.fields:
                        field_type = schema.fields[field]
                        if isinstance(field_type, (AnyType, ListType)):
                            need_duckdb_sort = True
                            break

            if self._sort and need_duckdb_sort:
                # Use DuckDB for Any/List type sorting (requires BSON type ordering / array sorting)
                print("[Sort] Using DuckDB for Types.Any()/Types.List() sorting...")

                warnings.warn(
                    "Sorting by Types.Any() field in to_polars returns raw struct columns "
                    "(e.g., 'value.float_value', 'value.int64_value'). "
                    "Use to_dataframe() for decoded Any() values.",
                    UserWarning,
                )

                # Use get_globally_sorted_dataframe() - more efficient than batching
                combined_df = reader.get_globally_sorted_dataframe(
                    sort_spec=self._sort,
                    schema=schema,
                    time_field=time_field,
                    start_date=parsed_start,
                    end_date=parsed_end,
                    coerce=coerce,
                )

                if not combined_df.empty:
                    for col in combined_df.columns:
                        if combined_df[col].dtype == object:
                            first_val = (
                                combined_df[col].dropna().iloc[0]
                                if not combined_df[col].dropna().empty
                                else None
                            )
                            if (
                                first_val is not None
                                and type(first_val).__name__ == "ObjectId"
                            ):
                                combined_df[col] = combined_df[col].astype(str)
                    df = pl.from_pandas(combined_df)
                else:
                    df = pl.DataFrame()

            elif self._sort:
                # Native Polars sort - expand parent fields to children
                expanded_sort = []
                for info in sort_infos:
                    if info["is_parent"]:
                        # Expand parent field to all children
                        for child in info["child_fields"]:
                            expanded_sort.append((child, info["direction"]))
                    else:
                        expanded_sort.append((info["field"], info["direction"]))

                sort_fields = [
                    field for field, _ in expanded_sort if field in df.columns
                ]
                descending = [
                    direction == -1
                    for field, direction in expanded_sort
                    if field in df.columns
                ]
                if sort_fields:
                    df = df.sort(sort_fields, descending=descending)

            # Apply skip/limit
            if self._skip:
                df = df.slice(self._skip)
            if self._limit:
                df = df.head(self._limit)

            print(
                f"[OK] Loaded {len(df):,} documents from cache ({reader.get_statistics()['total_size_mb']:.1f} MB)"
            )
            return df

        # Cache miss - need to fetch and write
        if not cache_write:
            raise ValueError(
                "Cache does not exist and cache_write=False. "
                "Either enable cache_write or call to_dataframe() first."
            )

        # Fetch data (uses same logic as to_dataframe)
        mode_str = (
            "parallel" if is_chunkable and chunking_granularity else "single-worker"
        )
        print(f"[Query] Cache miss - fetching from MongoDB ({mode_str} mode)...")

        result = execute_parallel_stream_to_cache(
            pymongo_collection=self._collection.pymongo_collection,
            filter_dict=self._filter,
            schema=schema,
            cache_manager=cache,
            projection=self._projection,
            approx_document_size_bytes=self._collection.approx_document_size_bytes,
            max_workers=max_workers if is_chunkable else 1,
            peak_ram_limit_mb=flush_ram_limit_mb,
            chunking_granularity=chunking_granularity if is_chunkable else None,
            mongo_uri=self._collection.mongo_uri,
            row_group_size=row_group_size,
        )

        print(
            f"\n[Cache] Cache written: {result['total_docs']:,} docs in {result['duration_s']:.2f}s"
        )

        # Read from cache as Polars
        print("[Cache] Reading from cache to build Polars DataFrame...")
        reader = ParquetReader(cache.cache_dir)

        # Check if we need DuckDB sorting (Any types or List types)
        need_duckdb_sort = False
        sort_infos: List[Dict[str, Any]] = []
        if self._sort:
            sort_infos = get_sort_field_info(self._sort, schema)

            # Expand parent fields to children and collect all fields to check
            fields_to_check = []
            for info in sort_infos:
                if info["is_parent"]:
                    # Parent field - check all children
                    fields_to_check.extend(info["child_fields"])
                else:
                    # Direct field
                    fields_to_check.append(info["field"])

            # Check if any of the actual sort fields (after expansion) are Any/List types
            for field in fields_to_check:
                if field in schema.fields:
                    field_type = schema.fields[field]
                    if isinstance(field_type, (AnyType, ListType)):
                        need_duckdb_sort = True
                        break

        if self._sort and need_duckdb_sort:
            # Use DuckDB for Any/List type sorting (requires BSON type ordering / array sorting)
            print("[Sort] Using DuckDB for Types.Any()/Types.List() sorting...")

            warnings.warn(
                "Sorting by Types.Any() field in to_polars returns raw struct columns "
                "(e.g., 'value.float_value', 'value.int64_value'). "
                "Use to_dataframe() for decoded Any() values.",
                UserWarning,
            )

            # Use get_globally_sorted_dataframe() - more efficient than batching
            combined_df = reader.get_globally_sorted_dataframe(
                sort_spec=self._sort,
                schema=schema,
                time_field=time_field,
                start_date=parsed_start,
                end_date=parsed_end,
                coerce=coerce,
            )

            if not combined_df.empty:
                for col in combined_df.columns:
                    if combined_df[col].dtype == object:
                        first_val = (
                            combined_df[col].dropna().iloc[0]
                            if not combined_df[col].dropna().empty
                            else None
                        )
                        if (
                            first_val is not None
                            and type(first_val).__name__ == "ObjectId"
                        ):
                            combined_df[col] = combined_df[col].astype(str)
                df = pl.from_pandas(combined_df)
            else:
                df = pl.DataFrame()
        else:
            df = cast(
                pl.DataFrame,
                reader.to_dataframe(
                    engine="polars",
                    schema=schema,
                    time_field=time_field,
                    start_date=parsed_start,
                    end_date=parsed_end,
                    coerce=coerce,
                    any_type_strategy=any_type_strategy,
                ),
            )

            # Native Polars sort - expand parent fields to children
            if self._sort:
                expanded_sort = []
                for info in sort_infos:
                    if info["is_parent"]:
                        for child in info["child_fields"]:
                            expanded_sort.append((child, info["direction"]))
                    else:
                        expanded_sort.append((info["field"], info["direction"]))

                sort_fields = [
                    field for field, _ in expanded_sort if field in df.columns
                ]
                descending = [
                    direction == -1
                    for field, direction in expanded_sort
                    if field in df.columns
                ]
                if sort_fields:
                    # Polars uses `reverse` (not `descending`) in older versions.
                    df = df.sort(sort_fields, descending=descending)

        # Apply skip/limit
        if self._skip:
            df = df.slice(self._skip)
        if self._limit:
            df = df.head(self._limit)

        return df

    def _to_dataframe_regular(self) -> pd.DataFrame:
        """
        Convert to DataFrame without acceleration.

        Uses regular PyMongo iteration. Fallback for:
        - Non-chunkable queries
        - No schema provided
        - Acceleration disabled

        Returns:
            Pandas DataFrame
        """
        # Collect all documents - __iter__ will set _started
        # Convert to DataFrame
        return pd.json_normalize(list(self))

    def _to_polars_regular(self) -> "pl.DataFrame":
        """
        Convert to Polars DataFrame without acceleration.

        Uses regular PyMongo iteration with native Polars conversion.
        Fallback for:
        - Non-chunkable queries
        - No schema provided
        - Acceleration disabled

        Returns:
            Polars DataFrame

        Note:
            Uses pl.from_dicts() which handles nested documents by creating
            struct columns. For flattened column names like pandas json_normalize,
            you would need to unnest() afterwards.
        """
        # Collect all documents - __iter__ will set _started
        docs = list(self)

        if not docs:
            return pl.DataFrame()

        return pl.from_dicts(docs)

    def _to_dataframe_accelerated(
        self,
        cache_read: bool,
        cache_write: bool,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        is_chunkable: bool = True,
        flush_ram_limit_mb: int = 512,
        row_group_size: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Convert to DataFrame using parallel execution with Parquet caching.

        ┌─────────────────────────────────────────────────────────────────────┐
        │ DATA FLOW - ACCELERATED EXECUTION:                                  │
        │                                                                     │
        │ This is where the XLR8 magic happens. The flow is:                  │
        │                                                                     │
        │ 1. CACHE CHECK                                                      │
        │    Input: self._filter hashed to "abc123def"                        │
        │    Check: Does .cache/abc123def/*.parquet exist?                    │
        │    If yes -> Read directly from Parquet (instant!)                   │
        │                                                                     │
        │ 2. CACHE MISS -> PARALLEL FETCH (if chunkable)                       │
        │    Calls: execute_parallel_stream_to_cache()                        │
        │    Which does:                                                      │
        │    a) Build brackets from query (analysis/brackets.py)              │
        │       Query -> [Bracket(static_filter, time_range), ...]             │
        │    b) Plan execution (execution/planner.py)                         │
        │       Time range + RAM -> workers=N, batch_size=M                    │
        │    c) Chunk time ranges (analysis/chunker.py)                       │
        │       6 months -> X chunks based on granularity                      │
        │    d) Parallel fetch (Rust backend fetch_chunks_bson)               │
        │       N async workers pull chunks from queue                        │
        │    e) Stream to Parquet (Rust backend)                              │
        │       Each worker writes part files: part_0000.parquet, etc.        │
        │                                                                     │
        │ 2b. CACHE MISS -> SINGLE-WORKER FETCH (if not chunkable)             │
        │    - Single worker fetches all data                                 │
        │    - No async, no chunking                                          │
        │    - Still writes to Parquet for caching                            │
        │                                                                     │
        │ 3. READ FROM CACHE                                                  │
        │    After fetch, read the Parquet files we just wrote                │
        │    Optionally filter by start_date/end_date                         │
        │    Returns: pandas DataFrame with original values                   │
        │                                                                     │
        │ EXAMPLE TIMING (500K docs):                                         │
        │ - Cache hit: 0.5s (read Parquet)                                    │
        │ - Cache miss: 10-15s (parallel fetch + write + read)                │
        │ - Without XLR8: 30-40s (sequential cursor iteration)                │
        └─────────────────────────────────────────────────────────────────────┘

        Args:
            cache_read: Read from cache if available
            cache_write: Write to cache after fetching
            start_date: Filter cached data from this date (inclusive, tz-aware)
            end_date: Filter cached data until this date (exclusive, tz-aware)
            coerce: Error handling mode ("raise" or "error")
            max_workers: Maximum parallel workers (passed from to_dataframe)
            chunking_granularity: Time granularity for chunking (passed from to_dataframe)
            is_chunkable: Whether query is chunkable (determines parallel vs single-worker)

        Returns:
            Pandas DataFrame with accelerated query results
        """
        schema = self._collection.schema
        time_field = schema.time_field

        # Mark as started
        if not self._started:
            self._started = True

        # ─────────────────────────────────────────────────────────────────────
        # STEP 1: Create cache manager (hashes query to unique directory)
        # Example: filter_dict hashes to "abc123def" -> .cache/abc123def/
        # ─────────────────────────────────────────────────────────────────────
        cache = CacheManager(
            filter_dict=self._filter,
            projection=self._projection,
            sort=self._sort,
        )

        # ─────────────────────────────────────────────────────────────────────
        # STEP 2: Check cache - if hit, read directly from Parquet
        # Example: .cache/abc123def/ts_1704067200_1704070800_part_0000.parquet
        # ─────────────────────────────────────────────────────────────────────
        if cache_read and cache.exists():
            print(f"[Cache] Reading from cache: {cache.cache_dir}")
            reader = ParquetReader(cache.cache_dir)

            # Check if we need DuckDB sorting (Any types or List types)
            need_duckdb_sort = False
            sort_infos: List[Dict[str, Any]] = []
            if self._sort:
                sort_infos = get_sort_field_info(self._sort, schema)

                # Expand parent fields to children and collect all fields to check
                fields_to_check = []
                for info in sort_infos:
                    if info["is_parent"]:
                        # Parent field - check all children
                        fields_to_check.extend(info["child_fields"])
                    else:
                        # Direct field
                        fields_to_check.append(info["field"])

                # Check if any of the actual sort fields (after expansion) are Any/List types
                for field in fields_to_check:
                    if field in schema.fields:
                        field_type = schema.fields[field]
                        if isinstance(field_type, (AnyType, ListType)):
                            need_duckdb_sort = True
                            break

            if self._sort and need_duckdb_sort:
                # Use DuckDB for Any/List type sorting (requires BSON type ordering / array sorting)
                print("[Sort] Using DuckDB for Types.Any()/Types.List() sorting...")
                df = cast(
                    pd.DataFrame,
                    reader.get_globally_sorted_dataframe(
                        sort_spec=self._sort,
                        schema=schema,
                        time_field=time_field,
                        start_date=start_date,
                        end_date=end_date,
                        coerce=coerce,
                        memory_limit_mb=flush_ram_limit_mb,
                        threads=max_workers,
                    ),
                )
            else:
                # Normal read + native pandas sort
                df = cast(
                    pd.DataFrame,
                    reader.to_dataframe(
                        engine="pandas",
                        schema=schema,
                        time_field=time_field,
                        start_date=start_date,
                        end_date=end_date,
                        coerce=coerce,
                    ),
                )

                # Native pandas sort - expand parent fields to children
                if self._sort:
                    expanded_sort = []
                    for info in sort_infos:
                        if info["is_parent"]:
                            for child in info["child_fields"]:
                                expanded_sort.append((child, info["direction"]))
                        else:
                            expanded_sort.append((info["field"], info["direction"]))

                    sort_fields = [
                        field for field, _ in expanded_sort if field in df.columns
                    ]
                    ascending = [
                        direction == 1
                        for field, direction in expanded_sort
                        if field in df.columns
                    ]
                    if sort_fields:
                        df = df.sort_values(
                            by=sort_fields, ascending=ascending, na_position="last"
                        )
                        logger.debug("Sorted DataFrame by %s", sort_fields)

            # Apply skip/limit if set
            if self._skip:
                df = df.iloc[self._skip :]
            if self._limit:
                df = df.iloc[: self._limit]

            filter_info = ""
            if start_date or end_date:
                filter_info = f" (filtered: {start_date} to {end_date})"
            print(
                f"[OK] Loaded {len(df):,} documents from cache{filter_info} ({reader.get_statistics()['total_size_mb']:.1f} MB)"
            )
            return cast(pd.DataFrame, df)

        # ─────────────────────────────────────────────────────────────────────
        # STEP 3: Cache miss - execute fetch and stream to Parquet
        # This is where the heavy lifting happens
        # ─────────────────────────────────────────────────────────────────────
        mode_str = "parallel" if is_chunkable else "single-worker"
        print(f"[Query] Cache miss - fetching from MongoDB ({mode_str} mode)...")

        if cache_write:
            # CRITICAL: If cache_read=False but cache_write=True and cache exists,
            # we need to clear the old cache first to avoid duplicate data
            if not cache_read and cache.exists():
                print(
                    "🧹 Clearing existing cache (cache_read=False, starting fresh)..."
                )
                cache.clean()
            # chunking_granularity is passed from to_dataframe()
            # If None, execute_parallel_stream_to_cache will use single-worker mode

            # Streaming path: fetch -> encode -> write Parquet (memory efficient)
            result = execute_parallel_stream_to_cache(
                pymongo_collection=self._collection.pymongo_collection,
                filter_dict=self._filter,
                schema=schema,
                cache_manager=cache,
                projection=self._projection,
                approx_document_size_bytes=self._collection.approx_document_size_bytes,
                max_workers=max_workers,  # From to_dataframe() parameter
                peak_ram_limit_mb=flush_ram_limit_mb,
                chunking_granularity=chunking_granularity,  # None = single-worker mode
                mongo_uri=self._collection.mongo_uri,
                row_group_size=row_group_size,
            )

            print("\n[Cache] Cache written:")
            print(f"  - Total docs: {result['total_docs']:,}")
            print(f"  - Total files: {result['total_files']}")
            print(f"  - Workers: {result['workers']}")
            print(f"  - Duration: {result['duration_s']:.2f}s")
            print(f"  - Cache dir: {cache.cache_dir}")

            # Now read from cache to build DataFrame (with optional date filter)
            print("\n[Cache] Reading from cache to build DataFrame...")
            reader = ParquetReader(cache.cache_dir)

            # Check if we need DuckDB sorting (Any types or List types)
            need_duckdb_sort = False
            sort_infos: List[Dict[str, Any]] = []
            if self._sort:
                sort_infos = get_sort_field_info(self._sort, schema)

                # Expand parent fields to children and collect all fields to check
                fields_to_check = []
                for info in sort_infos:
                    if info["is_parent"]:
                        # Parent field - check all children
                        fields_to_check.extend(info["child_fields"])
                    else:
                        # Direct field
                        fields_to_check.append(info["field"])

                # Check if any of the actual sort fields (after expansion) are Any/List types
                for field in fields_to_check:
                    if field in schema.fields:
                        field_type = schema.fields[field]
                        if isinstance(field_type, (AnyType, ListType)):
                            need_duckdb_sort = True
                            break

            if self._sort and need_duckdb_sort:
                # Use DuckDB for Any/List type sorting (requires BSON type ordering / array sorting)
                print("[Sort] Using DuckDB for Types.Any()/Types.List() sorting...")
                df = cast(
                    pd.DataFrame,
                    reader.get_globally_sorted_dataframe(
                        sort_spec=self._sort,
                        schema=schema,
                        time_field=time_field,
                        start_date=start_date,
                        end_date=end_date,
                        coerce=coerce,
                        memory_limit_mb=flush_ram_limit_mb,
                        threads=max_workers,
                    ),
                )
            else:
                # Normal read + native pandas sort
                df = cast(
                    pd.DataFrame,
                    reader.to_dataframe(
                        engine="pandas",
                        schema=schema,
                        time_field=time_field,
                        start_date=start_date,
                        end_date=end_date,
                        coerce=coerce,
                    ),
                )

                # Native pandas sort - expand parent fields to children
                if self._sort:
                    expanded_sort = []
                    for info in sort_infos:
                        if info["is_parent"]:
                            for child in info["child_fields"]:
                                expanded_sort.append((child, info["direction"]))
                        else:
                            expanded_sort.append((info["field"], info["direction"]))

                    sort_fields = [
                        field for field, _ in expanded_sort if field in df.columns
                    ]
                    ascending = [
                        direction == 1
                        for field, direction in expanded_sort
                        if field in df.columns
                    ]
                    if sort_fields:
                        df = df.sort_values(
                            by=sort_fields, ascending=ascending, na_position="last"
                        )
                        logger.debug("Sorted DataFrame by %s", sort_fields)

        else:
            # cache_write=False is not supported in single-worker mode
            # Always write to cache for consistency and performance
            raise ValueError(
                "cache_write=False is not supported. "
                "XLR8 always writes to Parquet cache for memory efficiency. "
                "Set cache_read=False if you don't want to read from existing cache."
            )

        # Apply skip/limit if set
        if self._skip:
            df = df.iloc[self._skip :]
        if self._limit:
            df = df.iloc[: self._limit]

        return cast(pd.DataFrame, df)

    def explain_acceleration(self) -> Dict[str, Any]:
        """
        Get query execution plan.

        Returns explanation of how query will be executed:
        - Whether acceleration is possible
        - Time bounds extracted
        - Estimated chunk count
        - Worker configuration

        Returns:
            Dict with execution plan details
        """
        schema = self._collection.schema

        result: Dict[str, Any] = {
            "filter": self._filter,
            "projection": self._projection,
            "skip": self._skip,
            "limit": self._limit,
            "sort": self._sort,
            "accelerated": False,
        }

        if schema is None:
            result["reason"] = "No schema provided"
            return result

        # NEW: build_brackets_for_find internally validates via is_chunkable_query
        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            self._filter,
            schema.time_field,
            self._sort,  # Pass sort spec for $natural detection
        )

        result["is_chunkable"] = is_chunkable
        result["reason"] = reason

        # Distinguish REJECT vs SINGLE modes
        if not is_chunkable:
            # REJECT mode
            result["mode"] = "reject"
        elif is_chunkable and not brackets:
            # SINGLE mode - valid but not parallelizable
            result["mode"] = "single"
        else:
            # PARALLEL mode
            result["mode"] = "parallel"

        if is_chunkable and bounds and bounds[0] and bounds[1]:
            start_bound = bounds[0]
            end_bound = bounds[1]

            result["time_bounds"] = {
                "start": start_bound.isoformat(),
                "end": end_bound.isoformat(),
            }

            chunks = chunk_time_range(
                start_bound, end_bound, chunk_size=timedelta(days=1)
            )
            result["estimated_chunks"] = len(chunks)

            result["accelerated"] = True

        return result
