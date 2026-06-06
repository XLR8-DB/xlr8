"""
CacheHandler and CacheCursor — query existing Parquet cache with MQL.

CacheHandler is a handle to an existing XLR8 Parquet cache directory.
It provides a find() method that returns a CacheCursor for querying
cached data with MongoDB Query Language (MQL) filters.

CacheCursor supports chaining (sort, limit, skip, projection) and
multiple output formats (pandas, Polars, batches, streaming callbacks).

Example:
    >>> handler = cursor.create_cache()
    >>> df = handler.find({"status": "active"}).sort("ts", -1).limit(100).to_dataframe()
    >>> pl_df = handler.find({"value": {"$gt": 50}}).to_polars()
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId

from xlr8.constants import DEFAULT_BATCH_SIZE
from xlr8.storage.mql_filter import translate_mql_to_sql

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────


def _quote_literal(value: Any) -> str:
    """Format a Python value as a DuckDB SQL literal.

    Handles: None→NULL, bool→TRUE/FALSE, int/float→literal,
    str→quoted string, datetime→TIMESTAMP '...', ObjectId→string.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        # Handle special float values
        import math

        if math.isnan(value):
            return "'NaN'"
        if math.isinf(value):
            return "'Infinity'" if value > 0 else "'-Infinity'"
        return repr(value)
    if isinstance(value, str):
        # Escape single quotes by doubling them (SQL standard)
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, datetime):
        # ISO format with timezone
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        iso = value.isoformat()
        return f"TIMESTAMPTZ '{iso}'"
    if isinstance(value, ObjectId):
        return f"'{str(value)}'"
    if isinstance(value, bytes):
        return f"'{value.decode('utf-8', errors='replace')}'"
    if isinstance(value, (list, dict)):
        # JSON encode lists/dicts
        return _quote_literal(json.dumps(value, default=str))
    # Fallback
    return _quote_literal(str(value))


def _inline_params(sql: str, params: Dict[str, Any]) -> str:
    """Replace $paramName placeholders in SQL with properly quoted literal values.

    Args:
        sql: SQL string with $p0, $p1, ... placeholders.
        params: Dict mapping parameter names to Python values.
                Names may or may not include the '$' prefix — this function
                handles both forms.

    Returns:
        SQL string with all placeholders replaced by literal values.
    """
    result = sql
    # Sort by key length descending to avoid partial replacements
    # (e.g., $p10 should be replaced before $p1)
    for name, value in sorted(params.items(), key=lambda x: len(x[0]), reverse=True):
        # Handle both "$prefix0" (name already includes $) and "prefix0" (bare name)
        if name.startswith("$"):
            placeholder = name
        else:
            placeholder = f"${name}"
        literal = _quote_literal(value)
        result = result.replace(placeholder, literal)
    return result


def _is_any_type(field_type: Any) -> bool:
    """Check if a field type is Types.Any (class or instance)."""
    try:
        from xlr8.schema.types import Any as AnyType
    except ImportError:
        return False
    if isinstance(field_type, AnyType):
        return True
    if isinstance(field_type, type) and issubclass(field_type, AnyType):
        return True
    return False


# ─────────────────────────────────────────────────────────────────────
# CacheHandler
# ─────────────────────────────────────────────────────────────────────


class CacheHandler:
    """Handle to an existing XLR8 Parquet cache directory.

    Allows querying cached data with new MQL filters without
    re-fetching from MongoDB. Created by XLR8Cursor.create_cache().

    Example:
        >>> handler = cursor.create_cache()
        >>> df = handler.find({"status": "active"}).to_dataframe()
        >>> pl_df = handler.find({"value": {"$gt": 100}}).sort("ts", -1).to_polars()
    """

    def __init__(
        self,
        cache_dir: Union[str, Path],
        schema: Any,
        filter_dict: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
    ):
        """Initialize a CacheHandler.

        Args:
            cache_dir: Path to the Parquet cache directory.
            schema: XLR8 Schema object with field type definitions.
            filter_dict: The original MQL query that populated this cache.
            projection: The projection used when populating the cache.
            sort: The sort used when populating the cache.

        Raises:
            FileNotFoundError: If cache_dir does not exist.
            ValueError: If cache_dir contains no Parquet files.
        """
        self.cache_dir = Path(cache_dir)
        self.schema = schema
        self.time_field = schema.time_field
        self.original_filter = filter_dict
        self.original_projection = projection
        self.original_sort = sort

        if not self.cache_dir.exists():
            raise FileNotFoundError(
                f"Cache directory not found: {cache_dir}. "
                f"Create it first with cursor.create_cache() or cursor.to_dataframe()."
            )

        self.parquet_files = sorted(self.cache_dir.glob("*.parquet"))
        if not self.parquet_files:
            raise ValueError(f"No Parquet files found in cache directory: {cache_dir}")

    # ── public API ──────────────────────────────────────────────────

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
    ) -> "CacheCursor":
        """Create a CacheCursor with the given MQL filter.

        Args:
            filter: MongoDB query filter dict. Default {} matches all cached data.
            projection: MongoDB projection dict for field selection.

        Returns:
            CacheCursor for chaining and output.

        Example:
            >>> cursor = handler.find({"status": "active", "value": {"$gt": 100}})
            >>> cursor = cursor.sort("timestamp", -1).limit(50)
            >>> df = cursor.to_dataframe()
        """
        return CacheCursor(
            cache_handler=self,
            filter_dict=filter or {},
            projection=projection,
        )

    # ── metadata ────────────────────────────────────────────────────

    @property
    def file_count(self) -> int:
        """Number of Parquet files in the cache."""
        return len(self.parquet_files)

    @property
    def cache_size_mb(self) -> float:
        """Total size of Parquet files in megabytes."""
        total = sum(f.stat().st_size for f in self.parquet_files)
        return total / (1024 * 1024)

    def get_metadata(self) -> Dict[str, Any]:
        """Get cache metadata.

        Returns:
            Dict with keys: cache_dir, file_count, cache_size_mb,
            original_filter, original_sort, time_field, schema_fields.
        """
        return {
            "cache_dir": str(self.cache_dir),
            "file_count": self.file_count,
            "cache_size_mb": round(self.cache_size_mb, 2),
            "original_filter": self.original_filter,
            "original_sort": self.original_sort,
            "time_field": self.time_field,
            "schema_fields": (
                list(self.schema.fields.keys())
                if hasattr(self.schema, "fields")
                else []
            ),
        }

    def __repr__(self) -> str:
        return (
            f"CacheHandler("
            f"dir={self.cache_dir.name[:16]}..., "
            f"files={self.file_count}, "
            f"size={self.cache_size_mb:.1f}MB)"
        )

    @classmethod
    def from_path(
        cls,
        path: Union[str, Path],
        schema: Any,
    ) -> "CacheHandler":
        """Create a CacheHandler pointing to an existing cache directory.

        Enables cross-container cache sharing via mounted storage.
        Container A creates the cache with create_cache(name="my_dataset"),
        container B mounts the same volume and uses from_path() to access it.

        Args:
            path: Path to the cache directory containing .parquet files.
            schema: XLR8 Schema matching the cached data.

        Returns:
            CacheHandler ready for .find() queries.

        Raises:
            FileNotFoundError: If path does not exist.
            ValueError: If path contains no Parquet files.

        Example:
            >>> # Container A (writes cache)
            >>> handler = cursor.create_cache(name="sensor_data_2024")
            >>> # Container B (reads cache from mounted storage)
            >>> handler = CacheHandler.from_path(
            ...     "/mnt/shared_cache/sensor_data_2024",
            ...     schema=schema,
            ... )
            >>> df = handler.find({"status": "active"}).to_dataframe()
        """
        return cls(cache_dir=Path(path), schema=schema)


# ─────────────────────────────────────────────────────────────────────
# CacheCursor
# ─────────────────────────────────────────────────────────────────────


class CacheCursor:
    """A cursor-like object that queries cached Parquet data via DuckDB.

    Supports chaining: handler.find(query).sort().limit().to_dataframe()

    Key differences from XLR8Cursor:
    - Operates on cached Parquet files only (no MongoDB connection)
    - Filters are applied via DuckDB SQL WHERE clauses
    - All sorting happens via DuckDB (with BSON type ordering for Any fields)
    - Skip/limit applied at SQL level (OFFSET/LIMIT) for efficiency

    Example:
        >>> cursor = handler.find({"status": "active"})
        >>> cursor = cursor.sort("timestamp", -1)
        >>> cursor = cursor.limit(100)
        >>> df = cursor.to_dataframe()
    """

    def __init__(
        self,
        cache_handler: CacheHandler,
        filter_dict: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
    ):
        self._handler = cache_handler
        self._filter = filter_dict
        self._projection = projection
        self._sort: Optional[List[Tuple[str, int]]] = None
        self._skip: int = 0
        self._limit: int = 0
        self._started: bool = False

    # ── chaining methods ────────────────────────────────────────────

    def sort(
        self,
        key_or_list: Union[str, List[Tuple[str, int]]],
        direction: int = 1,
    ) -> "CacheCursor":
        """Set sort order for results.

        Args:
            key_or_list: Sort field name (str) or list of (field, direction) tuples.
            direction: Sort direction: 1 (ascending, default) or -1 (descending).
                       Only used when key_or_list is a string.

        Returns:
            Self for chaining.

        Raises:
            RuntimeError: If cursor has already started iterating.
        """
        if self._started:
            raise RuntimeError("Cannot modify CacheCursor after iteration started")

        if isinstance(key_or_list, str):
            self._sort = [(key_or_list, direction)]
        else:
            self._sort = key_or_list
        return self

    def limit(self, n: int) -> "CacheCursor":
        """Limit the number of results.

        Args:
            n: Maximum number of documents to return.

        Returns:
            Self for chaining.
        """
        if self._started:
            raise RuntimeError("Cannot modify CacheCursor after iteration started")
        self._limit = n
        return self

    def skip(self, n: int) -> "CacheCursor":
        """Skip the first N results.

        Args:
            n: Number of documents to skip.

        Returns:
            Self for chaining.
        """
        if self._started:
            raise RuntimeError("Cannot modify CacheCursor after iteration started")
        self._skip = n
        return self

    def projection(self, proj: Dict[str, Any]) -> "CacheCursor":
        """Set field projection.

        Args:
            proj: MongoDB-style projection dict. Inclusion (1) or exclusion (0).

        Returns:
            Self for chaining.
        """
        if self._started:
            raise RuntimeError("Cannot modify CacheCursor after iteration started")
        self._projection = proj
        return self

    # ── output methods ──────────────────────────────────────────────

    def to_dataframe(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        coerce: Literal["raise", "error"] = "raise",
        flush_ram_limit_mb: int = 512,
        threads: Optional[int] = None,
    ) -> pd.DataFrame:
        """Execute the query and return results as a pandas DataFrame.

        Args:
            start_date: Filter data from this date (inclusive, tz-aware).
            end_date: Filter data until this date (exclusive, tz-aware).
            coerce: Error handling: "raise" (default) or "error" (log, continue).
            flush_ram_limit_mb: DuckDB memory limit in MB (default: 512).
            threads: DuckDB thread count. Defaults to DuckDB's auto-detection.

        Returns:
            pandas DataFrame with query results (structs flattened,
            ObjectIds reconstructed, Any types decoded).

        Example:
            >>> df = handler.find({"value": {"$gt": 50}}).to_dataframe(
            ...     start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            ...     end_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
            ... )
        """
        self._started = True
        schema = self._handler.schema

        # Build and execute DuckDB query
        query_sql = self._build_duckdb_query(start_date, end_date)
        conn = self._create_connection(flush_ram_limit_mb, threads)

        try:
            logger.debug("[CacheCursor] Executing DuckDB query (pandas path)")
            logger.debug("[CacheCursor] SQL: %s", query_sql[:500])

            # Execute and fetch as Arrow table for fast path Any decoding
            arrow_table = conn.execute(query_sql).fetch_arrow_table()
            logger.debug("[CacheCursor] Got %s rows as Arrow table", len(arrow_table))

            # Fast path: decode Any-typed struct columns directly in Arrow
            any_columns_decoded = {}
            columns_to_drop = []
            if schema and hasattr(schema, "fields"):
                from xlr8.rust_backend import decode_any_struct_arrow

                for field_name, field_type in schema.fields.items():
                    if (
                        _is_any_type(field_type)
                        and field_name in arrow_table.column_names
                    ):
                        col = arrow_table.column(field_name)
                        if pa.types.is_struct(col.type):
                            combined = col.combine_chunks()
                            try:
                                decoded_values = decode_any_struct_arrow(combined)
                                any_columns_decoded[field_name] = decoded_values
                                columns_to_drop.append(field_name)
                            except Exception as e:
                                if coerce == "error":
                                    logger.error(
                                        "Error decoding Any struct for '%s': %s",
                                        field_name,
                                        e,
                                    )
                                else:
                                    raise

            # Drop decoded struct columns before pandas conversion
            if columns_to_drop:
                arrow_table = arrow_table.drop(columns_to_drop)

            # Convert to pandas
            df = arrow_table.to_pandas()

            # Add back Any columns with decoded values
            for field_name, decoded_values in any_columns_decoded.items():
                df[field_name] = decoded_values

            # Post-process: flatten structs, reconstruct ObjectIds
            df = self._process_dataframe(df, "pandas", schema, coerce)

            logger.debug("[CacheCursor] Returned %s rows as DataFrame", len(df))
            return df

        finally:
            conn.close()

    def to_polars(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        coerce: Literal["raise", "error"] = "raise",
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
        flush_ram_limit_mb: int = 512,
        threads: Optional[int] = None,
    ) -> pl.DataFrame:
        """Execute the query and return results as a Polars DataFrame.

        Args:
            start_date: Filter data from this date (inclusive, tz-aware).
            end_date: Filter data until this date (exclusive, tz-aware).
            coerce: Error handling mode.
            any_type_strategy: How to decode Types.Any() struct columns:
                - "float": Coalesce to Float64, prioritize numeric (default)
                - "string": Convert everything to string (lossless)
                - "keep_struct": Keep raw struct, don't decode
            flush_ram_limit_mb: DuckDB memory limit in MB.
            threads: DuckDB thread count.

        Returns:
            Polars DataFrame with query results.
        """
        self._started = True
        schema = self._handler.schema

        query_sql = self._build_duckdb_query(start_date, end_date)
        conn = self._create_connection(flush_ram_limit_mb, threads)

        try:
            logger.debug("[CacheCursor] Executing DuckDB query (polars path)")

            # Execute and fetch as Arrow, then convert to Polars
            arrow_table = conn.execute(query_sql).fetch_arrow_table()
            df = pl.from_arrow(arrow_table)

            if df.is_empty():
                return df

            # Post-process
            df = self._process_dataframe_polars(df, schema, coerce, any_type_strategy)

            logger.debug("[CacheCursor] Returned %s rows as Polars DataFrame", len(df))
            return df

        finally:
            conn.close()

    def to_dataframe_batches(
        self,
        batch_size: int = DEFAULT_BATCH_SIZE,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        coerce: Literal["raise", "error"] = "raise",
        flush_ram_limit_mb: int = 512,
        threads: Optional[int] = None,
    ) -> Generator[pd.DataFrame, None, None]:
        """Yield results in batches as pandas DataFrames.

        Memory-efficient: only batch_size rows are in memory at a time.
        Uses DuckDB cursor-based streaming.

        Args:
            batch_size: Number of rows per batch (default: 10,000).
            start_date: Filter data from this date (inclusive).
            end_date: Filter data until this date (exclusive).
            coerce: Error handling mode.
            flush_ram_limit_mb: DuckDB memory limit in MB.
            threads: DuckDB thread count.

        Yields:
            pandas DataFrame batches.
        """
        self._started = True
        schema = self._handler.schema

        query_sql = self._build_duckdb_query(start_date, end_date)
        conn = self._create_connection(flush_ram_limit_mb, threads)

        try:
            logger.debug("[CacheCursor] Streaming batches (size=%s)", batch_size)

            result = conn.execute(query_sql)

            total_batches = 0
            total_rows = 0
            while True:
                batch = result.fetchmany(batch_size)
                if not batch:
                    break

                # Convert list-of-tuples to DataFrame
                columns = [desc[0] for desc in result.description]
                df = pd.DataFrame(batch, columns=columns)

                # Post-process
                df = self._process_dataframe(df, "pandas", schema, coerce)

                if df.empty:
                    continue

                total_batches += 1
                total_rows += len(df)
                yield df

            logger.debug(
                "[CacheCursor] Streamed %s batches, %s total rows",
                total_batches,
                total_rows,
            )

        finally:
            conn.close()

    def stream_to_callback(
        self,
        callback: Callable[["pa.Table", Dict[str, Any]], None],
        partition_time_delta: timedelta,
        partition_by: Optional[Union[str, List[str]]] = None,
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
        max_workers: int = 4,
        flush_ram_limit_mb: int = 512,
    ) -> Dict[str, Any]:
        """Stream partitioned results to a callback function.

        Uses DuckDB to discover partitions and execute callbacks in parallel.
        The additional MQL filter from find() is applied to each partition query.

        Args:
            callback: Function(arrow_table, metadata_dict) called per partition.
            partition_time_delta: Time bucket size (e.g., timedelta(days=7)).
            partition_by: Optional field(s) for additional partitioning.
            any_type_strategy: Any() decode mode ("float"/"string"/"keep_struct").
            max_workers: Number of parallel callback threads.
            flush_ram_limit_mb: DuckDB memory limit in MB.

        Returns:
            Dict with: total_partitions, total_rows, skipped_partitions, duration_s.

        Example:
            >>> def upload(table, meta):
            ...     pq.write_table(table, f"s3://bucket/part_{meta['partition_index']}.parquet")
            >>> handler.find({"status": "active"}).stream_to_callback(
            ...     upload, partition_time_delta=timedelta(days=7), max_workers=4
            ... )
        """
        self._started = True
        import time

        from xlr8.execution.callback import execute_partitioned_callback

        # Build the MQL WHERE clause (without date range — partitions handle that)
        where_sql, where_params = translate_mql_to_sql(
            self._filter,
            self._handler.schema,
            self._handler.time_field,
            start_date=None,
            end_date=None,
            param_prefix="mql",
        )

        # Inline params into the SQL
        if where_params:
            where_sql = _inline_params(where_sql, where_params)

        total_start = time.time()

        result = execute_partitioned_callback(
            cache_dir=str(self._handler.cache_dir),
            schema=self._handler.schema,
            callback=callback,
            partition_time_delta=partition_time_delta,
            partition_by=partition_by
            if isinstance(partition_by, list)
            else ([partition_by] if partition_by else None),
            any_type_strategy=any_type_strategy,
            max_workers=max_workers,
            sort_ascending=True,
            memory_limit_mb=flush_ram_limit_mb,
            extra_where_clause=where_sql if where_sql != "1=1" else None,
        )

        result["duration_s"] = round(time.time() - total_start, 2)
        return result

    def explain(self) -> Dict[str, Any]:
        """Return the generated SQL query and metadata without executing.

        Returns:
            Dict with keys: sql (the DuckDB query), filter, sort, skip, limit,
            projection, cache_files (list of Parquet file paths).
        """
        query_sql = self._build_duckdb_query()
        return {
            "sql": query_sql,
            "filter": self._filter,
            "sort": self._sort,
            "skip": self._skip,
            "limit": self._limit,
            "projection": self._projection,
            "cache_files": [str(f) for f in self._handler.parquet_files],
        }

    # ── internal: query building ────────────────────────────────────

    def _build_duckdb_query(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> str:
        """Build the complete DuckDB SQL query.

        Combines:
        1. SELECT clause (from projection if set)
        2. FROM read_parquet([files])
        3. WHERE clause (MQL filter + optional date range)
        4. ORDER BY (from sort)
        5. OFFSET (from skip)
        6. LIMIT (from limit)
        """
        handler = self._handler
        file_list = ", ".join(f"'{str(f)}'" for f in handler.parquet_files)

        # Build SELECT clause from projection
        select_clause = self._build_select_clause()

        # Build WHERE clause from MQL filter + date range
        where_sql, where_params = translate_mql_to_sql(
            self._filter,
            handler.schema,
            handler.time_field,
            start_date=start_date,
            end_date=end_date,
            param_prefix="mql",
        )

        # Inline params
        if where_params:
            where_sql = _inline_params(where_sql, where_params)

        # Build ORDER BY
        order_clause = self._build_order_clause()

        # Build the query
        parts = [f"SELECT {select_clause} FROM read_parquet([{file_list}])"]
        if where_sql and where_sql != "1=1":
            parts.append(f"WHERE ({where_sql})")
        if order_clause:
            parts.append(f"ORDER BY {order_clause}")
        if self._skip:
            parts.append(f"OFFSET {self._skip}")
        if self._limit:
            parts.append(f"LIMIT {self._limit}")

        return "\n".join(parts)

    def _get_parquet_column_names(self) -> List[str]:
        """Discover the actual column names from the first Parquet file.

        Uses PyArrow to read the schema, bypassing DuckDB's dot-in-column-name
        interpretation issue. Returns the list of column names as they appear
        in the Parquet file (may contain literal dots like 'metadata.instrument').
        """
        if not self._handler.parquet_files:
            return []
        schema = pq.read_schema(self._handler.parquet_files[0])
        return [schema.field(i).name for i in range(len(schema))]

    def _build_select_clause(self) -> str:
        """Build the SELECT clause from the projection.

        CRITICAL: Always produces an explicit column list with double-quoted
        identifiers. Never uses SELECT * because DuckDB interprets dots in
        column names (e.g. 'metadata.instrument') as struct-field access,
        causing NULL values. Quoted identifiers like "metadata.instrument"
        force DuckDB to treat them as literal column names.

        MongoDB projection semantics:
        - Inclusion: {"field": 1, ...} → only those fields (+ _id unless excluded)
        - Exclusion: {"field": 0} → all fields except field
        - Mixed inclusion/exclusion (except _id) is invalid in MongoDB
        """
        # Discover column names from Parquet (avoids DuckDB dot-interpretation bug)
        all_columns = self._get_parquet_column_names()

        if not self._projection:
            # No projection — SELECT all columns with quoted identifiers
            return ", ".join(f'"{c}"' for c in all_columns)

        has_inclusion = any(v == 1 for k, v in self._projection.items() if k != "_id")
        has_exclusion = any(v == 0 for k, v in self._projection.items() if k != "_id")

        if has_inclusion and not has_exclusion:
            # Inclusion: only specified fields
            included = [k for k, v in self._projection.items() if v == 1]
            # _id is included by default unless explicitly excluded
            if "_id" not in self._projection:
                if "_id" in all_columns:
                    included.append("_id")
            elif self._projection.get("_id") == 0:
                if "_id" in included:
                    included.remove("_id")
            # Only include fields that actually exist in the Parquet columns
            return ", ".join(f'"{f}"' for f in included if f in all_columns)

        elif has_exclusion and not has_inclusion:
            # Exclusion: all fields except specified ones
            excluded = {k for k, v in self._projection.items() if v == 0}
            remaining = [c for c in all_columns if c not in excluded]
            return ", ".join(f'"{c}"' for c in remaining)

        else:
            # Mixed or empty — SELECT all columns with quoted identifiers
            return ", ".join(f'"{c}"' for c in all_columns)

    def _build_order_clause(self) -> str:
        """Build the ORDER BY clause from sort spec.

        Uses generate_sort_sql from inspector.py for Any/List type sorting
        with MongoDB BSON type ordering.
        """
        if not self._sort:
            return ""

        # Validate sort fields against schema
        try:
            from xlr8.analysis.inspector import (
                generate_sort_sql,
                has_natural_sort,
                validate_sort_field,
            )
        except ImportError:
            # Fallback: simple sort
            parts = []
            for field, direction in self._sort:
                dir_str = "ASC" if direction == 1 else "DESC"
                parts.append(f'"{field}" {dir_str}')
            return ", ".join(parts)

        # Reject $natural sort
        if has_natural_sort(self._sort):
            raise ValueError(
                "$natural sort is not supported on cached Parquet data. "
                "Cached data has no insertion order. Use time field sorting instead."
            )

        # Validate sort fields
        validation = validate_sort_field(self._sort, self._handler.schema)
        if not validation.is_valid:
            raise ValueError(f"Sort validation failed: {validation.reason}")

        # Generate DuckDB ORDER BY with BSON type ordering
        return generate_sort_sql(self._sort, self._handler.schema)

    # ── internal: DuckDB connection ──────────────────────────────────

    def _create_connection(
        self,
        memory_limit_mb: int = 512,
        threads: Optional[int] = None,
    ) -> "duckdb.DuckDBPyConnection":
        """Create a configured DuckDB in-memory connection."""
        conn = duckdb.connect(":memory:")
        conn.execute(f"SET memory_limit = '{memory_limit_mb}MB'")
        if threads is not None:
            conn.execute(f"SET threads = {threads}")
        return conn

    # ── internal: post-processing ───────────────────────────────────

    def _process_dataframe(
        self,
        df: pd.DataFrame,
        engine: Literal["pandas", "polars"],
        schema: Any,
        coerce: Literal["raise", "error"] = "raise",
    ) -> pd.DataFrame:
        """Post-process a DataFrame: decode structs, flatten, reconstruct ObjectIds.

        Reuses the same logic as ParquetReader._process_dataframe in reader.py.
        """
        if df.empty:
            return df

        # We use ParquetReader's processing for pandas
        # Since the structs come from DuckDB which flattens them differently,
        # we need DuckDB-specific handling

        # DuckDB reads struct columns as dict-like values already.
        # We only need to flatten structs and reconstruct ObjectIds.

        if engine == "pandas":
            # Flatten struct columns
            df = self._flatten_struct_columns(df)
            # Reconstruct ObjectIds
            if schema is not None:
                try:
                    df = self._reconstruct_objectids(df, schema)
                except (AttributeError, KeyError, ValueError, TypeError) as e:
                    if coerce == "error":
                        logger.error("Error reconstructing ObjectIds: %s", e)
                    else:
                        raise
        return df

    def _process_dataframe_polars(
        self,
        df: pl.DataFrame,
        schema: Any,
        coerce: Literal["raise", "error"] = "raise",
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
    ) -> pl.DataFrame:
        """Post-process a Polars DataFrame."""
        if df.is_empty():
            return df

        # Decode Any-typed struct columns
        if schema is not None and any_type_strategy != "keep_struct":
            from xlr8.storage.reader import ParquetReader

            reader = ParquetReader.__new__(ParquetReader)
            try:
                df = reader._decode_struct_values_polars(df, schema, any_type_strategy)
            except (AttributeError, KeyError, ValueError, TypeError) as e:
                if coerce == "error":
                    logger.error("Error decoding struct values (polars): %s", e)
                else:
                    raise

        return df

    def _flatten_struct_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flatten nested struct columns into separate columns.

        DuckDB returns struct columns as dict values in pandas.
        Convert {"metadata": {"device_id": "123"}} to {"metadata.device_id": "123"}.
        """
        if df.empty:
            return df

        struct_cols = []
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].iloc[0], dict):
                struct_cols.append(col)

        for col in struct_cols:
            col_values = df[col].tolist()
            first_val = next((v for v in col_values if isinstance(v, dict)), {})
            subcolumns = list(first_val.keys()) if first_val else []

            new_cols = {}
            for subcol in subcolumns:
                new_col_name = f"{col}.{subcol}"
                new_cols[new_col_name] = [
                    row.get(subcol) if isinstance(row, dict) else None
                    for row in col_values
                ]

            df = df.drop(columns=[col])
            for new_col_name, values in new_cols.items():
                df[new_col_name] = values

        return df

    def _reconstruct_objectids(self, df: pd.DataFrame, schema: Any) -> pd.DataFrame:
        """Reconstruct ObjectId columns from string representation."""
        from xlr8.schema.types import ObjectId as ObjectIdType

        objectid_fields = []
        if hasattr(schema, "fields"):
            for field_name, field_type in schema.fields.items():
                if isinstance(field_type, ObjectIdType):
                    objectid_fields.append(field_name)
                elif hasattr(field_type, "fields"):
                    for nested_name, nested_type in field_type.fields.items():
                        if isinstance(nested_type, ObjectIdType):
                            objectid_fields.append(f"{field_name}.{nested_name}")

        for field in objectid_fields:
            if field in df.columns:
                df[field] = df[field].apply(
                    lambda x: ObjectId(x) if x and pd.notna(x) else x
                )

        return df

    def __repr__(self) -> str:
        return (
            f"CacheCursor(filter={self._filter}, "
            f"sort={self._sort}, limit={self._limit}, skip={self._skip})"
        )
