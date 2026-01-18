"""
Partition-based callback streaming for data lake population among other use cases.

================================================================================
ARCHITECTURE - STREAM TO CALLBACK WITH PARTITIONING
================================================================================

This module implements a two-phase approach:

PHASE 1: Download to Cache (reuses existing Rust backend)
────────────────────────────────────────────────────────────────────────────────
    MongoDB --> Rust Workers --> Parquet Cache (on disk)

    Uses execute_parallel_stream_to_cache() - battle-tested, memory-safe.

PHASE 2: Partition + Parallel Callbacks
────────────────────────────────────────────────────────────────────────────────
    1. Build partition plan using DuckDB:
       - Discover unique (time_bucket, partition_key) combinations
       - Create work items for each partition

    2. Execute callbacks in parallel (ThreadPoolExecutor):
       - Each worker: DuckDB query -> PyArrow Table -> decode -> callback()
       - DuckDB releases GIL -> true parallelism
       - User callbacks can use non-picklable objects (boto3, etc.)

EDGE CASES HANDLED:
────────────────────────────────────────────────────────────────────────────────
    - NULL values in partition_by fields -> grouped as one partition
    - Empty partitions (no data in time bucket) -> skipped
    - Parent fields (e.g., "metadata") -> expanded to child fields
    - Types.Any() fields -> decoded based on any_type_strategy
    - ObjectIds -> converted to strings (same as to_polars)
    - Large partitions -> DuckDB streams internally, memory-safe
    - Timezone handling -> all datetimes normalized to UTC

================================================================================
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, cast

import polars as pl
import pyarrow as pa

from xlr8.schema.types import Any as AnyType
from xlr8.schema.types import ObjectId as ObjectIdType

logger = logging.getLogger(__name__)


@dataclass
class PartitionWorkItem:
    """A single partition to process."""

    index: int
    total: int
    time_start: datetime
    time_end: datetime
    partition_values: Optional[Dict[str, Any]]  # None if no partition_by
    partition_fields: Optional[List[str]]  # Fields used for partitioning


def _expand_parent_fields(
    fields: List[str],
    schema: Any,
) -> List[str]:
    """
    Expand parent fields to their children in schema definition order.

    When user specifies a parent field like "metadata" but the schema has
    flattened fields like "metadata.device_id", expand to all children.

    Args:
        fields: Original field list
        schema: XLR8 schema with field definitions

    Returns:
        Expanded field list with parent fields replaced by children

    Raises:
        ValueError: If field not found and no children exist
    """
    if schema is None:
        return fields

    all_schema_fields = list(schema.fields.keys())
    expanded = []

    for field_name in fields:
        if schema.has_field(field_name):
            # Field exists directly in schema
            expanded.append(field_name)
        else:
            # Look for child fields with this prefix (in schema order)
            prefix = f"{field_name}."
            children = [f for f in all_schema_fields if f.startswith(prefix)]

            if children:
                logger.info(
                    "Partition field '%s' expanded to children: %s",
                    field_name,
                    children,
                )
                expanded.extend(children)
            else:
                raise ValueError(
                    (
                        f"Partition field '{field_name}' not found in schema. "
                        "No child fields. "
                        f"Available fields: {sorted(all_schema_fields)[:10]}"
                        + ("..." if len(all_schema_fields) > 10 else "")
                    )
                )

    return expanded


def _timedelta_to_duckdb_interval(td: timedelta) -> str:
    """
    Convert Python timedelta to DuckDB interval string.

    Examples:
        timedelta(days=7) -> "7 days"
        timedelta(hours=16) -> "16 hours"
        timedelta(minutes=30) -> "30 minutes"
    """
    total_seconds = int(td.total_seconds())

    if total_seconds >= 86400 and total_seconds % 86400 == 0:
        days = total_seconds // 86400
        return f"{days} day" if days == 1 else f"{days} days"
    elif total_seconds >= 3600 and total_seconds % 3600 == 0:
        hours = total_seconds // 3600
        return f"{hours} hour" if hours == 1 else f"{hours} hours"
    elif total_seconds >= 60 and total_seconds % 60 == 0:
        minutes = total_seconds // 60
        return f"{minutes} minute" if minutes == 1 else f"{minutes} minutes"
    else:
        return f"{total_seconds} seconds"


def _build_partition_plan(
    cache_dir: str,
    time_field: str,
    partition_time_delta: timedelta,
    partition_by: Optional[List[str]],
    memory_limit_mb: int,
    threads: int,
) -> List[PartitionWorkItem]:
    """
    Build partition plan by discovering unique partitions in the cached data.

    Uses DuckDB to efficiently scan all parquet files and find unique
    (time_bucket, partition_key) combinations.

    Natural Time Boundaries:
    - First partition's start time is floored to the start of the day (00:00:00)
    - End time of last partition is kept as actual max + partition_time_delta
    - This creates clean, predictable partition boundaries for data lakes

    Args:
        cache_dir: Path to cache directory with parquet files
        time_field: Name of the timestamp field
        partition_time_delta: Time bucket size (e.g., 7 days)
        partition_by: List of fields to partition by (e.g., ["metadata.instrument"])
        memory_limit_mb: DuckDB memory limit
        threads: DuckDB thread count

    Returns:
        List of PartitionWorkItem to process
    """
    import duckdb

    cache_path = Path(cache_dir)
    parquet_files = list(cache_path.glob("*.parquet"))

    if not parquet_files:
        logger.warning("No parquet files found in %s", cache_dir)
        return []

    file_paths = [str(f) for f in parquet_files]
    files_list = ", ".join([f"'{f}'" for f in file_paths])

    # STEP 1: Get global min timestamp to establish natural day boundary
    global_min_query = f"""
        SELECT MIN("{time_field}") AS global_min_time
        FROM read_parquet([{files_list}])
    """

    try:
        conn = duckdb.connect(":memory:")

        # Configure DuckDB
        if memory_limit_mb:
            conn.execute(f"SET memory_limit = '{memory_limit_mb}MB'")
        if threads:
            conn.execute(f"SET threads = {threads}")

        # Get global min and floor to start of day
        global_result = cast(
            Optional[Tuple[Any, ...]],
            conn.execute(global_min_query).fetchone(),
        )
        if global_result is None or global_result[0] is None:
            logger.warning("No data found in parquet files")
            conn.close()
            return []

        global_min_time = global_result[0]

        # Ensure timezone aware
        if hasattr(global_min_time, "tzinfo") and global_min_time.tzinfo is None:
            global_min_time = global_min_time.replace(tzinfo=timezone.utc)

        # Floor to start of day (zero hours, mins, seconds, microseconds)
        floored_start = global_min_time.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        logger.info(
            "Natural time boundary: floored %s -> %s",
            global_min_time.isoformat(),
            floored_start.isoformat(),
        )

        # Convert timedelta to DuckDB interval
        interval = _timedelta_to_duckdb_interval(partition_time_delta)

        # STEP 2: Build partition query using floored start as origin
        # DuckDB's time_bucket can take an origin parameter
        origin_str = floored_start.strftime("%Y-%m-%d %H:%M:%S")

        # Build SELECT clause for partition keys
        select_parts = [
            f"time_bucket(INTERVAL '{interval}', \"{time_field}\", "
            f"TIMESTAMP '{origin_str}') AS time_bucket"
        ]
        group_parts = ["time_bucket"]

        if partition_by:
            for field in partition_by:
                # Quote field name properly for DuckDB (handles dots in names)
                select_parts.append(f'"{field}" AS "{field}"')
                group_parts.append(f'"{field}"')

        select_clause = ", ".join(select_parts)
        group_clause = ", ".join(group_parts)

        # Build query to discover partitions
        query = f"""
            SELECT
                {select_clause},
                MIN("{time_field}") AS actual_min_time,
                MAX("{time_field}") AS actual_max_time,
                COUNT(*) AS row_count
            FROM read_parquet([{files_list}])
            GROUP BY {group_clause}
            ORDER BY time_bucket
        """

        result = conn.execute(query).fetchall()
        columns = [desc[0] for desc in conn.execute(query).description]  # type: ignore[union-attr]
        conn.close()

        # Build work items from results
        work_items = []
        total = len(result)

        for idx, row in enumerate(result):
            row_dict = dict(zip(columns, row))

            # Extract time bucket
            time_bucket = row_dict["time_bucket"]

            # Calculate time_start and time_end
            # time_bucket is the start of the bucket (aligned to floored origin)
            if isinstance(time_bucket, datetime):
                time_start = time_bucket
                if time_start.tzinfo is None:
                    time_start = time_start.replace(tzinfo=timezone.utc)
                time_end = time_start + partition_time_delta
            else:
                # Fallback: use actual min/max from data
                time_start = row_dict["actual_min_time"]
                time_end = row_dict["actual_max_time"]
                if time_start.tzinfo is None:
                    time_start = time_start.replace(tzinfo=timezone.utc)
                if time_end.tzinfo is None:
                    time_end = time_end.replace(tzinfo=timezone.utc)

            # Extract partition values
            partition_values = None
            if partition_by:
                partition_values = {}
                for field in partition_by:
                    partition_values[field] = row_dict.get(field)

            work_items.append(
                PartitionWorkItem(
                    index=idx,
                    total=total,
                    time_start=time_start,
                    time_end=time_end,
                    partition_values=partition_values,
                    partition_fields=partition_by,
                )
            )

        return work_items

    except (duckdb.Error, KeyError, AttributeError, TypeError, ValueError) as e:
        logger.error("Failed to build partition plan: %s", e)
        raise


def _build_partition_query(
    cache_dir: str,
    time_field: str,
    work_item: PartitionWorkItem,
    sort_ascending: bool = True,
) -> str:
    """
    Build DuckDB query to fetch data for a single partition.

    Args:
        cache_dir: Path to cache directory
        time_field: Timestamp field name
        work_item: Partition work item with time bounds and partition values
        sort_ascending: Sort direction for time field

    Returns:
        DuckDB SQL query string
    """
    cache_path = Path(cache_dir)
    parquet_files = list(cache_path.glob("*.parquet"))
    files_list = ", ".join([f"'{str(f)}'" for f in parquet_files])

    # Build WHERE clauses
    where_clauses = []

    # Time bounds - use proper timestamp formatting
    time_start_iso = work_item.time_start.isoformat()
    time_end_iso = work_item.time_end.isoformat()
    where_clauses.append(f"\"{time_field}\" >= '{time_start_iso}'::TIMESTAMPTZ")
    where_clauses.append(f"\"{time_field}\" < '{time_end_iso}'::TIMESTAMPTZ")

    # Partition values
    if work_item.partition_values:
        for field, value in work_item.partition_values.items():
            if value is None:
                where_clauses.append(f'"{field}" IS NULL')
            elif isinstance(value, str):
                # Escape single quotes in string values
                escaped = value.replace("'", "''")
                where_clauses.append(f"\"{field}\" = '{escaped}'")
            elif isinstance(value, bool):
                where_clauses.append(f'"{field}" = {str(value).upper()}')
            elif isinstance(value, (int, float)):
                where_clauses.append(f'"{field}" = {value}')
            else:
                # Convert to string for complex types
                escaped = str(value).replace("'", "''")
                where_clauses.append(f"\"{field}\" = '{escaped}'")

    where_clause = " AND ".join(where_clauses)
    order_dir = "ASC" if sort_ascending else "DESC"

    return f"""
        SELECT *
        FROM read_parquet([{files_list}])
        WHERE {where_clause}
        ORDER BY "{time_field}" {order_dir}
    """


def _decode_struct_values_polars(
    df: pl.DataFrame,
    schema: Any,
    any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
) -> pl.DataFrame:
    """
    Decode struct-encoded Any-typed columns back to actual values (Polars).

    This is copied from reader.py to avoid circular imports and reuse
    battle-tested Polars decode logic. DuckDB -> Polars -> decode -> Arrow.

    Args:
        df: Polars DataFrame from DuckDB
        schema: Schema with field type info
        any_type_strategy: How to decode:
            - "float": Coalesce to Float64, prioritize numeric (default)
            - "string": Convert everything to string (lossless)
            - "keep_struct": Keep raw struct, don't decode
    """
    if not hasattr(schema, "fields"):
        return df

    # Find Any-typed fields in schema
    for field_name, field_type in schema.fields.items():
        # Check if it's an Any type
        is_any = isinstance(field_type, AnyType) or (
            isinstance(field_type, type) and issubclass(field_type, AnyType)
        )

        if is_any and field_name in df.columns:
            # Check if column is a struct
            col_dtype = df.schema[field_name]
            if str(col_dtype).startswith("Struct"):
                # Strategy: keep_struct - don't decode at all
                if any_type_strategy == "keep_struct":
                    continue

                try:
                    # Get field names from the struct
                    struct_fields = cast(Any, getattr(col_dtype, "fields", []))
                    field_names = (
                        [f.name for f in struct_fields] if struct_fields else []
                    )

                    if any_type_strategy == "string":
                        # Convert ALL value types to string
                        coalesce_exprs = []

                        # String first (already string)
                        if "string_value" in field_names:
                            coalesce_exprs.append(
                                pl.col(field_name).struct.field("string_value")
                            )

                        # Float to string
                        if "float_value" in field_names:
                            coalesce_exprs.append(
                                pl.col(field_name)
                                .struct.field("float_value")
                                .cast(pl.Utf8)
                            )

                        # Int to string
                        for int_name in ["int64_value", "int32_value"]:
                            if int_name in field_names:
                                coalesce_exprs.append(
                                    pl.col(field_name)
                                    .struct.field(int_name)
                                    .cast(pl.Utf8)
                                )

                        # Bool to string
                        if "bool_value" in field_names:
                            coalesce_exprs.append(
                                pl.col(field_name)
                                .struct.field("bool_value")
                                .cast(pl.Utf8)
                            )

                        # ObjectId, decimal, etc. (already strings)
                        for str_field in [
                            "objectid_value",
                            "decimal128_value",
                            "regex_value",
                            "binary_value",
                            "document_value",
                            "array_value",
                        ]:
                            if str_field in field_names:
                                coalesce_exprs.append(
                                    pl.col(field_name).struct.field(str_field)
                                )

                        if coalesce_exprs:
                            df = df.with_columns(
                                pl.coalesce(coalesce_exprs).alias(field_name)
                            )

                    else:  # "float" strategy (default)
                        # Coalesce to Float64, prioritize numeric
                        coalesce_exprs = []

                        # Try float first (highest precision)
                        if "float_value" in field_names:
                            coalesce_exprs.append(
                                pl.col(field_name).struct.field("float_value")
                            )

                        # Try various int types, cast to float
                        for int_name in ["int64_value", "int32_value"]:
                            if int_name in field_names:
                                coalesce_exprs.append(
                                    pl.col(field_name)
                                    .struct.field(int_name)
                                    .cast(pl.Float64)
                                )

                        # Try bool (as 0.0/1.0)
                        if "bool_value" in field_names:
                            coalesce_exprs.append(
                                pl.col(field_name)
                                .struct.field("bool_value")
                                .cast(pl.Float64)
                            )

                        if coalesce_exprs:
                            if len(coalesce_exprs) == 1:
                                df = df.with_columns(
                                    coalesce_exprs[0].alias(field_name)
                                )
                            else:
                                df = df.with_columns(
                                    pl.coalesce(coalesce_exprs).alias(field_name)
                                )
                        else:
                            logger.warning(
                                "Could not decode struct column '%s': "
                                "no numeric fields in %s",
                                field_name,
                                field_names,
                            )
                except (KeyError, AttributeError, TypeError, ValueError) as e:
                    logger.warning("Error decoding struct '%s': %s", field_name, e)

    return df


def _convert_objectids_to_strings_polars(
    df: pl.DataFrame,
    schema: Any,
) -> pl.DataFrame:
    """
    Convert ObjectId columns to strings in Polars (same as reader.py behavior).

    ObjectIds are stored as 24-char hex strings in Parquet. This ensures they
    stay as strings in the final output.
    """
    if not hasattr(schema, "fields"):
        return df

    # Find ObjectId columns
    objectid_columns = []
    for field_name, field_type in schema.fields.items():
        is_oid = isinstance(field_type, ObjectIdType) or (
            isinstance(field_type, type) and issubclass(field_type, ObjectIdType)
        )
        if is_oid and field_name in df.columns:
            objectid_columns.append(field_name)

    if not objectid_columns:
        return df

    # Convert to string in Polars
    for col_name in objectid_columns:
        df = df.with_columns(pl.col(col_name).cast(pl.Utf8))

    return df


def _execute_partition_callback(
    work_item: PartitionWorkItem,
    cache_dir: str,
    callback: Callable[[pa.Table, Dict[str, Any]], None],
    schema: Any,
    time_field: str,
    any_type_strategy: Literal["float", "string", "keep_struct"],
    sort_ascending: bool,
    memory_limit_mb: int,
    threads: int = 1,
) -> Dict[str, Any]:
    """
    Execute callback for a single partition (runs in thread).

    This function:
    1. Builds DuckDB query for the partition
    2. Fetches data as PyArrow Table
    3. Decodes Any() struct columns
    4. Converts ObjectIds to strings
    5. Calls user callback

    Args:
        work_item: Partition to process
        cache_dir: Path to cache directory
        callback: User callback function
        schema: XLR8 schema
        time_field: Timestamp field name
        any_type_strategy: How to decode Any() columns
        sort_ascending: Sort direction
        memory_limit_mb: DuckDB memory limit
        threads: DuckDB thread count (per worker, usually 1)

    Returns:
        Dict with rows processed and partition info
    """
    import duckdb

    try:
        # Build query
        query = _build_partition_query(
            cache_dir=cache_dir,
            time_field=time_field,
            work_item=work_item,
            sort_ascending=sort_ascending,
        )

        # Execute query
        conn = duckdb.connect(":memory:")

        # Configure DuckDB for this worker
        # Use per-worker memory limit (divide total by num threads calling this)
        if memory_limit_mb:
            conn.execute(f"SET memory_limit = '{memory_limit_mb}MB'")

        # ThreadPoolExecutor provides parallelism; set DuckDB threads per worker here.
        conn.execute(f"SET threads = {threads}")

        # Fetch as Arrow Table (DuckDB native support) and convert to Polars
        arrow_tmp = conn.execute(query).fetch_arrow_table()
        polars_df = cast(pl.DataFrame, pl.from_arrow(arrow_tmp))
        conn.close()

        if len(polars_df) == 0:
            # Empty partition - skip callback
            return {
                "rows": 0,
                "partition_index": work_item.index,
                "skipped": True,
            }

        # Decode Any() struct columns using Polars (reuses reader.py logic)
        polars_df = _decode_struct_values_polars(polars_df, schema, any_type_strategy)

        # Convert ObjectIds to strings (Polars)
        polars_df = _convert_objectids_to_strings_polars(polars_df, schema)

        # Convert to Arrow for callback (zero-copy via Arrow C Data Interface)
        arrow_table = polars_df.to_arrow()

        # Build metadata for callback
        metadata = {
            "time_start": work_item.time_start,
            "time_end": work_item.time_end,
            "partition_values": work_item.partition_values or {},
            "row_count": arrow_table.num_rows,
            "partition_index": work_item.index,
            "total_partitions": work_item.total,
        }

        # Call user callback
        callback(arrow_table, metadata)

        return {
            "rows": arrow_table.num_rows,
            "partition_index": work_item.index,
            "skipped": False,
        }

    except Exception as e:  # noqa: BLE001
        logger.error("Partition %d failed: %s", work_item.index, e)
        raise


def execute_partitioned_callback(
    cache_dir: str,
    schema: Any,
    callback: Callable[[pa.Table, Dict[str, Any]], None],
    partition_time_delta: timedelta,
    partition_by: Optional[List[str]],
    any_type_strategy: Literal["float", "string", "keep_struct"],
    max_workers: int,
    sort_ascending: bool,
    memory_limit_mb: int,
) -> Dict[str, Any]:
    """
    Orchestrate parallel callback execution for partitioned data.

    This is the main entry point for Phase 2 (after cache is populated).

    Args:
        cache_dir: Path to cache directory with parquet files
        schema: XLR8 schema
        callback: User callback function(table, metadata)
        partition_time_delta: Time bucket size
        partition_by: Fields to partition by (optional)
        any_type_strategy: How to decode Any() columns
        max_workers: Number of parallel callback threads
        sort_ascending: Sort direction for time field
        memory_limit_mb: Total memory limit for DuckDB operations

    Returns:
        Dict with total_partitions, total_rows, skipped_partitions, duration_s
    """
    import time

    start_time = time.time()

    time_field = schema.time_field

    # Expand parent fields to children
    # (e.g., "metadata" -> ["metadata.device_id", "metadata.sensor_id"])
    if partition_by:
        partition_by = _expand_parent_fields(partition_by, schema)

    # Calculate per-worker memory limit
    # Each worker gets an equal share
    worker_memory_mb = max(64, memory_limit_mb // max_workers)

    print("\n[Partition] Building partition plan...")
    print(f"  - Time bucket: {partition_time_delta}")
    print(f"  - Partition by: {partition_by or 'None (time only)'}")

    # Build partition plan
    work_items = _build_partition_plan(
        cache_dir=cache_dir,
        time_field=time_field,
        partition_time_delta=partition_time_delta,
        partition_by=partition_by,
        memory_limit_mb=worker_memory_mb,
        threads=1,  # Single thread for planning
    )

    if not work_items:
        print("[Partition] No partitions found!")
        return {
            "total_partitions": 0,
            "total_rows": 0,
            "skipped_partitions": 0,
            "duration_s": time.time() - start_time,
        }

    print(f"[Partition] Found {len(work_items)} partitions")
    print(
        f"[Partition] Executing callbacks with {max_workers} workers "
        f"(memory per worker: {worker_memory_mb}MB)"
    )

    # Execute callbacks in parallel
    results = []
    skipped = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _execute_partition_callback,
                work_item=item,
                cache_dir=cache_dir,
                callback=callback,
                schema=schema,
                time_field=time_field,
                any_type_strategy=any_type_strategy,
                sort_ascending=sort_ascending,
                memory_limit_mb=worker_memory_mb,
                threads=1,  # Each worker uses 1 DuckDB thread
            ): item
            for item in work_items
        }

        for future in as_completed(futures):
            work_item = futures[future]
            try:
                result = future.result()
                results.append(result)

                if result.get("skipped"):
                    skipped += 1

            except Exception as e:  # noqa: BLE001
                logger.error("Partition %d failed: %s", work_item.index, e)
                raise RuntimeError(
                    f"Callback failed for partition {work_item.index} "
                    f"(time: {work_item.time_start} to {work_item.time_end}, "
                    f"values: {work_item.partition_values}): {e}"
                ) from e

    duration = time.time() - start_time
    total_rows = sum(r.get("rows", 0) for r in results)

    print("\n[Partition] Complete:")
    print(f"  - Total partitions: {len(work_items)}")
    print(f"  - Skipped (empty): {skipped}")
    print(f"  - Total rows: {total_rows:,}")
    print(f"  - Duration: {duration:.2f}s")

    return {
        "total_partitions": len(work_items),
        "total_rows": total_rows,
        "skipped_partitions": skipped,
        "duration_s": duration,
    }
