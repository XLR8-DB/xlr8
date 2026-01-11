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

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

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
    flattened fields like "metadata.vessel_id", expand to all children.

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
                    f"Partition field '{field_name}' expanded to children: {children}"
                )
                expanded.extend(children)
            else:
                raise ValueError(
                    f"Partition field '{field_name}' not found in schema and has "
                    f"no child fields. "
                    f"Available fields: {sorted(all_schema_fields)[:10]}"
                    + ("..." if len(all_schema_fields) > 10 else "")
                )

    return expanded


def _timedelta_to_duckdb_interval(td: timedelta) -> str:
    """
    Convert Python timedelta to DuckDB interval string.

    Examples:
        timedelta(days=7) → "7 days"
        timedelta(hours=16) → "16 hours"
        timedelta(minutes=30) → "30 minutes"
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
