"""
MongoDB Query Validator for XLR8 Parallel Execution.

XLR8 accelerates MongoDB queries by splitting them into smaller time-based
chunks that can be fetched in parallel. This module validates if a query is
safe to split. It does NOT perform the actual splitting—that's handled by
brackets.py and chunker.py.

================================================================================
HOW XLR8 PARALLELIZES QUERIES
================================================================================

Simple example - fetch 1 year of sensor data:

    # Original MongoDB query (fetches 365 days serially)
    db.sensors.find({
        "sensor_id": "temp_001",
        "timestamp": {"$gte": jan_1, "$lt": jan_1_next_year}
    })

    # XLR8 automatically splits this into N parallel chunks
    # fetched simultaneously using Rust workers

The process has two phases:

PHASE 1: Split $or branches into independent brackets (brackets.py)
    Query with $or:
        {"$or": [
            {"region": "US", "timestamp": {"$gte": t1, "$lt": t2}},
            {"region": "EU", "timestamp": {"$gte": t1, "$lt": t2}}
        ]}

    Becomes 2 brackets:
        Bracket 1: {"region": "US", "timestamp": {...}}
        Bracket 2: {"region": "EU", "timestamp": {...}}

PHASE 2: Split each bracket's time range into smaller chunks (chunker.py)
    Each bracket is split into N chunks (user sets chunking granularity
    timedelta(hours=16) etc.)  that are fetched in parallel.
    Results are written to separate Parquet files, then merged.

================================================================================
WHAT MAKES A QUERY SAFE TO PARALLELIZE?
================================================================================

A query is safe for parallel execution if it meets ALL these requirements:

1. TIME BOUNDS - Query must have complete time range
    SAFE:   {"timestamp": {"$gte": t1, "$lt": t2}}
    UNSAFE: {"timestamp": {"$gte": t1}}  (unbounded upper)
    UNSAFE: {}  (no time reference at all)

2. DOCUMENT-LOCAL OPERATORS - Each document evaluated independently
    SAFE FOR PARALLEL:   {"value": {"$gt": 100}}      (compare field to constant)
    SINGLE-WORKER ONLY:  {"$near": {"$geometry": ...}}
    (needs all docs to sort by distance)

    Why not parallel? If we split by time, $near would return "nearest in each chunk"
    not "nearest overall", giving wrong results. But works fine with single-worker.

3. NO TIME FIELD NEGATION - Cannot parallelize $ne/$nin/$not on time field
    SAFE FOR PARALLEL:   {"status": {"$nin": ["deleted", "draft"]}}
    SINGLE-WORKER ONLY:  {"timestamp": {"$nin": [specific_date]}}

    Why not parallel? Negating time creates unbounded ranges. Saying "not this date"
    means you need ALL other dates, which breaks the ability to split by time.
    But works fine with single-worker execution.

4. SIMPLE $or STRUCTURE - Nested $or too complex to parallelize
    SAFE FOR PARALLEL:   {"$or": [{"a": 1}, {"b": 2}]}
    SINGLE-WORKER ONLY:  {"$or": [{"$or": [{...}]}, {...}]}

    Why not parallel? Nested $or creates complex overlaps that cannot be safely
    split into independent brackets. But works fine with single-worker execution.

5. NO $natural SORT - Insertion order incompatible with time chunking
    SAFE FOR PARALLEL:   .sort([("timestamp", 1)])
    SINGLE-WORKER ONLY:  .sort([("$natural", 1)])

    Why not parallel? $natural returns documents in insertion order. When we
    split by time, each chunk is sorted by insertion within that chunk, not globally.
    But works fine with single-worker execution.

================================================================================
THREE EXECUTION MODES
================================================================================

Every query is classified into one of three modes:

┌─────────────────────────────────────────────────────────────────────────┐
│ PARALLEL - Safe for parallel time-chunked execution                     │
│   - Complete time bounds: {"timestamp": {"$gte": t1, "$lt": t2}}        │
│   - Document-local operators only ($gt, $in, $exists, etc.)             │
│   - No time field negation                                              │
│   - Simple $or structure (depth <= 1)                                   │
│   - No $natural sort                                                    │
│   - All operators recognized and safe                                   │
│                                                                         │
│   -> Parallel execution with Rust workers and Parquet caching           │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│ SINGLE - Valid query, cannot parallelize safely                         │
│   - Operators requiring full dataset ($text, $near, $expr, geospatial)  │
│   - Nested $or (depth > 1) - overlap handling too complex               │
│   - Time field negation ($timestamp: {$nin: [...]})                     │
│   - $natural sort (requires insertion order)                            │
│   - Unbounded/partial time ranges                                       │
│   - No time field reference                                             │
│   - Unknown operators (not yet classified)                              │
│                                                                         │
│   -> Single-worker execution                                            │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│ REJECT - Invalid query (MongoDB would also reject or return wrong data) │
│   (X) Empty $or: {"$or": []} (invalid MongoDB syntax)                   │
│   (X) Contradictory bounds: $gte: t2, $lt: t1 where t2 > t1             │
│                                                                         │
│   -> Error raised with clear explanation                                │
│   -> User must fix the query                                            │
└─────────────────────────────────────────────────────────────────────────┘

================================================================================
OPERATOR CLASSIFICATION
================================================================================

ALWAYS_ALLOWED (23 operators) - Document-local evaluation
    These are safe because they evaluate each document independently without
    needing other documents.
    9
    Comparison:  $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin
    Element:     $exists, $type
    Array:       $all, $elemMatch, $size
    Bitwise:     $bitsAllClear, $bitsAllSet, $bitsAnyClear, $bitsAnySet
    Evaluation:  $regex, $mod, $jsonSchema
    Logical:     $and
    Metadata:    $comment, $options

    Edge case: When used in $or branches, brackets.py performs additional overlap
    checks to prevent duplicate results. For example:
        {"$or": [{"x": {"$in": [1,2,3]}}, {"x": {"$in": [3,4,5]}}]}
    The value 3 appears in both branches, so this needs special handling.

CONDITIONAL (3 operators) - Safe under specific conditions
    $or   -> Allowed at depth 1 only (no nested $or)
    $nor  -> Allowed if it does NOT reference the time field
    $not  -> Allowed if NOT applied to the time field

    Examples:
        SAFE:   {"$or": [{"region": "US"}, {"region": "EU"}]}
        UNSAFE: {"$or": [{"$or": [{...}]}, {...}]}

        SAFE:   {"$nor": [{"status": "deleted"}], "timestamp": {...}}
        UNSAFE: {"$nor": [{"timestamp": {"$lt": t1}}]}

NEVER_ALLOWED (17 operators) - Require full dataset (triggers SINGLE mode)
    Geospatial:  $near, $nearSphere, $geoWithin, $geoIntersects, $geometry,
                 $box, $polygon, $center, $centerSphere, $maxDistance, $minDistance
    Text:        $text
    Dynamic:     $expr, $where
    Atlas:       $search, $vectorSearch
    Legacy:      $uniqueDocs

    Why parallelization is disabled:
    - $near/$nearSphere: Sort ALL docs by distance. If we split by time,
      we'd get "nearest in chunk" not "nearest overall"
    - $text: Uses corpus-wide IDF scores. Splitting changes term frequencies
    - $expr/$where: Cannot statically analyze. May have arbitrary logic
    - $search/$vectorSearch: Atlas-specific, require special infrastructure

    These operators work fine with single-worker execution (no splitting).

UNKNOWN operators -> Also triggers SINGLE mode (conservative/experimental)
    If XLR8 encounters an operator not in the above lists, it conservatively
    falls back to single-worker execution rather than risk incorrect results.

================================================================================
API USAGE
================================================================================

    from xlr8.analysis import is_chunkable_query, ChunkabilityMode

    # Basic usage
    query = {
        "sensor_id": "temp_001",
        "timestamp": {"$gte": jan_1, "$lt": feb_1}
    }

    result = is_chunkable_query(query, "timestamp")

    if result.mode == ChunkabilityMode.PARALLEL:
        print(f"Can parallelize from {result.bounds[0]} to {result.bounds[1]}")
        # Proceed with parallel execution
    elif result.mode == ChunkabilityMode.SINGLE:
        print(f"Single-worker mode: {result.reason}")
        # Execute with one worker (still faster than PyMongo)
    else:  # REJECT
        print(f"Cannot execute: {result.reason}")
        # Raise error or fall back to PyMongo

    # Backwards-compatible boolean properties
    result.is_chunkable    # True for PARALLEL only
    result.is_executable   # True for PARALLEL or SINGLE

    # Can also unpack as tuple (backwards compatibility)
    mode, reason, (start, end) = is_chunkable_query(query, "timestamp")

Common reasons by mode:
    REJECT:  "$or with empty array matches no documents"
             "invalid time range: lower bound >= upper bound
             (contradictory constraints)"

    SINGLE:  "operator '$text' requires full dataset (single-worker execution)"
             "operator '$near' requires full dataset (single-worker execution)"
             "nested $or operators (depth > 1) require single-worker execution"
             "query contains negation operators ($ne/$nin) on time field"
             "$natural sort requires insertion order (single-worker execution)"
             "no time bounds found (requires single-worker execution)"
             "unbounded $or branch (requires single-worker execution)"
             "unknown operator '$futureOp' (experimental single-worker execution)"

================================================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

_all__ = [
    # Classification sets
    "ALWAYS_ALLOWED",
    "CONDITIONAL",
    "NEVER_ALLOWED",
    # Validation
    "ValidationResult",
    "ChunkabilityMode",
    "ChunkabilityResult",
    "has_forbidden_ops",
    "has_unknown_operators",
    "check_conditional_operators",
    "validate_query_for_chunking",
    # Query analysis utilities
    "or_depth",
    "split_global_and",
    "extract_time_bounds",
    "normalize_datetime",
    "normalize_query",
    "extract_time_bounds_recursive",
    # Main entry point
    "is_chunkable_query",
    # Internal (exported for testing)
    "_or_depth",
    "_references_field",
]

# =============================================================================
# OPERATOR CLASSIFICATION
# =============================================================================

ALWAYS_ALLOWED: frozenset[str] = frozenset(
    {
        # -- Comparison ---------------------------------------------------------------
        # Compare field value against a constant. Always document-local.
        #
        # Example: Find all sensors with readings above threshold
        #   {"value": {"$gt": 100}, "timestamp": {"$gte": t1, "$lt": t2}}
        #
        "$eq",  # {"status": {"$eq": "active"}}  - equals
        "$ne",  # {"status": {"$ne": "deleted"}} - not equals
        "$gt",  # {"value": {"$gt": 100}}        - greater than
        "$gte",  # {"value": {"$gte": 100}}       - greater or equal
        "$lt",  # {"value": {"$lt": 0}}          - less than
        "$lte",  # {"value": {"$lte": 100}}       - less or equal
        "$in",  # {"type": {"$in": ["A", "B"]}}  - in set
        "$nin",  # {"type": {"$nin": ["X", "Y"]}} - not in set
        # -- Element ------------------------------------------------------------------
        # Check field existence or BSON type. Document-local metadata checks.
        #
        # Example: Only include documents with validated readings
        #   {"validated_at": {"$exists": true}, "value": {"$type": "double"}}
        #
        "$exists",  # {"email": {"$exists": true}}
        "$type",  # {"value": {"$type": "double"}}
        # -- Array --------------------------------------------------------------------
        # Evaluate array fields within a single document.
        #
        # Example: Find sensors with all required tags
        #   {"tags": {"$all": ["calibrated", "production"]}}
        #
        "$all",  # {"tags": {"$all": ["a", "b"]}}
        "$elemMatch",  # {"readings": {"$elemMatch": {"value": {"$gt": 100}}}}
        "$size",  # {"items": {"$size": 3}}
        # -- Bitwise ------------------------------------------------------------------
        # Compare integer bits against a bitmask. Document-local.
        #
        # Example: Find flags with specific bits set
        #   {"flags": {"$bitsAllSet": [0, 2, 4]}}
        #
        "$bitsAllClear",
        "$bitsAllSet",
        "$bitsAnyClear",
        "$bitsAnySet",
        # -- Evaluation (safe) --------------------------------------------------------
        # Pattern matching and validation that is document-local.
        #
        # Example: Match sensor names by pattern
        #   {"sensor_id": {"$regex": "^TEMP_", "$options": "i"}}
        #
        "$regex",  # {"name": {"$regex": "^sensor_"}}
        "$options",  # Modifier for $regex
        "$mod",  # {"value": {"$mod": [10, 0]}}  - divisible by 10
        "$jsonSchema",  # {"$jsonSchema": {"required": ["name"]}}
        "$comment",  # {"$comment": "audit query"}  - annotation only
        # -- Logical (safe) -----------------------------------------------------------
        # $and is always safe: conjunctions preserve correctness.
        #
        # Example: Multiple conditions all must match
        #   {"$and": [{"value": {"$gt": 0}}, {"status": "active"}]}
        #
        "$and",
    }
)


CONDITIONAL: frozenset[str] = frozenset(
    {
        # -- $or ----------------------------------------------------------------------
        # ALLOWED at depth 1 only. Top-level $or is decomposed into "brackets"
        # which are executed and cached independently.
        #
        # [OK] ALLOWED (depth 1):
        #   {"$or": [
        #       {"sensor_id": "A", "timestamp": {"$gte": t1, "$lt": t2}},
        #       {"sensor_id": "B", "timestamp": {"$gte": t1, "$lt": t2}}
        #   ]}
        #
        # [X] Triggers SINGLE mode (depth 2 - nested $or):
        #   {"$or": [{"$or": [{...}, {...}]}, {...}]}
        #
        "$or",
        # -- $nor ---------------------------------------------------------------------
        # ALLOWED if not referencing time field. Negating time bounds creates
        # unpredictable behavior when chunking.
        #
        # [OK] ALLOWED (excludes status values):
        #   {"$nor": [{"status": "deleted"}, {"status": "draft"}],
        #    "timestamp": {"$gte": t1, "$lt": t2}}
        #
        # [X] Triggers SINGLE mode (negates time constraint):
        #   {"$nor": [{"timestamp": {"$lt": "2024-01-01"}}]}
        #
        "$nor",
        # -- $not ---------------------------------------------------------------------
        # ALLOWED if not applied to time field. Same reasoning as $nor.
        #
        # [OK] ALLOWED (negates value constraint):
        #   {"value": {"$not": {"$lt": 0}}}   - equivalent to value >= 0
        #
        # [X] Triggers SINGLE mode (negates time constraint):
        #   {"timestamp": {"$not": {"$lt": "2024-01-15"}}}
        #
        "$not",
    }
)


NEVER_ALLOWED: frozenset[str] = frozenset(
    {
        # -- Evaluation (unsafe) ------------------------------------------------------
        # $expr and $where cannot be statically analyzed for safety.
        #
        # $expr can contain arbitrary aggregation expressions:
        #   {"$expr": {"$gt": ["$endTime", "$startTime"]}}
        #   While this example IS document-local, we cannot prove safety for all cases.
        #
        # $where executes JavaScript on the server:
        #   {"$where": "this.endTime > this.startTime"}
        #   Cannot analyze, may have side effects.
        #
        "$expr",
        "$where",
        # -- Text Search --------------------------------------------------------------
        # $text uses text indexes and corpus-wide IDF scoring.
        # Splitting the corpus changes term frequencies and relevance scores.
        #
        #   {"$text": {"$search": "mongodb performance tuning"}}
        #
        "$text",
        # -- Atlas Search -------------------------------------------------------------
        # Atlas-specific full-text and vector search operators.
        #
        "$search",
        "$vectorSearch",
        # -- Geospatial ---------------------------------------------------------------
        # Geospatial operators require special indexes and often involve
        # cross-document operations (sorting by distance, spatial joins).
        #
        # $near/$nearSphere return documents SORTED BY DISTANCE:
        #   {"location": {"$near": [lng, lat]}}
        #   If we chunk by time, we get "nearest in chunk" not "nearest overall".
        #
        # $geoWithin/$geoIntersects require 2dsphere indexes:
        #   {"location": {"$geoWithin": {"$geometry": {...}}}}
        #
        "$near",
        "$nearSphere",
        "$geoWithin",
        "$geoIntersects",
        "$geometry",
        "$box",
        "$polygon",
        "$center",
        "$centerSphere",
        "$maxDistance",
        "$minDistance",
        "$uniqueDocs",
    }
)

# =============================================================================
# VALIDATION RESULT
# =============================================================================


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Result of query validation for chunking."""

    is_valid: bool
    reason: str = ""
    forbidden_operator: str | None = None

    def __bool__(self) -> bool:
        return self.is_valid


class ChunkabilityMode(Enum):
    """Execution mode for MongoDB queries in XLR8.

    XLR8 classifies queries into three execution modes based on safety
    and parallelizability:

    - PARALLEL: Query can be safely executed with parallel time-chunked workers.
                Example: {"timestamp": {"$gte": t1, "$lt": t2}, "status": "active"}

    - SINGLE: Query is valid but cannot be safely parallelized. Executes with
              single worker but still uses Rust backend and Parquet caching.
              Example: {"timestamp": {"$gte": t1}}, sort=[("$natural", 1)]

    - REJECT: Invalid query syntax or contradictory constraints that MongoDB
              would also reject or return no results for (e.g., empty $or array,
              contradictory lo >= hi bounds). These queries should NOT be executed.
              Example: {"$or": []}
    """

    PARALLEL = "parallel"  # Safe for parallel time-chunked execution
    SINGLE = "single"  # Valid query, single-worker fallback
    REJECT = "reject"  # Invalid syntax or contradictory constraints


class ChunkabilityResult(NamedTuple):
    """Result of query chunkability analysis.

    Provides structured result with execution mode, reason, and time bounds.

    Attributes:
        mode: Execution mode (PARALLEL/SINGLE/REJECT)
        reason: Empty string if PARALLEL, explanation otherwise
        bounds: Time bounds tuple (lo, hi) or (None, None)

    Examples:
        >>> result = ChunkabilityResult(
        ...     mode=ChunkabilityMode.PARALLEL,
        ...     reason="",
        ...     bounds=(datetime(2024,1,1), datetime(2024,7,1))
        ... )
        >>> result.mode == ChunkabilityMode.PARALLEL
        True

        >>> result = ChunkabilityResult(
        ...     mode=ChunkabilityMode.SINGLE,
        ...     reason="$natural sort requires insertion order",
        ...     bounds=(datetime(2024,1,1), datetime(2024,7,1))
        ... )
        >>> result.mode == ChunkabilityMode.SINGLE
        True

        >>> result = ChunkabilityResult(
        ...     mode=ChunkabilityMode.REJECT,
        ...     reason="empty $or array (invalid MongoDB syntax)",
        ...     bounds=(None, None)
        ... )
        >>> result.mode == ChunkabilityMode.REJECT
        True
    """

    mode: ChunkabilityMode
    reason: str
    bounds: Tuple[Optional[datetime], Optional[datetime]]


# =============================================================================
# CORE VALIDATION FUNCTIONS
# =============================================================================


def has_forbidden_ops(query: Any) -> Tuple[bool, Optional[str]]:
    """
    Check if query contains any NEVER_ALLOWED operator.

    These operators require the full dataset and cannot be parallelized.
    Triggers SINGLE mode (single-worker execution).

    Recursively walks the query tree looking for forbidden operator keys.
    Returns on first forbidden operator found (fail-fast).

    Args:
        query: MongoDB query (dict, list, or primitive)

    Returns:
        Tuple of (has_forbidden, operator_name)

    Examples:
        >>> has_forbidden_ops({"status": "active"})
        (False, None)

        >>> has_forbidden_ops({"location": {"$near": [0, 0]}})
        (True, '$near')

        >>> has_forbidden_ops({"$and": [{"$text": {"$search": "test"}}]})
        (True, '$text')
    """
    if isinstance(query, dict):
        for key, value in query.items():
            if key in NEVER_ALLOWED:
                return True, key
            found, op = has_forbidden_ops(value)
            if found:
                return True, op
    elif isinstance(query, list):
        for item in query:
            found, op = has_forbidden_ops(item)
            if found:
                return True, op
    return False, None


def has_unknown_operators(query: Any) -> Tuple[bool, Optional[str]]:
    """
    Check if query contains operators not in our classification lists.

    This provides a conservative "fail-closed" approach for MongoDB operators
    that are not yet classified. Unknown operators trigger SINGLE mode execution
    (experimental/cautious path) rather than being silently allowed.

    Args:
        query: MongoDB query (dict, list, or primitive)

    Returns:
        Tuple of (has_unknown, operator_name)

    Examples:
        >>> has_unknown_operators({"status": "active"})
        (False, None)

        >>> has_unknown_operators({"$futureOp": {"$someLogic": "..."}})
        (True, '$futureOp')

        >>> has_unknown_operators({"value": {"$gt": 100}})
        (False, None)
    """
    KNOWN_OPS = ALWAYS_ALLOWED | CONDITIONAL | NEVER_ALLOWED

    if isinstance(query, dict):
        for key, value in query.items():
            # Check if key is an operator (starts with $) and not in known lists
            if key.startswith("$") and key not in KNOWN_OPS:
                return True, key
            # Recurse into value
            found, op = has_unknown_operators(value)
            if found:
                return True, op
    elif isinstance(query, list):
        for item in query:
            found, op = has_unknown_operators(item)
            if found:
                return True, op
    return False, None


def _references_field(obj: Any, field_name: str) -> bool:
    """Check if query fragment references a specific field name."""
    if isinstance(obj, dict):
        if field_name in obj:
            return True
        return any(_references_field(v, field_name) for v in obj.values())
    elif isinstance(obj, list):
        return any(_references_field(item, field_name) for item in obj)
    return False


def _or_depth(obj: Any, current: int = 0) -> int:
    """Calculate maximum nesting depth of $or operators."""
    if isinstance(obj, dict):
        depth = current + 1 if "$or" in obj else current
        child_depths = [
            _or_depth(v, current + 1) if k == "$or" else _or_depth(v, current)
            for k, v in obj.items()
        ]
        return max([depth] + child_depths) if child_depths else depth
    elif isinstance(obj, list):
        return max((_or_depth(item, current) for item in obj), default=current)
    return current


def check_conditional_operators(
    query: Dict[str, Any], time_field: str
) -> ValidationResult:
    """
    Validate CONDITIONAL operators are used safely.

    Rules:
        - $or: max depth 1 (no nested $or)
        - $nor: must not reference time_field
        - $not: must not be applied to time_field

    Args:
        query: MongoDB query dict
        time_field: Name of time field (e.g., "timestamp")

    Returns:
        ValidationResult with is_valid and reason

    Examples:
        >>> check_conditional_operators(
        ...     {"$or": [{"a": 1}, {"b": 2}], "ts": {"$gte": t1}},
        ...     "ts"
        ... )
        ValidationResult(is_valid=True)

        >>> check_conditional_operators(
        ...     {"$or": [{"$or": [{...}]}, {...}]},
        ...     "ts"
        ... )
        ValidationResult(is_valid=False, reason="nested $or (depth 2 > 1)")

        >>> check_conditional_operators(
        ...     {"ts": {"$not": {"$lt": "2024-01-15"}}},
        ...     "ts"
        ... )
        ValidationResult(is_valid=False, reason="$not applied to time field 'ts'")
    """
    # Check $or depth
    depth = _or_depth(query)
    if depth > 1:
        return ValidationResult(False, f"nested $or (depth {depth} > 1)")

    # Check for empty $or array
    def check_empty_or(obj: Any) -> Optional[str]:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "$or" and isinstance(value, list) and len(value) == 0:
                    return "$or with empty array matches no documents"
                error = check_empty_or(value)
                if error:
                    return error
        elif isinstance(obj, list):
            for item in obj:
                error = check_empty_or(item)
                if error:
                    return error
        return None

    error = check_empty_or(query)
    if error:
        return ValidationResult(False, error)

    # Check $nor doesn't reference time field
    def check_tree(obj: Any, parent_key: Optional[str] = None) -> Optional[str]:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "$nor" and isinstance(value, list):
                    for clause in value:
                        if _references_field(clause, time_field):
                            return f"$nor references time field '{time_field}'"
                if key == "$not" and parent_key == time_field:
                    return f"$not applied to time field '{time_field}'"
                error = check_tree(value, key)
                if error:
                    return error
        elif isinstance(obj, list):
            for item in obj:
                error = check_tree(item, parent_key)
                if error:
                    return error
        return None

    error = check_tree(query)
    return ValidationResult(False, error) if error else ValidationResult(True)


def validate_query_for_chunking(
    query: Dict[str, Any], time_field: str
) -> Tuple[bool, str]:
    """
    Validate query operators are compatible with chunking.

    This validates operators only - does not check for time bounds.
    For full chunkability check including time bounds, use is_chunkable_query().

    Args:
        query: MongoDB find() filter
        time_field: Name of time field for chunking

    Returns:
        Tuple of (is_valid, reason)

    Examples:
        # Valid query with common operators
        >>> validate_query_for_chunking({
        ...     "account_id": ObjectId("..."),
        ...     "region_id": {"$in": [ObjectId("..."), ...]},
        ...     "timestamp": {"$gte": t1, "$lt": t2}
        ... }, "timestamp")
        (True, '')

        # $or with per-branch time ranges (typical XLR8 pattern)
        >>> validate_query_for_chunking({
        ...     "$or": [
        ...         {"sensor": "A", "timestamp": {"$gte": t1, "$lt": t2}},
        ...         {"sensor": "B", "timestamp": {"$gte": t3, "$lt": t4}}
        ...     ],
        ...     "account_id": ObjectId("...")
        ... }, "timestamp")
        (True, '')

        # Cannot chunk: contains $expr (requires full dataset)
        >>> validate_query_for_chunking({
        ...     "$expr": {"$gt": ["$endTime", "$startTime"]}
        ... }, "timestamp")
        (False, "operator '$expr' requires full dataset (cannot chunk)")

        # Cannot chunk: geospatial operator
        >>> validate_query_for_chunking({
        ...     "location": {"$near": {"$geometry": {...}}}
        ... }, "timestamp")
        (False, "operator '$near' requires full dataset (cannot chunk)")
    """
    # Check for operators requiring full dataset (cannot chunk/parallelize)
    # Recurses the query tree and returns on first forbidden operator found.
    has_forbidden, op = has_forbidden_ops(query)
    if has_forbidden:
        return False, f"operator '{op}' requires full dataset (cannot chunk)"

    # Validate conditional operators
    result = check_conditional_operators(query, time_field)
    if not result:
        return False, result.reason

    return True, ""


# =============================================================================
# QUERY STRUCTURE ANALYSIS
# =============================================================================


def or_depth(obj: Any, depth: int = 0) -> int:
    """
    Calculate $or nesting depth (backwards-compatible API).

    Returns 0 for no $or, 1 for top-level $or, 2+ for nested.
    """
    if isinstance(obj, dict):
        local = 1 if "$or" in obj else 0
        return max(
            [depth + local]
            + [or_depth(v, depth + (1 if k == "$or" else 0)) for k, v in obj.items()]
        )
    if isinstance(obj, list):
        return max((or_depth(x, depth) for x in obj), default=depth)
    return depth


def split_global_and(
    query: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Split query into global AND conditions and $or branches.

    Used by brackets.py for bracket extraction.
    Note: is_chunkable_query() uses normalize_query() for validation.

    IMPORTANT: This function expects a normalized query (no nested $and).
    Use normalize_query() first if query structure is unknown.
    Used by bracket extraction to create parallel work units.

    Args:
        query: MongoDB query dict

    Returns:
        Tuple of (global_conditions, or_branches)
        or_branches is empty list if no $or present

    Examples:
        # Simple query without $or
        >>> split_global_and({"status": "active", "value": {"$gt": 0}})
        ({'status': 'active', 'value': {'$gt': 0}}, [])

        # Query with $or - separates global from branches
        >>> split_global_and({
        ...     "$or": [{"sensor": "A"}, {"sensor": "B"}],
        ...     "account_id": "123",
        ...     "timestamp": {"$gte": t1, "$lt": t2}
        ... })
        ({'account_id': '123', 'timestamp': {...}}, [{'sensor': 'A'}, {'sensor': 'B'}])

        # The global conditions apply to ALL branches:
        # Bracket 1: {"account_id": "123", "timestamp": {...}, "sensor": "A"}
        # Bracket 2: {"account_id": "123", "timestamp": {...}, "sensor": "B"}
    """
    q = dict(query)

    # Case 1: Direct top-level $or
    if "$or" in q:
        or_list = q.pop("$or")
        if not isinstance(or_list, list):
            return {}, []

        global_and: dict[str, Any] = {}
        if "$and" in q and isinstance(q["$and"], list):
            for item in q.pop("$and"):
                if isinstance(item, dict):
                    global_and.update(item)
        global_and.update(q)
        return global_and, or_list

    # Case 2: $or inside $and
    if "$and" in q and isinstance(q["$and"], list):
        and_items = q.pop("$and")
        found_or: list[dict[str, Any]] = []
        global_and: dict[str, Any] = {}

        for item in and_items:
            if not isinstance(item, dict):
                return {}, []
            if "$or" in item:
                if found_or:
                    return {}, []  # Multiple $or not supported
                or_content = item["$or"]
                if not isinstance(or_content, list):
                    return {}, []
                found_or = or_content
            else:
                global_and.update(item)

        global_and.update(q)
        return global_and, found_or

    # Case 3: No $or
    return q, []


# =============================================================================
# TIME BOUNDS EXTRACTION
# =============================================================================


def normalize_datetime(dt: Any) -> datetime | None:
    """
    Normalize to timezone-aware UTC datetime.

    Handles datetime objects and ISO format strings.
    Returns None if parsing fails.
    """
    if isinstance(dt, datetime):
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    if isinstance(dt, str):
        try:
            parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            return None
    return None


def extract_time_bounds(
    query_dict: dict[str, Any], time_field: str
) -> tuple[datetime | None, datetime | None]:
    """
    Extract time range [lo, hi) from query.

    Used by brackets.py for bracket time range extraction.
    Note: is_chunkable_query() uses extract_time_bounds_recursive() for validation.

    Handles:
        - $gte/$gt as lower bound
        - $lt/$lte as upper bound ($lte converted to $lt by adding 1 microsecond)

    Args:
        query_dict: Query fragment containing time field
        time_field: Name of time field

    Returns:
        Tuple of (lower_bound, upper_bound), each is datetime or None

    Examples:
        >>> extract_time_bounds(
        ...     {"timestamp": {"$gte": datetime(2024, 1, 1),
        ...     "$lt": datetime(2024, 2, 1)}},
        ...     "timestamp"
        ... )
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 1, tzinfo=UTC))

    Current implementation: $lte: t  $lt: t + 1 microsecond
    Pros: Clean internal model (half-open intervals [lo, hi))
    Cons: Tiny semantic change at microsecond precision
    Impact: Minimal - most MongoDB timestamps are millisecond precision anyway
    """
    lo, hi = None, None
    tf = query_dict.get(time_field)

    if isinstance(tf, dict):
        lo = normalize_datetime(tf.get("$gte") or tf.get("$gt"))
        if "$lt" in tf:
            hi = normalize_datetime(tf["$lt"])
        elif "$lte" in tf:
            hi_temp = normalize_datetime(tf["$lte"])
            if hi_temp:
                hi = hi_temp + timedelta(microseconds=1)

    return lo, hi


def normalize_query(query: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, bool]]:
    """
    Normalize query structure for consistent analysis.

    Transformations:
    - Flatten nested $and operators
    - Detect complexity patterns (multiple $or, nested $or)

    Args:
        query: MongoDB find() filter

    Returns:
        Tuple of (normalized_query, complexity_flags)
        - normalized_query: Flattened query
        - complexity_flags: {multiple_or, nested_or, complex_negation}
    """

    def flatten_and_operators(obj: Any) -> Any:
        """Recursively flatten nested $and operators."""
        if not isinstance(obj, dict):
            return obj

        result = {}
        for key, value in obj.items():
            if key == "$and" and isinstance(value, list):
                # Flatten nested $and
                flattened = []
                for item in value:
                    if isinstance(item, dict) and len(item) == 1 and "$and" in item:
                        # Nested $and - merge up
                        flattened.extend(flatten_and_operators(item)["$and"])
                    else:
                        flattened.append(flatten_and_operators(item))
                result["$and"] = flattened
            elif isinstance(value, dict):
                result[key] = flatten_and_operators(value)
            elif isinstance(value, list):
                result[key] = [flatten_and_operators(item) for item in value]
            else:
                result[key] = value

        return result

    def count_or_operators(obj: Any, depth: int = 0) -> Tuple[int, int]:
        """
        Count $or operators and find max nesting depth.
        Returns (or_count, max_or_depth)
        """
        if not isinstance(obj, dict):
            return 0, depth

        or_count = 0
        max_depth = depth

        for key, value in obj.items():
            if key == "$or":
                or_count += 1
                current_depth = depth + 1
                max_depth = max(max_depth, current_depth)

                # Check for nested $or inside branches
                if isinstance(value, list):
                    for branch in value:
                        sub_count, sub_depth = count_or_operators(branch, current_depth)
                        or_count += sub_count
                        max_depth = max(max_depth, sub_depth)
            elif isinstance(value, dict):
                sub_count, sub_depth = count_or_operators(value, depth)
                or_count += sub_count
                max_depth = max(max_depth, sub_depth)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        sub_count, sub_depth = count_or_operators(item, depth)
                        or_count += sub_count
                        max_depth = max(max_depth, sub_depth)

        return or_count, max_depth

    # Step 1: Flatten nested $and
    normalized = flatten_and_operators(query)

    # Step 2: Detect $or complexity
    or_count, max_or_depth = count_or_operators(normalized)

    # Step 3: Build complexity flags
    flags = {
        "multiple_or": or_count > 1,
        "nested_or": max_or_depth > 1,
        "complex_negation": False,  # Checked later by check_negation_safety()
    }

    return normalized, flags


def extract_time_bounds_recursive(
    query: Dict[str, Any], time_field: str, context: str = "POSITIVE"
) -> Tuple[Optional[Tuple[datetime, datetime]], bool]:
    """
    Recursively extract time bounds from query tree.

    Handles nested structures, $and (intersection), $or (union).

    Args:
        query: Query dict
        time_field: Name of time field
        context: "POSITIVE" or "NEGATED" (inside $nor/$not)

    Returns:
        Tuple of (time_bounds, has_time_ref)
        - time_bounds: (lo, hi) or None
        - has_time_ref: True if query references time field anywhere
    """

    def extract_from_time_field(value: Any) -> Tuple[Optional[Tuple], bool]:
        """Extract bounds from time field value."""
        if context == "NEGATED":
            # Time field in negated context  can't use
            return None, True

        if not isinstance(value, dict):
            # Direct equality: {"timestamp": t1}
            dt = normalize_datetime(value)
            return ((dt, dt), True) if dt else (None, True)

        lo, hi = None, None

        for op, operand in value.items():
            if op == "$gte":
                new_lo = normalize_datetime(operand)
                # Take most restrictive lower bound
                if new_lo:
                    lo = max(lo, new_lo) if lo is not None else new_lo
            elif op == "$gt":
                dt = normalize_datetime(operand)
                if dt:
                    new_lo = dt + timedelta(microseconds=1)
                    # Take most restrictive lower bound
                    lo = max(lo, new_lo) if lo is not None else new_lo
            elif op == "$lt":
                new_hi = normalize_datetime(operand)
                # Take most restrictive upper bound
                if new_hi:
                    hi = min(hi, new_hi) if hi is not None else new_hi
            elif op == "$lte":
                dt = normalize_datetime(operand)
                if dt:
                    new_hi = dt + timedelta(microseconds=1)
                    # Take most restrictive upper bound
                    hi = min(hi, new_hi) if hi is not None else new_hi
            elif op == "$eq":
                dt = normalize_datetime(operand)
                lo = hi = dt
            elif op == "$in":
                # Take envelope
                if isinstance(operand, list):
                    if not operand:
                        # Empty $in array matches no documents
                        return None, True
                    dates = [normalize_datetime(d) for d in operand]
                    dates = [d for d in dates if d is not None]
                    if dates:
                        lo = min(dates)
                        hi = max(dates)
            elif op in {"$ne", "$nin", "$not"}:
                # Negation on time field
                return None, True

        if lo is not None and hi is not None:
            # Validate bounds are sensible
            if lo >= hi:
                # Contradictory bounds (e.g., $gte: 2024-02-01, $lt: 2024-01-01)
                return None, True
            return (lo, hi), True

        return None, True

    def intersect_bounds(b1: Tuple, b2: Tuple) -> Optional[Tuple]:
        """Intersect two bounds."""
        lo = max(b1[0], b2[0])
        hi = min(b1[1], b2[1])

        if lo >= hi:
            return None  # Empty intersection

        return (lo, hi)

    # Check if this is time field directly
    if time_field in query:
        return extract_from_time_field(query[time_field])

    # Handle $and (intersection of bounds)
    if "$and" in query:
        all_bounds = []
        has_time_ref = False

        for item in query["$and"]:
            if isinstance(item, dict):
                bounds, has_ref = extract_time_bounds_recursive(
                    item, time_field, context
                )
                if has_ref:
                    has_time_ref = True
                if bounds:
                    all_bounds.append(bounds)

        if not all_bounds:
            return None, has_time_ref

        # Intersection
        merged = all_bounds[0]
        for bounds in all_bounds[1:]:
            merged = intersect_bounds(merged, bounds)
            if merged is None:
                return None, has_time_ref

        return merged, has_time_ref

    # Handle $or (union/envelope of bounds)
    if "$or" in query:
        all_bounds = []
        all_have_time_ref = []
        has_time_ref = False
        has_any_partial_or_missing = False

        for item in query["$or"]:
            if isinstance(item, dict):
                bounds, has_ref = extract_time_bounds_recursive(
                    item, time_field, context
                )
                all_have_time_ref.append(has_ref)

                if has_ref:
                    has_time_ref = True

                    if bounds is None:
                        # Branch references time field but has partial/no bounds
                        has_any_partial_or_missing = True
                    else:
                        all_bounds.append(bounds)
                else:
                    # Branch doesn't reference time field at all
                    has_any_partial_or_missing = True

        # CRITICAL: If ANY branch is unbounded, partial, or doesn't reference time,
        # we cannot safely extract bounds. Taking envelope of only bounded branches
        # would cause data loss from unbounded/unreferenced branches.
        if has_any_partial_or_missing:
            return None, has_time_ref

        if not all_bounds:
            return None, has_time_ref

        # All branches have full bounds - safe to take union (envelope)
        lo = min(b[0] for b in all_bounds)
        hi = max(b[1] for b in all_bounds)

        return (lo, hi), has_time_ref

    # Handle $nor (negates context)
    if "$nor" in query:
        new_context = "NEGATED" if context == "POSITIVE" else "POSITIVE"
        has_time_ref = False

        for item in query["$nor"]:
            if isinstance(item, dict):
                _, has_ref = extract_time_bounds_recursive(
                    item, time_field, new_context
                )
                if has_ref:
                    has_time_ref = True

        # $nor with time ref is unsafe (inverted bounds)
        return None, has_time_ref

    # Check all nested dicts
    all_bounds = []
    has_time_ref = False

    for key, value in query.items():
        if isinstance(value, dict):
            bounds, has_ref = extract_time_bounds_recursive(value, time_field, context)
            if has_ref:
                has_time_ref = True
            if bounds:
                all_bounds.append(bounds)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    bounds, has_ref = extract_time_bounds_recursive(
                        item, time_field, context
                    )
                    if has_ref:
                        has_time_ref = True
                    if bounds:
                        all_bounds.append(bounds)

    # Merge bounds (intersection)
    if not all_bounds:
        return None, has_time_ref

    merged = all_bounds[0]
    for bounds in all_bounds[1:]:
        merged = intersect_bounds(merged, bounds)
        if merged is None:
            return None, has_time_ref

    return merged, has_time_ref


def check_negation_safety(query: Dict[str, Any], time_field: str) -> Tuple[bool, str]:
    """
    Check if negation operators safely avoid time field.

    Ensures $nor, $not, $ne, $nin don't reference time field.

    Args:
        query: MongoDB find() filter
        time_field: Name of time field

    Returns:
        Tuple of (is_safe, rejection_reason)
    """

    def references_time_field(obj: Any, depth: int = 0) -> bool:
        """Check if query references time field at any nesting level."""
        if depth > 10:  # Prevent infinite recursion
            return False

        if not isinstance(obj, dict):
            return False

        if time_field in obj:
            return True

        for key, value in obj.items():
            if isinstance(value, dict):
                if references_time_field(value, depth + 1):
                    return True
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        if references_time_field(item, depth + 1):
                            return True

        return False

    def find_time_negations(obj: Any) -> List[str]:
        """Find negation operators applied to time field."""
        if not isinstance(obj, dict):
            return []

        negations = []

        if time_field in obj:
            time_value = obj[time_field]
            if isinstance(time_value, dict):
                for op in ["$ne", "$nin", "$not"]:
                    if op in time_value:
                        negations.append(op)

        for key, value in obj.items():
            if isinstance(value, dict):
                negations.extend(find_time_negations(value))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        negations.extend(find_time_negations(item))

        return negations

    # Check $nor
    if "$nor" in query:
        for branch in query["$nor"]:
            if isinstance(branch, dict) and references_time_field(branch):
                return False, f"$nor references time field '{time_field}'"

    # Check $not, $ne, $nin on time field
    unsafe = find_time_negations(query)
    if unsafe:
        return False, f"Negation operators on time field: {', '.join(set(unsafe))}"

    return True, ""


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================


def is_chunkable_query(
    query: Dict[str, Any], time_field: str
) -> Tuple[bool, str, Tuple[Optional[datetime], Optional[datetime]]]:
    """
    Determine if query can be chunked for parallel execution.

    A query is chunkable if:
        1. Contains no forbidden operators (geospatial, text, $expr, etc.)
        2. Conditional operators used safely ($or depth <=1, no $not on time field)
        3. Has extractable time bounds (both lower and upper)
        4. No negations on time field ($nor/$not/$ne/$nin)

    Args:
        query: MongoDB find() filter
        time_field: Name of time field for chunking

    Returns:
        Tuple of (is_chunkable, reason_if_not, (lo, hi))

    Examples:
        # Simple query - chunkable
        >>> is_chunkable_query({
        ...     "account_id": ObjectId("..."),
        ...     "device_id": {"$in": [ObjectId("..."), ...]},
        ...     "timestamp": {"$gte": t1, "$lt": t2}
        ... }, "timestamp")
        (True, '', (t1, t2))

        # $or query - chunkable
        >>> is_chunkable_query({
        ...     "$or": [
        ...         {"sensor": "A", "timestamp": {"$gte": t1, "$lt": t2}},
        ...         {"sensor": "B", "timestamp": {"$gte": t3, "$lt": t4}}
        ...     ]
        ... }, "timestamp")
        (True, '', (min(t1, t3), max(t2, t4)))

        # Nested $and with $or - NOW chunkable!
        >>> is_chunkable_query({
        ...     "$and": [
        ...         {"account_id": "123"},
        ...         {"$and": [
        ...             {"timestamp": {"$gte": t1, "$lt": t2}},
        ...             {"$or": [{"region": "US"}, {"region": "EU"}]}
        ...         ]}
        ...     ]
        ... }, "timestamp")
        (True, '', (t1, t2))

        # Multiple $or - NOW chunkable (single bracket, time-chunked)!
        >>> is_chunkable_query({
        ...     "$and": [
        ...         {"$or": [{"region": "US"}, {"region": "EU"}]},
        ...         {"$or": [{"sensor": "A"}, {"sensor": "B"}]}
        ...     ],
        ...     "timestamp": {"$gte": t1, "$lt": t2}
        ... }, "timestamp")
        (True, '', (t1, t2))
    """
    # Normalize query structure
    normalized, complexity_flags = normalize_query(query)

    # Validate operators
    # Check nested $or (reject if depth > 1)
    if complexity_flags["nested_or"]:
        return False, "nested $or operators (depth > 1) not supported", (None, None)

    # Check for empty $or array (matches no documents)
    if (
        "$or" in normalized
        and isinstance(normalized.get("$or"), list)
        and len(normalized["$or"]) == 0
    ):
        return False, "$or with empty array matches no documents", (None, None)

    # Check forbidden operators
    has_forbidden, op = has_forbidden_ops(normalized)
    if has_forbidden:
        return False, f"contains forbidden operator: {op}", (None, None)

    # Check conditional operators
    result = check_conditional_operators(normalized, time_field)
    if not result:
        return False, result.reason, (None, None)

    # Extract time bounds
    time_bounds, has_time_ref = extract_time_bounds_recursive(normalized, time_field)

    if not has_time_ref:
        return False, "no time field reference found", (None, None)

    if time_bounds is None:
        # More specific error messages based on the query structure
        if "$or" in normalized:
            # Check if $or has unbounded/partial branches
            reason = (
                "$or query has unbounded or partial time constraints "
                "in one or more branches"
            )
        elif "$ne" in str(normalized) or "$nin" in str(normalized):
            reason = "query contains negation operators ($ne/$nin) on time field"
        elif "$in" in str(normalized) and "[]" in str(normalized):
            reason = "query contains empty $in array on time field"
        else:
            reason = "no complete time range (invalid or contradictory bounds)"
        return False, reason, (None, None)

    lo, hi = time_bounds

    # Validate bounds are sensible
    if lo >= hi:
        return (
            False,
            (
                "invalid time range: lower bound >= upper bound "
                "(contradictory constraints)"
            ),
            (None, None),
        )

    # Check negation safety
    is_safe, reason = check_negation_safety(normalized, time_field)
    if not is_safe:
        return False, reason, (None, None)

    return True, "", (lo, hi)
