"""
MongoDB Query Validator for XLR8 Parallel Execution.

XLR8 speeds up MongoDB queries by splitting them into smaller pieces that can be
fetched in parallel. This module checks if a query is safe to split.

================================================================================
HOW XLR8 PARALLELIZES QUERIES
================================================================================

Simple example - fetch 1 year of sensor data:

    # Original MongoDB query (slow - fetches 365 days serially)
    db.sensors.find({
        "sensor_id": "temp_001",
        "timestamp": {"$gte": jan_1, "$lt": jan_1_next_year}
    })

    # XLR8 automatically splits this into 26 parallel chunks (14 days each)
    # and fetches them simultaneously using Rust workers.

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
    Each bracket is split into 14-day chunks that are fetched in parallel.
    Results are written to separate Parquet files.

This module validates that queries meet safety requirements for both phases.
It does NOT perform the actual splitting, only validation.

================================================================================
WHAT MAKES A QUERY SAFE TO PARALLELIZE?
================================================================================

A query is safe if it meets ALL these requirements:

1. TIME BOUNDS - Query must have a specific time range
    SAFE:   {"timestamp": {"$gte": t1, "$lt": t2}}
    UNSAFE: {"timestamp": {"$gte": t1}}  (no upper bound)

2. DOCUMENT-LOCAL OPERATORS - Each document can be evaluated independently
    SAFE:   {"value": {"$gt": 100}}      (compare field to constant)
    UNSAFE: {"$near": {"$geometry": ...}} (needs all docs to sort by distance)

3. NO TIME FIELD NEGATION - Cannot use $ne/$nin/$not on the time field
    SAFE:   {"status": {"$nin": ["deleted", "draft"]}}
    UNSAFE: {"timestamp": {"$nin": [specific_date]}}

   Why? Negating time creates unbounded ranges. Saying "not this date" means
   you need ALL other dates, which breaks the ability to split by time.

4. SIMPLE $or STRUCTURE - No nested $or operators
    SAFE:   {"$or": [{"a": 1}, {"b": 2}]}
    UNSAFE: {"$or": [{"$or": [{...}]}, {...}]}

================================================================================
OPERATOR REFERENCE
================================================================================

ALWAYS_ALLOWED (23 operators)
    These are safe for time chunking because they evaluate each document
    independently without needing other documents.

    Comparison:  $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin
    Element:     $exists, $type
    Array:       $all, $elemMatch, $size
    Bitwise:     $bitsAllClear, $bitsAllSet, $bitsAnyClear, $bitsAnySet
    Evaluation:  $regex, $mod, $jsonSchema
    Logical:     $and
    Metadata:    $comment, $options

    Note: When used in $or branches, brackets.py performs additional overlap
    checks to prevent duplicate results. For example:
        {"$or": [{"x": {"$in": [1,2,3]}}, {"x": {"$in": [3,4,5]}}]}
    The value 3 appears in both branches, so this needs special handling.

CONDITIONAL (3 operators)
    Safe only under specific conditions:

    $or   - Allowed at depth 1 only (no nested $or)
    $nor  - Allowed if it does NOT reference the time field
    $not  - Allowed if NOT applied to the time field

    Examples:
        allowed - {"$or": [{"region": "US"}, {"region": "EU"}]}
        disallowed - {"$or": [{"$or": [{...}]}, {...}]}

        allowed - {"$nor": [{"status": "deleted"}], "timestamp": {...}}
        disallowed - {"$nor": [{"timestamp": {"$lt": t1}}]}

NEVER_ALLOWED (17 operators)
    These cannot be parallelized safely:

    Geospatial:  $near, $nearSphere, $geoWithin, $geoIntersects, $geometry,
                 $box, $polygon, $center, $centerSphere, $maxDistance, $minDistance
    Text:        $text
    Dynamic:     $expr, $where
    Atlas:       $search, $vectorSearch
    Legacy:      $uniqueDocs

    Why? These operators either:
    - Require the full dataset ($near sorts ALL docs by distance)
    - Use corpus-wide statistics ($text uses IDF scores across all docs)
    - Cannot be statically analyzed ($expr can contain arbitrary logic)

================================================================================
API USAGE
================================================================================

    from xlr8.analysis import is_chunkable_query

    # Check if query can be parallelized
    query = {
        "sensor_id": "temp_001",
        "timestamp": {"$gte": jan_1, "$lt": feb_1}
    }

    is_safe, reason, (start, end) = is_chunkable_query(query, "timestamp")

    if is_safe:
        print(f"Can parallelize from {start} to {end}")
    else:
        print(f"Cannot parallelize: {reason}")

    # Common rejection reasons:
    # - "no complete time range (invalid or contradictory bounds)"
    # - "query contains negation operators ($ne/$nin) on time field"
    # - "contains forbidden operator: $near"
    # - "nested $or operators (depth > 1) not supported"

================================================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_all__ = [
    # Classification sets
    "ALWAYS_ALLOWED",
    "CONDITIONAL",
    "NEVER_ALLOWED",
    # Validation
    "ValidationResult",
    "has_forbidden_ops",
    "check_conditional_operators",
    "validate_query_for_chunking",
    # Query analysis utilities
    "or_depth",
    "split_global_and",
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
        # [X] REJECTED (depth 2 - nested $or):
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
        # [X] REJECTED (negates time constraint):
        #   {"$nor": [{"timestamp": {"$lt": "2024-01-01"}}]}
        #
        "$nor",
        # -- $not ---------------------------------------------------------------------
        # ALLOWED if not applied to time field. Same reasoning as $nor.
        #
        # [OK] ALLOWED (negates value constraint):
        #   {"value": {"$not": {"$lt": 0}}}   - equivalent to value >= 0
        #
        # [X] REJECTED (negates time constraint):
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


# =============================================================================
# CORE VALIDATION FUNCTIONS
# =============================================================================


def has_forbidden_ops(query: Any) -> tuple[bool, str | None]:
    """
    Check if query contains any NEVER_ALLOWED operator.

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
    query: dict[str, Any], time_field: str
) -> ValidationResult:
    """
    Validate CONDITIONAL operators are used safely.

    Rules:
        - $or: max depth 1 (no nested $or)
        - $nor: must not reference time_field
        - $not: must not be applied to time_field

    Args:
        query: MongoDB query dict
        time_field: Name of time field (e.g., "recordedAt")

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
    def check_empty_or(obj: Any) -> str | None:
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
    def check_tree(obj: Any, parent_key: str | None = None) -> str | None:
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
    query: dict[str, Any], time_field: str
) -> tuple[bool, str]:
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
        ...     "recordedAt": {"$gte": t1, "$lt": t2}
        ... }, "recordedAt")
        (True, '')

        # $or with per-branch time ranges (typical XLR8 pattern)
        >>> validate_query_for_chunking({
        ...     "$or": [
        ...         {"sensor": "A", "recordedAt": {"$gte": t1, "$lt": t2}},
        ...         {"sensor": "B", "recordedAt": {"$gte": t3, "$lt": t4}}
        ...     ],
        ...     "account_id": ObjectId("...")
        ... }, "recordedAt")
        (True, '')

        # Rejected: contains $expr
        >>> validate_query_for_chunking({
        ...     "$expr": {"$gt": ["$endTime", "$startTime"]}
        ... }, "recordedAt")
        (False, 'contains forbidden operator: $expr')

        # Rejected: geospatial operator
        >>> validate_query_for_chunking({
        ...     "location": {"$near": {"$geometry": {...}}}
        ... }, "recordedAt")
        (False, 'contains forbidden operator: $near')
    """
    # Phase 1: Check for forbidden operators
    # IT recurses the query tree and returns on first forbidden operator found.
    has_forbidden, op = has_forbidden_ops(query)
    if has_forbidden:
        return False, f"contains forbidden operator: {op}"

    # Phase 2: Validate conditional operators
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
        ...     "recordedAt": {"$gte": t1, "$lt": t2}
        ... })
        ({'account_id': '123', 'recordedAt': {...}}, [{'sensor': 'A'}, {'sensor': 'B'}])

        # The global conditions apply to ALL branches:
        # Bracket 1: {"account_id": "123", "recordedAt": {...}, "sensor": "A"}
        # Bracket 2: {"account_id": "123", "recordedAt": {...}, "sensor": "B"}
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
