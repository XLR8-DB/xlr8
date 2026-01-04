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
        ✓ {"$or": [{"region": "US"}, {"region": "EU"}]}
        ✗ {"$or": [{"$or": [{...}]}, {...}]}

        ✓ {"$nor": [{"status": "deleted"}], "timestamp": {...}}
        ✗ {"$nor": [{"timestamp": {"$lt": t1}}]}

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

_all__ = [
    # Classification sets
    "ALWAYS_ALLOWED",
    "CONDITIONAL",
]

# =============================================================================
# OPERATOR CLASSIFICATION
# =============================================================================

ALWAYS_ALLOWED: frozenset[str] = frozenset(
    {
        # - Comparison -----------------------------
        # Compare field value against a constant. Always document-local.
        #
        # Example: Find all sensors with readings above threshold
        #   {"value": {"$gt": 100}, "timestamp": {"$gte": t1, "$lt": t2}}
        #
        "$eq",  # {"status": {"$eq": "active"}}  — equals
        "$ne",  # {"status": {"$ne": "deleted"}} — not equals
        "$gt",  # {"value": {"$gt": 100}}        — greater than
        "$gte",  # {"value": {"$gte": 100}}       — greater or equal
        "$lt",  # {"value": {"$lt": 0}}          — less than
        "$lte",  # {"value": {"$lte": 100}}       — less or equal
        "$in",  # {"type": {"$in": ["A", "B"]}}  — in set
        "$nin",  # {"type": {"$nin": ["X", "Y"]}} — not in set
        # - Element -------------------------------
        # Check field existence or BSON type. Document-local metadata checks.
        #
        # Example: Only include documents with validated readings
        #   {"validated_at": {"$exists": true}, "value": {"$type": "double"}}
        #
        "$exists",  # {"email": {"$exists": true}}
        "$type",  # {"value": {"$type": "double"}}
        # - Array --------------------------------
        # Evaluate array fields within a single document.
        #
        # Example: Find sensors with all required tags
        #   {"tags": {"$all": ["calibrated", "production"]}}
        #
        "$all",  # {"tags": {"$all": ["a", "b"]}}
        "$elemMatch",  # {"readings": {"$elemMatch": {"value": {"$gt": 100}}}}
        "$size",  # {"items": {"$size": 3}}
        # - Bitwise -------------------------------
        # Compare integer bits against a bitmask. Document-local.
        #
        # Example: Find flags with specific bits set
        #   {"flags": {"$bitsAllSet": [0, 2, 4]}}
        #
        "$bitsAllClear",
        "$bitsAllSet",
        "$bitsAnyClear",
        "$bitsAnySet",
        # - Evaluation (safe) --------------------------
        # Pattern matching and validation that is document-local.
        #
        # Example: Match sensor names by pattern
        #   {"sensor_id": {"$regex": "^TEMP_", "$options": "i"}}
        #
        "$regex",  # {"name": {"$regex": "^sensor_"}}
        "$options",  # Modifier for $regex
        "$mod",  # {"value": {"$mod": [10, 0]}}  — divisible by 10
        "$jsonSchema",  # {"$jsonSchema": {"required": ["name"]}}
        "$comment",  # {"$comment": "audit query"}  — annotation only
        # - Logical (safe) ---------------------------
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
        # - $or ---------------------------------
        # ALLOWED at depth 1 only. Top-level $or is decomposed into "brackets"
        # which are executed and cached independently.
        #
        #  ALLOWED (depth 1):
        #   {"$or": [
        #       {"sensor_id": "A", "timestamp": {"$gte": t1, "$lt": t2}},
        #       {"sensor_id": "B", "timestamp": {"$gte": t1, "$lt": t2}}
        #   ]}
        #
        #  REJECTED (depth 2 - nested $or):
        #   {"$or": [{"$or": [{...}, {...}]}, {...}]}
        #
        "$or",
        # - $nor --------------------------------
        # ALLOWED if not referencing time field. Negating time bounds creates
        # unpredictable behavior when chunking.
        #
        #  ALLOWED (excludes status values):
        #   {"$nor": [{"status": "deleted"}, {"status": "draft"}],
        #    "timestamp": {"$gte": t1, "$lt": t2}}
        #
        #  REJECTED (negates time constraint):
        #   {"$nor": [{"timestamp": {"$lt": "2024-01-01"}}]}
        #
        "$nor",
        # - $not --------------------------------
        # ALLOWED if not applied to time field. Same reasoning as $nor.
        #
        #  ALLOWED (negates value constraint):
        #   {"value": {"$not": {"$lt": 0}}}   — equivalent to value >= 0
        #
        #  REJECTED (negates time constraint):
        #   {"timestamp": {"$not": {"$lt": "2024-01-15"}}}
        #
        "$not",
    }
)

NEVER_ALLOWED: frozenset[str] = frozenset(
    {
        # - Evaluation (unsafe) -------------------------
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
        # - Text Search -----------------------------
        # $text uses text indexes and corpus-wide IDF scoring.
        # Not typical for time-series - XLR8 is for sensor data, not full-text search
        #
        "$text",
        # - Atlas Search ----------------------------
        # Atlas-specific full-text and vector search operators.
        # Not typical for time-series - XLR8 is for sensor data, not full-text search
        #
        "$search",
        "$vectorSearch",
        # - Geospatial -----------------------------
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
