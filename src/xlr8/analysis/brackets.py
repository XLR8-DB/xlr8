"""Bracket-based query analysis for XLR8.

================================================================================
DATA FLOW - QUERY TO BRACKETS
================================================================================

This module transforms a MongoDB query into "Brackets" - the fundamental unit
of work for parallel execution.

WHAT IS A BRACKET?
--------------------------------------------------------------------------------

A Bracket = static_filter + TimeRange

It represents ONE chunk of work that can be executed independently:
- static_filter: Non-time conditions (e.g., {"region_id": "64a..."})
- timerange: Time bounds (lo, hi) that can be further chunked

EXAMPLE TRANSFORMATION:
--------------------------------------------------------------------------------

INPUT QUERY:
    {
        "$or": [
            {"region_id": ObjectId("64a...")},
            {"region_id": ObjectId("64b...")},
            {"region_id": ObjectId("64c...")},
        ],
        "account_id": ObjectId("123..."),  # Global AND condition
        "timestamp": {"$gte": datetime(2024,1,1), "$lt": datetime(2024,7,1)}
    }

STEP 1: split_global_and() extracts:
  global_and = {"account_id": ObjectId("123..."),
                "timestamp": {"$gte": ..., "$lt": ...}}
  or_list = [{"region_id": "64a..."},
             {"region_id": "64b..."}, ...]

STEP 2: For each $or branch, merge with global_and:
  Branch 1: {"account_id": "123...", "region_id": "64a...", "timestamp": {...}}
  Branch 2: {"account_id": "123...", "region_id": "64b...", "timestamp": {...}}
  ...

STEP 3: Extract time bounds and create Brackets:

    OUTPUT: List[Bracket]

    Bracket(
        static_filter={"account_id": "123...", "region_id": "64a..."},
        timerange=TimeRange(lo=2024-01-01, hi=2024-07-01, is_full=True)
    )

    Bracket(
        static_filter={"account_id": "123...", "region_id": "64b..."},
        timerange=TimeRange(lo=2024-01-01, hi=2024-07-01, is_full=True)
    )
    ...

NEXT STEP: Each bracket's timerange is chunked (14-day chunks) and queued
           for parallel execution.

WHY BRACKETS?
--------------------------------------------------------------------------------
1. Parallelization: Each bracket can be fetched independently
2. Caching: Same static_filter can reuse cached data
3. Time chunking: TimeRange can be split into smaller chunks for workers

================================================================================
"""

from typing import Set

# =============================================================================
# OVERLAP DETECTION HELPERS
# =============================================================================
# These helpers detect when $or branches may have overlapping result sets,
# which would cause duplicates when executing brackets independently.
#
# NEGATION OPERATORS: $nin, $ne, $not, $nor in an $or branch can overlap with
# other branches that use positive filters on the same field.
#
# $in OVERLAP: Two branches with $in on the same field may share values.
# Example: {"field": {"$in": [1,2,3]}} and {"field": {"$in": [3,4,5]}}
#
# INHERENTLY OVERLAPPING OPERATORS: Some operators can match the same document
# across different branches even with different values:
# - $all: {"tags": {"$all": ["a","b"]}} and {"tags": {"$all": ["b","c"]}}
#         both match a document with tags: ["a","b","c"]
# - $elemMatch: array element matching can overlap
# - $regex: pattern matching can overlap
# - $mod: modulo conditions can overlap
# - Comparison operators ($gt, $lt, etc.): ranges can overlap
# =============================================================================

# Operators that create negation/exclusion filters
NEGATION_OPERATORS: Set[str] = {"$nin", "$ne", "$not", "$nor"}

# Operators that can cause overlap between branches even with different values
# These should trigger single-bracket execution when used on differentiating fields
OVERLAP_PRONE_OPERATORS: Set[str] = {
    "$all",  # Array superset matching
    "$elemMatch",  # Array element matching
    "$regex",  # Pattern matching
    "$mod",  # Modulo matching
    "$gt",  # Greater than - ranges can overlap
    "$gte",  # Greater than or equal
    "$lt",  # Less than - ranges can overlap
    "$lte",  # Less than or equal
    "$bitsAllSet",  # Bitwise operations can overlap
    "$bitsAnySet",
    "$bitsAllClear",
    "$bitsAnyClear",
}
#          both match documents where field=3.
# =============================================================================

# Operators that create negation/exclusion filters
NEGATION_OPERATORS: Set[str] = {"$nin", "$ne", "$not", "$nor"}
