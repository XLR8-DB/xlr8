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

import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from xlr8.analysis.inspector import (
    ChunkabilityMode,
    extract_time_bounds_recursive,
    has_forbidden_ops,
    is_chunkable_query,
    normalize_query,
    or_depth,
    split_global_and,
)

__all__ = [
    # Data structures
    "Bracket",
    "TimeRange",
    # Main public function
    "build_brackets_for_find",
]


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


@dataclass
class TimeRange:
    """
    Time range for a bracket.

    Attributes:
        lo: Lower bound datetime
        hi: Upper bound datetime
        is_full: Whether both lo and hi are specified
        hi_inclusive: If True, use $lte; if False, use $lt (default: False for $lt)
        lo_inclusive: If True, use $gte; if False, use $gt (default: True for $gte)

    Example:
        TimeRange(
            lo=datetime(2024, 1, 1, tzinfo=UTC),
            hi=datetime(2024, 7, 1, tzinfo=UTC),
            is_full=True,
            hi_inclusive=False,  # Use $lt
            lo_inclusive=True    # Use $gte
        )
    """

    lo: Optional[datetime]
    hi: Optional[datetime]
    is_full: bool
    hi_inclusive: bool = False  # Default to $lt for backward compatibility
    lo_inclusive: bool = True  # Default to $gte for backward compatibility


@dataclass
class Bracket:
    """
    A unit of work for parallel execution.

    Example:
        Bracket(
            static_filter={"account_id": ObjectId("123..."),
                          "region_id": ObjectId("64a...")},
            timerange=TimeRange(lo=2024-01-01, hi=2024-07-01, is_full=True)
        )

    This bracket will be converted to a MongoDB query:
        {
            "account_id": ObjectId("123..."),
            "region_id": ObjectId("64a..."),
            "timestamp": {"$gte": 2024-01-01, "$lt": 2024-07-01}
        }
    """

    static_filter: Dict[str, Any]
    timerange: TimeRange


# =============================================================================
# Add overlap detection helpers
# =============================================================================


def _has_negation_operators(query: Dict[str, Any]) -> bool:
    """
    Check if query contains any negation operators.

    Negation operators ($nin, $ne, $not, $nor) in an $or branch create
    potential overlap with other branches, leading to duplicate results.

    Args:
        query: A query dict (typically an $or branch)

    Returns:
        True if any negation operator is found at any nesting level

    Examples:
        >>> _has_negation_operators({"field": {"$in": [1,2,3]}})
        False
        >>> _has_negation_operators({"field": {"$nin": [1,2,3]}})
        True
        >>> _has_negation_operators({"$and": [{"field": {"$ne": 5}}]})
        True
    """

    def _check(obj: Any) -> bool:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in NEGATION_OPERATORS:
                    return True
                if _check(value):
                    return True
        elif isinstance(obj, list):
            for item in obj:
                if _check(item):
                    return True
        return False

    return _check(query)


def _has_overlap_prone_operators(
    query: Dict[str, Any], time_field: str
) -> Tuple[bool, Optional[str]]:
    """
    Check if query contains operators that can cause overlap between branches.

    These operators can match the same document even with different values:
    - $all: array superset matching
    - $elemMatch: array element matching
    - $regex: pattern matching
    - $mod: modulo matching
    - Comparison operators ($gt, $lt, etc.): ranges can overlap

    NOTE: Comparison operators on the TIME FIELD are allowed (that's how we chunk).
    Only comparison operators on OTHER fields trigger this check.

    Args:
        query: A query dict (typically an $or branch)
        time_field: The time field name (excluded from comparison operator check)

    Returns:
        Tuple of (has_overlap_prone, operator_name)

    Examples:
        >>> _has_overlap_prone_operators({"tags": {"$all": ["a", "b"]}}, "ts")
        (True, '$all')
        >>> _has_overlap_prone_operators({"name": {"$regex": "^John"}}, "ts")
        (True, '$regex')
        >>> _has_overlap_prone_operators({"ts": {"$gte": t1, "$lt": t2}}, "ts")
        (False, None)  # Time field comparison is OK
        >>> _has_overlap_prone_operators({"value": {"$gt": 10}}, "ts")
        (True, '$gt')  # Non-time field comparison is problematic
    """
    # Operators that are always problematic (not context-dependent)
    always_problematic = {
        "$all",
        "$elemMatch",
        "$regex",
        "$mod",
        "$bitsAllSet",
        "$bitsAnySet",
        "$bitsAllClear",
        "$bitsAnyClear",
    }

    # Comparison operators - only problematic on non-time fields
    comparison_ops = {"$gt", "$gte", "$lt", "$lte"}

    def _check(obj: Any, current_field: Optional[str] = None) -> Optional[str]:
        if isinstance(obj, dict):
            for key, value in obj.items():
                # Track current field for comparison operator check
                field = key if not key.startswith("$") else current_field

                if key in always_problematic:
                    return key

                # Comparison operators are only problematic on non-time fields
                if key in comparison_ops and current_field != time_field:
                    return key

                result = _check(value, field)
                if result:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = _check(item, current_field)
                if result:
                    return result
        return None

    op = _check(query)
    return (True, op) if op else (False, None)


def _extract_in_values(query: Dict[str, Any], field: str) -> Optional[Set[Any]]:
    """
    Extract $in values for a specific field from query.

    Args:
        query: Query dict to search
        field: Field name to look for $in on

    Returns:
        Set of values if $in found, None if field uses different operator or not present

    Examples:
        >>> _extract_in_values({"field": {"$in": [1, 2, 3]}}, "field")
        {1, 2, 3}
        >>> _extract_in_values({"field": 5}, "field")  # Equality, not $in
        None
        >>> _extract_in_values({"other": {"$in": [1]}}, "field")  # Different field
        None
    """
    if field not in query:
        return None

    val = query[field]
    if isinstance(val, dict) and "$in" in val:
        in_vals = val["$in"]
        if isinstance(in_vals, list):
            # Convert to set of hashable representations
            result = set()
            for v in in_vals:
                try:
                    result.add(v)
                except TypeError:
                    # Unhashable value - convert to string
                    result.add(str(v))
            return result

    return None


def _find_in_fields(query: Dict[str, Any]) -> Dict[str, Set[Any]]:
    """
    Find all fields that use $in operator and their values.

    Only looks at top-level fields (not nested in $and, etc.)

    Args:
        query: Query dict (typically an $or branch)

    Returns:
        Dict mapping field name to set of $in values

    Examples:
        >>> _find_in_fields({"a": {"$in": [1,2]}, "b": {"$in": [3,4]}})
        {"a": {1, 2}, "b": {3, 4}}
        >>> _find_in_fields({"a": 5, "b": {"$gt": 10}})
        {}
    """
    result: Dict[str, Set[Any]] = {}

    for field, value in query.items():
        if field.startswith("$"):
            continue  # Skip operators
        if isinstance(value, dict) and "$in" in value:
            in_vals = value["$in"]
            if isinstance(in_vals, list):
                try:
                    result[field] = set(in_vals)
                except TypeError:
                    # Contains unhashable - convert to strings
                    result[field] = {str(v) for v in in_vals}

    return result


def _get_non_time_fields(branch: Dict[str, Any], time_field: str) -> Set[str]:
    """Get all top-level field names except the time field and operators."""
    return {k for k in branch.keys() if not k.startswith("$") and k != time_field}


def _check_or_branch_safety(
    branches: List[Dict[str, Any]], global_and: Dict[str, Any], time_field: str
) -> Tuple[bool, str, Optional[List[Dict[str, Any]]]]:
    """
    Analyze $or branches for safety (no overlapping result sets).

    This function implements the safe algorithm for detecting when $or
    branches can be executed independently as brackets vs when they must
    be executed as a single query to avoid duplicates.

    SAFETY RULES:
    1. If ANY branch has negation operators -> UNSAFE (cannot transform)
    2. If branches have different field sets -> UNSAFE (cannot determine overlap)
    3. If exactly ONE $in field differs -> TRANSFORM (subtract overlapping values)
    4. If multiple $in fields differ -> UNSAFE (explosion of combinations)
    5. If same $in fields with disjoint values -> SAFE
    6. If same equality values -> SAFE (same static_filter, handled by grouping)

    Args:
        branches: List of $or branch dicts
        global_and: Global conditions applied to all branches
        time_field: Time field name (excluded from field comparison)

    Returns:
        Tuple of (is_safe, reason, transformed_branches)
        - is_safe: True if brackets can be executed independently
        - reason: Description of why unsafe (empty if safe)
        - transformed_branches: Modified branches if transformation applied,
          None otherwise
    """
    if len(branches) <= 1:
        return True, "", None  # Single branch is always safe

    # Rule 1a: Check for negation operators in any branch
    for i, branch in enumerate(branches):
        if _has_negation_operators(branch):
            return (
                False,
                f"branch {i} contains negation operator ($nin/$ne/$not/$nor)",
                None,
            )

    # Rule 1b: Check for overlap-prone operators in any branch
    # These operators can match the same document across branches even with
    # different values
    for i, branch in enumerate(branches):
        has_overlap_op, op = _has_overlap_prone_operators(branch, time_field)
        if has_overlap_op:
            return False, f"branch {i} contains overlap-prone operator ({op})", None

    # Merge each branch with global_and for analysis
    effective_branches = []
    for br in branches:
        eff = {**global_and, **br}
        # Remove time field for field comparison
        if time_field in eff:
            eff_copy = dict(eff)
            eff_copy.pop(time_field)
            effective_branches.append(eff_copy)
        else:
            effective_branches.append(eff)

    # Rule 2: Check if all branches have the same field set
    field_sets = [_get_non_time_fields(eb, time_field) for eb in effective_branches]
    first_fields = field_sets[0]
    for i, fs in enumerate(field_sets[1:], 1):
        if fs != first_fields:
            return False, f"branch {i} has different field set than branch 0", None

    # All branches have same fields - now check for $in overlap
    # Find all $in fields in each branch
    all_in_fields: List[Dict[str, Set[Any]]] = [
        _find_in_fields(eb) for eb in effective_branches
    ]

    # Collect all $in field names across all branches
    in_field_names: Set[str] = set()
    for in_dict in all_in_fields:
        in_field_names.update(in_dict.keys())

    if not in_field_names:
        # No $in fields - check for equality overlap
        # Branches with identical static_filters will be grouped/merged by
        # the main algorithm. Different equality values are always disjoint (safe)
        return True, "", None

    # For each $in field, check if all branches use $in on it
    # and identify overlapping values
    fields_with_overlap: Dict[str, List[Tuple[int, int, Set[Any]]]] = {}

    for field in in_field_names:
        # Get $in values for this field from each branch
        branch_values: List[Optional[Set[Any]]] = []
        for in_dict in all_in_fields:
            branch_values.append(in_dict.get(field))

        # Check for overlap between any pair of branches
        overlaps: List[Tuple[int, int, Set[Any]]] = []
        for i in range(len(branches)):
            vals_i = branch_values[i]
            if vals_i is None:
                # This branch doesn't use $in on this field - could be equality
                # This creates potential overlap issues
                continue
            for j in range(i + 1, len(branches)):
                vals_j = branch_values[j]
                if vals_j is None:
                    continue
                common = vals_i & vals_j
                if common:
                    overlaps.append((i, j, common))

        if overlaps:
            fields_with_overlap[field] = overlaps

    if not fields_with_overlap:
        # No overlapping $in values - safe!
        return True, "", None

    # Rule 3 & 4: Handle overlapping $in values
    # IMPORTANT: Transformation is ONLY safe when all branches have the SAME
    # time bounds! If time bounds differ, we cannot subtract $in values because:
    #   - Branch A (IDs 1,2,3) with time [t1, t2]
    #   - Branch B (IDs 2,3,4) with time [t0, t3] (wider)
    #   If we remove 2,3 from Branch B, documents with IDs 2,3 in [t0,t1) and (t2,t3]
    #   would be LOST - not covered by either branch!
    #
    # So if overlapping $in values exist AND time ranges differ -> fall back
    # to single bracket

    # Extract time bounds from each branch to check if they're identical
    time_bounds = []
    for br in branches:
        combined = {**global_and, **br}
        bounds, _ = extract_time_bounds_recursive(combined, time_field)
        if bounds is None:
            lo, hi = None, None
        else:
            lo, hi, hi_inclusive, lo_inclusive = bounds
        time_bounds.append((lo, hi))

    # Check if all time bounds are identical
    first_bounds = time_bounds[0]
    all_same_time = all(bounds == first_bounds for bounds in time_bounds)

    if not all_same_time:
        # Overlapping $in with different time ranges - CANNOT safely transform
        return (
            False,
            (
                f"overlapping $in on '{list(fields_with_overlap.keys())[0]}' "
                "with different time ranges"
            ),
            None,
        )

    if len(fields_with_overlap) > 1:
        # Multiple $in fields have overlap - too complex to transform
        return (
            False,
            f"multiple $in fields have overlap: {list(fields_with_overlap.keys())}",
            None,
        )

    # Exactly one $in field has overlap AND same time ranges - we can transform
    field = list(fields_with_overlap.keys())[0]
    overlaps = fields_with_overlap[field]

    # Transform: For each pair with overlap, subtract overlapping values from one branch
    # Strategy: Build a "seen" set and subtract from later branches
    transformed = [deepcopy(br) for br in branches]
    seen_values: Set[Any] = set()

    for i, branch in enumerate(transformed):
        # Get current $in values for this branch (merged with global)
        eff = {**global_and, **branch}
        in_vals = _extract_in_values(eff, field)

        if in_vals is None:
            # Branch uses equality on this field - add to seen
            if field in eff and not isinstance(eff.get(field), dict):
                try:
                    seen_values.add(eff[field])
                except TypeError:
                    seen_values.add(str(eff[field]))
            continue

        # Subtract already-seen values
        remaining = in_vals - seen_values

        if not remaining:
            # All values already covered - mark branch for removal
            transformed[i] = None  # type: ignore
        elif remaining != in_vals:
            # Some values removed - update the $in
            if (
                field in branch
                and isinstance(branch.get(field), dict)
                and "$in" in branch[field]
            ):
                branch[field]["$in"] = list(remaining)
            elif field in global_and:
                # Field is in global_and - need to override in branch
                branch[field] = {"$in": list(remaining)}

        # Add all original values to seen (they're now covered by this bracket)
        seen_values.update(in_vals)

    # Filter out None branches (fully covered)
    transformed = [b for b in transformed if b is not None]

    if not transformed:
        # Edge case: all branches were fully covered (shouldn't happen normally)
        return True, "", None

    return True, "", transformed


# ============================================================================
# MAIN INTERFACE/ ENTRY Point
# ============================================================================


def _json_key(d: Dict[str, Any]) -> str:
    """Create a deterministic JSON key for deduplication."""
    return json.dumps(d, sort_keys=True, default=str)


def _merge_full_ranges(ranges: List[TimeRange]) -> List[TimeRange]:
    """Merge overlapping or adjacent time ranges into consolidated spans.

    Sorts ranges by start time, then iterates through merging any
    that overlap or touch (end of one equals start of next).
    Preserves hi_inclusive and lo_inclusive flags.
    """

    rs = [r for r in ranges if r.is_full and r.lo and r.hi]
    if not rs:
        return []

    rs.sort(key=lambda r: r.lo)  # type: ignore[arg-type]
    out: List[TimeRange] = [
        TimeRange(rs[0].lo, rs[0].hi, True, rs[0].hi_inclusive, rs[0].lo_inclusive)
    ]
    for r in rs[1:]:
        last = out[-1]
        # Type assertions: we filtered for r.lo and r.hi being not None above
        assert r.lo is not None and r.hi is not None
        assert last.lo is not None and last.hi is not None
        if r.lo <= last.hi:  # overlap or touch
            if r.hi > last.hi:
                last.hi = r.hi
                last.hi_inclusive = r.hi_inclusive
            elif r.hi == last.hi:
                last.hi_inclusive = last.hi_inclusive or r.hi_inclusive
        else:
            out.append(TimeRange(r.lo, r.hi, True, r.hi_inclusive, r.lo_inclusive))
    return out


def _partial_covers_full(partial: TimeRange, full: TimeRange) -> bool:
    """Check if a partial time range completely covers a full time range.

    A partial range covers a full range if:
    - partial has only $gte (lo) and full.lo >= partial.lo
    - partial has only $lt (hi) and full.hi <= partial.hi

    Args:
        partial: TimeRange with is_full=False (missing lo or hi)
        full: TimeRange with is_full=True

    Returns:
        True if partial completely covers full, False otherwise
    """
    if full.lo is None or full.hi is None:
        return False

    # Partial has only lower bound ($gte): covers if full starts at or after
    if partial.lo is not None and partial.hi is None:
        return full.lo >= partial.lo

    # Partial has only upper bound ($lt): covers if full ends at or before
    if partial.lo is None and partial.hi is not None:
        return full.hi <= partial.hi

    return False


def _merge_partial_ranges(partials: List[TimeRange]) -> List[TimeRange]:
    """Merge partial ranges where possible.

    Priority:
    - If ANY range is completely unbounded (no lo, no hi), it covers everything
    - Two $gte-only: keep the one with smallest lo (covers most)
    - Two $lt-only: keep the one with largest hi (covers most)
    Preserves lo_inclusive and hi_inclusive flags.
    """
    if not partials:
        return []

    # Check for completely unbounded ranges first - they cover everything
    unbounded = [r for r in partials if r.lo is None and r.hi is None]
    if unbounded:
        # One unbounded range covers all other partials
        return [TimeRange(None, None, False, False, True)]

    gte_only = [r for r in partials if r.lo is not None and r.hi is None]
    lt_only = [r for r in partials if r.lo is None and r.hi is not None]

    merged: List[TimeRange] = []

    # For $gte-only, keep the smallest lo (covers most data)
    assert gte_only or lt_only, "No partial ranges to merge"

    if gte_only:
        # Filter out None values for type safety
        min_lo = min(r.lo for r in gte_only if r.lo is not None)
        # Find the lo_inclusive from the range with min_lo
        lo_inclusive = next(r.lo_inclusive for r in gte_only if r.lo == min_lo)
        merged.append(TimeRange(min_lo, None, False, False, lo_inclusive))

    # For $lt-only, keep the largest hi (covers most data)
    if lt_only:
        max_hi = max(r.hi for r in lt_only if r.hi is not None)
        # Find the hi_inclusive from the range with max_hi
        hi_inclusive = next(r.hi_inclusive for r in lt_only if r.hi == max_hi)
        merged.append(TimeRange(None, max_hi, False, hi_inclusive, True))

    return merged


def build_brackets_for_find(
    query: Dict[str, Any],
    time_field: str,
    sort_spec: Optional[List[Tuple[str, int]]] = None,
) -> Tuple[
    bool, str, List[Bracket], Optional[Tuple[Optional[datetime], Optional[datetime]]]
]:
    """
    Build bracket list for a find() query based on its chunkability.

    This is the SINGLE ENTRY POINT for bracket creation. All queries flow through
    here to ensure consistent validation and bracket generation.

    IMPORTANT: Internally calls is_chunkable_query() to validate the query and
    determine execution mode (PARALLEL/SINGLE/REJECT). Cursor methods should NOT
    call is_chunkable_query() separately - this function handles all validation.

    Args:
        query: MongoDB find() filter dict
        time_field: Name of the timestamp field used for time-based chunking
                   (e.g., "timestamp", "recordedAt", "createdAt")
        sort_spec: Optional MongoDB sort specification as list of
                   (field, direction) tuples. Required for detecting $natural
                   sort. Format: [("field", 1)] or [("field", -1)]
                   Example: [("timestamp", 1)] or [("$natural", -1)]

    Returns:
        Tuple of (is_chunkable, reason, brackets, bounds):

        - is_chunkable: bool
            - True: Query is valid and executable (PARALLEL or SINGLE mode)
            - False: Invalid query syntax or contradictory constraints (REJECT mode)

        - reason: str
            - Empty string "" for PARALLEL mode (successful parallelization)
            - Descriptive message for SINGLE mode
              (e.g., "$natural sort requires insertion order")
            - Error description for REJECT mode
              (e.g., "empty $or array (invalid MongoDB syntax)")

        - brackets: List[Bracket]
            - PARALLEL mode: Non-empty list of Bracket objects for parallel execution
            - SINGLE mode: Empty list [] (signals to use single worker)
            - REJECT mode: Empty list []

        - bounds: Tuple[Optional[datetime], Optional[datetime]]
            - Time range extracted from query (lo, hi)
            - (None, None) if no time bounds found or query rejected

    CRITICAL: Empty brackets list has TWO meanings:
        1. If is_chunkable=True + brackets=[]: SINGLE mode (valid, use single worker)
        2. If is_chunkable=False + brackets=[]: REJECT mode (invalid, don't execute)

    Callers MUST check is_chunkable first, then interpret empty brackets accordingly.

    Example:
        >>> query = {
        ...     "$or": [
        ...         {"region_id": ObjectId("64a...")},
        ...         {"region_id": ObjectId("64b...")},
        ...     ],
        ...     "account_id": ObjectId("123..."),
        ...     "timestamp": {"$gte": datetime(2024,1,1), "$lt": datetime(2024,7,1)}
        ... }
        >>> ok, reason, brackets, bounds = build_brackets_for_find(query, "timestamp")
        >>> # Returns:
        >>> # (True, "", [
        >>> #     Bracket(static_filter={"account_id": "123...",
        >>> #                            "region_id": "64a..."},
        >>> #             timerange=TimeRange(lo=2024-01-01, hi=2024-07-01,
        >>> #                                 is_full=True)),
        >>> #     Bracket(static_filter={"account_id": "123...",
        >>> #                            "region_id": "64b..."},
        >>> #             timerange=TimeRange(lo=2024-01-01, hi=2024-07-01,
        >>> #                                 is_full=True)),
        >>> # ], (datetime(2024,1,1), datetime(2024,7,1)))

    Rejection Cases (returns is_chunkable=False):
        - Empty $or array (invalid MongoDB syntax) -> REJECT
        - Contradictory time bounds (lo >= hi) -> REJECT

    Single-Worker Cases (returns is_chunkable=True, empty brackets):
        - $natural sort (insertion order) -> SINGLE
        - Forbidden operators ($expr, $text, $near, etc.) -> SINGLE
        - Nested $or (depth > 1) -> SINGLE
        - Time field negation ($ne/$nin/$not/$nor on time field) -> SINGLE
        - Unbounded $or branches -> SINGLE
        - No time field reference -> SINGLE

    Implementation Note - Multiple Time Bounds Extraction:
        This function calls extract_time_bounds_recursive() multiple times in different
        code paths for different purposes:

        1. Via is_chunkable_query() - Validates overall query has time bounds
           Returns: result.bounds = union of all time ranges in query

        2. In _check_or_branch_safety() - Checks if $or branches have
           identical time bounds
           Purpose: Overlapping $in values can only be safely transformed
                   when all branches have the SAME time range. Different
                   ranges would cause data loss.
           Example: Branch A [Jan 1-15] with IDs {1,2,3} vs Branch B
                   [Jan 10-31] with IDs {2,3,4}. Cannot remove overlap {2,3}
                   because documents in [Jan 1-10) would be lost!

        3. In merge attempt (unsafe $or handling) - Extracts bounds from
           each branch
           Purpose: If branches have overlapping results (unsafe), check if
                   they can be merged into a single bracket. Only possible if
                   time ranges are contiguous with no gaps.
           Example: Branch A [Jan 1-15], Branch B [Jan 10-20]
                   -> Merged [Jan 1-20] ✓
                   Branch A [Jan 1-15], Branch B [Jan 20-31]
                   -> Cannot merge (gap!) ✗

        4. In final bracket creation - Sets TimeRange for each output
           bracket
           Purpose: Each bracket needs its specific time range for chunking.
           Example: {"sensor": "A", ts: [Jan 1-15]}
                   -> Bracket with TimeRange(Jan 1, Jan 15)
                   {"sensor": "B", ts: [Feb 1-28]}
                   -> Bracket with TimeRange(Feb 1, Feb 28)

        Why multiple calls are necessary:
        - is_chunkable_query() returns UNION of time bounds (overall range)
        - Each $or branch may have DIFFERENT time bounds (per-branch ranges)
        - Safety checks need to compare bounds across branches (identical?)
        - Merge logic needs to check contiguity (adjacent/overlapping?)
        - Final brackets need their specific ranges (individual TimeRange objects)

        This is NOT redundant - each extraction serves a different purpose in the
        validation -> optimization -> construction pipeline.
    """

    # PHASE 0: Validate query using is_chunkable_query
    # This is now the ONLY validation point - cursor methods don't need to
    # call it separately
    result = is_chunkable_query(query, time_field, sort_spec)

    bounds = result.bounds

    # Handle REJECT mode - invalid query syntax or contradictory constraints
    if result.mode == ChunkabilityMode.REJECT:
        return False, result.reason, [], (None, None)

    # Handle SINGLE mode - valid query, but single-worker fallback needed
    if result.mode == ChunkabilityMode.SINGLE:
        # Return empty brackets as signal to use single worker
        # is_chunkable=True means query is VALID and executable
        # Empty brackets means "don't parallelize, use single worker"
        return True, result.reason, [], bounds

    # =========================================================================
    # DEFENSE-IN-DEPTH: Redundant safety checks
    # =========================================================================
    # These checks duplicate validation already done in is_chunkable_query().
    # They're kept as a safety net in case:
    # 1. is_chunkable_query() has a bug and returns PARALLEL incorrectly
    # 2. Future code changes bypass is_chunkable_query() validation
    # 3. Query is mutated between validation and bracket building
    #
    # PARANOID but JUSTIFIED: Better to catch issues twice than produce
    # incorrect results. These checks are fast and prevent data corruption.
    # =========================================================================

    # High-level safety checks (kept for defense-in-depth)
    has_forbidden, forbidden_op = has_forbidden_ops(query)
    if has_forbidden:
        return False, f"forbidden-operator: {forbidden_op}", [], (None, None)

    # PHASE 1: Normalize query (flatten nested $and, detect complexity)
    normalized, complexity_flags = normalize_query(query)

    # Use normalized query for all subsequent operations
    global_and, or_list = split_global_and(normalized)

    # Check for nested $or or multiple $or
    if complexity_flags["nested_or"]:
        return False, "nested-or-depth>1", [], (None, None)

    if or_depth(normalized) > 1:
        return False, "nested-or-depth>1", [], (None, None)

    # No $or: treat as single branch represented by global_and
    if not or_list:
        branches: List[Dict[str, Any]] = [global_and]
    else:
        # =====================================================================
        # SAFETY CHECK: Detect overlapping $or branches
        # =====================================================================
        # Before splitting $or into independent brackets, we must verify that
        # branches don't have overlapping result sets. Overlap causes duplicates.
        #
        # Cases that cause overlap:
        # - Negation operators ($nin, $ne, $not, $nor) in any branch
        # - Overlapping $in values across branches
        # - Different field sets (can't determine disjointness)
        #
        # If overlap is detected and cannot be transformed, we return a single
        # bracket covering the entire query (executed as unchunked).
        # =====================================================================
        is_safe, reason, transformed = _check_or_branch_safety(
            or_list, global_and, time_field
        )

        if not is_safe:
            # Unsafe $or pattern detected - but check if we can MERGE branches
            #
            # OPTIMIZATION: If all branches have IDENTICAL static filters
            # (excluding time), AND their time ranges are contiguous (no gaps),
            # we can MERGE them into a single bracket with the union of time
            # ranges.
            #
            # Example (mergeable - overlapping):
            #   $or: [
            #     {filter_A, timestamp: {$gte: Jan 1, $lt: Jan 20}},
            #     {filter_A, timestamp: {$gte: Jan 15, $lt: Feb 1}},
            #   ]
            #   -> Merged: {filter_A, timestamp: {$gte: Jan 1, $lt: Feb 1}}
            #
            # Example (NOT mergeable - disjoint with gap):
            #   $or: [
            #     {filter_A, timestamp: {$gte: Jan 1, $lt: Jan 15}},
            #     {filter_A, timestamp: {$gte: Feb 1, $lt: Feb 15}},
            #   ]
            #   -> Cannot merge! Gap from Jan 15 to Feb 1 would include unwanted data.
            #   -> Fall back to single bracket with full $or query.

            # Extract static filters (without time) from each branch
            static_filters = []
            time_bounds_list = []
            has_unbounded_branch = False
            has_partial_branch = False  # Only $gte or only $lt

            for branch in or_list:
                combined = {**global_and, **branch}
                bounds, _ = extract_time_bounds_recursive(combined, time_field)
                if bounds is None:
                    branch_lo, branch_hi, branch_hi_inc, branch_lo_inc = (
                        None,
                        None,
                        False,
                        True,
                    )
                else:
                    branch_lo, branch_hi, branch_hi_inc, branch_lo_inc = bounds

                # Check if this branch has NO time constraint at all
                if branch_lo is None and branch_hi is None:
                    has_unbounded_branch = True
                # Check if partial (only one bound)
                elif branch_lo is None or branch_hi is None:
                    has_partial_branch = True

                time_bounds_list.append(
                    (branch_lo, branch_hi, branch_hi_inc, branch_lo_inc)
                )

                # Extract static filter (without time)
                static_wo_time = dict(combined)
                if time_field in static_wo_time:
                    static_wo_time.pop(time_field)
                static_filters.append(static_wo_time)

            # Check if all static filters are identical
            all_static_identical = all(
                _json_key(sf) == _json_key(static_filters[0])
                for sf in static_filters[1:]
            )

            # Can only merge if:
            # 1. All static filters identical
            # 2. All time ranges are FULL (both lo and hi)
            # 3. Time ranges are contiguous (no gaps)
            can_merge = False
            merged_lo, merged_hi = None, None
            merged_hi_inclusive, merged_lo_inclusive = False, True

            if (
                all_static_identical
                and not has_unbounded_branch
                and not has_partial_branch
            ):
                # All branches have identical static filters and full time ranges
                # Check if time ranges are contiguous (no gaps)
                #
                # Algorithm: Sort by start time, then verify each range starts
                # at or before the previous range's end (overlap or adjacent)
                full_ranges = [
                    (lo, hi, hi_inc, lo_inc)
                    for lo, hi, hi_inc, lo_inc in time_bounds_list
                ]
                sorted_ranges = sorted(full_ranges, key=lambda r: r[0])

                # Start with first range
                running_lo = sorted_ranges[0][0]
                running_hi = sorted_ranges[0][1]
                running_lo_inclusive = sorted_ranges[0][3]
                running_hi_inclusive = sorted_ranges[0][2]
                has_gap = False

                for lo, hi, hi_inc, lo_inc in sorted_ranges[1:]:
                    if lo > running_hi:
                        # Gap detected! This range starts after the previous ends
                        has_gap = True
                        break
                    # Extend running_hi if this range extends further
                    if hi > running_hi:
                        running_hi = hi
                        running_hi_inclusive = hi_inc
                    elif hi == running_hi:
                        running_hi_inclusive = running_hi_inclusive or hi_inc

                if not has_gap:
                    # All ranges are contiguous - we can merge!
                    merged_lo = running_lo
                    merged_hi = running_hi
                    merged_hi_inclusive = running_hi_inclusive
                    merged_lo_inclusive = running_lo_inclusive
                    can_merge = True

            if can_merge:
                # Merge into single clean bracket
                return (
                    True,
                    f"merged-branches:{reason}",
                    [
                        Bracket(
                            static_filter=static_filters[0],
                            timerange=TimeRange(
                                merged_lo,
                                merged_hi,
                                True,
                                merged_hi_inclusive,
                                merged_lo_inclusive,
                            ),
                        )
                    ],
                    (merged_lo, merged_hi),
                )

            # Cannot merge - fall back to single bracket with full $or
            # This preserves the original $or semantics
            lo, hi = None, None
            hi_inclusive, lo_inclusive = False, True

            for branch_lo, branch_hi, branch_hi_inc, branch_lo_inc in time_bounds_list:
                if branch_lo is not None:
                    if lo is None or branch_lo < lo:
                        lo = branch_lo
                        lo_inclusive = branch_lo_inc
                    elif branch_lo == lo:
                        lo_inclusive = lo_inclusive or branch_lo_inc
                if branch_hi is not None:
                    if hi is None or branch_hi > hi:
                        hi = branch_hi
                        hi_inclusive = branch_hi_inc
                    elif branch_hi == hi:
                        hi_inclusive = hi_inclusive or branch_hi_inc

            # If any branch is unbounded, the whole query is unbounded
            if has_unbounded_branch:
                lo, hi = None, None
                hi_inclusive, lo_inclusive = False, True

            # Build the single bracket with original query structure
            single_filter = dict(query)
            if time_field in single_filter:
                single_filter.pop(time_field)

            is_full = lo is not None and hi is not None
            return (
                True,
                f"single-bracket:{reason}",
                [
                    Bracket(
                        static_filter=single_filter,
                        timerange=TimeRange(
                            lo, hi, is_full, hi_inclusive, lo_inclusive
                        ),
                    )
                ],
                (lo, hi),
            )

        # Use transformed branches if available
        branches = transformed if transformed else or_list

    prelim: List[Bracket] = []
    for br in branches:
        if not isinstance(br, Dict):
            return False, "branch-not-dict", [], (None, None)

        eff: Dict[str, Any] = {}
        if global_and:
            eff.update(global_and)
        eff.update(br)

        br_bounds, _ = extract_time_bounds_recursive(eff, time_field)
        if br_bounds is None:
            lo, hi, hi_inclusive, lo_inclusive = None, None, False, True
        else:
            lo, hi, hi_inclusive, lo_inclusive = br_bounds
        is_full = lo is not None and hi is not None

        # Remove time field from static filter
        static_wo_time = dict(eff)
        if time_field in static_wo_time:
            static_wo_time.pop(time_field)

        if "$or" in static_wo_time:
            return False, "nested-or-in-branch", [], (None, None)

        prelim.append(
            Bracket(
                static_filter=static_wo_time,
                timerange=TimeRange(lo, hi, is_full, hi_inclusive, lo_inclusive),
            )
        )

    grouped: Dict[str, Dict[str, Any]] = {}
    for b in prelim:
        key = _json_key(b.static_filter)
        g = grouped.get(key)
        if g is None:
            g = {"static": b.static_filter, "full": [], "partial": []}
            grouped[key] = g
        (g["full"] if b.timerange.is_full else g["partial"]).append(b.timerange)

    out_brackets: List[Bracket] = []
    for g in grouped.values():
        static = g["static"]
        full_ranges = g["full"]
        partial_ranges = g["partial"]

        # Merge partial ranges first (keep most inclusive)
        # NOTE: _merge_partial_ranges handles unbounded (lo=None, hi=None) by
        # returning just the unbounded range, which covers everything
        merged_partials = _merge_partial_ranges(partial_ranges)

        # Check if any partial is completely unbounded - if so, it covers ALL
        # (both other partials AND all full ranges in this group)
        has_unbounded = any(r.lo is None and r.hi is None for r in merged_partials)
        if has_unbounded:
            # Unbounded covers everything - just emit the unbounded bracket
            out_brackets.append(
                Bracket(
                    static_filter=static,
                    timerange=TimeRange(None, None, False, False, True),
                )
            )
            continue  # Skip all full and other partial for this static_filter

        # Check if any partial covers all full ranges
        # If so, we only need the partial (it fetches everything the fulls would)
        remaining_fulls: List[TimeRange] = []
        for fr in full_ranges:
            covered = False
            for pr in merged_partials:
                if _partial_covers_full(pr, fr):
                    covered = True
                    break
            if not covered:
                remaining_fulls.append(fr)

        # Merge remaining full ranges
        for r in _merge_full_ranges(remaining_fulls):
            out_brackets.append(Bracket(static_filter=static, timerange=r))

        # Add merged partial ranges (these will be executed as single unchunked queries)
        for r in merged_partials:
            out_brackets.append(Bracket(static_filter=static, timerange=r))

    if not out_brackets:
        return False, "no-complete-time-range", [], (None, None)

    return True, "", out_brackets, bounds
