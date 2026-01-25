"""
Tests for brackets.py - Bracket-based query analysis for XLR8.

This module tests the transformation of MongoDB queries into "Brackets" -
independent units of parallel work that can be safely executed concurrently.

WHAT IS A BRACKET?
==================

A Bracket is the fundamental unit of parallel execution in XLR8:

    Bracket = static_filter + TimeRange

Where:
  - static_filter: MongoDB query dict (e.g., {"sensor_id": 123, "type": "telemetry"})
  - TimeRange: Time bounds with lo (inclusive) and hi (exclusive) datetime values

Example transformation:
  Query: {"sensor_id": {"$in": [1, 2, 3]}, "ts": {"$gte": t1, "$lt": t2}}
  Result: 3 Brackets:
    - Bracket({"sensor_id": 1}, TimeRange(t1, t2))
    - Bracket({"sensor_id": 2}, TimeRange(t1, t2))
    - Bracket({"sensor_id": 3}, TimeRange(t1, t2))

Each bracket can be time-chunked and executed in parallel without duplicates.


BRACKETS TRANSFORMATION ALGORITHM
==================================

The build_brackets_for_find() function performs 5 steps:

1. FORBIDDEN OPERATOR CHECK
   Reject queries with operators incompatible with chunking:
   - $expr, $where (cannot analyze statically)
   - $text, $search (corpus-wide scoring)
   - $near, $geoWithin (distance computations)
   Result: (False, "forbidden-operator: $xxx", [])

2. $or DEPTH CHECK
   Allow $or at depth 1 only (no nested $or inside $or).
   Nested $or creates exponential branch combinations.
   Result: (False, "nested-or-depth>1", [])

3. SPLIT QUERY
   Separate query into:
   - global_and: Conditions applied to ALL branches
   - or_list: List of $or branch dicts (empty if no $or)

   Example:
     Query: {"account_id": X, "$or": [{a:1}, {b:2}], "ts": {...}}
     global_and = {"account_id": X, "ts": {...}}
     or_list = [{a:1}, {b:2}]

4. BRANCH SAFETY CHECK
   If $or exists, determine if branches can be split into independent brackets
   without causing duplicate documents in results.

   SAFETY RULES:

   a) NEGATION OPERATORS ($nin, $ne, $not, $nor)
      Force single-bracket mode. Negations can match the same document across
      multiple branches.
      Example: $or: [{"status": "active"}, {"status": {"$ne": "deleted"}}]
               Document {"status": "pending"} matches BOTH branches!

   b) OVERLAP-PRONE OPERATORS
      Force single-bracket mode for: $all, $elemMatch, $regex, $mod,
      and comparison operators on non-time fields ($gt, $gte, $lt, $lte).
      Example: $or: [{"value": {"$gt": 10}}, {"value": {"$lt": 20}}]
               Document {"value": 15} matches BOTH branches (15>10 AND 15<20)!

   c) FIELD SET COMPARISON
      All branches must have the same set of field names (excluding time).
      Different field sets mean different filtering logic.

   d) $in VALUE OVERLAP
      If any field uses $in with overlapping values across branches:
      - Different time ranges? Force single-bracket
      - Multiple $in fields with overlap? Force single-bracket
      - Single $in field, same time, overlap? TRANSFORM by subtracting
        overlapping values from later branches

5. RESULT MODES

   MULTIPLE BRACKETS (safe to split):
     Each $or branch becomes an independent bracket.
     Example: $or: [{"sensor": A}, {"sensor": B}]
              Result: [Bracket({"sensor": A}, ...), Bracket({"sensor": B}, ...)]

   SINGLE BRACKET (unsafe to split):
     Cannot split branches, but can still time-chunk the full query.
     The $or is preserved in the static_filter.
     Example: $or with $nin operator
              Result: [Bracket({"$or": [...]}, ...)]

   MERGED BRACKET (special case):
     Branches have identical static filters with contiguous/overlapping time.
     Merge into single bracket with unified time range, no $or.


OVERLAP DETECTION - WHY IT MATTERS
===================================

Overlapping brackets cause duplicate documents in final results because the same
document would be fetched by multiple workers. XLR8 must guarantee disjoint
brackets to maintain result correctness.

Example of overlap:
  Branch 0: {"region": "US", "value": {"$gt": 10}}
  Branch 1: {"region": "US", "value": {"$lt": 20}}
  Document {"region": "US", "value": 15} matches BOTH branches!

XLR8 detects this overlap and forces single-bracket mode to prevent duplicates.


THREE-TIER CHUNKABILITY MODES
==============================

PARALLEL MODE (multiple brackets):
  Query can be split into independent brackets, each time-chunked and run
  in parallel by different workers.

  Requirements:
  - No forbidden operators
  - No nested $or
  - $or branches are disjoint (no overlap in matched documents)

SINGLE MODE (one bracket):
  Query cannot be split into multiple brackets due to overlap risk, but can
  still be time-chunked as a single unit.

  Reasons:
  - Negation operators in $or branches
  - Overlap-prone operators in $or branches
  - Different field sets across branches
  - $in overlap that cannot be resolved

  Performance: Moderate (time-chunking helps, but no parallelism across branches)

REJECT MODE:
  Query is fundamentally incompatible with chunking.

  Reasons:
  - Forbidden operators ($expr, $where, $text, $near, etc.)
  - Nested $or (depth > 1)

  Performance: Falls back to regular cursor iteration


TEST ORGANIZATION
=================

This test file covers ALL code paths:

1. REJECTION TESTS
   Each forbidden operator triggers reject mode

2. SINGLE BRACKET TESTS
   Negation operators, overlap-prone operators, field set mismatches

3. MERGE TESTS
   Identical static filters with overlapping/adjacent time ranges

4. MULTIPLE BRACKET TESTS
   Disjoint equality values, disjoint $in values

5. $in TRANSFORMATION TESTS
   Overlapping $in values with same time ranges (subtract and continue)

6. TIME RANGE HANDLING TESTS
   Full bounds, partial bounds, unbounded queries

7. REAL-WORLD SCENARIO TESTS
   device data with $nin, multi-region queries

Each test validates against actual brackets.py implementation.
"""

from datetime import datetime, timezone

import pytest
from bson import ObjectId

from xlr8.analysis.brackets import (
    _check_or_branch_safety,
    _extract_in_values,
    _find_in_fields,
    _has_negation_operators,
    _has_overlap_prone_operators,
    build_brackets_for_find,
)

# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture(name="time_field")
def time_field_fixture():
    return "recordedAt"


@pytest.fixture(name="t1")
def t1_fixture():
    return datetime(2024, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(name="t2")
def t2_fixture():
    return datetime(2024, 6, 1, tzinfo=timezone.utc)


# =============================================================================
# SECTION 1: REJECTION TESTS (NEVER_ALLOWED + Nested $or)
# =============================================================================


class TestRejectionNeverAllowedOperators:
    """Test that each NEVER_ALLOWED operator triggers SINGLE mode.

    These operators require the full dataset and cannot be parallelized:
    - $expr, $where: Cannot statically analyze
    - $text, $search, $vectorSearch: Corpus-wide scoring
    - Geospatial: Require distance computations across entire dataset

    They return is_chunkable=True (executable) but brackets=[] (single-worker mode).
    """

    @pytest.mark.parametrize(
        "operator,query_fragment",
        [
            # Evaluation operators
            ("$expr", {"$expr": {"$gt": ["$a", "$b"]}}),
            ("$where", {"$where": "this.a > this.b"}),
            # Text search
            ("$text", {"$text": {"$search": "mongodb"}}),
            # Atlas search (would be rejected if present)
            ("$search", {"$search": {"text": {"query": "test"}}}),
            ("$vectorSearch", {"$vectorSearch": {"vector": [1, 2, 3]}}),
            # Geospatial
            ("$near", {"location": {"$near": [0, 0]}}),
            (
                "$nearSphere",
                {
                    "location": {
                        "$nearSphere": {
                            "$geometry": {"type": "Point", "coordinates": [0, 0]}
                        }
                    }
                },
            ),
            ("$geoWithin", {"location": {"$geoWithin": {"$box": [[0, 0], [1, 1]]}}}),
            (
                "$geoIntersects",
                {
                    "location": {
                        "$geoIntersects": {
                            "$geometry": {"type": "Point", "coordinates": [0, 0]}
                        }
                    }
                },
            ),
        ],
    )
    def test_never_allowed_operator_rejected(
        self, operator, query_fragment, time_field, t1, t2
    ):
        query = {**query_fragment, time_field: {"$gte": t1, "$lt": t2}}
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        # SINGLE mode: executable but not parallelizable
        assert ok is True, f"{operator} should be executable in SINGLE mode"
        assert operator in reason, f"Reason should mention {operator}"
        assert "full dataset" in reason.lower() or "single-worker" in reason.lower()
        assert brackets == [], (
            f"{operator} should return empty brackets (single-worker)"
        )
        assert bounds == (
            t1,
            t2,
        ), "Should extract time bounds for single-worker execution"

    def test_forbidden_operator_nested_in_and(self, time_field, t1, t2):
        """Forbidden operators are detected even when nested (SINGLE mode)."""
        query = {
            "$and": [
                {"status": "active"},
                {"$expr": {"$gt": ["$endTime", "$startTime"]}},  # Nested!
            ],
            time_field: {"$gte": t1, "$lt": t2},
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)
        # SINGLE mode
        assert ok is True, "$expr should be executable in SINGLE mode"
        assert "$expr" in reason
        assert brackets == []
        assert bounds == (t1, t2)


class TestRejectionNestedOr:
    """Test that nested $or (depth > 1) triggers SINGLE mode.

    Complex but executable.
    """

    def test_nested_or_depth_2(self, time_field, t1, t2):
        query = {
            "$or": [
                {"$or": [{"a": 1}, {"a": 2}]},  # Nested!
                {"b": 3},
            ],
            time_field: {"$gte": t1, "$lt": t2},
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)
        assert ok is True  # Executable (SINGLE mode)
        assert "nested" in reason.lower()
        assert brackets == []  # Empty brackets = single-worker
        assert bounds == (t1, t2)

    def test_nested_or_depth_3(self, time_field, t1, t2):
        query = {
            "$or": [
                {"$or": [{"$or": [{"a": 1}]}, {"b": 2}]},  # Deeply nested
                {"c": 3},
            ],
            time_field: {"$gte": t1, "$lt": t2},
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)
        assert ok is True  # Executable (SINGLE mode)
        assert "nested" in reason.lower()
        assert brackets == []  # Empty brackets = single-worker
        assert bounds == (t1, t2)


# =============================================================================
# SECTION 2: SINGLE BRACKET TESTS (Unsafe patterns)
# =============================================================================


class TestSingleBracketNegationOperators:
    """Test that NEGATION_OPERATORS in $or branches force single-bracket.

    $nin, $ne, $not, $nor can match documents that other branches would
    also match, causing duplicates if split into independent brackets.
    """

    def test_nin_in_branch(self, time_field, t1, t2):
        """$nin forces single-bracket (or merged if identical filters)."""
        query = {
            "$or": [
                {"region_id": ObjectId(), time_field: {"$gte": t1, "$lt": t2}},
                {
                    "config_id": {"$nin": [ObjectId()]},
                    time_field: {"$gte": t1, "$lt": t2},
                },
            ]
        }
        ok, _reason, brackets, _bounds = build_brackets_for_find(query, time_field)
        assert ok is True
        assert len(brackets) == 1  # Single or merged

    def test_ne_in_branch(self, time_field, t1, t2):
        """$ne forces single-bracket."""
        query = {
            "$or": [
                {"status": "active", time_field: {"$gte": t1, "$lt": t2}},
                {"status": {"$ne": "deleted"}, time_field: {"$gte": t1, "$lt": t2}},
            ]
        }
        ok, _reason, brackets, _bounds = build_brackets_for_find(query, time_field)
        assert ok is True
        assert len(brackets) == 1

    def test_not_in_branch(self, time_field, t1, t2):
        """$not forces single-bracket."""
        query = {
            "$or": [
                {"name": "John", time_field: {"$gte": t1, "$lt": t2}},
                {
                    "name": {"$not": {"$regex": "^test"}},
                    time_field: {"$gte": t1, "$lt": t2},
                },
            ]
        }
        ok, _reason, brackets, _bounds = build_brackets_for_find(query, time_field)
        assert ok is True
        assert len(brackets) == 1

    def test_negation_deeply_nested(self, time_field, t1, t2):
        """Negation nested in $and is still detected."""
        query = {
            "$or": [
                {"a": 1, time_field: {"$gte": t1, "$lt": t2}},
                {
                    "$and": [{"b": {"$nin": [2, 3]}}],
                    time_field: {"$gte": t1, "$lt": t2},
                },
            ]
        }
        ok, _reason, brackets, _bounds = build_brackets_for_find(query, time_field)
        assert ok is True
        assert len(brackets) == 1


class TestSingleBracketOverlapProneOperators:
    """Test that OVERLAP_PRONE_OPERATORS in $or branches force single-bracket.

    These operators can match the same document from multiple branches
    even with different filter values.
    """

    @pytest.mark.parametrize(
        "desc,branch_filter",
        [
            # Always problematic
            ("$all on tags", {"tags": {"$all": ["a", "b"]}}),
            ("$elemMatch on items", {"items": {"$elemMatch": {"qty": {"$gt": 5}}}}),
            ("$regex on name", {"name": {"$regex": "^John"}}),
            ("$mod on qty", {"qty": {"$mod": [4, 0]}}),
            ("$bitsAllSet on flags", {"flags": {"$bitsAllSet": [0, 2]}}),
            ("$bitsAnySet on flags", {"flags": {"$bitsAnySet": [1, 3]}}),
            ("$bitsAllClear on flags", {"flags": {"$bitsAllClear": [4, 5]}}),
            ("$bitsAnyClear on flags", {"flags": {"$bitsAnyClear": [6, 7]}}),
            # Problematic on non-time fields
            ("$gt on value", {"value": {"$gt": 100}}),
            ("$gte on value", {"value": {"$gte": 100}}),
            ("$lt on value", {"value": {"$lt": 50}}),
            ("$lte on value", {"value": {"$lte": 50}}),
        ],
    )
    def test_overlap_prone_operator_creates_single_bracket(
        self, desc, branch_filter, time_field, t1, t2
    ):
        query = {
            "$or": [
                {**branch_filter, time_field: {"$gte": t1, "$lt": t2}},
                {**branch_filter, time_field: {"$gte": t1, "$lt": t2}},
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)
        assert ok is True, f"Query with {desc} should succeed"
        # Could be single-bracket OR merged-branches (both result in 1 bracket)
        assert "single-bracket" in reason or "merged-branches" in reason, (
            f"{desc} should trigger unsafe path"
        )
        assert len(brackets) == 1

    def test_comparison_on_time_field_allowed(self, time_field, t1, t2):
        """Comparison operators ON the time field are OK (that's how we chunk)."""
        oid1, oid2 = ObjectId(), ObjectId()
        query = {
            "$or": [
                {
                    "region_id": oid1,
                    time_field: {"$gte": t1, "$lt": t2},
                },  # $gte/$lt on time field
                {"region_id": oid2, time_field: {"$gte": t1, "$lt": t2}},
            ]
        }
        ok, _reason, brackets, _bounds = build_brackets_for_find(query, time_field)
        assert ok is True
        assert (
            len(brackets) == 2
        )  # MULTIPLE brackets (comparison on time field is safe)


class TestSingleBracketDifferentFieldSets:
    """Test that different field sets across branches force single-bracket."""

    def test_missing_field_in_one_branch(self, time_field, t1, t2):
        query = {
            "$or": [
                {
                    "region_id": ObjectId(),
                    "sensor": "A",
                    time_field: {"$gte": t1, "$lt": t2},
                },
                {
                    "region_id": ObjectId(),
                    time_field: {"$gte": t1, "$lt": t2},
                },  # Missing "sensor"
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)
        assert ok is True
        assert "single-bracket" in reason
        assert len(brackets) == 1

    def test_extra_field_in_one_branch(self, time_field, t1, t2):
        query = {
            "$or": [
                {"region_id": ObjectId(), time_field: {"$gte": t1, "$lt": t2}},
                {
                    "region_id": ObjectId(),
                    "extra": "value",
                    time_field: {"$gte": t1, "$lt": t2},
                },
            ]
        }
        ok, reason, _brackets, _bounds = build_brackets_for_find(query, time_field)
        assert ok is True
        assert "single-bracket" in reason


class TestSingleBracketInOverlapWithDifferentTime:
    """Test that overlapping $in with different time ranges forces single-bracket.

    CRITICAL: Cannot transform $in values when time ranges differ because
    removing overlapping values would lose data in the non-overlapping time.
    """

    def test_overlapping_in_different_time_unsafe(self, time_field):
        """Overlapping $in with different time = single-bracket (no transform)."""
        t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t1_local = datetime(2024, 1, 2, tzinfo=timezone.utc)
        t2_local = datetime(2024, 1, 29, tzinfo=timezone.utc)
        t3 = datetime(2024, 1, 30, tzinfo=timezone.utc)

        query = {
            "$or": [
                {
                    "field": {"$in": [1, 2, 3]},
                    time_field: {"$gte": t1_local, "$lt": t2_local},
                },  # Narrower
                {
                    "field": {"$in": [2, 3, 4]},
                    time_field: {"$gte": t0, "$lt": t3},
                },  # Wider, overlaps 2,3
            ]
        }
        ok, reason, _brackets, _bounds = build_brackets_for_find(query, time_field)
        assert ok is True
        assert "single-bracket" in reason

    def test_multiple_in_fields_overlap(self, time_field, t1, t2):
        """Multiple $in fields with overlap = too complex, single-bracket."""
        query = {
            "$or": [
                {
                    "a": {"$in": [1, 2]},
                    "b": {"$in": [10, 20]},
                    time_field: {"$gte": t1, "$lt": t2},
                },
                {
                    "a": {"$in": [2, 3]},
                    "b": {"$in": [20, 30]},
                    time_field: {"$gte": t1, "$lt": t2},
                },
            ]
        }
        ok, reason, _brackets, _bounds = build_brackets_for_find(query, time_field)
        assert ok is True
        assert "single-bracket" in reason


# =============================================================================
# SECTION 3: MERGE OPTIMIZATION TESTS
# =============================================================================


class TestMergeOptimizationSuccess:
    """Test cases where branches are successfully MERGED.

    Merge requirements (ALL must be true):
    1. All static filters identical (excluding time)
    2. All branches have FULL time ranges (both $gte and $lt)
    3. Time ranges are contiguous (no gaps)
    """

    def test_identical_filters_overlapping_time(self, time_field):
        """Overlapping time ranges with identical filters -> merge."""
        t_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t_mid = datetime(2024, 3, 1, tzinfo=timezone.utc)
        t_end = datetime(2024, 6, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {
                    "device_id": "X",
                    "config": {"$nin": ["a"]},
                    time_field: {"$gte": t_mid, "$lt": t_end},
                },
                {
                    "device_id": "X",
                    "config": {"$nin": ["a"]},
                    time_field: {"$gte": t_start, "$lt": t_end},
                },
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert "merged-branches" in reason
        assert len(brackets) == 1
        assert "$or" not in brackets[0].static_filter  # Clean, no $or
        assert brackets[0].timerange.lo == t_start
        assert brackets[0].timerange.hi == t_end

    def test_identical_filters_adjacent_time(self, time_field):
        """Adjacent time ranges (no gap) -> merge."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
        t3 = datetime(2024, 3, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {"s": "A", "c": {"$nin": ["x"]}, time_field: {"$gte": t1, "$lt": t2}},
                {"s": "A", "c": {"$nin": ["x"]}, time_field: {"$gte": t2, "$lt": t3}},
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert "merged-branches" in reason
        assert brackets[0].timerange.lo == t1
        assert brackets[0].timerange.hi == t3

    def test_identical_filters_contained_time(self, time_field):
        """One range contains another -> merge to outer range."""
        t_outer_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t_inner_start = datetime(2024, 2, 1, tzinfo=timezone.utc)
        t_inner_end = datetime(2024, 5, 1, tzinfo=timezone.utc)
        t_outer_end = datetime(2024, 6, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {
                    "s": "A",
                    "c": {"$nin": ["x"]},
                    time_field: {"$gte": t_inner_start, "$lt": t_inner_end},
                },
                {
                    "s": "A",
                    "c": {"$nin": ["x"]},
                    time_field: {"$gte": t_outer_start, "$lt": t_outer_end},
                },
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert "merged-branches" in reason
        assert brackets[0].timerange.lo == t_outer_start
        assert brackets[0].timerange.hi == t_outer_end

    def test_three_branches_filling_gap(self, time_field):
        """Three branches where middle one fills potential gap -> merge."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 3, 1, tzinfo=timezone.utc)
        t3 = datetime(2024, 4, 1, tzinfo=timezone.utc)
        t4 = datetime(2024, 6, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {"s": "A", "c": {"$nin": ["x"]}, time_field: {"$gte": t1, "$lt": t2}},
                {"s": "A", "c": {"$nin": ["x"]}, time_field: {"$gte": t3, "$lt": t4}},
                {
                    "s": "A",
                    "c": {"$nin": ["x"]},
                    time_field: {"$gte": t2, "$lt": t3},
                },  # Fills gap
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert "merged-branches" in reason
        assert brackets[0].timerange.lo == t1
        assert brackets[0].timerange.hi == t4


class TestMergeOptimizationBlocked:
    """Test cases where merge is BLOCKED and falls back to $or preservation."""

    def test_disjoint_time_gap_not_merged(self, time_field):
        """Gap between time ranges -> cannot merge (would include unwanted data)."""
        t1_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t1_end = datetime(2024, 1, 15, tzinfo=timezone.utc)
        t2_start = datetime(2024, 2, 1, tzinfo=timezone.utc)  # 17-day gap!
        t2_end = datetime(2024, 2, 15, tzinfo=timezone.utc)

        query = {
            "$or": [
                {
                    "s": "A",
                    "c": {"$nin": ["x"]},
                    time_field: {"$gte": t1_start, "$lt": t1_end},
                },
                {
                    "s": "A",
                    "c": {"$nin": ["x"]},
                    time_field: {"$gte": t2_start, "$lt": t2_end},
                },
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert "single-bracket" in reason  # NOT merged
        assert "$or" in brackets[0].static_filter  # $or preserved

    def test_different_static_filters_not_merged(self, time_field, t1, t2):
        """Different static filters -> cannot merge."""
        query = {
            "$or": [
                {
                    "sensor": "A",
                    "c": {"$nin": ["x"]},
                    time_field: {"$gte": t1, "$lt": t2},
                },
                {
                    "sensor": "B",
                    "c": {"$nin": ["x"]},
                    time_field: {"$gte": t1, "$lt": t2},
                },
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert "single-bracket" in reason
        assert "$or" in brackets[0].static_filter

    def test_partial_time_range_not_merged(self, time_field, t1, t2):
        """Partial time range (only $gte) -> SINGLE mode.

        Valid but not parallelizable.
        """
        query = {
            "$or": [
                {
                    "s": "A",
                    "c": {"$nin": ["x"]},
                    time_field: {"$gte": t1},
                },  # No upper bound
                {"s": "A", "c": {"$nin": ["x"]}, time_field: {"$gte": t1, "$lt": t2}},
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        # This query should return SINGLE mode (valid but uses single worker)
        assert ok is True
        assert brackets == []  # Empty brackets = single-worker mode
        assert "unbounded" in reason.lower() or "partial" in reason.lower()

    def test_unbounded_branch_not_merged(self, time_field, t1, t2):
        """Branch with no time constraint -> SINGLE mode.

        Valid but not parallelizable.
        """
        query = {
            "$or": [
                {"s": "A", "c": {"$nin": ["x"]}},  # No time at all
                {"s": "A", "c": {"$nin": ["x"]}, time_field: {"$gte": t1, "$lt": t2}},
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        # This query should return SINGLE mode (valid but uses single worker)
        assert ok is True
        assert brackets == []  # Empty brackets = single-worker mode
        assert "unbounded" in reason.lower() or "partial" in reason.lower()


# =============================================================================
# SECTION 4: MULTIPLE BRACKET TESTS (Safe patterns)
# =============================================================================


class TestMultipleBracketsCreation:
    """Test cases where branches safely become independent brackets."""

    def test_disjoint_equality_values(self, time_field, t1, t2):
        """Different equality values = disjoint = multiple brackets."""
        oid1, oid2, oid3 = ObjectId(), ObjectId(), ObjectId()
        query = {
            "$or": [
                {"region_id": oid1, time_field: {"$gte": t1, "$lt": t2}},
                {"region_id": oid2, time_field: {"$gte": t1, "$lt": t2}},
                {"region_id": oid3, time_field: {"$gte": t1, "$lt": t2}},
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert len(brackets) == 3

    def test_disjoint_in_values(self, time_field, t1, t2):
        """Completely disjoint $in values = multiple brackets."""
        query = {
            "$or": [
                {"region_id": {"$in": [1, 2, 3]}, time_field: {"$gte": t1, "$lt": t2}},
                {"region_id": {"$in": [4, 5, 6]}, time_field: {"$gte": t1, "$lt": t2}},
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert len(brackets) == 2

    def test_no_or_single_bracket(self, time_field, t1, t2):
        """Query without $or = single bracket."""
        query = {"region_id": ObjectId(), time_field: {"$gte": t1, "$lt": t2}}
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert len(brackets) == 1
        assert brackets[0].timerange.is_full is True


# =============================================================================
# SECTION 5: $in VALUE TRANSFORMATION TESTS
# =============================================================================


class TestInValueTransformation:
    """Test the $in overlap detection and transformation algorithm."""

    def test_overlapping_in_same_time_transformed(self, time_field, t1, t2):
        """Overlapping $in with same time = transform to remove overlap."""
        query = {
            "$or": [
                {
                    "region_id": {"$in": [1, 2, 3, 4]},
                    time_field: {"$gte": t1, "$lt": t2},
                },
                {
                    "region_id": {"$in": [3, 4, 5, 6]},
                    time_field: {"$gte": t1, "$lt": t2},
                },
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert len(brackets) == 2

        # Verify no duplicate values
        all_values = set()
        for b in brackets:
            in_vals = set(b.static_filter.get("region_id", {}).get("$in", []))
            assert all_values.isdisjoint(in_vals), "Found duplicate values"
            all_values.update(in_vals)

        assert all_values == {1, 2, 3, 4, 5, 6}

    def test_fully_covered_branch_removed(self, time_field, t1, t2):
        """Branch fully covered by prior branches is removed."""
        query = {
            "$or": [
                {"region_id": {"$in": [1, 2, 3]}, time_field: {"$gte": t1, "$lt": t2}},
                {
                    "region_id": {"$in": [1, 2]},
                    time_field: {"$gte": t1, "$lt": t2},
                },  # Covered!
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert len(brackets) == 1
        assert set(brackets[0].static_filter["region_id"]["$in"]) == {1, 2, 3}


# =============================================================================
# SECTION 6: TIME RANGE HANDLING TESTS
# =============================================================================


class TestTimeRangeHandling:
    """Test edge cases around time range extraction and handling."""

    def test_full_bounds(self, time_field, t1, t2):
        """Both $gte and $lt = full bracket."""
        query = {"region_id": ObjectId(), time_field: {"$gte": t1, "$lt": t2}}
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert brackets[0].timerange.lo == t1
        assert brackets[0].timerange.hi == t2
        assert brackets[0].timerange.is_full is True

    def test_partial_gte_only(self, time_field, t1):
        """Only $gte = SINGLE mode (valid but not parallelizable)."""
        query = {"region_id": ObjectId(), time_field: {"$gte": t1}}
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        # Changed: Now returns SINGLE mode (valid but single-worker)
        assert ok is True
        assert brackets == []  # Empty brackets = single-worker mode
        assert "no complete time range" in reason

    def test_partial_lt_only(self, time_field, t2):
        """Only $lt = SINGLE mode (valid but not parallelizable)."""
        query = {"region_id": ObjectId(), time_field: {"$lt": t2}}
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        # Changed: Now returns SINGLE mode (valid but single-worker)
        assert ok is True
        assert brackets == []  # Empty brackets = single-worker mode
        assert "no complete time range" in reason

    def test_unbounded(self, time_field):
        """No time constraint = SINGLE mode (valid but not parallelizable)."""
        query = {"region_id": ObjectId()}
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        # Changed: Now returns SINGLE mode (valid but single-worker)
        assert ok is True
        assert brackets == []  # Empty brackets = single-worker mode
        assert (
            "no time field reference found" in reason
            or "no complete time range" in reason
        )


# =============================================================================
# SECTION 7: REAL-WORLD SCENARIO TESTS
# =============================================================================


class TestRealWorldScenarios:
    """Test complex queries mimicking actual production usage."""

    def test_device_data_with_nin_merge(self, time_field):
        """The original bug case: device data with $nin and overlapping times.

        Query pattern:
            $or: [
                {device_id: X, sensor_id: {$nin: [...]},
                 recordedAt: {start+1day, now-1day}},
                {device_id: X, sensor_id: {$nin: [...]}, recordedAt: {start, now}},
            ]

        Expected: MERGE into single bracket because:
        - Same static filters (identical device_id and $nin)
        - Overlapping time ranges (second contains first)
        """
        device_id = ObjectId()
        excluded_configs = [ObjectId() for _ in range(10)]

        t_start = datetime(2024, 10, 21, tzinfo=timezone.utc)
        t_start_plus_1 = datetime(2024, 10, 22, tzinfo=timezone.utc)
        t_end_minus_1 = datetime(2024, 12, 19, tzinfo=timezone.utc)
        t_end = datetime(2024, 12, 20, tzinfo=timezone.utc)

        query = {
            "$or": [
                {
                    "metadata.device_id": device_id,
                    "metadata.sensor_id": {"$nin": excluded_configs},
                    time_field: {"$gte": t_start_plus_1, "$lt": t_end_minus_1},
                },
                {
                    "metadata.device_id": device_id,
                    "metadata.sensor_id": {"$nin": excluded_configs},
                    time_field: {"$gte": t_start, "$lt": t_end},
                },
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert "merged-branches" in reason
        assert len(brackets) == 1
        assert "$or" not in brackets[0].static_filter
        assert brackets[0].timerange.lo == t_start
        assert brackets[0].timerange.hi == t_end

    def test_multi_region_parallel(self, time_field, t1, t2):
        """Multiple regions = multiple parallel brackets."""
        regions = [ObjectId() for _ in range(5)]

        query = {
            "$or": [
                {
                    "region_id": rid,
                    "sensor_type": "temp",
                    time_field: {"$gte": t1, "$lt": t2},
                }
                for rid in regions
            ]
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert len(brackets) == 5

    def test_global_and_with_or(self, time_field, t1, t2):
        """Global filter ANDed with $or branches."""
        account_id = ObjectId()

        query = {
            "account_id": account_id,
            "$or": [
                {"sensor": "A", time_field: {"$gte": t1, "$lt": t2}},
                {
                    "sensor": "B",
                    time_field: {"$gte": t1, "$lt": t2},
                },  # Fixed: added upper bound
            ],
        }
        ok, reason, brackets, bounds = build_brackets_for_find(query, time_field)

        assert ok is True
        assert len(brackets) == 2
        for b in brackets:
            assert b.static_filter.get("account_id") == account_id


# =============================================================================
# SECTION 8: HELPER FUNCTION UNIT TESTS
# =============================================================================


class TestHasNegationOperators:
    """Unit tests for _has_negation_operators."""

    @pytest.mark.parametrize(
        "query,expected",
        [
            ({"field": {"$in": [1, 2, 3]}}, False),
            ({"status": "active"}, False),
            ({"field": {"$nin": [1, 2, 3]}}, True),
            ({"status": {"$ne": "deleted"}}, True),
            ({"field": {"$not": {"$eq": 5}}}, True),
            ({"$nor": [{"a": 1}, {"b": 2}]}, True),
            ({"$and": [{"a": 1}, {"b": {"$nin": [2, 3]}}]}, True),
        ],
    )
    def test_detection(self, query, expected):
        assert _has_negation_operators(query) is expected


class TestHasOverlapProneOperators:
    """Unit tests for _has_overlap_prone_operators."""

    @pytest.mark.parametrize(
        "query,expected_found,expected_op",
        [
            ({"region_id": ObjectId()}, False, None),
            ({"tags": {"$all": ["a", "b"]}}, True, "$all"),
            ({"items": {"$elemMatch": {"qty": 5}}}, True, "$elemMatch"),
            ({"name": {"$regex": "^John"}}, True, "$regex"),
            ({"qty": {"$mod": [4, 0]}}, True, "$mod"),
            ({"value": {"$gt": 100}}, True, "$gt"),
            (
                {"ts": {"$gte": "2024-01-01", "$lt": "2024-02-01"}},
                False,
                None,
            ),  # Time field OK
        ],
    )
    def test_detection(self, query, expected_found, expected_op):
        found, op = _has_overlap_prone_operators(query, "ts")
        assert found is expected_found
        assert op == expected_op


class TestExtractInValues:
    """Unit tests for _extract_in_values."""

    def test_in_present(self):
        assert _extract_in_values({"f": {"$in": [1, 2, 3]}}, "f") == {1, 2, 3}

    def test_field_not_present(self):
        assert _extract_in_values({"other": {"$in": [1]}}, "f") is None

    def test_equality_not_in(self):
        assert _extract_in_values({"f": 5}, "f") is None

    def test_empty_in(self):
        assert _extract_in_values({"f": {"$in": []}}, "f") == set()


class TestFindInFields:
    """Unit tests for _find_in_fields."""

    def test_single_in(self):
        assert _find_in_fields({"a": {"$in": [1, 2]}, "b": 5}) == {"a": {1, 2}}

    def test_multiple_in(self):
        assert _find_in_fields({"a": {"$in": [1]}, "b": {"$in": [2]}}) == {
            "a": {1},
            "b": {2},
        }

    def test_no_in(self):
        assert _find_in_fields({"a": 5, "b": {"$gt": 10}}) == {}


class TestCheckOrBranchSafety:
    """Unit tests for _check_or_branch_safety."""

    def test_single_branch_safe(self):
        branches = [{"region_id": ObjectId()}]
        is_safe, reason, transformed = _check_or_branch_safety(branches, {}, "ts")
        assert is_safe is True

    def test_disjoint_equality_safe(self):
        oid1, oid2 = ObjectId(), ObjectId()
        branches = [{"region_id": oid1}, {"region_id": oid2}]
        is_safe, reason, transformed = _check_or_branch_safety(branches, {}, "ts")
        assert is_safe is True

    def test_negation_unsafe(self):
        branches = [{"f": {"$in": [1]}}, {"f": {"$nin": [2]}}]
        is_safe, reason, transformed = _check_or_branch_safety(branches, {}, "ts")
        assert is_safe is False
        assert "negation" in reason.lower()

    def test_different_fields_unsafe(self):
        branches = [{"a": 1, "b": 2}, {"a": 3}]  # Missing "b"
        is_safe, reason, transformed = _check_or_branch_safety(branches, {}, "ts")
        assert is_safe is False
        assert "different field set" in reason.lower()


# =============================================================================
# Test Class: Three-Tier Graceful Degradation in build_brackets_for_find
# =============================================================================


class TestBuildBracketsThreeTier:
    """
    Test build_brackets_for_find with three-tier graceful degradation.

    The function should now handle:
    - PARALLEL mode: Return is_chunkable=True with bracket list
    - SINGLE mode: Return is_chunkable=True with empty brackets
    - REJECT mode: Return is_chunkable=False with empty brackets
    """

    # -------------------------------------------------------------------------
    # Test PARALLEL Mode (Successful Bracket Building)
    # -------------------------------------------------------------------------

    def test_parallel_mode_basic_query(self):
        """Basic query should return PARALLEL mode with brackets."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"timestamp": {"$gte": t1, "$lt": t2}, "status": "active"}

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True
        assert reason == ""
        assert len(brackets) == 1
        assert brackets[0].timerange.lo == t1
        assert brackets[0].timerange.hi == t2
        assert bounds == (t1, t2)

    def test_parallel_mode_or_query(self):
        """$or query with bounded branches should return PARALLEL.

        Should produce multiple brackets.
        """
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {"region_id": "A"},
                {"region_id": "B"},
            ],
            "timestamp": {"$gte": t1, "$lt": t2},
        }

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True
        assert len(brackets) == 2  # One bracket per $or branch
        assert bounds == (t1, t2)

    # -------------------------------------------------------------------------
    # Test SINGLE Mode (Valid Query, Empty Brackets)
    # -------------------------------------------------------------------------

    def test_single_mode_natural_sort(self):
        """$natural sort should return SINGLE mode with empty brackets."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"timestamp": {"$gte": t1, "$lt": t2}}
        sort_spec = [("$natural", 1)]

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp", sort_spec
        )

        assert is_chunkable is True  # Valid query
        assert "$natural sort" in reason
        assert brackets == []  # Empty brackets = single-worker signal
        assert bounds == (t1, t2)

    def test_single_mode_no_time_reference(self):
        """Query with no time reference should return SINGLE mode."""
        query = {"status": "active"}

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True  # Valid query
        assert "no time field reference" in reason
        assert brackets == []  # Empty brackets
        assert bounds == (None, None)

    def test_single_mode_unbounded_or_branch(self):
        """$or with unbounded branch should return SINGLE mode."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {"region_id": "A", "timestamp": {"$gte": t1, "$lt": t2}},
                {"region_id": "B"},  # No time constraint
            ]
        }

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True  # Valid query
        assert "$or query has unbounded" in reason
        assert brackets == []  # Empty brackets

    # -------------------------------------------------------------------------
    # Test REJECT Mode (Invalid Query, Cannot Execute)
    # -------------------------------------------------------------------------

    def test_reject_mode_expr_operator(self):
        """$expr should return SINGLE mode (can execute, just not parallelize)."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"timestamp": {"$gte": t1, "$lt": t2}, "$expr": {"$gt": ["$a", "$b"]}}

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True  # Executable (SINGLE mode)
        assert "$expr" in reason
        assert brackets == []  # Empty brackets = single-worker
        assert bounds == (t1, t2)  # Should extract time bounds

    def test_reject_mode_text_search(self):
        """$text should return SINGLE mode (can execute, just not parallelize)."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"timestamp": {"$gte": t1, "$lt": t2}, "$text": {"$search": "mongodb"}}

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True  # Executable (SINGLE mode)
        assert "$text" in reason
        assert brackets == []  # Empty brackets = single-worker
        assert bounds == (t1, t2)  # Should extract time bounds

    def test_reject_mode_nested_or(self):
        """Nested $or should return SINGLE mode (complex but executable)."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            "timestamp": {"$gte": t1, "$lt": t2},
            "$or": [
                {"$or": [{"a": 1}, {"b": 2}]},  # Nested $or
                {"c": 3},
            ],
        }

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True  # Executable (SINGLE mode)
        assert "nested $or" in reason
        assert brackets == []  # Empty brackets = single-worker
        assert bounds == (t1, t2)

    # -------------------------------------------------------------------------
    # Test Sort Spec Parameter Handling
    # -------------------------------------------------------------------------

    def test_sort_spec_none_parameter(self):
        """sort_spec=None should work (default behavior)."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"timestamp": {"$gte": t1, "$lt": t2}}

        # Calling without sort_spec (default None)
        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp", None
        )

        assert is_chunkable is True
        assert len(brackets) == 1

    def test_sort_spec_regular_field(self):
        """Regular field sort should not affect chunkability."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"timestamp": {"$gte": t1, "$lt": t2}}
        sort_spec = [("value", 1)]

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp", sort_spec
        )

        assert is_chunkable is True  # Regular sort is OK
        assert len(brackets) == 1

    def test_sort_spec_natural_descending(self):
        """$natural sort descending should also return SINGLE."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"timestamp": {"$gte": t1, "$lt": t2}}
        sort_spec = [("$natural", -1)]

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp", sort_spec
        )

        assert is_chunkable is True
        assert "$natural sort" in reason
        assert brackets == []

    # -------------------------------------------------------------------------
    # Edge Cases and Interaction Tests
    # -------------------------------------------------------------------------

    def test_reject_trumps_single_natural_sort(self):
        """Test combining SINGLE-tier conditions (use empty $or for true REJECT)."""
        # Empty $or is true REJECT, $natural is SINGLE
        query = {"$or": []}
        sort_spec = [("$natural", 1)]

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp", sort_spec
        )

        # Should REJECT due to empty $or
        assert is_chunkable is False
        assert "empty" in reason.lower()

    def test_single_mode_partial_bounds(self):
        """Query with only $gte or $lt should return SINGLE mode."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # Only lower bound
        query = {"timestamp": {"$gte": t1}}
        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True  # Valid but SINGLE
        assert "no complete time range" in reason
        assert brackets == []

    def test_parallel_mode_backwards_compatible(self):
        """PARALLEL mode should work with old code.

        Compatibility: (bool, str, list, tuple).
        """
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"timestamp": {"$gte": t1, "$lt": t2}}
        result = build_brackets_for_find(query, "timestamp")

        # Old code unpacks 4-tuple
        is_chunkable, reason, brackets, bounds = result

        assert is_chunkable is True
        assert isinstance(reason, str)
        assert isinstance(brackets, list)
        assert isinstance(bounds, tuple)


# =============================================================================
# BOUNDARY OPERATOR TESTS ($gt/$gte and $lt/$lte)
# =============================================================================
# These tests verify that XLR8 correctly preserves the original query's boundary
# operators ($gt vs $gte, $lt vs $lte) through the bracket creation process.
# This is critical for data correctness - using the wrong operator can cause
# documents at exact boundary timestamps to be missed.
# =============================================================================


class TestBoundaryOperatorExtraction:
    """Test that extract_time_bounds_recursive correctly tracks boundary operators."""

    def test_gte_lt_default_operators(self):
        """$gte (inclusive) and $lt (exclusive) - the most common case."""
        from xlr8.analysis.inspector import extract_time_bounds_recursive

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"recordedAt": {"$gte": t1, "$lt": t2}}
        bounds, has_ref = extract_time_bounds_recursive(query, "recordedAt")

        assert bounds is not None
        lo, hi, hi_inclusive, lo_inclusive = bounds
        assert lo == t1
        assert hi == t2
        assert lo_inclusive is True  # $gte is inclusive
        assert hi_inclusive is False  # $lt is exclusive

    def test_gte_lte_both_inclusive(self):
        """$gte and $lte - both boundaries included."""
        from xlr8.analysis.inspector import extract_time_bounds_recursive

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"recordedAt": {"$gte": t1, "$lte": t2}}
        bounds, has_ref = extract_time_bounds_recursive(query, "recordedAt")

        assert bounds is not None
        lo, hi, hi_inclusive, lo_inclusive = bounds
        assert lo == t1
        assert hi == t2
        assert lo_inclusive is True  # $gte is inclusive
        assert hi_inclusive is True  # $lte is inclusive

    def test_gt_lt_both_exclusive(self):
        """$gt and $lt - both boundaries excluded."""
        from xlr8.analysis.inspector import extract_time_bounds_recursive

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"recordedAt": {"$gt": t1, "$lt": t2}}
        bounds, has_ref = extract_time_bounds_recursive(query, "recordedAt")

        assert bounds is not None
        lo, hi, hi_inclusive, lo_inclusive = bounds
        assert lo == t1
        assert hi == t2
        assert lo_inclusive is False  # $gt is exclusive
        assert hi_inclusive is False  # $lt is exclusive

    def test_gt_lte_mixed_operators(self):
        """$gt (exclusive) and $lte (inclusive) - mixed boundaries."""
        from xlr8.analysis.inspector import extract_time_bounds_recursive

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {"recordedAt": {"$gt": t1, "$lte": t2}}
        bounds, has_ref = extract_time_bounds_recursive(query, "recordedAt")

        assert bounds is not None
        lo, hi, hi_inclusive, lo_inclusive = bounds
        assert lo == t1
        assert hi == t2
        assert lo_inclusive is False  # $gt is exclusive
        assert hi_inclusive is True  # $lte is inclusive

    def test_equality_is_both_inclusive(self):
        """Direct equality should be inclusive on both ends."""
        from xlr8.analysis.inspector import extract_time_bounds_recursive

        t1 = datetime(2024, 1, 15, tzinfo=timezone.utc)

        query = {"recordedAt": t1}
        bounds, has_ref = extract_time_bounds_recursive(query, "recordedAt")

        assert bounds is not None
        lo, hi, hi_inclusive, lo_inclusive = bounds
        assert lo == hi == t1
        assert lo_inclusive is True
        assert hi_inclusive is True


class TestBracketBoundaryPreservation:
    """Test that brackets preserve the original query's boundary operators."""

    def test_bracket_preserves_lte(self):
        """Bracket should preserve $lte from original query."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 31, tzinfo=timezone.utc)

        query = {"sensor_id": "sensor_1", "timestamp": {"$gte": t1, "$lte": t2}}

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True
        assert len(brackets) == 1

        bracket = brackets[0]
        assert bracket.timerange.hi_inclusive is True  # Preserves $lte
        assert bracket.timerange.lo_inclusive is True  # Default $gte

    def test_bracket_preserves_lt(self):
        """Bracket should preserve $lt from original query."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 31, tzinfo=timezone.utc)

        query = {"sensor_id": "sensor_1", "timestamp": {"$gte": t1, "$lt": t2}}

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True
        assert len(brackets) == 1

        bracket = brackets[0]
        assert bracket.timerange.hi_inclusive is False  # Preserves $lt
        assert bracket.timerange.lo_inclusive is True  # Default $gte

    def test_bracket_preserves_gt(self):
        """Bracket should preserve $gt from original query."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 31, tzinfo=timezone.utc)

        query = {"sensor_id": "sensor_1", "timestamp": {"$gt": t1, "$lt": t2}}

        is_chunkable, reason, brackets, bounds = build_brackets_for_find(
            query, "timestamp"
        )

        assert is_chunkable is True
        assert len(brackets) == 1

        bracket = brackets[0]
        assert bracket.timerange.lo_inclusive is False  # Preserves $gt
        assert bracket.timerange.hi_inclusive is False  # Default $lt


class TestBoundaryIntersection:
    """Test that bound intersections handle inclusivity correctly."""

    def test_intersection_uses_most_restrictive(self):
        """$and with overlapping bounds should use most restrictive operators."""
        from xlr8.analysis.inspector import extract_time_bounds_recursive

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 31, tzinfo=timezone.utc)
        t_mid_lo = datetime(2024, 1, 10, tzinfo=timezone.utc)
        t_mid_hi = datetime(2024, 1, 20, tzinfo=timezone.utc)

        # Outer range uses inclusive bounds, inner uses exclusive
        query = {
            "$and": [
                {"recordedAt": {"$gte": t1, "$lte": t2}},  # Inclusive
                {"recordedAt": {"$gt": t_mid_lo, "$lt": t_mid_hi}},  # Exclusive
            ]
        }

        bounds, has_ref = extract_time_bounds_recursive(query, "recordedAt")

        assert bounds is not None
        lo, hi, hi_inclusive, lo_inclusive = bounds
        # Should take the tighter inner bounds
        assert lo == t_mid_lo
        assert hi == t_mid_hi
        # Should use most restrictive (exclusive) operators
        assert lo_inclusive is False  # More restrictive ($gt)
        assert hi_inclusive is False  # More restrictive ($lt)


class TestBoundaryUnion:
    """Test that bound unions handle inclusivity correctly."""

    def test_or_union_preserves_inclusive_if_any(self):
        """$or with different end operators.

        Use inclusive if any branch uses it.
        """
        from xlr8.analysis.inspector import extract_time_bounds_recursive

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 31, tzinfo=timezone.utc)

        # Branch A uses $lt (exclusive), Branch B uses $lte (inclusive)
        query = {
            "$or": [
                {"sensor": "A", "recordedAt": {"$gte": t1, "$lt": t2}},
                {"sensor": "B", "recordedAt": {"$gte": t1, "$lte": t2}},
            ]
        }

        bounds, has_ref = extract_time_bounds_recursive(query, "recordedAt")

        assert bounds is not None
        lo, hi, hi_inclusive, lo_inclusive = bounds
        assert lo == t1
        assert hi == t2
        # If ANY branch uses $lte at the max hi, result should be inclusive
        assert hi_inclusive is True


class TestTimeRangeBoundaryDefaults:
    """Test that TimeRange has correct default values for boundary operators."""

    def test_timerange_default_values(self):
        """TimeRange should default to $gte (inclusive lo) and $lt (exclusive hi)."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        from xlr8.analysis.brackets import TimeRange

        # Create with minimal args
        tr = TimeRange(lo=t1, hi=t2, is_full=True)

        # Check defaults match common MongoDB pattern
        assert tr.lo_inclusive is True  # $gte
        assert tr.hi_inclusive is False  # $lt

    def test_timerange_explicit_values(self):
        """TimeRange should accept explicit boundary operator settings."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        from xlr8.analysis.brackets import TimeRange

        # Create with explicit inclusive bounds
        tr = TimeRange(
            lo=t1, hi=t2, is_full=True, hi_inclusive=True, lo_inclusive=False
        )

        assert tr.lo_inclusive is False  # $gt
        assert tr.hi_inclusive is True  # $lte
