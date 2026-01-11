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

  Performance: Best (2-5x speedup)

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
   Vessel data with $nin, multi-region queries

Each test validates against actual brackets.py implementation.
"""
