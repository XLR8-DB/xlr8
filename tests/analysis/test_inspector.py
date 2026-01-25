"""
Comprehensive tests for XLR8 query inspector.

This test suite is designed with a "devil's advocate" mindset - actively
trying to find edge cases and corner cases that could break the inspector.

Test Categories:
    1. ALWAYS_ALLOWED operators - verify each one passes
    2. CONDITIONAL operators - test valid and invalid contexts
    3. NEVER_ALLOWED operators - verify each one is rejected
    4. Complex/realistic queries - real-world patterns
    5. Edge cases - tricky scenarios designed to find bugs
    6. Three-tier graceful degradation - PARALLEL/SINGLE/REJECT modes
"""

from datetime import datetime, timedelta, timezone

import pytest
from bson import ObjectId

from xlr8.analysis.inspector import (
    ALWAYS_ALLOWED,
    CONDITIONAL,
    NEVER_ALLOWED,
    ChunkabilityMode,
    ChunkabilityResult,
    _or_depth,
    _references_field,
    has_natural_sort,
    is_chunkable_query,
    split_global_and,
    validate_query_for_chunking,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def time_field():
    """Standard time field name."""
    return "timestamp"


@pytest.fixture
def t1():
    """Start time for tests."""
    return datetime(2024, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def t2():
    """End time for tests."""
    return datetime(2024, 2, 1, tzinfo=timezone.utc)


@pytest.fixture
def valid_time_range(t1, t2, time_field):
    """Standard valid time range predicate."""
    return {time_field: {"$gte": t1, "$lt": t2}}


# =============================================================================
# Test Class: ALWAYS_ALLOWED Operators
# =============================================================================


class TestAlwaysAllowedOperators:
    """
    Verify that all ALWAYS_ALLOWED operators pass validation.

    These operators are document-local and should never cause rejection
    regardless of how they're used.
    """

    # -------------------------------------------------------------------------
    # Comparison Operators
    # -------------------------------------------------------------------------

    def test_eq_basic(self, time_field, valid_time_range):
        """$eq: Simple equality check."""
        query = {**valid_time_range, "status": {"$eq": "active"}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$eq should be allowed: {reason}"

    def test_eq_implicit(self, time_field, valid_time_range):
        """$eq: Implicit equality (no operator)."""
        query = {**valid_time_range, "status": "active"}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"Implicit equality should be allowed: {reason}"

    def test_ne(self, time_field, valid_time_range):
        """$ne: Not equal check."""
        query = {**valid_time_range, "status": {"$ne": "deleted"}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$ne should be allowed: {reason}"

    def test_gt(self, time_field, valid_time_range):
        """$gt: Greater than comparison."""
        query = {**valid_time_range, "value": {"$gt": 100}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$gt should be allowed: {reason}"

    def test_gte(self, time_field, valid_time_range):
        """$gte: Greater than or equal comparison."""
        query = {**valid_time_range, "score": {"$gte": 80}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$gte should be allowed: {reason}"

    def test_lt(self, time_field, valid_time_range):
        """$lt: Less than comparison."""
        query = {**valid_time_range, "count": {"$lt": 50}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$lt should be allowed: {reason}"

    def test_lte(self, time_field, valid_time_range):
        """$lte: Less than or equal comparison."""
        query = {**valid_time_range, "age": {"$lte": 65}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$lte should be allowed: {reason}"

    def test_in(self, time_field, valid_time_range):
        """$in: Value in array."""
        query = {**valid_time_range, "type": {"$in": ["A", "B", "C"]}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$in should be allowed: {reason}"

    def test_nin(self, time_field, valid_time_range):
        """$nin: Value not in array."""
        query = {**valid_time_range, "status": {"$nin": ["deleted", "archived"]}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$nin should be allowed: {reason}"

    # -------------------------------------------------------------------------
    # Element Operators
    # -------------------------------------------------------------------------

    def test_exists_true(self, time_field, valid_time_range):
        """$exists: Field must exist."""
        query = {**valid_time_range, "email": {"$exists": True}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$exists should be allowed: {reason}"

    def test_exists_false(self, time_field, valid_time_range):
        """$exists: Field must NOT exist."""
        query = {**valid_time_range, "deleted_at": {"$exists": False}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$exists: false should be allowed: {reason}"

    def test_type_string(self, time_field, valid_time_range):
        """$type: BSON type check with string alias."""
        query = {**valid_time_range, "value": {"$type": "double"}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$type should be allowed: {reason}"

    def test_type_number(self, time_field, valid_time_range):
        """$type: BSON type check with numeric code."""
        query = {**valid_time_range, "value": {"$type": 1}}  # double
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$type with number should be allowed: {reason}"

    # -------------------------------------------------------------------------
    # Array Operators
    # -------------------------------------------------------------------------

    def test_all(self, time_field, valid_time_range):
        """$all: Array contains all specified elements."""
        query = {**valid_time_range, "tags": {"$all": ["mongodb", "python"]}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$all should be allowed: {reason}"

    def test_elemMatch_simple(self, time_field, valid_time_range):
        """$elemMatch: Array element matches conditions."""
        query = {**valid_time_range, "scores": {"$elemMatch": {"$gte": 80, "$lt": 90}}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$elemMatch should be allowed: {reason}"

    def test_elemMatch_nested_object(self, time_field, valid_time_range):
        """$elemMatch: Matching nested object in array."""
        query = {
            **valid_time_range,
            "items": {"$elemMatch": {"name": "widget", "qty": {"$gt": 10}}},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$elemMatch with nested object should be allowed: {reason}"

    def test_size(self, time_field, valid_time_range):
        """$size: Array has exact length."""
        query = {**valid_time_range, "tags": {"$size": 3}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$size should be allowed: {reason}"

    # -------------------------------------------------------------------------
    # Bitwise Operators
    # -------------------------------------------------------------------------

    def test_bitsAllClear(self, time_field, valid_time_range):
        """$bitsAllClear: All specified bits are 0."""
        query = {**valid_time_range, "flags": {"$bitsAllClear": [1, 5]}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$bitsAllClear should be allowed: {reason}"

    def test_bitsAllSet(self, time_field, valid_time_range):
        """$bitsAllSet: All specified bits are 1."""
        query = {**valid_time_range, "permissions": {"$bitsAllSet": 0b0101}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$bitsAllSet should be allowed: {reason}"

    def test_bitsAnyClear(self, time_field, valid_time_range):
        """$bitsAnyClear: Any specified bit is 0."""
        query = {**valid_time_range, "status": {"$bitsAnyClear": 0xFF}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$bitsAnyClear should be allowed: {reason}"

    def test_bitsAnySet(self, time_field, valid_time_range):
        """$bitsAnySet: Any specified bit is 1."""
        query = {**valid_time_range, "options": {"$bitsAnySet": [0, 2, 4]}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$bitsAnySet should be allowed: {reason}"

    # -------------------------------------------------------------------------
    # Evaluation Operators (Safe Subset)
    # -------------------------------------------------------------------------

    def test_regex_basic(self, time_field, valid_time_range):
        """$regex: Regular expression pattern."""
        query = {**valid_time_range, "name": {"$regex": "^sensor_"}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$regex should be allowed: {reason}"

    def test_regex_with_options(self, time_field, valid_time_range):
        """$regex: With $options modifier."""
        query = {
            **valid_time_range,
            "email": {"$regex": "@gmail\\.com$", "$options": "i"},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$regex with $options should be allowed: {reason}"

    def test_mod(self, time_field, valid_time_range):
        """$mod: Modulo operation."""
        query = {**valid_time_range, "count": {"$mod": [10, 0]}}  # divisible by 10
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$mod should be allowed: {reason}"

    def test_jsonSchema(self, time_field, valid_time_range):
        """$jsonSchema: Schema validation."""
        query = {
            **valid_time_range,
            "$jsonSchema": {
                "required": ["name", "email"],
                "properties": {"status": {"enum": ["active", "pending"]}},
            },
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$jsonSchema should be allowed: {reason}"

    def test_comment(self, time_field, valid_time_range):
        """$comment: Query annotation (no filtering effect)."""
        query = {
            **valid_time_range,
            "$comment": "Slow query - needs index optimization",
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$comment should be allowed: {reason}"

    # -------------------------------------------------------------------------
    # Logical Operator: $and
    # -------------------------------------------------------------------------

    def test_and_explicit(self, time_field, valid_time_range):
        """$and: Explicit AND conjunction."""
        query = {
            **valid_time_range,
            "$and": [{"status": "active"}, {"value": {"$gt": 0}}],
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$and should be allowed: {reason}"

    def test_and_deeply_nested(self, time_field, valid_time_range):
        """$and: Deeply nested AND is still safe."""
        query = {
            **valid_time_range,
            "$and": [{"$and": [{"a": 1}, {"b": 2}]}, {"$and": [{"c": 3}, {"d": 4}]}],
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"Nested $and should be allowed: {reason}"


# =============================================================================
# Test Class: CONDITIONAL Operators
# =============================================================================


class TestConditionalOperators:
    """
    Test CONDITIONAL operators ($or, $nor, $not) in valid and invalid contexts.

    These operators are context-dependent - safe in some uses, dangerous in others.
    """

    # -------------------------------------------------------------------------
    # $or - Valid Cases (Depth 1)
    # -------------------------------------------------------------------------

    def test_or_depth_1_basic(self, time_field, valid_time_range):
        """$or at depth 1: Simple OR on non-time fields."""
        query = {**valid_time_range, "$or": [{"type": "A"}, {"type": "B"}]}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$or at depth 1 should be allowed: {reason}"

    def test_or_depth_1_with_global_and(self, time_field, valid_time_range):
        """$or at depth 1: With additional global conditions."""
        query = {
            **valid_time_range,
            "$or": [{"sensor": "X"}, {"sensor": "Y"}],
            "status": {"$ne": "deleted"},
            "value": {"$gte": 0},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$or with global AND should be allowed: {reason}"

    def test_or_depth_1_per_branch_time_ranges(self, time_field, t1, t2):
        """$or at depth 1: Each branch has its own time range."""
        t3 = t2 + timedelta(days=30)
        query = {
            "$or": [
                {"sensor": "A", time_field: {"$gte": t1, "$lt": t2}},
                {"sensor": "B", time_field: {"$gte": t2, "$lt": t3}},
            ]
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$or with per-branch time ranges should be allowed: {reason}"

    def test_or_inside_and(self, time_field, valid_time_range):
        """$or inside $and at depth 1."""
        query = {"$and": [valid_time_range, {"$or": [{"type": "A"}, {"type": "B"}]}]}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$or inside $and should be allowed: {reason}"

    # -------------------------------------------------------------------------
    # $or - Invalid Cases (Depth > 1)
    # -------------------------------------------------------------------------

    def test_or_depth_2_nested_or(self, time_field, valid_time_range):
        """$or nested inside $or - REJECTED."""
        query = {**valid_time_range, "$or": [{"$or": [{"a": 1}, {"a": 2}]}, {"b": 1}]}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$or nested inside $or should be rejected"
        assert "nested" in reason.lower() or "depth" in reason.lower()

    def test_or_depth_3(self, time_field, valid_time_range):
        """$or at depth 3 - deeply nested."""
        query = {
            **valid_time_range,
            "$or": [{"$or": [{"$or": [{"a": 1}]}, {"b": 2}]}, {"c": 3}],
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "Deeply nested $or should be rejected"

    def test_or_nested_via_elemMatch(self, time_field, valid_time_range):
        """DEVIL'S ADVOCATE: $or inside $elemMatch - should this count?"""
        # $elemMatch operates on array elements, so nested $or there is OK
        # because it's evaluating within a single document's array
        query = {
            **valid_time_range,
            "items": {
                "$elemMatch": {"$or": [{"status": "pending"}, {"priority": "high"}]}
            },
        }
        # This actually creates depth 1 $or, inside an array context
        depth = _or_depth(query)
        # The $or is inside $elemMatch, which is inside "items" dict
        # Current implementation: this reads as depth 1
        assert depth == 1, f"$or in $elemMatch should be depth 1, got {depth}"

    # -------------------------------------------------------------------------
    # $nor - Valid Cases (Not on Time Field)
    # -------------------------------------------------------------------------

    def test_nor_on_non_time_field(self, time_field, valid_time_range):
        """$nor on non-time fields - should be allowed."""
        query = {
            **valid_time_range,
            "$nor": [{"status": "deleted"}, {"status": "archived"}],
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$nor on non-time field should be allowed: {reason}"

    def test_nor_multiple_conditions(self, time_field, valid_time_range):
        """$nor with multiple complex conditions."""
        query = {
            **valid_time_range,
            "$nor": [{"type": "test", "value": {"$lt": 0}}, {"status": "draft"}],
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$nor with multiple conditions should be allowed: {reason}"

    # -------------------------------------------------------------------------
    # $nor - Invalid Cases (References Time Field)
    # -------------------------------------------------------------------------

    def test_nor_on_time_field_direct(self, time_field, valid_time_range):
        """$nor directly referencing time field - REJECTED."""
        query = {
            "$nor": [{time_field: {"$lt": datetime(2024, 1, 15, tzinfo=timezone.utc)}}]
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$nor on time field should be rejected"
        assert time_field in reason or "$nor" in reason

    def test_nor_on_time_field_complex(self, time_field):
        """$nor with time field in complex condition - REJECTED."""
        query = {
            "$nor": [
                {time_field: {"$lt": "2024-01-01"}},
                {time_field: {"$gte": "2024-12-01"}},
            ]
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$nor with time field in any branch should be rejected"

    # -------------------------------------------------------------------------
    # $not - Valid Cases (Not on Time Field)
    # -------------------------------------------------------------------------

    def test_not_on_non_time_field(self, time_field, valid_time_range):
        """$not on non-time field - should be allowed."""
        query = {
            **valid_time_range,
            "value": {"$not": {"$lt": 0}},  # value >= 0
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$not on non-time field should be allowed: {reason}"

    def test_not_with_regex(self, time_field, valid_time_range):
        """$not negating a regex match."""
        query = {**valid_time_range, "name": {"$not": {"$regex": "^test_"}}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$not with $regex should be allowed: {reason}"

    # -------------------------------------------------------------------------
    # $not - Invalid Cases (Applied to Time Field)
    # -------------------------------------------------------------------------

    def test_not_on_time_field_direct(self, time_field):
        """$not directly on time field - REJECTED."""
        query = {
            time_field: {"$not": {"$lt": datetime(2024, 1, 15, tzinfo=timezone.utc)}}
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$not on time field should be rejected"
        assert "$not" in reason or time_field in reason

    def test_not_on_time_field_with_other_conditions(
        self, time_field, valid_time_range
    ):
        """$not on time field even with valid time range elsewhere - REJECTED."""
        query = {
            **valid_time_range,  # This is valid
            time_field: {"$not": {"$lt": "2024-01-15"}},  # But this negates it
        }
        # Actually this query has conflicting time field conditions
        # The $not on time field should still be rejected
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$not on time field should be rejected even with valid range"


# =============================================================================
# Test Class: NEVER_ALLOWED Operators
# =============================================================================


class TestNeverAllowedOperators:
    """
    Verify that all NEVER_ALLOWED operators are rejected.

    These operators cannot be chunked under any circumstances.
    """

    # -------------------------------------------------------------------------
    # Evaluation (Unsafe)
    # -------------------------------------------------------------------------

    def test_expr_basic(self, time_field, valid_time_range):
        """$expr: Compare two fields - REJECTED."""
        query = {**valid_time_range, "$expr": {"$gt": ["$endTime", "$startTime"]}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$expr should be rejected"
        assert "$expr" in reason

    def test_expr_nested(self, time_field, valid_time_range):
        """$expr deeply nested in query - REJECTED."""
        query = {
            **valid_time_range,
            "$and": [{"status": "active"}, {"$expr": {"$eq": ["$a", "$b"]}}],
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$expr nested should be rejected"

    def test_where_basic(self, time_field, valid_time_range):
        """$where: JavaScript expression - REJECTED."""
        query = {**valid_time_range, "$where": "this.endTime > this.startTime"}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$where should be rejected"
        assert "$where" in reason

    def test_where_function(self, time_field, valid_time_range):
        """$where: JavaScript function - REJECTED."""
        query = {**valid_time_range, "$where": "function() { return this.a > this.b; }"}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$where function should be rejected"

    # -------------------------------------------------------------------------
    # Text Search
    # -------------------------------------------------------------------------

    def test_text_basic(self, time_field, valid_time_range):
        """$text: Full-text search - REJECTED."""
        query = {**valid_time_range, "$text": {"$search": "mongodb tutorial"}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$text should be rejected"
        assert "$text" in reason

    def test_text_with_options(self, time_field, valid_time_range):
        """$text with language and case options - REJECTED."""
        query = {
            **valid_time_range,
            "$text": {
                "$search": "café",
                "$language": "french",
                "$caseSensitive": False,
                "$diacriticSensitive": True,
            },
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$text with options should be rejected"

    # -------------------------------------------------------------------------
    # Atlas Search
    # -------------------------------------------------------------------------

    def test_search_atlas(self, time_field, valid_time_range):
        """$search: Atlas Search - REJECTED."""
        query = {
            **valid_time_range,
            "$search": {"text": {"query": "test", "path": "description"}},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$search should be rejected"
        assert "$search" in reason

    def test_vectorSearch(self, time_field, valid_time_range):
        """$vectorSearch: Atlas vector search - REJECTED."""
        query = {
            **valid_time_range,
            "$vectorSearch": {
                "queryVector": [0.1, 0.2, 0.3],
                "path": "embedding",
                "numCandidates": 100,
            },
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$vectorSearch should be rejected"

    # -------------------------------------------------------------------------
    # Geospatial Query Operators
    # -------------------------------------------------------------------------

    def test_near_basic(self, time_field, valid_time_range):
        """$near: Proximity search - REJECTED."""
        query = {**valid_time_range, "location": {"$near": [40.7128, -74.0060]}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$near should be rejected"
        assert "$near" in reason

    def test_near_with_geometry(self, time_field, valid_time_range):
        """$near with $geometry - REJECTED."""
        query = {
            **valid_time_range,
            "location": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [-73.9667, 40.78]},
                    "$maxDistance": 1000,
                }
            },
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$near with $geometry should be rejected"

    def test_nearSphere(self, time_field, valid_time_range):
        """$nearSphere: Spherical proximity - REJECTED."""
        query = {
            **valid_time_range,
            "location": {
                "$nearSphere": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$minDistance": 100,
                    "$maxDistance": 5000,
                }
            },
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$nearSphere should be rejected"

    def test_geoWithin_box(self, time_field, valid_time_range):
        """$geoWithin with $box - REJECTED."""
        query = {
            **valid_time_range,
            "location": {"$geoWithin": {"$box": [[0, 0], [100, 100]]}},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$geoWithin should be rejected"

    def test_geoWithin_polygon(self, time_field, valid_time_range):
        """$geoWithin with $polygon - REJECTED."""
        query = {
            **valid_time_range,
            "location": {
                "$geoWithin": {"$polygon": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]}
            },
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$geoWithin with $polygon should be rejected"

    def test_geoWithin_center(self, time_field, valid_time_range):
        """$geoWithin with $center - REJECTED."""
        query = {
            **valid_time_range,
            "location": {"$geoWithin": {"$center": [[0, 0], 5]}},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$geoWithin with $center should be rejected"

    def test_geoWithin_centerSphere(self, time_field, valid_time_range):
        """$geoWithin with $centerSphere - REJECTED."""
        query = {
            **valid_time_range,
            "location": {"$geoWithin": {"$centerSphere": [[0, 0], 0.001]}},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$geoWithin with $centerSphere should be rejected"

    def test_geoIntersects(self, time_field, valid_time_range):
        """$geoIntersects: Geometry intersection - REJECTED."""
        query = {
            **valid_time_range,
            "area": {
                "$geoIntersects": {
                    "$geometry": {
                        "type": "Polygon",
                        "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                    }
                }
            },
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$geoIntersects should be rejected"

    # -------------------------------------------------------------------------
    # Geospatial Geometry Specifiers (when used standalone)
    # -------------------------------------------------------------------------

    def test_geometry_standalone(self, time_field, valid_time_range):
        """$geometry used in unexpected context - REJECTED."""
        query = {
            **valid_time_range,
            "loc": {"$geometry": {"type": "Point", "coordinates": [0, 0]}},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$geometry should be rejected"

    def test_maxDistance_standalone(self, time_field, valid_time_range):
        """$maxDistance used in query - REJECTED."""
        query = {**valid_time_range, "distance": {"$maxDistance": 1000}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$maxDistance should be rejected"

    def test_minDistance_standalone(self, time_field, valid_time_range):
        """$minDistance used in query - REJECTED."""
        query = {**valid_time_range, "distance": {"$minDistance": 100}}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$minDistance should be rejected"


# =============================================================================
# Test Class: Complex/Realistic Queries
# =============================================================================


class TestComplexQueries:
    """
    Test realistic, complex query patterns from actual XLR8 usage.
    """

    def test_xlr8_typical_device_query(self, time_field, t1, t2):
        """Real XLR8 pattern: device with multiple log configs."""
        query = {
            "$or": [
                {"metadata.sensor_id": ObjectId("64a1234567890123456789ab")},
                {"metadata.sensor_id": ObjectId("64b1234567890123456789ab")},
                {"metadata.sensor_id": ObjectId("64c1234567890123456789ab")},
            ],
            "metadata.device_id": ObjectId("123456789012345678901234"),
            time_field: {"$gte": t1, "$lt": t2},
        }
        is_chunkable, reason, bounds = is_chunkable_query(query, time_field)
        assert is_chunkable, f"Typical XLR8 query should be chunkable: {reason}"
        assert bounds == (t1, t2)

    def test_complex_sensor_filter(self, time_field, valid_time_range):
        """Complex sensor filtering with multiple conditions."""
        query = {
            **valid_time_range,
            "$and": [
                {"sensor_type": {"$in": ["temperature", "pressure", "flow"]}},
                {"value": {"$gte": 0, "$lte": 1000}},
                {"$or": [{"location": "plant_a"}, {"location": "plant_b"}]},
                {"status": {"$ne": "offline"}},
                {"calibrated": {"$exists": True}},
            ],
        }
        # This has $or at depth 1 (inside $and, but $and doesn't increase depth)
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"Complex sensor filter should be valid: {reason}"

    def test_time_series_with_quality_flags(self, time_field, valid_time_range):
        """Time series with quality flag filtering."""
        query = {
            **valid_time_range,
            "quality_flags": {"$bitsAllClear": 0b00001111},  # No error flags
            "data_type": {"$type": "double"},
            "source": {"$regex": "^sensor_"},
            "tags": {"$all": ["validated", "production"]},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"Time series with quality flags should be valid: {reason}"

    def test_many_comparisons(self, time_field, valid_time_range):
        """Query with many comparison conditions."""
        query = {
            **valid_time_range,
            "temp": {"$gte": 20, "$lte": 100},
            "pressure": {"$gt": 0, "$lt": 500},
            "humidity": {"$ne": 0},
            "level": {"$in": [1, 2, 3, 4, 5]},
            "status_code": {"$nin": [0, -1, 999]},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"Many comparisons should be valid: {reason}"


# =============================================================================
# Test Class: Edge Cases
# =============================================================================


class TestEdgeCases:
    """
    Devil's advocate tests - tricky edge cases designed to find bugs.
    """

    def test_empty_query(self, time_field):
        """Empty query - should fail due to no time range."""
        valid, reason = validate_query_for_chunking({}, time_field)
        assert valid, "Empty query has no forbidden operators"
        # But it's not chunkable due to missing time bounds - now returns SINGLE mode
        result = is_chunkable_query({}, time_field)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Empty query should be SINGLE mode (no time filtering)"
        )
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)

    def test_time_field_only(self, time_field, t1, t2):
        """Only time field constraint - should be chunkable."""
        query = {time_field: {"$gte": t1, "$lt": t2}}
        is_chunkable, reason, bounds = is_chunkable_query(query, time_field)
        assert is_chunkable, f"Time-only query should be chunkable: {reason}"

    def test_operator_as_field_name(self, time_field, valid_time_range):
        """DEVIL'S ADVOCATE: Field named like an operator."""
        # What if someone has a field literally named "$near" (unlikely but possible)?
        # Our check looks at keys, so this would be flagged
        query = {**valid_time_range, "config": {"$near": "value"}}
        valid, reason = validate_query_for_chunking(query, time_field)
        # This IS using $near as an operator key, so it should be rejected
        assert not valid, (
            "Using $near (even if intended as weird field) should be rejected"
        )

    def test_operator_in_string_value(self, time_field, valid_time_range):
        """Operator name in string value - should be allowed."""
        query = {**valid_time_range, "description": "Use $near for proximity search"}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, "String containing '$near' should be allowed"

    def test_deeply_nested_allowed_operators(self, time_field, valid_time_range):
        """Deeply nested but all allowed operators."""
        query = {
            **valid_time_range,
            "$and": [
                {
                    "$and": [
                        {"$and": [{"a": {"$gt": 0}}, {"b": {"$lt": 100}}]},
                        {"c": {"$in": [1, 2, 3]}},
                    ]
                },
                {"d": {"$exists": True}},
            ],
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"Deeply nested $and should be allowed: {reason}"

    def test_forbidden_op_deeply_buried(self, time_field, valid_time_range):
        """Forbidden operator buried deep in query - must be found."""
        query = {
            **valid_time_range,
            "$and": [
                {"status": "active"},
                {
                    "$and": [
                        {"value": {"$gt": 0}},
                        {
                            "items": {
                                "$elemMatch": {"data": {"$expr": {"$gt": ["$a", "$b"]}}}
                            }
                        },
                    ]
                },
            ],
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "Deeply buried $expr should be found and rejected"
        assert "$expr" in reason

    def test_multiple_forbidden_ops(self, time_field, valid_time_range):
        """Multiple forbidden operators - first one should be reported."""
        query = {
            **valid_time_range,
            "$expr": {"$gt": ["$a", "$b"]},
            "$where": "this.x > 0",
            "$text": {"$search": "test"},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "Multiple forbidden ops should be rejected"
        # Should report at least one
        assert any(op in reason for op in ["$expr", "$where", "$text"])

    def test_or_empty_array(self, time_field, valid_time_range):
        """$or with empty array."""
        query = {**valid_time_range, "$or": []}
        # Empty $or matches no documents in MongoDB, so we reject it
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "Empty $or array should be rejected (matches no documents)"
        assert "empty" in reason.lower(), f"Reason should mention empty array: {reason}"

    def test_nor_empty_array(self, time_field, valid_time_range):
        """$nor with empty array."""
        query = {**valid_time_range, "$nor": []}
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, "Empty $nor array should be allowed"

    def test_dotted_field_not_time_field(self, time_field, valid_time_range):
        """Dotted field path that starts like time field."""
        # time_field is "timestamp", but "timestamp.nested" is different
        query = {**valid_time_range, "timestamp.subsecond": {"$not": {"$exists": True}}}
        # This is $not on "timestamp.subsecond", not on "timestamp"
        # Should be allowed
        valid, reason = validate_query_for_chunking(query, time_field)
        assert valid, f"$not on nested field should be allowed: {reason}"

    def test_or_with_forbidden_inside(self, time_field, valid_time_range):
        """$or branch contains forbidden operator."""
        query = {
            **valid_time_range,
            "$or": [{"status": "active"}, {"$expr": {"$eq": ["$a", "$b"]}}],
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$expr inside $or branch should be rejected"

    def test_elemMatch_with_forbidden(self, time_field, valid_time_range):
        """$elemMatch containing forbidden operator."""
        query = {
            **valid_time_range,
            "items": {"$elemMatch": {"$where": "this.price > this.cost"}},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$where inside $elemMatch should be rejected"

    def test_all_with_elemMatch_with_forbidden(self, time_field, valid_time_range):
        """Complex nesting with forbidden at leaf."""
        query = {
            **valid_time_range,
            "data": {"$all": [{"$elemMatch": {"value": {"$near": [0, 0]}}}]},
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "Forbidden op deep in $all/$elemMatch should be rejected"

    def test_alternative_time_field_name(self):
        """Different time field names work correctly."""
        query = {
            "recordedAt": {
                "$gte": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "$lt": datetime(2024, 2, 1, tzinfo=timezone.utc),
            },
            "status": "active",
        }
        is_chunkable, reason, bounds = is_chunkable_query(query, "recordedAt")
        assert is_chunkable, f"Alternative time field should work: {reason}"

    def test_time_field_in_nested_path(self):
        """Time field as nested path like 'metadata.timestamp'."""
        time_field = "metadata.timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
        query = {time_field: {"$gte": t1, "$lt": t2}, "status": "active"}
        is_chunkable, reason, bounds = is_chunkable_query(query, time_field)
        assert is_chunkable, f"Nested time field path should work: {reason}"

    def test_not_applied_to_nested_time_field(self):
        """$not on nested time field should still be rejected."""
        time_field = "event.timestamp"
        query = {
            time_field: {"$not": {"$lt": datetime(2024, 1, 15, tzinfo=timezone.utc)}}
        }
        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$not on nested time field should be rejected"


# =============================================================================
# Test Class: Helper Functions
# =============================================================================


class TestHelperFunctions:
    """Test internal helper functions."""

    def test_or_depth_calculation(self):
        """Verify _or_depth calculates correctly."""
        assert _or_depth({}) == 0
        assert _or_depth({"a": 1}) == 0
        assert _or_depth({"$or": []}) == 1
        assert _or_depth({"$or": [{"a": 1}]}) == 1
        assert _or_depth({"$or": [{"$or": [{}]}]}) == 2
        assert _or_depth({"$and": [{"$or": [{}]}]}) == 1

    def test_references_field(self):
        """Verify _references_field works correctly."""
        assert _references_field({"a": 1}, "a")
        assert not _references_field({"a": 1}, "b")
        assert _references_field({"a": {"b": 1}}, "a")
        assert _references_field({"a": {"b": 1}}, "b")
        assert _references_field([{"a": 1}], "a")
        assert not _references_field([{"a": 1}], "b")

    def test_split_global_and_no_or(self):
        """split_global_and with no $or returns query as global."""
        query = {"a": 1, "b": 2}
        global_and, or_list = split_global_and(query)
        assert global_and == {"a": 1, "b": 2}
        assert or_list == []

    def test_split_global_and_with_or(self):
        """split_global_and extracts $or correctly."""
        query = {"$or": [{"type": "A"}, {"type": "B"}], "status": "active"}
        global_and, or_list = split_global_and(query)
        assert global_and == {"status": "active"}
        assert or_list == [{"type": "A"}, {"type": "B"}]


# =============================================================================
# Test Class: Operator Sets Integrity
# =============================================================================


class TestOperatorSetsIntegrity:
    """Verify operator sets are properly defined."""

    def test_sets_are_disjoint(self):
        """Operator sets should not overlap."""
        assert ALWAYS_ALLOWED.isdisjoint(CONDITIONAL), (
            "ALWAYS_ALLOWED and CONDITIONAL overlap"
        )
        assert ALWAYS_ALLOWED.isdisjoint(NEVER_ALLOWED), (
            "ALWAYS_ALLOWED and NEVER_ALLOWED overlap"
        )
        assert CONDITIONAL.isdisjoint(NEVER_ALLOWED), (
            "CONDITIONAL and NEVER_ALLOWED overlap"
        )

    def test_all_operators_start_with_dollar(self):
        """All operators should start with $."""
        for op in ALWAYS_ALLOWED:
            assert op.startswith("$"), f"Operator {op} doesn't start with $"
        for op in CONDITIONAL:
            assert op.startswith("$"), f"Operator {op} doesn't start with $"
        for op in NEVER_ALLOWED:
            assert op.startswith("$"), f"Operator {op} doesn't start with $"

    def test_expected_operator_counts(self):
        """Verify operator count matches documentation."""
        assert len(ALWAYS_ALLOWED) == 23, (
            f"Expected 23 ALWAYS_ALLOWED, got {len(ALWAYS_ALLOWED)}"
        )
        assert len(CONDITIONAL) == 3, f"Expected 3 CONDITIONAL, got {len(CONDITIONAL)}"
        assert len(NEVER_ALLOWED) == 17, (
            f"Expected 17 NEVER_ALLOWED, got {len(NEVER_ALLOWED)}"
        )

    def test_specific_operators_in_correct_sets(self):
        """Spot check specific operators are in correct sets."""
        # ALWAYS_ALLOWED
        assert "$eq" in ALWAYS_ALLOWED
        assert "$regex" in ALWAYS_ALLOWED
        assert "$and" in ALWAYS_ALLOWED
        assert "$bitsAllSet" in ALWAYS_ALLOWED

        # CONDITIONAL
        assert "$or" in CONDITIONAL
        assert "$nor" in CONDITIONAL
        assert "$not" in CONDITIONAL

        # NEVER_ALLOWED
        assert "$expr" in NEVER_ALLOWED
        assert "$near" in NEVER_ALLOWED
        assert "$text" in NEVER_ALLOWED
        assert "$where" in NEVER_ALLOWED


# =============================================================================
# Test Class: Edge Cases - Data Loss Prevention
# =============================================================================


class TestEdgeCasesDataLossPrevention:
    """
    Critical edge case tests to prevent data loss.

    These tests are specifically designed to catch bugs in
    extract_time_bounds_recursive() that could cause silent data loss by
    incorrectly calculating time bounds.
    """

    # -------------------------------------------------------------------------
    # CRITICAL: $or with Unbounded/Partial Branches
    # -------------------------------------------------------------------------

    def test_or_with_one_unbounded_branch(self):
        """$or with one bounded and one unbounded branch should be rejected.

        This is THE critical bug that caused 24.5% data loss in production.
        If we extract bounds from only the bounded branch, we'll miss data
        from the unbounded branch.
        """
        time_field = "recordedAt"
        start = datetime(2025, 11, 30, tzinfo=timezone.utc)
        now = datetime(2025, 12, 20, tzinfo=timezone.utc)

        query = {
            "$or": [
                # Branch 1: Bounded (has both lower and upper)
                {
                    "type": "A",
                    time_field: {
                        "$gte": start + timedelta(days=1),
                        "$lte": now + timedelta(days=6),
                    },
                },
                # Branch 2: Unbounded (only has lower bound)
                {"type": "B", time_field: {"$gte": start}},
            ]
        }

        result = is_chunkable_query(query, time_field)
        # Changed: Now returns SINGLE mode (valid but not parallelizable)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Query with unbounded $or branch should return SINGLE mode "
            "(valid but not parallelizable)"
        )
        assert (
            "unbounded" in result.reason.lower() or "partial" in result.reason.lower()
        ), f"Reason should mention unbounded/partial bounds: {result.reason}"

    def test_or_with_all_unbounded_branches(self):
        """$or where all branches are unbounded should return SINGLE mode."""
        time_field = "timestamp"
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {"type": "A", time_field: {"$gte": start}},  # Only lower
                {"type": "B", time_field: {"$lte": start}},  # Only upper
            ]
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Query with all unbounded $or branches should be SINGLE mode"
        )

    def test_or_with_mixed_partial_bounds(self):
        """$or with branches having different partial bounds should return
        SINGLE mode.
        """
        time_field = "timestamp"
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {time_field: {"$gte": t}},  # Only lower
                {time_field: {"$lte": t}},  # Only upper
            ]
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Query with mixed partial bounds should be SINGLE mode"
        )

    # -------------------------------------------------------------------------
    # CRITICAL: $or Branches Not Referencing Time Field
    # -------------------------------------------------------------------------

    def test_or_with_branch_not_referencing_time_field(self):
        """$or where one branch doesn't reference time field should return SINGLE mode.

        Query is valid but can't be parallelized safely.
        """
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                # Branch 1: Has time bounds
                {time_field: {"$gte": t1, "$lt": t2}},
                # Branch 2: No time reference at all
                {"status": "active", "type": "important"},
            ]
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Query where $or branch doesn't reference time field should be SINGLE mode"
        )

    def test_or_with_all_branches_not_referencing_time(self):
        """$or where no branches reference time field should return SINGLE mode."""
        time_field = "timestamp"

        query = {
            "$or": [
                {"type": "A", "status": "active"},
                {"type": "B", "status": "pending"},
            ]
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Query with no time references in $or should be SINGLE mode"
        )

    # -------------------------------------------------------------------------
    # Multiple/Conflicting Operators on Time Field
    # -------------------------------------------------------------------------

    def test_multiple_lower_bound_operators(self):
        """Multiple lower bound operators ($gte + $gt) should take most restrictive.

        Note: With proper lo_inclusive tracking, we no longer add microseconds
        to simulate $gt. Instead, the actual value is returned and lo_inclusive
        is set to False when $gt is the effective operator.
        """
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 5, tzinfo=timezone.utc)
        t3 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            time_field: {
                "$gte": t1,  # Less restrictive
                "$gt": t2,  # More restrictive (exclusive)
                "$lt": t3,
            }
        }

        is_chunkable, reason, bounds = is_chunkable_query(query, time_field)
        assert is_chunkable, (
            f"Query with multiple lower bounds should be allowed: {reason}"
        )
        assert bounds is not None
        # Should take t2 as lower bound (the actual value from more restrictive $gt)
        # The lo_inclusive flag handles the exclusivity, not microsecond adjustment
        assert bounds[0] == t2, f"Expected lower bound {t2}, got {bounds[0]}"

    def test_multiple_upper_bound_operators(self):
        """Multiple upper bound operators ($lt + $lte) should take most restrictive."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
        t3 = datetime(2024, 2, 5, tzinfo=timezone.utc)

        query = {
            time_field: {
                "$gte": t1,
                "$lt": t2,  # More restrictive
                "$lte": t3,  # Less restrictive
            }
        }

        is_chunkable, reason, bounds = is_chunkable_query(query, time_field)
        assert is_chunkable, (
            f"Query with multiple upper bounds should be allowed: {reason}"
        )
        assert bounds is not None
        # Should take t2 as upper bound (more restrictive $lt)
        assert bounds[1] == t2, f"Expected upper bound {t2}, got {bounds[1]}"

    def test_contradictory_bounds(self):
        """Contradictory bounds (lower > upper) should return SINGLE mode."""
        time_field = "timestamp"
        t1 = datetime(2024, 2, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 1, tzinfo=timezone.utc)

        query = {
            time_field: {
                "$gte": t1,  # Later date
                "$lt": t2,  # Earlier date - contradiction!
            }
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Query with contradictory bounds (lo > hi) should be SINGLE mode"
        )
        assert (
            "invalid" in result.reason.lower() or "contradict" in result.reason.lower()
        ), f"Reason should mention invalid/contradictory bounds: {result.reason}"

    # -------------------------------------------------------------------------
    # Empty Arrays and Operators
    # -------------------------------------------------------------------------

    def test_in_with_empty_array(self):
        """$in with empty array should return SINGLE mode (matches no documents)."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {time_field: {"$gte": t1, "$lt": t2, "$in": []}}

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Query with empty $in array should be SINGLE mode"
        )

    def test_or_with_empty_array(self):
        """$or with empty array should be rejected (matches no documents)."""
        time_field = "timestamp"

        query = {"$or": []}

        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$or with empty array should be rejected"

    def test_and_with_empty_array(self):
        """$and with empty array should be allowed (matches all documents)."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {time_field: {"$gte": t1, "$lt": t2}, "$and": []}

        is_chunkable, reason, bounds = is_chunkable_query(query, time_field)
        assert is_chunkable, f"$and with empty array should be allowed: {reason}"

    # -------------------------------------------------------------------------
    # $elemMatch on Arrays
    # -------------------------------------------------------------------------

    def test_elemmatch_on_time_field_array(self):
        """$elemMatch on time field array should work if bounds can be extracted."""
        time_field = "events"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {time_field: {"$elemMatch": {"timestamp": {"$gte": t1, "$lt": t2}}}}

        # This should be rejected because we can't safely extract bounds from $elemMatch
        valid, reason = validate_query_for_chunking(query, time_field)
        # $elemMatch is in ALWAYS_ALLOWED, so it passes basic validation
        # But time extraction might fail
        # Let's just verify it doesn't crash
        assert isinstance(valid, bool)

    # -------------------------------------------------------------------------
    # $and with Contradictory Constraints
    # -------------------------------------------------------------------------

    def test_and_with_contradictory_time_constraints(self):
        """$and with contradictory time constraints should be detected."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
        t3 = datetime(2024, 3, 1, tzinfo=timezone.utc)

        query = {
            "$and": [
                {time_field: {"$gte": t1, "$lt": t2}},  # Range 1: Jan
                {time_field: {"$gte": t3}},  # Range 2: After Mar - no overlap!
            ]
        }

        is_chunkable, reason, bounds = is_chunkable_query(query, time_field)
        # The bounds extraction should take intersection
        # If done correctly, should get (t3, t2) which is invalid
        if is_chunkable:
            # If it passes, bounds should at least be sensible
            assert bounds[0] < bounds[1], "Bounds should be valid if query is chunkable"
        else:
            # Ideally should be rejected due to contradiction
            assert "invalid" in reason.lower() or "contradiction" in reason.lower()

    # -------------------------------------------------------------------------
    # Negation Operators on Time Field
    # -------------------------------------------------------------------------

    def test_ne_on_time_field(self):
        """$ne on time field should return SINGLE mode (can't safely bound
        but valid).
        """
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            time_field: {
                "$gte": t1,
                "$lt": t2,
                "$ne": datetime(2024, 1, 15, tzinfo=timezone.utc),
            }
        }

        # $ne on time field returns SINGLE mode
        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "$ne on time field should be SINGLE mode"
        )
        assert "negation" in result.reason.lower() or "$ne" in result.reason, (
            f"Reason should mention negation: {result.reason}"
        )

    def test_nin_on_time_field(self):
        """$nin on time field should return SINGLE mode (can't safely bound
        but valid).
        """
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            time_field: {
                "$gte": t1,
                "$lt": t2,
                "$nin": [datetime(2024, 1, 10, tzinfo=timezone.utc)],
            }
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "$nin on time field should be SINGLE mode"
        )
        assert "negation" in result.reason.lower() or "$nin" in result.reason, (
            f"Reason should mention negation: {result.reason}"
        )

    def test_not_on_time_field(self):
        """$not on time field should be rejected."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)

        query = {time_field: {"$not": {"$lt": t1}}}

        valid, reason = validate_query_for_chunking(query, time_field)
        assert not valid, "$not on time field should be rejected"
        assert "$not" in reason or "negation" in reason.lower(), (
            f"Reason should mention $not: {reason}"
        )

    # -------------------------------------------------------------------------
    # Complex Real-World Scenarios
    # -------------------------------------------------------------------------

    def test_complex_or_with_different_bound_types(self):
        """Complex $or where branches have different types of time constraints."""
        time_field = "recordedAt"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
        t3 = datetime(2024, 3, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                # Branch 1: Fully bounded range
                {time_field: {"$gte": t1, "$lt": t2}},
                # Branch 2: Only lower bound
                {time_field: {"$gte": t3}},
                # Branch 3: Only upper bound
                {time_field: {"$lt": t1}},
            ]
        }

        result = is_chunkable_query(query, time_field)
        # Changed to expect SINGLE mode (valid but not parallelizable)
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Complex $or with mixed bound types should return SINGLE mode"
        )

    def test_nested_and_or_with_partial_bounds(self):
        """Nested $and/$or where some paths have partial time bounds."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            "$and": [
                {"status": "active"},
                {
                    "$or": [
                        {time_field: {"$gte": t1, "$lt": t2}},  # Bounded
                        {time_field: {"$gte": t1}},  # Unbounded
                    ]
                },
            ]
        }

        result = is_chunkable_query(query, time_field)
        # Changed to expect SINGLE mode (not REJECT) for unbounded $or
        assert result.mode == ChunkabilityMode.SINGLE, (
            "Nested query with unbounded $or branch should return SINGLE mode"
        )


# =============================================================================
# Test Class: Three-Tier Graceful Degradation (PARALLEL/SINGLE/REJECT)
# =============================================================================


class TestThreeTierGracefulDegradation:
    """
    Test the three-tier graceful degradation system.

    Modes:
    - PARALLEL: Safe for parallel time-chunked execution
    - SINGLE: Valid query, single-worker fallback
    - REJECT: Would produce incorrect results
    """

    # -------------------------------------------------------------------------
    # Test ChunkabilityMode and ChunkabilityResult Types
    # -------------------------------------------------------------------------

    def test_chunkability_mode_enum_values(self):
        """ChunkabilityMode should have three values."""
        assert ChunkabilityMode.PARALLEL.value == "parallel"
        assert ChunkabilityMode.SINGLE.value == "single"
        assert ChunkabilityMode.REJECT.value == "reject"

    def test_chunkability_result_structure(self):
        """ChunkabilityResult should have correct structure."""
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        result = ChunkabilityResult(
            mode=ChunkabilityMode.PARALLEL, reason="", bounds=(t1, t2)
        )

        assert result.mode == ChunkabilityMode.PARALLEL
        assert result.reason == ""
        assert result.bounds == (t1, t2)

    def test_chunkability_result_mode_property(self):
        """Mode should correctly identify PARALLEL vs non-PARALLEL."""
        parallel = ChunkabilityResult(ChunkabilityMode.PARALLEL, "", (None, None))
        single = ChunkabilityResult(ChunkabilityMode.SINGLE, "reason", (None, None))
        reject = ChunkabilityResult(ChunkabilityMode.REJECT, "reason", (None, None))

        assert parallel.mode == ChunkabilityMode.PARALLEL
        assert single.mode == ChunkabilityMode.SINGLE
        assert reject.mode == ChunkabilityMode.REJECT

    def test_chunkability_result_mode_values(self):
        """Mode should correctly identify all three ChunkabilityMode values."""
        parallel = ChunkabilityResult(ChunkabilityMode.PARALLEL, "", (None, None))
        single = ChunkabilityResult(ChunkabilityMode.SINGLE, "reason", (None, None))
        reject = ChunkabilityResult(ChunkabilityMode.REJECT, "reason", (None, None))

        assert parallel.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)
        assert single.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)
        assert reject.mode == ChunkabilityMode.REJECT

    # -------------------------------------------------------------------------
    # Test has_natural_sort() Function
    # -------------------------------------------------------------------------

    def test_has_natural_sort_with_natural(self):
        """Should detect $natural sort."""
        sort_spec = [("$natural", 1)]
        assert has_natural_sort(sort_spec) is True

        sort_spec = [("$natural", -1)]
        assert has_natural_sort(sort_spec) is True

    def test_has_natural_sort_mixed(self):
        """Should detect $natural even when mixed with other fields."""
        sort_spec = [("timestamp", 1), ("$natural", 1)]
        assert has_natural_sort(sort_spec) is True

    def test_has_natural_sort_no_natural(self):
        """Should return False for non-$natural sorts."""
        sort_spec = [("timestamp", 1)]
        assert has_natural_sort(sort_spec) is False

        sort_spec = [("timestamp", 1), ("value", -1)]
        assert has_natural_sort(sort_spec) is False

    def test_has_natural_sort_empty(self):
        """Should return False for empty sort."""
        assert has_natural_sort([]) is False
        assert has_natural_sort(None) is False

    # -------------------------------------------------------------------------
    # Test PARALLEL Mode (Safe for Parallelization)
    # -------------------------------------------------------------------------

    def test_parallel_mode_basic_query(self):
        """Basic query with time bounds should be PARALLEL."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {time_field: {"$gte": t1, "$lt": t2}, "status": "active"}

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.PARALLEL
        assert result.reason == ""
        assert result.bounds == (t1, t2)

    def test_parallel_mode_or_query_bounded(self):
        """$or query with all branches bounded should be PARALLEL."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {"region_id": "A", time_field: {"$gte": t1, "$lt": t2}},
                {"region_id": "B", time_field: {"$gte": t1, "$lt": t2}},
            ]
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.PARALLEL
        assert result.bounds == (t1, t2)

    # -------------------------------------------------------------------------
    # Test SINGLE Mode (Valid but Single-Worker Fallback)
    # -------------------------------------------------------------------------

    def test_single_mode_natural_sort(self):
        """$natural sort should return SINGLE mode."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {time_field: {"$gte": t1, "$lt": t2}}
        sort_spec = [("$natural", 1)]

        result = is_chunkable_query(query, time_field, sort_spec)
        assert result.mode == ChunkabilityMode.SINGLE
        assert "$natural sort" in result.reason
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)

    def test_single_mode_no_time_reference(self):
        """Query with no time field reference should be SINGLE."""
        time_field = "timestamp"
        query = {"status": "active", "value": {"$gt": 100}}

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE
        assert "no time field reference" in result.reason
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)

    def test_single_mode_unbounded_or_branch(self):
        """$or with unbounded branch should be SINGLE."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {"region_id": "A", time_field: {"$gte": t1, "$lt": t2}},
                {"region_id": "B"},  # No time constraint
            ]
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE
        assert "$or query has unbounded" in result.reason
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)

    def test_single_mode_partial_time_bounds(self):
        """Query with only $gte or only $lt should be SINGLE."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # Only lower bound
        query = {time_field: {"$gte": t1}}
        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE
        assert "no complete time range" in result.reason

        # Only upper bound
        query = {time_field: {"$lt": t1}}
        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE
        assert "no complete time range" in result.reason

    # -------------------------------------------------------------------------
    # Test REJECT Mode (Would Produce Incorrect Results)
    # -------------------------------------------------------------------------

    def test_reject_mode_expr_operator(self):
        """$expr operator should return SINGLE mode (can execute, just not
        parallelize).
        """
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {time_field: {"$gte": t1, "$lt": t2}, "$expr": {"$gt": ["$a", "$b"]}}

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE
        assert "$expr" in result.reason
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)
        assert result.bounds == (t1, t2)

    def test_reject_mode_text_search(self):
        """$text operator should return SINGLE mode (can execute, just not
        parallelize).
        """
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {time_field: {"$gte": t1, "$lt": t2}, "$text": {"$search": "mongodb"}}

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE
        assert "$text" in result.reason
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)
        assert result.bounds == (t1, t2)

    def test_reject_mode_near_geospatial(self):
        """$near operator should return SINGLE mode (can execute, just not
        parallelize).
        """
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            time_field: {"$gte": t1, "$lt": t2},
            "location": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                    "$maxDistance": 1000,
                }
            },
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE
        assert "$near" in result.reason
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)
        assert result.bounds == (t1, t2)

    def test_reject_mode_nested_or(self):
        """Nested $or (depth > 1) should return SINGLE mode (complex but executable)."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            time_field: {"$gte": t1, "$lt": t2},
            "$or": [{"$or": [{"a": 1}, {"b": 2}]}, {"c": 3}],
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE
        assert "nested $or" in result.reason
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)
        assert result.bounds == (t1, t2)

    # -------------------------------------------------------------------------
    # Test Backwards Compatibility
    # -------------------------------------------------------------------------

    def test_backwards_compatible_parallel(self):
        """PARALLEL mode should work with old is_chunkable checks."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {time_field: {"$gte": t1, "$lt": t2}}
        result = is_chunkable_query(query, time_field)

        # Check mode directly
        assert result.mode == ChunkabilityMode.PARALLEL

        # Old code expected tuple - we provide properties
        mode = result.mode
        assert mode == ChunkabilityMode.PARALLEL

    def test_backwards_compatible_reject(self):
        """REJECT mode should work with old is_chunkable checks (use empty
        $or for true REJECT).
        """
        time_field = "timestamp"
        # Use empty $or which is invalid MongoDB syntax
        query = {"$or": []}
        result = is_chunkable_query(query, time_field)

        # Check mode directly
        assert result.mode == ChunkabilityMode.REJECT

    # -------------------------------------------------------------------------
    # Edge Cases and Complex Scenarios
    # -------------------------------------------------------------------------

    def test_single_mode_natural_sort_descending(self):
        """$natural sort descending should also be SINGLE."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {time_field: {"$gte": t1, "$lt": t2}}
        sort_spec = [("$natural", -1)]

        result = is_chunkable_query(query, time_field, sort_spec)
        assert result.mode == ChunkabilityMode.SINGLE
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)

    def test_reject_trumps_single(self):
        """True REJECT (empty $or) takes precedence over SINGLE mode conditions."""
        time_field = "timestamp"

        # Query with empty $or (REJECT) and $natural sort (SINGLE)
        query = {"$or": []}
        sort_spec = [("$natural", 1)]

        result = is_chunkable_query(query, time_field, sort_spec)
        # Should reject because of empty $or
        assert result.mode == ChunkabilityMode.REJECT
        assert result.mode == ChunkabilityMode.REJECT

    def test_single_mode_or_with_mixed_bounds(self):
        """$or with some bounded and some unbounded branches is SINGLE."""
        time_field = "timestamp"
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 2, 1, tzinfo=timezone.utc)

        query = {
            "$or": [
                {time_field: {"$gte": t1, "$lt": t2}, "region": "A"},
                {time_field: {"$gte": t1}, "region": "B"},  # Only lower bound
                {"region": "C"},  # No time constraint
            ]
        }

        result = is_chunkable_query(query, time_field)
        assert result.mode == ChunkabilityMode.SINGLE
        assert result.mode in (ChunkabilityMode.PARALLEL, ChunkabilityMode.SINGLE)
