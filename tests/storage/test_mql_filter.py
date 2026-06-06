"""
Tests for MQL-to-DuckDB-SQL filter translator (mql_filter.py).

Tests cover:
- Basic comparison operators ($eq, $ne, $gt, $gte, $lt, $lte)
- Array operators ($in, $nin, $all, $elemMatch, $size)
- Element operators ($exists, $type)
- Evaluation operators ($regex, $mod)
- Bitwise operators ($bitsAllSet, etc.)
- Logical operators ($and, $or, $nor, $not)
- Implicit $and (multiple top-level keys)
- Types.Any() field handling
- Forbidden operators (raises ValueError)
- Date range filtering
- Edge cases (empty queries, nulls, ObjectIds)
"""

from datetime import datetime, timezone

import pytest
from bson import ObjectId

from xlr8.schema import Schema, Types
from xlr8.storage.cache_handler import _inline_params, _quote_literal
from xlr8.storage.mql_filter import (
    _any_numeric_coalesce,
    _any_string_coalesce,
    _detect_value_kind,
    _format_param_value,
    translate_mql_to_sql,
)

# ────────────────────────────────────────────────────────────────
# Test fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def simple_schema():
    """Schema with typed fields (no Any types) for basic testing."""
    return Schema(
        time_field="timestamp",
        fields={
            "timestamp": Types.Timestamp("ms", tz="UTC"),
            "sensor_id": Types.String(),
            "value": Types.Float(),
            "status": Types.String(),
            "count": Types.Int(),
            "active": Types.Bool(),
            "device.region": Types.String(),
        },
        avg_doc_size_bytes=200,
    )


@pytest.fixture
def any_schema():
    """Schema with a Types.Any() field for Any-type testing."""
    return Schema(
        time_field="timestamp",
        fields={
            "timestamp": Types.Timestamp("ms", tz="UTC"),
            "sensor_id": Types.String(),
            "value": Types.Any(),
            "metadata": Types.Any(),
        },
        avg_doc_size_bytes=200,
    )


@pytest.fixture
def t1():
    return datetime(2024, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def t2():
    return datetime(2024, 7, 1, tzinfo=timezone.utc)


# ────────────────────────────────────────────────────────────────
# Helper function tests
# ────────────────────────────────────────────────────────────────


class TestDetectValueKind:
    def test_numeric(self):
        assert _detect_value_kind(42) == "numeric"
        assert _detect_value_kind(3.14) == "numeric"
        assert _detect_value_kind(0) == "numeric"

    def test_string(self):
        assert _detect_value_kind("hello") == "string"

    def test_bool(self):
        assert _detect_value_kind(True) == "bool"
        assert _detect_value_kind(False) == "bool"

    def test_datetime(self):
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert _detect_value_kind(dt) == "datetime"

    def test_objectid(self):
        assert _detect_value_kind(ObjectId()) == "objectid"

    def test_null(self):
        assert _detect_value_kind(None) == "null"


class TestFormatParamValue:
    def test_objectid_to_string(self):
        oid = ObjectId("507f1f77bcf86cd799439011")
        assert _format_param_value(oid) == "507f1f77bcf86cd799439011"

    def test_pass_through(self):
        assert _format_param_value(42) == 42
        assert _format_param_value("hello") == "hello"
        assert _format_param_value(True) is True


class TestQuoteLiteral:
    def test_null(self):
        assert _quote_literal(None) == "NULL"

    def test_bool(self):
        assert _quote_literal(True) == "TRUE"
        assert _quote_literal(False) == "FALSE"

    def test_int(self):
        assert _quote_literal(42) == "42"
        assert _quote_literal(-1) == "-1"

    def test_float(self):
        assert _quote_literal(3.14) == "3.14"

    def test_string(self):
        assert _quote_literal("hello") == "'hello'"

    def test_string_with_quote(self):
        assert _quote_literal("it's") == "'it''s'"

    def test_datetime(self):
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        result = _quote_literal(dt)
        assert "2024-01-15" in result
        assert "TIMESTAMPTZ" in result

    def test_objectid(self):
        oid = ObjectId("507f1f77bcf86cd799439011")
        assert _quote_literal(oid) == "'507f1f77bcf86cd799439011'"


class TestInlineParams:
    def test_single_param(self):
        result = _inline_params("x = $p0", {"p0": 42})
        assert result == "x = 42"

    def test_multiple_params(self):
        result = _inline_params(
            "x = $p0 AND y = $p1",
            {"p0": 42, "p1": "hello"},
        )
        assert result == "x = 42 AND y = 'hello'"

    def test_params_ordered_correctly(self):
        """p10 should be replaced before p1 to avoid partial match."""
        result = _inline_params(
            "x = $p1 AND y = $p10",
            {"p1": 5, "p10": 50},
        )
        assert result == "x = 5 AND y = 50"

    def test_no_params(self):
        result = _inline_params("x = 42", {})
        assert result == "x = 42"


class TestAnyExpressions:
    def test_numeric_coalesce(self):
        expr = _any_numeric_coalesce("value")
        assert "value" in expr
        assert "float_value" in expr
        assert "int64_value" in expr
        assert "int32_value" in expr
        assert "COALESCE" in expr

    def test_string_coalesce(self):
        expr = _any_string_coalesce("value")
        assert "value" in expr
        assert "string_value" in expr
        assert "COALESCE" in expr


# ────────────────────────────────────────────────────────────────
# Basic comparison operators
# ────────────────────────────────────────────────────────────────


class TestBasicComparisons:
    """Test $eq, $ne, $gt, $gte, $lt, $lte on typed fields."""

    def test_eq(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$eq": 42}}, simple_schema, "timestamp"
        )
        assert "=" in sql

    def test_implicit_eq(self, simple_schema):
        """Bare value is treated as $eq."""
        sql, params = translate_mql_to_sql(
            {"value": 42}, simple_schema, "timestamp"
        )
        assert "=" in sql

    def test_gt(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$gt": 100}}, simple_schema, "timestamp"
        )
        assert ">" in sql

    def test_gte(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$gte": 100}}, simple_schema, "timestamp"
        )
        assert ">=" in sql

    def test_lt(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$lt": 200}}, simple_schema, "timestamp"
        )
        assert "<" in sql

    def test_lte(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$lte": 200}}, simple_schema, "timestamp"
        )
        assert "<=" in sql

    def test_ne(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$ne": 42}}, simple_schema, "timestamp"
        )
        assert "!=" in sql

    def test_multiple_operators_on_same_field(self, simple_schema):
        """Range query: $gte AND $lt on same field."""
        sql, params = translate_mql_to_sql(
            {"value": {"$gte": 100, "$lt": 200}},
            simple_schema, "timestamp",
        )
        assert ">=" in sql
        assert "<" in sql
        assert "AND" in sql


# ────────────────────────────────────────────────────────────────
# $in / $nin
# ────────────────────────────────────────────────────────────────


class TestInOperator:
    def test_in(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$in": ["active", "pending"]}},
            simple_schema, "timestamp",
        )
        assert "IN" in sql

    def test_nin(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$nin": ["deleted", "archived"]}},
            simple_schema, "timestamp",
        )
        assert "NOT IN" in sql

    def test_empty_in(self, simple_schema):
        """Empty $in matches nothing."""
        sql, params = translate_mql_to_sql(
            {"status": {"$in": []}}, simple_schema, "timestamp"
        )
        assert sql == "1=0" or sql == "(1=0)"

    def test_empty_nin(self, simple_schema):
        """Empty $nin matches everything."""
        sql, params = translate_mql_to_sql(
            {"status": {"$nin": []}}, simple_schema, "timestamp"
        )
        assert sql == "1=1"

    def test_in_with_null(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$in": [None, "active"]}},
            simple_schema, "timestamp",
        )
        assert "IS NULL" in sql
        assert "IN" in sql


# ────────────────────────────────────────────────────────────────
# $exists
# ────────────────────────────────────────────────────────────────


class TestExistsOperator:
    def test_exists_true(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$exists": True}}, simple_schema, "timestamp"
        )
        assert "IS NOT NULL" in sql

    def test_exists_false(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$exists": False}}, simple_schema, "timestamp"
        )
        assert "IS NULL" in sql


# ────────────────────────────────────────────────────────────────
# $regex
# ────────────────────────────────────────────────────────────────


class TestRegexOperator:
    def test_regex(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"sensor_id": {"$regex": "^TEMP_"}},
            simple_schema, "timestamp",
        )
        assert "REGEXP_MATCHES" in sql


# ────────────────────────────────────────────────────────────────
# Logical operators
# ────────────────────────────────────────────────────────────────


class TestLogicalOperators:
    def test_implicit_and(self, simple_schema):
        """Multiple top-level keys = implicit $and."""
        sql, params = translate_mql_to_sql(
            {"status": "active", "value": {"$gt": 100}},
            simple_schema, "timestamp",
        )
        assert "AND" in sql

    def test_explicit_and(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"$and": [{"status": "active"}, {"value": {"$gt": 100}}]},
            simple_schema, "timestamp",
        )
        assert "AND" in sql

    def test_or(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"$or": [{"status": "active"}, {"status": "pending"}]},
            simple_schema, "timestamp",
        )
        assert "OR" in sql

    def test_nor(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"$nor": [{"status": "deleted"}, {"status": "archived"}]},
            simple_schema, "timestamp",
        )
        assert "NOT" in sql

    def test_not(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$not": {"$eq": "deleted"}}},
            simple_schema, "timestamp",
        )
        assert "NOT" in sql


# ────────────────────────────────────────────────────────────────
# $all, $elemMatch, $size
# ────────────────────────────────────────────────────────────────


class TestArrayOperators:
    def test_all(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$all": ["active", "verified"]}},
            simple_schema, "timestamp",
        )
        # $all on non-list field is AND-of-equalities
        assert sql

    def test_size(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$size": 3}}, simple_schema, "timestamp"
        )
        assert "len" in sql.lower()

    def test_elemMatch_simple(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$elemMatch": {"$eq": "active"}}},
            simple_schema, "timestamp",
        )
        # On non-list field, $elemMatch with no array is no-match
        assert sql


# ────────────────────────────────────────────────────────────────
# $mod
# ────────────────────────────────────────────────────────────────


class TestModOperator:
    def test_mod(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"count": {"$mod": [5, 0]}},
            simple_schema, "timestamp",
        )
        assert "%" in sql


# ────────────────────────────────────────────────────────────────
# $type
# ────────────────────────────────────────────────────────────────


class TestTypeOperator:
    def test_type_single(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$type": "string"}},
            any_schema, "timestamp",
        )
        assert "string_value" in sql
        assert "IS NOT NULL" in sql

    def test_type_multiple(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$type": ["string", "double"]}},
            any_schema, "timestamp",
        )
        assert "OR" in sql

    def test_type_on_typed_field(self, simple_schema):
        """$type on a known-type field is approximate."""
        sql, params = translate_mql_to_sql(
            {"value": {"$type": "double"}},
            simple_schema, "timestamp",
        )
        assert sql  # Should not raise


# ────────────────────────────────────────────────────────────────
# Bitwise operators
# ────────────────────────────────────────────────────────────────


class TestBitwiseOperators:
    def test_bitsAllSet(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"count": {"$bitsAllSet": 3}},
            simple_schema, "timestamp",
        )
        assert "&" in sql

    def test_bitsAllClear(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"count": {"$bitsAllClear": 3}},
            simple_schema, "timestamp",
        )
        assert "&" in sql
        assert "= 0" in sql

    def test_bitsAnySet(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"count": {"$bitsAnySet": 3}},
            simple_schema, "timestamp",
        )
        assert "!= 0" in sql

    def test_bitsAnyClear(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"count": {"$bitsAnyClear": 3}},
            simple_schema, "timestamp",
        )
        assert "!=" in sql


# ────────────────────────────────────────────────────────────────
# Types.Any() field tests
# ────────────────────────────────────────────────────────────────


class TestAnyTypeFields:
    """Test MQL-to-SQL translation when target field is Types.Any()."""

    def test_numeric_eq_on_any(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$eq": 42.5}}, any_schema, "timestamp"
        )
        assert "float_value" in sql
        assert "=" in sql

    def test_numeric_gt_on_any(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$gt": 100}}, any_schema, "timestamp"
        )
        assert "float_value" in sql
        assert ">" in sql

    def test_string_eq_on_any(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$eq": "hello"}}, any_schema, "timestamp"
        )
        assert "string_value" in sql

    def test_bool_eq_on_any(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$eq": True}}, any_schema, "timestamp"
        )
        assert "bool_value" in sql

    def test_datetime_eq_on_any(self, any_schema):
        dt = datetime(2024, 6, 15, tzinfo=timezone.utc)
        sql, params = translate_mql_to_sql(
            {"value": {"$eq": dt}}, any_schema, "timestamp"
        )
        assert "datetime_value" in sql

    def test_objectid_eq_on_any(self, any_schema):
        oid = ObjectId()
        sql, params = translate_mql_to_sql(
            {"value": {"$eq": oid}}, any_schema, "timestamp"
        )
        assert "objectid_value" in sql

    def test_in_on_any(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$in": [1, 2, 3]}}, any_schema, "timestamp"
        )
        assert "IN" in sql
        assert "float_value" in sql

    def test_exists_true_on_any(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$exists": True}}, any_schema, "timestamp"
        )
        assert "IS NOT NULL" in sql

    def test_exists_false_on_any(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$exists": False}}, any_schema, "timestamp"
        )
        # Should check for null struct or null_value=true
        assert "NULL" in sql

    def test_regex_on_any(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": {"$regex": "^hello"}}, any_schema, "timestamp"
        )
        assert "REGEXP_MATCHES" in sql
        assert "string_value" in sql

    def test_eq_null_on_any(self, any_schema):
        sql, params = translate_mql_to_sql(
            {"value": None}, any_schema, "timestamp"
        )
        assert "IS NULL" in sql


# ────────────────────────────────────────────────────────────────
# Forbidden operators
# ────────────────────────────────────────────────────────────────


class TestForbiddenOperators:
    """Test that forbidden operators raise ValueError."""

    def test_near_rejected(self, simple_schema):
        with pytest.raises(ValueError, match="\\$near"):
            translate_mql_to_sql(
                {"location": {"$near": [0, 0]}},
                simple_schema, "timestamp",
            )

    def test_geoWithin_rejected(self, simple_schema):
        with pytest.raises(ValueError, match="\\$geoWithin"):
            translate_mql_to_sql(
                {"location": {"$geoWithin": {}}},
                simple_schema, "timestamp",
            )

    def test_expr_rejected(self, simple_schema):
        with pytest.raises(ValueError, match="\\$expr"):
            translate_mql_to_sql(
                {"$expr": {"$gt": ["$a", "$b"]}},
                simple_schema, "timestamp",
            )

    def test_where_rejected(self, simple_schema):
        with pytest.raises(ValueError, match="\\$where"):
            translate_mql_to_sql(
                {"$where": "this.value > 0"},
                simple_schema, "timestamp",
            )

    def test_text_rejected(self, simple_schema):
        with pytest.raises(ValueError, match="\\$text"):
            translate_mql_to_sql(
                {"$text": {"$search": "hello"}},
                simple_schema, "timestamp",
            )

    def test_search_rejected(self, simple_schema):
        with pytest.raises(ValueError, match="\\$search"):
            translate_mql_to_sql(
                {"$search": "hello"},
                simple_schema, "timestamp",
            )

    def test_nested_or_rejected(self, simple_schema):
        """Nested $or (depth > 1) should raise ValueError."""
        with pytest.raises(ValueError, match="Nested \\$or"):
            translate_mql_to_sql(
                {"$or": [{"$or": [{"a": 1}, {"b": 2}]}, {"c": 3}]},
                simple_schema, "timestamp",
            )


# ────────────────────────────────────────────────────────────────
# Date range filtering
# ────────────────────────────────────────────────────────────────


class TestDateRange:
    def test_start_date_only(self, simple_schema, t1):
        sql, params = translate_mql_to_sql(
            {"status": "active"}, simple_schema, "timestamp",
            start_date=t1,
        )
        assert "timestamp" in sql
        assert ">=" in sql

    def test_end_date_only(self, simple_schema, t2):
        sql, params = translate_mql_to_sql(
            {"status": "active"}, simple_schema, "timestamp",
            end_date=t2,
        )
        assert "timestamp" in sql
        assert "<" in sql

    def test_both_dates(self, simple_schema, t1, t2):
        sql, params = translate_mql_to_sql(
            {"status": "active"}, simple_schema, "timestamp",
            start_date=t1, end_date=t2,
        )
        assert ">=" in sql
        assert "<" in sql
        assert "AND" in sql

    def test_dates_combined_with_mql(self, simple_schema, t1, t2):
        sql, params = translate_mql_to_sql(
            {"value": {"$gt": 100}}, simple_schema, "timestamp",
            start_date=t1, end_date=t2,
        )
        assert "value" in sql
        assert "timestamp" in sql


# ────────────────────────────────────────────────────────────────
# Edge cases
# ────────────────────────────────────────────────────────────────


class TestEdgeCases:
    def test_empty_query(self, simple_schema):
        sql, params = translate_mql_to_sql({}, simple_schema, "timestamp")
        assert sql == "1=1"
        assert params == {}

    def test_null_value_eq(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": None}, simple_schema, "timestamp"
        )
        assert "IS NULL" in sql

    def test_null_value_ne(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"status": {"$ne": None}}, simple_schema, "timestamp"
        )
        assert "IS NOT NULL" in sql

    def test_dotted_field_name(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"device.region": "us-east"}, simple_schema, "timestamp"
        )
        assert "device.region" in sql

    def test_objectid_value(self, simple_schema):
        oid = ObjectId()
        sql, params = translate_mql_to_sql(
            {"sensor_id": oid}, simple_schema, "timestamp"
        )
        assert str(oid) in params.values() or str(oid) in sql

    def test_boolean_value(self, simple_schema):
        sql, params = translate_mql_to_sql(
            {"active": True}, simple_schema, "timestamp"
        )
        # Should have a param for True
        assert True in params.values() or "TRUE" in sql

    def test_params_are_unique(self, simple_schema):
        """Each parameter should have a unique name."""
        sql, params = translate_mql_to_sql(
            {"status": "active", "value": {"$gte": 100, "$lt": 200}},
            simple_schema, "timestamp",
        )
        assert len(params) > 0
        assert len(params) == len(set(params.keys()))
