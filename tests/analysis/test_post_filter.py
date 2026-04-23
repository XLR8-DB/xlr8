"""Tests for analysis/post_filter.py — the MQL post-filter on cached data.

Organized into five sections mirroring the plan:

1. Schema.resolve_path (dotted path resolution used by the parser)
2. Parser / validator (operator allow-list, unknown fields, shape errors)
3. Typed / Any split (And splittable, Or / Not all-or-nothing)
4. Translators agree (polars / PyArrow / DuckDB produce the same row set)
5. Reader integration (end-to-end on real Parquet shards)
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from bson import ObjectId

from xlr8 import Schema, Types, XLR8FilterError
from xlr8.analysis.post_filter import (
    AndNode,
    Leaf,
    NotNode,
    compile_post_filter,
)
from xlr8.storage.reader import ParquetReader

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _base_schema() -> Schema:
    return Schema(
        time_field="recordedAt",
        fields={
            "recordedAt": Types.Timestamp("ms", tz="UTC"),
            "metadata.sensor_id": Types.ObjectId(),
            "metadata.device_id": Types.ObjectId(),
            "status": Types.String(),
            "count": Types.Int(),
            "ratio": Types.Float(),
            "active": Types.Bool(),
            "value": Types.Any(),
        },
    )


def _nested_struct_schema() -> Schema:
    """Alternate shape: metadata as a typed Struct (vs flat dotted keys)."""
    return Schema(
        time_field="ts",
        fields={
            "ts": Types.Timestamp("ms", tz="UTC"),
            "metadata": Types.Struct(
                {"user_id": Types.String(), "session_id": Types.Int()}
            ),
        },
    )


def _any_nested_schema() -> Schema:
    return Schema(
        time_field="ts",
        fields={
            "ts": Types.Timestamp("ms", tz="UTC"),
            "metadata": Types.Any(),
            "status": Types.String(),
        },
    )


# ---------------------------------------------------------------------------
# SECTION 1: Schema.resolve_path
# ---------------------------------------------------------------------------


class TestResolvePath:
    def test_flat_dotted_top_level_key(self):
        schema = _base_schema()
        resolved = schema.resolve_path("metadata.sensor_id")
        assert resolved is not None
        assert not resolved.is_any
        assert isinstance(resolved.field_type, Types.ObjectId)

    def test_typed_struct_path(self):
        schema = _nested_struct_schema()
        resolved = schema.resolve_path("metadata.user_id")
        assert resolved is not None
        assert not resolved.is_any
        assert isinstance(resolved.field_type, Types.String)
        assert resolved.is_orderable

    def test_nested_struct_unknown_leaf(self):
        schema = _nested_struct_schema()
        assert schema.resolve_path("metadata.unknown") is None

    def test_any_top_level(self):
        schema = _base_schema()
        resolved = schema.resolve_path("value")
        assert resolved is not None
        assert resolved.is_any
        assert not resolved.is_orderable

    def test_path_into_any(self):
        """metadata: Types.Any() + path metadata.user_id is VALID and Any."""
        schema = _any_nested_schema()
        resolved = schema.resolve_path("metadata.user_id")
        assert resolved is not None
        assert resolved.is_any

    def test_completely_unknown(self):
        schema = _base_schema()
        assert schema.resolve_path("nope") is None

    def test_orderability(self):
        schema = _base_schema()
        assert schema.resolve_path("count").is_orderable
        assert schema.resolve_path("ratio").is_orderable
        assert schema.resolve_path("status").is_orderable  # String: lex
        assert schema.resolve_path("recordedAt").is_orderable


# ---------------------------------------------------------------------------
# SECTION 2: Parser / validator
# ---------------------------------------------------------------------------


class TestParserBasics:
    def test_bare_equality(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {"status": "active"},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        assert cf.has_typed and not cf.has_any
        assert isinstance(cf.typed_ast, Leaf)
        assert cf.typed_ast.op == "$eq"
        assert cf.typed_ast.value == "active"

    def test_implicit_and_two_clauses(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {"status": "active", "count": 5},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        assert isinstance(cf.typed_ast, AndNode)
        assert len(cf.typed_ast.children) == 2

    def test_eq_ne_in_nin(self):
        schema = _base_schema()
        for op, operand in [
            ("$eq", "a"),
            ("$ne", "a"),
            ("$in", ["a", "b"]),
            ("$nin", ["a", "b"]),
        ]:
            cf = compile_post_filter(
                {"status": {op: operand}},
                schema,
                time_field="recordedAt",
                strip_time_field=False,
            )
            assert isinstance(cf.typed_ast, Leaf)
            assert cf.typed_ast.op == op

    def test_comparison_ops_orderable(self):
        schema = _base_schema()
        for op in ["$gt", "$gte", "$lt", "$lte"]:
            cf = compile_post_filter(
                {"count": {op: 5}},
                schema,
                time_field="recordedAt",
                strip_time_field=False,
            )
            assert isinstance(cf.typed_ast, Leaf)
            assert cf.typed_ast.op == op

    def test_and_or_nor_not(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {
                "$and": [
                    {"status": "active"},
                    {"$or": [{"count": 1}, {"count": 2}]},
                    {"$nor": [{"active": False}]},
                    {"count": {"$not": {"$eq": 99}}},
                ]
            },
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        # Should produce a compact AndNode at the root.
        assert isinstance(cf.typed_ast, AndNode)

    def test_nor_rewrites_to_and_of_nots(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {"$nor": [{"status": "active"}, {"count": 1}]},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        # And(Not(A), Not(B))
        assert isinstance(cf.typed_ast, AndNode)
        assert all(isinstance(c, NotNode) for c in cf.typed_ast.children)


class TestParserErrors:
    def test_unsupported_operator_raises(self):
        schema = _base_schema()
        bad_ops = ["$regex", "$exists", "$type", "$size", "$all", "$elemMatch", "$mod"]
        for bad in bad_ops:
            escaped = bad.replace("$", r"\$")
            with pytest.raises(XLR8FilterError, match=escaped):
                compile_post_filter(
                    {"status": {bad: "anything"}},
                    schema,
                    time_field="recordedAt",
                    strip_time_field=False,
                )

    def test_unsupported_top_level_logical(self):
        schema = _base_schema()
        with pytest.raises(XLR8FilterError, match="logical operator"):
            compile_post_filter(
                {"$expr": {"$eq": ["$a", "$b"]}},
                schema,
                time_field="recordedAt",
                strip_time_field=False,
            )

    def test_unknown_field_raises(self):
        schema = _base_schema()
        with pytest.raises(XLR8FilterError, match="not declared in the schema"):
            compile_post_filter(
                {"does_not_exist": 1},
                schema,
                time_field="recordedAt",
                strip_time_field=False,
            )

    def test_comparison_on_any_raises(self):
        schema = _base_schema()
        with pytest.raises(XLR8FilterError, match="comparison operators on Types.Any"):
            compile_post_filter(
                {"value": {"$gt": 5}},
                schema,
                time_field="recordedAt",
                strip_time_field=False,
            )

    def test_top_level_not_raises(self):
        schema = _base_schema()
        with pytest.raises(XLR8FilterError, match=r"\$not"):
            compile_post_filter(
                {"$not": {"status": "active"}},
                schema,
                time_field="recordedAt",
                strip_time_field=False,
            )

    def test_bare_list_rejected(self):
        schema = _base_schema()
        with pytest.raises(XLR8FilterError, match="bare list"):
            compile_post_filter(
                {"status": ["a", "b"]},
                schema,
                time_field="recordedAt",
                strip_time_field=False,
            )

    def test_empty_or_raises(self):
        schema = _base_schema()
        with pytest.raises(XLR8FilterError, match=r"\$or"):
            compile_post_filter(
                {"$or": []},
                schema,
                time_field="recordedAt",
                strip_time_field=False,
            )

    def test_none_filter_returns_empty(self):
        schema = _base_schema()
        cf = compile_post_filter(
            None, schema, time_field="recordedAt", strip_time_field=False
        )
        assert not cf.has_typed and not cf.has_any


# ---------------------------------------------------------------------------
# SECTION 3: Time-field stripping
# ---------------------------------------------------------------------------


class TestTimeFieldStrip:
    def test_strip_time_in_top_level_and(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {
                "recordedAt": {"$gte": datetime(2024, 1, 1, tzinfo=timezone.utc)},
                "status": "active",
            },
            schema,
            time_field="recordedAt",
            strip_time_field=True,
        )
        # Only status clause survives.
        assert isinstance(cf.typed_ast, Leaf)
        assert cf.typed_ast.field == "status"

    def test_strip_time_inside_explicit_and(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {
                "$and": [
                    {"recordedAt": {"$gte": datetime(2024, 1, 1, tzinfo=timezone.utc)}},
                    {"status": "active"},
                ]
            },
            schema,
            time_field="recordedAt",
            strip_time_field=True,
        )
        assert isinstance(cf.typed_ast, Leaf)
        assert cf.typed_ast.field == "status"

    def test_time_clause_in_or_raises_when_stripping(self):
        schema = _base_schema()
        t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(XLR8FilterError, match="conflicts|non-conjunctive"):
            compile_post_filter(
                {
                    "$or": [
                        {"recordedAt": {"$gte": t0}},
                        {"status": "active"},
                    ]
                },
                schema,
                time_field="recordedAt",
                strip_time_field=True,
            )

    def test_time_clause_preserved_when_not_stripping(self):
        """Without start_date/end_date, time-field clauses flow through normally."""
        schema = _base_schema()
        cf = compile_post_filter(
            {"recordedAt": {"$gte": datetime(2024, 1, 1, tzinfo=timezone.utc)}},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        assert isinstance(cf.typed_ast, Leaf)
        assert cf.typed_ast.field == "recordedAt"


# ---------------------------------------------------------------------------
# SECTION 4: Typed / Any split
# ---------------------------------------------------------------------------


class TestSplit:
    def test_pure_typed(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {"status": "active", "count": 5},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        assert cf.has_typed
        assert not cf.has_any

    def test_pure_any(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {"value": 42},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        assert not cf.has_typed
        assert cf.has_any

    def test_and_split(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {"status": "active", "value": 42},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        assert cf.has_typed
        assert cf.has_any

    def test_or_with_any_all_goes_to_any(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {"$or": [{"status": "active"}, {"value": 42}]},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        assert not cf.has_typed
        assert cf.has_any

    def test_not_with_any_goes_to_any(self):
        schema = _base_schema()
        cf = compile_post_filter(
            {"value": {"$not": {"$eq": 99}}},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        assert not cf.has_typed
        assert cf.has_any


# ---------------------------------------------------------------------------
# SECTION 5: Reader integration — translators agree + end-to-end
# ---------------------------------------------------------------------------


def _write_fixture_parquet(
    dir_: Path, schema: Schema
) -> tuple[ObjectId, ObjectId, ObjectId]:
    """Write a 4-row fixture matching what the Rust backend would emit."""
    oid1, oid2, oid3 = ObjectId(), ObjectId(), ObjectId()
    tbl = pa.table(
        {
            "recordedAt": pa.array(
                [
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, tzinfo=timezone.utc),
                    datetime(2024, 1, 3, tzinfo=timezone.utc),
                    datetime(2024, 1, 4, tzinfo=timezone.utc),
                ],
                type=pa.timestamp("ms", tz="UTC"),
            ),
            "metadata.sensor_id": pa.array(
                [str(oid1), str(oid2), str(oid1), str(oid3)], type=pa.string()
            ),
            "metadata.device_id": pa.array(
                [str(oid1), str(oid1), str(oid2), str(oid3)], type=pa.string()
            ),
            "status": pa.array(
                ["active", "inactive", "active", "active"], type=pa.string()
            ),
            "count": pa.array([1, 2, 3, 4], type=pa.int64()),
            "ratio": pa.array([0.1, 0.2, 0.3, 0.4], type=pa.float64()),
            "active": pa.array([True, False, True, True], type=pa.bool_()),
            "value": pa.array(
                [
                    _any_struct(float_value=1.0),
                    _any_struct(string_value="hello"),
                    _any_struct(float_value=3.14),
                    _any_struct(string_value="world"),
                ],
                type=schema.fields["value"].to_arrow(),
            ),
        }
    )
    pq.write_table(tbl, dir_ / "ts_1_100_part_0000.parquet")
    return oid1, oid2, oid3


def _any_struct(**kwargs):
    """Build a 13-field Any-struct dict with the one field populated."""
    base = {
        "float_value": None,
        "int32_value": None,
        "int64_value": None,
        "string_value": None,
        "objectid_value": None,
        "decimal128_value": None,
        "regex_value": None,
        "binary_value": None,
        "document_value": None,
        "array_value": None,
        "bool_value": None,
        "datetime_value": None,
        "null_value": None,
    }
    base.update(kwargs)
    return base


class TestReaderIntegration:
    def test_pandas_engine_typed_eq(self, tmp_path: Path):
        schema = _base_schema()
        oid1, _, _ = _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        cf = compile_post_filter(
            {"metadata.sensor_id": oid1},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        df = reader.to_dataframe(
            engine="pandas",
            schema=schema,
            time_field="recordedAt",
            post_filter=cf,
        )
        assert len(df) == 2
        assert (df["metadata.sensor_id"] == oid1).all()

    def test_pandas_engine_typed_in(self, tmp_path: Path):
        schema = _base_schema()
        oid1, _, oid3 = _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        cf = compile_post_filter(
            {"metadata.sensor_id": {"$in": [oid1, oid3]}},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        df = reader.to_dataframe(
            engine="pandas",
            schema=schema,
            time_field="recordedAt",
            post_filter=cf,
        )
        assert len(df) == 3

    def test_polars_engine_typed(self, tmp_path: Path):
        schema = _base_schema()
        oid1, _, oid3 = _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        cf = compile_post_filter(
            {"metadata.sensor_id": {"$in": [oid1, oid3]}},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        df = reader.to_dataframe(
            engine="polars",
            schema=schema,
            time_field="recordedAt",
            post_filter=cf,
        )
        assert df.shape[0] == 3

    def test_hybrid_any_and_typed(self, tmp_path: Path):
        """Typed field pushed down via PyArrow, Any() applied post-decode."""
        schema = _base_schema()
        _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        cf = compile_post_filter(
            {"status": "active", "value": 3.14},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        df = reader.to_dataframe(
            engine="pandas",
            schema=schema,
            time_field="recordedAt",
            post_filter=cf,
        )
        assert len(df) == 1
        assert df.iloc[0]["status"] == "active"

    def test_sorted_path_with_filter(self, tmp_path: Path):
        """DuckDB sorted path must honor both sort and filter."""
        schema = _base_schema()
        _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        cf = compile_post_filter(
            {"status": "active"},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        df = reader.get_globally_sorted_dataframe(
            sort_spec=[("recordedAt", -1)],  # desc
            schema=schema,
            time_field="recordedAt",
            post_filter=cf,
        )
        assert len(df) == 3
        # All active, sorted desc by time.
        assert (df["status"] == "active").all()
        ts_col = df["recordedAt"].tolist()
        assert ts_col == sorted(ts_col, reverse=True)

    def test_start_date_overrides_time_clause_in_filter(self, tmp_path: Path):
        schema = _base_schema()
        _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        # User's filter asks for >= Jan 3 on time field, but start_date=Jan 2 wins.
        cf = compile_post_filter(
            {"recordedAt": {"$gte": datetime(2024, 1, 3, tzinfo=timezone.utc)}},
            schema,
            time_field="recordedAt",
            strip_time_field=True,
        )
        df = reader.to_dataframe(
            engine="pandas",
            schema=schema,
            time_field="recordedAt",
            start_date=datetime(2024, 1, 2, tzinfo=timezone.utc),
            post_filter=cf,
        )
        # start_date=Jan 2 includes rows 2,3,4.
        assert len(df) == 3

    def test_filter_none_is_no_op(self, tmp_path: Path):
        schema = _base_schema()
        _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        # No post_filter kwarg: baseline behavior.
        df_a = reader.to_dataframe(
            engine="pandas", schema=schema, time_field="recordedAt"
        )
        # Explicit None: same result.
        cf = compile_post_filter(
            None, schema, time_field="recordedAt", strip_time_field=False
        )
        df_b = reader.to_dataframe(
            engine="pandas",
            schema=schema,
            time_field="recordedAt",
            post_filter=cf,
        )
        assert len(df_a) == len(df_b) == 4

    def test_batches_with_filter(self, tmp_path: Path):
        schema = _base_schema()
        oid1, _, _ = _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        cf = compile_post_filter(
            {"metadata.sensor_id": oid1},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        batches = list(
            reader.iter_dataframe_batches(
                batch_size=10,
                schema=schema,
                time_field="recordedAt",
                post_filter=cf,
            )
        )
        total = sum(len(b) for b in batches)
        assert total == 2

    def test_duckdb_batches_with_filter(self, tmp_path: Path):
        schema = _base_schema()
        _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        cf = compile_post_filter(
            {"status": "active"},
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )
        batches = list(
            reader.iter_globally_sorted_batches(
                sort_spec=[("recordedAt", 1)],
                batch_size=2,
                schema=schema,
                time_field="recordedAt",
                post_filter=cf,
            )
        )
        total = sum(len(b) for b in batches)
        assert total == 3


class TestTranslatorsAgree:
    """The same filter evaluated via polars / PyArrow / DuckDB paths must
    return the same row set."""

    def test_pandas_polars_duckdb_agree(self, tmp_path: Path):
        schema = _base_schema()
        _write_fixture_parquet(tmp_path, schema)
        reader = ParquetReader(tmp_path)
        cf = compile_post_filter(
            {
                "$or": [
                    {"status": "active", "count": {"$gte": 3}},
                    {"active": False},
                ]
            },
            schema,
            time_field="recordedAt",
            strip_time_field=False,
        )

        pandas_df = reader.to_dataframe(
            engine="pandas", schema=schema, time_field="recordedAt", post_filter=cf
        )
        polars_df = reader.to_dataframe(
            engine="polars", schema=schema, time_field="recordedAt", post_filter=cf
        )
        duckdb_df = reader.get_globally_sorted_dataframe(
            sort_spec=[("recordedAt", 1)],
            schema=schema,
            time_field="recordedAt",
            post_filter=cf,
        )

        # All three return the same rows (compare by stable key).
        pandas_keys = sorted(pandas_df["metadata.sensor_id"].astype(str).tolist())
        polars_keys = sorted(polars_df["metadata.sensor_id"].to_list())
        duckdb_keys = sorted(duckdb_df["metadata.sensor_id"].astype(str).tolist())
        assert pandas_keys == polars_keys == duckdb_keys
