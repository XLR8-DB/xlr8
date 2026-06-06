"""
Tests for CacheHandler and CacheCursor (cache_handler.py).

Tests cover:
- CacheHandler initialization and metadata
- CacheCursor creation via find()
- Output methods: to_dataframe, to_polars, to_dataframe_batches
- Chaining: sort, limit, skip, projection
- Filter correctness
- Edge cases (empty results, missing fields, operators on Any fields)
- Explain mode
- stream_to_callback
"""

import json
import pytest
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId

from xlr8.storage.cache_handler import (
    CacheHandler,
    CacheCursor,
    _inline_params,
    _quote_literal,
)
from xlr8.schema import Schema, Types


# ────────────────────────────────────────────────────────────────
# Test fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def simple_schema():
    """Schema with typed fields."""
    return Schema(
        time_field="timestamp",
        fields={
            "timestamp": Types.Timestamp("ms", tz="UTC"),
            "sensor_id": Types.String(),
            "value": Types.Float(),
            "status": Types.String(),
            "count": Types.Int(),
            "active": Types.Bool(),
        },
        avg_doc_size_bytes=200,
    )


@pytest.fixture
def any_schema():
    """Schema with Types.Any() field."""
    return Schema(
        time_field="timestamp",
        fields={
            "timestamp": Types.Timestamp("ms", tz="UTC"),
            "sensor_id": Types.String(),
            "value": Types.Any(),
            "status": Types.String(),
        },
        avg_doc_size_bytes=200,
    )


@pytest.fixture
def sample_parquet_cache(simple_schema, tmp_path):
    """Create a test Parquet cache with known data."""
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()

    # Create test data
    timestamps = [
        datetime(2024, 1, 15, tzinfo=timezone.utc),
        datetime(2024, 2, 15, tzinfo=timezone.utc),
        datetime(2024, 3, 15, tzinfo=timezone.utc),
        datetime(2024, 4, 15, tzinfo=timezone.utc),
        datetime(2024, 5, 15, tzinfo=timezone.utc),
        datetime(2024, 6, 15, tzinfo=timezone.utc),
    ]
    sensors = ["temp_001", "temp_001", "temp_002", "temp_002", "temp_003", "temp_003"]
    values = [10.5, 20.3, 30.1, 40.7, 50.2, 60.9]
    statuses = ["active", "active", "active", "inactive", "active", "inactive"]
    counts = [1, 2, 3, 4, 5, 6]

    table = pa.table({
        "timestamp": pa.array(timestamps, type=pa.timestamp("ms", tz="UTC")),
        "sensor_id": pa.array(sensors, type=pa.string()),
        "value": pa.array(values, type=pa.float64()),
        "status": pa.array(statuses, type=pa.string()),
        "count": pa.array(counts, type=pa.int64()),
        "active": pa.array([True, True, False, False, True, False], type=pa.bool_()),
    })

    pq.write_table(table, cache_dir / "test_part_0000.parquet")
    return cache_dir


@pytest.fixture
def any_parquet_cache(any_schema, tmp_path):
    """Create a test Parquet cache with Types.Any() field data."""
    cache_dir = tmp_path / "any_cache"
    cache_dir.mkdir()

    # Build the Any struct type
    any_struct_type = pa.struct([
        ("float_value", pa.float64()),
        ("int32_value", pa.int32()),
        ("int64_value", pa.int64()),
        ("string_value", pa.string()),
        ("objectid_value", pa.string()),
        ("decimal128_value", pa.string()),
        ("regex_value", pa.string()),
        ("binary_value", pa.string()),
        ("document_value", pa.string()),
        ("array_value", pa.string()),
        ("bool_value", pa.bool_()),
        ("datetime_value", pa.timestamp("ms")),
        ("null_value", pa.bool_()),
    ])

    timestamps = [
        datetime(2024, 1, 15, tzinfo=timezone.utc),
        datetime(2024, 2, 15, tzinfo=timezone.utc),
        datetime(2024, 3, 15, tzinfo=timezone.utc),
    ]

    # Create Any values manually as structs
    def make_any_float(v):
        return {
            "float_value": float(v),
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

    def make_any_string(v):
        return {
            "float_value": None,
            "int32_value": None,
            "int64_value": None,
            "string_value": str(v),
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

    def make_any_null():
        return {
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
            "null_value": True,
        }

    any_value = pa.array(
        [make_any_float(10.5), make_any_string("hello"), make_any_null()],
        type=any_struct_type,
    )

    table = pa.table({
        "timestamp": pa.array(timestamps, type=pa.timestamp("ms", tz="UTC")),
        "sensor_id": pa.array(["A", "B", "C"], type=pa.string()),
        "value": any_value,
        "status": pa.array(["active", "inactive", "active"], type=pa.string()),
    })

    pq.write_table(table, cache_dir / "any_part_0000.parquet")
    return cache_dir


@pytest.fixture
def handler(simple_schema, sample_parquet_cache):
    """Create a CacheHandler from the sample cache."""
    return CacheHandler(
        cache_dir=sample_parquet_cache,
        schema=simple_schema,
    )


@pytest.fixture
def any_handler(any_schema, any_parquet_cache):
    """Create a CacheHandler from the Any-type cache."""
    return CacheHandler(
        cache_dir=any_parquet_cache,
        schema=any_schema,
    )


# ────────────────────────────────────────────────────────────────
# CacheHandler initialization
# ────────────────────────────────────────────────────────────────


class TestCacheHandlerInit:
    def test_from_existing_cache(self, sample_parquet_cache, simple_schema):
        handler = CacheHandler(sample_parquet_cache, simple_schema)
        assert handler.file_count == 1
        assert handler.cache_size_mb > 0

    def test_missing_directory_raises(self, simple_schema):
        with pytest.raises(FileNotFoundError):
            CacheHandler("/nonexistent/path", simple_schema)

    def test_empty_directory_raises(self, simple_schema, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        with pytest.raises(ValueError, match="No Parquet files"):
            CacheHandler(empty_dir, simple_schema)

    def test_file_count(self, handler):
        assert handler.file_count == 1

    def test_cache_size_mb(self, handler):
        assert handler.cache_size_mb > 0

    def test_get_metadata(self, handler):
        meta = handler.get_metadata()
        assert "cache_dir" in meta
        assert "file_count" in meta
        assert meta["file_count"] == 1
        assert "time_field" in meta
        assert meta["time_field"] == "timestamp"
        assert "schema_fields" in meta

    def test_time_field_property(self, handler):
        assert handler.time_field == "timestamp"

    def test_repr(self, handler):
        r = repr(handler)
        assert "CacheHandler" in r


# ────────────────────────────────────────────────────────────────
# CacheCursor creation and chaining
# ────────────────────────────────────────────────────────────────


class TestCacheCursorBasic:
    def test_find_creates_cursor(self, handler):
        cursor = handler.find({"status": "active"})
        assert isinstance(cursor, CacheCursor)

    def test_find_empty_query(self, handler):
        cursor = handler.find({})
        assert isinstance(cursor, CacheCursor)

    def test_find_no_args(self, handler):
        cursor = handler.find()
        assert isinstance(cursor, CacheCursor)


class TestCacheCursorChaining:
    def test_sort_string(self, handler):
        cursor = handler.find().sort("timestamp", -1)
        assert cursor._sort == [("timestamp", -1)]

    def test_sort_list(self, handler):
        cursor = handler.find().sort([("timestamp", 1), ("value", -1)])
        assert len(cursor._sort) == 2

    def test_limit(self, handler):
        cursor = handler.find().limit(10)
        assert cursor._limit == 10

    def test_skip(self, handler):
        cursor = handler.find().skip(5)
        assert cursor._skip == 5

    def test_projection(self, handler):
        cursor = handler.find().projection({"sensor_id": 1, "value": 1})
        assert cursor._projection == {"sensor_id": 1, "value": 1}

    def test_chaining(self, handler):
        cursor = (
            handler.find({"status": "active"})
            .sort("timestamp", -1)
            .limit(10)
            .skip(5)
        )
        assert cursor._limit == 10
        assert cursor._skip == 5
        assert cursor._sort == [("timestamp", -1)]

    def test_cannot_modify_after_started(self, handler):
        cursor = handler.find()
        cursor._started = True
        with pytest.raises(RuntimeError):
            cursor.sort("timestamp", 1)
        with pytest.raises(RuntimeError):
            cursor.limit(10)
        with pytest.raises(RuntimeError):
            cursor.skip(5)

    def test_repr(self, handler):
        cursor = handler.find({"status": "active"}).sort("ts", -1).limit(10)
        r = repr(cursor)
        assert "CacheCursor" in r


# ────────────────────────────────────────────────────────────────
# Output methods: to_dataframe
# ────────────────────────────────────────────────────────────────


class TestCacheCursorToDataFrame:
    def test_to_dataframe_basic(self, handler):
        df = handler.find().to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 6

    def test_to_dataframe_with_filter(self, handler):
        df = handler.find({"sensor_id": "temp_001"}).to_dataframe()
        assert len(df) == 2
        assert all(df["sensor_id"] == "temp_001")

    def test_to_dataframe_gt_filter(self, handler):
        df = handler.find({"value": {"$gt": 30}}).to_dataframe()
        assert len(df) > 0
        assert all(df["value"] > 30)

    def test_to_dataframe_in_filter(self, handler):
        df = handler.find({"sensor_id": {"$in": ["temp_001", "temp_002"]}}).to_dataframe()
        assert len(df) == 4
        assert all(df["sensor_id"].isin(["temp_001", "temp_002"]))

    def test_to_dataframe_and_filter(self, handler):
        df = handler.find({
            "status": "active",
            "value": {"$gt": 20},
        }).to_dataframe()
        assert len(df) > 0
        assert all(df["status"] == "active")
        assert all(df["value"] > 20)

    def test_to_dataframe_or_filter(self, handler):
        df = handler.find({
            "$or": [
                {"sensor_id": "temp_001"},
                {"status": "inactive"},
            ]
        }).to_dataframe()
        assert len(df) > 0
        assert all(
            (df["sensor_id"] == "temp_001") | (df["status"] == "inactive")
        )

    def test_to_dataframe_empty_result(self, handler):
        """Filter that matches no documents should return empty DataFrame."""
        df = handler.find({"sensor_id": "nonexistent"}).to_dataframe()
        assert len(df) == 0

    def test_to_dataframe_with_sort(self, handler):
        df = handler.find().sort("value", 1).to_dataframe()
        assert len(df) == 6
        # Check values are sorted ascending
        values = df["value"].tolist()
        assert values == sorted(values)

    def test_to_dataframe_with_limit(self, handler):
        df = handler.find().limit(3).to_dataframe()
        assert len(df) == 3

    def test_to_dataframe_with_skip(self, handler):
        df = handler.find().skip(2).to_dataframe()
        assert len(df) == 4

    def test_to_dataframe_with_skip_and_limit(self, handler):
        df = handler.find().sort("value", 1).skip(1).limit(2).to_dataframe()
        assert len(df) == 2
        # Should be second and third smallest values
        all_values = [10.5, 20.3, 30.1, 40.7, 50.2, 60.9]
        expected = sorted(all_values)[1:3]
        assert df["value"].tolist() == expected

    def test_to_dataframe_limit_beyond_data(self, handler):
        df = handler.find().limit(100).to_dataframe()
        assert len(df) == 6  # All data returned

    def test_to_dataframe_skip_beyond_data(self, handler):
        df = handler.find().skip(100).to_dataframe()
        assert len(df) == 0

    def test_to_dataframe_with_date_range(self, handler):
        df = handler.find().to_dataframe(
            start_date=datetime(2024, 3, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
        )
        assert len(df) > 0
        # All timestamps should be in range
        for ts in df["timestamp"]:
            assert ts >= datetime(2024, 3, 1, tzinfo=timezone.utc)
            assert ts < datetime(2024, 6, 1, tzinfo=timezone.utc)

    def test_to_dataframe_combined_filter_and_dates(self, handler):
        df = handler.find({"status": "active"}).to_dataframe(
            start_date=datetime(2024, 3, 1, tzinfo=timezone.utc),
        )
        assert all(df["status"] == "active")


# ────────────────────────────────────────────────────────────────
# Output methods: to_polars
# ────────────────────────────────────────────────────────────────


class TestCacheCursorToPolars:
    def test_to_polars_basic(self, handler):
        df = handler.find().to_polars()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 6

    def test_to_polars_with_filter(self, handler):
        df = handler.find({"sensor_id": "temp_001"}).to_polars()
        assert len(df) == 2
        assert (df["sensor_id"] == "temp_001").all()

    def test_to_polars_empty_result(self, handler):
        df = handler.find({"sensor_id": "nonexistent"}).to_polars()
        assert len(df) == 0


# ────────────────────────────────────────────────────────────────
# Output methods: to_dataframe_batches
# ────────────────────────────────────────────────────────────────


class TestCacheCursorBatches:
    def test_to_dataframe_batches(self, handler):
        batches = list(handler.find().to_dataframe_batches(batch_size=2))
        assert len(batches) == 3  # 6 rows / 2 = 3 batches
        total_rows = sum(len(b) for b in batches)
        assert total_rows == 6

    def test_to_dataframe_batches_with_filter(self, handler):
        batches = list(handler.find(
            {"sensor_id": "temp_001"}
        ).to_dataframe_batches(batch_size=1))
        assert len(batches) == 2  # 2 rows / 1 = 2 batches
        for b in batches:
            assert all(b["sensor_id"] == "temp_001")

    def test_to_dataframe_batches_empty(self, handler):
        batches = list(handler.find(
            {"sensor_id": "nonexistent"}
        ).to_dataframe_batches())
        assert len(batches) == 0


# ────────────────────────────────────────────────────────────────
# Explain mode
# ────────────────────────────────────────────────────────────────


class TestCacheCursorExplain:
    def test_explain_shows_sql(self, handler):
        info = handler.find({"status": "active"}).explain()
        assert "sql" in info
        assert "cache_files" in info
        assert len(info["cache_files"]) == 1

    def test_explain_includes_metadata(self, handler):
        info = handler.find({"value": {"$gt": 50}}).sort("value", -1).limit(5).explain()
        assert info["sort"] == [("value", -1)]
        assert info["limit"] == 5
        assert info["filter"] == {"value": {"$gt": 50}}


# ────────────────────────────────────────────────────────────────
# Any-type field handling
# ────────────────────────────────────────────────────────────────


class TestCacheCursorAnyType:
    def test_any_type_to_dataframe(self, any_handler):
        df = any_handler.find().to_dataframe()
        assert len(df) == 3
        assert "value" in df.columns

    def test_any_type_filter_numeric(self, any_handler):
        df = any_handler.find({"value": {"$eq": 10.5}}).to_dataframe()
        assert len(df) == 1

    def test_any_type_filter_string(self, any_handler):
        df = any_handler.find({"value": {"$eq": "hello"}}).to_dataframe()
        assert len(df) == 1


# ────────────────────────────────────────────────────────────────
# Edge cases
# ────────────────────────────────────────────────────────────────


class TestCacheCursorEdgeCases:
    def test_empty_filter_returns_all(self, handler):
        df = handler.find({}).to_dataframe()
        assert len(df) == 6

    def test_regex_filter(self, handler):
        df = handler.find({"sensor_id": {"$regex": "^temp_"}}).to_dataframe()
        assert len(df) == 6

    def test_regex_filter_specific(self, handler):
        df = handler.find({"sensor_id": {"$regex": "001$"}}).to_dataframe()
        assert len(df) == 2

    def test_exists_filter(self, handler):
        df = handler.find({"status": {"$exists": True}}).to_dataframe()
        assert len(df) == 6

    def test_not_equal_filter(self, handler):
        df = handler.find({"sensor_id": {"$ne": "temp_001"}}).to_dataframe()
        assert len(df) == 4
        assert "temp_001" not in df["sensor_id"].values

    def test_nin_filter(self, handler):
        df = handler.find({"sensor_id": {"$nin": ["temp_001", "temp_002"]}}).to_dataframe()
        assert len(df) == 2
        assert all(s == "temp_003" for s in df["sensor_id"])

    def test_sort_descending(self, handler):
        df = handler.find().sort("value", -1).to_dataframe()
        values = df["value"].tolist()
        assert values == sorted(values, reverse=True)

    def test_missing_field_in_filter_raises(self, handler):
        """Filter referencing a field not in the Parquet schema raises an error."""
        from xlr8.storage.mql_filter import translate_mql_to_sql

        # The translator generates SQL — DuckDB will reject unknown columns
        # This is expected: the field truly doesn't exist in the cached data
        with pytest.raises(Exception):
            handler.find({"nonexistent_column_xyz": "value"}).to_dataframe()


# ────────────────────────────────────────────────────────────────
# Multiple Parquet files
# ────────────────────────────────────────────────────────────────


class TestMultipleParquetFiles:
    def test_multiple_files(self, simple_schema, tmp_path):
        """CacheHandler should handle multiple Parquet files correctly."""
        cache_dir = tmp_path / "multi_cache"
        cache_dir.mkdir()

        # Write two Parquet files with different data
        t1 = datetime(2024, 1, 15, tzinfo=timezone.utc)
        t2 = datetime(2024, 7, 15, tzinfo=timezone.utc)

        table1 = pa.table({
            "timestamp": pa.array([t1], type=pa.timestamp("ms", tz="UTC")),
            "sensor_id": pa.array(["A"], type=pa.string()),
            "value": pa.array([1.0], type=pa.float64()),
            "status": pa.array(["active"], type=pa.string()),
            "count": pa.array([1], type=pa.int64()),
            "active": pa.array([True], type=pa.bool_()),
        })
        table2 = pa.table({
            "timestamp": pa.array([t2], type=pa.timestamp("ms", tz="UTC")),
            "sensor_id": pa.array(["B"], type=pa.string()),
            "value": pa.array([2.0], type=pa.float64()),
            "status": pa.array(["inactive"], type=pa.string()),
            "count": pa.array([2], type=pa.int64()),
            "active": pa.array([False], type=pa.bool_()),
        })

        pq.write_table(table1, cache_dir / "part_0000.parquet")
        pq.write_table(table2, cache_dir / "part_0001.parquet")

        handler = CacheHandler(cache_dir, simple_schema)
        assert handler.file_count == 2

        df = handler.find().to_dataframe()
        assert len(df) == 2

        # Filter should work across files
        df_a = handler.find({"sensor_id": "A"}).to_dataframe()
        assert len(df_a) == 1
