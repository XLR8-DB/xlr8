"""
Tests for xlr8.execution.executor module.

Covers:
- serialize_chunks_for_rust BSON encoding
- Bracket building and boundary operator preservation
- Boundary data validation
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, cast

from bson import ObjectId
from bson import decode as bson_decode

from xlr8.execution.executor import serialize_chunks_for_rust


class TestSerializeChunksForRust:
    """Test serialize_chunks_for_rust BSON encoding."""

    def test_converts_chunks_to_bson_bytes(self):
        """serialize_chunks_for_rust() should convert chunks to BSON bytes."""
        chunks = [
            (
                {"sensor_id": ObjectId("64a1b2c3d4e5f6a7b8c9d0e1")},
                0,
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 15, tzinfo=timezone.utc),
            ),
        ]

        bson_bytes = serialize_chunks_for_rust(chunks)

        assert isinstance(bson_bytes, bytes)
        assert len(bson_bytes) > 0

    def test_preserves_objectid(self):
        """serialize_chunks_for_rust() should preserve ObjectId in BSON."""
        oid = ObjectId("64a1b2c3d4e5f6a7b8c9d0e1")
        chunks = [
            (
                {"sensor_id": oid},
                0,
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 15, tzinfo=timezone.utc),
            ),
        ]

        bson_bytes = serialize_chunks_for_rust(chunks)
        decoded = cast(Dict[str, Any], bson_decode(bson_bytes))

        # Should have chunks key
        assert "chunks" in decoded
        assert len(decoded["chunks"]) == 1
        # ObjectId should be preserved
        assert decoded["chunks"][0]["filter"]["sensor_id"] == oid

    def test_handles_datetime_to_milliseconds(self):
        """serialize_chunks_for_rust() should convert datetime to milliseconds."""
        chunks = [
            (
                {},
                0,
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 15, tzinfo=timezone.utc),
            ),
        ]

        bson_bytes = serialize_chunks_for_rust(chunks)
        decoded = cast(Dict[str, Any], bson_decode(bson_bytes))

        # Timestamps should be present
        assert "start_ms" in decoded["chunks"][0]
        assert "end_ms" in decoded["chunks"][0]
        assert isinstance(decoded["chunks"][0]["start_ms"], int)

    def test_handles_unbounded_queries_none_timestamps(self):
        """Handle None timestamps for unbounded queries."""
        chunks = [
            (
                {},
                0,
                None,  # Unbounded start
                None,  # Unbounded end
            ),
        ]

        bson_bytes = serialize_chunks_for_rust(chunks)
        decoded = cast(Dict[str, Any], bson_decode(bson_bytes))

        # None timestamps should be handled (might be null or omitted)
        chunk = decoded["chunks"][0]
        # Either not present or null
        assert chunk.get("start_ms") is None or "start_ms" not in chunk
        assert chunk.get("end_ms") is None or "end_ms" not in chunk

    def test_multiple_chunks(self):
        """serialize_chunks_for_rust() should handle multiple chunks."""
        chunks = [
            (
                {"sensor_id": ObjectId()},
                0,
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 15, tzinfo=timezone.utc),
            ),
            (
                {"sensor_id": ObjectId()},
                1,
                datetime(2024, 1, 15, tzinfo=timezone.utc),
                datetime(2024, 2, 1, tzinfo=timezone.utc),
            ),
            (
                {"sensor_id": ObjectId()},
                2,
                datetime(2024, 2, 1, tzinfo=timezone.utc),
                datetime(2024, 2, 15, tzinfo=timezone.utc),
            ),
        ]

        bson_bytes = serialize_chunks_for_rust(chunks)
        decoded = cast(Dict[str, Any], bson_decode(bson_bytes))

        assert len(decoded["chunks"]) == 3
        # Chunk indices should be preserved
        assert decoded["chunks"][0]["chunk_idx"] == 0
        assert decoded["chunks"][1]["chunk_idx"] == 1
        assert decoded["chunks"][2]["chunk_idx"] == 2


class TestExecuteParallelStreamToCache:
    """Test execute_parallel_stream_to_cache orchestration."""

    def test_serialize_chunks_integration(self):
        """Test that chunks can be serialized for Rust backend."""
        # This test verifies the chunk serialization works
        chunks = [
            (
                {"sensor_id": ObjectId("64a1b2c3d4e5f6a7b8c9d0e1")},
                0,
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 15, tzinfo=timezone.utc),
            ),
            (
                {"sensor_id": ObjectId("64a1b2c3d4e5f6a7b8c9d0e1")},
                1,
                datetime(2024, 1, 15, tzinfo=timezone.utc),
                datetime(2024, 2, 1, tzinfo=timezone.utc),
            ),
        ]

        bson_bytes = serialize_chunks_for_rust(chunks)

        # Should be valid BSON
        assert isinstance(bson_bytes, bytes)
        assert len(bson_bytes) > 0

        # Should be decodable
        decoded = cast(Dict[str, Any], bson_decode(bson_bytes))
        assert "chunks" in decoded
        assert len(decoded["chunks"]) == 2


class TestBoundaryOperatorChunkConstruction:
    """
    Test that chunk filter construction preserves boundary operators correctly.

    This is CRITICAL to prevent data loss:
    - $lte on the upper bound should be preserved on the LAST chunk
    - $gt on the lower bound should be preserved on the FIRST chunk
    - Intermediate chunks always use $gte/$lt for no-overlap guarantee
    """

    def test_lte_preserved_in_last_chunk_filter(self):
        """
        When user query has $lte, the last chunk filter should use $lte not $lt.

        This prevents the data loss bug where documents at exact end boundary
        are excluded when query uses $lte.
        """
        from xlr8.analysis.brackets import build_brackets_for_find
        from xlr8.analysis.chunker import chunk_time_range

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 10, tzinfo=timezone.utc)

        # Query with $lte - inclusive upper bound
        query = {"sensor_id": "sensor_1", "timestamp": {"$gte": t1, "$lte": t2}}

        is_chunkable, _reason, brackets, _bounds = build_brackets_for_find(
            query, "timestamp"
        )
        assert is_chunkable is True
        bracket = brackets[0]

        # Key assertion: hi_inclusive should be True for $lte
        assert bracket.timerange.hi_inclusive is True, (
            "Bracket should preserve $lte as hi_inclusive=True"
        )

        # Simulate chunk creation for last chunk
        chunks = chunk_time_range(start=t1, end=t2, chunk_size=timedelta(days=3))
        last_chunk_start, last_chunk_end = chunks[-1]

        # The last chunk should end exactly at t2
        assert last_chunk_end == t2, "Last chunk should end at the query boundary"

        # When building the actual filter, the LAST chunk should use $lte
        # This is what the executor does:
        is_last = last_chunk_end == t2
        assert is_last is True

        # Build filter like executor does
        time_clause = {}
        time_clause["$gte"] = last_chunk_start
        if is_last and bracket.timerange.hi_inclusive:
            time_clause["$lte"] = last_chunk_end
        else:
            time_clause["$lt"] = last_chunk_end

        # CRITICAL: Last chunk filter should have $lte, not $lt
        assert "$lte" in time_clause, (
            "Last chunk filter should use $lte when query uses $lte"
        )
        assert "$lt" not in time_clause, (
            "Last chunk filter should NOT use $lt when query uses $lte"
        )
        assert time_clause["$lte"] == t2

    def test_lt_preserved_when_query_uses_lt(self):
        """When user query has $lt (exclusive), last chunk should use $lt."""
        from xlr8.analysis.brackets import build_brackets_for_find
        from xlr8.analysis.chunker import chunk_time_range

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 10, tzinfo=timezone.utc)

        # Query with $lt - exclusive upper bound
        query = {"sensor_id": "sensor_1", "timestamp": {"$gte": t1, "$lt": t2}}

        _is_chunkable, _reason, brackets, _bounds = build_brackets_for_find(
            query, "timestamp"
        )
        bracket = brackets[0]

        # hi_inclusive should be False for $lt
        assert bracket.timerange.hi_inclusive is False, (
            "Bracket should preserve $lt as hi_inclusive=False"
        )

        chunks = chunk_time_range(start=t1, end=t2, chunk_size=timedelta(days=3))
        last_chunk_start, last_chunk_end = chunks[-1]
        is_last = last_chunk_end == t2

        # Build filter like executor does
        time_clause = {}
        time_clause["$gte"] = last_chunk_start
        if is_last and bracket.timerange.hi_inclusive:
            time_clause["$lte"] = last_chunk_end
        else:
            time_clause["$lt"] = last_chunk_end

        # Last chunk filter should have $lt (exclusive)
        assert "$lt" in time_clause, (
            "Last chunk filter should use $lt when query uses $lt"
        )
        assert "$lte" not in time_clause
        assert time_clause["$lt"] == t2

    def test_gt_preserved_in_first_chunk_unchunked(self):
        """
        When user query has $gt, unchunked bracket should use $gt not $gte.
        """
        from xlr8.analysis.brackets import build_brackets_for_find

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 10, tzinfo=timezone.utc)

        # Query with $gt - exclusive lower bound
        query = {"sensor_id": "sensor_1", "timestamp": {"$gt": t1, "$lt": t2}}

        _is_chunkable, _reason, brackets, _bounds = build_brackets_for_find(
            query, "timestamp"
        )
        bracket = brackets[0]

        # lo_inclusive should be False for $gt
        assert bracket.timerange.lo_inclusive is False, (
            "Bracket should preserve $gt as lo_inclusive=False"
        )

        # Simulate unchunked bracket filter construction (like executor does)
        time_clause = {}
        if bracket.timerange.lo is not None:
            if bracket.timerange.lo_inclusive:
                time_clause["$gte"] = bracket.timerange.lo
            else:
                time_clause["$gt"] = bracket.timerange.lo

        # Should use $gt, not $gte
        assert "$gt" in time_clause, "Filter should use $gt when query uses $gt"
        assert "$gte" not in time_clause
        assert time_clause["$gt"] == t1

    def test_gte_is_default_for_lower_bound(self):
        """Default lower bound operator should be $gte (inclusive)."""
        from xlr8.analysis.brackets import build_brackets_for_find

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 10, tzinfo=timezone.utc)

        # Query with $gte (most common)
        query = {"sensor_id": "sensor_1", "timestamp": {"$gte": t1, "$lt": t2}}

        _is_chunkable, _reason, brackets, _bounds = build_brackets_for_find(
            query, "timestamp"
        )
        bracket = brackets[0]

        # lo_inclusive should be True for $gte
        assert bracket.timerange.lo_inclusive is True, (
            "Bracket should preserve $gte as lo_inclusive=True"
        )

    def test_intermediate_chunks_always_use_gte_lt(self):
        """
        Intermediate chunks (not first, not last) should always use $gte/$lt
        regardless of original query operators, to guarantee no overlap.
        """
        from xlr8.analysis.brackets import build_brackets_for_find
        from xlr8.analysis.chunker import chunk_time_range

        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(
            2024, 1, 20, tzinfo=timezone.utc
        )  # Long range for multiple chunks

        # Query with $lte - but intermediate chunks should still use $lt
        query = {"sensor_id": "sensor_1", "timestamp": {"$gte": t1, "$lte": t2}}

        _is_chunkable, _reason, brackets, _bounds = build_brackets_for_find(
            query, "timestamp"
        )
        bracket = brackets[0]

        # Create chunks - should have at least 3 with default chunk size
        chunks = chunk_time_range(start=t1, end=t2, chunk_size=timedelta(days=5))
        assert len(chunks) >= 3, f"Expected at least 3 chunks, got {len(chunks)}"

        # Check intermediate chunk (not first, not last)
        middle_chunk_start, middle_chunk_end = chunks[1]
        is_last = middle_chunk_end == t2
        assert is_last is False, "Middle chunk should not be the last chunk"

        # Build filter for intermediate chunk
        time_clause = {}
        time_clause["$gte"] = middle_chunk_start
        if is_last and bracket.timerange.hi_inclusive:
            time_clause["$lte"] = middle_chunk_end
        else:
            time_clause["$lt"] = middle_chunk_end

        # Intermediate chunk should use $lt even though query uses $lte
        assert "$lt" in time_clause, "Intermediate chunk should use $lt for no-overlap"
        assert "$lte" not in time_clause
        # Lower bound always $gte for chunk continuity
        assert "$gte" in time_clause


class TestBoundaryDataValidation:
    """
    Validate that boundary handling prevents actual data loss.

    These tests simulate the scenario where documents exist at exact boundary
    timestamps and verify they would be included/excluded correctly.
    """

    def test_document_at_exact_lte_boundary_included(self):
        """
        Documents at exact $lte boundary timestamp should be included.

        Scenario: Query for timestamp <= 2024-01-10T00:00:00Z
        Document at timestamp = 2024-01-10T00:00:00Z should be included.
        """
        from xlr8.analysis.brackets import build_brackets_for_find

        boundary = datetime(2024, 1, 10, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)

        query = {"sensor_id": "sensor_1", "timestamp": {"$gte": t1, "$lte": boundary}}

        _is_chunkable, _reason, brackets, _bounds = build_brackets_for_find(
            query, "timestamp"
        )
        bracket = brackets[0]

        # Build the final chunk filter
        time_clause = {}
        time_clause["$gte"] = bracket.timerange.lo
        if bracket.timerange.hi_inclusive:
            time_clause["$lte"] = bracket.timerange.hi
        else:
            time_clause["$lt"] = bracket.timerange.hi

        # Simulate MongoDB matching: doc_ts <= filter["$lte"]
        doc_timestamp = boundary  # Document at exact boundary

        if "$lte" in time_clause:
            matches = doc_timestamp <= cast(datetime, time_clause["$lte"])
        else:
            matches = doc_timestamp < cast(datetime, time_clause["$lt"])

        assert matches is True, "Document at exact $lte boundary should be included"

    def test_document_at_exact_lt_boundary_excluded(self):
        """
        Documents at exact $lt boundary timestamp should be excluded.

        Scenario: Query for timestamp < 2024-01-10T00:00:00Z
        Document at timestamp = 2024-01-10T00:00:00Z should be EXCLUDED.
        """
        from xlr8.analysis.brackets import build_brackets_for_find

        boundary = datetime(2024, 1, 10, tzinfo=timezone.utc)
        t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)

        query = {"sensor_id": "sensor_1", "timestamp": {"$gte": t1, "$lt": boundary}}

        _is_chunkable, _reason, brackets, _bounds = build_brackets_for_find(
            query, "timestamp"
        )
        bracket = brackets[0]

        # Build the final chunk filter
        time_clause = {}
        time_clause["$gte"] = bracket.timerange.lo
        if bracket.timerange.hi_inclusive:
            time_clause["$lte"] = bracket.timerange.hi
        else:
            time_clause["$lt"] = bracket.timerange.hi

        # Document at exact boundary
        doc_timestamp = boundary

        if "$lte" in time_clause:
            matches = doc_timestamp <= cast(datetime, time_clause["$lte"])
        else:
            matches = doc_timestamp < cast(datetime, time_clause["$lt"])

        assert matches is False, "Document at exact $lt boundary should be excluded"

    def test_document_at_exact_gt_boundary_excluded(self):
        """
        Documents at exact $gt boundary timestamp should be excluded.
        """
        from xlr8.analysis.brackets import build_brackets_for_find

        boundary = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 10, tzinfo=timezone.utc)

        query = {"sensor_id": "sensor_1", "timestamp": {"$gt": boundary, "$lt": t2}}

        _is_chunkable, _reason, brackets, _bounds = build_brackets_for_find(
            query, "timestamp"
        )
        bracket = brackets[0]

        # Build filter for unchunked case (preserves $gt)
        time_clause = {}
        if bracket.timerange.lo_inclusive:
            time_clause["$gte"] = bracket.timerange.lo
        else:
            time_clause["$gt"] = bracket.timerange.lo

        # Document at exact boundary
        doc_timestamp = boundary

        if "$gte" in time_clause:
            matches = doc_timestamp >= cast(datetime, time_clause["$gte"])
        else:
            matches = doc_timestamp > cast(datetime, time_clause["$gt"])

        assert matches is False, "Document at exact $gt boundary should be excluded"

    def test_document_at_exact_gte_boundary_included(self):
        """
        Documents at exact $gte boundary timestamp should be included.
        """
        from xlr8.analysis.brackets import build_brackets_for_find

        boundary = datetime(2024, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 10, tzinfo=timezone.utc)

        query = {"sensor_id": "sensor_1", "timestamp": {"$gte": boundary, "$lt": t2}}

        _is_chunkable, _reason, brackets, _bounds = build_brackets_for_find(
            query, "timestamp"
        )
        bracket = brackets[0]

        # Build filter
        time_clause = {}
        if bracket.timerange.lo_inclusive:
            time_clause["$gte"] = bracket.timerange.lo
        else:
            time_clause["$gt"] = bracket.timerange.lo

        # Document at exact boundary
        doc_timestamp = boundary

        if "$gte" in time_clause:
            matches = doc_timestamp >= cast(datetime, time_clause["$gte"])
        else:
            matches = doc_timestamp > cast(datetime, time_clause["$gt"])

        assert matches is True, "Document at exact $gte boundary should be included"
