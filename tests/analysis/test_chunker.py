"""Tests for chunker.py - Time range chunking for XLR8."""

from datetime import datetime, timedelta, timezone

from xlr8.analysis.chunker import chunk_time_range


class TestChunkTimeRangeSubDayBoundary:
    """chunk_time_range must produce valid chunks even when chunk_size < time-of-day.

    Bug scenario: start=12:30, chunk_size=1hr
    Old code: first_boundary = midnight + 1hr = 01:00 (BEFORE start!)
    First chunk would be (12:30, 01:00) -- inverted, MongoDB returns 0 docs.
    """

    def test_hourly_chunks_non_midnight_start(self):
        """1-hour chunks with start at 12:30 -- no chunk should be inverted."""
        start = datetime(2024, 1, 5, 12, 30, tzinfo=timezone.utc)
        end = datetime(2024, 1, 5, 16, 0, tzinfo=timezone.utc)
        chunks = chunk_time_range(start, end, chunk_size=timedelta(hours=1))

        assert len(chunks) > 0
        # First chunk must start at original start
        assert chunks[0][0] == start
        # Every chunk must have start < end
        for lo, hi in chunks:
            assert lo < hi, f"Inverted chunk: ({lo}, {hi})"
        # Chunks must be contiguous and cover full range
        assert chunks[0][0] == start
        assert chunks[-1][1] == end
        for i in range(1, len(chunks)):
            assert chunks[i][0] == chunks[i - 1][1]

    def test_15min_chunks_at_half_hour(self):
        """15-minute chunks starting at 00:30 -- first boundary at 00:45."""
        start = datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 2, 0, tzinfo=timezone.utc)
        chunks = chunk_time_range(start, end, chunk_size=timedelta(minutes=15))

        assert chunks[0][0] == start
        # First chunk: start + 15min = 00:45
        assert chunks[0][1] == datetime(2024, 1, 1, 0, 45, tzinfo=timezone.utc)
        for lo, hi in chunks:
            assert lo < hi

    def test_30min_chunks_at_noon(self):
        """30-minute chunks starting at 12:00 -- boundary aligned to 12:30."""
        start = datetime(2024, 3, 10, 12, 0, tzinfo=timezone.utc)
        end = datetime(2024, 3, 10, 14, 0, tzinfo=timezone.utc)
        chunks = chunk_time_range(start, end, chunk_size=timedelta(minutes=30))

        assert len(chunks) > 0
        for lo, hi in chunks:
            assert lo < hi

    def test_day_chunks_still_work(self):
        """Default 1-day chunks are unaffected (step > time-of-day)."""
        start = datetime(2024, 1, 5, 12, 30, tzinfo=timezone.utc)
        end = datetime(2024, 1, 10, 0, 0, tzinfo=timezone.utc)
        chunks = chunk_time_range(start, end, chunk_size=timedelta(days=1))

        assert chunks[0][0] == start
        assert chunks[-1][1] == end
        for lo, hi in chunks:
            assert lo < hi

    def test_empty_range(self):
        """start >= end returns empty list."""
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert chunk_time_range(t, t) == []
        assert chunk_time_range(t, t - timedelta(hours=1)) == []

    def test_single_chunk_when_range_smaller_than_step(self):
        """When total range < chunk_size, exactly one chunk is produced."""
        start = datetime(2024, 1, 5, 12, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 5, 12, 30, tzinfo=timezone.utc)
        chunks = chunk_time_range(start, end, chunk_size=timedelta(hours=1))

        assert len(chunks) == 1
        assert chunks[0] == (start, end)
