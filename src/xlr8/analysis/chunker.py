"""
Time-range chunking utilities for XLR8.

Splits a time range into fixed-size chunks for parallel processing.
Each chunk becomes a work item that a worker can fetch independently.

WHY CHUNK BY TIME?
------------------

MongoDB time-series data is typically indexed by time. Chunking allows:
1. Parallel fetches - Multiple workers can fetch different time chunks
2. Incremental caching - Cache chunks separately, reuse when time range overlaps
3. Memory control - Each chunk fits in worker's RAM budget

CHUNKING ALGORITHM
------------------

Walks forward from start by step until it hits end. Last chunk may be shorter.

INPUT:
  start = datetime(2024, 1, 5, 12, 30)
  end = datetime(2024, 1, 15, 8, 0)
  chunk_size = timedelta(days=3)

OUTPUT:
  Chunk 1: 2024-01-05 12:30 -> 2024-01-08 12:30
  Chunk 2: 2024-01-08 12:30 -> 2024-01-11 12:30
  Chunk 3: 2024-01-11 12:30 -> 2024-01-14 12:30
  Chunk 4: 2024-01-14 12:30 -> 2024-01-15 08:00  (partial last chunk)

TYPICAL USAGE
-------------

6-month query with 14-day chunks, 10 workers:
  ~13 chunks, workers grab them round-robin as they finish.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

__all__ = [
    "chunk_time_range",
]


def chunk_time_range(
    start: datetime,
    end: datetime,
    chunk_size: Optional[timedelta] = None,
) -> List[Tuple[datetime, datetime]]:
    """
    Split time range into chunks.

    Creates chunks of specified size, aligned to boundaries.

    Args:
        start: Start datetime (inclusive)
        end: End datetime (exclusive)
        chunk_size: Size of each chunk as timedelta (default: 1 day)

    Returns:
        List of (chunk_start, chunk_end) tuples

    Examples:
        Day-level chunking:
        >>> start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        >>> end = datetime(2024, 1, 5, 8, 0, 0, tzinfo=timezone.utc)
        >>> chunks = chunk_time_range(start, end, chunk_size=timedelta(days=1))

        Hour-level chunking:
        >>> chunks = chunk_time_range(start, end, chunk_size=timedelta(hours=8))
    """
    # Ensure timezone-aware
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    if start >= end:
        return []

    # Determine step size
    if chunk_size is not None:
        step = chunk_size
    else:
        step = timedelta(days=1)  # Default to 1 day

    out: List[Tuple[datetime, datetime]] = []

    lo = start
    cur = start + step

    while lo < end:
        chunk_end = cur if cur < end else end
        out.append((lo, chunk_end))
        lo = cur
        cur = cur + step

    return out
