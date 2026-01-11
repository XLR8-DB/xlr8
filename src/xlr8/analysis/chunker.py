"""
Time-range chunking utilities for XLR8.

This module splits time ranges into day-aligned chunks for parallel processing.
Each chunk becomes a work item that a worker can fetch independently.

WHY CHUNK BY TIME?
------------------

MongoDB time-series data is typically indexed by time. Chunking allows:
1. Parallel fetches - Multiple workers can fetch different time chunks
2. Incremental caching - Cache chunks separately, reuse when time range overlaps
3. Memory control - Each chunk fits in worker's RAM budget

CHUNKING ALGORITHM
------------------

INPUT:
  start = datetime(2024, 1, 5, 12, 30)  # Mid-day start
  end = datetime(2024, 1, 15, 8, 0)     # Mid-day end
  chunk_days = 3

OUTPUT (day-aligned chunks):

    Chunk 1: 2024-01-05 12:30 -> 2024-01-08 00:00 (partial first chunk)
    Chunk 2: 2024-01-08 00:00 -> 2024-01-11 00:00 (full 3-day chunk)
    Chunk 3: 2024-01-11 00:00 -> 2024-01-14 00:00 (full 3-day chunk)
    Chunk 4: 2024-01-14 00:00 -> 2024-01-15 08:00 (partial last chunk)

Note: First boundary is aligned to day start + step after the start time.

TYPICAL USAGE
-------------

6-month query with 14-day chunks:
  start = 2024-01-01
  end = 2024-07-01
  chunk_days = 14 (default)

Result: ~13 chunks
  Chunk 1: Jan 1-15
  Chunk 2: Jan 15-29
  Chunk 3: Jan 29 - Feb 12
  ...
  Chunk 13: Jun 17 - Jul 1

With 10 workers, chunks are processed in parallel:
  Workers 0-9 grab chunks 1-10 immediately
  As workers finish, they grab chunks 11-13
"""
