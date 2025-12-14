"""
MongoDB Query Chunkability Inspector for XLR8.

This module determines whether a MongoDB find() query can be safely split by time
for parallel execution. A query is "chunkable" if running it on time-based chunks
and merging results is equivalent to running on the full dataset.
"""
