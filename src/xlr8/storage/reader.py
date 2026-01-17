"""
Parquet file reader for cache-aware loading.

This module reads Parquet files written by the Rust backend and converts them
back into DataFrames with proper value decoding and type reconstruction.

DATA FLOW
=========

STEP 1: DISCOVER RUST-WRITTEN FILES
------------------------------------
The Rust backend (rust_backend.fetch_chunks_bson) writes Parquet files with
timestamp-based naming derived from actual document data:

    cache_dir/.cache/abc123def/
        ts_1704067200_1704070800_part_0000.parquet
        ts_1704070801_1704074400_part_0000.parquet
        ts_1704074401_1704078000_part_0000.parquet
        ts_1704078001_1704081600_part_0000.parquet
        ...

Filename format: ts_{min_sec}_{max_sec}_part_{counter:04}.parquet
- min_sec: Unix timestamp (seconds) of earliest document in file
- max_sec: Unix timestamp (seconds) of latest document in file
- counter: Per-worker sequential counter (0000, 0001, 0002, ...)
  Only increments if same worker writes multiple files with identical timestamps

How timestamps ensure uniqueness:
- Each chunk/bracket targets different time ranges
- Multiple workers process non-overlapping time ranges
- Natural file separation by actual data timestamps
- Counter only needed if worker flushes multiple batches with identical ranges

Fallback format (no timestamps): part_{counter:04}.parquet
Used when time_field is None or documents lack timestamps


STEP 2: READ & CONCATENATE
---------------------------
Pandas: Read all files sequentially, concatenate into single DataFrame
Polars: Read all files in parallel (native multi-file support)

Both engines use PyArrow under the hood for efficient Parquet parsing.


STEP 3: DECODE TYPES.ANY STRUCT VALUES
---------------------------------------
Types.Any fields are encoded as Arrow structs by Rust backend:

    Parquet stores:
    {
        "value": {
            "float_value": 42.5,
            "int_value": null,
            "string_value": null,
            "bool_value": null,
            ...
        }
    }

    After decoding (coalesce first non-null field):
    {"value": 42.5}

This decoding happens in Rust via decode_any_struct_arrow() for maximum
performance.


STEP 4: FLATTEN NESTED STRUCTS
-------------------------------
Convert nested struct columns to dotted field names:

    Before: {"metadata": {"device_id": "123...", "sensor_id": "456..."}}
    After:  {"metadata.device_id": "123...", "metadata.sensor_id": "456..."}


STEP 5: RECONSTRUCT OBJECTIDS
------------------------------
Convert string-encoded ObjectIds back to bson.ObjectId instances:

    "507f1f77bcf86cd799439011" -> ObjectId("507f1f77bcf86cd799439011")


OUTPUT: DataFrame ( or Polars to stream pyarrow.Table )
-----------------
    timestamp          metadata.device_id    value
 0  2024-01-15 12:00   64a1b2c3...           42.5
 1  2024-01-15 12:01   64a1b2c3...           43.1
 2  2024-01-15 12:02   64a1b2c3...           "active"

"""

import logging
from pathlib import Path
from typing import Any, Dict, Iterator, Union

import pyarrow.parquet as pq

from xlr8.constants import DEFAULT_BATCH_SIZE

logger = logging.getLogger(__name__)


class ParquetReader:
    """
    Reads Parquet files from cache directory.

    Provides streaming and batch reading of documents from Parquet files.
    Supports reading all files in a cache directory or specific partitions.

    Example:
        >>> reader = ParquetReader(cache_dir=".cache/abc123def")
        >>>
        >>> # Stream all documents
        >>> for doc in reader.iter_documents():
        ...     print(doc)
        >>>
        >>> # Or load to DataFrame
        >>> df = reader.to_dataframe()
    """

    def __init__(self, cache_dir: Union[str, Path]):
        """
        Initialize reader for cache directory.

        Args:
            cache_dir: Directory containing parquet files
        """
        self.cache_dir = Path(cache_dir)

        if not self.cache_dir.exists():
            raise FileNotFoundError(f"Cache directory not found: {cache_dir}")

        # Find all parquet files (may be empty if query returned no results)
        self.parquet_files = sorted(self.cache_dir.glob("*.parquet"))

    def iter_documents(
        self,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[Dict[str, Any]]:
        """
        Stream documents from all parquet files.

        Reads in batches to avoid loading entire dataset into memory.

        Args:
            batch_size: Number of rows to read per batch

        Yields:
            Document dictionaries

        Example:
            >>> for doc in reader.iter_documents(batch_size=5000):
            ...     process(doc)
        """
        for parquet_file in self.parquet_files:
            # Read in batches
            parquet_file_obj = pq.ParquetFile(parquet_file)

            for batch in parquet_file_obj.iter_batches(batch_size=batch_size):
                # Convert Arrow batch to pandas then to dicts
                df_batch = batch.to_pandas()

                for _, row in df_batch.iterrows():
                    yield row.to_dict()
