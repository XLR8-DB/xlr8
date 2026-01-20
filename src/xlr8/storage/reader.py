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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Iterator, List, Literal, Optional, Tuple, Union

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId

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

    def _is_any_type(self, field_type: Any) -> bool:
        """Check if field_type is an Any type (supports both class and instance)."""
        from xlr8.schema.types import Any as AnyType

        # Support both Types.Any (class) and Types.Any() (instance)
        if isinstance(field_type, AnyType):
            return True
        if isinstance(field_type, type) and issubclass(field_type, AnyType):
            return True
        return False

    def _decode_struct_values(self, df: pd.DataFrame, schema: Any) -> pd.DataFrame:
        """
        Decode struct-encoded Any-typed columns back to actual values.

        For columns marked as Any type in schema, extracts the actual value
        from the struct bitmap representation (float_value, int_value, etc.).

        Uses Rust Arrow-native decoding for maximum performance (~40x faster).

        Note: This is a fallback path. The fast path decodes directly from Arrow
        before to_pandas() conversion, avoiding dict overhead entirely.
        """
        if not hasattr(schema, "fields"):
            return df

        # Import Rust Arrow-native decoder (required)
        from xlr8.rust_backend import decode_any_struct_arrow

        # Find Any-typed fields in schema
        for field_name, field_type in schema.fields.items():
            if self._is_any_type(field_type) and field_name in df.columns:
                # Column contains struct-encoded values (dicts)
                col = df[field_name]

                if len(col) == 0:
                    continue

                # Check if it's a struct (dict) column - skip if already decoded
                first_val = col.iloc[0]
                if not isinstance(first_val, dict):
                    # Already decoded in fast path - skip
                    continue

                # Build struct type dynamically based on the dict keys
                sample_dict = first_val
                struct_fields = []
                field_type_map = {
                    "float_value": pa.float64(),
                    "int32_value": pa.int32(),
                    "int64_value": pa.int64(),
                    "string_value": pa.string(),
                    "objectid_value": pa.string(),
                    "decimal128_value": pa.string(),
                    "regex_value": pa.string(),
                    "binary_value": pa.string(),
                    "document_value": pa.string(),
                    "array_value": pa.string(),
                    "bool_value": pa.bool_(),
                    "datetime_value": pa.timestamp("ms"),  # Use ms for new schema
                    "null_value": pa.bool_(),
                }

                for key in sample_dict.keys():
                    if key in field_type_map:
                        struct_fields.append((key, field_type_map[key]))

                any_struct_type = pa.struct(struct_fields)

                # Convert to PyArrow array - this is a single pass over the data
                arrow_array = pa.array(col.tolist(), type=any_struct_type)

                # Decode in Rust - direct memory access to Arrow memory
                decoded_values = decode_any_struct_arrow(arrow_array)
                df[field_name] = decoded_values

        return df

    def _flatten_struct_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flatten nested struct columns into separate columns.

        Example:
            metadata: {'sensor_id': '...', 'device_id': '...'}
            -> metadata.sensor_id: '...', metadata.device_id: '...'

        """
        if df.empty:
            return df

        struct_cols = []
        for col in df.columns:
            # Check if column contains dicts (structs)
            if len(df) > 0 and isinstance(df[col].iloc[0], dict):
                struct_cols.append(col)

        for col in struct_cols:
            # FAST PATH: Extract struct fields directly using list comprehension
            # This is ~5x faster than pd.json_normalize() for large datasets
            col_values = df[col].tolist()

            # Detect subcolumns from first non-null row
            first_val = col_values[0] if col_values else {}
            subcolumns = list(first_val.keys()) if isinstance(first_val, dict) else []

            # Build new columns efficiently
            new_cols = {}
            for subcol in subcolumns:
                new_col_name = f"{col}.{subcol}"
                new_cols[new_col_name] = [
                    row.get(subcol) if isinstance(row, dict) else None
                    for row in col_values
                ]

            # Drop original struct column
            df = df.drop(columns=[col])

            # Add flattened columns
            for new_col_name, values in new_cols.items():
                df[new_col_name] = values

        return df

    def _reconstruct_objectids(self, df: pd.DataFrame, schema: Any) -> pd.DataFrame:
        """
        Reconstruct ObjectId columns from string representation.

        Converts string ObjectIds back to bson.ObjectId instances.
        """
        from xlr8.schema.types import ObjectId as ObjectIdType

        # Find all ObjectId fields in schema (including nested ones)
        objectid_fields = []

        if hasattr(schema, "fields"):
            for field_name, field_type in schema.fields.items():
                if isinstance(field_type, ObjectIdType):
                    objectid_fields.append(field_name)
                elif hasattr(field_type, "fields"):
                    # Nested struct with ObjectId fields
                    for nested_name, nested_type in field_type.fields.items():
                        if isinstance(nested_type, ObjectIdType):
                            objectid_fields.append(f"{field_name}.{nested_name}")

        # Convert string columns back to ObjectId
        for field in objectid_fields:
            if field in df.columns:
                df[field] = df[field].apply(
                    lambda x: ObjectId(x) if x and pd.notna(x) else x
                )

        return df

    def _decode_struct_values_polars(
        self,
        df: "pl.DataFrame",
        schema: Any,
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
    ) -> "pl.DataFrame":
        """
        Decode struct-encoded Any-typed columns back to actual values (Polars).

        Args:
            df: Polars DataFrame
            schema: Schema with field type info
            any_type_strategy: How to decode:
                - "float": Coalesce to Float64, prioritize numeric (default)
                - "string": Convert everything to string (lossless)
                - "keep_struct": Keep raw struct, don't decode
        """
        if not hasattr(schema, "fields"):
            return df

        # Find Any-typed fields in schema
        for field_name, field_type in schema.fields.items():
            if self._is_any_type(field_type) and field_name in df.columns:
                # Check if column is a struct
                col_dtype = df.schema[field_name]
                if str(col_dtype).startswith("Struct"):
                    # Strategy: keep_struct - don't decode at all
                    if any_type_strategy == "keep_struct":
                        continue

                    try:
                        # Get field names from the struct
                        struct_fields = (
                            col_dtype.fields if hasattr(col_dtype, "fields") else []
                        )  # type: ignore[attr-defined]
                        field_names = (
                            [f.name for f in struct_fields] if struct_fields else []
                        )

                        if any_type_strategy == "string":
                            # Convert ALL value types to string
                            coalesce_exprs = []

                            # String first (already string)
                            if "string_value" in field_names:
                                coalesce_exprs.append(
                                    pl.col(field_name).struct.field("string_value")
                                )

                            # Float to string
                            if "float_value" in field_names:
                                coalesce_exprs.append(
                                    pl.col(field_name)
                                    .struct.field("float_value")
                                    .cast(pl.Utf8)
                                )

                            # Int to string
                            for int_name in ["int64_value", "int32_value"]:
                                if int_name in field_names:
                                    coalesce_exprs.append(
                                        pl.col(field_name)
                                        .struct.field(int_name)
                                        .cast(pl.Utf8)
                                    )

                            # Bool to string
                            if "bool_value" in field_names:
                                coalesce_exprs.append(
                                    pl.col(field_name)
                                    .struct.field("bool_value")
                                    .cast(pl.Utf8)
                                )

                            # ObjectId, decimal, etc. (already strings)
                            for str_field in [
                                "objectid_value",
                                "decimal128_value",
                                "regex_value",
                                "binary_value",
                                "document_value",
                                "array_value",
                            ]:
                                if str_field in field_names:
                                    coalesce_exprs.append(
                                        pl.col(field_name).struct.field(str_field)
                                    )

                            if coalesce_exprs:
                                df = df.with_columns(
                                    pl.coalesce(coalesce_exprs).alias(field_name)
                                )

                        else:  # "float" strategy (default)
                            # Coalesce to Float64, prioritize numeric
                            coalesce_exprs = []

                            # Try float first (highest precision)
                            if "float_value" in field_names:
                                coalesce_exprs.append(
                                    pl.col(field_name).struct.field("float_value")
                                )

                            # Try various int types, cast to float
                            for int_name in ["int64_value", "int32_value"]:
                                if int_name in field_names:
                                    coalesce_exprs.append(
                                        pl.col(field_name)
                                        .struct.field(int_name)
                                        .cast(pl.Float64)
                                    )

                            # Try bool (as 0.0/1.0)
                            if "bool_value" in field_names:
                                coalesce_exprs.append(
                                    pl.col(field_name)
                                    .struct.field("bool_value")
                                    .cast(pl.Float64)
                                )

                            if coalesce_exprs:
                                if len(coalesce_exprs) == 1:
                                    df = df.with_columns(
                                        coalesce_exprs[0].alias(field_name)
                                    )
                                else:
                                    df = df.with_columns(
                                        pl.coalesce(coalesce_exprs).alias(field_name)
                                    )
                            else:
                                logger.warning(
                                    "Could not decode struct column '%s': "
                                    "no numeric fields in %s",
                                    field_name,
                                    field_names,
                                )
                    except (AttributeError, KeyError, ValueError) as e:
                        logger.warning("Error decoding struct '%s': %s", field_name, e)

        return df

    def _process_dataframe(
        self,
        df: Union[pd.DataFrame, "pl.DataFrame"],
        engine: Literal["pandas", "polars"],
        schema: Optional[Any] = None,
        coerce: Literal["raise", "error"] = "raise",
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
    ) -> Union[pd.DataFrame, "pl.DataFrame"]:
        """
        Process DataFrame: decode struct values, flatten structs and
        reconstruct ObjectIds.

        Args:
            df: DataFrame to process
            engine: "pandas" or "polars"
            schema: Schema for ObjectId reconstruction
            coerce: Error handling mode ("raise" or "error")
            any_type_strategy: How to decode Any() structs in Polars
                (float/string/keep_struct)

        Returns:
            Processed DataFrame
        """
        if engine == "pandas":
            # First, decode Any-typed struct columns back to actual values
            if schema is not None:
                try:
                    df = self._decode_struct_values(df, schema)  # type: ignore[arg-type]
                except (AttributeError, KeyError, ValueError, TypeError) as e:
                    if coerce == "error":
                        logger.error("Error decoding struct values: %s", e)
                    else:
                        raise

            # Flatten struct columns (e.g., metadata -> metadata.sensor_id)
            df = self._flatten_struct_columns(df)  # type: ignore[arg-type]

            # Reconstruct ObjectIds from strings
            if schema is not None:
                try:
                    df = self._reconstruct_objectids(df, schema)
                except (AttributeError, KeyError, ValueError, TypeError) as e:
                    if coerce == "error":
                        logger.error("Error reconstructing ObjectIds: %s", e)
                    else:
                        raise

            return df
        elif engine == "polars":
            # Polars: decode Any-typed struct columns and keep dotted column names
            if schema is not None:
                try:
                    df = self._decode_struct_values_polars(
                        df, schema, any_type_strategy
                    )  # type: ignore[arg-type]
                except (AttributeError, KeyError, ValueError, TypeError) as e:
                    if coerce == "error":
                        logger.error("Error decoding struct values (polars): %s", e)
                    else:
                        raise
            return df

    def to_dataframe(
        self,
        engine: str = "pandas",
        schema: Optional[Any] = None,
        time_field: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        coerce: Literal["raise", "error"] = "raise",
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
    ) -> Union[pd.DataFrame, "pl.DataFrame"]:
        """
        Load all parquet files into a DataFrame.

        Args:
            engine: "pandas" or "polars"
            schema: Schema for ObjectId reconstruction and struct flattening (required)
            time_field: Name of time field for date filtering (from schema.time_field)
            start_date: Filter data from this date (inclusive, tz-aware datetime)
            end_date: Filter data until this date (exclusive, tz-aware datetime)
            coerce: Error handling mode:
                    - "raise": Raise exceptions on schema validation errors (default)
                    - "error": Log errors and store None for invalid values
            any_type_strategy: How to decode Types.Any() struct columns in Polars:
                    - "float": Coalesce to Float64, prioritize numeric (default)
                    - "string": Convert everything to string (lossless)
                    - "keep_struct": Keep raw struct, don't decode

        Returns:
            DataFrame with all documents (structs flattened, ObjectIds reconstructed)

        Example:
            >>> df = reader.to_dataframe(
            ...     schema=schema,
            ...     time_field="timestamp",
            ...     start_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
            ...     end_date=datetime(2024, 6, 15, tzinfo=timezone.utc),
            ... )
        """
        # Build PyArrow filter for date range (predicate pushdown)
        # Convert datetime to PyArrow timestamp[ms] to match Parquet column type (no tz)
        filters = None
        if time_field and (start_date or end_date):
            filter_conditions = []
            if start_date:
                # Convert to ms timestamp (no tz) to match Parquet storage
                start_ts = pa.scalar(start_date, type=pa.timestamp("ms"))
                filter_conditions.append((time_field, ">=", start_ts))
            if end_date:
                end_ts = pa.scalar(end_date, type=pa.timestamp("ms"))
                filter_conditions.append((time_field, "<", end_ts))
            if filter_conditions:
                filters = filter_conditions

        if engine == "polars":
            # Return empty DataFrame if no parquet files (query returned no results)
            if not self.parquet_files:
                return pl.DataFrame()

            # Use scan_parquet for lazy evaluation with predicate pushdown
            # This only reads the row groups that match the filter conditions
            lf = pl.scan_parquet(self.parquet_files)

            # Apply date filter with predicate pushdown (reads only matching data)
            # Convert datetime to naive (no timezone) to match Parquet column dtype
            if time_field and (start_date or end_date):
                if start_date:
                    start_naive = (
                        start_date.replace(tzinfo=None)
                        if start_date.tzinfo
                        else start_date
                    )
                    lf = lf.filter(pl.col(time_field) >= start_naive)
                if end_date:
                    end_naive = (
                        end_date.replace(tzinfo=None) if end_date.tzinfo else end_date
                    )
                    lf = lf.filter(pl.col(time_field) < end_naive)

            # Collect executes the query with predicate pushdown
            df = lf.collect()

            return self._process_dataframe(
                df, engine, schema, coerce, any_type_strategy
            )

        elif engine == "pandas":
            # Return empty DataFrame if no parquet files (query returned no results)
            if not self.parquet_files:
                return pd.DataFrame()

            # Read all files with optional filter (predicate pushdown)
            # Use PyArrow to read, then convert to pandas - this allows
            # struct columns to stay in Arrow format for fast Rust decoding
            tables = []
            for parquet_file in self.parquet_files:
                try:
                    # Use PyArrow filters for efficient predicate pushdown
                    table = pq.read_table(parquet_file, filters=filters)
                    tables.append(table)
                except Exception as e:
                    if coerce == "error":
                        logger.error(f"Error reading {parquet_file}: {e}")
                        continue
                    raise

            if not tables:
                return pd.DataFrame()

            # Concatenate Arrow tables
            combined_table = pa.concat_tables(tables)

            # FAST PATH: Decode Any-typed struct columns directly in Arrow
            # This gives us 44x speedup because Rust reads Arrow memory directly
            # without Python iteration over dicts
            any_columns_decoded = {}
            columns_to_drop = []
            if schema and hasattr(schema, "fields"):
                from xlr8.rust_backend import decode_any_struct_arrow

                for field_name, field_type in schema.fields.items():
                    if (
                        self._is_any_type(field_type)
                        and field_name in combined_table.column_names
                    ):
                        col = combined_table.column(field_name)
                        if pa.types.is_struct(col.type):
                            # Decode in Rust - returns Python list of mixed types
                            combined = col.combine_chunks()
                            decoded_values = decode_any_struct_arrow(combined)
                            any_columns_decoded[field_name] = decoded_values
                            # Mark for removal to avoid slow dict conversion
                            # in to_pandas()
                            columns_to_drop.append(field_name)

            # Drop decoded struct columns before pandas conversion
            # to avoid dict overhead
            if columns_to_drop:
                combined_table = combined_table.drop(columns_to_drop)

            # Convert to pandas (non-Any columns go through normal path)
            df = combined_table.to_pandas()

            # Add back Any columns with decoded values
            # (bypassing struct->dict->decode path)
            for field_name, decoded_values in any_columns_decoded.items():
                df[field_name] = decoded_values

            return self._process_dataframe(df, engine, schema, coerce)

        else:
            raise ValueError(f"Unknown engine: {engine}. Use 'pandas' or 'polars'")

    def iter_dataframe_batches(
        self,
        batch_size: int = 10000,
        schema: Optional[Any] = None,
        time_field: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        coerce: Literal["raise", "error"] = "raise",
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Yield DataFrames in batches without loading all data into memory.

        This is memory-efficient: only batch_size rows are in memory at a time.
        Uses PyArrow's batch iteration for efficient streaming.

        Use this when NO sorting is needed. For sorted batches, use
        iter_globally_sorted_batches().

        Args:
            batch_size: Number of rows per batch (default: 10,000)
            schema: Schema for struct decoding and ObjectId reconstruction
            time_field: Name of time field for date filtering
            start_date: Filter data from this date (inclusive, tz-aware)
            end_date: Filter data until this date (exclusive, tz-aware)
            coerce: Error handling mode ("raise" or "error")

        Yields:
            pd.DataFrame: Batches of processed rows

        Example:
            >>> for batch_df in reader.iter_dataframe_batches(batch_size=5000):
            ...     process(batch_df)
        """
        import pyarrow.parquet as pq

        batch_count = 0
        total_rows = 0

        for parquet_file in self.parquet_files:
            try:
                # Open parquet file for batch iteration
                parquet_file_obj = pq.ParquetFile(parquet_file)

                for batch in parquet_file_obj.iter_batches(batch_size=batch_size):
                    # Convert Arrow batch to pandas
                    batch_df = batch.to_pandas()

                    # Apply date filter if specified
                    # (in case predicate pushdown not supported)
                    if time_field and (start_date or end_date):
                        if time_field in batch_df.columns:
                            if start_date:
                                batch_df = batch_df[batch_df[time_field] >= start_date]
                            if end_date:
                                batch_df = batch_df[batch_df[time_field] < end_date]

                    if len(batch_df) == 0:
                        continue

                    # Process the batch (decode structs, flatten, reconstruct ObjectIds)
                    processed_df = self._process_dataframe(
                        batch_df, "pandas", schema, coerce
                    )

                    batch_count += 1
                    total_rows += len(processed_df)

                    yield processed_df

            except Exception as e:
                if coerce == "error":
                    logger.error(f"Error reading batch from {parquet_file}: {e}")
                    continue
                raise

        logger.debug(f"Yielded {batch_count} batches, {total_rows} total rows")

    def get_globally_sorted_dataframe(
        self,
        sort_spec: List[Tuple[str, int]],
        schema: Optional[Any] = None,
        time_field: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        coerce: Literal["raise", "error"] = "raise",
        memory_limit_mb: Optional[int] = None,
        threads: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Return entire globally sorted DataFrame using DuckDB K-way merge.

        More efficient than iter_globally_sorted_batches() when you want
        the full result, as it avoids batch iteration overhead and just
        fetches all rows at once.
        for to_dataframe_batches() where streaming is required.

        Args:
            sort_spec: Sort specification as [(field, direction), ...]
            schema: Schema for ObjectId reconstruction and advanced sorting
            time_field: Field for date filtering
            start_date: Filter data from this date (inclusive, tz-aware)
            end_date: Filter data until this date (exclusive, tz-aware)
            coerce: Error handling mode
            memory_limit_mb: DuckDB memory limit
            threads: DuckDB thread count

        Returns:
            pd.DataFrame: Complete sorted DataFrame
        """
        if not self.parquet_files:
            return pd.DataFrame()

        # Expand parent fields to children in schema definition order
        sort_spec = self._expand_parent_sort_fields(sort_spec, schema)

        # Get list of parquet files
        file_paths = [str(f) for f in self.parquet_files]

        logger.debug(
            f"DuckDB K-way merge (full): {len(file_paths)} files, sort_spec={sort_spec}"
        )

        try:
            # Create DuckDB connection
            conn = duckdb.connect(":memory:")

            # Configure DuckDB to use allocated resources
            if memory_limit_mb:
                conn.execute(f"SET memory_limit = '{memory_limit_mb}MB'")
                logger.info(f"DuckDB memory_limit set to {memory_limit_mb} MB")

            if threads:
                conn.execute(f"SET threads = {threads}")
                logger.info(f"DuckDB threads set to {threads}")

            # Build ORDER BY with MongoDB type ordering
            # (same logic as iter_globally_sorted_batches)
            order_clauses = []
            for field_name, direction in sort_spec:
                dir_sql = "ASC" if direction == 1 else "DESC"
                if schema and schema.has_field(field_name):
                    field_type = schema.get_field_type(field_name)
                else:
                    field_type = None
                is_any = self._is_any_type(field_type) if field_type else True

                if is_any:
                    # Complete MongoDB type ordering for Any() fields
                    type_clause = f"""CASE
                        WHEN "{field_name}" IS NULL OR "{field_name}".null_value IS TRUE
                            THEN 0
                        WHEN "{field_name}".float_value IS NOT NULL
                            OR "{field_name}".int32_value IS NOT NULL
                            OR "{field_name}".int64_value IS NOT NULL
                            OR "{field_name}".decimal128_value IS NOT NULL
                            THEN 1
                        WHEN "{field_name}".string_value IS NOT NULL THEN 2
                        WHEN "{field_name}".document_value IS NOT NULL THEN 3
                        WHEN "{field_name}".array_value IS NOT NULL THEN 4
                        WHEN "{field_name}".binary_value IS NOT NULL THEN 5
                        WHEN "{field_name}".objectid_value IS NOT NULL THEN 6
                        WHEN "{field_name}".bool_value IS NOT NULL THEN 7
                        WHEN "{field_name}".datetime_value IS NOT NULL THEN 8
                        WHEN "{field_name}".regex_value IS NOT NULL THEN 9
                        ELSE 10
                    END {dir_sql}"""

                    # Value comparisons for each type
                    num_clause = (
                        f'COALESCE("{field_name}".float_value, '
                        f'CAST("{field_name}".int32_value AS DOUBLE), '
                        f'CAST("{field_name}".int64_value AS DOUBLE)) {dir_sql}'
                    )
                    str_clause = f'"{field_name}".string_value {dir_sql}'
                    doc_clause = f'"{field_name}".document_value {dir_sql}'
                    arr_clause = f'"{field_name}".array_value {dir_sql}'
                    bin_clause = f'"{field_name}".binary_value {dir_sql}'
                    oid_clause = f'"{field_name}".objectid_value {dir_sql}'
                    bool_clause = f'"{field_name}".bool_value {dir_sql}'
                    date_clause = f'"{field_name}".datetime_value {dir_sql}'
                    regex_clause = f'"{field_name}".regex_value {dir_sql}'

                    order_clauses.extend(
                        [
                            type_clause,
                            num_clause,
                            str_clause,
                            doc_clause,
                            arr_clause,
                            bin_clause,
                            oid_clause,
                            bool_clause,
                            date_clause,
                            regex_clause,
                        ]
                    )
                else:
                    # Simple field - use direct comparison
                    order_clauses.append(f'"{field_name}" {dir_sql}')

            order_by = ", ".join(order_clauses)
            files = ", ".join([f"'{f}'" for f in file_paths])
            query = f"SELECT * FROM read_parquet([{files}]) ORDER BY {order_by}"

            print(f"[DuckDB] K-way merge (full): {len(file_paths)} files")

            # Fetch entire result at once using df()
            df = conn.execute(query).df()

            # Ensure time field is UTC
            if time_field and time_field in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[time_field]):
                    if df[time_field].dt.tz is not None:
                        df[time_field] = df[time_field].dt.tz_convert("UTC")
                    else:
                        df[time_field] = df[time_field].dt.tz_localize("UTC")

            # Apply date filtering if needed
            if time_field and (start_date or end_date):
                if start_date:
                    df = df[df[time_field] >= start_date]
                if end_date:
                    df = df[df[time_field] < end_date]

            # Process the DataFrame (decode structs, reconstruct ObjectIds)
            df = self._process_dataframe(df, "pandas", schema, coerce)

            conn.close()
            print(f"[DuckDB] K-way merge complete: {len(df):,} rows")
            logger.debug(f"DuckDB K-way merge complete: {len(df):,} rows")

            return df

        except Exception as e:
            logger.error(f"DuckDB K-way merge failed: {e}")
            raise

    def _expand_parent_sort_fields(
        self, sort_spec: List[Tuple[str, int]], schema: Optional[Any]
    ) -> List[Tuple[str, int]]:
        """
        Expand parent field sorts to their child fields in schema definition order.

        When user sorts by a parent field like "metadata" but the schema has
        flattened fields like "metadata.device_id", expand to all children.

        Args:
            sort_spec: Original [(field, direction), ...]
            schema: XLR8 schema with field definitions

        Returns:
            Expanded sort spec with parent fields replaced by children

        Raises:
            ValueError: If field not found and no children exist
        """
        if schema is None:
            return sort_spec

        expanded = []
        # Schema.fields preserves insertion order (Python 3.7+)
        all_fields = list(schema.fields.keys())

        for field_name, direction in sort_spec:
            if schema.has_field(field_name):
                # Field exists directly in schema
                expanded.append((field_name, direction))
            else:
                # Look for child fields with this prefix (in schema order)
                prefix = f"{field_name}."
                children = [f for f in all_fields if f.startswith(prefix)]

                if children:
                    logger.info(
                        f"Sort field '{field_name}' expanded to children "
                        f"(schema order): {children}"
                    )
                    for child in children:
                        expanded.append((child, direction))
                else:
                    raise ValueError(
                        f"Sort field '{field_name}' not found in schema "
                        f"and has no child fields. "
                        f"Available fields: {sorted(all_fields)[:10]}"
                        + ("..." if len(all_fields) > 10 else "")
                    )

        return expanded

    def iter_globally_sorted_batches(
        self,
        sort_field: Optional[str] = None,
        ascending: bool = True,
        batch_size: int = DEFAULT_BATCH_SIZE,
        schema: Optional[Any] = None,
        time_field: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        coerce: Literal["raise", "error"] = "raise",
        sort_spec: Optional[List[Tuple[str, int]]] = None,
        # DuckDB configuration
        memory_limit_mb: Optional[int] = None,
        threads: Optional[int] = None,
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Yield globally sorted batches using DuckDB K-way merge.

        This method reads all Parquet files in the cache directory and
        yields batches in globally sorted order. Uses Rust's K-way merge
        with MongoDB BSON comparison for 100% compatibility.

        Supports advanced sorting:
        - Parent fields (e.g., "metadata" expands to all child fields)
        - Types.Any() with full MongoDB BSON type ordering (Objects, Arrays, Binary)

        RAM Usage:
            O(K × batch_size) where K = number of files.
            Already handled by flush_ram_limit_mb.

        Args:
            sort_field: Field to sort by (use sort_spec for multi-field sorting).
            ascending: Sort direction (use sort_spec for mixed directions).
            batch_size: Number of rows per yielded DataFrame (default: 10,000)
            schema: Schema for ObjectId reconstruction and advanced sorting
            time_field: Field for date filtering (usually same as sort_field)
            start_date: Filter data from this date (inclusive, tz-aware)
            end_date: Filter data until this date (exclusive, tz-aware)
            coerce: Error handling mode ("raise" or "error")
            sort_spec: Sort specification as [(field, direction), ...] where
                      direction is 1 (ASC) or -1 (DESC). Preferred over sort_field.

        Yields:
            pd.DataFrame: Batches in globally sorted order

        Example:
            >>> reader = ParquetReader(".cache/abc123def")
            >>> # Simple sort
            >>> for batch in reader.iter_globally_sorted_batches(
            ...     sort_spec=[("timestamp", 1)],
            ...     schema=schema,
            ...     batch_size=10_000
            ... ):
            ...     process(batch)
            >>>
            >>> # Advanced: parent field + Any type
            >>> for batch in reader.iter_globally_sorted_batches(
            ...     sort_spec=[("metadata", -1), ("value", 1)],
            ...     schema=schema,
            ... ):
            ...     process(batch)
        """

        if not self.parquet_files:
            return

        # Handle backwards compatibility
        if sort_spec is None and sort_field is not None:
            direction = 1 if ascending else -1
            sort_spec = [(sort_field, direction)]

        if sort_spec is None:
            raise ValueError("sort_spec or sort_field is required")

        # Expand parent fields to children in schema definition order
        sort_spec = self._expand_parent_sort_fields(sort_spec, schema)

        # Get list of parquet files
        file_paths = [str(f) for f in self.parquet_files]

        logger.debug(
            f"DuckDB K-way merge: {len(file_paths)} files, sort_spec={sort_spec}"
        )

        try:
            # Create DuckDB connection
            conn = duckdb.connect(":memory:")

            # Configure DuckDB to use allocated resources
            if memory_limit_mb:
                conn.execute(f"SET memory_limit = '{memory_limit_mb}MB'")
                logger.info(f"DuckDB memory_limit set to {memory_limit_mb} MB")

            if threads:
                conn.execute(f"SET threads = {threads}")
                logger.info(f"DuckDB threads set to {threads}")

            # Query DuckDB settings to verify
            memory_result = conn.execute(
                "SELECT current_setting('memory_limit')"
            ).fetchone()
            actual_memory = memory_result[0] if memory_result else "unknown"
            threads_result = conn.execute(
                "SELECT current_setting('threads')"
            ).fetchone()
            actual_threads = threads_result[0] if threads_result else "unknown"
            logger.debug(
                f"DuckDB configured: memory={actual_memory}, threads={actual_threads}"
            )

            # Build ORDER BY with MongoDB type ordering
            order_clauses = []
            for field_name, direction in sort_spec:
                dir_sql = "ASC" if direction == 1 else "DESC"
                # Check if field exists in schema before getting type
                if schema and schema.has_field(field_name):
                    field_type = schema.get_field_type(field_name)
                else:
                    field_type = None
                is_any = self._is_any_type(field_type) if field_type else True

                if is_any:
                    # Complete MongoDB type ordering for Any() fields:
                    # Reference: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
                    # 1. MinKey (internal)
                    # 2. Null
                    # 3. Numbers (int, long, double, decimal)
                    # 4. Symbol, String
                    # 5. Object
                    # 6. Array
                    # 7. BinData
                    # 8. ObjectId
                    # 9. Boolean
                    # 10. Date
                    # 11. Timestamp
                    # 12. Regular Expression
                    # 13. MaxKey (internal)

                    # Type priority clause
                    type_clause = f"""CASE
                        WHEN "{field_name}" IS NULL OR "{field_name}".null_value IS TRUE
                            THEN 0
                        WHEN "{field_name}".float_value IS NOT NULL
                            OR "{field_name}".int32_value IS NOT NULL
                            OR "{field_name}".int64_value IS NOT NULL
                            OR "{field_name}".decimal128_value IS NOT NULL
                            THEN 1
                        WHEN "{field_name}".string_value IS NOT NULL THEN 2
                        WHEN "{field_name}".document_value IS NOT NULL THEN 3
                        WHEN "{field_name}".array_value IS NOT NULL THEN 4
                        WHEN "{field_name}".binary_value IS NOT NULL THEN 5
                        WHEN "{field_name}".objectid_value IS NOT NULL THEN 6
                        WHEN "{field_name}".bool_value IS NOT NULL THEN 7
                        WHEN "{field_name}".datetime_value IS NOT NULL THEN 8
                        WHEN "{field_name}".regex_value IS NOT NULL THEN 9
                        ELSE 10
                    END {dir_sql}"""

                    # Value comparisons for each type
                    num_clause = (
                        f'COALESCE("{field_name}".float_value, '
                        f'CAST("{field_name}".int32_value AS DOUBLE), '
                        f'CAST("{field_name}".int64_value AS DOUBLE)) {dir_sql}'
                    )
                    str_clause = f'"{field_name}".string_value {dir_sql}'
                    # JSON strings compare lexicographically
                    doc_clause = f'"{field_name}".document_value {dir_sql}'
                    # JSON arrays compare lexicographically
                    arr_clause = f'"{field_name}".array_value {dir_sql}'
                    bin_clause = f'"{field_name}".binary_value {dir_sql}'
                    oid_clause = f'"{field_name}".objectid_value {dir_sql}'
                    bool_clause = f'"{field_name}".bool_value {dir_sql}'
                    date_clause = f'"{field_name}".datetime_value {dir_sql}'
                    regex_clause = f'"{field_name}".regex_value {dir_sql}'

                    order_clauses.extend(
                        [
                            type_clause,
                            num_clause,
                            str_clause,
                            doc_clause,
                            arr_clause,
                            bin_clause,
                            oid_clause,
                            bool_clause,
                            date_clause,
                            regex_clause,
                        ]
                    )
                else:
                    # Simple field - use direct comparison
                    order_clauses.append(f'"{field_name}" {dir_sql}')

            order_by = ", ".join(order_clauses)
            files = ", ".join([f"'{f}'" for f in file_paths])
            query = f"SELECT * FROM read_parquet([{files}]) ORDER BY {order_by}"

            result = conn.execute(query)

            # Use fetchmany() cursor API - this ACTUALLY streams incrementally
            # without loading all data into memory (unlike fetch_df_chunk)
            # NOTE: DuckDB's k-way merge uses internal buffering
            # separate from batch_size.
            # batch_size only controls how much we pull at once,
            # not DuckDB's merge buffer.
            batch_count = 0
            total_rows = 0
            column_names = [desc[0] for desc in result.description]

            print(
                f"[DuckDB] K-way merge started: {len(file_paths)} files, "
                f"batch_size={batch_size:,}"
            )

            while True:
                # Fetch batch as list of tuples
                rows = result.fetchmany(batch_size)
                if not rows:
                    break

                batch_count += 1
                total_rows += len(rows)

                # Convert to DataFrame
                batch_df = pd.DataFrame(rows, columns=column_names)
                logger.debug(
                    f"Streamed batch {batch_count}: {len(batch_df)} rows "
                    f"from DuckDB K-way merge"
                )

                # Ensure time field is UTC (DuckDB might return naive)
                if time_field and time_field in batch_df.columns:
                    if pd.api.types.is_datetime64_any_dtype(batch_df[time_field]):
                        if batch_df[time_field].dt.tz is not None:
                            batch_df[time_field] = batch_df[time_field].dt.tz_convert(
                                "UTC"
                            )
                        else:
                            batch_df[time_field] = batch_df[time_field].dt.tz_localize(
                                "UTC"
                            )

                # Apply date filtering if needed
                if time_field and (start_date or end_date):
                    if start_date:
                        batch_df = batch_df[batch_df[time_field] >= start_date]
                    if end_date:
                        batch_df = batch_df[batch_df[time_field] < end_date]
                    if len(batch_df) == 0:
                        continue

                # Process the batch (decode structs, reconstruct ObjectIds)
                processed_df = self._process_dataframe(
                    batch_df, "pandas", schema, coerce
                )
                yield processed_df

            conn.close()
            logger.debug("DuckDB K-way merge complete")

        except Exception as e:
            if coerce == "error":
                logger.error(f"Error in globally sorted streaming: {e}")
                return
            raise

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about cached data.

        Returns:
            Dict with file count, total rows, size, schema info
        """
        total_rows = 0
        total_size = 0
        schema = None

        for parquet_file in self.parquet_files:
            # File size
            total_size += parquet_file.stat().st_size

            # Read metadata
            parquet_meta = pq.read_metadata(parquet_file)
            total_rows += parquet_meta.num_rows

            # Get schema from first file
            if schema is None:
                schema = parquet_meta.schema.to_arrow_schema()

        return {
            "file_count": len(self.parquet_files),
            "total_rows": total_rows,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "schema_fields": [field.name for field in schema] if schema else [],
            "cache_dir": str(self.cache_dir),
        }

    def __repr__(self) -> str:
        stats = self.get_statistics()
        return (
            f"ParquetReader(files={stats['file_count']}, "
            f"rows={stats['total_rows']:,}, "
            f"size={stats['total_size_mb']:.1f}MB)"
        )

    def __len__(self) -> int:
        """Return total number of rows across all files."""
        return self.get_statistics()["total_rows"]
