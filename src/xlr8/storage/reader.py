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
from typing import Any, Dict, Iterator, Literal, Optional, Union

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
        filters = None
        if time_field and (start_date or end_date):
            filter_conditions = []
            if start_date:
                filter_conditions.append((time_field, ">=", start_date))
            if end_date:
                filter_conditions.append((time_field, "<", end_date))
            if filter_conditions:
                filters = filter_conditions

        if engine == "polars":
            # Return empty DataFrame if no parquet files (query returned no results)
            if not self.parquet_files:
                return pl.DataFrame()

            # Polars can read multiple parquet files efficiently
            # Note: Polars handles filters differently, apply post-read
            df = pl.read_parquet(self.parquet_files)

            # Apply date filter post-read for Polars
            if time_field and (start_date or end_date):
                if start_date:
                    df = df.filter(pl.col(time_field) >= start_date)
                if end_date:
                    df = df.filter(pl.col(time_field) < end_date)

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
            import pyarrow as pa

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
            # (bypassing struct→dict→decode path)
            for field_name, decoded_values in any_columns_decoded.items():
                df[field_name] = decoded_values

            return self._process_dataframe(df, engine, schema, coerce)

        else:
            raise ValueError(f"Unknown engine: {engine}. Use 'pandas' or 'polars'")
