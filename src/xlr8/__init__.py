# XLR8: High-Performance MongoDB Acceleration Layer

"""
XLR8 - Accelerate MongoDB analytical queries with Parquet caching.

XLR8 is a high-performance wrapper for PyMongo that:
- Decomposes queries into brackets for parallel execution
- Executes parallel async MongoDB fetches with Rust backend
- Caches results in compressed Parquet files
- Reconstructs pandas/Polars DataFrames efficiently using Rust backend.

Quick Start:
```python
from pymongo import MongoClient
from xlr8 import accelerate, Schema, Types

# Define your schema with time field and typed fields
schema = Schema(
    time_field="recordedAt",
    fields={
        "recordedAt": Types.Timestamp("ms", tz="UTC"),
        "metadata.device_id": Types.ObjectId(),
        "metadata.sensor_id": Types.ObjectId(),
        "value": Types.Any(),  # Polymorphic - int, float, str, etc.
    },
    avg_doc_size_bytes=250,
)

# Connect to MongoDB and get collection
client = MongoClient("mongodb://localhost:27017")
collection = client["main"]["sensorLogs"]

# Wrap collection with schema for acceleration
xlr8_collection = accelerate(
    collection,
    schema=schema,
    mongo_uri="mongodb://localhost:27017"
)

# Use like normal PyMongo - find() returns XLR8Cursor
cursor = xlr8_collection.find({
    "recordedAt": {"$gte": start_date, "$lt": end_date}
}).sort("recordedAt", 1)

# Accelerated DataFrame construction
df = cursor.to_dataframe()

# Clean up
client.close()
```
"""

from __future__ import annotations

from importlib.metadata import version as _get_version
from typing import TYPE_CHECKING

from .schema import types as Types
from .schema.schema import Schema

try:
    __version__ = _get_version("xlr8")
except Exception:
    __version__ = "0.0.0.dev"  # Fallback for editable installs without metadata

# Lazy loader for rust_backend to avoid import errors when Rust isn't built yet
# This allows `from xlr8 import rust_backend` to work without importing at module load
_rust_backend_cached = None
_collection_exports_cached: dict[str, object] | None = None


if TYPE_CHECKING:
    from .collection.cursor import XLR8Cursor as XLR8Cursor
    from .collection.wrapper import XLR8Collection as XLR8Collection
    from .collection.wrapper import accelerate as accelerate


def __getattr__(name: str):
    global _rust_backend_cached
    global _collection_exports_cached
    if name == "rust_backend":
        if _rust_backend_cached is None:
            # Import the module directly to avoid recursion
            import importlib

            _rust_backend_cached = importlib.import_module(
                ".rust_backend", package="xlr8"
            )
        return _rust_backend_cached

    if name in {"XLR8Cursor", "XLR8Collection", "accelerate"}:
        if _collection_exports_cached is None:
            from .collection.cursor import XLR8Cursor
            from .collection.wrapper import XLR8Collection, accelerate

            _collection_exports_cached = {
                "XLR8Cursor": XLR8Cursor,
                "XLR8Collection": XLR8Collection,
                "accelerate": accelerate,
            }
        return _collection_exports_cached[name]

    raise AttributeError(f"module 'xlr8' has no attribute '{name}'")


__all__ = [
    "Schema",
    "Types",
    "rust_backend",
    "accelerate",
    "XLR8Collection",
    "XLR8Cursor",
]
