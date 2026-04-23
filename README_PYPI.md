<p align="center">
    <img src="https://raw.githubusercontent.com/XLR8-DB/xlr8/main/.github/XLR8_logo.png" alt="XLR8 Logo" width="360"/>
</p>

<p align="center">
  <strong>Accelerate MongoDB analytical queries with parallel execution and Parquet caching</strong>
</p>

<p align="center">
  <em>Faster Queries → Less Memory → Real Savings</em>
</p>

<p align="center">
  <a href="https://github.com/XLR8-DB/xlr8"><img src="https://img.shields.io/badge/GitHub-Repository-blue?logo=github" alt="GitHub"/></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3.11%2B-blue.svg" alt="Python versions"/></a>
  <a href="https://github.com/XLR8-DB/xlr8/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"/></a>
</p>

<p align="center">
  <strong>🦀 Rust-Backed</strong> · <strong>⚡ Up to 4x Faster Queries</strong> · <strong>📦 10-12x Compression</strong> · <strong>📊 Configurable Memory Limits</strong>
</p>

---

## Minimal Code Changes

```python
# Before: PyMongo
df = pd.DataFrame(list(collection.find(query)))

# After: XLR8 - just wrap and go!
xlr8_collection = accelerate(collection, schema, mongodb_uri)
df = xlr8_collection.find(query).to_dataframe()
```

That's it. Same query syntax, same DataFrame output - just faster.

---

## The Problem

When running analytical queries over large MongoDB collections, you encounter two fundamental bottlenecks:

**I/O Bound**: PyMongo uses a single cursor, fetching documents one batch at a time. Your CPU sits idle waiting for network round trips.

**CPU/GIL Bound**: Even with the data in hand, Python's Global Interpreter Lock (GIL) means BSON decoding and DataFrame construction happen on a single core.

These aren't PyMongo limitations - they're inherent to Python's design. XLR8 provides a solution.

---

## How XLR8 Solves It

XLR8 releases Python's GIL and hands execution to a **Rust backend** powered by Tokio's async runtime:

1. **Query Planning** → Splits your query into time-based chunks
2. **Parallel Workers** → Multiple workers fetch from MongoDB simultaneously  
3. **BSON → Arrow** → Direct conversion without Python overhead
4. **Parquet Caching** → Results cached for instant reuse
5. **DataFrame Assembly** → Final merge via DuckDB (GIL-free)

The result? Your analytical queries run **significantly faster**, especially for large result sets.

---

## Installation

```bash
pip install xlr8
```

XLR8 requires Python 3.11+ and includes pre-compiled Rust extensions for Linux, macOS, and Windows.

---

## Quick Start

```python
from pymongo import MongoClient
from xlr8 import accelerate, Schema, Types
from datetime import datetime, timezone, timedelta

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
collection = client["iot"]["sensor_readings"]

# Define your schema
schema = Schema(
    time_field="timestamp",
    fields={
        "timestamp": Types.Timestamp("ms", tz="UTC"),
        "device_id": Types.ObjectId(),
        "reading": Types.Any(),  # Handles int, float, string dynamically
    },
    avg_doc_size_bytes=200,
)

# Wrap collection with XLR8
xlr8_col = accelerate(collection, schema=schema, mongo_uri="mongodb://localhost:27017")

# Query like normal PyMongo
cursor = xlr8_col.find({
    "timestamp": {"$gte": datetime(2024, 1, 1, tzinfo=timezone.utc)}
}).sort("timestamp", 1)

# Get DataFrame - parallel fetch, cached for reuse
df = cursor.to_dataframe(
    chunking_granularity=timedelta(days=7),
    max_workers=8,
)
```

---

## Key Features

| Feature | Description |
|---------|-------------|
| 🦀 **GIL-Free Rust Backend** | Python's GIL is released. Rust's Tokio runtime handles async I/O across all cores. |
| ⚡ **Parallel MongoDB Fetching** | Queries split into time chunks. Each worker has its own MongoDB connection. |
| 💾 **Smart Query Cache** | Results cached by query hash. Re-slice the cache with a MongoDB-style `filter={...}` kwarg on `to_dataframe` / `to_polars` / `to_dataframe_batches` — no extra round trips. |
| 🔀 **DuckDB K-Way Merge** | GIL-free sorting across shards - O(N log K) complexity. |
| 🐻‍❄️ **Pandas & Polars** | `to_dataframe()` for pandas, `to_polars()` for Polars. |
| 📊 **Memory Control** | Set `flush_ram_limit_mb` to prevent OOM errors on large datasets. |
| 📤 **Stream to Data Lakes** | `stream_to_callback()` for S3/GCS ingestion pipelines. |

---

## Performance

| Metric | Improvement |
|--------|-------------|
| Query Speed | Up to **4x faster** on large result sets |
| Compression | **10-12x** storage reduction with Parquet |
| Memory | Configurable limits prevent OOM |
| Repeat Queries | **Instant** from cache |

---

## Documentation

📖 **Full documentation with architecture diagrams**: [GitHub Repository](https://github.com/XLR8-DB/xlr8)

---

## License

Apache 2.0 - See [LICENSE](https://github.com/XLR8-DB/xlr8/blob/main/LICENSE) for details.
