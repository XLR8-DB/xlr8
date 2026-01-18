"""
XLR8 collection wrapper with PyMongo compatibility.

================================================================================
DATA FLOW - COLLECTION WRAPPER
================================================================================

This module wraps pymongo.collection.Collection to provide the `accelerate()`
function - the main entry point for users.

TYPICAL USAGE FLOW:
────────────────────────────────────────────────────────────────────────────────

1. USER WRAPS A COLLECTION:
┌─────────────────────────────────────────────────────────────────────────────┐
│  from xlr8 import accelerate, Schema, Types                                 │
│                                                                             │
│  schema = Schema(                                                           │
│      time_field="timestamp",                                                │
│      fields={                                                               │
│          "timestamp": Types.Timestamp("ms", tz="UTC"),                      │
│          "metadata.device_id": Types.ObjectId(),                            │
│          "metadata.sensor_id": Types.ObjectId(),                         │
│          "value": Types.Any(),  # Polymorphic - can be int, float, str etc..│
│      }                                                                      │
│  )                                                                          │
│                                                                             │
│  xlr8_col = accelerate(pymongo_collection, schema, mongo_uri)               │
└─────────────────────────────────────────────────────────────────────────────┘

2. USER CALLS find() - RETURNS XLR8Cursor (NOT PYMONGO CURSOR):
┌─────────────────────────────────────────────────────────────────────────────┐
│  cursor = xlr8_col.find({                                                   │
│      "timestamp": {"$gte": start, "$lt": end},                              │
│      "metadata.device_id": ObjectId("64a..."),                              │
│  })                                                                         │
│  # cursor is XLR8Cursor, wrapping the query params                          │
└─────────────────────────────────────────────────────────────────────────────┘

3. USER CALLS to_dataframe() - TRIGGERS ACCELERATION:
┌─────────────────────────────────────────────────────────────────────────────┐
│  df = cursor.to_dataframe()                                                 │
│  # This triggers:                                                           │
│  # 1. Query analysis (can we chunk by time?)                                │
│  # 2. Check cache (have we fetched this before?)                            │
│  # 3. Parallel fetch via Rust async backend                               │
│  # 4. Stream to Parquet cache                                               │
│  # 5. Read back and return DataFrame                                        │
└─────────────────────────────────────────────────────────────────────────────┘

KEY CONFIG OPTIONS:
────────────────────────────────────────────────────────────────────────────────
- schema: Required for type-aware encoding (especially Types.Any)
- mongo_uri: Required for accelerated execution (workers create connections)
- cache_dir: Where to store Parquet cache (default: .xlr8_cache)

PER-QUERY OPTIONS (via to_dataframe):
────────────────────────────────────────────────────────────────────────────────
- max_workers: Number of parallel workers (default: 4)
- flush_ram_limit_mb: RAM budget for batch sizing (default: 512)
- chunking_granularity: Time chunk size (e.g., timedelta(days=7))

================================================================================
"""

from typing import Any, Callable, Dict, List, Optional, Union

from pymongo.collection import Collection as PyMongoCollection

from xlr8.collection.cursor import XLR8Cursor
from xlr8.schema import Schema


class XLR8Collection:
    """
    PyMongo-compatible collection wrapper with acceleration.

    Drop-in replacement for pymongo.collection.Collection that transparently
    accelerates analytical queries through parallel execution and caching.

    All write operations (insert, update, delete) pass through to PyMongo.
    Read operations (find, aggregate) can be accelerated if:
    - Schema is provided
    - Query has time-range predicates
    - Query doesn't use complex operators ($nor, $where, etc.)

    Example:
        >>> import pymongo
        >>> from xlr8 import XLR8Collection, Schema, Types
        >>>
        >>> # Create schema
        >>> schema = Schema(
        ...     time_field="timestamp",
        ...     fields={
        ...         "timestamp": Types.Timestamp(),
        ...         "value": Types.Float(),
        ...         "sensor_id": Types.String(),
        ...     }
        ... )
        >>>
        >>> # Wrap collection with mongo_uri for accelerated execution
        >>> client = pymongo.MongoClient("mongodb://localhost:27017")
        >>> pymongo_col = client.mydb.mycollection
        >>> col = XLR8Collection(pymongo_col, schema=schema, mongo_uri="mongodb://localhost:27017")
        >>>
        >>> # Use like regular PyMongo
        >>> cursor = col.find({"timestamp": {"$gte": start, "$lt": end}})
        >>> df = cursor.to_dataframe(flush_ram_limit_mb=2000)
    """

    def __init__(
        self,
        pymongo_collection,
        schema: Optional[Schema] = None,
        mongo_uri: Union[str, Callable[[], str], None] = None,
        cache_dir: Optional[str] = None,
        enable_cache: bool = True,
        metadata_cardinality: int = 1,
        approx_document_size_bytes: int = 500,
    ):
        """
        Initialize XLR8 collection wrapper.

        Args:
            pymongo_collection: PyMongo Collection instance
            schema: Optional schema definition for acceleration
            mongo_uri: MongoDB connection string (str) or callable that returns one.
                       Required for accelerated execution. Can be:
                       - A string: "mongodb://localhost:27017"
                       - A callable: lambda: os.environ["MONGODB_URI"]
            cache_dir: Directory for Parquet cache (default: ./.xlr8_cache)
            enable_cache: Enable Parquet caching
            metadata_cardinality: Number of unique metadata combinations
                (e.g., sensor count)
            approx_document_size_bytes: Approximate size of each document in bytes
                (default: 500)

        Note:
            flush_ram_limit_mb and max_workers are parameters of to_dataframe(),
            to_polars(), etc. for per-query control.
        """
        self._pymongo_collection = pymongo_collection
        self._schema = schema
        self._mongo_uri = mongo_uri
        self._cache_dir = cache_dir or "./.xlr8_cache"
        self._enable_cache = enable_cache
        self._metadata_cardinality = metadata_cardinality
        self._approx_document_size_bytes = approx_document_size_bytes

    def raw_collection(self):
        """
        Get direct access to underlying PyMongo collection.

        This is an escape hatch for power users who need direct access to PyMongo
        collection methods that may not be available through delegation.

        Returns:
            pymongo.collection.Collection: The underlying PyMongo collection

        Example:
            >>> xlr8_col = accelerate(collection, schema=schema)
            >>> xlr8_col.raw_collection().watch()  # Use MongoDB change streams
            >>> xlr8_col.raw_collection().list_indexes()  # Direct PyMongo access
        """
        return self._pymongo_collection

    # PyMongo pass-through properties

    @property
    def name(self) -> str:
        """Collection name."""
        return self._pymongo_collection.name

    @property
    def full_name(self) -> str:
        """Full collection name (database.collection)."""
        return self._pymongo_collection.full_name

    @property
    def database(self):
        """Parent database."""
        return self._pymongo_collection.database

    # Public accessor properties for cursor usage

    @property
    def schema(self):
        """Schema definition for acceleration."""
        return self._schema

    @property
    def pymongo_collection(self):
        """Underlying PyMongo collection instance."""
        return self._pymongo_collection

    @property
    def mongo_uri(self):
        """MongoDB connection URI for accelerated execution."""
        return self._mongo_uri

    @property
    def approx_document_size_bytes(self) -> int:
        """Approximate size of each document in bytes."""
        return self._approx_document_size_bytes

    def __getattr__(self, name: str):
        """
        Delegate unknown methods to PyMongo collection.

        Why:
            Provides full PyMongo compatibility without manually implementing
            every collection method (insert, update, delete, indexes, etc.).

        Example:
            >>> xlr8_col.insert_one({...})  # Works via delegation
            >>> xlr8_col.create_index("timestamp")  # Works via delegation
            >>> count = xlr8_col.count_documents({})  # Works via delegation
        """
        return getattr(self._pymongo_collection, name)

    # Read operations (can be accelerated)

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        skip: int = 0,
        limit: int = 0,
        sort: Optional[List[tuple]] = None,
        batch_size: int = 1000,
        **kwargs,
    ) -> XLR8Cursor:
        """
        Query collection with optional acceleration.

        Returns XLR8Cursor which is PyMongo-compatible but can accelerate
        to_dataframe() / to_polars() conversions.

        ┌─────────────────────────────────────────────────────────────────────┐
        │ DATA FLOW EXAMPLE:                                                  │
        │                                                                     │
        │ INPUT (filter parameter):                                           │
        │ {                                                                   │
        │     "$or": [                                                        │
        │         {"metadata.sensor_id": ObjectId("64a...")},              │
        │         {"metadata.sensor_id": ObjectId("64b...")},              │
        │     ],                                                              │
        │     "timestamp": {"$gte": datetime(2024,1,1), "$lt": datetime(...)} │
        │ }                                                                   │
        │                                                                     │
        │ OUTPUT: XLR8Cursor object containing:                               │
        │ - _filter: The query dict (unchanged)                               │
        │ - _collection: Reference back to this XLR8Collection                │
        │ - _projection, _skip, _limit, _sort: Query modifiers                │
        │                                                                     │
        │ NEXT STEP: User calls cursor.to_dataframe() which triggers:         │
        │ 1. Query analysis in analysis/brackets.py                           │
        │ 2. Execution planning in execution/planner.py                       │
        │ 3. Parallel fetch in execution/worker.py                            │
        └─────────────────────────────────────────────────────────────────────┘

        Args:
            filter: Query filter dict
            projection: Field projection dict
            skip: Number of documents to skip
            limit: Maximum documents to return
            sort: Sort specification
            batch_size: Batch size for iteration
            **kwargs: Additional PyMongo cursor options

        Returns:
            XLR8Cursor instance

        Example:
            >>> # Simple query
            >>> cursor = col.find({"status": "active"})
            >>>
            >>> # Query with time range (accelerated)
            >>> cursor = col.find({
            ...     "timestamp": {"$gte": start, "$lt": end},
            ...     "sensor_id": "sensor_1"
            ... })
            >>> df = cursor.to_dataframe()
        """
        if filter is None:
            filter = {}

        return XLR8Cursor(
            collection=self,
            query_filter=filter,
            projection=projection,
            skip=skip,
            limit=limit,
            sort=sort,
            batch_size=batch_size,
        )

    def find_one(
        self,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Optional[Dict[str, Any]]:
        """
        Get single document.

        Pass-through to PyMongo (no acceleration).

        Args:
            filter: Query filter
            projection: Field projection
            **kwargs: Additional options

        Returns:
            Document dict or None
        """
        return self._pymongo_collection.find_one(
            filter=filter, projection=projection, **kwargs
        )

    def count_documents(self, filter: Dict[str, Any], **kwargs) -> int:
        """
        Count matching documents.

        Pass-through to PyMongo (no acceleration).

        Args:
            filter: Query filter
            **kwargs: Additional options

        Returns:
            Document count
        """
        return self._pymongo_collection.count_documents(filter, **kwargs)

    def estimated_document_count(self, **kwargs) -> int:
        """
        Get estimated total document count.

        Pass-through to PyMongo.

        Returns:
            Estimated count
        """
        return self._pymongo_collection.estimated_document_count(**kwargs)

    def distinct(
        self, key: str, filter: Optional[Dict[str, Any]] = None, **kwargs
    ) -> List[Any]:
        """
        Get distinct values.

        Pass-through to PyMongo (no acceleration).

        Args:
            key: Field name
            filter: Query filter
            **kwargs: Additional options

        Returns:
            List of distinct values
        """
        return self._pymongo_collection.distinct(key, filter=filter, **kwargs)

    def aggregate(self, pipeline: List[Dict[str, Any]], **kwargs):
        """
        Run aggregation pipeline.

        Pass-through to PyMongo (no acceleration for now).

        TODO: Implement acceleration for time-range aggregations.

        Args:
            pipeline: Aggregation pipeline
            **kwargs: Additional options

        Returns:
            PyMongo CommandCursor
        """
        return self._pymongo_collection.aggregate(pipeline, **kwargs)

    # Write operations (pass-through to PyMongo)

    def insert_one(self, document: Dict[str, Any], **kwargs):
        """
        Insert single document.

        Pass-through to PyMongo.

        Args:
            document: Document to insert
            **kwargs: Additional options

        Returns:
            InsertOneResult
        """
        return self._pymongo_collection.insert_one(document, **kwargs)

    def insert_many(self, documents: List[Dict[str, Any]], **kwargs):
        """
        Insert multiple documents.

        Pass-through to PyMongo.

        Args:
            documents: Documents to insert
            **kwargs: Additional options

        Returns:
            InsertManyResult
        """
        return self._pymongo_collection.insert_many(documents, **kwargs)

    def update_one(self, filter: Dict[str, Any], update: Dict[str, Any], **kwargs):
        """
        Update single document.

        Pass-through to PyMongo.

        Args:
            filter: Query filter
            update: Update operations
            **kwargs: Additional options

        Returns:
            UpdateResult
        """
        return self._pymongo_collection.update_one(filter, update, **kwargs)

    def update_many(self, filter: Dict[str, Any], update: Dict[str, Any], **kwargs):
        """
        Update multiple documents.

        Pass-through to PyMongo.

        Args:
            filter: Query filter
            update: Update operations
            **kwargs: Additional options

        Returns:
            UpdateResult
        """
        return self._pymongo_collection.update_many(filter, update, **kwargs)

    def replace_one(
        self, filter: Dict[str, Any], replacement: Dict[str, Any], **kwargs
    ):
        """
        Replace single document.

        Pass-through to PyMongo.

        Args:
            filter: Query filter
            replacement: Replacement document
            **kwargs: Additional options

        Returns:
            UpdateResult
        """
        return self._pymongo_collection.replace_one(filter, replacement, **kwargs)

    def delete_one(self, filter: Dict[str, Any], **kwargs):
        """
        Delete single document.

        Pass-through to PyMongo.

        Args:
            filter: Query filter
            **kwargs: Additional options

        Returns:
            DeleteResult
        """
        return self._pymongo_collection.delete_one(filter, **kwargs)

    def delete_many(self, filter: Dict[str, Any], **kwargs):
        """
        Delete multiple documents.

        Pass-through to PyMongo.

        Args:
            filter: Query filter
            **kwargs: Additional options

        Returns:
            DeleteResult
        """
        return self._pymongo_collection.delete_many(filter, **kwargs)

    # Index operations (pass-through)

    def create_index(self, keys, **kwargs):
        """Create index. Pass-through to PyMongo."""
        return self._pymongo_collection.create_index(keys, **kwargs)

    def create_indexes(self, indexes, **kwargs):
        """Create multiple indexes. Pass-through to PyMongo."""
        return self._pymongo_collection.create_indexes(indexes, **kwargs)

    def drop_index(self, index_or_name, **kwargs):
        """Drop index. Pass-through to PyMongo."""
        return self._pymongo_collection.drop_index(index_or_name, **kwargs)

    def drop_indexes(self, **kwargs):
        """Drop all indexes. Pass-through to PyMongo."""
        return self._pymongo_collection.drop_indexes(**kwargs)

    def list_indexes(self, **kwargs):
        """List indexes. Pass-through to PyMongo."""
        return self._pymongo_collection.list_indexes(**kwargs)

    def index_information(self, **kwargs):
        """Get index information. Pass-through to PyMongo."""
        return self._pymongo_collection.index_information(**kwargs)

    # Collection operations

    def drop(self, **kwargs):
        """Drop collection. Pass-through to PyMongo."""
        return self._pymongo_collection.drop(**kwargs)

    def rename(self, new_name: str, **kwargs):
        """Rename collection. Pass-through to PyMongo."""
        return self._pymongo_collection.rename(new_name, **kwargs)

    # XLR8-specific methods

    def set_schema(self, schema: Schema) -> None:
        """
        Set or update schema for acceleration.

        Args:
            schema: Schema definition
        """
        self._schema = schema

    def get_schema(self) -> Optional[Schema]:
        """
        Get current schema.

        Returns:
            Schema or None
        """
        return self._schema

    def clear_cache(self) -> None:
        """
        Clear Parquet cache for this collection.

        TODO: Implement in storage layer.
        """
        # Will be implemented in storage milestone
        pass


def accelerate(
    pymongo_collection: PyMongoCollection,
    schema: Schema,
    mongo_uri: Union[str, Callable[[], str]],
    cache_dir: Optional[str] = None,
    enable_cache: bool = True,
    metadata_cardinality: int = 1,
    approx_document_size_bytes: int = 500,
) -> XLR8Collection:
    """
    Convenience function to wrap a PyMongo collection with acceleration.

    ┌─────────────────────────────────────────────────────────────────────────┐
    │ DATA FLOW EXAMPLE - MAIN ENTRY POINT:                                   │
    │                                                                         │
    │ INPUT:                                                                  │
    │ - pymongo_collection: client["main"]["sensorLogs"]                      │
    │ - schema: Schema(time_field="timestamp", fields={...})                  │
    │ - mongo_uri: Connection string used by accelerated workers              │
    │                                                                         │
    │ Example:                                                                │
    │ accelerate(                                                             │
    │     collection,                                                         │
    │     schema,                                                             │
    │     mongo_uri="mongodb://localhost:27017",  # Or callable               │
    │ )                                                                       │
    │                                                                         │
    │ OUTPUT: XLR8Collection wrapper that:                                    │
    │ - Wraps pymongo collection for transparent pass-through                 │
    │ - Stores schema for type-aware Parquet encoding                         │
    │ - Stores mongo_uri for workers to create their own connections          │
    │                                                                         │
    │ WHAT HAPPENS NEXT:                                                      │
    │ 1. User calls: xlr8_col.find({...})                                     │
    │ 2. Returns XLR8Cursor (wraps query params)                              │
    │ 3. User calls: cursor.to_dataframe()                                    │
    │ 4. Workers use mongo_uri to create their own connections                │
    └─────────────────────────────────────────────────────────────────────────┘

    Args:
        pymongo_collection: PyMongo Collection instance
        schema: Schema definition
        mongo_uri: MongoDB connection string (str) or callable that returns one.
                   Required for accelerated execution. Can be:
                   - A string: "mongodb://localhost:27017"
                   - A callable: lambda: os.environ["MONGODB_URI"]
        cache_dir: Cache directory (default: .xlr8_cache)
        enable_cache: Enable caching
        metadata_cardinality: Number of unique metadata combinations
            (e.g., number of sensors)
        approx_document_size_bytes: Approximate size of each document in bytes
            (default: 500)

    Returns:
        XLR8Collection wrapper

    Note:
        flush_ram_limit_mb and max_workers are parameters of to_dataframe(),
        to_polars(), etc. for per-query control.

    Example:
        >>> import pymongo
        >>> from xlr8 import accelerate, Schema, Types
        >>>
        >>> # Connection string or callable
        >>> MONGO_URI = "mongodb://localhost:27017"
        >>> # OR: get_uri = lambda: os.environ["MONGODB_URI"]
        >>>
        >>> client = pymongo.MongoClient(MONGO_URI)
        >>> col = client.mydb.sensor_logs
        >>>
        >>> schema = Schema(
        ...     time_field="timestamp",
        ...     fields={
        ...         "timestamp": Types.Timestamp(),
        ...         "sensor_id": Types.String(),
        ...         "value": Types.Float(),
        ...     },
        ... )
        >>>
        >>> # Pass mongo_uri for accelerated workers
        >>> accelerated_col = accelerate(col, schema, mongo_uri=MONGO_URI)
        >>>
        >>> # max_workers and flush_ram_limit_mb are per-query
        >>> from datetime import timedelta
        >>> df = accelerated_col.find({
        ...     "timestamp": {"$gte": start, "$lt": end}
        ... }).to_dataframe(
        ...     max_workers=8,
        ...     chunking_granularity=timedelta(days=1),
        ...     flush_ram_limit_mb=2000,
        ... )
    """
    return XLR8Collection(
        pymongo_collection=pymongo_collection,
        schema=schema,
        mongo_uri=mongo_uri,
        cache_dir=cache_dir,
        enable_cache=enable_cache,
        metadata_cardinality=metadata_cardinality,
        approx_document_size_bytes=approx_document_size_bytes,
    )
