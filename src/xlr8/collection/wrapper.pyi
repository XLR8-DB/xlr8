"""Type stubs for XLR8 collection wrapper - provides IDE autocomplete."""

from typing import Any, Dict, List, Optional, Tuple, Union

from pymongo.collection import Collection as PyMongoCollection
from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)

from .cursor import XLR8Cursor

class XLR8Collection:
    """
    XLR8 accelerated collection - drop-in replacement for PyMongo collection.

    Supports all PyMongo collection methods via delegation, with accelerated .find()
    that returns XLR8Cursor for parallel query execution.

    For direct access to underlying PyMongo collection, use .raw_collection().
    """

    def __init__(
        self,
        pymongo_collection: PyMongoCollection,
        schema: Optional[Any] = ...,
        mongo_uri: Optional[str] = ...,
        approx_document_size_bytes: int = ...,
    ) -> None: ...
    def find(
        self,
        filter: Optional[Dict[str, Any]] = ...,
        projection: Optional[Dict[str, Any]] = ...,
        skip: int = ...,
        limit: int = ...,
        sort: Optional[List[Tuple[str, int]]] = ...,
        batch_size: int = ...,
    ) -> XLR8Cursor: ...
    def raw_collection(self) -> PyMongoCollection: ...
    @property
    def name(self) -> str: ...
    @property
    def full_name(self) -> str: ...
    @property
    def database(self) -> Any: ...
    def insert_one(
        self,
        document: Dict[str, Any],
        bypass_document_validation: bool = ...,
        session: Optional[Any] = ...,
    ) -> InsertOneResult: ...
    def insert_many(
        self,
        documents: List[Dict[str, Any]],
        ordered: bool = ...,
        bypass_document_validation: bool = ...,
        session: Optional[Any] = ...,
    ) -> InsertManyResult: ...
    def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = ...,
        bypass_document_validation: bool = ...,
        collation: Optional[Dict[str, Any]] = ...,
        array_filters: Optional[List[Dict[str, Any]]] = ...,
        hint: Optional[Union[str, List[Tuple[str, int]]]] = ...,
        session: Optional[Any] = ...,
    ) -> UpdateResult: ...
    def update_many(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = ...,
        array_filters: Optional[List[Dict[str, Any]]] = ...,
        bypass_document_validation: bool = ...,
        collation: Optional[Dict[str, Any]] = ...,
        hint: Optional[Union[str, List[Tuple[str, int]]]] = ...,
        session: Optional[Any] = ...,
    ) -> UpdateResult: ...
    def replace_one(
        self,
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
        upsert: bool = ...,
        bypass_document_validation: bool = ...,
        collation: Optional[Dict[str, Any]] = ...,
        hint: Optional[Union[str, List[Tuple[str, int]]]] = ...,
        session: Optional[Any] = ...,
    ) -> UpdateResult: ...
    def delete_one(
        self,
        filter: Dict[str, Any],
        collation: Optional[Dict[str, Any]] = ...,
        hint: Optional[Union[str, List[Tuple[str, int]]]] = ...,
        session: Optional[Any] = ...,
    ) -> DeleteResult: ...
    def delete_many(
        self,
        filter: Dict[str, Any],
        collation: Optional[Dict[str, Any]] = ...,
        hint: Optional[Union[str, List[Tuple[str, int]]]] = ...,
        session: Optional[Any] = ...,
    ) -> DeleteResult: ...
    def find_one(
        self,
        filter: Optional[Dict[str, Any]] = ...,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]: ...
    def find_one_and_delete(
        self,
        filter: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = ...,
        sort: Optional[List[Tuple[str, int]]] = ...,
        hint: Optional[Union[str, List[Tuple[str, int]]]] = ...,
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]: ...
    def find_one_and_replace(
        self,
        filter: Dict[str, Any],
        replacement: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = ...,
        sort: Optional[List[Tuple[str, int]]] = ...,
        upsert: bool = ...,
        return_document: bool = ...,
        hint: Optional[Union[str, List[Tuple[str, int]]]] = ...,
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]: ...
    def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = ...,
        sort: Optional[List[Tuple[str, int]]] = ...,
        upsert: bool = ...,
        return_document: bool = ...,
        array_filters: Optional[List[Dict[str, Any]]] = ...,
        hint: Optional[Union[str, List[Tuple[str, int]]]] = ...,
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> Optional[Dict[str, Any]]: ...
    def count_documents(
        self,
        filter: Dict[str, Any],
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> int: ...
    def estimated_document_count(self, **kwargs: Any) -> int: ...
    def distinct(
        self,
        key: str,
        filter: Optional[Dict[str, Any]] = ...,
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> List[Any]: ...
    def aggregate(
        self,
        pipeline: List[Dict[str, Any]],
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> Any: ...
    def bulk_write(
        self,
        requests: List[Any],
        ordered: bool = ...,
        bypass_document_validation: bool = ...,
        session: Optional[Any] = ...,
    ) -> BulkWriteResult: ...
    def create_index(
        self,
        keys: Union[str, List[Tuple[str, int]]],
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> str: ...
    def create_indexes(
        self,
        indexes: List[Any],
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> List[str]: ...
    def drop_index(
        self,
        index_or_name: Union[str, List[Tuple[str, int]]],
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> None: ...
    def drop_indexes(self, session: Optional[Any] = ..., **kwargs: Any) -> None: ...
    def list_indexes(self, session: Optional[Any] = ..., **kwargs: Any) -> Any: ...
    def index_information(
        self, session: Optional[Any] = ..., **kwargs: Any
    ) -> Dict[str, Any]: ...
    def drop(self, session: Optional[Any] = ..., **kwargs: Any) -> None: ...
    def rename(
        self,
        new_name: str,
        session: Optional[Any] = ...,
        **kwargs: Any,
    ) -> Dict[str, Any]: ...
    def options(
        self, session: Optional[Any] = ..., **kwargs: Any
    ) -> Dict[str, Any]: ...
    def __getattr__(self, name: str) -> Any: ...

def accelerate(
    pymongo_collection: PyMongoCollection,
    schema: Any,
    mongo_uri: Union[str, Any],
    cache_dir: Optional[str] = ...,
    enable_cache: bool = ...,
    metadata_cardinality: int = ...,
    approx_document_size_bytes: int = ...,
) -> XLR8Collection: ...
