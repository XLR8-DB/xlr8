"""Type stubs for XLR8 Cursor.

Provides IDE autocomplete for all PyMongo cursor methods plus XLR8 extensions.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

import pandas as pd
import polars as pl
import pyarrow as pa
from bson.code import Code
from pymongo.cursor_shared import _Hint, _Sort
from pymongo.synchronous.client_session import ClientSession
from pymongo.synchronous.collection import Collection
from pymongo.synchronous.cursor import Cursor as PyMongoCursor
from pymongo.typings import _CollationIn

_DocumentType = TypeVar("_DocumentType", bound=Mapping[str, Any])

class XLR8Cursor(Generic[_DocumentType]):
    """PyMongo-compatible cursor with optional acceleration.

    All PyMongo cursor methods work via delegation.
    Adds 4 XLR8-specific methods for DataFrame conversion.
    """

    def __init__(
        self,
        collection: Any,  # XLR8Collection
        query_filter: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = None,
        skip: int = 0,
        limit: int = 0,
        sort: Optional[List[Tuple[str, int]]] = None,
        batch_size: int = 1000,
        # PyMongo compatibility parameters (passed through to PyMongo cursor)
        no_cursor_timeout: bool = False,
        cursor_type: int = ...,
        allow_partial_results: bool = False,
        oplog_replay: bool = False,
        collation: Optional[Dict[str, Any]] = None,
        hint: Optional[Any] = None,
        max_scan: Optional[int] = None,
        max_time_ms: Optional[int] = None,
        max: Optional[List[Tuple[str, Any]]] = None,
        min: Optional[List[Tuple[str, Any]]] = None,
        return_key: Optional[bool] = None,
        show_record_id: Optional[bool] = None,
        snapshot: Optional[bool] = None,
        comment: Optional[Any] = None,
        session: Optional[Any] = None,
        allow_disk_use: Optional[bool] = None,
        let: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def collection(self) -> Collection[_DocumentType]: ...
    @property
    def retrieved(self) -> int: ...
    def clone(self) -> XLR8Cursor[_DocumentType]: ...
    def add_option(self, mask: int) -> XLR8Cursor[_DocumentType]: ...
    def remove_option(self, mask: int) -> XLR8Cursor[_DocumentType]: ...
    def allow_disk_use(self, allow_disk_use: bool) -> XLR8Cursor[_DocumentType]: ...
    def limit(self, limit: int) -> XLR8Cursor[_DocumentType]: ...
    def batch_size(self, batch_size: int) -> XLR8Cursor[_DocumentType]: ...
    def skip(self, skip: int) -> XLR8Cursor[_DocumentType]: ...
    def max_time_ms(self, max_time_ms: int | None) -> XLR8Cursor[_DocumentType]: ...
    def max_await_time_ms(
        self, max_await_time_ms: int | None
    ) -> XLR8Cursor[_DocumentType]: ...
    @overload
    def __getitem__(self, index: int) -> _DocumentType: ...
    @overload
    def __getitem__(self, index: slice) -> XLR8Cursor[_DocumentType]: ...
    def max_scan(self, max_scan: int | None) -> XLR8Cursor[_DocumentType]: ...
    def max(self, spec: _Sort) -> XLR8Cursor[_DocumentType]: ...
    def min(self, spec: _Sort) -> XLR8Cursor[_DocumentType]: ...
    def sort(
        self, key_or_list: _Hint, direction: int | str | None = None
    ) -> XLR8Cursor[_DocumentType]: ...
    def explain(self) -> _DocumentType: ...
    def hint(self, index: _Hint | None) -> XLR8Cursor[_DocumentType]: ...
    def comment(self, comment: Any) -> XLR8Cursor[_DocumentType]: ...
    def where(self, code: str | Code) -> XLR8Cursor[_DocumentType]: ...
    def collation(
        self, collation: _CollationIn | None
    ) -> XLR8Cursor[_DocumentType]: ...
    @property
    def alive(self) -> bool: ...
    @property
    def cursor_id(self) -> int | None: ...
    @property
    def address(self) -> tuple[str, Any] | None: ...
    @property
    def session(self) -> ClientSession | None: ...
    def close(self) -> None: ...
    def distinct(self, key: str) -> list[Any]: ...
    def rewind(self) -> XLR8Cursor[_DocumentType]: ...
    def next(self) -> _DocumentType: ...
    def __next__(self) -> _DocumentType: ...
    def __iter__(self) -> XLR8Cursor[_DocumentType]: ...
    def __enter__(self) -> XLR8Cursor[_DocumentType]: ...
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...
    def to_list(self, length: int | None = None) -> list[_DocumentType]: ...

    # XLR8-specific accelerated methods
    def to_dataframe(
        self,
        accelerate: bool = True,
        cache_read: bool = True,
        cache_write: bool = True,
        start_date: Optional[Union[datetime, date, str]] = None,
        end_date: Optional[Union[datetime, date, str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        flush_ram_limit_mb: int = 512,
    ) -> pd.DataFrame: ...
    def to_polars(
        self,
        accelerate: bool = True,
        cache_read: bool = True,
        cache_write: bool = True,
        start_date: Optional[Union[datetime, date, str]] = None,
        end_date: Optional[Union[datetime, date, str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
        flush_ram_limit_mb: int = 512,
    ) -> pl.DataFrame: ...
    def to_dataframe_batches(
        self,
        batch_size: int = 10000,
        cache_read: bool = True,
        cache_write: bool = True,
        start_date: Optional[Union[datetime, date, str]] = None,
        end_date: Optional[Union[datetime, date, str]] = None,
        filter: Optional[Dict[str, Any]] = None,
        coerce: Literal["raise", "error"] = "raise",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        flush_ram_limit_mb: int = 512,
    ) -> Generator[pd.DataFrame, None, None]: ...
    def stream_to_callback(
        self,
        callback: Callable[[pa.Table, Dict[str, Any]], None],
        *,
        partition_time_delta: timedelta,
        partition_by: Optional[Union[str, List[str]]] = None,
        any_type_strategy: Literal["float", "string", "keep_struct"] = "float",
        max_workers: int = 4,
        chunking_granularity: Optional[timedelta] = None,
        row_group_size: Optional[int] = None,
        flush_ram_limit_mb: int = 512,
        cache_read: bool = True,
        cache_write: bool = True,
    ) -> Dict[str, Any]: ...
    def raw_cursor(self) -> PyMongoCursor[_DocumentType]: ...
    def explain_acceleration(self) -> Dict[str, Any]: ...
