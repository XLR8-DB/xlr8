"""Type stubs for XLR8 Cursor - provides full IDE autocomplete and type safety.

This stub intentionally exposes the full PyMongo Cursor surface area via typing
inheritance, since XLR8Cursor delegates unsupported methods/attributes to the
underlying PyMongo cursor at runtime (typically via __getattr__).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pandas as pd
import polars as pl
import pyarrow as pa
from pymongo.cursor import Cursor as PyMongoCursor
from pymongo.cursor import CursorType

class XLR8Cursor(PyMongoCursor):
    """PyMongo-compatible cursor with optional acceleration.

    Notes:
    - This type stub subclasses PyMongoCursor to provide full autocomplete for
      PyMongo cursor methods.
    - At runtime, XLR8Cursor may delegate to an internal PyMongo cursor.
    - For direct access to the underlying PyMongo cursor, use raw_cursor().
    """

    def __init__(
        self,
        collection: Any,
        query_filter: Dict[str, Any],
        projection: Optional[Dict[str, Any]] = ...,
        skip: int = ...,
        limit: int = ...,
        sort: Optional[List[Tuple[str, int]]] = ...,
        batch_size: int = ...,
    ) -> None: ...

    # =========================================================================
    # XLR8 Accelerated Methods
    # =========================================================================

    def to_dataframe(
        self,
        accelerate: bool = ...,
        cache_read: bool = ...,
        cache_write: bool = ...,
        start_date: Optional[Union[datetime, date, str]] = ...,
        end_date: Optional[Union[datetime, date, str]] = ...,
        coerce: Literal["raise", "error"] = ...,
        max_workers: int = ...,
        chunking_granularity: Optional[timedelta] = ...,
        row_group_size: Optional[int] = ...,
        flush_ram_limit_mb: int = ...,
    ) -> "pd.DataFrame": ...
    def to_polars(
        self,
        accelerate: bool = ...,
        cache_read: bool = ...,
        cache_write: bool = ...,
        start_date: Optional[Union[datetime, date, str]] = ...,
        end_date: Optional[Union[datetime, date, str]] = ...,
        coerce: Literal["raise", "error"] = ...,
        max_workers: int = ...,
        chunking_granularity: Optional[timedelta] = ...,
        row_group_size: Optional[int] = ...,
        any_type_strategy: Literal["float", "string", "keep_struct"] = ...,
        flush_ram_limit_mb: int = ...,
    ) -> "pl.DataFrame": ...
    def to_dataframe_batches(
        self,
        batch_size: int = ...,
        cache_read: bool = ...,
        cache_write: bool = ...,
        start_date: Optional[Union[datetime, date, str]] = ...,
        end_date: Optional[Union[datetime, date, str]] = ...,
        coerce: Literal["raise", "error"] = ...,
        max_workers: int = ...,
        chunking_granularity: Optional[timedelta] = ...,
        row_group_size: Optional[int] = ...,
        flush_ram_limit_mb: int = ...,
    ) -> Generator["pd.DataFrame", None, None]: ...
    def stream_to_callback(
        self,
        callback: Callable[["pa.Table", Dict[str, Any]], None],
        *,
        partition_time_delta: timedelta,
        partition_by: Optional[Union[str, List[str]]] = ...,
        any_type_strategy: Literal["float", "string", "keep_struct"] = ...,
        max_workers: int = ...,
        chunking_granularity: Optional[timedelta] = ...,
        row_group_size: Optional[int] = ...,
        flush_ram_limit_mb: int = ...,
        cache_read: bool = ...,
        cache_write: bool = ...,
    ) -> Dict[str, Any]: ...
    def explain_acceleration(self) -> Dict[str, Any]: ...
    def raw_cursor(self) -> PyMongoCursor: ...

    # =========================================================================
    # Delegation escape hatch
    # =========================================================================

    def __getattr__(self, name: str) -> Any: ...
    def __iter__(self) -> Iterator[Dict[str, Any]]: ...
    def __next__(self) -> Dict[str, Any]: ...
    def __enter__(self) -> "XLR8Cursor": ...
    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None: ...

    # =========================================================================
    # Common PyMongo Cursor API (typed overrides for chaining)
    # =========================================================================

    def next(self) -> Dict[str, Any]: ...
    def clone(self) -> "XLR8Cursor": ...
    def rewind(self) -> "XLR8Cursor": ...
    def close(self) -> None: ...
    @property
    def alive(self) -> bool: ...
    @property
    def cursor_id(self) -> Optional[int]: ...
    @property
    def address(self) -> Optional[Tuple[str, int]]: ...
    @property
    def session(self) -> Optional[Any]: ...

    # Cursor modifiers (return self for chaining)
    def add_option(self, mask: int) -> "XLR8Cursor": ...
    def remove_option(self, mask: int) -> "XLR8Cursor": ...
    def allow_disk_use(self, allow: bool = ...) -> "XLR8Cursor": ...
    def batch_size(self, batch_size: int) -> "XLR8Cursor": ...
    def collation(self, collation: Dict[str, Any]) -> "XLR8Cursor": ...
    def comment(self, comment: str) -> "XLR8Cursor": ...
    def hint(self, index: Union[str, List[Tuple[str, int]]]) -> "XLR8Cursor": ...
    def limit(self, limit: int) -> "XLR8Cursor": ...
    def max(self, spec: List[Tuple[str, Any]]) -> "XLR8Cursor": ...
    def max_await_time_ms(self, max_await_time_ms: int) -> "XLR8Cursor": ...
    def max_scan(self, max_scan: int) -> "XLR8Cursor": ...
    def max_time_ms(self, max_time_ms: int) -> "XLR8Cursor": ...
    def min(self, spec: List[Tuple[str, Any]]) -> "XLR8Cursor": ...
    def skip(self, skip: int) -> "XLR8Cursor": ...
    def sort(
        self,
        key_or_list: Union[str, List[Tuple[str, int]]],
        direction: Optional[int] = ...,
    ) -> "XLR8Cursor": ...
    def where(self, code: str) -> "XLR8Cursor": ...

    # CursorType helpers
    def cursor_type(self, cursor_type: CursorType) -> "XLR8Cursor": ...

    # Query execution
    def count(self, with_limit_and_skip: bool = ...) -> int: ...
    def distinct(self, key: str) -> List[Any]: ...
    def explain(self) -> Dict[str, Any]: ...
