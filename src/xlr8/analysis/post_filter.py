"""MQL post-filter: query the Parquet cache with a MongoDB-style filter.

WHAT THIS IS
============
The XLR8 cursor populates a Parquet cache via ``.find(query).to_dataframe(...)``.
The cache is keyed by the ``find()`` filter and is typically a superset of what
any single caller actually wants to work with. This module lets callers narrow
that cache AT READ TIME without going back to MongoDB::

    cursor = xlr8_col.find({"recordedAt": {"$gte": d1, "$lt": d2},
                            "metadata.sensor_id": {"$in": [oid1, oid2, oid3]}})
    cursor.to_dataframe(...)                                          # fetches
    cursor.to_dataframe(filter={"metadata.sensor_id": oid1})       # narrows
    cursor.to_dataframe(filter={"metadata.sensor_id": oid2})       # narrows

The filter is a subset of MongoDB Query Language (MQL) that we can confidently
translate into push-down predicates against Parquet. What we support:

    Logical:     $and, $or, $nor, $not
    Equality:    bare value, $eq, $ne
    Set:         $in, $nin
    Comparison:  $gt, $gte, $lt, $lte  (on orderable scalar types only)

What we reject (fast, with a clear error): $regex, $exists, $type, $size,
$all, $elemMatch, $mod, $bits*, $expr, $text, $near, geospatial — anything
that is either hard to translate or could silently disagree with MongoDB.

HYBRID PUSH-DOWN
================
Fields declared in the schema as ``Types.Any()`` are decoded at the reader
boundary into a single polymorphic value per row. We cannot push down
predicates on them through PyArrow / Polars / DuckDB — the struct layout is
row-dependent. Clauses on typed fields (scalars, typed struct leaves) ARE
pushable.

We split the parsed AST into ``typed_ast`` (pushable) and ``any_ast``
(post-decode). An ``$and`` is freely splittable. ``$or`` / ``$not`` are all
or nothing: if any descendant references an Any() field, the whole subtree
must be applied post-decode.

TIME-FIELD PRECEDENCE
=====================
When the caller passes ``start_date=`` / ``end_date=``, those override any
time-field clause in the filter (same semantics as the existing kwargs). We
strip time-field clauses during parsing, but only in contexts where stripping
preserves meaning (top-level implicit-And, or inside explicit $and). If a
time-field clause appears inside $or or $not, we raise — silently substituting
tautology there would change the query semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from bson import ObjectId

from xlr8.schema.schema import ResolvedField, Schema
from xlr8.schema.types import ObjectId as ObjectIdType


class XLR8FilterError(ValueError):
    """Raised when an MQL post-filter cannot be compiled.

    Reasons include unsupported operators, unknown fields, non-orderable
    comparisons, time-field clauses conflicting with start_date/end_date, or
    malformed filter shapes.
    """


# Operators recognized at a leaf. Values mirror MongoDB names exactly.
_LEAF_OPS: Set[str] = {"$eq", "$ne", "$in", "$nin", "$gt", "$gte", "$lt", "$lte"}
_COMPARISON_OPS: Set[str] = {"$gt", "$gte", "$lt", "$lte"}
_SUPPORTED_LEAF_OPS_STR = ", ".join(sorted(_LEAF_OPS))
_SUPPORTED_LOGICAL_OPS_STR = "$and, $or, $nor, $not"


# ---------------------------------------------------------------------------
# AST nodes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Tautology:
    """A node that unconditionally matches every row. Used during
    time-field stripping before the tree is simplified."""


@dataclass(frozen=True)
class Leaf:
    field: str
    op: str  # one of _LEAF_OPS
    value: Any
    is_any: bool  # True iff field resolves to Types.Any() (or lives inside one)


@dataclass(frozen=True)
class AndNode:
    children: Tuple["Node", ...]


@dataclass(frozen=True)
class OrNode:
    children: Tuple["Node", ...]


@dataclass(frozen=True)
class NotNode:
    inner: "Node"


Node = Union[Tautology, Leaf, AndNode, OrNode, NotNode]


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def _parse_filter(
    mql: Dict[str, Any],
    schema: Schema,
    *,
    time_field: str,
    strip_time_field: bool,
    _in_and_context: bool = True,
    _referenced: Optional[Set[str]] = None,
) -> Node:
    """Parse a dict of the shape ``{field: val, ...}`` (implicit AND)."""
    if not isinstance(mql, dict):
        raise XLR8FilterError(
            f"filter must be a dict, got {type(mql).__name__}: {mql!r}"
        )

    if _referenced is None:
        _referenced = set()

    children: List[Node] = []
    for key, value in mql.items():
        if key == "$and":
            children.append(
                _parse_logical_and(
                    value,
                    schema,
                    time_field=time_field,
                    strip_time_field=strip_time_field,
                    referenced=_referenced,
                )
            )
        elif key == "$or":
            children.append(
                _parse_logical_or(
                    value,
                    schema,
                    time_field=time_field,
                    strip_time_field=strip_time_field,
                    referenced=_referenced,
                )
            )
        elif key == "$nor":
            children.append(
                _parse_logical_nor(
                    value,
                    schema,
                    time_field=time_field,
                    strip_time_field=strip_time_field,
                    referenced=_referenced,
                )
            )
        elif key == "$not":
            raise XLR8FilterError(
                "$not must appear inside a field clause "
                "(e.g. {field: {$not: {$eq: ...}}}), not at the top level."
            )
        elif key.startswith("$"):
            raise XLR8FilterError(
                f"logical operator {key!r} is not supported. "
                f"Supported: {_SUPPORTED_LOGICAL_OPS_STR}."
            )
        else:
            # Field clause. Strip-time-field handling happens here.
            if key == time_field and strip_time_field:
                if not _in_and_context:
                    raise XLR8FilterError(
                        f"time-field clause on {time_field!r} appears inside "
                        "$or/$not/$nor while start_date/end_date was also "
                        "supplied. Remove the time-field clause from filter "
                        "or drop start_date/end_date — they conflict in "
                        "non-conjunctive context."
                    )
                # Safe to strip — time constraints will come from start/end.
                children.append(Tautology())
                continue
            children.extend(
                _parse_field_clause(key, value, schema, referenced=_referenced)
            )

    return _collapse_and(children)


def _parse_logical_and(
    value: Any,
    schema: Schema,
    *,
    time_field: str,
    strip_time_field: bool,
    referenced: Set[str],
) -> Node:
    if not isinstance(value, list) or not value:
        raise XLR8FilterError(
            f"$and must be a non-empty list of sub-filters, got: {value!r}"
        )
    parsed_children: List[Node] = []
    for sub in value:
        parsed_children.append(
            _parse_filter(
                sub,
                schema,
                time_field=time_field,
                strip_time_field=strip_time_field,
                _in_and_context=True,
                _referenced=referenced,
            )
        )
    return _collapse_and(parsed_children)


def _parse_logical_or(
    value: Any,
    schema: Schema,
    *,
    time_field: str,
    strip_time_field: bool,
    referenced: Set[str],
) -> Node:
    if not isinstance(value, list) or not value:
        raise XLR8FilterError(
            f"$or must be a non-empty list of sub-filters, got: {value!r}"
        )
    parsed_children: List[Node] = []
    for sub in value:
        parsed_children.append(
            _parse_filter(
                sub,
                schema,
                time_field=time_field,
                strip_time_field=strip_time_field,
                _in_and_context=False,
                _referenced=referenced,
            )
        )
    return OrNode(tuple(parsed_children))


def _parse_logical_nor(
    value: Any,
    schema: Schema,
    *,
    time_field: str,
    strip_time_field: bool,
    referenced: Set[str],
) -> Node:
    """$nor([A, B, ...]) rewrites to AND(NOT A, NOT B, ...)."""
    if not isinstance(value, list) or not value:
        raise XLR8FilterError(
            f"$nor must be a non-empty list of sub-filters, got: {value!r}"
        )
    parsed_children: List[Node] = []
    for sub in value:
        inner = _parse_filter(
            sub,
            schema,
            time_field=time_field,
            strip_time_field=strip_time_field,
            _in_and_context=False,
            _referenced=referenced,
        )
        parsed_children.append(NotNode(inner))
    return _collapse_and(parsed_children)


def _parse_field_clause(
    field_name: str, value: Any, schema: Schema, *, referenced: Set[str]
) -> List[Node]:
    """Parse a single ``{field: value_or_operator_dict}`` clause.

    Returns a list of leaves (a dict with multiple operators produces one
    leaf per operator, implicitly AND'd).
    """
    resolved = schema.resolve_path(field_name)
    if resolved is None:
        raise XLR8FilterError(
            f"field {field_name!r} is not declared in the schema. "
            "Only fields present in Schema(fields={...}) can be filtered."
        )
    referenced.add(field_name)

    # Bare value: treated as $eq against scalar. Lists as bare value are
    # ambiguous (MongoDB treats them as element-in-array match which we don't
    # support) — reject cleanly.
    if not isinstance(value, dict):
        if isinstance(value, list):
            raise XLR8FilterError(
                f"bare list as value for field {field_name!r} is not "
                "supported — use {$in: [...]} for set membership."
            )
        normalized_bare = _normalize_value("$eq", value, resolved)
        return [Leaf(field_name, "$eq", normalized_bare, is_any=resolved.is_any)]

    leaves: List[Node] = []
    for op, operand in value.items():
        if not op.startswith("$"):
            # MongoDB would treat this as a nested-document equality match; we
            # don't support that.
            raise XLR8FilterError(
                f"unexpected key {op!r} inside operator dict for field "
                f"{field_name!r}. Expected operators like $eq, $in, ..."
            )
        if op == "$not":
            if not isinstance(operand, dict):
                raise XLR8FilterError(
                    "$not operand must be an operator dict "
                    f"(e.g. {{$eq: ...}}), got {operand!r}"
                )
            inner_leaves = _parse_field_clause(
                field_name, operand, schema, referenced=referenced
            )
            leaves.append(NotNode(_collapse_and(inner_leaves)))
            continue
        if op not in _LEAF_OPS:
            raise XLR8FilterError(
                f"operator {op!r} is not supported in post-filter "
                f"(field: {field_name}). Supported operators: "
                f"{_SUPPORTED_LEAF_OPS_STR}."
            )
        _validate_operand(op, operand, field_name, resolved)
        normalized = _normalize_value(op, operand, resolved)
        leaves.append(Leaf(field_name, op, normalized, is_any=resolved.is_any))

    if not leaves:
        raise XLR8FilterError(
            f"empty operator dict for field {field_name!r}: {value!r}"
        )
    return leaves


def _normalize_value(op: str, operand: Any, resolved: ResolvedField) -> Any:
    """Coerce operand values into the form actually stored in the Parquet cache.

    - ObjectIds are stored as 24-char hex strings (for both typed ObjectId
      fields and ObjectIds inside a ``Types.Any()`` blob, since the Any
      struct's ``objectid_value`` is a string). Convert ``ObjectId(...)``
      values to ``str(...)`` so comparisons work on both push-down (polars /
      PyArrow / DuckDB see strings) and in-memory post-decode (pandas sees
      ObjectId only for typed fields; for Any it sees strings). For typed
      ObjectId fields the in-memory path never runs (we push down instead).
    """
    is_oid_field = isinstance(resolved.field_type, ObjectIdType)
    needs_oid_str = is_oid_field or resolved.is_any

    def coerce_one(v: Any) -> Any:
        if needs_oid_str and isinstance(v, ObjectId):
            return str(v)
        return v

    if op in ("$in", "$nin"):
        return [coerce_one(v) for v in operand]
    return coerce_one(operand)


def _validate_operand(
    op: str, operand: Any, field_name: str, resolved: ResolvedField
) -> None:
    """Check operand shape and field-type compatibility."""
    if op in ("$in", "$nin"):
        if not isinstance(operand, (list, tuple)):
            raise XLR8FilterError(
                f"{op} on {field_name!r} requires a list, got "
                f"{type(operand).__name__}: {operand!r}"
            )
        return
    if op in _COMPARISON_OPS:
        if resolved.is_any:
            raise XLR8FilterError(
                f"comparison operators on Types.Any() fields are not "
                f"supported (field {field_name!r}, op {op}). Use equality "
                "or $in/$nin."
            )
        if not resolved.is_orderable:
            raise XLR8FilterError(
                f"{op} not supported on field {field_name!r} of type "
                f"{type(resolved.field_type).__name__} — comparison "
                "operators require an orderable scalar type."
            )
        return
    # $eq / $ne: no extra shape check; any scalar operand is fine.
    return


def _collapse_and(nodes: List[Node]) -> Node:
    """Collapse a list of conjuncts into an AndNode, simplifying Tautologies
    and empty/singleton lists."""
    flat: List[Node] = []
    for n in nodes:
        if isinstance(n, Tautology):
            continue
        if isinstance(n, AndNode):
            flat.extend(n.children)
        else:
            flat.append(n)
    if not flat:
        return Tautology()
    if len(flat) == 1:
        return flat[0]
    return AndNode(tuple(flat))


# ---------------------------------------------------------------------------
# Typed / Any split
# ---------------------------------------------------------------------------


def _has_any_ref(node: Node) -> bool:
    if isinstance(node, Tautology):
        return False
    if isinstance(node, Leaf):
        return node.is_any
    if isinstance(node, AndNode):
        return any(_has_any_ref(c) for c in node.children)
    if isinstance(node, OrNode):
        return any(_has_any_ref(c) for c in node.children)
    if isinstance(node, NotNode):
        return _has_any_ref(node.inner)
    raise AssertionError(f"unknown node: {node!r}")


def _split_typed_any(node: Node) -> Tuple[Optional[Node], Optional[Node]]:
    """Split into (typed-pushdown, any-post-decode). Either may be None."""
    if isinstance(node, Tautology):
        return None, None
    if isinstance(node, Leaf):
        if node.is_any:
            return None, node
        return node, None
    if isinstance(node, AndNode):
        typed_parts: List[Node] = []
        any_parts: List[Node] = []
        for c in node.children:
            t, a = _split_typed_any(c)
            if t is not None:
                typed_parts.append(t)
            if a is not None:
                any_parts.append(a)
        typed_result = _collapse_and(typed_parts) if typed_parts else None
        any_result = _collapse_and(any_parts) if any_parts else None
        if isinstance(typed_result, Tautology):
            typed_result = None
        if isinstance(any_result, Tautology):
            any_result = None
        return typed_result, any_result
    if isinstance(node, (OrNode, NotNode)):
        if _has_any_ref(node):
            return None, node
        return node, None
    raise AssertionError(f"unknown node: {node!r}")


# ---------------------------------------------------------------------------
# Datetime literal handling — Parquet stores timestamps at a specific unit
# (ms / us / ns) and may be tz-naive (BSON default after Arrow round-trip) or
# tz-aware. Python ``datetime`` objects coming from the user's filter may not
# agree on either dimension. We normalize per-engine to avoid runtime type
# mismatches like ``timestamp[ms] vs timestamp[s, tz=UTC]``.
# ---------------------------------------------------------------------------


def _normalize_dt_to_arrow(dt: datetime, arrow_ts: pa.TimestampType) -> datetime:
    """Match tz-awareness of ``dt`` to an Arrow timestamp type. Unit is
    handled downstream by the engine (scalar construction / cast)."""
    target_has_tz = arrow_ts.tz is not None
    input_has_tz = dt.tzinfo is not None
    if target_has_tz and not input_has_tz:
        return dt.replace(tzinfo=timezone.utc)
    if not target_has_tz and input_has_tz:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _arrow_scalar(value: Any, arrow_ts: Optional[pa.DataType]) -> Any:
    """Promote a ``datetime`` to a typed PyArrow scalar that matches the
    Parquet column's unit and tz. Pass-through for non-datetime values."""
    if arrow_ts is None or not isinstance(value, datetime):
        return value
    if not pa.types.is_timestamp(arrow_ts):
        return value
    return pa.scalar(_normalize_dt_to_arrow(value, arrow_ts), type=arrow_ts)


def _polars_lit(value: Any, arrow_ts: Optional[pa.DataType]) -> Any:
    """Build a typed polars literal for a datetime value. Non-datetime
    values are returned unchanged so polars' own inference applies."""
    if arrow_ts is None or not isinstance(value, datetime):
        return value
    if not pa.types.is_timestamp(arrow_ts):
        return value
    normalized = _normalize_dt_to_arrow(value, arrow_ts)
    return pl.lit(normalized).cast(pl.Datetime(arrow_ts.unit, arrow_ts.tz))


def _pandas_value(value: Any, arrow_ts: Optional[pa.DataType]) -> Any:
    """Adjust tz-awareness of a datetime so a pandas ``Series`` comparison
    against a ``datetime64[unit]`` column succeeds."""
    if arrow_ts is None or not isinstance(value, datetime):
        return value
    if not pa.types.is_timestamp(arrow_ts):
        return value
    return _normalize_dt_to_arrow(value, arrow_ts)


# ---------------------------------------------------------------------------
# Translators — typed part
# ---------------------------------------------------------------------------


def _polars_expr(node: Node, ts_types: Dict[str, pa.DataType]) -> pl.Expr:
    if isinstance(node, Tautology):
        return pl.lit(True)
    if isinstance(node, Leaf):
        col = pl.col(node.field)
        arrow_ts = ts_types.get(node.field)
        op = node.op
        if op in ("$in", "$nin"):
            if arrow_ts is not None and pa.types.is_timestamp(arrow_ts):
                normalized = [
                    _normalize_dt_to_arrow(v, arrow_ts)
                    if isinstance(v, datetime)
                    else v
                    for v in node.value
                ]
                # ``.implode()`` treats the series as a single list-value so
                # ``is_in`` does set-membership (polars ≥ 1.20 deprecates the
                # ambiguous element-wise form when dtypes match exactly).
                typed_series = pl.Series(
                    values=normalized,
                    dtype=pl.Datetime(arrow_ts.unit, arrow_ts.tz),
                ).implode()
                return (
                    col.is_in(typed_series) if op == "$in" else ~col.is_in(typed_series)
                )
            return (
                col.is_in(list(node.value))
                if op == "$in"
                else ~col.is_in(list(node.value))
            )
        val = _polars_lit(node.value, arrow_ts)
        if op == "$eq":
            return col == val
        if op == "$ne":
            return col != val
        if op == "$gt":
            return col > val
        if op == "$gte":
            return col >= val
        if op == "$lt":
            return col < val
        if op == "$lte":
            return col <= val
        raise AssertionError(f"unknown op: {op}")
    if isinstance(node, AndNode):
        result = _polars_expr(node.children[0], ts_types)
        for c in node.children[1:]:
            result = result & _polars_expr(c, ts_types)
        return result
    if isinstance(node, OrNode):
        result = _polars_expr(node.children[0], ts_types)
        for c in node.children[1:]:
            result = result | _polars_expr(c, ts_types)
        return result
    if isinstance(node, NotNode):
        return ~_polars_expr(node.inner, ts_types)
    raise AssertionError(f"unknown node: {node!r}")


def _pyarrow_expr(node: Node, ts_types: Dict[str, pa.DataType]) -> pc.Expression:
    if isinstance(node, Tautology):
        return pc.scalar(True)
    if isinstance(node, Leaf):
        f = pc.field(node.field)
        arrow_ts = ts_types.get(node.field)
        op = node.op
        if op in ("$in", "$nin"):
            if arrow_ts is not None and pa.types.is_timestamp(arrow_ts):
                normalized = [
                    _normalize_dt_to_arrow(v, arrow_ts)
                    if isinstance(v, datetime)
                    else v
                    for v in node.value
                ]
                typed_array = pa.array(normalized, type=arrow_ts)
                return f.isin(typed_array) if op == "$in" else ~f.isin(typed_array)
            return (
                f.isin(list(node.value)) if op == "$in" else ~f.isin(list(node.value))
            )
        val = _arrow_scalar(node.value, arrow_ts)
        if op == "$eq":
            return f == val
        if op == "$ne":
            return f != val
        if op == "$gt":
            return f > val
        if op == "$gte":
            return f >= val
        if op == "$lt":
            return f < val
        if op == "$lte":
            return f <= val
        raise AssertionError(f"unknown op: {op}")
    if isinstance(node, AndNode):
        result = _pyarrow_expr(node.children[0], ts_types)
        for c in node.children[1:]:
            result = result & _pyarrow_expr(c, ts_types)
        return result
    if isinstance(node, OrNode):
        result = _pyarrow_expr(node.children[0], ts_types)
        for c in node.children[1:]:
            result = result | _pyarrow_expr(c, ts_types)
        return result
    if isinstance(node, NotNode):
        return ~_pyarrow_expr(node.inner, ts_types)
    raise AssertionError(f"unknown node: {node!r}")


def _duckdb_where(
    node: Node, params: List[Any], ts_types: Dict[str, pa.DataType]
) -> str:
    """Emit a DuckDB SQL fragment. Field values go through ``params``
    (appended) and are referenced as ``?`` placeholders."""
    if isinstance(node, Tautology):
        return "TRUE"
    if isinstance(node, Leaf):
        col = f'"{node.field}"'
        arrow_ts = ts_types.get(node.field)

        def _p(v: Any) -> Any:
            if arrow_ts is not None and isinstance(v, datetime):
                if pa.types.is_timestamp(arrow_ts):
                    return _normalize_dt_to_arrow(v, arrow_ts)
            return v

        op = node.op
        if op == "$eq":
            if node.value is None:
                return f"{col} IS NULL"
            params.append(_p(node.value))
            return f"({col} = ?)"
        if op == "$ne":
            if node.value is None:
                return f"{col} IS NOT NULL"
            params.append(_p(node.value))
            # Match MongoDB: $ne returns true for docs missing the field too.
            # We mirror that with IS DISTINCT FROM semantics:
            return f"({col} IS DISTINCT FROM ?)"
        if op == "$in":
            if not node.value:
                return "FALSE"
            placeholders = ",".join(["?"] * len(node.value))
            params.extend([_p(v) for v in node.value])
            return f"({col} IN ({placeholders}))"
        if op == "$nin":
            if not node.value:
                return "TRUE"
            placeholders = ",".join(["?"] * len(node.value))
            params.extend([_p(v) for v in node.value])
            return f"({col} NOT IN ({placeholders}) OR {col} IS NULL)"
        if op in _COMPARISON_OPS:
            sql_op = {"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}[op]
            params.append(_p(node.value))
            return f"({col} {sql_op} ?)"
        raise AssertionError(f"unknown op: {op}")
    if isinstance(node, AndNode):
        parts = [_duckdb_where(c, params, ts_types) for c in node.children]
        return "(" + " AND ".join(parts) + ")"
    if isinstance(node, OrNode):
        parts = [_duckdb_where(c, params, ts_types) for c in node.children]
        return "(" + " OR ".join(parts) + ")"
    if isinstance(node, NotNode):
        return "(NOT " + _duckdb_where(node.inner, params, ts_types) + ")"
    raise AssertionError(f"unknown node: {node!r}")


# ---------------------------------------------------------------------------
# Translators — any part (in-memory on decoded DataFrames)
# ---------------------------------------------------------------------------


def _apply_any_pandas(
    df: pd.DataFrame, node: Node, ts_types: Dict[str, pa.DataType]
) -> pd.DataFrame:
    mask = _pandas_mask(df, node, ts_types)
    if mask is None:
        return df
    return df.loc[mask].reset_index(drop=True)


def _pandas_mask(
    df: pd.DataFrame, node: Node, ts_types: Dict[str, pa.DataType]
) -> Optional[pd.Series]:
    """Build a boolean mask over df. None means "match everything"."""
    if isinstance(node, Tautology):
        return None
    if isinstance(node, Leaf):
        if node.field not in df.columns:
            raise XLR8FilterError(
                f"field {node.field!r} is not present in the cached data; "
                "either the cache was populated with a projection that "
                "excluded it, or the field never appears in the documents."
            )
        col = df[node.field]
        arrow_ts = ts_types.get(node.field)

        def _v(v: Any) -> Any:
            return _pandas_value(v, arrow_ts)

        op = node.op
        if op == "$eq":
            if node.value is None:
                return col.isna()
            return col == _v(node.value)
        if op == "$ne":
            if node.value is None:
                return col.notna()
            return (col != _v(node.value)) | col.isna()
        if op == "$in":
            return col.isin([_v(v) for v in node.value])
        if op == "$nin":
            return ~col.isin([_v(v) for v in node.value])
        if op == "$gt":
            return col > _v(node.value)
        if op == "$gte":
            return col >= _v(node.value)
        if op == "$lt":
            return col < _v(node.value)
        if op == "$lte":
            return col <= _v(node.value)
        raise AssertionError(f"unknown op: {op}")
    if isinstance(node, AndNode):
        masks = [_pandas_mask(df, c, ts_types) for c in node.children]
        combined: Optional[pd.Series] = None
        for m in masks:
            if m is None:
                continue
            combined = m if combined is None else combined & m
        return combined
    if isinstance(node, OrNode):
        masks = [_pandas_mask(df, c, ts_types) for c in node.children]
        combined = None
        for m in masks:
            if m is None:
                return None  # Tautology under Or — whole thing is TRUE
            combined = m if combined is None else combined | m
        return combined
    if isinstance(node, NotNode):
        inner = _pandas_mask(df, node.inner, ts_types)
        if inner is None:
            # NOT(TRUE) == FALSE for every row.
            return pd.Series([False] * len(df), index=df.index)
        return ~inner
    raise AssertionError(f"unknown node: {node!r}")


def _apply_any_polars(
    df: pl.DataFrame, node: Node, ts_types: Dict[str, pa.DataType]
) -> pl.DataFrame:
    if isinstance(node, Tautology):
        return df
    # Ensure referenced columns exist.
    for f in _leaf_fields(node):
        if f not in df.columns:
            raise XLR8FilterError(
                f"field {f!r} is not present in the cached data; "
                "either the cache was populated with a projection that "
                "excluded it, or the field never appears in the documents."
            )
    return df.filter(_polars_expr(node, ts_types))


def _leaf_fields(node: Node) -> Set[str]:
    if isinstance(node, Tautology):
        return set()
    if isinstance(node, Leaf):
        return {node.field}
    if isinstance(node, (AndNode, OrNode)):
        out: Set[str] = set()
        for c in node.children:
            out |= _leaf_fields(c)
        return out
    if isinstance(node, NotNode):
        return _leaf_fields(node.inner)
    raise AssertionError(f"unknown node: {node!r}")


# ---------------------------------------------------------------------------
# Compiled handle
# ---------------------------------------------------------------------------


@dataclass
class CompiledPostFilter:
    """Opaque object passed from the cursor to the reader.

    Exposes per-engine translators for the typed (pushable) subtree and
    in-memory evaluators for the Any() subtree. Either subtree may be None
    (meaning "no predicate for this path")."""

    typed_ast: Optional[Node]
    any_ast: Optional[Node]
    referenced_fields: Set[str] = dataclass_field(default_factory=set)
    # Populated by ``bind_parquet_schema``. Maps referenced fields whose
    # Parquet column is a timestamp type → that pa.TimestampType. Empty
    # when the filter has no datetime leaves or no schema was bound yet.
    _ts_types: Dict[str, pa.DataType] = dataclass_field(default_factory=dict)

    @property
    def has_typed(self) -> bool:
        return self.typed_ast is not None and not isinstance(self.typed_ast, Tautology)

    @property
    def has_any(self) -> bool:
        return self.any_ast is not None and not isinstance(self.any_ast, Tautology)

    def bind_parquet_schema(self, parquet_schema: pa.Schema) -> None:
        """Extract timestamp column types for referenced fields. Must be
        called by the reader before any ``to_*_expr`` / mask method when
        a datetime literal might appear in the filter — otherwise
        comparisons can fail on timestamp unit / tz mismatches between the
        Python ``datetime`` and the Parquet ``timestamp[ms]`` column."""
        types_map: Dict[str, pa.DataType] = {}
        for field in self.referenced_fields:
            idx = parquet_schema.get_field_index(field)
            if idx >= 0:
                ft = parquet_schema.field(idx).type
                if pa.types.is_timestamp(ft):
                    types_map[field] = ft
        self._ts_types = types_map

    def to_polars_expr(self) -> Optional[pl.Expr]:
        if not self.has_typed:
            return None
        return _polars_expr(self.typed_ast, self._ts_types)  # type: ignore[arg-type]

    def to_pyarrow_expr(self) -> Optional[pc.Expression]:
        if not self.has_typed:
            return None
        return _pyarrow_expr(self.typed_ast, self._ts_types)  # type: ignore[arg-type]

    def to_duckdb_where(self) -> Tuple[str, List[Any]]:
        if not self.has_typed:
            return "TRUE", []
        params: List[Any] = []
        sql = _duckdb_where(self.typed_ast, params, self._ts_types)  # type: ignore[arg-type]
        return sql, params

    def to_pandas_mask(self, df: pd.DataFrame) -> Optional[pd.Series]:
        """Build a pandas mask for the typed subtree. Used by reader paths
        (e.g. ``iter_batches``) that cannot push the predicate down."""
        if not self.has_typed:
            return None
        return _pandas_mask(df, self.typed_ast, self._ts_types)  # type: ignore[arg-type]

    def apply_any_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.has_any:
            return df
        return _apply_any_pandas(df, self.any_ast, self._ts_types)  # type: ignore[arg-type]

    def apply_any_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        if not self.has_any:
            return df
        return _apply_any_polars(df, self.any_ast, self._ts_types)  # type: ignore[arg-type]

    def apply_all_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply BOTH typed and Any() predicates in-memory on a pandas frame.

        Used by code paths that cannot push down (e.g. PyArrow's per-file
        ``iter_batches`` has no filter argument), where we need the full
        predicate applied on the materialized rows.
        """
        if self.has_typed:
            mask = _pandas_mask(df, self.typed_ast, self._ts_types)  # type: ignore[arg-type]
            if mask is not None:
                df = df.loc[mask].reset_index(drop=True)
        if self.has_any:
            df = _apply_any_pandas(df, self.any_ast, self._ts_types)  # type: ignore[arg-type]
        return df


def compile_post_filter(
    mql: Dict[str, Any],
    schema: Schema,
    *,
    time_field: str,
    strip_time_field: bool,
) -> CompiledPostFilter:
    """Parse + validate + split an MQL filter dict.

    Args:
        mql: The filter dict supplied by the caller.
        schema: The XLR8 Schema (used for field resolution).
        time_field: Name of the time field (usually ``schema.time_field``).
        strip_time_field: If True, clauses on ``time_field`` are stripped at
            parse time (caller passed start_date/end_date which override).

    Raises:
        XLR8FilterError: for any unsupported operator, unknown field,
            non-orderable comparison, or conflicting time-field clause.
    """
    if mql is None:
        return CompiledPostFilter(typed_ast=None, any_ast=None)
    if not isinstance(mql, dict):
        raise XLR8FilterError(
            f"filter must be a dict, got {type(mql).__name__}: {mql!r}"
        )
    if not mql:
        return CompiledPostFilter(typed_ast=None, any_ast=None)

    referenced: Set[str] = set()
    root = _parse_filter(
        mql,
        schema,
        time_field=time_field,
        strip_time_field=strip_time_field,
        _in_and_context=True,
        _referenced=referenced,
    )
    typed_ast, any_ast = _split_typed_any(root)
    return CompiledPostFilter(
        typed_ast=typed_ast, any_ast=any_ast, referenced_fields=referenced
    )


__all__ = [
    "XLR8FilterError",
    "CompiledPostFilter",
    "compile_post_filter",
]
