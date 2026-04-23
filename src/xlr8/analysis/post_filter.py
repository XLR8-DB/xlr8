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
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import pandas as pd
import polars as pl
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
# Translators — typed part
# ---------------------------------------------------------------------------


def _polars_expr(node: Node) -> pl.Expr:
    if isinstance(node, Tautology):
        return pl.lit(True)
    if isinstance(node, Leaf):
        col = pl.col(node.field)
        op = node.op
        if op == "$eq":
            return col == node.value
        if op == "$ne":
            return col != node.value
        if op == "$in":
            return col.is_in(list(node.value))
        if op == "$nin":
            return ~col.is_in(list(node.value))
        if op == "$gt":
            return col > node.value
        if op == "$gte":
            return col >= node.value
        if op == "$lt":
            return col < node.value
        if op == "$lte":
            return col <= node.value
        raise AssertionError(f"unknown op: {op}")
    if isinstance(node, AndNode):
        result = _polars_expr(node.children[0])
        for c in node.children[1:]:
            result = result & _polars_expr(c)
        return result
    if isinstance(node, OrNode):
        result = _polars_expr(node.children[0])
        for c in node.children[1:]:
            result = result | _polars_expr(c)
        return result
    if isinstance(node, NotNode):
        return ~_polars_expr(node.inner)
    raise AssertionError(f"unknown node: {node!r}")


def _pyarrow_expr(node: Node) -> pc.Expression:
    if isinstance(node, Tautology):
        return pc.scalar(True)
    if isinstance(node, Leaf):
        f = pc.field(node.field)
        op = node.op
        if op == "$eq":
            return f == node.value
        if op == "$ne":
            return f != node.value
        if op == "$in":
            return f.isin(list(node.value))
        if op == "$nin":
            return ~f.isin(list(node.value))
        if op == "$gt":
            return f > node.value
        if op == "$gte":
            return f >= node.value
        if op == "$lt":
            return f < node.value
        if op == "$lte":
            return f <= node.value
        raise AssertionError(f"unknown op: {op}")
    if isinstance(node, AndNode):
        result = _pyarrow_expr(node.children[0])
        for c in node.children[1:]:
            result = result & _pyarrow_expr(c)
        return result
    if isinstance(node, OrNode):
        result = _pyarrow_expr(node.children[0])
        for c in node.children[1:]:
            result = result | _pyarrow_expr(c)
        return result
    if isinstance(node, NotNode):
        return ~_pyarrow_expr(node.inner)
    raise AssertionError(f"unknown node: {node!r}")


def _duckdb_where(node: Node, params: List[Any]) -> str:
    """Emit a DuckDB SQL fragment. Field values go through ``params``
    (appended) and are referenced as ``?`` placeholders."""
    if isinstance(node, Tautology):
        return "TRUE"
    if isinstance(node, Leaf):
        col = f'"{node.field}"'
        op = node.op
        if op == "$eq":
            if node.value is None:
                return f"{col} IS NULL"
            params.append(node.value)
            return f"({col} = ?)"
        if op == "$ne":
            if node.value is None:
                return f"{col} IS NOT NULL"
            params.append(node.value)
            # Match MongoDB: $ne returns true for docs missing the field too.
            # We mirror that with IS DISTINCT FROM semantics:
            return f"({col} IS DISTINCT FROM ?)"
        if op == "$in":
            if not node.value:
                return "FALSE"
            placeholders = ",".join(["?"] * len(node.value))
            params.extend(list(node.value))
            return f"({col} IN ({placeholders}))"
        if op == "$nin":
            if not node.value:
                return "TRUE"
            placeholders = ",".join(["?"] * len(node.value))
            params.extend(list(node.value))
            return f"({col} NOT IN ({placeholders}) OR {col} IS NULL)"
        if op in _COMPARISON_OPS:
            sql_op = {"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}[op]
            params.append(node.value)
            return f"({col} {sql_op} ?)"
        raise AssertionError(f"unknown op: {op}")
    if isinstance(node, AndNode):
        parts = [_duckdb_where(c, params) for c in node.children]
        return "(" + " AND ".join(parts) + ")"
    if isinstance(node, OrNode):
        parts = [_duckdb_where(c, params) for c in node.children]
        return "(" + " OR ".join(parts) + ")"
    if isinstance(node, NotNode):
        return "(NOT " + _duckdb_where(node.inner, params) + ")"
    raise AssertionError(f"unknown node: {node!r}")


# ---------------------------------------------------------------------------
# Translators — any part (in-memory on decoded DataFrames)
# ---------------------------------------------------------------------------


def _apply_any_pandas(df: pd.DataFrame, node: Node) -> pd.DataFrame:
    mask = _pandas_mask(df, node)
    if mask is None:
        return df
    return df.loc[mask].reset_index(drop=True)


def _pandas_mask(df: pd.DataFrame, node: Node) -> Optional[pd.Series]:
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
        op = node.op
        if op == "$eq":
            if node.value is None:
                return col.isna()
            return col == node.value
        if op == "$ne":
            if node.value is None:
                return col.notna()
            return (col != node.value) | col.isna()
        if op == "$in":
            return col.isin(list(node.value))
        if op == "$nin":
            return ~col.isin(list(node.value))
        if op == "$gt":
            return col > node.value
        if op == "$gte":
            return col >= node.value
        if op == "$lt":
            return col < node.value
        if op == "$lte":
            return col <= node.value
        raise AssertionError(f"unknown op: {op}")
    if isinstance(node, AndNode):
        masks = [_pandas_mask(df, c) for c in node.children]
        combined: Optional[pd.Series] = None
        for m in masks:
            if m is None:
                continue
            combined = m if combined is None else combined & m
        return combined
    if isinstance(node, OrNode):
        masks = [_pandas_mask(df, c) for c in node.children]
        combined = None
        for m in masks:
            if m is None:
                return None  # Tautology under Or — whole thing is TRUE
            combined = m if combined is None else combined | m
        return combined
    if isinstance(node, NotNode):
        inner = _pandas_mask(df, node.inner)
        if inner is None:
            # NOT(TRUE) == FALSE for every row.
            return pd.Series([False] * len(df), index=df.index)
        return ~inner
    raise AssertionError(f"unknown node: {node!r}")


def _apply_any_polars(df: pl.DataFrame, node: Node) -> pl.DataFrame:
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
    return df.filter(_polars_expr(node))


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

    @property
    def has_typed(self) -> bool:
        return self.typed_ast is not None and not isinstance(self.typed_ast, Tautology)

    @property
    def has_any(self) -> bool:
        return self.any_ast is not None and not isinstance(self.any_ast, Tautology)

    def to_polars_expr(self) -> Optional[pl.Expr]:
        if not self.has_typed:
            return None
        return _polars_expr(self.typed_ast)  # type: ignore[arg-type]

    def to_pyarrow_expr(self) -> Optional[pc.Expression]:
        if not self.has_typed:
            return None
        return _pyarrow_expr(self.typed_ast)  # type: ignore[arg-type]

    def to_duckdb_where(self) -> Tuple[str, List[Any]]:
        if not self.has_typed:
            return "TRUE", []
        params: List[Any] = []
        sql = _duckdb_where(self.typed_ast, params)  # type: ignore[arg-type]
        return sql, params

    def apply_any_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.has_any:
            return df
        return _apply_any_pandas(df, self.any_ast)  # type: ignore[arg-type]

    def apply_any_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        if not self.has_any:
            return df
        return _apply_any_polars(df, self.any_ast)  # type: ignore[arg-type]

    def apply_all_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply BOTH typed and Any() predicates in-memory on a pandas frame.

        Used by code paths that cannot push down (e.g. PyArrow's per-file
        ``iter_batches`` has no filter argument), where we need the full
        predicate applied on the materialized rows.
        """
        if self.has_typed:
            mask = _pandas_mask(df, self.typed_ast)  # type: ignore[arg-type]
            if mask is not None:
                df = df.loc[mask].reset_index(drop=True)
        if self.has_any:
            df = _apply_any_pandas(df, self.any_ast)  # type: ignore[arg-type]
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
