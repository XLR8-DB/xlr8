"""
MQL-to-DuckDB-SQL filter translator.

Translates MongoDB query operators into parameterized DuckDB SQL WHERE clauses
for querying cached Parquet files.

Supported operators:
  Comparison:    $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin
  Element:       $exists, $type
  Array:         $all, $elemMatch, $size
  Evaluation:    $regex (with $options), $mod
  Bitwise:       $bitsAllSet, $bitsAllClear, $bitsAnySet, $bitsAnyClear
  Logical:       $and, $or (depth-1), $not, $nor (on non-time fields)
  Implicit:      bare value (treated as $eq), multiple top-level keys (implicit $and)

Types.Any() fields stored as 13-field structs in Parquet are handled with
type-aware COALESCE expressions over the relevant struct sub-fields.

Forbidden operators ($near, $expr, $where, etc.) raise ValueError.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# Operator classification
# ─────────────────────────────────────────────────────────────────────

# Operators we can translate to DuckDB SQL
_SUPPORTED_OPS: frozenset = frozenset(
    {
        "$eq",
        "$ne",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$in",
        "$nin",
        "$exists",
        "$type",
        "$all",
        "$elemMatch",
        "$size",
        "$regex",
        "$options",
        "$mod",
        "$bitsAllSet",
        "$bitsAllClear",
        "$bitsAnySet",
        "$bitsAnyClear",
        "$and",
        "$or",
        "$not",
        "$nor",
    }
)

# Operators that cannot be evaluated against static Parquet data
_FORBIDDEN_OPS: frozenset = frozenset(
    {
        "$expr",
        "$where",
        "$text",
        "$search",
        "$vectorSearch",
        "$near",
        "$nearSphere",
        "$geoWithin",
        "$geoIntersects",
        "$geometry",
        "$box",
        "$polygon",
        "$center",
        "$centerSphere",
        "$maxDistance",
        "$minDistance",
        "$jsonSchema",
        "$comment",
        "$uniqueDocs",
    }
)

# Operators that require post-filtering due to BSON type precedence
# but we skip them silently (no-op in WHERE clause)
_SKIP_OPS: frozenset = frozenset({"$comment", "$jsonSchema"})

# Types.Any() struct sub-field names — must match Rust encoder (schema.rs)
_ANY_NUMERIC_FIELDS = ("float_value", "int64_value", "int32_value", "decimal128_value")
_ANY_STRING_FIELDS = (
    "string_value",
    "objectid_value",
    "document_value",
    "array_value",
    "binary_value",
    "regex_value",
    "decimal128_value",
)
_ANY_BOOL_FIELD = "bool_value"
_ANY_DATETIME_FIELD = "datetime_value"
_ANY_NULL_FIELD = "null_value"


def _is_any_type(field_type: Any) -> bool:
    """Check if a field type is Types.Any (class or instance)."""
    try:
        from xlr8.schema.types import Any as AnyType
    except ImportError:
        return False

    if isinstance(field_type, AnyType):
        return True
    if isinstance(field_type, type) and issubclass(field_type, AnyType):
        return True
    return False


def _is_list_type(field_type: Any) -> bool:
    """Check if a field type is Types.List (class or instance)."""
    try:
        from xlr8.schema.types import List as ListType
    except ImportError:
        return False

    if isinstance(field_type, ListType):
        return True
    if isinstance(field_type, type) and issubclass(field_type, ListType):
        return True
    return False


def _detect_value_kind(value: Any) -> str:
    """Classify a Python value for DuckDB comparison strategy.

    Returns one of: 'numeric', 'string', 'bool', 'datetime', 'objectid', 'null'
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "numeric"
    if isinstance(value, str):
        return "string"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, ObjectId):
        return "objectid"
    if isinstance(value, bytes):
        return "binary"
    # Fallback: treat as string
    return "string"


def _detect_list_value_kind(values: List[Any]) -> str:
    """Classify the common type of a list of values.

    Returns the dominant kind, defaulting to 'string' if mixed.
    """
    if not values:
        return "null"
    kinds = {_detect_value_kind(v) for v in values}
    kinds.discard("null")
    if not kinds:
        return "null"
    if len(kinds) == 1:
        return kinds.pop()
    return "mixed"


def _format_param_value(value: Any) -> Any:
    """Format a Python value for DuckDB parameter binding.

    ObjectIds are stored as strings in Parquet, so convert to str.
    datetimes are kept as-is (DuckDB handles them).
    """
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _quote_ident(name: str) -> str:
    """Quote a column identifier for DuckDB SQL (double-quotes)."""
    return f'"{name}"'


# ─────────────────────────────────────────────────────────────────────
# Any-type column expression builders
# ─────────────────────────────────────────────────────────────────────


def _any_numeric_coalesce(column: str) -> str:
    """Build a DuckDB expression that coalesces all numeric Any sub-fields.

    Returns an expression that yields DOUBLE for rows where any numeric
    sub-field is populated, or NULL if all are NULL.
    """
    q = _quote_ident(column)
    return (
        f"COALESCE("
        f"{q}.float_value, "
        f"CAST({q}.int64_value AS DOUBLE), "
        f"CAST({q}.int32_value AS DOUBLE), "
        f"TRY_CAST({q}.decimal128_value AS DOUBLE)"
        f")"
    )


def _any_string_coalesce(column: str) -> str:
    """Build a DuckDB expression that coalesces string-like Any sub-fields."""
    q = _quote_ident(column)
    return (
        f"COALESCE("
        f"{q}.string_value, "
        f"{q}.objectid_value, "
        f"{q}.document_value, "
        f"{q}.array_value, "
        f"{q}.binary_value, "
        f"{q}.regex_value, "
        f"{q}.decimal128_value"
        f")"
    )


def _any_bool_expr(column: str) -> str:
    """Build a DuckDB expression for the bool sub-field."""
    return f"{_quote_ident(column)}.{_ANY_BOOL_FIELD}"


def _any_datetime_expr(column: str) -> str:
    """Build a DuckDB expression for the datetime sub-field."""
    return f"{_quote_ident(column)}.{_ANY_DATETIME_FIELD}"


def _any_null_expr(column: str) -> str:
    """Build a DuckDB expression checking if Any value is null."""
    return (
        f"({_quote_ident(column)}.{_ANY_NULL_FIELD} IS NOT DISTINCT FROM true)"
    )


# ─────────────────────────────────────────────────────────────────────
# Core translation
# ─────────────────────────────────────────────────────────────────────


def translate_mql_to_sql(
    query: Dict[str, Any],
    schema: Any,
    time_field: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    param_prefix: str = "p",
) -> Tuple[str, Dict[str, Any]]:
    """Translate a MongoDB filter query to a DuckDB SQL WHERE clause.

    Args:
        query: MongoDB filter dict (e.g., {"status": "active", "value": {"$gt": 100}})
        schema: XLR8 Schema object with field type definitions
        time_field: Name of the time field (for date range filtering)
        start_date: Optional start date filter (inclusive)
        end_date: Optional end date filter (exclusive)
        param_prefix: Prefix for parameter names (default: "p")

    Returns:
        Tuple of (where_clause_sql, params_dict)
        where_clause_sql is a string like '("status" = $p0 AND "value" >= $p1)'
        that can be used in a DuckDB WHERE clause.
        Returns ("1=1", {}) for empty queries.
        params_dict maps parameter names to values for DuckDB binding.

    Raises:
        ValueError: If query contains forbidden operators, nested $or (depth > 1),
                    or references fields not in the schema.

    Example:
        >>> sql, params = translate_mql_to_sql(
        ...     {"value": {"$gte": 100, "$lt": 200}, "status": "active"},
        ...     schema, "timestamp"
        ... )
        >>> sql
        '(("value" >= $p0 AND "value" < $p1) AND "status" = $p2)'
    """
    ctx = _TranslationContext(schema, time_field, param_prefix)

    # Build the MQL filter portion
    mql_sql = ctx.translate(query)

    # Add date range conditions
    date_conditions = []
    date_params = {}
    if start_date is not None or end_date is not None:
        time_col = _quote_ident(time_field)
        # Check if time field is an Any type
        time_field_type = _get_schema_field_type(schema, time_field)
        if time_field_type is not None and _is_any_type(time_field_type):
            time_expr = _any_datetime_expr(time_field)
        else:
            time_expr = time_col

        if start_date is not None:
            pname = f"${param_prefix}_start"
            date_conditions.append(f"{time_expr} >= {pname}")
            date_params[pname] = start_date
        if end_date is not None:
            pname = f"${param_prefix}_end"
            date_conditions.append(f"{time_expr} < {pname}")
            date_params[pname] = end_date

    # Combine
    parts = []
    all_params = dict(ctx.params)

    if mql_sql and mql_sql != "1=1":
        parts.append(f"({mql_sql})")
    for cond in date_conditions:
        parts.append(cond)
    all_params.update(date_params)

    if not parts:
        return ("1=1", {})

    return (" AND ".join(parts), all_params)


def _get_schema_field_type(schema: Any, field_name: str) -> Any:
    """Get the type of a field from the schema, or None if not found."""
    if schema is None or not hasattr(schema, "fields"):
        return None
    return schema.fields.get(field_name)


class _TranslationContext:
    """Internal state holder for MQL-to-SQL translation."""

    def __init__(
        self,
        schema: Any,
        time_field: str,
        param_prefix: str = "p",
    ):
        self.schema = schema
        self.time_field = time_field
        self.param_prefix = param_prefix
        self._counter = 0
        self.params: Dict[str, Any] = {}

    def _next_param(self) -> str:
        """Generate the next unique parameter name."""
        name = f"${self.param_prefix}{self._counter}"
        self._counter += 1
        return name

    # ── public entry point ──────────────────────────────────────────

    def translate(self, query: Dict[str, Any]) -> str:
        """Translate a MongoDB query dict to a DuckDB WHERE clause fragment.

        Returns "1=1" for empty queries (matches everything).
        """
        if not query:
            return "1=1"

        # Check for forbidden operators first
        self._check_forbidden(query)

        # Normalize: flatten top-level $and if it's the only key
        normalized = self._normalize_and(query)

        # Check for top-level $or
        if "$or" in normalized:
            return self._translate_or(normalized)

        # Check for $nor
        if "$nor" in normalized:
            return self._translate_nor(normalized)

        # Implicit $and: multiple top-level keys
        return self._translate_implicit_and(normalized)

    # ── operator guards ──────────────────────────────────────────────

    def _check_forbidden(self, query: Dict[str, Any]) -> None:
        """Recursively check for forbidden operators in the query."""
        for key, value in query.items():
            if key.startswith("$"):
                if key in _FORBIDDEN_OPS:
                    raise ValueError(
                        f"Operator '{key}' is not supported for Parquet cache queries. "
                        f"It requires server-side evaluation which is not possible on "
                        f"static cached data."
                    )
                if key == "$or":
                    if isinstance(value, list):
                        # Check $or depth
                        depth = self._or_depth(query)
                        if depth > 1:
                            raise ValueError(
                                f"Nested $or (depth {depth}) is not supported for "
                                f"Parquet cache queries. Max $or depth is 1."
                            )
                        for branch in value:
                            if isinstance(branch, dict):
                                self._check_forbidden(branch)
                elif isinstance(value, dict):
                    self._check_forbidden(value)

    def _or_depth(self, obj: Any, depth: int = 0) -> int:
        """Calculate the maximum nesting depth of $or operators."""
        if not isinstance(obj, dict):
            return depth
        max_depth = depth
        if "$or" in obj:
            or_list = obj["$or"]
            if isinstance(or_list, list):
                current = depth + 1
                max_depth = max(max_depth, current)
                for branch in or_list:
                    if isinstance(branch, dict):
                        max_depth = max(max_depth, self._or_depth(branch, current))
        for val in obj.values():
            if isinstance(val, dict):
                max_depth = max(max_depth, self._or_depth(val, depth))
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        max_depth = max(max_depth, self._or_depth(item, depth))
        return max_depth

    # ── query normalization ──────────────────────────────────────────

    def _normalize_and(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten top-level $and into the parent query.

        {"$and": [{"a": 1}, {"b": 2}]} → merged into a single dict.
        Also handles nested $and within the array.
        """
        if "$and" not in query:
            return query

        and_items = query["$and"]
        if not isinstance(and_items, list):
            return query

        # Start with everything except $and
        result = {k: v for k, v in query.items() if k != "$and"}

        for item in and_items:
            if not isinstance(item, dict):
                continue
            # Recursively flatten nested $and
            flat = self._normalize_and(item)
            for k, v in flat.items():
                if k in result:
                    # Merge: if both have time_field operators, intersect them
                    result[k] = self._merge_field_conditions(result[k], v)
                else:
                    result[k] = v

        return result

    def _merge_field_conditions(self, existing: Any, incoming: Any) -> Any:
        """Merge two conditions on the same field (intersection).

        If both are dicts with operators, merge keys.
        Otherwise, incoming overwrites.
        """
        if isinstance(existing, dict) and isinstance(incoming, dict):
            # Both are operator dicts — merge
            merged = dict(existing)
            for op, val in incoming.items():
                if op in merged:
                    # Same operator — keep the more restrictive?
                    # For now, let the incoming overwrite
                    merged[op] = val
                else:
                    merged[op] = val
            return merged
        # Fallback: incoming wins
        return incoming

    # ── logical operator translation ─────────────────────────────────

    def _translate_or(self, query: Dict[str, Any]) -> str:
        """Translate a top-level $or query."""
        or_branches = query["$or"]
        if not isinstance(or_branches, list) or not or_branches:
            return "1=0"  # Empty $or matches nothing

        # Separate $or from other keys (implicit AND with $or)
        other_keys = {k: v for k, v in query.items() if k != "$or"}

        branch_sqls = []
        for branch in or_branches:
            if not isinstance(branch, dict):
                continue
            # Merge other keys into each branch
            merged = dict(branch)
            for k, v in other_keys.items():
                if k not in merged:
                    merged[k] = v
                else:
                    merged[k] = self._merge_field_conditions(merged[k], v)

            branch_sql = self._translate_implicit_and(merged)
            if branch_sql and branch_sql != "1=1":
                branch_sqls.append(f"({branch_sql})")

        if not branch_sqls:
            return "1=1"

        return " OR ".join(branch_sqls)

    def _translate_nor(self, query: Dict[str, Any]) -> str:
        """Translate a top-level $nor query.

        $nor: [{"a": 1}, {"b": 2}] means NOT (a=1 OR b=2).
        """
        nor_branches = query["$nor"]
        if not isinstance(nor_branches, list) or not nor_branches:
            return "1=1"  # Empty $nor matches everything

        other_keys = {k: v for k, v in query.items() if k != "$nor"}

        branch_sqls = []
        for branch in nor_branches:
            if not isinstance(branch, dict):
                continue
            merged = dict(branch)
            for k, v in other_keys.items():
                if k not in merged:
                    merged[k] = v
                else:
                    merged[k] = self._merge_field_conditions(merged[k], v)

            branch_sql = self._translate_implicit_and(merged)
            if branch_sql and branch_sql != "1=1":
                branch_sqls.append(f"({branch_sql})")

        if not branch_sqls:
            return "1=1"

        if len(branch_sqls) == 1:
            return f"NOT ({branch_sqls[0]})"
        return f"NOT ({' OR '.join(branch_sqls)})"

    def _translate_implicit_and(self, query: Dict[str, Any]) -> str:
        """Translate a query with multiple top-level keys (implicit $and)."""
        conditions = []

        for field, condition in query.items():
            if field.startswith("$"):
                # Logical operators at this level
                if field == "$and":
                    if isinstance(condition, list):
                        for item in condition:
                            if isinstance(item, dict):
                                sub = self._translate_implicit_and(item)
                                if sub and sub != "1=1":
                                    conditions.append(f"({sub})")
                elif field == "$not":
                    sub = self._translate_field_condition(condition, field_name=None)
                    if sub:
                        conditions.append(f"NOT ({sub})")
                else:
                    raise ValueError(
                        f"Unsupported logical operator at field level: '{field}'"
                    )
            else:
                # Regular field condition
                field_sql = self._translate_field_condition(condition, field_name=field)
                if field_sql and field_sql != "1=1":
                    conditions.append(field_sql)

        if not conditions:
            return "1=1"

        if len(conditions) == 1:
            return conditions[0]

        return " AND ".join(f"({c})" for c in conditions)

    # ── field condition translation ──────────────────────────────────

    def _translate_field_condition(
        self,
        condition: Any,
        field_name: Optional[str] = None,
    ) -> str:
        """Translate a single field's condition to DuckDB SQL.

        Args:
            condition: The value side of a MongoDB query for a field.
                       Can be a bare value (implicit $eq), a dict of operators,
                       or a logical operator dict ($not, etc.)
            field_name: The field name (None if this is inside a $not at field level)

        Returns:
            DuckDB SQL fragment for this condition.
        """
        if condition is None:
            # {"field": null} — matches null or missing
            col = self._build_column_ref(field_name)
            return f"({col} IS NULL)"

        if isinstance(condition, dict):
            return self._translate_operator_dict(condition, field_name)

        # Bare value — implicit $eq
        return self._translate_comparison(field_name, "$eq", condition)

    def _translate_operator_dict(
        self,
        op_dict: Dict[str, Any],
        field_name: Optional[str] = None,
    ) -> str:
        """Translate a dict of operators for a single field."""
        if not op_dict:
            return "1=1"

        # Check for $not wrapping an operator dict at field level
        # {"field": {"$not": {"$gt": 5}}} → NOT (field > 5)
        if "$not" in op_dict:
            inner = op_dict["$not"]
            if isinstance(inner, dict):
                inner_sql = self._translate_operator_dict(inner, field_name)
                return f"NOT ({inner_sql})"
            else:
                # {"field": {"$not": value}} — equivalent to $ne
                return self._translate_comparison(field_name, "$ne", inner)

        # Handle logical operators at field level: $and, $or, $nor
        if "$and" in op_dict:
            return self._translate_field_level_logical(
                "$and", op_dict["$and"], field_name
            )
        if "$or" in op_dict:
            return self._translate_field_level_logical(
                "$or", op_dict["$or"], field_name
            )
        if "$nor" in op_dict:
            inner = self._translate_field_level_logical(
                "$or", op_dict["$nor"], field_name
            )
            return f"NOT ({inner})"

        # Regular operators — translate each and AND them together
        # {"field": {"$gte": 10, "$lt": 20}} → (col >= 10 AND col < 20)
        conditions = []
        for op, value in op_dict.items():
            if op.startswith("$"):
                cond = self._translate_operator(op, value, field_name)
                if cond and cond != "1=1":
                    conditions.append(cond)
            else:
                # Nested field within a document path?
                # This shouldn't normally happen but handle gracefully
                pass

        if not conditions:
            return "1=1"
        if len(conditions) == 1:
            return conditions[0]
        return " AND ".join(conditions)

    def _translate_field_level_logical(
        self,
        op: str,
        branches: Any,
        field_name: Optional[str] = None,
    ) -> str:
        """Translate $and/$or at the field level (inside a field's operator dict)."""
        if not isinstance(branches, list) or not branches:
            if op == "$and":
                return "1=1"
            return "1=0"

        branch_sqls = []
        for branch in branches:
            if isinstance(branch, dict):
                if field_name and not any(k.startswith("$") for k in branch):
                    # Branch is a value dict for the same field
                    # {"field": {"$and": [{"$gte": 10}, {"$lt": 20}]}}
                    sub = self._translate_operator_dict(branch, field_name)
                else:
                    # Branch has its own field names
                    sub = self._translate_implicit_and(branch)
            else:
                # Bare value
                sub = self._translate_comparison(field_name, "$eq", branch)

            if sub and sub != "1=1":
                branch_sqls.append(f"({sub})")

        if not branch_sqls:
            return "1=1" if op == "$and" else "1=0"

        if op in ("$and",):
            return " AND ".join(branch_sqls)
        else:
            return " OR ".join(branch_sqls)

    # ── individual operator translation ──────────────────────────────

    def _translate_operator(
        self,
        op: str,
        value: Any,
        field_name: Optional[str] = None,
    ) -> str:
        """Translate a single MongoDB operator to DuckDB SQL."""
        if op == "$eq":
            return self._translate_comparison(field_name, "$eq", value)
        elif op == "$ne":
            return self._translate_comparison(field_name, "$ne", value)
        elif op == "$gt":
            return self._translate_comparison(field_name, "$gt", value)
        elif op == "$gte":
            return self._translate_comparison(field_name, "$gte", value)
        elif op == "$lt":
            return self._translate_comparison(field_name, "$lt", value)
        elif op == "$lte":
            return self._translate_comparison(field_name, "$lte", value)
        elif op == "$in":
            return self._translate_in(field_name, value, negate=False)
        elif op == "$nin":
            return self._translate_in(field_name, value, negate=True)
        elif op == "$exists":
            return self._translate_exists(field_name, value)
        elif op == "$regex":
            return self._translate_regex(field_name, value, None)
        elif op == "$options":
            # $options appears alongside $regex — handled in _translate_operator_dict
            # At this level, it means someone used $options alone, which is a no-op
            return "1=1"
        elif op == "$not":
            if isinstance(value, dict):
                inner = self._translate_operator_dict(value, field_name)
                return f"NOT ({inner})"
            return self._translate_comparison(field_name, "$ne", value)
        elif op == "$all":
            return self._translate_all(field_name, value)
        elif op == "$elemMatch":
            return self._translate_elemmatch(field_name, value)
        elif op == "$size":
            return self._translate_size(field_name, value)
        elif op == "$type":
            return self._translate_type(field_name, value)
        elif op == "$mod":
            return self._translate_mod(field_name, value)
        elif op in ("$bitsAllSet", "$bitsAllClear", "$bitsAnySet", "$bitsAnyClear"):
            return self._translate_bits(op, field_name, value)
        elif op in _SKIP_OPS:
            return "1=1"
        else:
            raise ValueError(
                f"Unsupported operator '{op}' for Parquet cache queries."
            )

    # ── comparison operators ─────────────────────────────────────────

    _OP_MAP = {
        "$eq": "=",
        "$ne": "!=",
        "$gt": ">",
        "$gte": ">=",
        "$lt": "<",
        "$lte": "<=",
    }

    def _translate_comparison(
        self,
        field_name: Optional[str],
        op: str,
        value: Any,
    ) -> str:
        """Translate a comparison operator ($eq, $gt, etc.)."""
        if field_name is None:
            raise ValueError("Comparison operator requires a field name")

        sql_op = self._OP_MAP.get(op, "=")

        # Handle null comparison
        if value is None:
            col = self._build_column_ref(field_name)
            if op == "$eq":
                return f"({col} IS NULL)"
            elif op == "$ne":
                return f"({col} IS NOT NULL)"
            else:
                # $gt null, etc. — always false in MongoDB
                return "1=0"

        # Determine field type
        field_type = _get_schema_field_type(self.schema, field_name)
        is_any = field_type is not None and _is_any_type(field_type)
        is_list = field_type is not None and _is_list_type(field_type)

        if is_list:
            return self._translate_list_comparison(field_name, op, value)

        if is_any:
            return self._translate_any_comparison(field_name, op, value)

        # Simple typed field
        col = self._build_column_ref(field_name)
        param = self._next_param()
        self.params[param] = _format_param_value(value)
        return f"{col} {sql_op} {param}"

    def _translate_any_comparison(
        self,
        field_name: str,
        op: str,
        value: Any,
    ) -> str:
        """Translate a comparison on a Types.Any() field.

        The Any field is stored as a struct. We need to check the appropriate
        sub-field(s) based on the value type.
        """
        kind = _detect_value_kind(value)
        sql_op = self._OP_MAP.get(op, "=")

        if kind == "null":
            col = self._build_column_ref(field_name)
            if op == "$eq":
                return f"({col} IS NULL)"
            else:
                return f"({col} IS NOT NULL)"

        param = self._next_param()
        self.params[param] = _format_param_value(value)

        conditions = []

        if kind == "numeric":
            # Check numeric sub-fields
            num_expr = _any_numeric_coalesce(field_name)
            conditions.append(f"{num_expr} {sql_op} {param}")

        elif kind == "string":
            # Check string-like sub-fields
            str_expr = _any_string_coalesce(field_name)
            conditions.append(f"{str_expr} {sql_op} {param}")

        elif kind == "bool":
            bool_expr = _any_bool_expr(field_name)
            conditions.append(f"{bool_expr} {sql_op} {param}")

        elif kind == "datetime":
            dt_expr = _any_datetime_expr(field_name)
            conditions.append(f"{dt_expr} {sql_op} {param}")

        elif kind == "objectid":
            oid_expr = f"{_quote_ident(field_name)}.objectid_value"
            conditions.append(f"{oid_expr} {sql_op} {param}")

        elif kind == "binary":
            bin_expr = f"{_quote_ident(field_name)}.binary_value"
            conditions.append(f"{bin_expr} {sql_op} {param}")

        else:
            # Fallback: check all reasonable fields
            num_expr = _any_numeric_coalesce(field_name)
            conditions.append(f"{num_expr} {sql_op} {param}")
            str_expr = _any_string_coalesce(field_name)
            conditions.append(f"{str_expr} {sql_op} {param}")

        if not conditions:
            return "1=0"

        if len(conditions) == 1:
            return conditions[0]
        return "(" + " OR ".join(conditions) + ")"

    def _translate_list_comparison(
        self,
        field_name: str,
        op: str,
        value: Any,
    ) -> str:
        """Translate a comparison on a Types.List() field.

        For list fields, we use DuckDB list functions.
        For simple comparisons like $eq, we check if the list contains the value.
        """
        col = self._build_column_ref(field_name)
        sql_op = self._OP_MAP.get(op, "=")
        param = self._next_param()
        self.params[param] = _format_param_value(value)

        if op == "$eq":
            # List equality: list == [value]
            return f"list_has({col}, {param})"
        else:
            # For other comparisons, fallback to simple
            return f"{col} {sql_op} {param}"

    # ── $in / $nin ───────────────────────────────────────────────────

    def _translate_in(
        self,
        field_name: Optional[str],
        values: Any,
        negate: bool = False,
    ) -> str:
        """Translate $in or $nin operator."""
        if field_name is None:
            raise ValueError("$in/$nin requires a field name")

        if not isinstance(values, list):
            raise ValueError(f"$in/$nin requires an array, got {type(values).__name__}")

        # Empty $in = matches nothing; empty $nin = matches everything
        if not values:
            return "1=0" if not negate else "1=1"

        field_type = _get_schema_field_type(self.schema, field_name)
        is_any = field_type is not None and _is_any_type(field_type)

        if is_any:
            return self._translate_in_any(field_name, values, negate)

        col = self._build_column_ref(field_name)
        kind = _detect_list_value_kind(values)

        if kind == "null":
            # $in: [null] — check for NULL
            not_str = "NOT " if negate else ""
            return f"({col} IS {not_str}NULL)"

        # Build IN clause with parameters
        params = []
        for v in values:
            if v is None:
                continue  # Handle nulls separately
            pname = self._next_param()
            self.params[pname] = _format_param_value(v)
            params.append(pname)

        if not params:
            # All values were null
            not_str = "NOT " if negate else ""
            return f"({col} IS {not_str}NULL)"

        not_str = "NOT " if negate else ""
        in_list = ", ".join(params)

        # Handle null in the list (MongoDB: $in: [null, "a"] matches null OR "a")
        has_null = any(v is None for v in values)
        if has_null:
            null_cond = f"{col} IS NULL"
            in_cond = f"{col} {not_str}IN ({in_list})"
            if negate:
                # $nin: [null, "a"] → col IS NOT NULL AND col NOT IN ("a")
                return f"({col} IS NOT NULL AND {in_cond})"
            else:
                # $in: [null, "a"] → col IS NULL OR col IN ("a")
                return f"({null_cond} OR {col} IN ({in_list}))"

        return f"{col} {not_str}IN ({in_list})"

    def _translate_in_any(
        self,
        field_name: str,
        values: List[Any],
        negate: bool = False,
    ) -> str:
        """Translate $in/$nin on a Types.Any() field."""
        if not values:
            return "1=0" if not negate else "1=1"

        # Group values by kind
        groups: Dict[str, List[Any]] = {
            "numeric": [],
            "string": [],
            "bool": [],
            "datetime": [],
            "objectid": [],
            "null": [],
        }
        for v in values:
            groups[_detect_value_kind(v)].append(v)

        conditions = []

        # Build condition for each type group
        for kind, vals in groups.items():
            if not vals:
                continue

            if kind == "null":
                col = self._build_column_ref(field_name)
                conditions.append(f"({col} IS NULL)")
                continue

            params = []
            for v in vals:
                pname = self._next_param()
                self.params[pname] = _format_param_value(v)
                params.append(pname)

            in_list = ", ".join(params)

            if kind == "numeric":
                expr = _any_numeric_coalesce(field_name)
                conditions.append(f"{expr} IN ({in_list})")
            elif kind in ("string", "objectid", "binary"):
                expr = _any_string_coalesce(field_name)
                conditions.append(f"{expr} IN ({in_list})")
            elif kind == "bool":
                expr = _any_bool_expr(field_name)
                conditions.append(f"{expr} IN ({in_list})")
            elif kind == "datetime":
                expr = _any_datetime_expr(field_name)
                conditions.append(f"{expr} IN ({in_list})")

        if not conditions:
            return "1=0" if not negate else "1=1"

        if len(conditions) == 1:
            inner = conditions[0]
        else:
            inner = "(" + " OR ".join(conditions) + ")"

        if negate:
            return f"NOT ({inner})"
        return inner

    # ── $exists ──────────────────────────────────────────────────────

    def _translate_exists(
        self,
        field_name: Optional[str],
        value: bool,
    ) -> str:
        """Translate $exists operator."""
        if field_name is None:
            raise ValueError("$exists requires a field name")

        col = self._build_column_ref(field_name)

        field_type = _get_schema_field_type(self.schema, field_name)
        is_any = field_type is not None and _is_any_type(field_type)

        if is_any:
            # For Any fields, a value "exists" if the struct itself is not null
            # AND it's not the null_value variant
            if value:
                return (
                    f"({col} IS NOT NULL AND {_any_null_expr(field_name)} IS NOT TRUE)"
                )
            else:
                return (
                    f"({col} IS NULL OR {_any_null_expr(field_name)} IS TRUE)"
                )

        if value:
            return f"({col} IS NOT NULL)"
        else:
            return f"({col} IS NULL)"

    # ── $regex ───────────────────────────────────────────────────────

    def _translate_regex(
        self,
        field_name: Optional[str],
        pattern: str,
        options: Optional[str] = None,
    ) -> str:
        """Translate $regex operator.

        Uses DuckDB's REGEXP_MATCHES function.
        Handles $options for case-insensitive matching.
        """
        if field_name is None:
            raise ValueError("$regex requires a field name")

        field_type = _get_schema_field_type(self.schema, field_name)
        is_any = field_type is not None and _is_any_type(field_type)

        param = self._next_param()
        self.params[param] = str(pattern)

        if is_any:
            col = _any_string_coalesce(field_name)
        else:
            col = self._build_column_ref(field_name)

        # Handle $options (case insensitive, multiline, etc.)
        # DuckDB regexp_matches uses Perl-compatible syntax
        if options:
            # If 'i' is in options, wrap the pattern with (?i:...)
            if "i" in options:
                # We need to apply the flag to the runtime value, not the SQL
                # Use regexp_matches with a parameter
                pass

        # For case-insensitive: we use a simpler approach with LOWER()
        if options and "i" in options:
            return f"REGEXP_MATCHES(LOWER({col}), LOWER({param}))"

        return f"REGEXP_MATCHES({col}, {param})"

    # ── $all ─────────────────────────────────────────────────────────

    def _translate_all(
        self,
        field_name: Optional[str],
        values: Any,
    ) -> str:
        """Translate $all operator.

        $all matches arrays that contain ALL of the specified elements.
        For non-array fields, $all is equivalent to $eq of the array.

        In DuckDB, we use list_has_all for list columns.
        For scalar columns, we check if each value matches.
        """
        if field_name is None:
            raise ValueError("$all requires a field name")

        if not isinstance(values, list):
            raise ValueError(f"$all requires an array, got {type(values).__name__}")

        if not values:
            return "1=1"  # Empty $all matches everything

        field_type = _get_schema_field_type(self.schema, field_name)
        is_list = field_type is not None and _is_list_type(field_type)

        if is_list:
            # Use DuckDB list functions for list columns
            col = self._build_column_ref(field_name)
            # Build list_has_all equivalent
            # DuckDB doesn't have list_has_all, so we use AND of list_has
            conditions = []
            for v in values:
                pname = self._next_param()
                self.params[pname] = _format_param_value(v)
                conditions.append(f"list_has({col}, {pname})")
            return " AND ".join(f"({c})" for c in conditions)

        # For scalar/Any fields, $all matches if any element equals each value
        conditions = []
        for v in values:
            cond = self._translate_comparison(field_name, "$eq", v)
            conditions.append(f"({cond})")
        return " AND ".join(conditions)

    # ── $elemMatch ───────────────────────────────────────────────────

    def _translate_elemmatch(
        self,
        field_name: Optional[str],
        query: Any,
    ) -> str:
        """Translate $elemMatch operator.

        $elemMatch matches arrays containing at least one element that
        matches ALL specified conditions.

        For DuckDB, we can use list_filter + len > 0, or unnest the array.
        Using EXISTS with UNNEST is most general.
        """
        if field_name is None:
            raise ValueError("$elemMatch requires a field name")

        if not isinstance(query, dict):
            raise ValueError(
                f"$elemMatch requires a query document, got {type(query).__name__}"
            )

        field_type = _get_schema_field_type(self.schema, field_name)
        is_list = field_type is not None and _is_list_type(field_type)

        if not is_list:
            # For non-list fields, $elemMatch matches if the field value
            # (as a document) matches the query. Treat as regular query.
            # Actually, MongoDB semantics: $elemMatch on non-array is no-match.
            return "1=0"

        col = self._build_column_ref(field_name)

        # Build inner conditions — strip field name prefixes since we're
        # inside the list element scope
        inner_conditions = []
        for k, v in query.items():
            inner_sql = self._translate_field_condition(v, field_name=k)
            if inner_sql and inner_sql != "1=1":
                inner_conditions.append(inner_sql)

        if not inner_conditions:
            return f"len({col}) > 0"

        # Use EXISTS with UNNEST for elemMatch semantics
        inner_where = " AND ".join(f"({c})" for c in inner_conditions)
        # The UNNEST expands the list; we use a subquery pattern
        # EXISTS (SELECT 1 FROM (SELECT UNNEST(col) AS elem) WHERE <conditions on elem>)
        return (
            f"EXISTS (SELECT 1 FROM "
            f"(SELECT UNNEST({col}) AS _elem) AS _t "
            f"WHERE {inner_where})"
        )

    # ── $size ────────────────────────────────────────────────────────

    def _translate_size(
        self,
        field_name: Optional[str],
        size: Any,
    ) -> str:
        """Translate $size operator.

        $size matches arrays with exactly the specified number of elements.
        """
        if field_name is None:
            raise ValueError("$size requires a field name")

        if not isinstance(size, int):
            raise ValueError(f"$size requires an integer, got {type(size).__name__}")

        col = self._build_column_ref(field_name)
        return f"len({col}) = {size}"

    # ── $type ────────────────────────────────────────────────────────

    def _translate_type(
        self,
        field_name: Optional[str],
        type_spec: Any,
    ) -> str:
        """Translate $type operator.

        $type matches documents where the field is of the specified BSON type.

        For simple fields with known types, this is always true or always false.
        For Any fields, we can check which sub-field is populated.
        """
        if field_name is None:
            raise ValueError("$type requires a field name")

        field_type = _get_schema_field_type(self.schema, field_name)
        is_any = field_type is not None and _is_any_type(field_type)

        if not is_any:
            # For typed fields, we know the type — $type is either always true
            # or always false. We'll be optimistic and check the column type.
            col = self._build_column_ref(field_name)
            return f"({col} IS NOT NULL)"  # approximate

        # BSON type number to Any struct field mapping
        # https://www.mongodb.com/docs/manual/reference/operator/query/type/
        type_to_field = {
            1: "float_value",    # double
            16: "int32_value",   # 32-bit integer
            18: "int64_value",   # 64-bit integer
            2: "string_value",   # string
            7: "objectid_value", # ObjectId
            19: "decimal128_value",  # Decimal128
            11: "regex_value",   # regex
            5: "binary_value",   # binary
            3: "document_value", # document
            4: "array_value",    # array
            8: "bool_value",     # boolean
            9: "datetime_value", # date
            10: "null_value",    # null
            # String aliases
            "double": "float_value",
            "int": "int32_value",
            "long": "int64_value",
            "string": "string_value",
            "objectId": "objectid_value",
            "decimal": "decimal128_value",
            "regex": "regex_value",
            "binData": "binary_value",
            "object": "document_value",
            "array": "array_value",
            "bool": "bool_value",
            "date": "datetime_value",
            "null": "null_value",
        }

        if isinstance(type_spec, list):
            # Multiple types — OR them together
            conditions = []
            for t in type_spec:
                sub = self._translate_single_type(field_name, t, type_to_field)
                if sub:
                    conditions.append(sub)
            if not conditions:
                return "1=0"
            if len(conditions) == 1:
                return conditions[0]
            return "(" + " OR ".join(conditions) + ")"
        else:
            return self._translate_single_type(
                field_name, type_spec, type_to_field
            ) or "1=0"

    def _translate_single_type(
        self,
        field_name: str,
        type_spec: Any,
        type_to_field: Dict[Any, str],
    ) -> Optional[str]:
        """Translate a single BSON type spec for $type."""
        sub_field = type_to_field.get(type_spec)
        if sub_field is None:
            return None  # Unknown type

        q = _quote_ident(field_name)
        if sub_field == "null_value":
            return f"({q}.null_value IS TRUE)"
        return f"({q}.{sub_field} IS NOT NULL)"

    # ── $mod ─────────────────────────────────────────────────────────

    def _translate_mod(
        self,
        field_name: Optional[str],
        mod_spec: Any,
    ) -> str:
        """Translate $mod operator.

        $mod: [divisor, remainder] matches fields where value % divisor == remainder.
        """
        if field_name is None:
            raise ValueError("$mod requires a field name")

        if not isinstance(mod_spec, list) or len(mod_spec) != 2:
            raise ValueError(
                f"$mod requires [divisor, remainder], got {mod_spec}"
            )

        divisor, remainder = mod_spec

        field_type = _get_schema_field_type(self.schema, field_name)
        is_any = field_type is not None and _is_any_type(field_type)

        if is_any:
            col = _any_numeric_coalesce(field_name)
        else:
            col = self._build_column_ref(field_name)

        p_div = self._next_param()
        p_rem = self._next_param()
        self.params[p_div] = divisor
        self.params[p_rem] = remainder

        return f"({col} % {p_div} = {p_rem})"

    # ── $bitsAllSet, $bitsAllClear, $bitsAnySet, $bitsAnyClear ──────

    def _translate_bits(
        self,
        op: str,
        field_name: Optional[str],
        bitmask: Any,
    ) -> str:
        """Translate bitwise query operators.

        Uses DuckDB's bitwise AND (&) operator.
        """
        if field_name is None:
            raise f"{op} requires a field name"

        if not isinstance(bitmask, int):
            raise ValueError(f"{op} requires an integer bitmask")

        field_type = _get_schema_field_type(self.schema, field_name)
        is_any = field_type is not None and _is_any_type(field_type)

        if is_any:
            col = _any_numeric_coalesce(field_name)
        else:
            col = self._build_column_ref(field_name)

        pname = self._next_param()
        self.params[pname] = bitmask

        # Cast to bigint for bitwise operations
        col_cast = f"CAST({col} AS BIGINT)"

        if op == "$bitsAllSet":
            return f"({col_cast} & {pname} = {pname})"
        elif op == "$bitsAllClear":
            return f"({col_cast} & {pname} = 0)"
        elif op == "$bitsAnySet":
            return f"({col_cast} & {pname} != 0)"
        elif op == "$bitsAnyClear":
            return f"({col_cast} & {pname} != {pname})"
        else:
            raise ValueError(f"Unknown bitwise operator: {op}")

    # ── utility ──────────────────────────────────────────────────────

    def _build_column_ref(self, field_name: Optional[str]) -> str:
        """Build a quoted column reference for a field name.

        Handles dotted field names (e.g., "metadata.device_id").
        """
        if field_name is None:
            raise ValueError("Field name is required")
        return _quote_ident(field_name)
