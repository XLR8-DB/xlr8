"""
Schema definition for XLR8.

Schema describes the structure of MongoDB documents and how they map to Arrow/Parquet.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

import pyarrow as pa

from .types import Any as AnyType
from .types import (
    BaseType,
    Bool,
    DateTime,
    Float,
    Int,
    ObjectId,
    String,
    Struct,
    Timestamp,
)

# Scalar types where $gt/$gte/$lt/$lte have a well-defined ordering.
_ORDERABLE_TYPES = (Int, Float, Bool, Timestamp, DateTime, String, ObjectId)


@dataclass(frozen=True)
class ResolvedField:
    """Result of walking a dotted path through a Schema.

    `is_any` is True iff the path terminates at a ``Types.Any()`` field, or the
    path crosses a ``Types.Any()`` node (in which case the sub-path is stored
    inside an Any blob and we cannot know its type statically). Those filters
    must be applied post-decode in memory rather than pushed down.
    `is_orderable` is True iff the terminal type supports $gt/$gte/$lt/$lte.
    """

    field_type: BaseType
    is_any: bool
    is_orderable: bool


class Schema:
    """
    Defines the structure of MongoDB documents for XLR8 acceleration.

    Schema is required to:
    - Convert MongoDB documents to Arrow tables
    - Store data efficiently in Parquet
    - Reconstruct DataFrames with correct types

    Example:
        ```python
        schema = Schema(
            time_field="timestamp",
            fields={
                "timestamp": Types.Timestamp("ns", tz="UTC"),
                "sensor_id": Types.String(),
                "value": Types.Float(),
                "metadata": Types.Any,  # Polymorphic
            }
        )
        ```
    """

    def __init__(
        self,
        time_field: str,
        fields: Dict[str, BaseType],
        avg_doc_size_bytes: int = 500,
        flatten_nested: bool = True,
    ):
        """
        Create a schema definition.

        Args:
            time_field: Name of the timestamp field (required for chunking)
            fields: Dict mapping field name to XLR8 type
            avg_doc_size_bytes: Average BSON document size in bytes (default: 500)
                Used for memory-aware batch sizing and execution planning
            flatten_nested: If True, flatten nested paths like "metadata.user_id"

        Raises:
            ValueError: If time_field not in fields or not Timestamp type
        """
        self.time_field = time_field
        self.fields = fields
        self.avg_doc_size_bytes = avg_doc_size_bytes
        self.flatten_nested = flatten_nested
        self._validate()

    SPEC_VERSION = 1

    def _validate(self):
        """Validate schema configuration."""
        if self.time_field not in self.fields:
            raise ValueError(
                f"time_field '{self.time_field}' must be present in fields. "
                f"Available fields: {list(self.fields.keys())}"
            )

        time_field_type = self.fields[self.time_field]
        if not isinstance(time_field_type, (Timestamp, DateTime)):
            raise ValueError(
                f"time_field '{self.time_field}' must be Timestamp or DateTime type, "
                f"got {type(time_field_type).__name__}"
            )

    def to_arrow_schema(self) -> pa.Schema:
        """
        Convert to PyArrow schema.

        Returns:
            PyArrow schema object
        """
        return pa.schema(
            [(name, field_type.to_arrow()) for name, field_type in self.fields.items()]
        )

    def get_any_fields(self) -> List[str]:
        """
        Get list of fields with Types.Any (polymorphic types).

        Returns:
            List of field names that are Any type
        """
        return [
            name
            for name, field_type in self.fields.items()
            if isinstance(field_type, AnyType)
        ]

    def get_field_names(self) -> List[str]:
        """
        Get all field names in schema.

        Returns:
            List of field names
        """
        return list(self.fields.keys())

    def has_field(self, name: str) -> bool:
        """
        Check if field exists in schema.

        Args:
            name: Field name to check

        Returns:
            True if field exists
        """
        return name in self.fields

    def get_field_type(self, name: str) -> BaseType:
        """
        Get type for a field.

        Args:
            name: Field name

        Returns:
            XLR8 type object

        Raises:
            KeyError: If field not in schema
        """
        return self.fields[name]

    def resolve_path(self, dotted: str) -> Optional[ResolvedField]:
        """Resolve a dotted MongoDB path against the schema.

        Two shapes are supported (both appear in the wild):

        1. Flat dotted keys: ``{"metadata.device_id": Types.ObjectId()}``
        2. Nested struct: ``{"metadata": Types.Struct({"device_id": ...})}``

        Both forms return the same leaf type for ``"metadata.device_id"``.

        Any() semantics:
        - If the schema declares ``"metadata": Types.Any()`` and the caller
          asks for ``"metadata.user_id"``, we return the Any type with
          ``is_any=True`` — the sub-path is stored inside the Any blob and the
          filter must be applied post-decode in memory.
        - A terminal Any() field returns ``is_any=True`` too.

        Returns ``None`` when the path cannot be resolved (unknown field).
        """
        # Fast path: flat dotted key match.
        flat = self.fields.get(dotted)
        if flat is not None:
            return ResolvedField(
                field_type=flat,
                is_any=isinstance(flat, AnyType),
                is_orderable=isinstance(flat, _ORDERABLE_TYPES),
            )

        # Walk parts through nested Struct / Any.
        parts = dotted.split(".")
        current: Optional[BaseType] = self.fields.get(parts[0])
        if current is None:
            return None

        for part in parts[1:]:
            if isinstance(current, AnyType):
                # Rest of the path lives inside an Any blob — can't push down.
                return ResolvedField(
                    field_type=current, is_any=True, is_orderable=False
                )
            if isinstance(current, Struct):
                next_type = current.fields.get(part)
                if next_type is None:
                    return None
                current = next_type
                continue
            # Hit a leaf scalar but there's more path to walk — dead end.
            return None

        if current is None:
            return None
        return ResolvedField(
            field_type=current,
            is_any=isinstance(current, AnyType),
            is_orderable=isinstance(current, _ORDERABLE_TYPES),
        )

    def to_spec(self) -> Dict[str, object]:
        """Export schema to a JSON-serializable specification.

        Converts Python schema objects to a plain dict that can be:
        - Saved to disk (JSON/YAML)
        - Transmitted over network
        - Consumed by native backends (e.g., Rust)
        - Reconstructed later using from_spec()

        The spec format is intentionally generic and uses introspection to
        automatically handle any user-defined Types.* classes without
        hardcoding each type. This means you can add new type classes and
        they'll automatically work with serialization/deserialization.

        Returns:
            Dict containing schema version, time field, and field specifications

        Example:
            >>> schema = Schema(
            ...     time_field="ts",
            ...     fields={"ts": Timestamp("ms"), "value": Float()}
            ... )
            >>> spec = schema.to_spec()
            >>> # Later: schema2 = Schema.from_spec(spec)
        """
        from . import types as Types  # local to avoid cycles

        fields_spec: List[Dict[str, object]] = []
        for name, f in self.fields.items():
            entry: Dict[str, object] = {"name": name}

            if isinstance(f, (Types.Timestamp, Types.DateTime)):
                # Both Timestamp and DateTime serialize the same way
                # DateTime is just a convenience wrapper that defaults to "ms"
                entry.update(
                    {
                        "kind": "timestamp",
                        "unit": getattr(f, "unit", "ms"),
                        "tz": getattr(f, "tz", "UTC") or "UTC",
                    }
                )
            elif isinstance(f, Types.ObjectId):
                entry.update({"kind": "objectid"})
            elif isinstance(f, Types.Any):
                # Preserve Any() union/bitmap layout; the concrete
                # encoder decides how to materialize this.
                any_layout: Dict[str, object] = {
                    "variants": [
                        {"name": "int64", "id": 0},
                        {"name": "float64", "id": 1},
                        {"name": "bool", "id": 2},
                        {"name": "string", "id": 3},
                        {"name": "timestamp_ms_utc", "id": 4},
                        {"name": "json_blob", "id": 5},
                    ],
                }

                # If the Any type exposes explicit bitmap/payload
                # field naming, surface that; otherwise let the
                # backend choose sensible defaults.
                bitmap_field = getattr(f, "bitmap_field_name", None)
                payload_field = getattr(f, "payload_field_name", None)
                if bitmap_field is not None:
                    any_layout["bitmap_field"] = bitmap_field
                if payload_field is not None:
                    any_layout["payload_field"] = payload_field

                entry.update({"kind": "any", "any_layout": any_layout})
            elif isinstance(f, Types.Int):
                entry.update({"kind": "int64"})
            elif isinstance(f, Types.Float):
                entry.update({"kind": "float64"})
            elif isinstance(f, Types.String):
                entry.update({"kind": "string"})
            elif isinstance(f, Types.Bool):
                entry.update({"kind": "bool"})
            elif isinstance(f, Types.List):
                # List type - serialize with element type info
                # Map element type to kind string
                elem_type = f.element_type
                if isinstance(elem_type, Types.Float):
                    elem_kind = "float"
                elif isinstance(elem_type, Types.Int):
                    elem_kind = "int"
                elif isinstance(elem_type, Types.String):
                    elem_kind = "string"
                elif isinstance(elem_type, Types.Bool):
                    elem_kind = "bool"
                elif isinstance(elem_type, Types.DateTime):
                    elem_kind = "datetime"
                elif isinstance(elem_type, Types.ObjectId):
                    elem_kind = "objectid"
                else:
                    raise ValueError(
                        f"Unsupported List element type: {type(elem_type).__name__}. "
                        f"Supported types: Float, Int, String, Bool, DateTime, ObjectId"
                    )
                entry.update({"kind": f"list:{elem_kind}"})
            else:
                # Conservative fallback: treat as json_blob-backed Any.
                entry.update(
                    {
                        "kind": "any",
                        "any_layout": {
                            "variants": [{"name": "json_blob", "id": 0}],
                        },
                    }
                )

            fields_spec.append(entry)

        return {
            "version": self.SPEC_VERSION,
            "time_field": self.time_field,
            "avg_doc_size_bytes": self.avg_doc_size_bytes,
            "fields": fields_spec,
        }

    def __repr__(self) -> str:
        field_lines = [
            f"  {name}: {field_type}" for name, field_type in self.fields.items()
        ]
        return (
            f"Schema(time_field='{self.time_field}',\n" + "\n".join(field_lines) + "\n)"
        )
