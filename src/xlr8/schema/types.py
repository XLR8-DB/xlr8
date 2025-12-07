"""
Type definitions for XLR8 schema system. These types will be used to define MongoDB document schema.
When reading/writing Parquet files, these types will be converted to/from PyArrow types. However, 
the users can with with Types.Any which will automatically be handled while reading/writing data to fully
support flexible schema. If a field is not defined in the schema, it will be discarded when writing to Parquet.

Types define how MongoDB values map to Arrow/Parquet types.
"""
"""
Type definitions for XLR8's schema system.

This module provides type classes that define how MongoDB BSON values are mapped to Parquet types.
These types form the foundation of XLR8's schema system, enabling efficient storage and querying of MongoDB data.

Key Features:
- **Type Safety**: Explicit type definitions for MongoDB document schemas
- **Arrow Integration**: Seamless conversion between MongoDB BSON and Apache Arrow types
- **Flexible Schema**: Support for both strict and flexible schemas via Types.Any

Supported Types:
- Primitives: String, Int, Float, Bool, Timestamp, ObjectId, TODO: include all BSON types
- Complex: Struct (nested documents), List (arrays)

Schema Behavior:
- Fields defined in the schema are type-checked and converted to Arrow types
- Fields not in the schema are discarded when writing to Parquet.
- Types.Any provides a flexible escape hatch for dynamic/unknown fields which are stored as structs in Parquet and later decoded back to original BSON types via the Rust backend.

"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional
import pyarrow as pa


class BaseType(ABC):
    """Base class for all XLR8 types."""

    @abstractmethod
    def to_arrow(self) -> pa.DataType:
        """Convert to PyArrow data type."""
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def __eq__(self, other) -> bool:
        """Compare types for equality."""
        return isinstance(other, self.__class__)

    def __hash__(self) -> int:
        """Make types hashable for use in sets/dicts."""
        return hash(self.__class__.__name__)


class String(BaseType):
    """String type."""

    def to_arrow(self) -> pa.DataType:
        return pa.string()

    def __eq__(self, other) -> bool:
        return isinstance(other, String)

# TODO: Support Int32 vs Int64 based on schema definition
class Int(BaseType):
    """Integer type (always 64-bit)."""

    def to_arrow(self) -> pa.DataType:
        return pa.int64()

# TODO: Support Float32 vs Float64 based on schema definition
class Float(BaseType):
    """Floating-point type (always 64-bit)."""

    def to_arrow(self) -> pa.DataType:
        return pa.float64()

class Bool(BaseType):
    """Boolean type."""

    def to_arrow(self) -> pa.DataType:
        return pa.bool_()

    def __eq__(self, other) -> bool:
        return isinstance(other, Bool)


@dataclass(frozen=True)
class Timestamp(BaseType):
    """Timestamp type."""
    unit: str = "ns"
    tz: Optional[str] = "UTC"

    def to_arrow(self) -> pa.DataType:
        return pa.timestamp(self.unit, tz=self.tz)
    def __post_init__(self):
        if self.unit not in ("s", "ms", "us", "ns"):
            raise ValueError("Timestamp unit must be one of 's', 'ms', 'us', 'ns'")

class ObjectId(BaseType):
    """MongoDB ObjectId type (stored as string in Parquet)."""

    def to_arrow(self) -> pa.DataType:
        return pa.string()

    def __eq__(self, other) -> bool:
        return isinstance(other, ObjectId)


class Any(BaseType):
    """
    Polymorphic type - can hold any MongoDB value.

    Stored as a union struct in Parquet with fields for each possible type.
    The Rust backend handles encoding/decoding for performance.

    Supports ALL MongoDB BSON types:
    - Double (float64)
    - Int32 (int32)
    - Int64 (int64)
    - String (utf8)
    - ObjectId (hex string)
    - Decimal128 (string)
    - Regex (pattern string)
    - Binary (base64 string)
    - Document (JSON string)
    - Array (JSON string)
    - Boolean (bool)
    - Date (timestamp[ms])
    - Null (bool indicator)
    """

    def to_arrow(self) -> pa.DataType:
        """Return the Arrow struct type for polymorphic values.
        
        This schema must match the Rust backend's encode_any_values_to_arrow
        and decode_any_struct_arrow functions exactly.
        """
        return pa.struct([
            ("float_value", pa.float64()),
            ("int32_value", pa.int32()),
            ("int64_value", pa.int64()),
            ("string_value", pa.string()),
            ("objectid_value", pa.string()),
            ("decimal128_value", pa.string()),
            ("regex_value", pa.string()),
            ("binary_value", pa.string()),
            ("document_value", pa.string()),
            ("array_value", pa.string()),
            ("bool_value", pa.bool_()),
            ("datetime_value", pa.timestamp("ms")),
            ("null_value", pa.bool_()),
        ])

    def __eq__(self, other) -> bool:
        return isinstance(other, Any)


class Struct(BaseType):
    """Nested struct type."""

    def __init__(self, fields: Dict[str, BaseType]):
        """
        Args:
            fields: Dict mapping field name to type
        """
        self.fields = fields

    def to_arrow(self) -> pa.DataType:
        return pa.struct([
            (name, field_type.to_arrow())
            for name, field_type in self.fields.items()
        ])

    def __repr__(self) -> str:
        field_str = ", ".join(f"{k}: {v}" for k, v in self.fields.items())
        return f"Struct({{{field_str}}})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, Struct):
            return False
        if set(self.fields.keys()) != set(other.fields.keys()):
            return False
        return all(self.fields[k] == other.fields[k] for k in self.fields)


class List(BaseType):
    """List type."""

    def __init__(self, element_type: BaseType):
        """
        Args:
            element_type: Type of list elements
        """
        self.element_type = element_type

    def to_arrow(self) -> pa.DataType:
        return pa.list_(self.element_type.to_arrow())

    def __repr__(self) -> str:
        return f"List({self.element_type})"

    def __eq__(self, other) -> bool:
        return isinstance(other, List) and self.element_type == other.element_type
