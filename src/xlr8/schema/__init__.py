"""
Schema system for XLR8.

Provides types, schema definitions, and value encoding for MongoDB documents.
"""

# Import types module for Types.X syntax
from . import types as Types
from .encoder import ValueEncoder
from .schema import Schema
from .types import (
    Any,
    BaseType,
    Bool,
    Float,
    Int,
    List,
    ObjectId,
    String,
    Struct,
    Timestamp,
)

__all__ = [
    # Types module for Types.X syntax
    "Types",
    # Individual type classes
    "BaseType",
    "String",
    "Int",
    "Float",
    "Bool",
    "Timestamp",
    "ObjectId",
    "Any",
    "Struct",
    "List",
    # Schema
    "Schema",
    # Encoder
    "ValueEncoder",
]
