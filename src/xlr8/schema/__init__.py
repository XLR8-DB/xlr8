"""
Schema system for XLR8.

Provides types and schema definitions for MongoDB documents.
"""

from .types import (
    BaseType,
    String,
    Int,
    Float,
    Bool,
    Timestamp,
    ObjectId,
    Any,
    Struct,
    List,
)

# Import types module for Types.X syntax
from . import types as Types
from .schema import Schema

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
    "Schema"
]
