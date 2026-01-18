"""
Query analysis and execution planning.

This module provides utilities for analyzing MongoDB queries and
creating optimal execution plans for parallel processing.
"""

from .brackets import (
    build_brackets_for_find,
)
from .chunker import (
    chunk_time_range,
)
from .inspector import (
    # Operator classification sets
    ALWAYS_ALLOWED,
    CONDITIONAL,
    NEVER_ALLOWED,
    # Validation
    ValidationResult,
    check_conditional_operators,
    extract_time_bounds_recursive,
    generate_sort_sql,
    get_sort_field_info,
    has_forbidden_ops,
    is_chunkable_query,
    normalize_datetime,
    # Query analysis
    or_depth,
    split_global_and,
    validate_query_for_chunking,
    validate_sort_field,
)

__all__ = [
    # inspector - operator sets
    "ALWAYS_ALLOWED",
    "CONDITIONAL",
    "NEVER_ALLOWED",
    # inspector - validation
    "ValidationResult",
    "has_forbidden_ops",
    "validate_query_for_chunking",
    "validate_sort_field",
    "get_sort_field_info",
    "generate_sort_sql",
    "check_conditional_operators",
    # inspector - analysis
    "or_depth",
    "split_global_and",
    "normalize_datetime",
    "extract_time_bounds_recursive",
    "is_chunkable_query",
    # brackets
    "build_brackets_for_find",
    # chunker
    "chunk_time_range",
]
