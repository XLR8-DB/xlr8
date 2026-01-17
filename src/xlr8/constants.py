"""
XLR8 constants and configuration values.

Centralized constants to avoid magic numbers scattered throughout codebase.
All tuneable performance parameters should be defined here.
"""

# =============================================================================
# PARQUET FILE SETTINGS
# =============================================================================

# Default row group size for compression can be altered via argument passed
# to the special cursor methods e.g to_dataframe
PARQUET_ROW_GROUP_SIZE = 100_000

# Default compression codec for Parquet files
DEFAULT_COMPRESSION = "zstd"

# =============================================================================
# BATCH PROCESSING
# =============================================================================

# Default batch size for DataFrame operations
DEFAULT_BATCH_SIZE = 10_000
