"""
Execution Planner for XLR8.

================================================================================
MEMORY MODEL FOR RUST BACKEND
================================================================================

The Rust backend uses a memory-aware buffering system to control RAM usage
during parallel MongoDB fetches. Key concepts:

1. BSON DOCUMENT MEMORY OVERHEAD (15x Multiplier)
   When MongoDB sends documents over the wire (avg_doc_size_bytes), they expand
   to ~15x in memory due to heap allocations, pointers, and HashMap
   structures. Measured: 14.8x, rounded to 15x for safety.

2. BUFFER MANAGEMENT
   Each async worker maintains its own MemoryAwareBuffer that:
   - Tracks estimated memory using the 15x multiplier
   - Flushes to Parquet when estimated bytes >= flush_trigger_mb
   - Dynamically calibrates after first 10 documents

3. MEMORY FORMULA
   Given user's flush_ram_limit_mb and max_workers:

   Per-Worker Allocation:
     available_ram = flush_ram_limit_mb - BASELINE_MB
    cursor_overhead = max_workers x CURSOR_OVERHEAD_MB_PER_WORKER
     ram_for_data = available_ram - cursor_overhead
     worker_allocation = ram_for_data / max_workers
     flush_trigger_mb = worker_allocation  # Rust handles 15x internally

================================================================================
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Union

logger = logging.getLogger(__name__)


# =============================================================================
# BACKEND CONFIGURATION
# =============================================================================


class Backend(Enum):
    """Supported execution backends."""

    RUST = "rust"
    PYTHON = "python"  # Future use


@dataclass(frozen=True)
class BackendConfig:
    """
    Configuration constants for a specific backend.

    All values are empirically measured. See:
    - Rust: rust/xlr8_rust/tests/doc_memory_test.rs
    - Python: tests/test_schema_memory.py
    """

    # Baseline memory before any data processing
    baseline_mb: int

    # Memory expansion factor during flush/encoding
    # - Python: Arrow conversion spike (lists + arrays coexist)
    # - Rust: BSON Document heap overhead (15x serialized size)
    memory_multiplier: float

    # Per-worker MongoDB cursor overhead
    cursor_overhead_mb: int

    # Memory retention factor (Python GC holds onto freed memory)
    retention_factor: float

    # Description for logging
    description: str


# Rust backend: optimized for async workers in single process
RUST_CONFIG = BackendConfig(
    baseline_mb=7,  # Minimal Rust runtime overhead
    memory_multiplier=15.0,  # BSON Document heap overhead (measured 14.8x)
    cursor_overhead_mb=8,  # Async MongoDB cursor buffer
    retention_factor=1.0,  # Rust drops immediately, no GC retention
    description="Rust async (single process, tokio threads)",
)

# Python backend: reserved for future non-Rust implementation
PYTHON_CONFIG = BackendConfig(
    baseline_mb=120,  # pymongo + pandas + pyarrow imports
    memory_multiplier=3.0,  # Arrow conversion spike
    cursor_overhead_mb=16,  # Python async cursor overhead
    retention_factor=1.25,  # Python GC retention
    description="Python async (future implementation)",
)

# Default backend for current implementation
DEFAULT_BACKEND = Backend.RUST
DEFAULT_CONFIG = RUST_CONFIG


# =============================================================================
# SHARED CONSTANTS
# =============================================================================

# MongoDB cursor efficiency: below this, network overhead dominates
MIN_BATCH_SIZE = 2_000

# Buffer headroom for in-flight batch (flush check happens after batch added)
BATCH_HEADROOM_RATIO = 0.2


# =============================================================================
# EXECUTION PLAN
# =============================================================================


@dataclass
class ExecutionPlan:
    """
    Execution plan for parallel query execution.

    Attributes:
        worker_count: Number of parallel workers
        batch_size_docs: Documents per MongoDB cursor batch
        chunk_size: Time chunk size as timedelta
        estimated_ram_mb: Estimated peak RAM usage
        flush_trigger_mb: Memory threshold to trigger buffer flush (per worker)
    """

    worker_count: int
    batch_size_docs: int
    chunk_size: timedelta
    estimated_ram_mb: int
    flush_trigger_mb: int


# =============================================================================
# MEMORY CALCULATION
# =============================================================================


def calculate_flush_trigger(
    peak_ram_limit_mb: int,
    worker_count: int,
    avg_doc_size_bytes: int,
    config: BackendConfig = DEFAULT_CONFIG,
) -> tuple[int, int]:
    """
    Calculate flush trigger and batch size from memory constraints.

    This is the core memory planning function. It divides available RAM
    among workers while accounting for baseline overhead and cursor buffers.

    Args:
        peak_ram_limit_mb: Total RAM budget from user
        worker_count: Number of parallel workers
        avg_doc_size_bytes: Average document size for batch sizing
        config: Backend-specific memory constants

    Returns:
        Tuple of (flush_trigger_mb, batch_size_docs)

    Example:
        >>> trigger, batch = calculate_flush_trigger(5000, 16, 250)
        >>> print(f"Per-worker: {trigger}MB, batch: {batch} docs")
        Per-worker: 300MB, batch: 500000 docs
    """
    # Available RAM after baseline overhead
    available_ram_mb = peak_ram_limit_mb - config.baseline_mb

    if available_ram_mb <= 0:
        raise ValueError(
            f"peak_ram_limit_mb ({peak_ram_limit_mb} MB) must be greater than "
            f"baseline ({config.baseline_mb} MB). "
            f"Minimum viable: {config.baseline_mb + 50} MB."
        )

    # Account for GC retention (Python holds onto freed memory)
    effective_ram_mb = available_ram_mb / config.retention_factor

    # Subtract cursor overhead (each worker has a live MongoDB cursor)
    cursor_overhead_total = worker_count * config.cursor_overhead_mb
    ram_for_data = effective_ram_mb - cursor_overhead_total

    # Ensure we have at least some RAM for data
    ram_for_data = max(ram_for_data, worker_count * 1)  # At least 1 MB per worker

    # Each worker's allocation
    worker_allocation_mb = ram_for_data / worker_count

    # For Rust backend: the 15x multiplier is handled INSIDE the Rust buffer
    # So flush_trigger_mb is the actual MB limit the buffer should use
    # No need to divide by memory_multiplier here - Rust does that internally

    # Split: 80% flush trigger, 20% batch headroom
    flush_trigger_mb = worker_allocation_mb * (1 - BATCH_HEADROOM_RATIO)
    batch_headroom_mb = worker_allocation_mb * BATCH_HEADROOM_RATIO

    # Batch size from headroom
    batch_headroom_bytes = batch_headroom_mb * 1024 * 1024
    batch_size_docs = int(batch_headroom_bytes / avg_doc_size_bytes)

    # Floor at MIN_BATCH_SIZE for MongoDB efficiency
    batch_size_docs = max(MIN_BATCH_SIZE, batch_size_docs)

    # Floor flush trigger at 1 MB (sanity check)
    flush_trigger_mb = max(1, int(flush_trigger_mb))

    return flush_trigger_mb, batch_size_docs


def build_execution_plan(
    start_time: Union[datetime, None],
    end_time: Union[datetime, None],
    avg_doc_size_bytes: int,
    max_workers: int = 4,
    peak_ram_limit_mb: int = 512,
    chunking_granularity: Optional[timedelta] = timedelta(hours=8),
    num_unchunked_queries: Optional[int] = None,
    backend: Backend = DEFAULT_BACKEND,
) -> ExecutionPlan:
    """
    Build execution plan for a time-range query, unchunked queries, or both.

    All parameters derived from user inputs and empirically measured constants.
    No arbitrary hardcodes.

    Work items = (time chunks from full brackets) + (unchunked queries)

    Unchunked queries include:
    - Partial brackets: one-sided time bound (e.g., $gte only)
    - Unbounded brackets: no time bounds at all

    Args:
        start_time: Query start time (None if no chunkable brackets)
        end_time: Query end time (None if no chunkable brackets)
        avg_doc_size_bytes: Average document size from schema
        max_workers: Maximum workers (user-specified)
        peak_ram_limit_mb: Total RAM budget (user-specified)
        chunking_granularity: Time chunk size (optional, for time-range mode)
        num_unchunked_queries: Number of unchunked queries
            (partial + unbounded brackets)
        backend: Execution backend (RUST or PYTHON)

    Returns:
        ExecutionPlan with memory-safe settings

    Raises:
        ValueError: If RAM budget is too low for single worker
        ValueError: If no work items (neither time range nor unchunked queries)
    """
    config = RUST_CONFIG if backend == Backend.RUST else PYTHON_CONFIG

    # ==========================================================================
    # CALCULATE TOTAL WORK ITEMS
    # ==========================================================================
    # Total work = (time chunks from full brackets) + (unchunked queries)
    # Unchunked queries = partial brackets + unbounded brackets
    # ==========================================================================
    chunk_size_seconds: Optional[int] = None
    time_chunks = 0

    if start_time is not None and end_time is not None:
        # Calculate time chunks from chunkable (full) brackets
        time_range = end_time - start_time
        time_range_seconds = max(1, time_range.total_seconds())

        if chunking_granularity is not None:
            chunk_size_seconds = int(chunking_granularity.total_seconds())
            time_chunks = max(
                1,
                int(
                    (time_range_seconds + chunk_size_seconds - 1) // chunk_size_seconds
                ),
            )
        else:
            # No granularity specified, treat as single chunk
            time_chunks = 1

    # Add unchunked queries (partial + unbounded brackets)
    unchunked = num_unchunked_queries or 0
    num_chunks = time_chunks + unchunked

    if num_chunks == 0:
        raise ValueError(
            "No work items found. Either (start_time, end_time) "
            "or num_unchunked_queries must be provided to determine work distribution."
        )
    # ==========================================================================
    # DETERMINE WORKER COUNT
    # ==========================================================================
    # Can't have more workers than chunks
    worker_count = min(max_workers, num_chunks)
    worker_count = max(1, worker_count)

    # Check if we have enough RAM for this many workers
    available_ram_mb = peak_ram_limit_mb - config.baseline_mb
    effective_ram = available_ram_mb / config.retention_factor
    min_ram_per_worker = config.cursor_overhead_mb + 1  # Cursor + 1MB buffer
    max_workers_for_ram = max(1, int(effective_ram / min_ram_per_worker))

    if worker_count > max_workers_for_ram:
        logger.warning(
            "RAM budget too tight for %d workers. Reducing to %d workers. "
            "Consider increasing peak_ram_limit_mb from %d MB.",
            worker_count,
            max_workers_for_ram,
            peak_ram_limit_mb,
        )
        worker_count = max_workers_for_ram

    # ==========================================================================
    # CALCULATE MEMORY PARAMETERS
    # ==========================================================================
    flush_trigger_mb, batch_size_docs = calculate_flush_trigger(
        peak_ram_limit_mb=peak_ram_limit_mb,
        worker_count=worker_count,
        avg_doc_size_bytes=avg_doc_size_bytes,
        config=config,
    )

    # Warn if flush trigger is very small
    if flush_trigger_mb < 5:
        logger.warning(
            "Low memory budget results in %d MB flush trigger per worker. "
            "This may create many small Parquet files. Consider reducing max_workers "
            "from %d or increasing peak_ram_limit_mb from %d MB.",
            flush_trigger_mb,
            max_workers,
            peak_ram_limit_mb,
        )

    # ==========================================================================
    # ESTIMATE PEAK RAM USAGE
    # ==========================================================================
    cursor_overhead_total = worker_count * config.cursor_overhead_mb
    # For Rust: memory_multiplier is handled inside buffer, not here
    # Estimate is: baseline + cursors + (flush_trigger x workers)
    data_buffers = worker_count * flush_trigger_mb

    allocated = cursor_overhead_total + data_buffers
    estimated_ram_mb = int(config.baseline_mb + allocated * config.retention_factor)
    estimated_ram_mb = min(estimated_ram_mb, peak_ram_limit_mb)

    # Store chunk size as timedelta
    chunk_size_td = (
        timedelta(seconds=chunk_size_seconds)
        if chunk_size_seconds is not None
        else timedelta(days=1)
    )

    return ExecutionPlan(
        worker_count=worker_count,
        batch_size_docs=batch_size_docs,
        chunk_size=chunk_size_td,
        estimated_ram_mb=estimated_ram_mb,
        flush_trigger_mb=flush_trigger_mb,
    )


__all__ = [
    "Backend",
    "BackendConfig",
    "RUST_CONFIG",
    "PYTHON_CONFIG",
    "DEFAULT_BACKEND",
    "DEFAULT_CONFIG",
    "ExecutionPlan",
    "calculate_flush_trigger",
    "build_execution_plan",
]
