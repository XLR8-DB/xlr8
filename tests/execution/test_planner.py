"""
Tests for planner.py module.

Memory planning correctness is critical for preventing OOM errors.
"""

from datetime import datetime, timedelta

import pytest

from xlr8.execution.planner import (
    PYTHON_CONFIG,
    RUST_CONFIG,
    ExecutionPlan,
    build_execution_plan,
    calculate_flush_trigger,
)


class TestBackendConfig:
    """Test backend configuration dataclasses."""

    def test_rust_config_values(self):
        """RUST_CONFIG should have expected values."""
        assert RUST_CONFIG.baseline_mb == 7
        assert RUST_CONFIG.memory_multiplier == 15.0
        assert RUST_CONFIG.cursor_overhead_mb == 8
        assert RUST_CONFIG.retention_factor == 1.0
        assert "Rust" in RUST_CONFIG.description

    def test_python_config_values(self):
        """PYTHON_CONFIG should have expected values."""
        assert PYTHON_CONFIG.baseline_mb == 120
        assert PYTHON_CONFIG.memory_multiplier == 3.0
        assert PYTHON_CONFIG.cursor_overhead_mb == 16
        assert PYTHON_CONFIG.retention_factor == 1.25
        assert "Python" in PYTHON_CONFIG.description

    def test_configs_are_immutable(self):
        """Configs should be immutable (frozen dataclass)."""
        with pytest.raises(AttributeError):
            RUST_CONFIG.baseline_mb = 100  # type: ignore

        with pytest.raises(AttributeError):
            PYTHON_CONFIG.baseline_mb = 200  # type: ignore


class TestCalculateFlushTrigger:
    """Test calculate_flush_trigger() function."""

    def test_basic_calculation(self):
        """Basic calculation with typical inputs."""
        worker_count = 4
        flush_mb, batch_size = calculate_flush_trigger(
            peak_ram_limit_mb=1000,
            worker_count=worker_count,
            avg_doc_size_bytes=250,
            config=RUST_CONFIG,
        )

        # Should have reasonable values
        assert flush_mb > 0
        assert batch_size >= 2000  # MIN_BATCH_SIZE
        assert flush_mb * worker_count < 1000  # Total < peak

    def test_low_ram_reduces_flush_trigger(self):
        """Low RAM budget should reduce flush trigger."""
        flush_high, _ = calculate_flush_trigger(
            peak_ram_limit_mb=2000,
            worker_count=4,
            avg_doc_size_bytes=250,
            config=RUST_CONFIG,
        )

        flush_low, _ = calculate_flush_trigger(
            peak_ram_limit_mb=500,
            worker_count=4,
            avg_doc_size_bytes=250,
            config=RUST_CONFIG,
        )

        assert flush_low < flush_high

    def test_many_workers_smaller_allocation(self):
        """Many workers should result in smaller per-worker allocation."""
        flush_few, _ = calculate_flush_trigger(
            peak_ram_limit_mb=1000,
            worker_count=2,
            avg_doc_size_bytes=250,
            config=RUST_CONFIG,
        )

        flush_many, _ = calculate_flush_trigger(
            peak_ram_limit_mb=1000,
            worker_count=8,
            avg_doc_size_bytes=250,
            config=RUST_CONFIG,
        )

        assert flush_many < flush_few

    def test_min_batch_size_enforced(self):
        """Batch size should be at least 2000."""
        _, batch_size = calculate_flush_trigger(
            peak_ram_limit_mb=100,
            worker_count=10,
            avg_doc_size_bytes=1000,  # Large docs
            config=RUST_CONFIG,
        )

        assert batch_size >= 2000

    def test_min_flush_trigger_enforced(self):
        """Flush trigger should be at least 1 MB."""
        flush_mb, _ = calculate_flush_trigger(
            peak_ram_limit_mb=50,
            worker_count=20,
            avg_doc_size_bytes=100,
            config=RUST_CONFIG,
        )

        assert flush_mb >= 1

    def test_raises_on_insufficient_ram(self):
        """Should raise ValueError if RAM < baseline."""
        with pytest.raises(ValueError, match="must be greater than"):
            calculate_flush_trigger(
                peak_ram_limit_mb=5,  # Less than RUST_CONFIG.baseline_mb (7)
                worker_count=4,
                avg_doc_size_bytes=250,
                config=RUST_CONFIG,
            )


class TestBuildExecutionPlan:
    """Test build_execution_plan() function."""

    def test_time_range_query(self):
        """Time-range query should produce appropriate worker count."""
        plan = build_execution_plan(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            avg_doc_size_bytes=250,
            max_workers=10,
            peak_ram_limit_mb=1000,
            chunking_granularity=timedelta(days=7),
        )

        assert isinstance(plan, ExecutionPlan)
        assert plan.worker_count > 0
        assert plan.worker_count <= 10
        assert plan.batch_size_docs >= 2000
        assert plan.flush_trigger_mb > 0
        assert plan.estimated_ram_mb <= 1000

    def test_worker_count_capped_by_chunks(self):
        """Worker count should be capped by number of chunks."""
        # 10 days, 5-day chunks = 2 chunks
        plan = build_execution_plan(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 11),
            avg_doc_size_bytes=250,
            max_workers=10,  # Request 10 workers
            peak_ram_limit_mb=2000,
            chunking_granularity=timedelta(days=5),
        )

        # Should cap at 2 workers (can't have more workers than chunks)
        assert plan.worker_count <= 2

    def test_worker_count_reduced_if_ram_insufficient(self):
        """Worker count should reduce if RAM insufficient."""
        plan = build_execution_plan(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 12, 31),
            avg_doc_size_bytes=250,
            max_workers=50,  # Request many workers
            peak_ram_limit_mb=100,  # But low RAM
            chunking_granularity=timedelta(days=7),
        )

        # Should reduce workers to fit RAM
        assert plan.worker_count < 50
        assert plan.worker_count >= 1

    def test_unchunked_queries_only(self):
        """Should handle unchunked queries (no time range)."""
        plan = build_execution_plan(
            start_time=None,
            end_time=None,
            avg_doc_size_bytes=250,
            max_workers=10,
            peak_ram_limit_mb=1000,
            num_unchunked_queries=5,  # 5 unchunked queries
        )

        assert plan.worker_count <= 5
        assert plan.worker_count >= 1

    def test_mixed_workload(self):
        """Should handle mixed time chunks + unchunked queries."""
        plan = build_execution_plan(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 15),  # 15 days
            avg_doc_size_bytes=250,
            max_workers=10,
            peak_ram_limit_mb=1000,
            chunking_granularity=timedelta(days=7),  # 2 time chunks
            num_unchunked_queries=3,  # + 3 unchunked = 5 total
        )

        # Should have workers for all work items
        assert plan.worker_count <= 5
        assert plan.worker_count >= 1

    def test_raises_on_no_work_items(self):
        """Should raise ValueError if no work items."""
        with pytest.raises(ValueError, match="No work items"):
            build_execution_plan(
                start_time=None,
                end_time=None,
                avg_doc_size_bytes=250,
                max_workers=10,
                peak_ram_limit_mb=1000,
                num_unchunked_queries=None,  # No work!
            )

    def test_chunk_size_calculated_from_granularity(self):
        """chunk_size should be calculated from chunking_granularity."""
        plan = build_execution_plan(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            avg_doc_size_bytes=250,
            max_workers=10,
            peak_ram_limit_mb=1000,
            chunking_granularity=timedelta(hours=8),
        )

        assert plan.chunk_size == timedelta(hours=8)


class TestMemoryEstimation:
    """Test memory estimation in execution plans."""

    def test_estimated_ram_within_limit(self):
        """Estimated RAM should be <= peak_ram_limit_mb."""
        for ram_limit in [500, 1000, 2000, 5000]:
            plan = build_execution_plan(
                start_time=datetime(2024, 1, 1),
                end_time=datetime(2024, 12, 31),
                avg_doc_size_bytes=250,
                max_workers=10,
                peak_ram_limit_mb=ram_limit,
                chunking_granularity=timedelta(days=7),
            )

            assert plan.estimated_ram_mb <= ram_limit

    def test_more_workers_increase_estimate(self):
        """More workers should increase estimated RAM."""
        plan_few = build_execution_plan(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 12, 31),
            avg_doc_size_bytes=250,
            max_workers=2,
            peak_ram_limit_mb=5000,
            chunking_granularity=timedelta(days=30),  # Many chunks
        )

        plan_many = build_execution_plan(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 12, 31),
            avg_doc_size_bytes=250,
            max_workers=20,
            peak_ram_limit_mb=5000,
            chunking_granularity=timedelta(days=7),  # Many chunks
        )

        assert plan_many.worker_count > plan_few.worker_count
        assert plan_many.estimated_ram_mb >= plan_few.estimated_ram_mb

    def test_baseline_and_overhead_accounted(self):
        """Baseline and cursor overhead should be accounted for."""
        plan = build_execution_plan(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 31),
            avg_doc_size_bytes=250,
            max_workers=4,
            peak_ram_limit_mb=1000,
            chunking_granularity=timedelta(days=7),
        )

        # Estimated should include baseline + cursors + buffers
        # For RUST_CONFIG: baseline=7MB, cursor=8MB per worker
        min_estimate = RUST_CONFIG.baseline_mb + (
            plan.worker_count * RUST_CONFIG.cursor_overhead_mb
        )

        assert plan.estimated_ram_mb >= min_estimate
