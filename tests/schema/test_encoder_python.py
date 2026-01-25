"""
Comprehensive tests for XLR8's ValueEncoder using property-based testing.

Uses MockDataGenerator to create random data and validates full round-trip:
1. Generate random data
2. Encode using ValueEncoder.encode_any()
3. Store in Parquet
4. Read back from Parquet
5. Decode using ValueEncoder.decode_any()
6. Verify original data matches decoded data
"""

import logging
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest
from bson import ObjectId

from tests.utils.data_generator import MockDataGenerator
from xlr8.schema.encoder import ValueEncoder


class TestValueEncoderRoundTrip:
    """Test ValueEncoder with real data generation."""

    @pytest.fixture
    def generator(self):
        """Create data generator with fixed seed for reproducibility."""
        return MockDataGenerator(seed=42)

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for Parquet files."""
        with TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_encode_decode_null(self, generator):
        """Test encoding and decoding null values."""
        value = generator.null()

        # Encode
        encoded = ValueEncoder.encode_any(value)

        # Verify structure
        assert isinstance(encoded, dict)
        assert encoded["null_value"] is True
        assert all(encoded[k] is None for k in encoded if k != "null_value")

        # Decode
        decoded = ValueEncoder.decode_any(encoded)
        assert decoded is None

    def test_encode_decode_boolean(self, generator):
        """Test encoding and decoding boolean values."""
        for _ in range(10):
            value = generator.boolean()

            # Round-trip
            encoded = ValueEncoder.encode_any(value)
            decoded = ValueEncoder.decode_any(encoded)

            # Verify
            assert encoded["bool_value"] == value
            assert decoded == value
            assert isinstance(decoded, bool)

    def test_encode_decode_integer(self, generator):
        """Test encoding and decoding integer values."""
        for _ in range(20):
            value = generator.integer()

            # Round-trip
            encoded = ValueEncoder.encode_any(value)
            decoded = ValueEncoder.decode_any(encoded)

            # Verify
            assert encoded["int_value"] == value
            assert decoded == value
            assert isinstance(decoded, int)

    def test_encode_decode_float(self, generator):
        """Test encoding and decoding float values."""
        for _ in range(20):
            value = generator.float_value()

            # Round-trip
            encoded = ValueEncoder.encode_any(value)
            decoded = ValueEncoder.decode_any(encoded)

            # Verify (use approximate comparison for floats)
            assert encoded["float_value"] == pytest.approx(value)
            assert decoded == pytest.approx(value)
            assert isinstance(decoded, float)

    def test_encode_decode_string(self, generator):
        """Test encoding and decoding string values."""
        for charset in ["ascii", "alphanumeric", "unicode"]:
            for _ in range(10):
                value = generator.string(max_len=100, charset=charset)

                # Round-trip
                encoded = ValueEncoder.encode_any(value)
                decoded = ValueEncoder.decode_any(encoded)

                # Verify
                assert encoded["string_value"] == value
                assert decoded == value
                assert isinstance(decoded, str)

    def test_encode_decode_objectid(self, generator):
        """Test encoding and decoding ObjectId values."""
        for _ in range(10):
            value = generator.objectid()

            # Round-trip
            encoded = ValueEncoder.encode_any(value)
            decoded = ValueEncoder.decode_any(encoded)

            # Verify (ObjectId stored as string)
            assert encoded["objectid_value"] == str(value)
            assert decoded == ObjectId(str(value))
            assert isinstance(decoded, ObjectId)

    def test_encode_decode_datetime(self, generator):
        """Test encoding and decoding datetime values."""
        for _ in range(10):
            value = generator.datetime_value()

            # Round-trip
            encoded = ValueEncoder.encode_any(value)
            decoded = ValueEncoder.decode_any(encoded)

            # Verify
            assert encoded["datetime_value"] == value
            assert decoded == value
            assert isinstance(decoded, datetime)
            assert decoded.tzinfo is not None  # Should have timezone

    def test_encode_batch_mixed_types(self, generator):
        """Test batch encoding of mixed types."""
        # Generate 100 random values of different types
        values = [generator.any_value(include_complex=False) for _ in range(100)]

        # Batch encode
        encoded_batch = ValueEncoder.encode_batch(values)

        # Verify structure
        assert len(encoded_batch) == 100
        assert all(isinstance(e, dict) for e in encoded_batch)

        # Batch decode
        decoded_batch = ValueEncoder.decode_batch(encoded_batch)

        # Verify each value
        for original, decoded in zip(values, decoded_batch):
            if isinstance(original, float):
                assert decoded == pytest.approx(original)
            elif isinstance(original, ObjectId):
                assert str(decoded) == str(original)
            else:
                assert decoded == original

    def test_parquet_roundtrip_with_any_type(self, generator, temp_dir):
        """
        Test full Parquet round-trip with Type.Any fields.

        This is the REAL test - can we store mixed-type data in Parquet
        and get it back correctly?
        """
        # Generate 1000 random documents with Type.Any field
        num_docs = 1000
        docs = []

        for i in range(num_docs):
            docs.append(
                {
                    "id": i,
                    "value": generator.any_value(include_complex=False),
                    "timestamp": generator.datetime_value(),
                }
            )

        # Encode the "value" field (Type.Any)
        encoded_values = ValueEncoder.encode_batch([d["value"] for d in docs])

        # Create DataFrame with encoded values
        df = pd.DataFrame(
            {
                "id": [d["id"] for d in docs],
                "value_encoded": encoded_values,
                "timestamp": [d["timestamp"] for d in docs],
            }
        )

        # Write to Parquet
        parquet_file = temp_dir / "test_any_type.parquet"
        df.to_parquet(parquet_file, engine="pyarrow", index=False)

        # Read back
        df_read = pd.read_parquet(parquet_file, engine="pyarrow")

        # Decode values
        decoded_values = ValueEncoder.decode_batch(df_read["value_encoded"].tolist())

        # Verify ALL values match
        original_values = [d["value"] for d in docs]
        for i, (original, decoded) in enumerate(zip(original_values, decoded_values)):
            if isinstance(original, float):
                assert decoded == pytest.approx(original), (
                    f"Row {i}: {original} != {decoded}"
                )
            elif isinstance(original, ObjectId):
                assert str(decoded) == str(original), (
                    f"Row {i}: {original} != {decoded}"
                )
            elif original is None:
                assert decoded is None, f"Row {i}: expected None, got {decoded}"
            else:
                assert decoded == original, f"Row {i}: {original} != {decoded}"

        logging.debug(
            f"✅ Successfully round-tripped {num_docs} documents with mixed types!"
        )

    def test_type_ordering_edge_cases(self, generator):
        """Test encoding/decoding of edge cases for sorting."""
        edge_cases = generator.generate_sorting_edge_cases()

        for description, value in edge_cases:
            # Skip types not handled by ValueEncoder
            if isinstance(value, float) and (
                value == float("inf") or value == float("-inf")
            ):
                continue
            # Binary data is JSON-serialized as string representation
            if isinstance(value, bytes):
                continue

            # Round-trip
            encoded = ValueEncoder.encode_any(value)
            decoded = ValueEncoder.decode_any(encoded)

            # Verify
            if isinstance(value, float):
                assert decoded == pytest.approx(value), (
                    f"Failed on {description}: {value}"
                )
            elif isinstance(value, ObjectId):
                assert str(decoded) == str(value), f"Failed on {description}: {value}"
            else:
                assert decoded == value, f"Failed on {description}: {value}"

    def test_bool_vs_int_priority(self, generator):
        """
        CRITICAL: Test that bool is checked before int during encoding.

        In Python, isinstance(True, int) returns True, so we must
        check bool BEFORE int to avoid encoding booleans as integers.
        """
        # True should encode as bool, not int
        encoded_true = ValueEncoder.encode_any(True)
        assert encoded_true["bool_value"] is True
        assert encoded_true["int_value"] is None

        # False should encode as bool, not int
        encoded_false = ValueEncoder.encode_any(False)
        assert encoded_false["bool_value"] is False
        assert encoded_false["int_value"] is None

        # Integer 1 should encode as int, not bool
        encoded_one = ValueEncoder.encode_any(1)
        assert encoded_one["int_value"] == 1
        assert encoded_one["bool_value"] is None

        # Integer 0 should encode as int, not bool
        encoded_zero = ValueEncoder.encode_any(0)
        assert encoded_zero["int_value"] == 0
        assert encoded_zero["bool_value"] is None

    def test_empty_values(self, generator):
        """Test encoding of empty/edge case values."""
        test_cases = [
            ("empty_string", ""),
            ("zero_int", 0),
            ("zero_float", 0.0),
            ("false", False),
        ]

        for description, value in test_cases:
            encoded = ValueEncoder.encode_any(value)
            decoded = ValueEncoder.decode_any(encoded)

            if isinstance(value, float):
                assert decoded == pytest.approx(value)
            else:
                assert decoded == value, f"Failed on {description}"


class TestMockDataGenerator:
    """Test the MockDataGenerator itself."""

    def test_generator_reproducibility(self):
        """Verify generator produces same values with same seed."""
        gen1 = MockDataGenerator(seed=123)
        gen2 = MockDataGenerator(seed=123)

        # Should produce identical sequences
        for _ in range(10):
            assert gen1.integer() == gen2.integer()
            assert gen1.float_value() == gen2.float_value()
            assert gen1.string() == gen2.string()

    def test_generate_batch(self):
        """Test batch generation with schema."""
        gen = MockDataGenerator(seed=42)

        schema = {
            "user_id": "int",
            "username": "string",
            "active": "bool",
            "created_at": "datetime",
            "score": "float",
            "tags": "array_string",
        }

        docs = gen.generate_batch(schema, num_docs=50)

        # Verify count
        assert len(docs) == 50

        # Verify all have required fields
        for doc in docs:
            assert "user_id" in doc
            assert "username" in doc
            assert "active" in doc
            assert "created_at" in doc
            assert "score" in doc
            assert "tags" in doc

            # Verify types
            assert isinstance(doc["user_id"], int)
            assert isinstance(doc["username"], str)
            assert isinstance(doc["active"], bool)
            assert isinstance(doc["created_at"], datetime)
            assert isinstance(doc["score"], float)
            assert isinstance(doc["tags"], list)

    def test_unicode_generation(self):
        """Test unicode string generation."""
        gen = MockDataGenerator(seed=42)

        for _ in range(10):
            unicode_str = gen.string(min_len=5, max_len=20, charset="unicode")
            assert len(unicode_str) >= 5
            assert len(unicode_str) <= 20
            # Should contain some non-ASCII characters
            assert any(ord(c) > 127 for c in unicode_str)
