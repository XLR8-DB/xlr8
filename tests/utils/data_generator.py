"""
Mock data generator for XLR8 testing.

Generates random MongoDB-like documents with various data types
to enable comprehensive property-based testing.
"""

import random
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId


class MockDataGenerator:
    """
    Generates random mock data for all MongoDB/XLR8 data types.

    Supports:
    - Null values
    - Numbers (int, float)
    - Strings (ASCII, Unicode)
    - Binary data
    - Object Ids
    - Booleans
    - Datetimes
    - Arrays
    - Nested documents
    - Mixed types (Type.Any)
    """

    def __init__(self, seed: Optional[int] = None):
        """Initialize generator with optional seed for reproducibility."""
        self._random = random.Random(seed)

    # ==================== Primitive Types ====================

    def null(self) -> None:
        """Generate null value."""
        return None

    def boolean(self) -> bool:
        """Generate random boolean."""
        return self._random.choice([True, False])

    def integer(self, min_val: int = -1000000, max_val: int = 1000000) -> int:
        """Generate random integer."""
        return self._random.randint(min_val, max_val)

    def float_value(
        self, min_val: float = -1000000.0, max_val: float = 1000000.0
    ) -> float:
        """Generate random float."""
        return self._random.uniform(min_val, max_val)

    def string(
        self, min_len: int = 0, max_len: int = 50, charset: str = "ascii"
    ) -> str:
        """
        Generate random string.

        Args:
            min_len: Minimum string length
            max_len: Maximum string length
            charset: 'ascii', 'unicode', 'alphanumeric'
        """
        length = self._random.randint(min_len, max_len)

        if charset == "ascii":
            chars = string.ascii_letters + string.digits + " _-"
            return "".join(self._random.choice(chars) for _ in range(length))
        elif charset == "alphanumeric":
            return "".join(
                self._random.choice(string.ascii_letters + string.digits)
                for _ in range(length)
            )
        elif charset == "unicode":
            # Mix of ASCII and various unicode ranges
            unicode_ranges = [
                (0x0041, 0x005A),  # Latin uppercase
                (0x0061, 0x007A),  # Latin lowercase
                (0x00C0, 0x00FF),  # Latin extended
                (0x0400, 0x04FF),  # Cyrillic
                (0x4E00, 0x9FFF),  # CJK (Chinese)
                (0x0600, 0x06FF),  # Arabic
            ]
            result = []
            for _ in range(length):
                range_choice = self._random.choice(unicode_ranges)
                codepoint = self._random.randint(
                    range_choice[0], min(range_choice[1], range_choice[0] + 100)
                )
                result.append(chr(codepoint))
            return "".join(result)
        else:
            raise ValueError(f"Unknown charset: {charset}")

    def objectid(self) -> ObjectId:
        """Generate random ObjectId."""
        return ObjectId()

    def datetime_value(self, start_year: int = 2020, end_year: int = 2025) -> datetime:
        """Generate random datetime (always UTC)."""
        start = datetime(start_year, 1, 1, tzinfo=timezone.utc)
        end = datetime(end_year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        delta = end - start
        random_seconds = self._random.randint(0, int(delta.total_seconds()))

        return start + timedelta(seconds=random_seconds)

    def binary_data(self, min_len: int = 0, max_len: int = 100) -> bytes:
        """Generate random binary data."""
        length = self._random.randint(min_len, max_len)
        return bytes(self._random.randint(0, 255) for _ in range(length))

    # ==================== Complex Types ====================

    def array(
        self, element_type: str = "int", min_len: int = 0, max_len: int = 10
    ) -> List[Any]:
        """
        Generate random array of specified type.

        Args:
            element_type: Type of elements ('int', 'float', 'string', 'mixed')
            min_len: Minimum array length
            max_len: Maximum array length
        """
        length = self._random.randint(min_len, max_len)

        if element_type == "int":
            return [self.integer() for _ in range(length)]
        elif element_type == "float":
            return [self.float_value() for _ in range(length)]
        elif element_type == "string":
            return [self.string(max_len=20) for _ in range(length)]
        elif element_type == "mixed":
            return [self.any_value() for _ in range(length)]
        else:
            raise ValueError(f"Unknown element_type: {element_type}")

    def document(
        self, num_fields: int = 5, field_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Generate random nested document.

        Args:
            num_fields: Number of fields in document
            field_types: List of types for each field (or None for random)
        """
        if field_types is None:
            field_types = ["int", "string", "float", "bool", "null"]

        doc = {}
        for i in range(num_fields):
            field_name = f"field_{i}"
            field_type = self._random.choice(field_types)

            if field_type == "int":
                doc[field_name] = self.integer()
            elif field_type == "float":
                doc[field_name] = self.float_value()
            elif field_type == "string":
                doc[field_name] = self.string(max_len=30)
            elif field_type == "bool":
                doc[field_name] = self.boolean()
            elif field_type == "null":
                doc[field_name] = None
            elif field_type == "objectid":
                doc[field_name] = self.objectid()
            elif field_type == "datetime":
                doc[field_name] = self.datetime_value()

        return doc

    def any_value(self, include_complex: bool = False) -> Any:
        """
        Generate random value of ANY type (Type.Any simulation).

        Args:
            include_complex: Include arrays/documents or just primitives
        """
        if include_complex:
            types = [
                self.null,
                self.boolean,
                self.integer,
                self.float_value,
                self.string,
                self.objectid,
                self.datetime_value,
                self.binary_data,
                lambda: self.array(element_type="int", max_len=5),
                lambda: self.document(num_fields=3),
            ]
        else:
            # Only primitive types (commonly used for Type.Any)
            types = [
                self.null,
                self.boolean,
                self.integer,
                self.float_value,
                self.string,
                self.objectid,
                self.datetime_value,
            ]

        return self._random.choice(types)()

    # ==================== Batch Generation ====================

    def generate_batch(
        self, schema: Dict[str, str], num_docs: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Generate batch of documents according to schema.

        Args:
            schema: Dict mapping field names to types
                    e.g., {"id": "int", "name": "string", "active": "bool"}
            num_docs: Number of documents to generate

        Returns:
            List of documents
        """
        docs = []

        for _ in range(num_docs):
            doc = {}
            for field_name, field_type in schema.items():
                if field_type == "int":
                    doc[field_name] = self.integer()
                elif field_type == "float":
                    doc[field_name] = self.float_value()
                elif field_type == "string":
                    doc[field_name] = self.string()
                elif field_type == "bool":
                    doc[field_name] = self.boolean()
                elif field_type == "null":
                    doc[field_name] = None
                elif field_type == "objectid":
                    doc[field_name] = self.objectid()
                elif field_type == "datetime":
                    doc[field_name] = self.datetime_value()
                elif field_type == "binary":
                    doc[field_name] = self.binary_data()
                elif field_type == "any":
                    doc[field_name] = self.any_value()
                elif field_type.startswith("array_"):
                    elem_type = field_type.split("_")[1]
                    doc[field_name] = self.array(element_type=elem_type)
                else:
                    raise ValueError(f"Unknown field type: {field_type}")

            docs.append(doc)

        return docs

    # ==================== Special Cases ====================

    def generate_sorting_edge_cases(self) -> List[Tuple[str, Any]]:
        """
        Generate edge cases specifically for sorting tests.

        Returns:
            List of (description, value) tuples
        """
        return [
            # Nulls
            ("null", None),
            # Numbers - edge cases
            ("zero", 0),
            ("negative_int", -42),
            ("positive_int", 42),
            ("large_int", 10**15),
            ("negative_float", -3.14),
            ("positive_float", 3.14),
            ("float_zero", 0.0),
            ("infinity", float("inf")),
            ("negative_infinity", float("-inf")),
            # Strings - edge cases
            ("empty_string", ""),
            ("single_char", "a"),
            ("whitespace", "   "),
            ("unicode_chinese", "北京"),
            ("unicode_arabic", "مرحبا"),
            ("unicode_emoji", "🎉🚀"),
            # Binary - edge cases
            ("empty_binary", b""),
            ("binary_zeros", b"\x00\x00\x00"),
            ("binary_ones", b"\xff\xff\xff"),
            ("binary_mixed", b"\x00\x7f\xff"),
            # Booleans
            ("bool_true", True),
            ("bool_false", False),
            # Dates - edge cases
            ("epoch", datetime(1970, 1, 1, tzinfo=timezone.utc)),
            ("y2k", datetime(2000, 1, 1, tzinfo=timezone.utc)),
            ("recent", datetime(2024, 1, 1, tzinfo=timezone.utc)),
            # ObjectIds
            ("objectid_min", ObjectId("000000000000000000000000")),
            ("objectid_max", ObjectId("ffffffffffffffffffffffff")),
            ("objectid_random", ObjectId()),
        ]
