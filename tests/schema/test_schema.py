"""
Unit tests for XLR8 Schema class.

Tests schema creation, validation, and operations including:
- Schema initialization and validation
- Arrow schema conversion
- Field access methods
- Schema equality
"""

import pyarrow as pa
import pytest

from xlr8.schema import Schema
from xlr8.schema import types as Types


class TestSchemaCreation:
    """Tests for Schema initialization."""

    def test_schema_basic(self):
        """Test basic schema creation."""
        schema = Schema(
            time_field="timestamp",
            fields={
                "timestamp": Types.Timestamp("ms", tz="UTC"),
                "value": Types.Float(),
                "sensor_id": Types.String(),
            },
        )

        assert schema.time_field == "timestamp"
        assert len(schema.fields) == 3
        assert isinstance(schema.fields["timestamp"], Types.Timestamp)
        assert isinstance(schema.fields["value"], Types.Float)
        assert isinstance(schema.fields["sensor_id"], Types.String)

    def test_schema_with_avg_doc_size(self):
        """Test schema with custom avg_doc_size_bytes."""
        schema = Schema(
            time_field="ts", fields={"ts": Types.Timestamp()}, avg_doc_size_bytes=1000
        )

        assert schema.avg_doc_size_bytes == 1000

    def test_schema_default_avg_doc_size(self):
        """Test schema uses default avg_doc_size_bytes."""
        schema = Schema(time_field="ts", fields={"ts": Types.Timestamp()})

        assert schema.avg_doc_size_bytes == 500  # Default

    def test_schema_with_flatten_nested(self):
        """Test schema with flatten_nested configuration."""
        schema = Schema(
            time_field="ts", fields={"ts": Types.Timestamp()}, flatten_nested=False
        )

        assert schema.flatten_nested is False


class TestSchemaValidation:
    """Tests for schema validation logic."""

    def test_schema_missing_time_field(self):
        """Test that schema requires time_field to be in fields."""
        with pytest.raises(ValueError, match="must be present in fields"):
            Schema(time_field="timestamp", fields={"value": Types.Float()})

    def test_schema_time_field_not_timestamp(self):
        """Test that time_field must be Timestamp type."""
        with pytest.raises(ValueError, match="must be Timestamp or DateTime type"):
            Schema(
                time_field="timestamp",
                fields={
                    "timestamp": Types.String(),  # Wrong type!
                    "value": Types.Float(),
                },
            )

    def test_schema_time_field_correct_type(self):
        """Test that Timestamp type is accepted for time_field."""
        schema = Schema(
            time_field="recordedAt",
            fields={
                "recordedAt": Types.Timestamp("ms", tz="UTC"),
                "value": Types.Float(),
            },
        )

        assert schema.time_field == "recordedAt"


class TestSchemaArrowConversion:
    """Tests for Arrow schema generation."""

    def test_to_arrow_schema_simple(self):
        """Test Arrow schema conversion with simple types."""
        schema = Schema(
            time_field="ts",
            fields={
                "ts": Types.Timestamp("ms", tz="UTC"),
                "name": Types.String(),
                "count": Types.Int(),
                "active": Types.Bool(),
            },
        )

        arrow_schema = schema.to_arrow_schema()
        assert isinstance(arrow_schema, pa.Schema)
        assert len(arrow_schema) == 4
        assert arrow_schema.field("ts").type == pa.timestamp("ms", tz="UTC")
        assert arrow_schema.field("name").type == pa.string()
        assert arrow_schema.field("count").type == pa.int64()
        assert arrow_schema.field("active").type == pa.bool_()

    def test_to_arrow_schema_with_any(self):
        """Test Arrow schema with Any type (becomes struct)."""
        schema = Schema(
            time_field="ts", fields={"ts": Types.Timestamp(), "data": Types.Any()}
        )

        arrow_schema = schema.to_arrow_schema()
        assert pa.types.is_struct(arrow_schema.field("data").type)

    def test_to_arrow_schema_with_nested(self):
        """Test Arrow schema with nested Struct."""
        schema = Schema(
            time_field="ts",
            fields={
                "ts": Types.Timestamp(),
                "metadata": Types.Struct(
                    {"user_id": Types.String(), "session_id": Types.Int()}
                ),
            },
        )

        arrow_schema = schema.to_arrow_schema()
        metadata_field = arrow_schema.field("metadata")
        assert pa.types.is_struct(metadata_field.type)
        assert metadata_field.type.field("user_id").type == pa.string()
        assert metadata_field.type.field("session_id").type == pa.int64()

    def test_to_arrow_schema_with_list(self):
        """Test Arrow schema with List type."""
        schema = Schema(
            time_field="ts",
            fields={"ts": Types.Timestamp(), "tags": Types.List(Types.String())},
        )

        arrow_schema = schema.to_arrow_schema()
        tags_field = arrow_schema.field("tags")
        assert pa.types.is_list(tags_field.type)
        assert tags_field.type.value_type == pa.string()


class TestSchemaFieldAccess:
    """Tests for schema field access methods."""

    def test_get_field_names(self):
        """Test get_field_names() returns all field names."""
        schema = Schema(
            time_field="ts",
            fields={
                "ts": Types.Timestamp(),
                "value": Types.Float(),
                "id": Types.String(),
            },
        )

        field_names = schema.get_field_names()
        assert set(field_names) == {"ts", "value", "id"}

    def test_has_field(self):
        """Test has_field() checks field existence."""
        schema = Schema(
            time_field="ts", fields={"ts": Types.Timestamp(), "value": Types.Float()}
        )

        assert schema.has_field("ts") is True
        assert schema.has_field("value") is True
        assert schema.has_field("missing") is False

    def test_get_field_type(self):
        """Test get_field_type() returns correct type."""
        schema = Schema(
            time_field="ts",
            fields={
                "ts": Types.Timestamp("ms", tz="UTC"),
                "value": Types.Float(),
                "name": Types.String(),
            },
        )

        assert isinstance(schema.get_field_type("ts"), Types.Timestamp)
        assert isinstance(schema.get_field_type("value"), Types.Float)
        assert isinstance(schema.get_field_type("name"), Types.String)

    def test_get_field_type_missing(self):
        """Test get_field_type() raises KeyError for missing field."""
        schema = Schema(time_field="ts", fields={"ts": Types.Timestamp()})

        with pytest.raises(KeyError):
            schema.get_field_type("missing")

    def test_get_any_fields(self):
        """Test get_any_fields() returns only Any type fields."""
        schema = Schema(
            time_field="ts",
            fields={
                "ts": Types.Timestamp(),
                "value": Types.Float(),
                "data": Types.Any(),
                "metadata": Types.Any(),
                "name": Types.String(),
            },
        )

        any_fields = schema.get_any_fields()
        assert set(any_fields) == {"data", "metadata"}

    def test_get_any_fields_empty(self):
        """Test get_any_fields() returns empty list when no Any fields."""
        schema = Schema(
            time_field="ts", fields={"ts": Types.Timestamp(), "value": Types.Float()}
        )

        any_fields = schema.get_any_fields()
        assert any_fields == []


class TestSchemaSpec:
    """Tests for schema serialization (to_spec/from_spec)."""

    def test_to_spec_basic(self):
        """Test to_spec() generates JSON-serializable dict."""
        schema = Schema(
            time_field="ts",
            fields={"ts": Types.Timestamp("ms", tz="UTC"), "value": Types.Float()},
        )

        spec = schema.to_spec()
        assert isinstance(spec, dict)
        assert spec["version"] == 1
        assert spec["time_field"] == "ts"
        assert spec["avg_doc_size_bytes"] == 500
        assert "fields" in spec
        assert len(spec["fields"]) == 2

    def test_to_spec_field_kinds(self):
        """Test to_spec() generates correct field kinds."""
        schema = Schema(
            time_field="ts",
            fields={
                "ts": Types.Timestamp("ms", tz="UTC"),
                "name": Types.String(),
                "count": Types.Int(),
                "active": Types.Bool(),
                "data": Types.Any(),
                "id": Types.ObjectId(),
            },
        )

        spec = schema.to_spec()
        fields_by_name = {f["name"]: f for f in spec["fields"]}

        assert fields_by_name["ts"]["kind"] == "timestamp"
        assert fields_by_name["name"]["kind"] == "string"
        assert fields_by_name["count"]["kind"] == "int64"
        assert fields_by_name["active"]["kind"] == "bool"
        assert fields_by_name["data"]["kind"] == "any"
        assert fields_by_name["id"]["kind"] == "objectid"

    def test_to_spec_timestamp_details(self):
        """Test to_spec() includes Timestamp unit and timezone."""
        schema = Schema(
            time_field="ts", fields={"ts": Types.Timestamp("ms", tz="America/New_York")}
        )

        spec = schema.to_spec()
        ts_field = spec["fields"][0]
        assert ts_field["unit"] == "ms"
        assert ts_field["tz"] == "America/New_York"


class TestSchemaRepr:
    """Tests for schema string representation."""

    def test_schema_repr(self):
        """Test schema __repr__ includes time_field and fields."""
        schema = Schema(
            time_field="ts", fields={"ts": Types.Timestamp(), "value": Types.Float()}
        )

        repr_str = repr(schema)
        assert "Schema" in repr_str
        assert "time_field='ts'" in repr_str
        assert "ts:" in repr_str
        assert "value:" in repr_str


class TestSchemaComplexTypes:
    """Tests for schemas with complex nested types."""

    def test_schema_deeply_nested(self):
        """Test schema with deeply nested structures."""
        schema = Schema(
            time_field="ts",
            fields={
                "ts": Types.Timestamp(),
                "data": Types.Struct(
                    {
                        "user": Types.Struct(
                            {
                                "profile": Types.Struct(
                                    {"name": Types.String(), "age": Types.Int()}
                                )
                            }
                        ),
                        "tags": Types.List(Types.String()),
                    }
                ),
            },
        )

        arrow_schema = schema.to_arrow_schema()
        assert pa.types.is_struct(arrow_schema.field("data").type)

    def test_schema_with_list_of_structs(self):
        """Test schema with List of Struct (common pattern)."""
        schema = Schema(
            time_field="ts",
            fields={
                "ts": Types.Timestamp(),
                "events": Types.List(
                    Types.Struct(
                        {
                            "event_type": Types.String(),
                            "timestamp": Types.Timestamp(),
                            "data": Types.Any(),
                        }
                    )
                ),
            },
        )

        arrow_schema = schema.to_arrow_schema()
        events_field = arrow_schema.field("events")
        assert pa.types.is_list(events_field.type)
        assert pa.types.is_struct(events_field.type.value_type)

    def test_schema_all_types(self):
        """Test schema with all available types."""
        schema = Schema(
            time_field="ts",
            fields={
                "ts": Types.Timestamp("ms", tz="UTC"),
                "string_field": Types.String(),
                "int_field": Types.Int(),
                "float_field": Types.Float(),
                "bool_field": Types.Bool(),
                "objectid_field": Types.ObjectId(),
                "any_field": Types.Any(),
                "struct_field": Types.Struct({"x": Types.Int()}),
                "list_field": Types.List(Types.String()),
            },
        )

        arrow_schema = schema.to_arrow_schema()
        assert len(arrow_schema) == 9

        # Verify all conversions work
        for field_name in schema.get_field_names():
            field_type = arrow_schema.field(field_name).type
            assert isinstance(field_type, pa.DataType)
