from datetime import date, datetime

import pyarrow as pa
import polars as pl
import pytest
from pyiceberg.transforms import IdentityTransform, MonthTransform, YearTransform

from core.iceberg.table import (
    build_iceberg_schema,
    build_partition_spec,
    build_sort_order,
)
from core.helpers.polars import cast_frame_to_arrow as coerce_to_schema


def test_coerce_exact_match():
    schema = pa.schema([pa.field("x", pa.int64()), pa.field("y", pa.float64())])
    frame = pl.DataFrame({"x": [1, 2], "y": [1.0, 2.0]})
    result = coerce_to_schema(frame, schema)
    assert result.schema.equals(schema, check_metadata=False)
    assert result.num_rows == 2


def test_coerce_type_cast():
    schema = pa.schema([pa.field("x", pa.int64())])
    frame = pl.DataFrame({"x": pl.Series([1, 2], dtype=pl.Int32)})
    result = coerce_to_schema(frame, schema)
    assert result.schema.field("x").type == pa.int64()


def test_coerce_missing_column_raises():
    schema = pa.schema([pa.field("x", pa.int64()), pa.field("missing", pa.utf8())])
    frame = pl.DataFrame({"x": [1]})
    with pytest.raises(ValueError, match="missing"):
        coerce_to_schema(frame, schema)


def test_coerce_extra_column_raises():
    schema = pa.schema([pa.field("x", pa.int64())])
    frame = pl.DataFrame({"x": [1], "extra": ["drift"]})
    with pytest.raises(ValueError, match="absent from Iceberg schema"):
        coerce_to_schema(frame, schema)


def test_coerce_preserves_values():
    schema = pa.schema([pa.field("name", pa.utf8()), pa.field("val", pa.float64())])
    frame = pl.DataFrame({"name": ["btc"], "val": [65000.0]})
    result = coerce_to_schema(frame, schema)
    assert result["name"][0].as_py() == "btc"
    assert result["val"][0].as_py() == 65000.0


class TestBuildPartitionSpec:
    def _schema(self):
        return build_iceberg_schema({"open_time": datetime, "symbol": str, "value": float})

    def test_empty_returns_none(self):
        assert build_partition_spec((), self._schema()) is None

    def test_months_transform(self):
        schema = self._schema()
        spec = build_partition_spec(("months(open_time)",), schema)
        assert spec is not None
        assert len(spec.fields) == 1
        pf = spec.fields[0]
        assert pf.source_id == schema.find_field("open_time").field_id
        assert pf.field_id == 1000
        assert isinstance(pf.transform, MonthTransform)
        assert pf.name == "open_time_months"

    def test_years_transform(self):
        schema = build_iceberg_schema({"metric_date": date})
        spec = build_partition_spec(("years(metric_date)",), schema)
        assert spec is not None
        pf = spec.fields[0]
        assert isinstance(pf.transform, YearTransform)
        assert pf.name == "metric_date_years"

    def test_identity_transform_explicit(self):
        schema = self._schema()
        spec = build_partition_spec(("identity(symbol)",), schema)
        assert spec is not None
        pf = spec.fields[0]
        assert isinstance(pf.transform, IdentityTransform)

    def test_multiple_partition_fields(self):
        schema = build_iceberg_schema({"ts": datetime, "asset": str})
        spec = build_partition_spec(("months(ts)", "identity(asset)"), schema)
        assert spec is not None
        assert len(spec.fields) == 2
        assert spec.fields[0].field_id == 1000
        assert spec.fields[1].field_id == 1001

    def test_unknown_transform_raises(self):
        with pytest.raises(ValueError, match="Unknown partition transform"):
            build_partition_spec(("bucket(open_time)",), self._schema())

    def test_unknown_column_raises(self):
        with pytest.raises(Exception):
            build_partition_spec(("months(nonexistent)",), self._schema())


class TestBuildSortOrder:
    def _schema(self):
        return build_iceberg_schema({"ts": datetime, "symbol": str, "value": float})

    def test_empty_returns_none(self):
        assert build_sort_order((), self._schema()) is None

    def test_single_field(self):
        schema = self._schema()
        order = build_sort_order(("ts",), schema)
        assert order is not None
        assert len(order.fields) == 1
        sf = order.fields[0]
        assert sf.source_id == schema.find_field("ts").field_id
        assert isinstance(sf.transform, IdentityTransform)

    def test_multiple_fields_preserves_order(self):
        schema = self._schema()
        order = build_sort_order(("symbol", "ts"), schema)
        assert order is not None
        assert len(order.fields) == 2
        assert order.fields[0].source_id == schema.find_field("symbol").field_id
        assert order.fields[1].source_id == schema.find_field("ts").field_id

    def test_unknown_field_raises(self):
        with pytest.raises(Exception):
            build_sort_order(("nonexistent",), self._schema())
