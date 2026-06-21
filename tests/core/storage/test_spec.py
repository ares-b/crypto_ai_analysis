from __future__ import annotations

import pyarrow as pa
import polars as pl
import pytest

from core.storage._spec import coerce_to_schema


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


def test_coerce_preserves_values():
    schema = pa.schema([pa.field("name", pa.utf8()), pa.field("val", pa.float64())])
    frame = pl.DataFrame({"name": ["btc"], "val": [65000.0]})
    result = coerce_to_schema(frame, schema)
    assert result["name"][0].as_py() == "btc"
    assert result["val"][0].as_py() == 65000.0
