from __future__ import annotations

import pytest
from pydantic import ValidationError

from core.models import StoreRow


class SampleRow(StoreRow):
    name: str
    value: int


def test_to_frame_produces_correct_columns():
    rows = [SampleRow(name="a", value=1), SampleRow(name="b", value=2)]
    frame = SampleRow.to_frame(rows)
    assert set(frame.columns) == {"name", "value"}


def test_to_frame_row_count():
    rows = [SampleRow(name="a", value=1), SampleRow(name="b", value=2)]
    assert len(SampleRow.to_frame(rows)) == 2


def test_to_frame_empty():
    frame = SampleRow.to_frame([])
    assert len(frame) == 0


def test_to_frame_values():
    rows = [SampleRow(name="x", value=42)]
    frame = SampleRow.to_frame(rows)
    assert frame["name"][0] == "x"
    assert frame["value"][0] == 42


def test_store_row_is_frozen():
    row = SampleRow(name="a", value=1)
    with pytest.raises((ValidationError, TypeError)):
        row.name = "b"  # type: ignore[misc]
