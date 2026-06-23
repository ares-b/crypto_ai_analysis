import pytest
from pydantic import ValidationError

from core.models import Record


class SampleRecord(Record):
    name: str
    value: int


def test_to_frame_produces_correct_columns():
    rows = [SampleRecord(name="a", value=1), SampleRecord(name="b", value=2)]
    frame = SampleRecord.to_frame(rows)
    assert set(frame.columns) == {"name", "value"}


def test_to_frame_row_count():
    rows = [SampleRecord(name="a", value=1), SampleRecord(name="b", value=2)]
    assert len(SampleRecord.to_frame(rows)) == 2


def test_to_frame_empty():
    frame = SampleRecord.to_frame([])
    assert len(frame) == 0


def test_to_frame_values():
    rows = [SampleRecord(name="x", value=42)]
    frame = SampleRecord.to_frame(rows)
    assert frame["name"][0] == "x"
    assert frame["value"][0] == 42


def test_record_is_frozen():
    row = SampleRecord(name="a", value=1)
    with pytest.raises((ValidationError, TypeError)):
        row.name = "b"  # type: ignore[misc]
