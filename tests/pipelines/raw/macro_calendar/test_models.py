from datetime import UTC, datetime

from pipelines.raw.macro_calendar.models import MacroCalendarRow, build_rows

_NOW = datetime(2024, 6, 1, tzinfo=UTC)


def _make_entry(**overrides) -> dict:
    base = {
        "release_id": 42,
        "release_name": "GDP",
        "date": "2024-06-01",
    }
    return {**base, **overrides}


class TestBuildRows:
    def test_valid_entry(self):
        rows = build_rows([_make_entry()], source_updated_at=_NOW)
        assert len(rows) == 1
        assert rows[0].title == "GDP"
        assert rows[0].event_id == "42:2024-06-01"
        assert rows[0].event_time_utc.tzinfo is not None

    def test_event_id_format(self):
        rows = build_rows([_make_entry(release_id=99, date="2024-05-15")], source_updated_at=_NOW)
        assert rows[0].event_id == "99:2024-05-15"

    def test_empty_entries(self):
        assert build_rows([], source_updated_at=_NOW) == []

    def test_to_frame(self):
        rows = build_rows([_make_entry()], source_updated_at=_NOW)
        frame = MacroCalendarRow.to_frame(rows)
        assert "event_id" in frame.columns
        assert "title" in frame.columns
        assert len(frame) == 1
