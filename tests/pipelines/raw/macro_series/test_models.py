from datetime import UTC, datetime

from pipelines.raw.macro_series.models import MacroSeriesRow, build_rows

_NOW = datetime(2024, 6, 1, tzinfo=UTC)


def _make_observation(date_str: str = "2024-06-01", value: str = "4.5") -> dict:
    return {"date": date_str, "value": value}


class TestBuildRows:
    def test_valid_observation(self):
        rows = build_rows("DGS10", [_make_observation()], source_updated_at=_NOW)
        assert len(rows) == 1
        assert rows[0].series_id == "DGS10"
        assert rows[0].value == 4.5

    def test_dot_value_becomes_none(self):
        # FRED uses "." for missing values
        rows = build_rows("UNRATE", [_make_observation(value=".")], source_updated_at=_NOW)
        assert rows[0].value is None

    def test_empty_string_becomes_none(self):
        rows = build_rows("UNRATE", [_make_observation(value="")], source_updated_at=_NOW)
        assert rows[0].value is None

    def test_none_value_becomes_none(self):
        rows = build_rows("UNRATE", [_make_observation(value=None)], source_updated_at=_NOW)
        assert rows[0].value is None

    def test_multiple_observations(self):
        obs = [_make_observation(f"2024-0{i}-01", str(i)) for i in range(1, 4)]
        rows = build_rows("DGS10", obs, source_updated_at=_NOW)
        assert len(rows) == 3
        assert rows[1].value == 2.0

    def test_to_frame(self):
        rows = build_rows("DGS10", [_make_observation()], source_updated_at=_NOW)
        frame = MacroSeriesRow.to_frame(rows)
        assert "series_id" in frame.columns
        assert "value" in frame.columns
        assert len(frame) == 1
