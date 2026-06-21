from datetime import UTC, date, datetime

from pipelines.raw.cot_positioning.models import CotPositioningRow

_NOW = datetime(2024, 6, 1, tzinfo=UTC)


def _make_item(**overrides) -> dict:
    base = {
        "report_date_as_yyyy_mm_dd": "2024-05-28",
        "noncomm_positions_long_all": "50000",
        "noncomm_positions_short_all": "20000",
        "open_interest_all": "120000",
    }
    return {**base, **overrides}


class TestCotPositioningRowFromApi:
    def test_valid_item(self):
        row = CotPositioningRow.from_api_response(_make_item(), source_updated_at=_NOW)
        assert row.report_date == date(2024, 5, 28)
        assert row.noncommercial_long == 50000
        assert row.noncommercial_short == 20000
        assert row.open_interest == 120000
        assert row.source_updated_at == _NOW

    def test_none_values_become_none(self):
        row = CotPositioningRow.from_api_response(
            _make_item(
                noncomm_positions_long_all=None,
                noncomm_positions_short_all="",
                open_interest_all=None,
            ),
            source_updated_at=_NOW,
        )
        assert row.noncommercial_long is None
        assert row.noncommercial_short is None
        assert row.open_interest is None

    def test_float_strings_parsed(self):
        # CFTC API returns integers as float strings like "50000.0"
        row = CotPositioningRow.from_api_response(
            _make_item(noncomm_positions_long_all="50000.0"),
            source_updated_at=_NOW,
        )
        assert row.noncommercial_long == 50000

    def test_to_frame(self):
        row = CotPositioningRow.from_api_response(_make_item(), source_updated_at=_NOW)
        frame = CotPositioningRow.to_frame([row])
        assert "report_date" in frame.columns
        assert "noncommercial_long" in frame.columns
        assert len(frame) == 1
