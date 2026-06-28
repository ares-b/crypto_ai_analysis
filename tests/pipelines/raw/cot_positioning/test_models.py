from datetime import UTC, date, datetime

from pipelines.raw.cot_positioning.models import CotPositioningRow

_NOW = datetime(2024, 6, 1, tzinfo=UTC)


def _make_item(**overrides) -> dict:
    base = {
        "report_date_as_yyyy_mm_dd": "2024-05-28",
        "open_interest_all": "120000",
        "dealer_positions_long_all": "6000",
        "dealer_positions_short_all": "2000",
        "asset_mgr_positions_long": "5000",
        "asset_mgr_positions_short": "3000",
        "lev_money_positions_long": "6000",
        "lev_money_positions_short": "12000",
    }
    return {**base, **overrides}


class TestCotPositioningRowFromApi:
    def test_valid_item(self):
        row = CotPositioningRow.from_api_response(_make_item(), source_updated_at=_NOW)
        assert row.report_date == date(2024, 5, 28)
        assert row.open_interest == 120000
        assert row.dealer_long == 6000
        assert row.asset_mgr_short == 3000
        assert row.lev_money_long == 6000
        assert row.lev_money_short == 12000
        assert row.source_updated_at == _NOW

    def test_none_values_become_none(self):
        row = CotPositioningRow.from_api_response(
            _make_item(open_interest_all=None, lev_money_positions_long="", dealer_positions_long_all=None),
            source_updated_at=_NOW,
        )
        assert row.open_interest is None
        assert row.lev_money_long is None
        assert row.dealer_long is None

    def test_float_strings_parsed(self):
        # CFTC API returns integers as float strings like "6000.0"
        row = CotPositioningRow.from_api_response(
            _make_item(lev_money_positions_long="6000.0"), source_updated_at=_NOW
        )
        assert row.lev_money_long == 6000

    def test_to_frame(self):
        row = CotPositioningRow.from_api_response(_make_item(), source_updated_at=_NOW)
        frame = CotPositioningRow.to_frame([row])
        assert "report_date" in frame.columns
        assert "lev_money_long" in frame.columns
        assert len(frame) == 1
