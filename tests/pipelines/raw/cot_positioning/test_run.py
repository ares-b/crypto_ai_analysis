from datetime import date
from unittest.mock import MagicMock

from core.http import HttpError
from pipelines.raw.cot_positioning.config import CotPositioningSettings
from pipelines.raw.cot_positioning.run import fetch_cot_positioning, run_cot_positioning
from tests.conftest import MemoryStore

SETTINGS = CotPositioningSettings(page_size=3)
RUN_DATE = date(2024, 6, 1)


def _make_api_item(report_date: str = "2024-05-28") -> dict:
    return {
        "report_date_as_yyyy_mm_dd": report_date,
        "open_interest_all": "120000",
        "dealer_positions_long_all": "6000",
        "dealer_positions_short_all": "2000",
        "asset_mgr_positions_long": "5000",
        "asset_mgr_positions_short": "3000",
        "lev_money_positions_long": "6000",
        "lev_money_positions_short": "12000",
    }


class TestFetchRows:
    def test_single_page_returns_rows(self, logger):
        client = MagicMock()
        client.get_json.return_value = [_make_api_item()]

        rows = fetch_cot_positioning(client, settings=SETTINGS, since=None)

        assert len(rows) == 1
        assert rows[0].lev_money_short == 12000
        assert rows[0].open_interest == 120000

    def test_pagination_triggers_next_request(self, logger):
        client = MagicMock()
        full_batch = [_make_api_item(f"2024-05-{i:02d}") for i in range(1, 4)]
        client.get_json.side_effect = [full_batch, [_make_api_item("2024-04-30")]]

        rows = fetch_cot_positioning(client, settings=SETTINGS, since=None)

        assert client.get_json.call_count == 2
        assert len(rows) == 4

    def test_since_filter_passed_in_where(self, logger):
        client = MagicMock()
        client.get_json.return_value = []

        fetch_cot_positioning(client, settings=SETTINGS, since=date(2024, 5, 1))

        call_params = client.get_json.call_args[1]["params"]["$where"]
        assert "2024-05-01" in call_params

    def test_empty_response_returns_empty(self, logger):
        client = MagicMock()
        client.get_json.return_value = []

        rows = fetch_cot_positioning(client, settings=SETTINGS, since=None)

        assert rows == []


class TestRunCotPositioning:
    def test_rows_written_to_store(self, logger):
        client = MagicMock()
        client.get_json.return_value = [_make_api_item()]
        store = MemoryStore()

        metrics = run_cot_positioning(
            store=store, logger=logger, run_date=RUN_DATE, client=client, settings=SETTINGS
        )

        assert metrics["rows_affected"] == 1
        assert SETTINGS.table_name in store._tables

    def test_http_error_returns_zero(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(500, "test")
        store = MemoryStore()

        metrics = run_cot_positioning(
            store=store, logger=logger, run_date=RUN_DATE, client=client, settings=SETTINGS
        )

        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_empty_rows_skips_upsert(self, logger):
        client = MagicMock()
        client.get_json.return_value = []
        store = MemoryStore()

        metrics = run_cot_positioning(
            store=store, logger=logger, run_date=RUN_DATE, client=client, settings=SETTINGS
        )

        assert metrics["rows_affected"] == 0
        assert store._tables == {}
