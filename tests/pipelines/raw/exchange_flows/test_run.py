from datetime import date
from unittest.mock import MagicMock

from core.http import HttpError
from pipelines.raw.exchange_flows.config import ExchangeFlowSettings
from pipelines.raw.exchange_flows.run import fetch_exchange_flows, run_exchange_flows
from tests.conftest import MemoryStore

SETTINGS = ExchangeFlowSettings()
SINCE = date(2024, 6, 1)
UNTIL = date(2024, 6, 1)


def _make_api_data(date_str: str = "2024-06-01T00:00:00") -> dict:
    return {
        "date": date_str,
        "reserve": "125000.5",
        "reserve_usd": "8125032500.0",
        "inflow": "1200.0",
        "outflow": "950.0",
        "netflow": "250.0",
    }


class TestFetchExchangeFlows:
    def test_valid_response(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"result": {"data": [_make_api_data()]}}

        rows = fetch_exchange_flows(client=client, settings=SETTINGS, since=SINCE, until=UNTIL)

        assert len(rows) == 1
        assert rows[0].asset == SETTINGS.asset
        assert rows[0].exchange == SETTINGS.exchange

    def test_empty_result(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"result": {"data": []}}

        rows = fetch_exchange_flows(client=client, settings=SETTINGS, since=SINCE, until=UNTIL)

        assert rows == []

    def test_missing_result_key(self, logger):
        client = MagicMock()
        client.get_json.return_value = {}

        rows = fetch_exchange_flows(client=client, settings=SETTINGS, since=SINCE, until=UNTIL)

        assert rows == []


class TestRunExchangeFlows:
    def test_rows_written_to_store(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"result": {"data": [_make_api_data()]}}
        store = MemoryStore()

        metrics = run_exchange_flows(
            logger=logger, settings=SETTINGS, store=store,
            client=client, since=SINCE, until=UNTIL,
        )

        assert metrics["rows"] == 1
        assert metrics["rows_affected"] == 1
        assert SETTINGS.table_name in store._tables

    def test_http_error_returns_zero(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(401, "test")
        store = MemoryStore()

        metrics = run_exchange_flows(
            logger=logger, settings=SETTINGS, store=store,
            client=client, since=SINCE, until=UNTIL,
        )

        assert metrics["rows"] == 0
        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_empty_rows_skips_upsert(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"result": {"data": []}}
        store = MemoryStore()

        metrics = run_exchange_flows(
            logger=logger, settings=SETTINGS, store=store,
            client=client, since=SINCE, until=UNTIL,
        )

        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_metrics_shape(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"result": {"data": []}}
        store = MemoryStore()

        metrics = run_exchange_flows(
            logger=logger, settings=SETTINGS, store=store,
            client=client, since=SINCE, until=UNTIL,
        )

        assert "rows" in metrics
        assert "rows_affected" in metrics
        assert "duration_seconds" in metrics
