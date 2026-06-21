from datetime import date
from unittest.mock import MagicMock

from core.http import HttpError
from pipelines.raw.onchain_metrics.config import OnchainSettings
from pipelines.raw.onchain_metrics.run import run_onchain_metrics
from tests.conftest import MemoryStore

SETTINGS = OnchainSettings()
RUN_DATE = date(2024, 6, 1)


def _make_coinmetrics_response(date_str: str = "2024-06-01T00:00:00Z") -> dict:
    return {
        "data": [
            {
                "time": date_str,
                "asset": "btc",
                "CapMrktCurUSD": "1200000000000",
                "CapMVRVCur": "2.5",
                "SplyCur": "19700000",
                "AdrActCnt": "850000",
                "HashRate": "600000000000000000",
                "SOPR": "1.02",
                "PriceRealizedUSD": "35000",
            }
        ]
    }


def _make_blockchain_response(value: float = 30_000_000.0) -> dict:
    return {"values": [{"x": 1717200000, "y": value}]}


class TestRunOnchainMetrics:
    def test_success_with_both_clients(self, logger):
        coinmetrics_client = MagicMock()
        coinmetrics_client.get_json.return_value = _make_coinmetrics_response()
        blockchain_client = MagicMock()
        blockchain_client.get_json.return_value = _make_blockchain_response()
        store = MemoryStore()

        metrics = run_onchain_metrics(
            logger=logger, settings=SETTINGS, store=store,
            coinmetrics_client=coinmetrics_client,
            blockchain_client=blockchain_client,
            run_date=RUN_DATE,
        )

        assert metrics["rows"] == 1
        assert metrics["rows_affected"] == 1
        assert SETTINGS.table_name in store._tables
        assert SETTINGS.blockchain_charts_table in store._tables

    def test_coinmetrics_error_continues(self, logger):
        coinmetrics_client = MagicMock()
        coinmetrics_client.get_json.side_effect = HttpError(500, "test")
        blockchain_client = MagicMock()
        blockchain_client.get_json.return_value = _make_blockchain_response()
        store = MemoryStore()

        metrics = run_onchain_metrics(
            logger=logger, settings=SETTINGS, store=store,
            coinmetrics_client=coinmetrics_client,
            blockchain_client=blockchain_client,
            run_date=RUN_DATE,
        )

        assert metrics["rows"] == 0
        assert metrics["rows_affected"] == 0
        # Blockchain charts still attempted
        assert metrics["blockchain_requests"] == 2

    def test_blockchain_error_graceful(self, logger):
        coinmetrics_client = MagicMock()
        coinmetrics_client.get_json.return_value = _make_coinmetrics_response()
        blockchain_client = MagicMock()
        blockchain_client.get_json.side_effect = HttpError(503, "test")
        store = MemoryStore()

        metrics = run_onchain_metrics(
            logger=logger, settings=SETTINGS, store=store,
            coinmetrics_client=coinmetrics_client,
            blockchain_client=blockchain_client,
            run_date=RUN_DATE,
        )

        # CoinMetrics rows still written
        assert metrics["rows"] == 1
        assert metrics["rows_affected"] == 1
        # Blockchain charts written with None values (error absorbed)
        assert SETTINGS.blockchain_charts_table in store._tables

    def test_metrics_shape(self, logger):
        coinmetrics_client = MagicMock()
        coinmetrics_client.get_json.return_value = {"data": []}
        blockchain_client = MagicMock()
        blockchain_client.get_json.return_value = _make_blockchain_response()
        store = MemoryStore()

        metrics = run_onchain_metrics(
            logger=logger, settings=SETTINGS, store=store,
            coinmetrics_client=coinmetrics_client,
            blockchain_client=blockchain_client,
            run_date=RUN_DATE,
        )

        assert "rows" in metrics
        assert "rows_affected" in metrics
        assert "blockchain_requests" in metrics
        assert "duration_seconds" in metrics
        assert "latest_source_date" in metrics
