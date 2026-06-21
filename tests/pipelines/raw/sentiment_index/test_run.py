from datetime import date
from unittest.mock import MagicMock

from core.http import HttpError
from pipelines.raw.sentiment_index.config import SentimentSettings
from pipelines.raw.sentiment_index.run import run_sentiment_index
from tests.conftest import MemoryStore

SETTINGS = SentimentSettings(incremental_lookback_days=3)
RUN_DATE = date(2024, 6, 1)
_TS = 1717200000  # 2024-06-01 00:00:00 UTC as seconds


def _make_fear_greed_response(ts: int = _TS, value: int = 65) -> dict:
    return {
        "data": [{"timestamp": str(ts), "value": str(value), "value_classification": "Greed"}]
    }


def _make_dvol_response() -> dict:
    return {
        "result": {
            "data": [
                [_TS * 1000, 50.0, 55.0, 48.0, 52.0]  # [ts_ms, open, high, low, close]
            ]
        }
    }


def _make_put_call_response() -> dict:
    return {
        "result": [
            {"instrument_name": "BTC-1JUN24-60000-P", "open_interest": 1000.0},
            {"instrument_name": "BTC-1JUN24-65000-C", "open_interest": 2000.0},
        ]
    }


class TestRunSentimentIndex:
    def test_success_with_deribit(self, logger):
        fear_greed_client = MagicMock()
        fear_greed_client.get_json.return_value = _make_fear_greed_response()
        deribit_client = MagicMock()
        deribit_client.get_json.side_effect = [_make_dvol_response(), _make_put_call_response()]
        store = MemoryStore()

        metrics = run_sentiment_index(
            store=store, logger=logger, run_date=RUN_DATE, settings=SETTINGS,
            fear_greed_client=fear_greed_client, deribit_client=deribit_client,
        )

        assert metrics["rows_affected"] == 1
        assert SETTINGS.table_name in store._tables

    def test_success_without_deribit(self, logger):
        fear_greed_client = MagicMock()
        fear_greed_client.get_json.return_value = _make_fear_greed_response()
        store = MemoryStore()

        metrics = run_sentiment_index(
            store=store, logger=logger, run_date=RUN_DATE, settings=SETTINGS,
            fear_greed_client=fear_greed_client, deribit_client=None,
        )

        assert metrics["rows_affected"] == 1
        assert metrics["dvol_rows_affected"] == 0
        assert metrics["put_call_rows_affected"] == 0

    def test_fear_greed_http_error(self, logger):
        fear_greed_client = MagicMock()
        fear_greed_client.get_json.side_effect = HttpError(500, "test")
        store = MemoryStore()

        metrics = run_sentiment_index(
            store=store, logger=logger, run_date=RUN_DATE, settings=SETTINGS,
            fear_greed_client=fear_greed_client, deribit_client=None,
        )

        assert metrics["rows_affected"] == 0
        assert metrics["available_days"] == 0

    def test_dvol_http_error_graceful(self, logger):
        fear_greed_client = MagicMock()
        fear_greed_client.get_json.return_value = _make_fear_greed_response()
        deribit_client = MagicMock()
        # dvol error, put_call succeeds
        deribit_client.get_json.side_effect = [
            HttpError(503, "test"),
            _make_put_call_response(),
        ]
        store = MemoryStore()

        metrics = run_sentiment_index(
            store=store, logger=logger, run_date=RUN_DATE, settings=SETTINGS,
            fear_greed_client=fear_greed_client, deribit_client=deribit_client,
        )

        # Fear & greed still written
        assert metrics["rows_affected"] == 1
        assert metrics["dvol_rows_affected"] == 0

    def test_metrics_shape(self, logger):
        fear_greed_client = MagicMock()
        fear_greed_client.get_json.return_value = _make_fear_greed_response()
        store = MemoryStore()

        metrics = run_sentiment_index(
            store=store, logger=logger, run_date=RUN_DATE, settings=SETTINGS,
            fear_greed_client=fear_greed_client, deribit_client=None,
        )

        assert "rows_affected" in metrics
        assert "dvol_rows_affected" in metrics
        assert "put_call_rows_affected" in metrics
        assert "available_days" in metrics
