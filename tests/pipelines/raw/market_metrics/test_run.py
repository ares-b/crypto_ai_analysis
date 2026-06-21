from datetime import date
from unittest.mock import MagicMock

from core.http import HttpError
from pipelines.raw.market_metrics.config import MarketMetricSettings
from pipelines.raw.market_metrics.run import (
    fetch_market_metric_row,
    fetch_stablecoin_supply,
    run_market_metrics,
    run_stablecoin_supply,
)
from tests.conftest import MemoryStore

SETTINGS = MarketMetricSettings()
RUN_DATE = date(2024, 6, 1)
_TS_MS = 1717200000000  # 2024-06-01 00:00:00 UTC


def _make_history_response(ts_ms: int = _TS_MS, pct: float = 53.5) -> dict:
    btc_cap = pct / 100 * 1_000_000_000_000
    total_cap = 1_000_000_000_000.0
    return {
        "market_caps": [[ts_ms, btc_cap]],
    }


def _make_global_market_cap_response(ts_ms: int = _TS_MS, total: float = 1e12) -> dict:
    return {"market_cap_chart": {"market_cap": [[ts_ms, total]]}}


def _make_snapshot_response(pct: float = 55.0) -> dict:
    return {"data": {"market_cap_percentage": {"btc": pct}}}


def _make_coin_response(market_cap: float = 80e9) -> dict:
    return {"market_data": {"market_cap": {"usd": market_cap}}}


class TestFetchMarketMetricRow:
    def test_history_hit_uses_historical_point(self, logger):
        client = MagicMock()
        client.get_json.side_effect = [
            _make_history_response(),
            _make_global_market_cap_response(),
        ]

        row, request_count = fetch_market_metric_row(
            logger=logger, client=client, run_date=RUN_DATE
        )

        assert row is not None
        assert row.date == RUN_DATE
        assert request_count == 2

    def test_snapshot_fallback_when_date_missing(self, logger):
        # History has a different date → falls back to snapshot
        client = MagicMock()
        other_ts = 1717113600000  # 2024-05-31
        client.get_json.side_effect = [
            _make_history_response(ts_ms=other_ts),
            _make_global_market_cap_response(ts_ms=other_ts),
            _make_snapshot_response(pct=55.0),
        ]

        row, request_count = fetch_market_metric_row(
            logger=logger, client=client, run_date=RUN_DATE
        )

        assert row is not None
        assert row.btc_dominance_pct == 55.0
        assert request_count == 3

    def test_http_error_returns_none(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(500, "test")

        row, request_count = fetch_market_metric_row(
            logger=logger, client=client, run_date=RUN_DATE
        )

        assert row is None
        assert request_count == 0


class TestFetchStablecoinSupply:
    def test_both_coins(self, logger):
        client = MagicMock()
        client.get_json.side_effect = [_make_coin_response(80e9), _make_coin_response(25e9)]

        row = fetch_stablecoin_supply(logger=logger, client=client, run_date=RUN_DATE)

        assert row is not None
        assert row.usdt_market_cap_usd == 80e9
        assert row.usdc_market_cap_usd == 25e9
        assert row.total_stablecoin_market_cap_usd == 105e9

    def test_one_coin_http_error(self, logger):
        client = MagicMock()
        client.get_json.side_effect = [
            HttpError(429, "test"),
            _make_coin_response(25e9),
        ]

        row = fetch_stablecoin_supply(logger=logger, client=client, run_date=RUN_DATE)

        assert row is not None
        assert row.usdt_market_cap_usd is None
        assert row.usdc_market_cap_usd == 25e9

    def test_both_coins_error_returns_none(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(500, "test")

        row = fetch_stablecoin_supply(logger=logger, client=client, run_date=RUN_DATE)

        assert row is None


class TestRunMarketMetrics:
    def test_row_written_to_store(self, logger):
        client = MagicMock()
        client.get_json.side_effect = [
            _make_history_response(),
            _make_global_market_cap_response(),
        ]
        store = MemoryStore()

        metrics = run_market_metrics(
            logger=logger, settings=SETTINGS, store=store, client=client, run_date=RUN_DATE
        )

        assert metrics["rows_affected"] == 1
        assert SETTINGS.table_name in store._tables

    def test_no_row_skips_upsert(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(500, "test")
        store = MemoryStore()

        metrics = run_market_metrics(
            logger=logger, settings=SETTINGS, store=store, client=client, run_date=RUN_DATE
        )

        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_metrics_shape(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(500, "test")
        store = MemoryStore()

        metrics = run_market_metrics(
            logger=logger, settings=SETTINGS, store=store, client=client, run_date=RUN_DATE
        )

        assert "requests" in metrics
        assert "rows_affected" in metrics
        assert "duration_seconds" in metrics
        assert "btc_dominance_pct" in metrics


class TestRunStablecoinSupply:
    def test_row_written_to_store(self, logger):
        client = MagicMock()
        client.get_json.side_effect = [_make_coin_response(80e9), _make_coin_response(25e9)]
        store = MemoryStore()

        metrics = run_stablecoin_supply(
            logger=logger, settings=SETTINGS, store=store, client=client, run_date=RUN_DATE
        )

        assert metrics["rows_affected"] == 1
        assert SETTINGS.stablecoin_table in store._tables

    def test_no_row_skips_upsert(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(500, "test")
        store = MemoryStore()

        metrics = run_stablecoin_supply(
            logger=logger, settings=SETTINGS, store=store, client=client, run_date=RUN_DATE
        )

        assert metrics["rows_affected"] == 0
        assert store._tables == {}
