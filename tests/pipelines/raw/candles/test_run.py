from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from binance.exceptions import BinanceAPIException

from pipelines.raw.candles.run import _MAX_KLINES_LIMIT, fetch_candles, run_binance_candles
from tests.conftest import MemoryStore, make_kline

PAST_MS = 1717200000000   # 2024-06-01 00:00:00 UTC — well in the past
CLOSE_MS = 1717286399999  # 2024-06-01 23:59:59 UTC


def _make_rate_limit_error(retry_after: int = 1) -> BinanceAPIException:
    response = MagicMock()
    response.headers = {"Retry-After": str(retry_after)}
    return BinanceAPIException(response, 429, '{"msg": "Too many requests"}')


class TestFetchCandles:
    def test_empty_response_returns_empty(self, logger, candle_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        client = MagicMock()
        client.get_klines.return_value = []

        result = fetch_candles(
            logger=logger, client=client, settings=candle_settings,
            window_start=window_start, window_end=window_end,
        )
        assert result.candles == []
        assert result.raw_kline_count == 0
        assert result.request_count == 1

    def test_single_page_returns_candles(self, logger, candle_settings, window_start, window_end, mocker):
        now_ms = CLOSE_MS + 10_000_000
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=now_ms)
        mocker.patch("time.sleep")
        client = MagicMock()
        client.get_klines.return_value = [make_kline(PAST_MS, CLOSE_MS)]

        result = fetch_candles(
            logger=logger, client=client, settings=candle_settings,
            window_start=window_start, window_end=window_end,
        )
        assert len(result.candles) == 1
        assert result.raw_kline_count == 1
        assert result.request_count == 1

    def test_pagination_triggers_second_request(self, logger, candle_settings, window_start, window_end, mocker):
        now_ms = CLOSE_MS + 10_000_000
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=now_ms)
        client = MagicMock()
        full_batch = [make_kline(PAST_MS + i, CLOSE_MS + i) for i in range(_MAX_KLINES_LIMIT)]
        client.get_klines.side_effect = [full_batch, []]

        result = fetch_candles(
            logger=logger, client=client, settings=candle_settings,
            window_start=window_start, window_end=window_end,
        )
        assert client.get_klines.call_count == 2
        assert result.request_count == 2

    def test_future_candles_excluded(self, logger, candle_settings, window_start, window_end, mocker):
        future_close_ms = PAST_MS + 10_000_000_000  # far future
        now_ms = PAST_MS + 1_000_000  # now is before close_time
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=now_ms)
        client = MagicMock()
        client.get_klines.return_value = [make_kline(PAST_MS, future_close_ms)]

        result = fetch_candles(
            logger=logger, client=client, settings=candle_settings,
            window_start=window_start, window_end=window_end,
        )
        assert len(result.candles) == 0
        assert result.raw_kline_count == 1

    def test_rate_limit_retries_then_succeeds(self, logger, candle_settings, window_start, window_end, mocker):
        now_ms = CLOSE_MS + 10_000_000
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=now_ms)
        mocker.patch("time.sleep")
        client = MagicMock()
        client.get_klines.side_effect = [
            _make_rate_limit_error(),
            [make_kline(PAST_MS, CLOSE_MS)],
        ]

        result = fetch_candles(
            logger=logger, client=client, settings=candle_settings,
            window_start=window_start, window_end=window_end,
        )
        assert len(result.candles) == 1
        assert client.get_klines.call_count == 2

    def test_rate_limit_exhausted_raises(self, logger, candle_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=CLOSE_MS + 10_000_000)
        mocker.patch("time.sleep")
        client = MagicMock()
        client.get_klines.side_effect = _make_rate_limit_error()

        with pytest.raises(BinanceAPIException):
            fetch_candles(
                logger=logger, client=client, settings=candle_settings,
                window_start=window_start, window_end=window_end,
            )

    def test_non_429_error_raises_immediately(self, logger, candle_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=CLOSE_MS + 10_000_000)
        client = MagicMock()
        response = MagicMock()
        response.headers = {}
        client.get_klines.side_effect = BinanceAPIException(response, 400, '{"msg": "Bad symbol"}')

        with pytest.raises(BinanceAPIException) as exc:
            fetch_candles(
                logger=logger, client=client, settings=candle_settings,
                window_start=window_start, window_end=window_end,
            )
        assert exc.value.status_code == 400
        assert client.get_klines.call_count == 1


class TestRunBinanceCandles:
    def test_no_candles_skips_upsert(self, logger, candle_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=CLOSE_MS + 10_000_000)
        store = MemoryStore()
        client = MagicMock()
        client.get_klines.return_value = []

        metrics = run_binance_candles(
            logger=logger, settings=candle_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["candles"] == 0
        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_candles_written_to_store(self, logger, candle_settings, window_start, window_end, mocker):
        now_ms = CLOSE_MS + 10_000_000
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=now_ms)
        store = MemoryStore()
        client = MagicMock()
        client.get_klines.return_value = [make_kline(PAST_MS, CLOSE_MS)]

        metrics = run_binance_candles(
            logger=logger, settings=candle_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["candles"] == 1
        assert metrics["rows_affected"] == 1
        assert candle_settings.table_name in store._tables

    def test_metrics_shape(self, logger, candle_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=CLOSE_MS + 10_000_000)
        store = MemoryStore()
        client = MagicMock()
        client.get_klines.return_value = [make_kline(PAST_MS, CLOSE_MS)]

        metrics = run_binance_candles(
            logger=logger, settings=candle_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert "symbol" in metrics
        assert "interval" in metrics
        assert "raw_klines" in metrics
        assert "candles" in metrics
        assert "rows_affected" in metrics
        assert "binance_requests" in metrics
        assert "duration_seconds" in metrics
        assert "latest_source_close_time" in metrics

    def test_latest_close_time_none_when_no_candles(self, logger, candle_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.candles.run.utc_now_ms", return_value=CLOSE_MS + 10_000_000)
        store = MemoryStore()
        client = MagicMock()
        client.get_klines.return_value = []

        metrics = run_binance_candles(
            logger=logger, settings=candle_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["latest_source_close_time"] is None
