from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from binance.exceptions import BinanceAPIException

from pipelines.raw.futures.config import LongShortSettings
from pipelines.raw.futures.run import (
    _MAX_FUNDING_RATE_LIMIT,
    fetch_funding_rates,
    fetch_futures_metric,
    fetch_long_short_ratio,
    run_funding_rates,
    run_futures_metrics,
    run_long_short_ratio,
)
from tests.conftest import (
    MemoryStore,
    make_basis_payload,
    make_funding_rate_payload,
    make_open_interest_payload,
    make_premium_index_kline,
)

PAST_MS = 1717200000000


def _make_rate_limit_error(retry_after: int = 1) -> BinanceAPIException:
    response = MagicMock()
    response.headers = {"Retry-After": str(retry_after)}
    return BinanceAPIException(response, 429, '{"msg": "Too many requests"}')


def _make_client(
    funding_rates=None,
    open_interest=None,
    basis=None,
    klines=None,
) -> MagicMock:
    client = MagicMock()
    client.futures_funding_rate.return_value = funding_rates or []
    client.futures_open_interest_hist.return_value = open_interest or []
    client.futures_basis.return_value = basis or []
    client.futures_klines.return_value = klines or []
    return client


class TestFetchFundingRates:
    def test_empty_response(self, logger, funding_rate_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.futures.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        client = _make_client()
        rows = fetch_funding_rates(
            logger=logger, client=client, settings=funding_rate_settings,
            window_start=window_start, window_end=window_end,
        )
        assert rows == []

    def test_single_page(self, logger, funding_rate_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.futures.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        payload = [make_funding_rate_payload(PAST_MS + i * 1000) for i in range(3)]
        client = _make_client(funding_rates=payload)

        rows = fetch_funding_rates(
            logger=logger, client=client, settings=funding_rate_settings,
            window_start=window_start, window_end=window_end,
        )
        assert len(rows) == 3
        assert rows[0].instrument == "BTC"

    def test_pagination_triggers_next_page(self, logger, funding_rate_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.futures.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        full_batch = [make_funding_rate_payload(PAST_MS + i * 1000) for i in range(_MAX_FUNDING_RATE_LIMIT)]
        second_batch = [make_funding_rate_payload(PAST_MS + (_MAX_FUNDING_RATE_LIMIT + 1) * 1000)]
        client = MagicMock()
        client.futures_funding_rate.side_effect = [full_batch, second_batch]

        rows = fetch_funding_rates(
            logger=logger, client=client, settings=funding_rate_settings,
            window_start=window_start, window_end=window_end,
        )
        assert client.futures_funding_rate.call_count == 2
        assert len(rows) == _MAX_FUNDING_RATE_LIMIT + 1

    def test_window_start_after_end_returns_empty(self, logger, funding_rate_settings, mocker):
        mocker.patch("pipelines.raw.futures.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        client = _make_client()
        rows = fetch_funding_rates(
            logger=logger, client=client, settings=funding_rate_settings,
            window_start=datetime(2024, 6, 2, tzinfo=UTC),
            window_end=datetime(2024, 6, 1, tzinfo=UTC),
        )
        assert rows == []
        client.futures_funding_rate.assert_not_called()

    def test_rate_limit_retry(self, logger, funding_rate_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.futures.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        mocker.patch("time.sleep")
        client = MagicMock()
        client.futures_funding_rate.side_effect = [
            _make_rate_limit_error(),
            [make_funding_rate_payload()],
        ]
        rows = fetch_funding_rates(
            logger=logger, client=client, settings=funding_rate_settings,
            window_start=window_start, window_end=window_end,
        )
        assert len(rows) == 1
        assert client.futures_funding_rate.call_count == 2

    def test_rate_limit_exhausted_raises(self, logger, funding_rate_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.futures.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        mocker.patch("time.sleep")
        client = MagicMock()
        client.futures_funding_rate.side_effect = _make_rate_limit_error()

        with pytest.raises(BinanceAPIException):
            fetch_funding_rates(
                logger=logger, client=client, settings=funding_rate_settings,
                window_start=window_start, window_end=window_end,
            )


class TestFetchFuturesMetric:
    def test_all_sources_returns_row(self, logger, futures_metric_settings, window_start, window_end):
        client = _make_client(
            open_interest=[make_open_interest_payload()],
            basis=[make_basis_payload()],
            klines=[make_premium_index_kline()],
        )
        row = fetch_futures_metric(
            logger=logger, client=client, settings=futures_metric_settings,
            window_start=window_start, window_end=window_end,
        )
        assert row is not None
        assert row.open_interest == 12345.67
        assert row.basis == 100.50
        assert row.premium_index == 0.0012

    def test_metric_date_from_window_start(self, logger, futures_metric_settings, window_start, window_end):
        client = _make_client(open_interest=[make_open_interest_payload()])
        row = fetch_futures_metric(
            logger=logger, client=client, settings=futures_metric_settings,
            window_start=window_start, window_end=window_end,
        )
        assert row is not None
        assert row.date == window_start.date()

    def test_all_sources_none_returns_none(self, logger, futures_metric_settings, window_start, window_end):
        client = _make_client()
        row = fetch_futures_metric(
            logger=logger, client=client, settings=futures_metric_settings,
            window_start=window_start, window_end=window_end,
        )
        assert row is None

    def test_partial_sources_returns_row(self, logger, futures_metric_settings, window_start, window_end):
        client = _make_client(open_interest=[make_open_interest_payload()])
        row = fetch_futures_metric(
            logger=logger, client=client, settings=futures_metric_settings,
            window_start=window_start, window_end=window_end,
        )
        assert row is not None
        assert row.open_interest == 12345.67
        assert row.basis is None
        assert row.premium_index is None

    def test_rate_limit_retry_on_oi(self, logger, futures_metric_settings, window_start, window_end, mocker):
        mocker.patch("time.sleep")
        client = MagicMock()
        client.futures_open_interest_hist.side_effect = [
            _make_rate_limit_error(),
            [make_open_interest_payload()],
        ]
        client.futures_basis.return_value = []
        client.futures_klines.return_value = []

        row = fetch_futures_metric(
            logger=logger, client=client, settings=futures_metric_settings,
            window_start=window_start, window_end=window_end,
        )
        assert client.futures_open_interest_hist.call_count == 2
        assert row is not None


class TestRunFundingRates:
    def test_no_rows_skips_upsert(self, logger, funding_rate_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.futures.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        store = MemoryStore()
        client = _make_client()

        metrics = run_funding_rates(
            logger=logger, settings=funding_rate_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["events"] == 0
        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_rows_written_to_store(self, logger, funding_rate_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.futures.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        store = MemoryStore()
        client = _make_client(funding_rates=[make_funding_rate_payload()])

        metrics = run_funding_rates(
            logger=logger, settings=funding_rate_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["events"] == 1
        assert metrics["rows_affected"] == 1
        assert funding_rate_settings.table_name in store._tables

    def test_metrics_shape(self, logger, funding_rate_settings, window_start, window_end, mocker):
        mocker.patch("pipelines.raw.futures.run.utc_now_ms", return_value=PAST_MS + 10_000_000)
        store = MemoryStore()
        client = _make_client()

        metrics = run_funding_rates(
            logger=logger, settings=funding_rate_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert "symbol" in metrics
        assert "events" in metrics
        assert "rows_affected" in metrics
        assert "duration_seconds" in metrics
        assert "latest_source_funding_time" in metrics


class TestRunFuturesMetrics:
    def test_no_data_skips_upsert(self, logger, futures_metric_settings, window_start, window_end):
        store = MemoryStore()
        client = _make_client()

        metrics = run_futures_metrics(
            logger=logger, settings=futures_metric_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["rows"] == 0
        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_row_written_to_store(self, logger, futures_metric_settings, window_start, window_end):
        store = MemoryStore()
        client = _make_client(open_interest=[make_open_interest_payload()])

        metrics = run_futures_metrics(
            logger=logger, settings=futures_metric_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["rows"] == 1
        assert metrics["rows_affected"] == 1
        assert futures_metric_settings.table_name in store._tables

    def test_partition_date_in_metrics(self, logger, futures_metric_settings, window_start, window_end):
        store = MemoryStore()
        client = _make_client()

        metrics = run_futures_metrics(
            logger=logger, settings=futures_metric_settings, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["partition_date"] == window_start.date().isoformat()


def make_long_short_payload(ts_ms: int = 1717200000000) -> dict:
    return {
        "timestamp": str(ts_ms),
        "longShortRatio": "1.25",
        "longAccount": "0.555",
        "shortAccount": "0.445",
    }


_LONG_SHORT_SETTINGS = LongShortSettings()


class TestFetchLongShortRatio:
    def test_empty_response(self, logger, window_start, window_end):
        client = MagicMock()
        client.futures_global_longshort_ratio.return_value = []
        rows = fetch_long_short_ratio(
            logger=logger, client=client, settings=_LONG_SHORT_SETTINGS,
            window_start=window_start, window_end=window_end,
        )
        assert rows == []

    def test_single_row(self, logger, window_start, window_end):
        client = MagicMock()
        client.futures_global_longshort_ratio.return_value = [make_long_short_payload()]
        rows = fetch_long_short_ratio(
            logger=logger, client=client, settings=_LONG_SHORT_SETTINGS,
            window_start=window_start, window_end=window_end,
        )
        assert len(rows) == 1
        assert rows[0].long_short_ratio == 1.25
        assert rows[0].instrument == "BTC"

    def test_none_response_returns_empty(self, logger, window_start, window_end):
        client = MagicMock()
        client.futures_global_longshort_ratio.return_value = None
        rows = fetch_long_short_ratio(
            logger=logger, client=client, settings=_LONG_SHORT_SETTINGS,
            window_start=window_start, window_end=window_end,
        )
        assert rows == []

    def test_rate_limit_retry(self, logger, window_start, window_end, mocker):
        mocker.patch("time.sleep")
        client = MagicMock()
        client.futures_global_longshort_ratio.side_effect = [
            _make_rate_limit_error(),
            [make_long_short_payload()],
        ]
        rows = fetch_long_short_ratio(
            logger=logger, client=client, settings=_LONG_SHORT_SETTINGS,
            window_start=window_start, window_end=window_end,
        )
        assert len(rows) == 1
        assert client.futures_global_longshort_ratio.call_count == 2


class TestRunLongShortRatio:
    def test_no_rows_skips_upsert(self, logger, window_start, window_end):
        store = MemoryStore()
        client = MagicMock()
        client.futures_global_longshort_ratio.return_value = []
        metrics = run_long_short_ratio(
            logger=logger, settings=_LONG_SHORT_SETTINGS, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["rows"] == 0
        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_rows_written_to_store(self, logger, window_start, window_end):
        store = MemoryStore()
        client = MagicMock()
        client.futures_global_longshort_ratio.return_value = [make_long_short_payload()]
        metrics = run_long_short_ratio(
            logger=logger, settings=_LONG_SHORT_SETTINGS, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert metrics["rows"] == 1
        assert metrics["rows_affected"] == 1
        assert _LONG_SHORT_SETTINGS.table_name in store._tables

    def test_metrics_shape(self, logger, window_start, window_end):
        store = MemoryStore()
        client = MagicMock()
        client.futures_global_longshort_ratio.return_value = []
        metrics = run_long_short_ratio(
            logger=logger, settings=_LONG_SHORT_SETTINGS, store=store,
            client=client, window_start=window_start, window_end=window_end,
        )
        assert "symbol" in metrics
        assert "rows" in metrics
        assert "rows_affected" in metrics
        assert "duration_seconds" in metrics
