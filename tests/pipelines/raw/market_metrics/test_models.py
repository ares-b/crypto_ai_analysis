from datetime import UTC, date, datetime

import pytest

from pipelines.raw.market_metrics.models import (
    MarketMetricRow,
    RawHistoricalBtcDominancePoint,
    RawMarketMetric,
    StablecoinSupplyRow,
)

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
_DATE = date(2024, 6, 1)
_TS_MS = 1717200000000  # 2024-06-01 00:00:00 UTC


class TestRawHistoricalBtcDominancePoint:
    def test_valid_payload(self):
        point = RawHistoricalBtcDominancePoint.from_api_response([_TS_MS, 53.5])
        assert point.date == _DATE
        assert point.btc_dominance_pct == 53.5

    def test_string_numbers_parsed(self):
        point = RawHistoricalBtcDominancePoint.from_api_response([str(_TS_MS), "53.5"])
        assert point.btc_dominance_pct == 53.5

    def test_short_payload_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            RawHistoricalBtcDominancePoint.from_api_response([_TS_MS])

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            RawHistoricalBtcDominancePoint.from_api_response(["bad", "worse"])


class TestRawMarketMetric:
    def test_from_coingecko_global(self):
        raw = RawMarketMetric.from_coingecko_global(
            metric_date=_DATE,
            snapshot_at=_NOW,
            btc_dominance_pct=53.5,
        )
        assert raw.date == _DATE
        assert raw.btc_dominance_pct == 53.5
        assert raw.source_updated_at.tzinfo is UTC


class TestMarketMetricRow:
    def test_from_raw(self):
        raw = RawMarketMetric(date=_DATE, btc_dominance_pct=53.5, source_updated_at=_NOW)
        row = MarketMetricRow.from_raw(raw)
        assert row.date == _DATE
        assert row.btc_dominance_pct == 53.5

    def test_from_historical_point(self):
        point = RawHistoricalBtcDominancePoint(date=_DATE, btc_dominance_pct=53.5)
        row = MarketMetricRow.from_historical_point(point)
        assert row.date == _DATE
        assert row.btc_dominance_pct == 53.5
        assert row.source_updated_at.tzinfo is not None

    def test_to_frame(self):
        raw = RawMarketMetric(date=_DATE, btc_dominance_pct=53.5, source_updated_at=_NOW)
        frame = MarketMetricRow.to_frame([MarketMetricRow.from_raw(raw)])
        assert "btc_dominance_pct" in frame.columns
        assert len(frame) == 1


class TestStablecoinSupplyRow:
    def test_to_frame(self):
        row = StablecoinSupplyRow(
            date=_DATE,
            usdt_market_cap_usd=80e9,
            usdc_market_cap_usd=25e9,
            total_stablecoin_market_cap_usd=105e9,
            source_updated_at=_NOW,
        )
        frame = StablecoinSupplyRow.to_frame([row])
        assert "usdt_market_cap_usd" in frame.columns
        assert len(frame) == 1
