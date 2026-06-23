
from datetime import date

import pytest

from pipelines.raw.futures.config import FundingRateSettings, FuturesMetricSettings
from pipelines.raw.futures.models import (
    FundingRateRow,
    FuturesMetricRow,
    RawBasisPoint,
    RawFundingRate,
    RawOpenInterest,
    RawPremiumIndexKline,
)
from tests.conftest import (
    make_basis_payload,
    make_funding_rate_payload,
    make_open_interest_payload,
    make_premium_index_kline,
)

FUNDING_SETTINGS = FundingRateSettings()
METRIC_SETTINGS = FuturesMetricSettings()


class TestRawFundingRate:
    def test_valid_payload(self):
        raw = RawFundingRate.from_api_response(make_funding_rate_payload())
        assert raw.asset == "BTCUSDT"
        assert raw.funding_rate == 0.0001
        assert raw.mark_price == 65000.0

    def test_missing_mark_price_is_none(self):
        payload = make_funding_rate_payload()
        payload["markPrice"] = ""
        raw = RawFundingRate.from_api_response(payload)
        assert raw.mark_price is None

    def test_null_mark_price_is_none(self):
        payload = make_funding_rate_payload()
        payload["markPrice"] = None
        raw = RawFundingRate.from_api_response(payload)
        assert raw.mark_price is None

    def test_missing_key_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            RawFundingRate.from_api_response({"symbol": "BTCUSDT"})


class TestRawOpenInterest:
    def test_valid_payload(self):
        raw = RawOpenInterest.from_api_response(make_open_interest_payload())
        assert raw.asset == "BTCUSDT"
        assert raw.open_interest == 12345.67

    def test_missing_key_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            RawOpenInterest.from_api_response({"symbol": "BTCUSDT"})


class TestRawBasisPoint:
    def test_valid_payload(self):
        raw = RawBasisPoint.from_api_response(make_basis_payload())
        assert raw.asset == "BTCUSDT"
        assert raw.basis == 100.50
        assert raw.basis_rate == 0.0015

    def test_null_basis_is_none(self):
        payload = make_basis_payload()
        payload["basis"] = None
        raw = RawBasisPoint.from_api_response(payload)
        assert raw.basis is None

    def test_missing_key_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            RawBasisPoint.from_api_response({"pair": "BTCUSDT"})


class TestRawPremiumIndexKline:
    def test_valid_payload(self):
        raw = RawPremiumIndexKline.from_api_response("BTCUSDT", make_premium_index_kline())
        assert raw.asset == "BTCUSDT"
        assert raw.close_premium_index == 0.0012

    def test_null_premium_is_none(self):
        payload = make_premium_index_kline()
        payload[4] = None
        raw = RawPremiumIndexKline.from_api_response("BTCUSDT", payload)
        assert raw.close_premium_index is None

    def test_short_payload_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            RawPremiumIndexKline.from_api_response("BTCUSDT", [1, 2, 3])


class TestFundingRateRow:
    def test_from_raw(self):
        raw = RawFundingRate.from_api_response(make_funding_rate_payload(1717200000000))
        row = FundingRateRow.from_raw(raw, settings=FUNDING_SETTINGS)
        assert row.instrument == "BTC"
        assert row.counterpart == "USDT"
        assert row.funding_rate == 0.0001
        assert row.funding_time_ms == 1717200000000

    def test_to_frame(self):
        raw = RawFundingRate.from_api_response(make_funding_rate_payload())
        row = FundingRateRow.from_raw(raw, settings=FUNDING_SETTINGS)
        frame = FundingRateRow.to_frame([row])
        assert "instrument" in frame.columns
        assert "funding_rate" in frame.columns
        assert len(frame) == 1


class TestFuturesMetricRow:
    def test_from_all_sources(self):
        oi = RawOpenInterest.from_api_response(make_open_interest_payload())
        bp = RawBasisPoint.from_api_response(make_basis_payload())
        pk = RawPremiumIndexKline.from_api_response("BTCUSDT", make_premium_index_kline())

        row = FuturesMetricRow.from_sources(
            settings=METRIC_SETTINGS,
            metric_date=date(2026, 6, 7),
            open_interest=oi,
            basis_point=bp,
            premium_index_kline=pk,
        )
        assert row.instrument == "BTC"
        assert row.open_interest == 12345.67
        assert row.basis == 100.50
        assert row.premium_index == 0.0012
        assert row.date == date(2026, 6, 7)

    def test_from_partial_sources(self):
        oi = RawOpenInterest.from_api_response(make_open_interest_payload())
        row = FuturesMetricRow.from_sources(
            settings=METRIC_SETTINGS,
            metric_date=date(2026, 6, 7),
            open_interest=oi,
            basis_point=None,
            premium_index_kline=None,
        )
        assert row.open_interest == 12345.67
        assert row.basis is None
        assert row.premium_index is None

    def test_to_frame(self):
        oi = RawOpenInterest.from_api_response(make_open_interest_payload())
        row = FuturesMetricRow.from_sources(
            settings=METRIC_SETTINGS,
            metric_date=date(2026, 6, 7),
            open_interest=oi,
            basis_point=None,
            premium_index_kline=None,
        )
        frame = FuturesMetricRow.to_frame([row])
        assert "open_interest" in frame.columns
        assert len(frame) == 1
