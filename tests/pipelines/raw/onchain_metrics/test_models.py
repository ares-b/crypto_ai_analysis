from datetime import UTC, datetime

import pytest

from pipelines.raw.onchain_metrics.models import OnchainMetricRow, RawOnchainMetric

_NOW = datetime(2024, 6, 1, tzinfo=UTC)


def _make_api_data(**overrides) -> dict:
    base = {
        "time": "2024-06-01T00:00:00Z",
        "asset": "btc",
        "CapMrktCurUSD": "1200000000000",
        "CapMVRVCur": "2.5",
        "SplyCur": "19700000",
        "AdrActCnt": "850000",
        "HashRate": "600000000000000000",
        "SOPR": "1.02",
        "PriceRealizedUSD": "35000",
    }
    return {**base, **overrides}


class TestRawOnchainMetric:
    def test_valid_all_fields(self):
        raw = RawOnchainMetric.from_api_response(_make_api_data())
        assert raw.asset == "btc"
        assert raw.market_cap_usd == 1.2e12
        assert raw.mvrv == 2.5
        assert raw.active_addresses == 850000
        assert raw.sopr == 1.02
        assert raw.realized_price_usd == 35000.0

    def test_optional_fields_none(self):
        raw = RawOnchainMetric.from_api_response(
            _make_api_data(CapMrktCurUSD=None, SOPR="", PriceRealizedUSD=None)
        )
        assert raw.market_cap_usd is None
        assert raw.sopr is None
        assert raw.realized_price_usd is None

    def test_missing_time_raises(self):
        data = _make_api_data()
        del data["time"]
        with pytest.raises(ValueError, match="Invalid"):
            RawOnchainMetric.from_api_response(data)

    def test_missing_asset_raises(self):
        data = _make_api_data()
        del data["asset"]
        with pytest.raises(ValueError, match="Invalid"):
            RawOnchainMetric.from_api_response(data)


class TestOnchainMetricRow:
    def test_from_raw(self):
        raw = RawOnchainMetric.from_api_response(_make_api_data())
        row = OnchainMetricRow.from_raw(raw, instrument="BTC", counterpart="USD", source_updated_at=_NOW)

        assert row.instrument == "BTC"
        assert row.counterpart == "USD"
        assert row.market_cap_usd == 1.2e12
        assert row.sopr == 1.02
        assert row.source_updated_at.tzinfo is UTC

    def test_to_frame(self):
        raw = RawOnchainMetric.from_api_response(_make_api_data())
        row = OnchainMetricRow.from_raw(raw, instrument="BTC", counterpart="USD", source_updated_at=_NOW)
        frame = OnchainMetricRow.to_frame([row])

        assert "instrument" in frame.columns
        assert "sopr" in frame.columns
        assert "realized_price_usd" in frame.columns
        assert len(frame) == 1
