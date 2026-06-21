from datetime import UTC, datetime

import pytest

from pipelines.raw.exchange_flows.models import ExchangeFlowRow

_NOW = datetime(2024, 6, 1, tzinfo=UTC)


def _make_payload(**overrides) -> dict:
    base = {
        "date": "2024-06-01T00:00:00",
        "reserve": "125000.5",
        "reserve_usd": "8125032500.0",
        "inflow": "1200.0",
        "outflow": "950.0",
        "netflow": "250.0",
    }
    return {**base, **overrides}


class TestExchangeFlowRowFromApiResponse:
    def test_valid_payload(self):
        row = ExchangeFlowRow.from_api_response(
            _make_payload(), asset="btc", exchange="all_exchange", source_updated_at=_NOW
        )
        assert row.asset == "btc"
        assert row.exchange == "all_exchange"
        assert row.reserve == 125000.5
        assert row.inflow == 1200.0
        assert row.netflow == 250.0
        assert row.source_updated_at == _NOW

    def test_optional_fields_none(self):
        row = ExchangeFlowRow.from_api_response(
            _make_payload(reserve=None, inflow=None, outflow=None, netflow=None),
            asset="btc",
            exchange="all_exchange",
            source_updated_at=_NOW,
        )
        assert row.reserve is None
        assert row.inflow is None
        assert row.netflow is None

    def test_missing_date_raises(self):
        payload = _make_payload()
        del payload["date"]
        with pytest.raises(ValueError, match="Invalid"):
            ExchangeFlowRow.from_api_response(
                payload, asset="btc", exchange="all_exchange", source_updated_at=_NOW
            )

    def test_to_frame(self):
        row = ExchangeFlowRow.from_api_response(
            _make_payload(), asset="btc", exchange="all_exchange", source_updated_at=_NOW
        )
        frame = ExchangeFlowRow.to_frame([row])
        assert "reserve" in frame.columns
        assert "netflow" in frame.columns
        assert len(frame) == 1
