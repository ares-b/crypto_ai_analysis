
import pytest

from pipelines.raw.candles.config import BinanceCandleSettings
from pipelines.raw.candles.models import BinanceCandleRow, RawKline
from tests.conftest import make_kline

SETTINGS = BinanceCandleSettings(interval="1d")


class TestRawKlineFromApiResponse:
    def test_valid_payload(self):
        kline = RawKline.from_api_response(make_kline())
        assert kline.open_time_ms == 1717200000000
        assert kline.open == 65000.0
        assert kline.close == 65500.0
        assert kline.number_of_trades == 5000

    def test_wrong_length_raises(self):
        with pytest.raises(ValueError, match="length"):
            RawKline.from_api_response(make_kline()[:11])

    def test_too_long_raises(self):
        with pytest.raises(ValueError, match="length"):
            RawKline.from_api_response(make_kline() + ["extra"])

    def test_invalid_type_raises(self):
        bad = make_kline()
        bad[0] = "not_a_number"
        with pytest.raises(ValueError, match="Invalid"):
            RawKline.from_api_response(bad)

    def test_string_numbers_parsed(self):
        kline = RawKline.from_api_response(make_kline())
        assert isinstance(kline.open, float)
        assert isinstance(kline.volume, float)


class TestBinanceCandleRowFromKline:
    def test_fields_mapped_correctly(self):
        kline = RawKline.from_api_response(make_kline())
        row = BinanceCandleRow.from_kline(settings=SETTINGS, kline=kline)
        assert row.instrument == "BTC"
        assert row.counterpart == "USDT"
        assert row.interval == "1d"
        assert row.open == 65000.0
        assert row.close == 65500.0

    def test_timestamps_are_utc(self):
        from datetime import UTC
        kline = RawKline.from_api_response(make_kline())
        row = BinanceCandleRow.from_kline(settings=SETTINGS, kline=kline)
        assert row.open_time.tzinfo is UTC
        assert row.close_time.tzinfo is UTC

    def test_close_time_ms_roundtrip(self):
        kline = RawKline.from_api_response(make_kline())
        row = BinanceCandleRow.from_kline(settings=SETTINGS, kline=kline)
        assert row.close_time_ms == kline.close_time_ms

    def test_to_frame(self):
        kline = RawKline.from_api_response(make_kline())
        row = BinanceCandleRow.from_kline(settings=SETTINGS, kline=kline)
        frame = BinanceCandleRow.to_frame([row])
        assert "instrument" in frame.columns
        assert "open" in frame.columns
        assert len(frame) == 1
