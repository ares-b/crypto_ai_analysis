
from datetime import UTC, datetime

from core.helpers import isoformat_ms, utc_now_ms


def test_utc_now_ms_returns_int():
    result = utc_now_ms()
    assert isinstance(result, int)


def test_utc_now_ms_is_recent():
    before = int(datetime.now(UTC).timestamp() * 1000)
    result = utc_now_ms()
    after = int(datetime.now(UTC).timestamp() * 1000)
    assert before <= result <= after


def test_isoformat_ms_known_value():
    ms = 1717200000000  # 2024-06-01 00:00:00 UTC
    result = isoformat_ms(ms)
    assert result.startswith("2024-06-01T00:00:00")
    assert result.endswith("+00:00")


def test_isoformat_ms_returns_string():
    assert isinstance(isoformat_ms(0), str)
