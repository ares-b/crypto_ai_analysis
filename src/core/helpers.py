from __future__ import annotations

from datetime import UTC, datetime


def utc_now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def isoformat_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()
