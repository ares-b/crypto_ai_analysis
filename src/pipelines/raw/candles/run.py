import logging
import time
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter

from binance.client import Client
from binance.exceptions import BinanceAPIException

from core.helpers import isoformat_ms, utc_now_ms
from core.storage import Store
from pipelines import MetricValue

from .config import BinanceCandleSettings
from .models import BinanceCandleRow, RawKline

_MAX_KLINES_LIMIT = 1000
_MAX_RATE_LIMIT_RETRIES = 3


@dataclass(frozen=True)
class BinanceFetchResult:
    candles: list[BinanceCandleRow]
    raw_kline_count: int
    request_count: int


def fetch_candles(
    *,
    logger: logging.Logger,
    client: Client,
    settings: BinanceCandleSettings,
    window_start: datetime,
    window_end: datetime,
) -> BinanceFetchResult:
    now_ms = utc_now_ms()
    start_ms = int(window_start.timestamp() * 1000)
    effective_end_ms = min(int(window_end.timestamp() * 1000), now_ms - 1)
    next_start_ms = start_ms
    klines: list[RawKline] = []
    request_count = 0

    while next_start_ms <= effective_end_ms:
        batch: list | None = None
        for attempt in range(_MAX_RATE_LIMIT_RETRIES):
            try:
                batch = client.get_klines(  # type: ignore[no-any-return]
                    symbol=settings.symbol,
                    interval=settings.interval,
                    startTime=next_start_ms,
                    endTime=effective_end_ms,
                    limit=_MAX_KLINES_LIMIT,
                )
                break
            except BinanceAPIException as error:
                if error.status_code == 429 and attempt < _MAX_RATE_LIMIT_RETRIES - 1:
                    wait = int(error.response.headers.get("Retry-After", "60"))
                    logger.warning(
                        f"[{settings.interval}] {settings.symbol}: rate limited, sleeping {wait}s "
                        f"(attempt {attempt + 1}/{_MAX_RATE_LIMIT_RETRIES})"
                    )
                    time.sleep(wait)
                    continue
                raise

        request_count += 1
        if not batch:
            break

        parsed = [RawKline.from_api_response(item) for item in batch]
        klines.extend(parsed)
        next_start_ms = parsed[-1].open_time_ms + 1
        if len(batch) < _MAX_KLINES_LIMIT:
            break

    candles = [
        BinanceCandleRow.from_kline(settings=settings, kline=kline)
        for kline in klines
        if kline.close_time_ms < now_ms
    ]
    return BinanceFetchResult(candles=candles, raw_kline_count=len(klines), request_count=request_count)


def run_binance_candles(
    *,
    logger: logging.Logger,
    settings: BinanceCandleSettings,
    store: Store,
    client: Client,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, MetricValue]:
    started_at = perf_counter()

    result = fetch_candles(
        logger=logger,
        client=client,
        settings=settings,
        window_start=window_start,
        window_end=window_end,
    )

    write_result = None
    latest_close_ms: int | None = None
    if result.candles:
        write_result = store.upsert(settings.table_name, BinanceCandleRow.to_frame(result.candles))
        latest_close_ms = max(c.close_time_ms for c in result.candles)

    logger.info(
        f"[{settings.interval}] {settings.symbol}: "
        f"raw_klines={result.raw_kline_count} candles={len(result.candles)} "
        f"inserted={write_result.rows_affected if write_result else 0}"
    )

    return {
        "symbol": settings.symbol,
        "interval": settings.interval,
        "raw_klines": result.raw_kline_count,
        "candles": len(result.candles),
        "rows_affected": write_result.rows_affected if write_result else 0,
        "binance_requests": result.request_count,
        "duration_seconds": round(perf_counter() - started_at, 3),
        "latest_source_close_time": (
            isoformat_ms(latest_close_ms) if latest_close_ms is not None else None
        ),
    }
