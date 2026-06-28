import logging
import time
from datetime import UTC, datetime
from time import perf_counter

from binance.client import Client
from binance.exceptions import BinanceAPIException

from core.helpers import utc_now_ms
from core.storage import Store
from pipelines import MetricValue
from pipelines.quality import check_frame

from .config import FundingRateSettings, FuturesMetricSettings, LongShortSettings
from .models import (
    FundingRateRow,
    FuturesMetricRow,
    LongShortRatioRow,
    RawBasisPoint,
    RawFundingRate,
    RawOpenInterest,
    RawPremiumIndexKline,
)

_MAX_FUNDING_RATE_LIMIT = 1000
_MAX_RATE_LIMIT_RETRIES = 3


def _call_with_retry(fn, logger: logging.Logger, label: str):  # type: ignore[no-untyped-def]
    for attempt in range(_MAX_RATE_LIMIT_RETRIES):
        try:
            return fn()
        except BinanceAPIException as error:
            if error.status_code == 429 and attempt < _MAX_RATE_LIMIT_RETRIES - 1:
                wait = int(error.response.headers.get("Retry-After", "60"))
                logger.warning(f"{label}: rate limited, sleeping {wait}s (attempt {attempt + 1}/{_MAX_RATE_LIMIT_RETRIES})")
                time.sleep(wait)
                continue
            raise


def _select_latest_open_interest(payload: list[dict]) -> RawOpenInterest | None:
    return RawOpenInterest.from_api_response(payload[-1]) if payload else None


def _select_latest_basis_point(payload: list[dict]) -> RawBasisPoint | None:
    return RawBasisPoint.from_api_response(payload[-1]) if payload else None


def _select_latest_premium_index_kline(asset: str, payload: list[list]) -> RawPremiumIndexKline | None:
    return RawPremiumIndexKline.from_api_response(asset, payload[-1]) if payload else None


def fetch_funding_rates(
    *,
    logger: logging.Logger,
    client: Client,
    settings: FundingRateSettings,
    window_start: datetime,
    window_end: datetime,
) -> list[FundingRateRow]:
    start_ms = int(window_start.timestamp() * 1000)
    end_ms = min(int(window_end.timestamp() * 1000), utc_now_ms() - 1)
    if start_ms > end_ms:
        return []

    rows: list[FundingRateRow] = []
    next_start_ms = start_ms
    label = f"[funding_rates] {settings.symbol}"

    while next_start_ms <= end_ms:
        batch = _call_with_retry(
            lambda: client.futures_funding_rate(  # type: ignore[no-any-return]
                symbol=settings.symbol,
                startTime=next_start_ms,
                endTime=end_ms,
                limit=_MAX_FUNDING_RATE_LIMIT,
            ),
            logger=logger,
            label=label,
        )
        if not batch:
            break

        rows.extend(
            FundingRateRow.from_raw(RawFundingRate.from_api_response(item), settings=settings)
            for item in batch
        )
        if len(batch) < _MAX_FUNDING_RATE_LIMIT:
            break
        next_start_ms = int(batch[-1]["fundingTime"]) + 1

    return rows


def fetch_futures_metric(
    *,
    logger: logging.Logger,
    client: Client,
    settings: FuturesMetricSettings,
    window_start: datetime,
    window_end: datetime,
) -> FuturesMetricRow | None:
    start_ms = int(window_start.timestamp() * 1000)
    end_ms = int(window_end.timestamp() * 1000)
    label = f"[futures_metrics] {settings.symbol}"

    open_interest = _select_latest_open_interest(
        _call_with_retry(
            lambda: client.futures_open_interest_hist(  # type: ignore[no-any-return]
                symbol=settings.symbol,
                period=settings.period,
                startTime=start_ms,
                endTime=end_ms,
            ),
            logger=logger,
            label=label,
        )
    )
    basis_point = _select_latest_basis_point(
        _call_with_retry(
            lambda: client.futures_basis(  # type: ignore[no-any-return]
                pair=settings.symbol,
                contractType=settings.contract_type,
                period=settings.period,
                startTime=start_ms,
                endTime=end_ms,
            ),
            logger=logger,
            label=label,
        )
    )
    premium_index_kline = _select_latest_premium_index_kline(
        settings.symbol,
        _call_with_retry(
            lambda: client.futures_klines(  # type: ignore[no-any-return]
                symbol=settings.symbol,
                interval=settings.premium_index_interval,
                startTime=start_ms,
                endTime=end_ms,
            ),
            logger=logger,
            label=label,
        ),
    )

    if all(v is None for v in (open_interest, basis_point, premium_index_kline)):
        return None

    return FuturesMetricRow.from_sources(
        settings=settings,
        metric_date=window_start.date(),
        open_interest=open_interest,
        basis_point=basis_point,
        premium_index_kline=premium_index_kline,
    )


def fetch_long_short_ratio(
    *,
    logger: logging.Logger,
    client: Client,
    settings: LongShortSettings,
    window_start: datetime,
    window_end: datetime,
) -> list[LongShortRatioRow]:
    start_ms = int(window_start.timestamp() * 1000)
    end_ms = int(window_end.timestamp() * 1000)
    label = f"[long_short_ratio] {settings.symbol}"
    payload = _call_with_retry(
        lambda: client.futures_global_longshort_ratio(  # type: ignore[no-any-return]
            symbol=settings.symbol,
            period=settings.period,
            startTime=start_ms,
            endTime=end_ms,
            limit=500,
        ),
        logger=logger,
        label=label,
    )
    return [LongShortRatioRow.from_api_response(item, settings=settings) for item in (payload or [])]


def run_funding_rates(
    *,
    logger: logging.Logger,
    settings: FundingRateSettings,
    store: Store,
    client: Client,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, MetricValue]:
    started_at = perf_counter()

    rows = fetch_funding_rates(
        logger=logger,
        client=client,
        settings=settings,
        window_start=window_start,
        window_end=window_end,
    )

    write_result = None
    latest_funding_ms: int | None = None
    quality_metrics: dict[str, MetricValue] = {}
    if rows:
        frame = FundingRateRow.to_frame(rows)
        report = check_frame(frame, FundingRateRow.quality_checks(), logger=logger, table=settings.table_name)
        quality_metrics = report.to_metrics()
        write_result = store.upsert(settings.table_name, frame)
        latest_funding_ms = max(row.funding_time_ms for row in rows)

    logger.info(
        f"[funding_rates] {settings.symbol}: "
        f"events={len(rows)} affected={write_result.rows_affected if write_result else 0}"
    )

    return {
        "symbol": settings.symbol,
        "events": len(rows),
        "rows_affected": write_result.rows_affected if write_result else 0,
        "duration_seconds": round(perf_counter() - started_at, 3),
        "latest_source_funding_time": (
            datetime.fromtimestamp(latest_funding_ms / 1000, tz=UTC).isoformat()
            if latest_funding_ms is not None
            else None
        ),
        **quality_metrics,
    }


def run_futures_metrics(
    *,
    logger: logging.Logger,
    settings: FuturesMetricSettings,
    store: Store,
    client: Client,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, MetricValue]:
    started_at = perf_counter()

    row = fetch_futures_metric(
        logger=logger,
        client=client,
        settings=settings,
        window_start=window_start,
        window_end=window_end,
    )

    write_result = None
    quality_metrics: dict[str, MetricValue] = {}
    if row is not None:
        frame = FuturesMetricRow.to_frame([row])
        report = check_frame(frame, FuturesMetricRow.quality_checks(), logger=logger, table=settings.table_name)
        quality_metrics = report.to_metrics()
        write_result = store.upsert(settings.table_name, frame)

    logger.info(
        f"[futures_metrics] {settings.symbol}: "
        f"rows={1 if row else 0} affected={write_result.rows_affected if write_result else 0}"
    )

    return {
        "symbol": settings.symbol,
        "rows": 1 if row is not None else 0,
        "rows_affected": write_result.rows_affected if write_result else 0,
        "duration_seconds": round(perf_counter() - started_at, 3),
        "partition_date": window_start.date().isoformat(),
        **quality_metrics,
    }


def run_long_short_ratio(
    *,
    logger: logging.Logger,
    settings: LongShortSettings,
    store: Store,
    client: Client,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, MetricValue]:
    started_at = perf_counter()

    rows = fetch_long_short_ratio(
        logger=logger,
        client=client,
        settings=settings,
        window_start=window_start,
        window_end=window_end,
    )

    write_result = None
    quality_metrics: dict[str, MetricValue] = {}
    if rows:
        frame = LongShortRatioRow.to_frame(rows)
        report = check_frame(frame, LongShortRatioRow.quality_checks(), logger=logger, table=settings.table_name)
        quality_metrics = report.to_metrics()
        write_result = store.upsert(settings.table_name, frame)

    logger.info(
        f"[long_short_ratio] {settings.symbol}: "
        f"rows={len(rows)} affected={write_result.rows_affected if write_result else 0}"
    )

    return {
        "symbol": settings.symbol,
        "rows": len(rows),
        "rows_affected": write_result.rows_affected if write_result else 0,
        "duration_seconds": round(perf_counter() - started_at, 3),
        "partition_date": window_start.date().isoformat(),
        **quality_metrics,
    }
