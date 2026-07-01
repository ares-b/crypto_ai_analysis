import logging
from datetime import UTC, date, datetime
from time import perf_counter

from core.http import HttpClient, HttpError
from core.quality import QualitySubject, RunResult
from core.storage import Store
from pipelines import MetricValue
from pipelines.quality import check_frame

from .config import MarketMetricSettings
from .models import (
    MarketMetricRow,
    RawHistoricalBtcDominancePoint,
    RawMarketMetric,
    StablecoinSupplyRow,
)

_DOMINANCE_DAYS = 365


def _get_btc_dominance_history(client: HttpClient) -> list[list]:
    btc_data = client.get_json(
        "/api/v3/coins/bitcoin/market_chart",
        params={"vs_currency": "usd", "days": _DOMINANCE_DAYS, "interval": "daily"},
    )
    global_data = client.get_json(
        "/api/v3/global/market_cap_chart",
        params={"days": _DOMINANCE_DAYS},
    )
    btc_caps: list[list] = btc_data.get("market_caps", [])
    total_caps: list[list] = global_data.get("market_cap_chart", {}).get("market_cap", [])

    total_by_ts = {int(row[0]): float(row[1]) for row in total_caps}
    result: list[list] = []
    for row in btc_caps:
        ts = int(row[0])
        total = total_by_ts.get(ts)
        if total and total > 0:
            result.append([ts, float(row[1]) / total * 100])
    return result


def _get_btc_dominance_snapshot(client: HttpClient) -> tuple[datetime, float]:
    payload = client.get_json("/api/v3/global")
    pct = float(payload["data"]["market_cap_percentage"]["btc"])
    return datetime.now(UTC), pct


def _fetch_coin_market_cap(client: HttpClient, coin_id: str) -> float | None:
    try:
        payload = client.get_json(
            f"/api/v3/coins/{coin_id}",
            params={
                "localization": "false",
                "tickers": "false",
                "market_data": "true",
                "community_data": "false",
                "developer_data": "false",
            },
        )
        return float(payload["market_data"]["market_cap"]["usd"])
    except (HttpError, KeyError, TypeError, ValueError):
        return None


def fetch_market_metric_row(
    *,
    logger: logging.Logger,
    client: HttpClient,
    run_date: date,
) -> tuple[MarketMetricRow | None, int]:
    try:
        history_raw = _get_btc_dominance_history(client)
        history = {
            point.date: point
            for point in (RawHistoricalBtcDominancePoint.from_api_response(item) for item in history_raw)
        }
        point = history.get(run_date)
        if point is not None:
            return MarketMetricRow.from_historical_point(point), 2

        snapshot_at, btc_dominance_pct = _get_btc_dominance_snapshot(client)
        row = MarketMetricRow.from_raw(
            RawMarketMetric.from_coingecko_global(
                metric_date=run_date,
                snapshot_at=snapshot_at,
                btc_dominance_pct=btc_dominance_pct,
            )
        )
        return row, 3
    except HttpError as exc:
        logger.warning(f"[market_metrics] CoinGecko error for {run_date.isoformat()}: {exc}")
        return None, 0


def fetch_stablecoin_supply(
    *,
    logger: logging.Logger,
    client: HttpClient,
    run_date: date,
) -> StablecoinSupplyRow | None:
    source_updated_at = datetime.now(UTC)
    usdt = _fetch_coin_market_cap(client, "tether")
    usdc = _fetch_coin_market_cap(client, "usd-coin")
    if usdt is None and usdc is None:
        logger.warning(f"[stablecoin_supply] no data for {run_date.isoformat()}")
        return None
    total = (usdt or 0.0) + (usdc or 0.0) or None
    return StablecoinSupplyRow(
        date=run_date,
        usdt_market_cap_usd=usdt,
        usdc_market_cap_usd=usdc,
        total_stablecoin_market_cap_usd=total,
        source_updated_at=source_updated_at,
    )


def market_metrics_quality_subjects(*, settings: MarketMetricSettings) -> list[QualitySubject]:
    return [(settings.table_name, MarketMetricRow.quality_checks())]


def stablecoin_supply_quality_subjects(*, settings: MarketMetricSettings) -> list[QualitySubject]:
    return [(settings.stablecoin_table, StablecoinSupplyRow.quality_checks())]


def run_market_metrics(
    *,
    logger: logging.Logger,
    settings: MarketMetricSettings,
    store: Store,
    client: HttpClient,
    run_date: date,
) -> RunResult:
    started_at = perf_counter()
    row, request_count = fetch_market_metric_row(logger=logger, client=client, run_date=run_date)
    rows_affected = 0
    quality_metrics: dict[str, MetricValue] = {}
    reports = []
    if row is not None:
        frame = MarketMetricRow.to_frame([row])
        report = check_frame(frame, MarketMetricRow.quality_checks(), logger=logger, table=settings.table_name)
        reports.append(report)
        quality_metrics = report.to_metrics()
        if report.ok:
            rows_affected = store.upsert(settings.table_name, frame).rows_affected

    btc_dominance_pct = row.btc_dominance_pct if row is not None else None
    logger.info(
        f"[market_metrics] date={run_date.isoformat()} requests={request_count} "
        f"btc_dominance_pct={btc_dominance_pct} rows_affected={rows_affected}"
    )
    return RunResult({
        "requests": request_count,
        "rows_affected": rows_affected,
        "duration_seconds": round(perf_counter() - started_at, 3),
        "btc_dominance_pct": btc_dominance_pct,
        **quality_metrics,
    }, tuple(reports))


def run_stablecoin_supply(
    *,
    logger: logging.Logger,
    settings: MarketMetricSettings,
    store: Store,
    client: HttpClient,
    run_date: date,
) -> RunResult:
    started_at = perf_counter()
    row = fetch_stablecoin_supply(logger=logger, client=client, run_date=run_date)
    rows_affected = 0
    quality_metrics: dict[str, MetricValue] = {}
    reports = []
    if row is not None:
        frame = StablecoinSupplyRow.to_frame([row])
        report = check_frame(frame, StablecoinSupplyRow.quality_checks(), logger=logger, table=settings.stablecoin_table)
        reports.append(report)
        quality_metrics = report.to_metrics()
        if report.ok:
            rows_affected = store.upsert(settings.stablecoin_table, frame).rows_affected
    logger.info(f"[stablecoin_supply] date={run_date.isoformat()} rows_affected={rows_affected}")
    return RunResult({
        "rows_affected": rows_affected,
        "duration_seconds": round(perf_counter() - started_at, 3),
        **quality_metrics,
    }, tuple(reports))
