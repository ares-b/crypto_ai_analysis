import logging
from datetime import UTC, date, datetime
from time import perf_counter

from core.http import HttpClient, HttpError
from core.storage import Store
from pipelines import MetricValue

from .config import OnchainSettings
from .models import (
    MINER_REVENUE_CHART,
    TRANSACTION_FEES_CHART,
    BlockchainChartRow,
    OnchainMetricRow,
    RawOnchainMetric,
)


def _fetch_onchain_rows(
    client: HttpClient,
    *,
    settings: OnchainSettings,
    run_date: date,
    source_updated_at: datetime,
) -> list[OnchainMetricRow]:
    date_str = run_date.isoformat()
    payload = client.get_json(
        "/v4/timeseries/asset-metrics",
        params={
            "assets": settings.onchain_asset,
            "metrics": ",".join(settings.metrics),
            "start_time": date_str,
            "end_time": date_str,
            "frequency": "1d",
        },
    )
    return [
        OnchainMetricRow.from_raw(
            RawOnchainMetric.from_api_response(item),
            instrument=settings.instrument,
            counterpart=settings.counterpart,
            source_updated_at=source_updated_at,
        )
        for item in payload.get("data", [])
    ]


def _fetch_blockchain_rows(
    client: HttpClient,
    *,
    logger: logging.Logger,
    metric_date: date,
    source_updated_at: datetime,
) -> list[BlockchainChartRow]:
    date_str = metric_date.isoformat()
    rows: list[BlockchainChartRow] = []
    for chart_name, endpoint in (
        (MINER_REVENUE_CHART, "/charts/miners-revenue"),
        (TRANSACTION_FEES_CHART, "/charts/transaction-fees-usd"),
    ):
        try:
            payload = client.get_json(
                endpoint,
                params={"format": "json", "start": date_str, "end": date_str},
            )
            values = payload.get("values", [])
            value = float(values[0]["y"]) if values else None
        except (HttpError, KeyError, IndexError, TypeError, ValueError) as exc:
            logger.warning(f"[onchain_metrics] Blockchain.info {chart_name} unavailable: {exc!r}")
            value = None
        rows.append(
            BlockchainChartRow(
                chart_name=chart_name,
                date=metric_date,
                value=value,
                source_updated_at=source_updated_at,
            )
        )
    return rows


def run_onchain_metrics(
    *,
    logger: logging.Logger,
    settings: OnchainSettings,
    store: Store,
    coinmetrics_client: HttpClient,
    blockchain_client: HttpClient,
    run_date: date,
) -> dict[str, MetricValue]:
    started_at = perf_counter()
    source_updated_at = datetime.now(tz=UTC)

    try:
        rows = _fetch_onchain_rows(
            coinmetrics_client, settings=settings, run_date=run_date, source_updated_at=source_updated_at
        )
    except HttpError as exc:
        logger.warning(f"[onchain_metrics] CoinMetrics error for date={run_date.isoformat()}: {exc!r}")
        rows = []

    rows_affected = 0
    latest_source_date: date | None = None
    if rows:
        rows_affected = store.upsert(settings.table_name, OnchainMetricRow.to_frame(rows)).rows_affected
        latest_source_date = max(row.date for row in rows)

    blockchain_rows = _fetch_blockchain_rows(
        blockchain_client, logger=logger, metric_date=run_date, source_updated_at=source_updated_at
    )
    if blockchain_rows:
        store.upsert(settings.blockchain_charts_table, BlockchainChartRow.to_frame(blockchain_rows))

    logger.info(
        f"[onchain_metrics] {settings.instrument}/{settings.counterpart}: date={run_date.isoformat()} "
        f"rows={len(rows)} rows_affected={rows_affected}"
    )
    return {
        "rows": len(rows),
        "rows_affected": rows_affected,
        "blockchain_requests": len(blockchain_rows),
        "duration_seconds": round(perf_counter() - started_at, 3),
        "latest_source_date": latest_source_date.isoformat() if latest_source_date is not None else None,
    }
