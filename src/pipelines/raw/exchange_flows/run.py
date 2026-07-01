import logging
from datetime import UTC, date, datetime
from time import perf_counter

from core.http import HttpClient, HttpError
from core.quality import QualitySubject, RunResult
from core.storage import Store
from pipelines import MetricValue
from pipelines.quality import check_frame

from .config import ExchangeFlowSettings
from .models import ExchangeFlowRow


def fetch_exchange_flows(
    *,
    client: HttpClient,
    settings: ExchangeFlowSettings,
    since: date,
    until: date,
) -> list[ExchangeFlowRow]:
    source_updated_at = datetime.now(UTC)
    payload = client.get_json(
        f"/{settings.asset}/exchange-flows/reserve",
        params={
            "exchange": settings.exchange,
            "window": settings.window,
            "from_time": since.isoformat(),
            "to_time": until.isoformat(),
            "limit": 1000,
        },
    )
    data: list[dict] = payload.get("result", {}).get("data", [])
    return [
        ExchangeFlowRow.from_api_response(
            item,
            asset=settings.asset,
            exchange=settings.exchange,
            source_updated_at=source_updated_at,
        )
        for item in data
    ]


def exchange_flows_quality_subjects(*, settings: ExchangeFlowSettings) -> list[QualitySubject]:
    return [(settings.table_name, ExchangeFlowRow.quality_checks())]


def run_exchange_flows(
    *,
    logger: logging.Logger,
    settings: ExchangeFlowSettings,
    store: Store,
    client: HttpClient,
    since: date,
    until: date,
) -> RunResult:
    started_at = perf_counter()
    try:
        rows = fetch_exchange_flows(
            client=client,
            settings=settings,
            since=since,
            until=until,
        )
    except HttpError as exc:
        logger.warning(f"[exchange_flows] CryptoQuant error: {exc}")
        return RunResult({"rows": 0, "rows_affected": 0, "duration_seconds": round(perf_counter() - started_at, 3)})
    rows_affected = 0
    quality_metrics: dict[str, MetricValue] = {}
    reports = []
    if rows:
        frame = ExchangeFlowRow.to_frame(rows)
        report = check_frame(frame, ExchangeFlowRow.quality_checks(), logger=logger, table=settings.table_name)
        reports.append(report)
        quality_metrics = report.to_metrics()
        if report.ok:
            rows_affected = store.upsert(settings.table_name, frame).rows_affected
    logger.info(
        f"[exchange_flows] {since.isoformat()}–{until.isoformat()} "
        f"rows={len(rows)} affected={rows_affected}"
    )
    return RunResult({
        "rows": len(rows),
        "rows_affected": rows_affected,
        "duration_seconds": round(perf_counter() - started_at, 3),
        **quality_metrics,
    }, tuple(reports))
