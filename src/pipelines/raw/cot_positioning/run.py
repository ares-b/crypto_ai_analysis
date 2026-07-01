import logging
from datetime import UTC, date, datetime, timedelta

from core.http import HttpClient, HttpError
from core.quality import QualitySubject, RunResult
from core.storage import Store
from pipelines import MetricValue
from pipelines.quality import check_frame

from .config import CotPositioningSettings
from .models import CotPositioningRow


def fetch_cot_positioning(
    client: HttpClient,
    *,
    settings: CotPositioningSettings,
    since: date | None,
) -> list[CotPositioningRow]:
    where = f"market_and_exchange_names like '{settings.market_filter}'"
    if since is not None:
        where += f" AND report_date_as_yyyy_mm_dd >= '{since.isoformat()}'"

    now = datetime.now(UTC)
    rows: list[CotPositioningRow] = []
    offset = 0

    # Socrata SODA: /resource/{id}.json returns a JSON array directly.
    while True:
        batch = client.get_json(
            f"/resource/{settings.dataset}.json",
            params={
                "$where": where,
                "$limit": settings.page_size,
                "$offset": offset,
                "$order": "report_date_as_yyyy_mm_dd DESC",
            },
        )
        rows.extend(CotPositioningRow.from_api_response(item, source_updated_at=now) for item in batch)

        if len(batch) < settings.page_size:
            break
        offset += len(batch)

    return rows


def cot_positioning_quality_subjects(*, settings: CotPositioningSettings) -> list[QualitySubject]:
    return [(settings.table_name, CotPositioningRow.quality_checks())]


def run_cot_positioning(
    *,
    store: Store,
    logger: logging.Logger,
    run_date: date,
    client: HttpClient,
    settings: CotPositioningSettings,
) -> RunResult:
    since = run_date - timedelta(days=settings.incremental_lookback_days)
    try:
        rows = fetch_cot_positioning(client, settings=settings, since=since)
    except HttpError as exc:
        logger.warning(f"[cot_positioning] CFTC API error for {run_date.isoformat()}: {exc}")
        return RunResult({"rows_affected": 0})
    rows_affected = 0
    quality_metrics: dict[str, MetricValue] = {}
    reports = []
    if rows:
        frame = CotPositioningRow.to_frame(rows)
        report = check_frame(frame, CotPositioningRow.quality_checks(), logger=logger, table=settings.table_name)
        reports.append(report)
        quality_metrics = report.to_metrics()
        if report.ok:
            rows_affected = store.upsert(settings.table_name, frame).rows_affected
    logger.info(f"[cot_positioning] {run_date.isoformat()} reports={rows_affected}")
    return RunResult({"rows_affected": rows_affected, **quality_metrics}, tuple(reports))
