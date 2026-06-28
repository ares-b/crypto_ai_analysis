import logging
from datetime import UTC, date, datetime

from core.http import HttpClient, HttpError
from core.storage import Store
from pipelines import MetricValue
from pipelines.quality import check_frame

from .config import MacroCalendarSettings
from .models import MacroCalendarRow, build_rows

_LIMIT = 1000


def fetch_macro_calendar(
    client: HttpClient,
    *,
    settings: MacroCalendarSettings,
    since: date | None,
) -> list[MacroCalendarRow]:
    source_updated_at = datetime.now(UTC)
    rows: list[MacroCalendarRow] = []

    # Fetch each curated release separately via /fred/release/dates. A bad/unknown
    # release_id is skipped rather than aborting the whole calendar.
    for release_id, release_name in settings.releases:
        offset = 0
        while True:
            params: dict[str, str | int] = {
                "api_key": settings.fred_api_key,
                "file_type": "json",
                "sort_order": "asc",
                "limit": _LIMIT,
                "offset": offset,
                "release_id": release_id,
            }
            if since is not None:
                params["realtime_start"] = since.isoformat()
            try:
                payload = client.get_json("/fred/release/dates", params=params)
            except HttpError:
                break
            batch: list[dict] = payload.get("release_dates", [])
            for entry in batch:
                entry["release_id"] = release_id
                entry["release_name"] = release_name
            rows.extend(build_rows(batch, source_updated_at=source_updated_at))

            if len(batch) < _LIMIT:
                break
            offset += len(batch)

    return rows


def run_macro_calendar(
    *,
    store: Store,
    logger: logging.Logger,
    run_date: date,
    client: HttpClient,
    settings: MacroCalendarSettings,
    since: date | None,
) -> dict[str, MetricValue]:
    try:
        rows = fetch_macro_calendar(client, settings=settings, since=since)
    except HttpError as exc:
        logger.warning(f"[macro_calendar] FRED API error for {run_date.isoformat()}: {exc}")
        return {"rows_affected": 0}
    rows_affected = 0
    quality_metrics: dict[str, MetricValue] = {}
    if rows:
        frame = MacroCalendarRow.to_frame(rows)
        report = check_frame(frame, MacroCalendarRow.quality_checks(), logger=logger, table=settings.table_name)
        quality_metrics = report.to_metrics()
        rows_affected = store.upsert(settings.table_name, frame).rows_affected
    logger.info(f"[macro_calendar] {run_date.isoformat()} events={rows_affected}")
    return {"rows_affected": rows_affected, **quality_metrics}
