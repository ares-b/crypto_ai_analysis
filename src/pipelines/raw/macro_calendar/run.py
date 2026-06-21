import logging
from datetime import UTC, date, datetime

from core.http import HttpClient, HttpError
from core.storage import Store
from pipelines import MetricValue

from .config import MacroCalendarSettings
from .models import MacroCalendarRow, build_rows

_LIMIT = 1000


def fetch_macro_calendar(
    client: HttpClient,
    *,
    settings: MacroCalendarSettings,
    since: date | None,
) -> list[MacroCalendarRow]:
    params: dict[str, str | int] = {
        "api_key": settings.fred_api_key,
        "file_type": "json",
        "sort_order": "asc",
        "limit": _LIMIT,
    }
    if since is not None:
        params["realtime_start"] = since.isoformat()

    source_updated_at = datetime.now(UTC)
    rows: list[MacroCalendarRow] = []
    offset = 0

    while True:
        params["offset"] = offset
        payload = client.get_json("/fred/releases/dates", params=params)
        batch: list[dict] = payload.get("release_dates", [])
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
    if rows:
        rows_affected = store.upsert(settings.table_name, MacroCalendarRow.to_frame(rows)).rows_affected
    logger.info(f"[macro_calendar] {run_date.isoformat()} events={rows_affected}")
    return {"rows_affected": rows_affected}
