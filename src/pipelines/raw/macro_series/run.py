import logging
from datetime import UTC, date, datetime

from core.http import HttpClient, HttpError
from core.storage import Store
from pipelines import MetricValue

from .config import MacroSeriesSettings
from .models import MacroSeriesRow, build_rows


def fetch_macro_series(
    client: HttpClient,
    *,
    settings: MacroSeriesSettings,
    since: date | None,
) -> list[MacroSeriesRow]:
    now = datetime.now(UTC)
    rows: list[MacroSeriesRow] = []
    params: dict[str, str] = {
        "api_key": settings.fred_api_key,
        "file_type": "json",
        "sort_order": "asc",
    }
    if since is not None:
        params["observation_start"] = since.isoformat()

    for series_id in settings.series_ids:
        payload = client.get_json(
            "/fred/series/observations",
            params={"series_id": series_id, **params},
        )
        observations: list[dict] = payload.get("observations", [])
        rows.extend(build_rows(series_id, observations, source_updated_at=now))

    return rows


def run_macro_series(
    *,
    store: Store,
    logger: logging.Logger,
    run_date: date,
    client: HttpClient,
    settings: MacroSeriesSettings,
    since: date | None,
) -> dict[str, MetricValue]:
    try:
        rows = fetch_macro_series(client, settings=settings, since=since)
    except HttpError as exc:
        logger.warning(f"[macro_series] FRED API error for {run_date.isoformat()}: {exc}")
        return {"rows_affected": 0}
    rows_affected = 0
    if rows:
        rows_affected = store.upsert(settings.table_name, MacroSeriesRow.to_frame(rows)).rows_affected
    logger.info(f"[macro_series] {run_date.isoformat()} series={len(settings.series_ids)} rows={rows_affected}")
    return {"rows_affected": rows_affected}
