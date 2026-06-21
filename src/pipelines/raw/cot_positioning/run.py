import logging
from datetime import UTC, date, datetime, timedelta

from core.http import HttpClient, HttpError
from core.storage import Store
from pipelines import MetricValue

from .config import CotPositioningSettings
from .models import CotPositioningRow

_DATASET = "jun_dea_fut_disagg_pos"
_BTC_FILTER = "market_and_exchange_names LIKE 'BITCOIN%'"


def fetch_cot_positioning(
    client: HttpClient,
    *,
    settings: CotPositioningSettings,
    since: date | None,
) -> list[CotPositioningRow]:
    where = _BTC_FILTER
    if since is not None:
        where += f" AND report_date_as_yyyy_mm_dd >= '{since.isoformat()}'"

    now = datetime.now(UTC)
    rows: list[CotPositioningRow] = []
    offset = 0

    while True:
        payload = client.get_json(
            f"/api/explore/v2.1/catalog/datasets/{_DATASET}/records",
            params={
                "$where": where,
                "$limit": settings.page_size,
                "$offset": offset,
                "$order_by": "report_date_as_yyyy_mm_dd DESC",
            },
        )
        batch = payload.get("results", [])
        rows.extend(CotPositioningRow.from_api_response(item, source_updated_at=now) for item in batch)

        if len(batch) < settings.page_size:
            break
        offset += len(batch)

    return rows


def run_cot_positioning(
    *,
    store: Store,
    logger: logging.Logger,
    run_date: date,
    client: HttpClient,
    settings: CotPositioningSettings,
) -> dict[str, MetricValue]:
    since = run_date - timedelta(days=settings.incremental_lookback_days)
    try:
        rows = fetch_cot_positioning(client, settings=settings, since=since)
    except HttpError as exc:
        logger.warning(f"[cot_positioning] CFTC API error for {run_date.isoformat()}: {exc}")
        return {"rows_affected": 0}
    rows_affected = 0
    if rows:
        rows_affected = store.upsert(settings.table_name, CotPositioningRow.to_frame(rows)).rows_affected
    logger.info(f"[cot_positioning] {run_date.isoformat()} reports={rows_affected}")
    return {"rows_affected": rows_affected}
