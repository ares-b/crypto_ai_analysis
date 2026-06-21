import logging
from datetime import UTC, date, datetime
from time import perf_counter

from core.http import HttpClient, HttpError
from core.storage import Store
from pipelines import MetricValue

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


def run_exchange_flows(
    *,
    logger: logging.Logger,
    settings: ExchangeFlowSettings,
    store: Store,
    client: HttpClient,
    since: date,
    until: date,
) -> dict[str, MetricValue]:
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
        return {"rows": 0, "rows_affected": 0, "duration_seconds": round(perf_counter() - started_at, 3)}
    rows_affected = 0
    if rows:
        rows_affected = store.upsert(settings.table_name, ExchangeFlowRow.to_frame(rows)).rows_affected
    logger.info(
        f"[exchange_flows] {since.isoformat()}–{until.isoformat()} "
        f"rows={len(rows)} affected={rows_affected}"
    )
    return {
        "rows": len(rows),
        "rows_affected": rows_affected,
        "duration_seconds": round(perf_counter() - started_at, 3),
    }
