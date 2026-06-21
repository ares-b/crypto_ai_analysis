import logging
from datetime import UTC, date, datetime, timedelta

from core.http import HttpClient, HttpError
from core.storage import Store
from pipelines import MetricValue

from .config import SentimentSettings
from .models import DeribitDvolRow, DeribitPutCallRow, SentimentRow, build_dvol_rows, build_rows

_DVOL_BACKFILL_START_MS = int(datetime(2021, 3, 24, tzinfo=UTC).timestamp() * 1000)
_DERIBIT_RESOLUTION = "1D"


def _fetch_fear_greed(client: HttpClient, *, limit: int) -> list[dict]:
    payload = client.get_json("/fng/", params={"limit": limit, "format": "json"})
    return payload.get("data", [])


def _dvol_by_date(
    client: HttpClient | None,
    *,
    start_ms: int = _DVOL_BACKFILL_START_MS,
) -> dict[date, float]:
    if client is None:
        return {}
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    try:
        payload = client.get_json(
            "/api/v2/public/get_volatility_index_data",
            params={
                "currency": "BTC",
                "start_timestamp": start_ms,
                "end_timestamp": now_ms,
                "resolution": _DERIBIT_RESOLUTION,
            },
        )
    except HttpError:
        return {}
    candles: list[list] = payload.get("result", {}).get("data", [])
    return {
        datetime.fromtimestamp(candle[0] / 1000, tz=UTC).date(): float(candle[4])
        for candle in candles
    }


def _fetch_put_call_row(
    client: HttpClient | None,
    *,
    run_date: date,
    source_updated_at: datetime,
) -> DeribitPutCallRow | None:
    if client is None:
        return None
    try:
        payload = client.get_json(
            "/api/v2/public/get_book_summary_by_currency",
            params={"currency": "BTC", "kind": "option"},
        )
    except HttpError:
        return None
    data: list[dict] = payload.get("result", [])
    put_oi = sum(item.get("open_interest", 0) for item in data if "-P" in item.get("instrument_name", ""))
    call_oi = sum(item.get("open_interest", 0) for item in data if "-C" in item.get("instrument_name", ""))
    ratio = put_oi / call_oi if call_oi > 0 else None
    return DeribitPutCallRow(
        date=run_date,
        put_oi=put_oi or None,
        call_oi=call_oi or None,
        put_call_ratio=ratio,
        source_updated_at=source_updated_at,
    )


def run_sentiment_index(
    *,
    store: Store,
    logger: logging.Logger,
    run_date: date,
    settings: SentimentSettings,
    fear_greed_client: HttpClient,
    deribit_client: HttpClient | None,
) -> dict[str, MetricValue]:
    source_updated_at = datetime.now(UTC)

    try:
        items = _fetch_fear_greed(fear_greed_client, limit=settings.incremental_lookback_days)
    except HttpError as exc:
        logger.warning(f"[sentiment_index] Alternative.me error for {run_date.isoformat()}: {exc}")
        items = []

    rows = build_rows(items, source_updated_at=source_updated_at)
    dvol_start_ms = int(
        datetime.combine(
            run_date - timedelta(days=settings.incremental_lookback_days),
            datetime.min.time(),
            tzinfo=UTC,
        ).timestamp()
        * 1000
    )
    dvol_rows = build_dvol_rows(
        _dvol_by_date(deribit_client, start_ms=dvol_start_ms),
        source_updated_at=source_updated_at,
    )
    put_call_row = _fetch_put_call_row(deribit_client, run_date=run_date, source_updated_at=source_updated_at)

    day_rows = [row for row in rows if row.date == run_date]
    day_dvol_rows = [row for row in dvol_rows if row.date == run_date]

    rows_affected = 0
    if day_rows:
        rows_affected = store.upsert(settings.table_name, SentimentRow.to_frame(day_rows)).rows_affected
    dvol_rows_affected = 0
    if day_dvol_rows:
        dvol_rows_affected = store.upsert(
            settings.dvol_table, DeribitDvolRow.to_frame(day_dvol_rows)
        ).rows_affected
    put_call_rows_affected = 0
    if put_call_row is not None:
        put_call_rows_affected = store.upsert(
            settings.put_call_table, DeribitPutCallRow.to_frame([put_call_row])
        ).rows_affected

    if rows_affected == 0:
        logger.warning(f"[sentiment_index] no reading for {run_date.isoformat()}")
    else:
        logger.info(f"[sentiment_index] {run_date.isoformat()} fear_greed={day_rows[0].fear_greed_value}")
    return {
        "rows_affected": rows_affected,
        "dvol_rows_affected": dvol_rows_affected,
        "put_call_rows_affected": put_call_rows_affected,
        "available_days": len(rows),
    }
