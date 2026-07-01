import logging
from datetime import UTC, date, datetime, timedelta

from core.http import HttpClient, HttpError
from core.quality import QualitySubject, Report, RunResult
from core.storage import Store
from pipelines import MetricValue
from pipelines.quality import check_frame

from .config import SentimentSettings
from .models import DeribitDvolRow, DeribitPutCallRow, SentimentRow, build_dvol_rows, build_rows

_DVOL_BACKFILL_START_MS = int(datetime(2021, 3, 24, tzinfo=UTC).timestamp() * 1000)
_DERIBIT_RESOLUTION = "1D"


def _fetch_fear_greed(client: HttpClient, *, limit: int) -> list[dict]:
    payload = client.get_json("/fng/", params={"limit": limit, "format": "json"})
    return payload.get("data", [])


_DVOL_DAY_MS = 86_400_000
_DVOL_MAX_PAGES = 50  # 50 * 1000 daily candles >> full history


def _dvol_by_date(
    client: HttpClient | None,
    *,
    start_ms: int = _DVOL_BACKFILL_START_MS,
) -> dict[date, float]:
    if client is None:
        return {}
    out: dict[date, float] = {}
    # Deribit returns at most 1000 candles, the most recent within [start, end].
    # Page backward by moving end_timestamp earlier until we reach start_ms.
    cursor_end = int(datetime.now(UTC).timestamp() * 1000)
    for _ in range(_DVOL_MAX_PAGES):
        if cursor_end <= start_ms:
            break
        try:
            payload = client.get_json(
                "/api/v2/public/get_volatility_index_data",
                params={
                    "currency": "BTC",
                    "start_timestamp": start_ms,
                    "end_timestamp": cursor_end,
                    "resolution": _DERIBIT_RESOLUTION,
                },
            )
        except HttpError:
            break
        candles: list[list] = payload.get("result", {}).get("data", [])
        if not candles:
            break
        for candle in candles:
            out[datetime.fromtimestamp(candle[0] / 1000, tz=UTC).date()] = float(candle[4])
        earliest = min(candle[0] for candle in candles)
        if len(candles) < 1000 or earliest <= start_ms:
            break
        cursor_end = earliest - _DVOL_DAY_MS
    return out


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


def sentiment_quality_subjects(*, settings: SentimentSettings) -> list[QualitySubject]:
    return [
        (settings.table_name, SentimentRow.quality_checks()),
        (settings.dvol_table, DeribitDvolRow.quality_checks()),
        (settings.put_call_table, DeribitPutCallRow.quality_checks()),
    ]


def run_sentiment_index(
    *,
    store: Store,
    logger: logging.Logger,
    run_date: date,
    settings: SentimentSettings,
    fear_greed_client: HttpClient,
    deribit_client: HttpClient | None,
) -> RunResult:
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

    quality_metrics: dict[str, MetricValue] = {}
    reports: list[Report] = []
    rows_affected = 0
    if day_rows:
        frame = SentimentRow.to_frame(day_rows)
        report = check_frame(frame, SentimentRow.quality_checks(), logger=logger, table=settings.table_name)
        reports.append(report)
        quality_metrics |= report.to_metrics()
        if report.ok:
            rows_affected = store.upsert(settings.table_name, frame).rows_affected
    dvol_rows_affected = 0
    if day_dvol_rows:
        dvol_frame = DeribitDvolRow.to_frame(day_dvol_rows)
        dvol_report = check_frame(dvol_frame, DeribitDvolRow.quality_checks(), logger=logger, table=settings.dvol_table)
        reports.append(dvol_report)
        quality_metrics |= dvol_report.to_metrics(prefix="quality_dvol")
        if dvol_report.ok:
            dvol_rows_affected = store.upsert(settings.dvol_table, dvol_frame).rows_affected
    put_call_rows_affected = 0
    if put_call_row is not None:
        pc_frame = DeribitPutCallRow.to_frame([put_call_row])
        pc_report = check_frame(pc_frame, DeribitPutCallRow.quality_checks(), logger=logger, table=settings.put_call_table)
        reports.append(pc_report)
        quality_metrics |= pc_report.to_metrics(prefix="quality_put_call")
        if pc_report.ok:
            put_call_rows_affected = store.upsert(settings.put_call_table, pc_frame).rows_affected

    if rows_affected == 0:
        logger.warning(f"[sentiment_index] no reading for {run_date.isoformat()}")
    else:
        logger.info(f"[sentiment_index] {run_date.isoformat()} fear_greed={day_rows[0].fear_greed_value}")
    return RunResult({
        "rows_affected": rows_affected,
        "dvol_rows_affected": dvol_rows_affected,
        "put_call_rows_affected": put_call_rows_affected,
        "available_days": len(rows),
        **quality_metrics,
    }, tuple(reports))
