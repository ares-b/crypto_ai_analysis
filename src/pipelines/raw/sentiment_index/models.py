from datetime import UTC, date, datetime
from typing import Any

from core.models import IcebergRow, StoreRow


class SentimentRow(IcebergRow, table="raw.sentiment_index", identity=("date",)):
    date: date
    fear_greed_value: int | None
    fear_greed_label: str | None
    source_updated_at: datetime


def build_rows(
    fear_greed_items: list[dict[str, Any]],
    *,
    source_updated_at: datetime,
) -> list[SentimentRow]:
    rows: list[SentimentRow] = []
    for item in fear_greed_items:
        item_date = datetime.fromtimestamp(int(item["timestamp"]), tz=UTC).date()
        rows.append(
            SentimentRow(
                date=item_date,
                fear_greed_value=int(item["value"]),
                fear_greed_label=item.get("value_classification"),
                source_updated_at=source_updated_at,
            )
        )
    rows.sort(key=lambda row: row.date)
    return rows


class DeribitDvolRow(IcebergRow, table="raw.deribit_dvol", identity=("date",)):
    date: date
    dvol: float | None
    source_updated_at: datetime


def build_dvol_rows(dvol_by_date: dict[date, float], *, source_updated_at: datetime) -> list[DeribitDvolRow]:
    return [
        DeribitDvolRow(date=item_date, dvol=value, source_updated_at=source_updated_at)
        for item_date, value in sorted(dvol_by_date.items())
    ]


class DeribitPutCallRow(IcebergRow, table="raw.deribit_put_call", identity=("date",)):
    date: date
    put_oi: float | None
    call_oi: float | None
    put_call_ratio: float | None
    source_updated_at: datetime
