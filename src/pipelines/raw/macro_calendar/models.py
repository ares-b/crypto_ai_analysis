from datetime import UTC, date, datetime
from typing import Any

from core.models import IcebergRow, StoreRow


class MacroCalendarRow(IcebergRow, table="raw.macro_calendar", identity=("event_id",)):
    event_id: str
    event_time_utc: datetime
    title: str
    source_updated_at: datetime


def build_rows(
    release_dates: list[dict[str, Any]],
    *,
    source_updated_at: datetime,
) -> list[MacroCalendarRow]:
    rows: list[MacroCalendarRow] = []
    for entry in release_dates:
        name = str(entry.get("release_name", ""))
        event_date = date.fromisoformat(str(entry["date"])[:10])
        rows.append(
            MacroCalendarRow(
                event_id=f"{entry['release_id']}:{event_date.isoformat()}",
                event_time_utc=datetime(event_date.year, event_date.month, event_date.day, tzinfo=UTC),
                title=name,
                source_updated_at=source_updated_at,
            )
        )
    return rows
