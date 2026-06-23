from datetime import date, datetime
from typing import Any

from core.models import IcebergRow, StoreRow


class MacroSeriesRow(IcebergRow, table="raw.macro_series", identity=("series_id", "date")):
    series_id: str
    date: date
    value: float | None
    source_updated_at: datetime


def build_rows(
    series_id: str,
    observations: list[dict[str, Any]],
    *,
    source_updated_at: datetime,
) -> list[MacroSeriesRow]:
    rows: list[MacroSeriesRow] = []
    for observation in observations:
        raw_value = observation.get("value")
        value = None if raw_value in (None, "", ".") else float(raw_value)
        rows.append(
            MacroSeriesRow(
                series_id=series_id,
                date=date.fromisoformat(observation["date"]),
                value=value,
                source_updated_at=source_updated_at,
            )
        )
    return rows
