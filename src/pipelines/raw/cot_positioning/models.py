from datetime import date, datetime
from typing import Any

from core.models import StoreRow


def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(float(value))


class CotPositioningRow(StoreRow):
    report_date: date
    noncommercial_long: int | None
    noncommercial_short: int | None
    open_interest: int | None
    source_updated_at: datetime

    @classmethod
    def from_api_response(cls, item: dict[str, Any], *, source_updated_at: datetime) -> "CotPositioningRow":
        return cls(
            report_date=date.fromisoformat(str(item["report_date_as_yyyy_mm_dd"])[:10]),
            noncommercial_long=_int(item.get("noncomm_positions_long_all")),
            noncommercial_short=_int(item.get("noncomm_positions_short_all")),
            open_interest=_int(item.get("open_interest_all")),
            source_updated_at=source_updated_at,
        )
