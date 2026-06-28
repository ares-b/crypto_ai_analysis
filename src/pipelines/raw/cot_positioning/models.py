from datetime import date, datetime
from typing import Any

from core.iceberg import IcebergRecord
from core.models import Record


def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(float(value))


class CotPositioningRow(
    IcebergRecord,
    table="raw.cot_positioning",
    identity=("report_date",),
    sort=("report_date",),
):
    # CFTC Traders in Financial Futures (TFF), Futures-Only, BTC/CME.
    report_date: date
    open_interest: int | None
    dealer_long: int | None
    dealer_short: int | None
    asset_mgr_long: int | None
    asset_mgr_short: int | None
    lev_money_long: int | None
    lev_money_short: int | None
    source_updated_at: datetime

    @classmethod
    def from_api_response(cls, item: dict[str, Any], *, source_updated_at: datetime) -> "CotPositioningRow":
        return cls(
            report_date=date.fromisoformat(str(item["report_date_as_yyyy_mm_dd"])[:10]),
            open_interest=_int(item.get("open_interest_all")),
            dealer_long=_int(item.get("dealer_positions_long_all")),
            dealer_short=_int(item.get("dealer_positions_short_all")),
            asset_mgr_long=_int(item.get("asset_mgr_positions_long")),
            asset_mgr_short=_int(item.get("asset_mgr_positions_short")),
            lev_money_long=_int(item.get("lev_money_positions_long")),
            lev_money_short=_int(item.get("lev_money_positions_short")),
            source_updated_at=source_updated_at,
        )
