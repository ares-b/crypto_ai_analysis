from datetime import UTC, date, datetime
from typing import Any, Mapping

from pydantic import ValidationError

from core.iceberg import IcebergRecord
from core.models import Record


class ExchangeFlowRow(
    IcebergRecord,
    table="raw.exchange_flows",
    identity=("date", "asset", "exchange"),
    partition=("years(date)",),
    sort=("date", "asset"),
):
    date: date
    asset: str
    exchange: str
    reserve: float | None
    reserve_usd: float | None
    inflow: float | None
    outflow: float | None
    netflow: float | None
    source_updated_at: datetime

    @classmethod
    def from_api_response(
        cls,
        data: Mapping[str, Any],
        *,
        asset: str,
        exchange: str,
        source_updated_at: datetime,
    ) -> "ExchangeFlowRow":
        try:
            metric_date = datetime.fromisoformat(str(data["date"])).replace(tzinfo=UTC).date()

            def _opt(key: str) -> float | None:
                v = data.get(key)
                return float(v) if v is not None else None

            return cls(
                date=metric_date,
                asset=asset,
                exchange=exchange,
                reserve=_opt("reserve"),
                reserve_usd=_opt("reserve_usd"),
                inflow=_opt("inflow"),
                outflow=_opt("outflow"),
                netflow=_opt("netflow"),
                source_updated_at=source_updated_at,
            )
        except (KeyError, TypeError, ValueError, ValidationError) as error:
            raise ValueError(f"Invalid CryptoQuant exchange flow payload: {data!r}") from error
