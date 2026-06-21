from datetime import UTC, date, datetime
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, ValidationError

from core.models import StoreRow

MINER_REVENUE_CHART = "miners-revenue"
TRANSACTION_FEES_CHART = "transaction-fees-usd"


class RawOnchainMetric(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset: str
    date: date
    market_cap_usd: float | None
    mvrv: float | None
    supply: float | None
    active_addresses: int | None
    hash_rate: float | None
    sopr: float | None
    realized_price_usd: float | None

    @classmethod
    def from_api_response(cls, data: Mapping[str, Any]) -> "RawOnchainMetric":
        try:
            timestamp = datetime.fromisoformat(str(data["time"]).replace("Z", "+00:00"))

            def _optional_float(key: str) -> float | None:
                value = data.get(key)
                return float(value) if value not in (None, "") else None

            def _optional_int(key: str) -> int | None:
                value = data.get(key)
                return int(float(value)) if value not in (None, "") else None

            return cls(
                asset=str(data["asset"]),
                date=timestamp.astimezone(UTC).date(),
                market_cap_usd=_optional_float("CapMrktCurUSD"),
                mvrv=_optional_float("CapMVRVCur"),
                supply=_optional_float("SplyCur"),
                active_addresses=_optional_int("AdrActCnt"),
                hash_rate=_optional_float("HashRate"),
                sopr=_optional_float("SOPR"),
                realized_price_usd=_optional_float("PriceRealizedUSD"),
            )
        except (KeyError, TypeError, ValueError, ValidationError) as error:
            raise ValueError(f"Invalid CoinMetrics on-chain metric payload: {data!r}") from error


class OnchainMetricRow(StoreRow):
    instrument: str
    counterpart: str
    date: date
    market_cap_usd: float | None
    mvrv: float | None
    supply: float | None
    active_addresses: int | None
    hash_rate: float | None
    sopr: float | None
    realized_price_usd: float | None
    source_updated_at: datetime

    @classmethod
    def from_raw(
        cls,
        raw: RawOnchainMetric,
        *,
        instrument: str,
        counterpart: str,
        source_updated_at: datetime,
    ) -> "OnchainMetricRow":
        return cls(
            instrument=instrument,
            counterpart=counterpart,
            date=raw.date,
            market_cap_usd=raw.market_cap_usd,
            mvrv=raw.mvrv,
            supply=raw.supply,
            active_addresses=raw.active_addresses,
            hash_rate=raw.hash_rate,
            sopr=raw.sopr,
            realized_price_usd=raw.realized_price_usd,
            source_updated_at=source_updated_at.astimezone(UTC),
        )


class BlockchainChartRow(StoreRow):
    chart_name: str
    date: date
    value: float | None
    source_updated_at: datetime
