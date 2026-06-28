from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import polars as pl
from pydantic import ValidationError

from core.iceberg import IcebergRecord
from core.models import Record
from core.quality import Check, expression, from_spec


@dataclass(frozen=True)
class RawMarketMetric:
    date: date
    btc_dominance_pct: float
    source_updated_at: datetime

    @classmethod
    def from_coingecko_global(
        cls,
        *,
        metric_date: date,
        snapshot_at: datetime,
        btc_dominance_pct: float,
    ) -> "RawMarketMetric":
        return cls(
            date=metric_date,
            btc_dominance_pct=btc_dominance_pct,
            source_updated_at=snapshot_at.astimezone(UTC),
        )


@dataclass(frozen=True)
class RawHistoricalBtcDominancePoint:
    date: date
    btc_dominance_pct: float

    @classmethod
    def from_api_response(cls, payload: list[Any]) -> "RawHistoricalBtcDominancePoint":
        try:
            timestamp_ms = int(payload[0])
            metric_date = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).date()
            return cls(date=metric_date, btc_dominance_pct=float(payload[1]))
        except (IndexError, TypeError, ValueError) as error:
            raise ValueError(f"Invalid CoinGecko historical BTC dominance payload: {payload!r}") from error


class MarketMetricRow(
    IcebergRecord,
    table="raw.market_metrics",
    identity=("date",),
    sort=("date",),
):
    date: date
    btc_dominance_pct: float
    source_updated_at: datetime

    @classmethod
    def quality_checks(cls) -> list[Check]:
        return from_spec(cls, extra=[
            expression("dominance_in_(0,100]",
                       (pl.col("btc_dominance_pct") > 0) & (pl.col("btc_dominance_pct") <= 100)),
        ])

    @classmethod
    def from_raw(cls, raw: RawMarketMetric) -> "MarketMetricRow":
        return cls(
            date=raw.date,
            btc_dominance_pct=raw.btc_dominance_pct,
            source_updated_at=raw.source_updated_at.astimezone(UTC),
        )

    @classmethod
    def from_historical_point(cls, point: RawHistoricalBtcDominancePoint) -> "MarketMetricRow":
        return cls(
            date=point.date,
            btc_dominance_pct=point.btc_dominance_pct,
            source_updated_at=datetime.combine(point.date, datetime.min.time(), tzinfo=UTC),
        )


class StablecoinSupplyRow(
    IcebergRecord,
    table="raw.stablecoin_supply",
    identity=("date",),
    sort=("date",),
):
    date: date
    usdt_market_cap_usd: float | None
    usdc_market_cap_usd: float | None
    total_stablecoin_market_cap_usd: float | None
    source_updated_at: datetime
