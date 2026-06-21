from datetime import UTC, date, datetime
from typing import Any, Mapping

from pydantic import ValidationError

from core.models import StoreRow

from .config import FundingRateSettings, FuturesMetricSettings, LongShortSettings


class RawFundingRate(StoreRow):
    asset: str
    funding_time_ms: int
    funding_rate: float
    mark_price: float | None

    @classmethod
    def from_api_response(cls, data: Mapping[str, Any]) -> "RawFundingRate":
        try:
            mark_price_raw = data.get("markPrice", "")
            return cls(
                asset=str(data["symbol"]),
                funding_time_ms=int(data["fundingTime"]),
                funding_rate=float(data["fundingRate"]),
                mark_price=float(mark_price_raw) if mark_price_raw not in (None, "") else None,
            )
        except (KeyError, TypeError, ValueError, ValidationError) as error:
            raise ValueError(f"Invalid Binance funding-rate payload: {data!r}") from error


class RawOpenInterest(StoreRow):
    asset: str
    timestamp_ms: int
    open_interest: float

    @classmethod
    def from_api_response(cls, data: Mapping[str, Any]) -> "RawOpenInterest":
        try:
            return cls(
                asset=str(data["symbol"]),
                timestamp_ms=int(data["timestamp"]),
                open_interest=float(data["sumOpenInterest"]),
            )
        except (KeyError, TypeError, ValueError, ValidationError) as error:
            raise ValueError(f"Invalid Binance open-interest payload: {data!r}") from error


class RawBasisPoint(StoreRow):
    asset: str
    timestamp_ms: int
    basis: float | None
    basis_rate: float | None

    @classmethod
    def from_api_response(cls, data: Mapping[str, Any]) -> "RawBasisPoint":
        try:
            basis = data.get("basis")
            basis_rate = data.get("basisRate")
            return cls(
                asset=str(data["pair"]),
                timestamp_ms=int(data["timestamp"]),
                basis=float(basis) if basis not in (None, "") else None,
                basis_rate=float(basis_rate) if basis_rate not in (None, "") else None,
            )
        except (KeyError, TypeError, ValueError, ValidationError) as error:
            raise ValueError(f"Invalid Binance basis payload: {data!r}") from error


class RawPremiumIndexKline(StoreRow):
    asset: str
    open_time_ms: int
    close_time_ms: int
    close_premium_index: float | None

    @classmethod
    def from_api_response(cls, asset: str, payload: list[Any]) -> "RawPremiumIndexKline":
        try:
            return cls(
                asset=asset,
                open_time_ms=int(payload[0]),
                close_time_ms=int(payload[6]),
                close_premium_index=float(payload[4]) if payload[4] is not None else None,
            )
        except (IndexError, TypeError, ValueError, ValidationError) as error:
            raise ValueError(f"Invalid Binance premium-index kline payload: {payload!r}") from error


class FundingRateRow(StoreRow):
    instrument: str
    counterpart: str
    funding_time: datetime
    funding_rate: float
    mark_price: float | None

    @property
    def funding_time_ms(self) -> int:
        return int(self.funding_time.timestamp() * 1000)

    @classmethod
    def from_raw(cls, raw: RawFundingRate, *, settings: FundingRateSettings) -> "FundingRateRow":
        return cls(
            instrument=settings.instrument,
            counterpart=settings.counterpart,
            funding_time=datetime.fromtimestamp(raw.funding_time_ms / 1000, tz=UTC),
            funding_rate=raw.funding_rate,
            mark_price=raw.mark_price,
        )


class FuturesMetricRow(StoreRow):
    instrument: str
    counterpart: str
    date: date
    open_interest: float | None
    basis: float | None
    premium_index: float | None

    @classmethod
    def from_sources(
        cls,
        *,
        settings: FuturesMetricSettings,
        metric_date: date,
        open_interest: RawOpenInterest | None,
        basis_point: RawBasisPoint | None,
        premium_index_kline: RawPremiumIndexKline | None,
    ) -> "FuturesMetricRow":
        return cls(
            instrument=settings.instrument,
            counterpart=settings.counterpart,
            date=metric_date,
            open_interest=open_interest.open_interest if open_interest is not None else None,
            basis=basis_point.basis if basis_point is not None else None,
            premium_index=(
                premium_index_kline.close_premium_index
                if premium_index_kline is not None
                else None
            ),
        )


class LongShortRatioRow(StoreRow):
    instrument: str
    counterpart: str
    date: date
    long_short_ratio: float
    long_account_pct: float
    short_account_pct: float
    source_updated_at: datetime

    @classmethod
    def from_api_response(cls, data: Mapping[str, Any], *, settings: LongShortSettings) -> "LongShortRatioRow":
        try:
            ts_ms = int(data["timestamp"])
            return cls(
                instrument=settings.instrument,
                counterpart=settings.counterpart,
                date=datetime.fromtimestamp(ts_ms / 1000, tz=UTC).date(),
                long_short_ratio=float(data["longShortRatio"]),
                long_account_pct=float(data["longAccount"]),
                short_account_pct=float(data["shortAccount"]),
                source_updated_at=datetime.now(UTC),
            )
        except (KeyError, TypeError, ValueError, ValidationError) as error:
            raise ValueError(f"Invalid Binance long/short ratio payload: {data!r}") from error
