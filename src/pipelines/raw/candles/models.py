from datetime import UTC, datetime
from typing import Any, ClassVar, Self, Sequence

from pydantic import ValidationError

from core.models import StoreRow
from core.storage import IcebergRow
from .config import BinanceCandleSettings


class RawKline(StoreRow):
    EXPECTED_API_RESPONSE_LENGTH: ClassVar[int] = 12

    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time_ms: int
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float

    @classmethod
    def from_api_response(cls, data: Sequence[Any]) -> Self:
        if len(data) != cls.EXPECTED_API_RESPONSE_LENGTH:
            raise ValueError(
                f"Unexpected Binance kline payload length: "
                f"expected={cls.EXPECTED_API_RESPONSE_LENGTH} actual={len(data)}"
            )
        try:
            return cls(
                open_time_ms=int(data[0]),
                open=float(data[1]),
                high=float(data[2]),
                low=float(data[3]),
                close=float(data[4]),
                volume=float(data[5]),
                close_time_ms=int(data[6]),
                quote_asset_volume=float(data[7]),
                number_of_trades=int(data[8]),
                taker_buy_base_asset_volume=float(data[9]),
                taker_buy_quote_asset_volume=float(data[10]),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise ValueError(f"Invalid Binance kline payload: {data!r}") from error


class BinanceCandleRow(
    IcebergRow,
    table="raw.candles",
    identity=("instrument", "counterpart", "interval", "open_time"),
):
    instrument: str
    counterpart: str
    interval: str
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float

    @staticmethod
    def _ms_to_datetime(ms: int) -> datetime:
        return datetime.fromtimestamp(ms / 1000, tz=UTC)

    @property
    def close_time_ms(self) -> int:
        return int(self.close_time.timestamp() * 1000)

    @classmethod
    def from_kline(cls, *, settings: BinanceCandleSettings, kline: RawKline) -> Self:
        return cls(
            instrument=settings.instrument,
            counterpart=settings.counterpart,
            interval=settings.interval,
            open_time=cls._ms_to_datetime(kline.open_time_ms),
            close_time=cls._ms_to_datetime(kline.close_time_ms),
            open=kline.open,
            high=kline.high,
            low=kline.low,
            close=kline.close,
            volume=kline.volume,
            quote_asset_volume=kline.quote_asset_volume,
            number_of_trades=kline.number_of_trades,
            taker_buy_base_asset_volume=kline.taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume=kline.taker_buy_quote_asset_volume,
        )
