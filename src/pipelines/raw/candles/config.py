from pydantic import BaseModel, ConfigDict


class BinanceCandleSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: str = "BTC"
    counterpart: str = "USDT"
    interval: str
    table_name: str = "raw.candles"

    @property
    def symbol(self) -> str:
        return self.instrument + self.counterpart


DAILY_CANDLES = BinanceCandleSettings(interval="1d")
WEEKLY_CANDLES = BinanceCandleSettings(interval="1w")
FOUR_HOUR_CANDLES = BinanceCandleSettings(interval="4h")
HOURLY_CANDLES = BinanceCandleSettings(interval="1h")
