from pydantic import BaseModel, ConfigDict


class FundingRateSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: str = "BTC"
    counterpart: str = "USDT"
    table_name: str = "raw.funding_rates"

    @property
    def symbol(self) -> str:
        return self.instrument + self.counterpart


class FuturesMetricSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: str = "BTC"
    counterpart: str = "USDT"
    table_name: str = "raw.futures_metrics"
    period: str = "1d"
    contract_type: str = "PERPETUAL"
    premium_index_interval: str = "1d"

    @property
    def symbol(self) -> str:
        return self.instrument + self.counterpart


class LongShortSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: str = "BTC"
    counterpart: str = "USDT"
    table_name: str = "raw.long_short_ratio"
    period: str = "1d"

    @property
    def symbol(self) -> str:
        return self.instrument + self.counterpart


FUNDING_RATES = FundingRateSettings()
DERIVATIVES_METRICS = FuturesMetricSettings()
LONG_SHORT_RATIO = LongShortSettings()
