from pydantic import BaseModel, ConfigDict


class MarketMetricSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.market_metrics"
    stablecoin_table: str = "raw.stablecoin_supply"
    coingecko_base_url: str = "https://api.coingecko.com"


MARKET_METRIC_SETTINGS = MarketMetricSettings()
