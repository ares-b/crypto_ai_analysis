import os

from pydantic import BaseModel, ConfigDict, Field


class ExchangeFlowSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.exchange_flows"
    base_url: str = "https://api.cryptoquant.com/v1"
    api_key: str = Field(default_factory=lambda: os.environ.get("CRYPTOQUANT_API_KEY", ""))
    asset: str = "btc"
    exchange: str = "all_exchange"
    window: str = "day"


EXCHANGE_FLOW_SETTINGS = ExchangeFlowSettings()
