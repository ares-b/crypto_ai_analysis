from pydantic import BaseModel, ConfigDict


class SentimentSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.sentiment_index"
    dvol_table: str = "raw.deribit_dvol"
    put_call_table: str = "raw.deribit_put_call"
    fear_greed_base_url: str = "https://api.alternative.me"
    deribit_base_url: str = "https://www.deribit.com"
    include_dvol: bool = True
    request_timeout_seconds: float = 20.0
    incremental_lookback_days: int = 7


SENTIMENT_SETTINGS = SentimentSettings()
