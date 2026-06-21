from pydantic import BaseModel, ConfigDict


class EtfFlowsSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.etf_flows"
    farside_url: str = "https://farside.co.uk/bitcoin-etf-flow-all-data/"
    request_timeout_seconds: float = 30.0
    incremental_lookback_days: int = 7


ETF_FLOWS_SETTINGS = EtfFlowsSettings()
