from pydantic import BaseModel, ConfigDict


class CotPositioningSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.cot_positioning"
    cftc_base_url: str = "https://publicreporting.cftc.gov"
    # Traders in Financial Futures - Futures Only (Socrata); BTC lives here, not in
    # the disaggregated (physical-commodity) report.
    dataset: str = "gpe5-46if"
    market_filter: str = "BITCOIN -%"
    page_size: int = 1000
    incremental_lookback_days: int = 30
    request_timeout_seconds: float = 30.0


COT_POSITIONING_SETTINGS = CotPositioningSettings()
