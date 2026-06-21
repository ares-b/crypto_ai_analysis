from pydantic import BaseModel, ConfigDict

COINMETRICS_ONCHAIN_METRICS: tuple[str, ...] = (
    "CapMrktCurUSD",
    "CapMVRVCur",
    "SplyCur",
    "AdrActCnt",
    "HashRate",
    "SOPR",
    "PriceRealizedUSD",
)


class OnchainSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: str = "BTC"
    counterpart: str = "USD"
    onchain_asset: str = "btc"
    metrics: tuple[str, ...] = COINMETRICS_ONCHAIN_METRICS
    table_name: str = "raw.onchain_metrics"
    blockchain_charts_table: str = "raw.blockchain_charts"
    coinmetrics_base_url: str = "https://community-api.coinmetrics.io"
    blockchain_base_url: str = "https://api.blockchain.info"


ONCHAIN_SETTINGS = OnchainSettings()
