from pydantic import BaseModel, ConfigDict

# Community-tier metrics only. SOPR (403) and PriceRealizedUSD (400) require a
# CoinMetrics PRO key; sopr/realized_price_usd columns stay null until then.
COINMETRICS_ONCHAIN_METRICS: tuple[str, ...] = (
    "CapMrktCurUSD",
    "CapMVRVCur",
    "SplyCur",
    "AdrActCnt",
    "HashRate",
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
