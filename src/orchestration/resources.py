from dagster import ConfigurableResource, EnvVar

from binance.client import Client
from core.http import HttpClient
from core.storage.iceberg import IcebergStore
from pipelines.raw.cot_positioning.config import COT_POSITIONING_SETTINGS
from pipelines.raw.etf_flows.config import ETF_FLOWS_SETTINGS
from pipelines.raw.macro_calendar.config import MACRO_CALENDAR_SETTINGS
from pipelines.raw.market_metrics.config import MARKET_METRIC_SETTINGS
from pipelines.raw.onchain_metrics.config import ONCHAIN_SETTINGS
from pipelines.raw.sentiment_index.config import SENTIMENT_SETTINGS
from schemas import ALL_SPECS


class IcebergStoreResource(ConfigurableResource):
    def create(self) -> IcebergStore:
        return IcebergStore.from_env(ALL_SPECS)


class BinanceClientResource(ConfigurableResource):
    def create(self) -> Client:
        return Client()


class HttpClientResource(ConfigurableResource):
    base_url: str = ""
    timeout: float = 30.0

    def create(self) -> HttpClient:
        return HttpClient(self.base_url, timeout=self.timeout)


class CryptoQuantClientResource(ConfigurableResource):
    api_key: str = EnvVar("CRYPTOQUANT_API_KEY")
    base_url: str = "https://api.cryptoquant.com/v1"
    timeout: float = 30.0

    def create(self) -> HttpClient:
        return HttpClient(
            self.base_url,
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self.timeout,
        )


ALL_RESOURCES: dict = {
    "iceberg_store": IcebergStoreResource(),
    "binance_client": BinanceClientResource(),
    "cryptoquant_client": CryptoQuantClientResource(),
    "cftc_client": HttpClientResource(base_url=COT_POSITIONING_SETTINGS.cftc_base_url),
    "farside_client": HttpClientResource(base_url=ETF_FLOWS_SETTINGS.farside_url),
    "fred_client": HttpClientResource(base_url=MACRO_CALENDAR_SETTINGS.fred_base_url),
    "coingecko_client": HttpClientResource(base_url=MARKET_METRIC_SETTINGS.coingecko_base_url),
    "market_news_client": HttpClientResource(),
    "coinmetrics_client": HttpClientResource(base_url=ONCHAIN_SETTINGS.coinmetrics_base_url),
    "blockchain_client": HttpClientResource(base_url=ONCHAIN_SETTINGS.blockchain_base_url),
    "fear_greed_client": HttpClientResource(base_url=SENTIMENT_SETTINGS.fear_greed_base_url),
    "deribit_client": HttpClientResource(base_url=SENTIMENT_SETTINGS.deribit_base_url),
}
