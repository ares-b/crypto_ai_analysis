from dagster import ConfigurableResource, EnvVar

from binance.client import Client
from core.http import HttpClient
from core.storage.iceberg import IcebergStore
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
