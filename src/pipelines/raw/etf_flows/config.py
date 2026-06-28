from pydantic import BaseModel, ConfigDict

# Farside is behind Cloudflare and 403s non-browser clients.
_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}


class EtfFlowsSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.etf_flows"
    farside_url: str = "https://farside.co.uk/bitcoin-etf-flow-all-data/"
    request_timeout_seconds: float = 30.0
    incremental_lookback_days: int = 7
    request_headers: dict[str, str] = _BROWSER_HEADERS


ETF_FLOWS_SETTINGS = EtfFlowsSettings()
