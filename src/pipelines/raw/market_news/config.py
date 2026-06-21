from pydantic import BaseModel, ConfigDict


class FeedSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    url: str
    source_type: str
    reliability_tier: int


DEFAULT_FEEDS: tuple[FeedSpec, ...] = (
    FeedSpec(
        name="SEC press releases",
        url="https://www.sec.gov/news/pressreleases.rss",
        source_type="official",
        reliability_tier=1,
    ),
    FeedSpec(
        name="Federal Reserve press",
        url="https://www.federalreserve.gov/feeds/press_all.xml",
        source_type="official",
        reliability_tier=1,
    ),
    FeedSpec(
        name="CoinDesk",
        url="https://www.coindesk.com/arc/outboundfeeds/rss/",
        source_type="rss_news",
        reliability_tier=3,
    ),
    FeedSpec(
        name="Cointelegraph",
        url="https://cointelegraph.com/rss",
        source_type="rss_news",
        reliability_tier=3,
    ),
)


class MarketNewsSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.market_news_items"
    feeds: tuple[FeedSpec, ...] = DEFAULT_FEEDS
    request_timeout_seconds: float = 20.0
    request_interval_seconds: float = 0.5


MARKET_NEWS_SETTINGS = MarketNewsSettings()
