from datetime import date
from unittest.mock import MagicMock

from core.http import HttpError
from pipelines.raw.market_news.config import FeedSpec, MarketNewsSettings
from pipelines.raw.market_news.run import _parse_feed, fetch_market_news, run_market_news
from tests.conftest import MemoryStore

RUN_DATE = date(2024, 6, 1)

_FEED = FeedSpec(
    name="TestFeed",
    url="https://test.com/feed.rss",
    source_type="rss_news",
    reliability_tier=3,
)
SETTINGS = MarketNewsSettings(feeds=(_FEED,))

_RSS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>BTC hits $100k</title>
      <link>https://example.com/article1</link>
      <description>Bitcoin milestone reached</description>
      <pubDate>Sat, 01 Jun 2024 12:00:00 +0000</pubDate>
    </item>
    <item>
      <title>Fed holds rates</title>
      <link>https://example.com/article2</link>
      <pubDate>Sat, 01 Jun 2024 13:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>"""

_ATOM_XML = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>Rate decision</title>
    <link href="https://example.com/fed"/>
    <published>2024-06-01T12:00:00Z</published>
    <summary>Federal Reserve holds steady</summary>
  </entry>
</feed>"""


class TestParseFeed:
    def test_rss_feed(self):
        items = _parse_feed(_RSS_XML)
        assert len(items) == 2
        assert items[0].url == "https://example.com/article1"
        assert items[0].title == "BTC hits $100k"
        assert items[0].source_domain == "example.com"
        assert items[0].published_at is not None

    def test_atom_feed(self):
        items = _parse_feed(_ATOM_XML)
        assert len(items) == 1
        assert items[0].url == "https://example.com/fed"
        assert items[0].title == "Rate decision"
        assert items[0].summary == "Federal Reserve holds steady"

    def test_invalid_xml_returns_empty(self):
        items = _parse_feed("not xml at all <<<")
        assert items == []

    def test_rss_item_without_link_skipped(self):
        xml = """<rss><channel>
          <item><title>No link</title></item>
          <item><title>Has link</title><link>https://x.com/a</link></item>
        </channel></rss>"""
        items = _parse_feed(xml)
        assert len(items) == 1
        assert items[0].url == "https://x.com/a"


class TestFetchMarketNews:
    def test_new_items_written(self, logger):
        client = MagicMock()
        client.get_text.side_effect = [_RSS_XML, "content1", "content2"]

        rows, items_seen = fetch_market_news(
            logger=logger, client=client, settings=SETTINGS, known_urls=set()
        )

        assert items_seen == 2
        assert len(rows) == 2
        assert rows[0].content_raw == "content1"

    def test_known_urls_skipped(self, logger):
        known = {"https://example.com/article1"}
        client = MagicMock()
        client.get_text.side_effect = [_RSS_XML, "content2"]

        rows, items_seen = fetch_market_news(
            logger=logger, client=client, settings=SETTINGS, known_urls=known
        )

        assert items_seen == 2
        assert len(rows) == 1
        assert rows[0].source_url == "https://example.com/article2"

    def test_feed_http_error_continues(self, logger):
        client = MagicMock()
        client.get_text.side_effect = HttpError(503, "test")

        rows, items_seen = fetch_market_news(
            logger=logger, client=client, settings=SETTINGS, known_urls=set()
        )

        assert rows == []
        assert items_seen == 0

    def test_article_fetch_error_row_still_written(self, logger):
        client = MagicMock()
        client.get_text.side_effect = [
            _RSS_XML,
            HttpError(404, "https://example.com/article1"),
            "content2",
        ]

        rows, items_seen = fetch_market_news(
            logger=logger, client=client, settings=SETTINGS, known_urls=set()
        )

        assert len(rows) == 2
        assert rows[0].content_raw is None  # article fetch failed
        assert rows[1].content_raw == "content2"


class TestRunMarketNews:
    def test_rows_written_to_store(self, logger):
        client = MagicMock()
        client.get_text.side_effect = [_RSS_XML, "c1", "c2"]
        store = MemoryStore()

        metrics = run_market_news(
            store=store, logger=logger, run_date=RUN_DATE,
            settings=SETTINGS, client=client,
        )

        assert metrics["rows_affected"] == 2
        assert metrics["items_written"] == 2
        assert SETTINGS.table_name in store._tables

    def test_empty_returns_zero(self, logger):
        client = MagicMock()
        client.get_text.return_value = "<rss><channel></channel></rss>"
        store = MemoryStore()

        metrics = run_market_news(
            store=store, logger=logger, run_date=RUN_DATE,
            settings=SETTINGS, client=client,
        )

        assert metrics["rows_affected"] == 0
        assert metrics["items_seen"] == 0

    def test_metrics_shape(self, logger):
        client = MagicMock()
        client.get_text.return_value = "<rss><channel></channel></rss>"
        store = MemoryStore()

        metrics = run_market_news(
            store=store, logger=logger, run_date=RUN_DATE,
            settings=SETTINGS, client=client,
        )

        assert "rows_affected" in metrics
        assert "items_seen" in metrics
        assert "items_written" in metrics
