from datetime import UTC, datetime

from pipelines.raw.market_news.models import MarketNewsItemRow, RawNewsItem, content_hash

_NOW = datetime(2024, 6, 1, tzinfo=UTC)

_RAW = RawNewsItem(
    url="https://example.com/article",
    title="BTC hits new high",
    summary="Bitcoin reaches $100k",
    source_domain="example.com",
    published_at=_NOW,
)


class TestContentHash:
    def test_consistent_for_same_inputs(self):
        h1 = content_hash("https://example.com", "Title", "Summary")
        h2 = content_hash("https://example.com", "Title", "Summary")
        assert h1 == h2

    def test_different_inputs_differ(self):
        h1 = content_hash("https://a.com", "T1", None)
        h2 = content_hash("https://b.com", "T1", None)
        assert h1 != h2

    def test_none_summary_does_not_raise(self):
        h = content_hash("https://example.com", "Title", None)
        assert isinstance(h, str)
        assert len(h) == 64  # sha256 hex


class TestMarketNewsItemRow:
    def test_from_raw(self):
        row = MarketNewsItemRow.from_raw(
            _RAW,
            source_name="TestFeed",
            source_type="rss_news",
            reliability_tier=3,
            fetched_at=_NOW,
        )
        assert row.source_url == _RAW.url
        assert row.title == _RAW.title
        assert row.source_name == "TestFeed"
        assert row.reliability_tier == 3
        assert row.content_raw is None

    def test_from_raw_with_content(self):
        row = MarketNewsItemRow.from_raw(
            _RAW,
            source_name="TestFeed",
            source_type="rss_news",
            reliability_tier=3,
            fetched_at=_NOW,
            content_raw="<html>article</html>",
        )
        assert row.content_raw == "<html>article</html>"

    def test_to_frame(self):
        row = MarketNewsItemRow.from_raw(
            _RAW,
            source_name="TestFeed",
            source_type="rss_news",
            reliability_tier=3,
            fetched_at=_NOW,
        )
        frame = MarketNewsItemRow.to_frame([row])
        assert "source_url" in frame.columns
        assert "content_hash" in frame.columns
        assert len(frame) == 1
