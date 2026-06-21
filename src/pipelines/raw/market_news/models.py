import hashlib
from dataclasses import dataclass
from datetime import datetime

from core.models import StoreRow


@dataclass(frozen=True)
class RawNewsItem:
    url: str
    title: str
    summary: str | None
    source_domain: str | None
    published_at: datetime | None


def content_hash(url: str, title: str, summary: str | None = None) -> str:
    return hashlib.sha256(f"{url}\n{title}\n{summary or ''}".encode()).hexdigest()


class MarketNewsItemRow(StoreRow):
    source_url: str
    content_hash: str
    source_name: str | None
    source_domain: str | None
    source_type: str | None
    reliability_tier: int | None
    published_at: datetime | None
    fetched_at: datetime | None
    title: str | None
    summary_raw: str | None
    content_raw: str | None
    source_updated_at: datetime

    @classmethod
    def from_raw(
        cls,
        item: RawNewsItem,
        *,
        source_name: str,
        source_type: str,
        reliability_tier: int,
        fetched_at: datetime,
        content_raw: str | None = None,
    ) -> "MarketNewsItemRow":
        return cls(
            source_url=item.url,
            content_hash=content_hash(item.url, item.title, item.summary),
            source_name=source_name,
            source_domain=item.source_domain,
            source_type=source_type,
            reliability_tier=reliability_tier,
            published_at=item.published_at,
            fetched_at=fetched_at,
            title=item.title,
            summary_raw=item.summary,
            content_raw=content_raw,
            source_updated_at=fetched_at,
        )
