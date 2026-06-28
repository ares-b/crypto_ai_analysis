import logging
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

from core.http import HttpClient, HttpError
from core.storage import Store
from pipelines import MetricValue
from pipelines.quality import check_frame

from .config import MarketNewsSettings
from .models import MarketNewsItemRow, RawNewsItem, content_hash

_ATOM_NS = "http://www.w3.org/2005/Atom"


def _parse_rss_date(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _parse_feed(xml_text: str) -> list[RawNewsItem]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    if _ATOM_NS in root.tag:
        return _parse_atom(root)
    return _parse_rss(root)


def _parse_rss(root: ET.Element) -> list[RawNewsItem]:
    items: list[RawNewsItem] = []
    for item in root.iter("item"):
        url = (item.findtext("link") or "").strip()
        if not url:
            continue
        items.append(
            RawNewsItem(
                url=url,
                title=(item.findtext("title") or "").strip(),
                summary=item.findtext("description"),
                source_domain=urlparse(url).netloc or None,
                published_at=_parse_rss_date(item.findtext("pubDate")),
            )
        )
    return items


def _parse_atom(root: ET.Element) -> list[RawNewsItem]:
    items: list[RawNewsItem] = []
    ns = {"a": _ATOM_NS}
    for entry in root.findall("a:entry", ns):
        link_el = entry.find("a:link", ns)
        url = (link_el.get("href") if link_el is not None else None) or ""
        if not url:
            continue
        pub_raw = entry.findtext("a:published", namespaces=ns) or entry.findtext(
            "a:updated", namespaces=ns
        )
        items.append(
            RawNewsItem(
                url=url,
                title=(entry.findtext("a:title", namespaces=ns) or "").strip(),
                summary=entry.findtext("a:summary", namespaces=ns),
                source_domain=urlparse(url).netloc or None,
                published_at=_parse_rss_date(pub_raw),
            )
        )
    return items


def _stored_content_hashes(store: Store, table_name: str) -> set[str]:
    frame = store.read(table_name, columns=["content_hash"])
    if frame.is_empty():
        return set()
    return set(frame.get_column("content_hash").to_list())


def fetch_market_news(
    *,
    logger: logging.Logger,
    client: HttpClient,
    settings: MarketNewsSettings,
    known_hashes: set[str],
) -> tuple[list[MarketNewsItemRow], int]:
    fetched_at = datetime.now(UTC)
    rows: list[MarketNewsItemRow] = []
    items_seen = 0
    for feed in settings.feeds:
        try:
            xml_text = client.get_text(feed.url)
        except HttpError as exc:
            logger.warning(f"[market_news] feed unavailable {feed.name}: {exc}")
            continue
        for item in _parse_feed(xml_text):
            items_seen += 1
            # Hash computed from feed metadata only, before the costly article-body fetch,
            # so reposts with edited title/summary register as new content.
            item_hash = content_hash(item.url, item.title, item.summary)
            if item_hash in known_hashes:
                continue
            known_hashes.add(item_hash)
            content_raw: str | None = None
            try:
                content_raw = client.get_text(item.url)
            except HttpError as exc:
                logger.warning(f"[market_news] article unavailable {item.url}: {exc}")
            rows.append(
                MarketNewsItemRow.from_raw(
                    item,
                    source_name=feed.name,
                    source_type=feed.source_type,
                    reliability_tier=feed.reliability_tier,
                    fetched_at=fetched_at,
                    content_raw=content_raw,
                )
            )
    return rows, items_seen


def run_market_news(
    *,
    store: Store,
    logger: logging.Logger,
    run_date: date,
    settings: MarketNewsSettings,
    client: HttpClient,
) -> dict[str, MetricValue]:
    known_hashes = _stored_content_hashes(store, settings.table_name)
    rows, items_seen = fetch_market_news(
        logger=logger,
        client=client,
        settings=settings,
        known_hashes=known_hashes,
    )
    rows_affected = 0
    quality_metrics: dict[str, MetricValue] = {}
    if rows:
        frame = MarketNewsItemRow.to_frame(rows)
        report = check_frame(frame, MarketNewsItemRow.quality_checks(), logger=logger, table=settings.table_name)
        quality_metrics = report.to_metrics()
        rows_affected = store.upsert(settings.table_name, frame).rows_affected
    logger.info(f"[market_news] {run_date.isoformat()} seen={items_seen} written={len(rows)}")
    return {"rows_affected": rows_affected, "items_seen": items_seen, "items_written": len(rows), **quality_metrics}
