import argparse
from datetime import UTC, date, datetime

import polars as pl

from core.http import HttpClient

from pipelines.raw.market_metrics.models import MarketMetricRow

from core.iceberg import IcebergStore
from core.logging import get_logger
from core.quality import not_empty, time_in_window
from schemas import ALL_SPECS
from . import helpers

_TABLE = "raw.market_metrics"
_IDENTITY = MarketMetricRow.TABLE_SPEC.identity_columns
_BASE_URL = "https://www.coingecko.com"
_ENDPOINT = "/global_charts/bitcoin_dominance_data"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}


def _fetch_dominance_history(client: HttpClient) -> dict[date, float]:
    series = client.get_json(_ENDPOINT, params={"locale": "en"})
    btc = next(s for s in series if s.get("name") == "BTC")
    # Last value per day wins (dedups any intraday points near the present).
    return {
        datetime.fromtimestamp(ms / 1000, tz=UTC).date(): float(pct)
        for ms, pct in btc["data"]
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill BTC dominance (CoinGecko global chart)")
    helpers.add_common_args(parser, default_start="2013-04-29")
    args = parser.parse_args()
    logger = get_logger("backfill.market_metrics")
    source_updated_at = datetime.now(UTC)

    client = HttpClient(_BASE_URL, headers=_HEADERS)
    rows = [
        MarketMetricRow(date=d, btc_dominance_pct=pct, source_updated_at=source_updated_at)
        for d, pct in sorted(_fetch_dominance_history(client).items())
    ]
    frame = MarketMetricRow.to_frame(rows).filter(
        (pl.col("date") >= args.start) & (pl.col("date") < args.end)
    )

    checks = [
        not_empty(),
        time_in_window("date", args.start, args.end),
        *MarketMetricRow.quality_checks(),
    ]
    store = None if args.dry_run else IcebergStore.from_env(ALL_SPECS)
    written = helpers.commit(store, _TABLE, frame, _IDENTITY, checks=checks, logger=logger, dry_run=args.dry_run)
    logger.info(f"[market_metrics] done rows_written={written}")


if __name__ == "__main__":
    main()
