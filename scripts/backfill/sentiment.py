import argparse
from datetime import UTC, datetime

import polars as pl

from core.http import HttpClient

from pipelines.raw.sentiment_index.config import SENTIMENT_SETTINGS
from pipelines.raw.sentiment_index.models import DeribitDvolRow, SentimentRow, build_dvol_rows, build_rows
from pipelines.raw.sentiment_index.run import _dvol_by_date, _fetch_fear_greed

from core.iceberg import IcebergStore
from core.logging import get_logger
from core.quality import not_empty
from schemas import ALL_SPECS
from . import helpers


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Fear & Greed + Deribit DVOL")
    helpers.add_common_args(parser, default_start="2018-02-01")
    args = parser.parse_args()
    logger = get_logger("backfill.sentiment")
    store = None if args.dry_run else IcebergStore.from_env(ALL_SPECS)
    source_updated_at = datetime.now(UTC)

    # limit=0 returns entire Fear & Greed history.
    fng_client = HttpClient(SENTIMENT_SETTINGS.fear_greed_base_url)
    fng_rows = build_rows(_fetch_fear_greed(fng_client, limit=0), source_updated_at=source_updated_at)
    fng_frame = SentimentRow.to_frame(fng_rows).filter(
        (pl.col("date") >= args.start) & (pl.col("date") < args.end)
    )
    fng_id = SentimentRow.TABLE_SPEC.identity_columns
    written = helpers.commit(store, SENTIMENT_SETTINGS.table_name, fng_frame, fng_id,
                             checks=[not_empty(), *SentimentRow.quality_checks()],
                             logger=logger, dry_run=args.dry_run)
    logger.info(f"[fear_greed] rows_written={written}")

    dvol_client = HttpClient(SENTIMENT_SETTINGS.deribit_base_url)
    start_ms = int(helpers.window(args)[0].timestamp() * 1000)
    dvol_rows = build_dvol_rows(_dvol_by_date(dvol_client, start_ms=start_ms), source_updated_at=source_updated_at)
    dvol_frame = DeribitDvolRow.to_frame(dvol_rows).filter(
        (pl.col("date") >= args.start) & (pl.col("date") < args.end)
    )
    dvol_id = DeribitDvolRow.TABLE_SPEC.identity_columns
    written = helpers.commit(store, SENTIMENT_SETTINGS.dvol_table, dvol_frame, dvol_id,
                             checks=DeribitDvolRow.quality_checks(), logger=logger, dry_run=args.dry_run)
    logger.info(f"[dvol] rows_written={written}")


if __name__ == "__main__":
    main()
