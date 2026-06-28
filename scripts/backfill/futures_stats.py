import argparse
from datetime import UTC, date, datetime, timedelta

from binance.client import Client

from pipelines.raw.futures.config import DERIVATIVES_METRICS, LONG_SHORT_RATIO
from pipelines.raw.futures.models import FuturesMetricRow, LongShortRatioRow
from pipelines.raw.futures.run import fetch_futures_metric, fetch_long_short_ratio

from core.iceberg import IcebergStore
from core.logging import get_logger
from schemas import ALL_SPECS
from . import helpers


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Binance futures stats (~30d)")
    helpers.add_common_args(parser, default_start=(date.today() - timedelta(days=30)).isoformat())
    args = parser.parse_args()
    logger = get_logger("backfill.futures_stats")
    client = Client()
    store = None if args.dry_run else IcebergStore.from_env(ALL_SPECS)

    ls_start, ls_end = helpers.window(args)
    ls_rows = fetch_long_short_ratio(
        logger=logger, client=client, settings=LONG_SHORT_RATIO,
        window_start=ls_start, window_end=ls_end,
    )
    ls_frame = LongShortRatioRow.to_frame(ls_rows)
    ls_id = LongShortRatioRow.TABLE_SPEC.identity_columns
    written = helpers.commit(store, LONG_SHORT_RATIO.table_name, ls_frame, ls_id,
                             checks=LongShortRatioRow.quality_checks(), logger=logger, dry_run=args.dry_run)
    logger.info(f"[long_short_ratio] rows_written={written}")

    # Futures metrics endpoint returns one row per day; loop the window.
    metric_rows = []
    day = args.start
    while day < args.end:
        ws = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
        row = fetch_futures_metric(
            logger=logger, client=client, settings=DERIVATIVES_METRICS,
            window_start=ws, window_end=ws + timedelta(days=1),
        )
        if row is not None:
            metric_rows.append(row)
        day += timedelta(days=1)
    fm_frame = FuturesMetricRow.to_frame(metric_rows)
    fm_id = FuturesMetricRow.TABLE_SPEC.identity_columns
    written = helpers.commit(store, DERIVATIVES_METRICS.table_name, fm_frame, fm_id,
                             checks=FuturesMetricRow.quality_checks(), logger=logger, dry_run=args.dry_run)
    logger.info(f"[futures_metrics] rows_written={written}")


if __name__ == "__main__":
    main()
