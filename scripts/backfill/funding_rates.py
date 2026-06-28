import argparse

from binance.client import Client

from pipelines.raw.futures.config import FUNDING_RATES
from pipelines.raw.futures.models import FundingRateRow
from pipelines.raw.futures.run import fetch_funding_rates

from core.iceberg import IcebergStore
from core.logging import get_logger
from core.quality import not_empty, time_in_window
from schemas import ALL_SPECS
from . import helpers

_TABLE = "raw.funding_rates"
_IDENTITY = FundingRateRow.TABLE_SPEC.identity_columns


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Binance funding rates")
    helpers.add_common_args(parser, default_start="2019-09-13")
    args = parser.parse_args()
    logger = get_logger("backfill.funding_rates")

    window_start, window_end = helpers.window(args)

    rows = fetch_funding_rates(
        logger=logger, client=Client(), settings=FUNDING_RATES,
        window_start=window_start, window_end=window_end,
    )
    frame = FundingRateRow.to_frame(rows)

    checks = [
        not_empty(),
        time_in_window("funding_time", window_start, window_end),
        *FundingRateRow.quality_checks(),
    ]
    store = None if args.dry_run else IcebergStore.from_env(ALL_SPECS)
    written = helpers.commit(store, _TABLE, frame, _IDENTITY, checks=checks, logger=logger, dry_run=args.dry_run)
    logger.info(f"[funding_rates] done rows_written={written}")


if __name__ == "__main__":
    main()
