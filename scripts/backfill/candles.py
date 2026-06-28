import argparse

from binance.client import Client

from pipelines.raw.candles.config import BinanceCandleSettings
from pipelines.raw.candles.models import BinanceCandleRow
from pipelines.raw.candles.run import fetch_candles

from core.iceberg import IcebergStore
from core.logging import get_logger
from core.quality import not_empty, time_in_window
from schemas import ALL_SPECS
from . import helpers

_TABLE = "raw.candles"
_IDENTITY = BinanceCandleRow.TABLE_SPEC.identity_columns


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Binance candles")
    parser.add_argument("--interval", required=True, choices=["1d", "4h", "1h", "1w"])
    helpers.add_common_args(parser, default_start="2017-08-17")
    args = parser.parse_args()
    logger = get_logger("backfill.candles")

    settings = BinanceCandleSettings(interval=args.interval)
    window_start, window_end = helpers.window(args)

    result = fetch_candles(
        logger=logger, client=Client(), settings=settings,
        window_start=window_start, window_end=window_end,
    )
    frame = BinanceCandleRow.to_frame(result.candles)

    checks = [
        not_empty(),
        time_in_window("open_time", window_start, window_end),
        *BinanceCandleRow.quality_checks(),
    ]
    store = None if args.dry_run else IcebergStore.from_env(ALL_SPECS)
    written = helpers.commit(store, _TABLE, frame, _IDENTITY, checks=checks, logger=logger, dry_run=args.dry_run)
    logger.info(f"[candles {args.interval}] done rows_written={written}")


if __name__ == "__main__":
    main()
