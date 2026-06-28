import argparse

from core.http import HttpClient

from pipelines.raw.cot_positioning.config import COT_POSITIONING_SETTINGS
from pipelines.raw.cot_positioning.models import CotPositioningRow
from pipelines.raw.cot_positioning.run import fetch_cot_positioning

from core.iceberg import IcebergStore
from core.logging import get_logger
from core.quality import not_empty, time_in_window
from schemas import ALL_SPECS
from . import helpers

_TABLE = "raw.cot_positioning"
_IDENTITY = CotPositioningRow.TABLE_SPEC.identity_columns


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill CFTC COT positioning")
    helpers.add_common_args(parser, default_start="2018-01-02")
    args = parser.parse_args()
    logger = get_logger("backfill.cot_positioning")

    client = HttpClient(COT_POSITIONING_SETTINGS.cftc_base_url)
    rows = fetch_cot_positioning(client, settings=COT_POSITIONING_SETTINGS, since=args.start)
    frame = CotPositioningRow.to_frame(rows)

    checks = [
        not_empty(),
        time_in_window("report_date", args.start, args.end),
        *CotPositioningRow.quality_checks(),
    ]
    store = None if args.dry_run else IcebergStore.from_env(ALL_SPECS)
    written = helpers.commit(store, _TABLE, frame, _IDENTITY, checks=checks, logger=logger, dry_run=args.dry_run)
    logger.info(f"[cot_positioning] done rows_written={written}")


if __name__ == "__main__":
    main()
