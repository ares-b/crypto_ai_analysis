import argparse

from core.http import HttpClient

from pipelines.raw.macro_calendar.config import MACRO_CALENDAR_SETTINGS
from pipelines.raw.macro_calendar.models import MacroCalendarRow
from pipelines.raw.macro_calendar.run import fetch_macro_calendar
from pipelines.raw.macro_series.config import MACRO_SERIES_SETTINGS
from pipelines.raw.macro_series.models import MacroSeriesRow
from pipelines.raw.macro_series.run import fetch_macro_series

from core.iceberg import IcebergStore
from core.logging import get_logger
from core.quality import not_empty
from schemas import ALL_SPECS
from . import helpers


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill FRED macro series + calendar")
    helpers.add_common_args(parser, default_start="2017-01-01")
    args = parser.parse_args()
    logger = get_logger("backfill.macro")
    store = None if args.dry_run else IcebergStore.from_env(ALL_SPECS)

    series_client = HttpClient(MACRO_SERIES_SETTINGS.fred_base_url)
    series_rows = fetch_macro_series(series_client, settings=MACRO_SERIES_SETTINGS, since=args.start)
    series_frame = MacroSeriesRow.to_frame(series_rows)
    series_id = MacroSeriesRow.TABLE_SPEC.identity_columns
    written = helpers.commit(store, "raw.macro_series", series_frame, series_id,
                             checks=[not_empty(), *MacroSeriesRow.quality_checks()],
                             logger=logger, dry_run=args.dry_run)
    logger.info(f"[macro_series] rows_written={written}")

    # releases/dates is a large, slow endpoint; give it a generous timeout.
    cal_client = HttpClient(MACRO_CALENDAR_SETTINGS.fred_base_url, timeout=120.0)
    cal_rows = fetch_macro_calendar(cal_client, settings=MACRO_CALENDAR_SETTINGS, since=args.start)
    cal_frame = MacroCalendarRow.to_frame(cal_rows)
    cal_id = MacroCalendarRow.TABLE_SPEC.identity_columns
    written = helpers.commit(store, "raw.macro_calendar", cal_frame, cal_id,
                             checks=[not_empty(), *MacroCalendarRow.quality_checks()],
                             logger=logger, dry_run=args.dry_run)
    logger.info(f"[macro_calendar] rows_written={written}")


if __name__ == "__main__":
    main()
