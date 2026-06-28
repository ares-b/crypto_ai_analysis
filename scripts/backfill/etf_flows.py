import argparse

import polars as pl

from core.http import HttpClient

from pipelines.raw.etf_flows.config import ETF_FLOWS_SETTINGS
from pipelines.raw.etf_flows.models import EtfFlowRow
from pipelines.raw.etf_flows.run import fetch_etf_flows

from core.iceberg import IcebergStore
from core.logging import get_logger
from core.quality import not_empty, time_in_window
from schemas import ALL_SPECS
from . import helpers

_TABLE = "raw.etf_flows"
_IDENTITY = EtfFlowRow.TABLE_SPEC.identity_columns


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill BTC ETF flows")
    helpers.add_common_args(parser, default_start="2024-01-11")
    args = parser.parse_args()
    logger = get_logger("backfill.etf_flows")

    client = HttpClient(ETF_FLOWS_SETTINGS.farside_url, headers=ETF_FLOWS_SETTINGS.request_headers)
    frame = EtfFlowRow.to_frame(fetch_etf_flows(client))
    frame = frame.filter((pl.col("date") >= args.start) & (pl.col("date") < args.end))

    checks = [
        not_empty(),
        time_in_window("date", args.start, args.end),
        *EtfFlowRow.quality_checks(),
    ]
    store = None if args.dry_run else IcebergStore.from_env(ALL_SPECS)
    written = helpers.commit(store, _TABLE, frame, _IDENTITY, checks=checks, logger=logger, dry_run=args.dry_run)
    logger.info(f"[etf_flows] done rows_written={written}")


if __name__ == "__main__":
    main()
