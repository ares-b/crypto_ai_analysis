import argparse
from datetime import UTC, date, datetime

import polars as pl

from core.http import HttpClient

from pipelines.raw.onchain_metrics.config import ONCHAIN_SETTINGS
from pipelines.raw.onchain_metrics.models import (
    MINER_REVENUE_CHART,
    TRANSACTION_FEES_CHART,
    BlockchainChartRow,
    OnchainMetricRow,
    RawOnchainMetric,
)

from core.iceberg import IcebergStore
from core.logging import get_logger
from core.quality import not_empty, time_in_window
from schemas import ALL_SPECS
from . import helpers


def _fetch_onchain_range(client, *, start: date, end: date, source_updated_at) -> list[OnchainMetricRow]:
    params = {
        "assets": ONCHAIN_SETTINGS.onchain_asset,
        "metrics": ",".join(ONCHAIN_SETTINGS.metrics),
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "frequency": "1d",
        "page_size": 10000,
    }
    rows: list[OnchainMetricRow] = []
    while True:
        payload = client.get_json("/v4/timeseries/asset-metrics", params=params)
        for item in payload.get("data", []):
            rows.append(
                OnchainMetricRow.from_raw(
                    RawOnchainMetric.from_api_response(item),
                    instrument=ONCHAIN_SETTINGS.instrument,
                    counterpart=ONCHAIN_SETTINGS.counterpart,
                    source_updated_at=source_updated_at,
                )
            )
        token = payload.get("next_page_token")
        if not token:
            return rows
        params = {"next_page_token": token}


def _fetch_chart_range(client, *, start: date, end: date, source_updated_at) -> list[BlockchainChartRow]:
    rows: list[BlockchainChartRow] = []
    for chart_name, endpoint in (
        (MINER_REVENUE_CHART, "/charts/miners-revenue"),
        (TRANSACTION_FEES_CHART, "/charts/transaction-fees-usd"),
    ):
        payload = client.get_json(endpoint, params={"format": "json", "timespan": "all"})
        for point in payload.get("values", []):
            d = datetime.fromtimestamp(point["x"], tz=UTC).date()
            if start <= d < end:
                rows.append(
                    BlockchainChartRow(
                        chart_name=chart_name, date=d,
                        value=float(point["y"]), source_updated_at=source_updated_at,
                    )
                )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill on-chain metrics + charts")
    helpers.add_common_args(parser, default_start="2017-01-01")
    args = parser.parse_args()
    logger = get_logger("backfill.onchain_metrics")
    store = None if args.dry_run else IcebergStore.from_env(ALL_SPECS)
    source_updated_at = datetime.now(UTC)

    cm_client = HttpClient(ONCHAIN_SETTINGS.coinmetrics_base_url)
    metric_rows = _fetch_onchain_range(cm_client, start=args.start, end=args.end, source_updated_at=source_updated_at)
    metric_frame = OnchainMetricRow.to_frame(metric_rows)
    metric_id = OnchainMetricRow.TABLE_SPEC.identity_columns
    metric_checks = [not_empty(), time_in_window("date", args.start, args.end), *OnchainMetricRow.quality_checks()]
    written = helpers.commit(store, ONCHAIN_SETTINGS.table_name, metric_frame, metric_id,
                             checks=metric_checks, logger=logger, dry_run=args.dry_run)
    logger.info(f"[onchain_metrics] rows_written={written}")

    bc_client = HttpClient(ONCHAIN_SETTINGS.blockchain_base_url)
    chart_rows = _fetch_chart_range(bc_client, start=args.start, end=args.end, source_updated_at=source_updated_at)
    chart_frame = BlockchainChartRow.to_frame(chart_rows)
    chart_id = BlockchainChartRow.TABLE_SPEC.identity_columns
    written = helpers.commit(store, ONCHAIN_SETTINGS.blockchain_charts_table, chart_frame, chart_id,
                             checks=[not_empty(), *BlockchainChartRow.quality_checks()],
                             logger=logger, dry_run=args.dry_run)
    logger.info(f"[blockchain_charts] rows_written={written}")


if __name__ == "__main__":
    main()
