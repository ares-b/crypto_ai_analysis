from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

_dominance_partitions = DailyPartitionsDefinition(start_date="2025-06-21", timezone="UTC")
_stablecoin_partitions = DailyPartitionsDefinition(start_date="2026-06-20", timezone="UTC")
from orchestration.resources import HttpClientResource, IcebergStoreResource
from pipelines.raw.market_metrics.config import MARKET_METRIC_SETTINGS
from pipelines.raw.market_metrics.run import run_market_metrics, run_stablecoin_supply


def _run(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    coingecko_client: HttpClientResource,
    *,
    fn,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    metrics = fn(
        logger=context.log,
        settings=MARKET_METRIC_SETTINGS,
        store=iceberg_store.create(),
        client=coingecko_client.create(),
        run_date=run_date,
    )
    return MaterializeResult(metadata=metrics)


@asset(partitions_def=_dominance_partitions, group_name="raw", compute_kind="python", tags={"source": "coingecko"})
def raw_market_metrics(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    coingecko_client: HttpClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, coingecko_client, fn=run_market_metrics)


@asset(partitions_def=_stablecoin_partitions, group_name="raw", compute_kind="python", tags={"source": "coingecko"})
def raw_stablecoin_supply(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    coingecko_client: HttpClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, coingecko_client, fn=run_stablecoin_supply)


raw_market_metrics_job = define_asset_job("raw_market_metrics_job", selection=[raw_market_metrics])
raw_stablecoin_supply_job = define_asset_job("raw_stablecoin_supply_job", selection=[raw_stablecoin_supply])

raw_market_metrics_schedule = build_schedule_from_partitioned_job(raw_market_metrics_job, hour_of_day=7)
raw_stablecoin_supply_schedule = build_schedule_from_partitioned_job(raw_stablecoin_supply_job, hour_of_day=8)
