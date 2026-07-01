from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.market_metrics.config import MARKET_METRIC_SETTINGS
from pipelines.raw.market_metrics.run import (
    market_metrics_quality_subjects,
    run_market_metrics,
    run_stablecoin_supply,
    stablecoin_supply_quality_subjects,
)
from orchestration.partitions import DEPLOY_DATE
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_B, TIER_C
from orchestration.resources import HttpClientResource, IcebergStoreResource

_dominance_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
_stablecoin_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


@raw_asset(
    name="raw_market_metrics",
    source="coingecko",
    partitions_def=_dominance_partitions,
    subjects=market_metrics_quality_subjects(settings=MARKET_METRIC_SETTINGS),
)
def raw_market_metrics(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    coingecko_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    result = run_market_metrics(
        logger=context.log,
        settings=MARKET_METRIC_SETTINGS,
        store=iceberg_store.create(),
        client=coingecko_client.create(),
        run_date=run_date,
    )
    return to_materialize_result(context, result)


@raw_asset(
    name="raw_stablecoin_supply",
    source="coingecko",
    partitions_def=_stablecoin_partitions,
    subjects=stablecoin_supply_quality_subjects(settings=MARKET_METRIC_SETTINGS),
)
def raw_stablecoin_supply(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    coingecko_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    result = run_stablecoin_supply(
        logger=context.log,
        settings=MARKET_METRIC_SETTINGS,
        store=iceberg_store.create(),
        client=coingecko_client.create(),
        run_date=run_date,
    )
    return to_materialize_result(context, result)
# 01:00 UTC - crypto data closes at midnight UTC
raw_market_metrics_job = define_asset_job("raw_market_metrics_job", selection=[raw_market_metrics], tags=TIER_B)
raw_market_metrics_schedule = build_schedule_from_partitioned_job(raw_market_metrics_job, hour_of_day=1)
raw_stablecoin_supply_job = define_asset_job("raw_stablecoin_supply_job", selection=[raw_stablecoin_supply], tags=TIER_C)
raw_stablecoin_supply_schedule = build_schedule_from_partitioned_job(raw_stablecoin_supply_job, hour_of_day=1)

JOBS = [raw_market_metrics_job, raw_stablecoin_supply_job]
SCHEDULES = [raw_market_metrics_schedule, raw_stablecoin_supply_schedule]

ASSETS = [raw_market_metrics, raw_stablecoin_supply]
