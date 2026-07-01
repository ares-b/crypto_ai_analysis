from datetime import UTC, datetime, timedelta

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.futures.config import DERIVATIVES_METRICS, FUNDING_RATES, LONG_SHORT_RATIO
from pipelines.raw.futures.run import (
    funding_quality_subjects,
    futures_metrics_quality_subjects,
    long_short_quality_subjects,
    run_funding_rates,
    run_futures_metrics,
    run_long_short_ratio,
)
from orchestration.partitions import DEPLOY_DATE
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_A, TIER_B
from orchestration.resources import BinanceClientResource, IcebergStoreResource

_funding_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
_binance_stats_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


def _window(context: AssetExecutionContext) -> tuple[datetime, datetime]:
    window_start = datetime.fromisoformat(context.partition_key).replace(tzinfo=UTC)
    return window_start, window_start + timedelta(days=1)


@raw_asset(
    name="raw_funding_rates",
    source="binance",
    partitions_def=_funding_partitions,
    subjects=funding_quality_subjects(settings=FUNDING_RATES),
)
def raw_funding_rates(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    window_start, window_end = _window(context)
    result = run_funding_rates(
        logger=context.log,
        settings=FUNDING_RATES,
        store=iceberg_store.create(),
        client=binance_client.create(),
        window_start=window_start,
        window_end=window_end,
    )
    return to_materialize_result(context, result)


@raw_asset(
    name="raw_futures_metrics",
    source="binance",
    partitions_def=_binance_stats_partitions,
    subjects=futures_metrics_quality_subjects(settings=DERIVATIVES_METRICS),
)
def raw_futures_metrics(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    window_start, window_end = _window(context)
    result = run_futures_metrics(
        logger=context.log,
        settings=DERIVATIVES_METRICS,
        store=iceberg_store.create(),
        client=binance_client.create(),
        window_start=window_start,
        window_end=window_end,
    )
    return to_materialize_result(context, result)


@raw_asset(
    name="raw_long_short_ratio",
    source="binance",
    partitions_def=_binance_stats_partitions,
    subjects=long_short_quality_subjects(settings=LONG_SHORT_RATIO),
)
def raw_long_short_ratio(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    window_start, window_end = _window(context)
    result = run_long_short_ratio(
        logger=context.log,
        settings=LONG_SHORT_RATIO,
        store=iceberg_store.create(),
        client=binance_client.create(),
        window_start=window_start,
        window_end=window_end,
    )
    return to_materialize_result(context, result)


# 01:00 UTC - crypto data closes at midnight UTC
raw_funding_rates_job = define_asset_job("raw_funding_rates_job", selection=[raw_funding_rates], tags=TIER_A)
raw_funding_rates_schedule = build_schedule_from_partitioned_job(raw_funding_rates_job, hour_of_day=1)
# futures_metrics and long_short_ratio share the 30-day stats partition; one trigger
raw_binance_stats_job = define_asset_job("raw_binance_stats_job", selection=[raw_futures_metrics, raw_long_short_ratio], tags=TIER_B)
raw_binance_stats_schedule = build_schedule_from_partitioned_job(raw_binance_stats_job, hour_of_day=1)

JOBS = [raw_funding_rates_job, raw_binance_stats_job]
SCHEDULES = [raw_funding_rates_schedule, raw_binance_stats_schedule]

ASSETS = [raw_funding_rates, raw_futures_metrics, raw_long_short_ratio]
