from datetime import UTC, datetime, timedelta

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.futures.config import DERIVATIVES_METRICS, FUNDING_RATES, LONG_SHORT_RATIO
from pipelines.raw.futures.run import run_funding_rates, run_futures_metrics, run_long_short_ratio
from orchestration.partitions import DEPLOY_DATE
from orchestration._runtime import DEFAULT_RETRY_POLICY, TIER_A, TIER_B
from orchestration.resources import BinanceClientResource, IcebergStoreResource

_funding_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
_binance_stats_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


def _window(context: AssetExecutionContext) -> tuple[datetime, datetime]:
    window_start = datetime.fromisoformat(context.partition_key).replace(tzinfo=UTC)
    return window_start, window_start + timedelta(days=1)


def _run(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
    *,
    fn,
    settings,
) -> MaterializeResult:
    window_start, window_end = _window(context)
    metrics = fn(
        logger=context.log,
        settings=settings,
        store=iceberg_store.create(),
        client=binance_client.create(),
        window_start=window_start,
        window_end=window_end,
    )
    return MaterializeResult(metadata=metrics)


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=_funding_partitions, group_name="raw", compute_kind="python", tags={"source": "binance"}, retry_policy=DEFAULT_RETRY_POLICY)
def raw_funding_rates(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, fn=run_funding_rates, settings=FUNDING_RATES)


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=_binance_stats_partitions, group_name="raw", compute_kind="python", tags={"source": "binance"}, retry_policy=DEFAULT_RETRY_POLICY)
def raw_futures_metrics(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, fn=run_futures_metrics, settings=DERIVATIVES_METRICS)


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=_binance_stats_partitions, group_name="raw", compute_kind="python", tags={"source": "binance"}, retry_policy=DEFAULT_RETRY_POLICY)
def raw_long_short_ratio(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, fn=run_long_short_ratio, settings=LONG_SHORT_RATIO)


# 01:00 UTC - crypto data closes at midnight UTC
raw_funding_rates_job = define_asset_job("raw_funding_rates_job", selection=[raw_funding_rates], tags=TIER_A)
raw_funding_rates_schedule = build_schedule_from_partitioned_job(raw_funding_rates_job, hour_of_day=1)
# futures_metrics and long_short_ratio share the 30-day stats partition; one trigger
raw_binance_stats_job = define_asset_job("raw_binance_stats_job", selection=[raw_futures_metrics, raw_long_short_ratio], tags=TIER_B)
raw_binance_stats_schedule = build_schedule_from_partitioned_job(raw_binance_stats_job, hour_of_day=1)

JOBS = [raw_funding_rates_job, raw_binance_stats_job]
SCHEDULES = [raw_funding_rates_schedule, raw_binance_stats_schedule]

ASSETS = [raw_funding_rates, raw_futures_metrics, raw_long_short_ratio]
