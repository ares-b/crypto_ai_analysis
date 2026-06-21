from datetime import UTC, datetime, timedelta

from dagster import AssetExecutionContext, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.assets.raw import daily_partitions
from orchestration.resources import BinanceClientResource, IcebergStoreResource
from pipelines.raw.futures.config import DERIVATIVES_METRICS, FUNDING_RATES, LONG_SHORT_RATIO
from pipelines.raw.futures.run import run_funding_rates, run_futures_metrics, run_long_short_ratio


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


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="binance")
def raw_funding_rates(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, fn=run_funding_rates, settings=FUNDING_RATES)


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="binance")
def raw_futures_metrics(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, fn=run_futures_metrics, settings=DERIVATIVES_METRICS)


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="binance")
def raw_long_short_ratio(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, fn=run_long_short_ratio, settings=LONG_SHORT_RATIO)


raw_funding_rates_job = define_asset_job("raw_funding_rates_job", selection=[raw_funding_rates])
raw_futures_metrics_job = define_asset_job("raw_futures_metrics_job", selection=[raw_futures_metrics])
raw_long_short_ratio_job = define_asset_job("raw_long_short_ratio_job", selection=[raw_long_short_ratio])

raw_funding_rates_schedule = build_schedule_from_partitioned_job(raw_funding_rates_job, hour_of_day=9)
raw_futures_metrics_schedule = build_schedule_from_partitioned_job(raw_futures_metrics_job, hour_of_day=10)
raw_long_short_ratio_schedule = build_schedule_from_partitioned_job(raw_long_short_ratio_job, hour_of_day=11)
