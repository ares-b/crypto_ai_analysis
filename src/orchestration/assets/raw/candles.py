from datetime import UTC, datetime, timedelta

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    WeeklyPartitionsDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from orchestration.resources import BinanceClientResource, IcebergStoreResource
from pipelines.raw.candles.config import DAILY_CANDLES, WEEKLY_CANDLES, BinanceCandleSettings
from pipelines.raw.candles.run import run_binance_candles

_BINANCE_LAUNCH_DATE = "2017-08-17"

daily_partitions = DailyPartitionsDefinition(start_date=_BINANCE_LAUNCH_DATE, timezone="UTC")
weekly_partitions = WeeklyPartitionsDefinition(
    start_date=_BINANCE_LAUNCH_DATE, timezone="UTC", day_offset=0
)


def _run(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
    *,
    settings: BinanceCandleSettings,
    window_delta: timedelta,
) -> MaterializeResult:
    window_start = datetime.fromisoformat(context.partition_key).replace(tzinfo=UTC)
    metrics = run_binance_candles(
        logger=context.log,
        settings=settings,
        store=iceberg_store.create(),
        client=binance_client.create(),
        window_start=window_start,
        window_end=window_start + window_delta,
    )
    return MaterializeResult(metadata=metrics)


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="binance")
def binance_candles_daily(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, settings=DAILY_CANDLES, window_delta=timedelta(days=1))


@asset(partitions_def=weekly_partitions, group_name="raw", compute_kind="binance")
def binance_candles_weekly(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, settings=WEEKLY_CANDLES, window_delta=timedelta(weeks=1))


raw_daily_candles_job = define_asset_job("raw_daily_candles_job", selection=[binance_candles_daily])
raw_weekly_candles_job = define_asset_job("raw_weekly_candles_job", selection=[binance_candles_weekly])

daily_candles_schedule = build_schedule_from_partitioned_job(raw_daily_candles_job, hour_of_day=2)
weekly_candles_schedule = build_schedule_from_partitioned_job(raw_weekly_candles_job, hour_of_day=3, day_of_week=0)
