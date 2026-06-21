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

from pipelines.raw.candles.config import DAILY_CANDLES, WEEKLY_CANDLES, BinanceCandleSettings
from pipelines.raw.candles.run import run_binance_candles
from orchestration.resources import BinanceClientResource, IcebergStoreResource

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


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=daily_partitions, group_name="raw", compute_kind="python", tags={"source": "binance"})
def binance_candles_daily(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, settings=DAILY_CANDLES, window_delta=timedelta(days=1))


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=weekly_partitions, group_name="raw", compute_kind="python", tags={"source": "binance"})
def binance_candles_weekly(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, binance_client, settings=WEEKLY_CANDLES, window_delta=timedelta(weeks=1))


# 01:00 UTC - daily candle closes at midnight UTC
raw_daily_candles_job = define_asset_job("raw_daily_candles_job", selection=[binance_candles_daily])
raw_daily_candles_schedule = build_schedule_from_partitioned_job(raw_daily_candles_job, hour_of_day=1)

# Monday 01:00 UTC - weekly candle closes Sunday midnight UTC
raw_weekly_candles_job = define_asset_job("raw_weekly_candles_job", selection=[binance_candles_weekly])
raw_weekly_candles_schedule = build_schedule_from_partitioned_job(raw_weekly_candles_job, hour_of_day=1, day_of_week=0)

JOBS = [raw_daily_candles_job, raw_weekly_candles_job]
SCHEDULES = [raw_daily_candles_schedule, raw_weekly_candles_schedule]

ASSETS = [binance_candles_daily, binance_candles_weekly]
