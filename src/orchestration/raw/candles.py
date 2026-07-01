from datetime import UTC, datetime, timedelta

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    WeeklyPartitionsDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from pipelines.raw.candles.config import (
    DAILY_CANDLES,
    FOUR_HOUR_CANDLES,
    HOURLY_CANDLES,
    WEEKLY_CANDLES,
)
from pipelines.raw.candles.run import quality_subjects, run_binance_candles
from orchestration.partitions import DEPLOY_DATE, DEPLOY_WEEK_START
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_A
from orchestration.resources import BinanceClientResource, IcebergStoreResource

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
four_hour_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
hourly_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
weekly_partitions = WeeklyPartitionsDefinition(
    start_date=DEPLOY_WEEK_START, timezone="UTC", day_offset=0
)


@raw_asset(
    name="binance_candles_daily",
    source="binance",
    partitions_def=daily_partitions,
    subjects=quality_subjects(settings=DAILY_CANDLES),
)
def binance_candles_daily(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    window_start = datetime.fromisoformat(context.partition_key).replace(tzinfo=UTC)
    result = run_binance_candles(
        logger=context.log,
        settings=DAILY_CANDLES,
        store=iceberg_store.create(),
        client=binance_client.create(),
        window_start=window_start,
        window_end=window_start + timedelta(days=1),
    )
    return to_materialize_result(context, result)


@raw_asset(
    name="binance_candles_4h",
    source="binance",
    partitions_def=four_hour_partitions,
    subjects=quality_subjects(settings=FOUR_HOUR_CANDLES),
)
def binance_candles_4h(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    window_start = datetime.fromisoformat(context.partition_key).replace(tzinfo=UTC)
    result = run_binance_candles(
        logger=context.log,
        settings=FOUR_HOUR_CANDLES,
        store=iceberg_store.create(),
        client=binance_client.create(),
        window_start=window_start,
        window_end=window_start + timedelta(days=1),
    )
    return to_materialize_result(context, result)


@raw_asset(
    name="binance_candles_1h",
    source="binance",
    partitions_def=hourly_partitions,
    subjects=quality_subjects(settings=HOURLY_CANDLES),
)
def binance_candles_1h(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    window_start = datetime.fromisoformat(context.partition_key).replace(tzinfo=UTC)
    result = run_binance_candles(
        logger=context.log,
        settings=HOURLY_CANDLES,
        store=iceberg_store.create(),
        client=binance_client.create(),
        window_start=window_start,
        window_end=window_start + timedelta(days=1),
    )
    return to_materialize_result(context, result)


@raw_asset(
    name="binance_candles_weekly",
    source="binance",
    partitions_def=weekly_partitions,
    subjects=quality_subjects(settings=WEEKLY_CANDLES),
)
def binance_candles_weekly(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    binance_client: BinanceClientResource,
) -> MaterializeResult:
    window_start = datetime.fromisoformat(context.partition_key).replace(tzinfo=UTC)
    result = run_binance_candles(
        logger=context.log,
        settings=WEEKLY_CANDLES,
        store=iceberg_store.create(),
        client=binance_client.create(),
        window_start=window_start,
        window_end=window_start + timedelta(weeks=1),
    )
    return to_materialize_result(context, result)


# 01:00 UTC - daily candle closes at midnight UTC
raw_daily_candles_job = define_asset_job("raw_daily_candles_job", selection=[binance_candles_daily], tags=TIER_A)
raw_daily_candles_schedule = build_schedule_from_partitioned_job(raw_daily_candles_job, hour_of_day=1)

# 01:00 UTC - prior day's intraday bars are all closed by midnight UTC
raw_candles_4h_job = define_asset_job("raw_candles_4h_job", selection=[binance_candles_4h], tags=TIER_A)
raw_candles_4h_schedule = build_schedule_from_partitioned_job(raw_candles_4h_job, hour_of_day=1)

raw_candles_1h_job = define_asset_job("raw_candles_1h_job", selection=[binance_candles_1h], tags=TIER_A)
raw_candles_1h_schedule = build_schedule_from_partitioned_job(raw_candles_1h_job, hour_of_day=1)

# Monday 01:00 UTC - weekly candle closes Sunday midnight UTC
raw_weekly_candles_job = define_asset_job("raw_weekly_candles_job", selection=[binance_candles_weekly], tags=TIER_A)
raw_weekly_candles_schedule = build_schedule_from_partitioned_job(raw_weekly_candles_job, hour_of_day=1, day_of_week=0)

JOBS = [
    raw_daily_candles_job,
    raw_candles_4h_job,
    raw_candles_1h_job,
    raw_weekly_candles_job,
]
SCHEDULES = [
    raw_daily_candles_schedule,
    raw_candles_4h_schedule,
    raw_candles_1h_schedule,
    raw_weekly_candles_schedule,
]

ASSETS = [
    binance_candles_daily,
    binance_candles_4h,
    binance_candles_1h,
    binance_candles_weekly,
]
