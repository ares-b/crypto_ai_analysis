from datetime import date, timedelta

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.macro_calendar.config import MACRO_CALENDAR_SETTINGS
from pipelines.raw.macro_calendar.run import macro_calendar_quality_subjects, run_macro_calendar
from pipelines.raw.macro_series.config import MACRO_SERIES_SETTINGS
from pipelines.raw.macro_series.run import macro_series_quality_subjects, run_macro_series
from orchestration.partitions import DEPLOY_DATE
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_C
from orchestration.resources import HttpClientResource, IcebergStoreResource

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


@raw_asset(
    name="raw_macro_calendar",
    source="fred",
    partitions_def=daily_partitions,
    subjects=macro_calendar_quality_subjects(settings=MACRO_CALENDAR_SETTINGS),
)
def raw_macro_calendar(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    fred_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    since = run_date - timedelta(days=MACRO_CALENDAR_SETTINGS.incremental_lookback_days)
    result = run_macro_calendar(
        logger=context.log,
        settings=MACRO_CALENDAR_SETTINGS,
        store=iceberg_store.create(),
        client=fred_client.create(),
        run_date=run_date,
        since=since,
    )
    return to_materialize_result(context, result)


@raw_asset(
    name="raw_macro_series",
    source="fred",
    partitions_def=daily_partitions,
    subjects=macro_series_quality_subjects(settings=MACRO_SERIES_SETTINGS),
)
def raw_macro_series(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    fred_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    since = run_date - timedelta(days=MACRO_SERIES_SETTINGS.incremental_lookback_days)
    result = run_macro_series(
        logger=context.log,
        settings=MACRO_SERIES_SETTINGS,
        store=iceberg_store.create(),
        client=fred_client.create(),
        run_date=run_date,
        since=since,
    )
    return to_materialize_result(context, result)
# 22:00 UTC - FRED: treasury rates at 20:00 UTC, DTWEXBGS at 21:15 UTC, M2SL at 21:30 UTC
# macro_calendar and macro_series share daily_partitions; merged into one trigger
raw_macro_job = define_asset_job("raw_macro_job", selection=[raw_macro_calendar, raw_macro_series], tags=TIER_C)
raw_macro_schedule = build_schedule_from_partitioned_job(raw_macro_job, hour_of_day=22)

JOBS = [raw_macro_job]
SCHEDULES = [raw_macro_schedule]

ASSETS = [raw_macro_calendar, raw_macro_series]
