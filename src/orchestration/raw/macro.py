from datetime import date, timedelta

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

daily_partitions = DailyPartitionsDefinition(start_date="2017-01-01", timezone="UTC")
from pipelines.raw.macro_calendar.config import MACRO_CALENDAR_SETTINGS
from pipelines.raw.macro_calendar.run import run_macro_calendar
from pipelines.raw.macro_series.config import MACRO_SERIES_SETTINGS
from pipelines.raw.macro_series.run import run_macro_series


def _run(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    fred_client: HttpClientResource,
    *,
    fn,
    settings,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    since = run_date - timedelta(days=settings.incremental_lookback_days)
    metrics = fn(
        logger=context.log,
        settings=settings,
        store=iceberg_store.create(),
        client=fred_client.create(),
        run_date=run_date,
        since=since,
    )
    return MaterializeResult(metadata=metrics)


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=daily_partitions, group_name="raw", compute_kind="python", tags={"source": "fred"})
def raw_macro_calendar(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    fred_client: HttpClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, fred_client, fn=run_macro_calendar, settings=MACRO_CALENDAR_SETTINGS)


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=daily_partitions, group_name="raw", compute_kind="python", tags={"source": "fred"})
def raw_macro_series(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    fred_client: HttpClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, fred_client, fn=run_macro_series, settings=MACRO_SERIES_SETTINGS)
# 22:00 UTC - FRED: treasury rates at 20:00 UTC, DTWEXBGS at 21:15 UTC, M2SL at 21:30 UTC
# macro_calendar and macro_series share daily_partitions; merged into one trigger
raw_macro_job = define_asset_job("raw_macro_job", selection=[raw_macro_calendar, raw_macro_series])
raw_macro_schedule = build_schedule_from_partitioned_job(raw_macro_job, hour_of_day=22)

JOBS = [raw_macro_job]
SCHEDULES = [raw_macro_schedule]

ASSETS = [raw_macro_calendar, raw_macro_series]
