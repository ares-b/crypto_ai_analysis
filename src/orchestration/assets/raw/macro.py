from datetime import date, timedelta

from dagster import AssetExecutionContext, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.assets.raw import daily_partitions
from orchestration.resources import HttpClientResource, IcebergStoreResource
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


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="http")
def raw_macro_calendar(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    fred_client: HttpClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, fred_client, fn=run_macro_calendar, settings=MACRO_CALENDAR_SETTINGS)


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="http")
def raw_macro_series(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    fred_client: HttpClientResource,
) -> MaterializeResult:
    return _run(context, iceberg_store, fred_client, fn=run_macro_series, settings=MACRO_SERIES_SETTINGS)


raw_macro_calendar_job = define_asset_job("raw_macro_calendar_job", selection=[raw_macro_calendar])
raw_macro_series_job = define_asset_job("raw_macro_series_job", selection=[raw_macro_series])

raw_macro_calendar_schedule = build_schedule_from_partitioned_job(raw_macro_calendar_job, hour_of_day=4)
raw_macro_series_schedule = build_schedule_from_partitioned_job(raw_macro_series_job, hour_of_day=1)
