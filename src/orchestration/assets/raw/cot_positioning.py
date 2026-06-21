from datetime import date

from dagster import AssetExecutionContext, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.assets.raw import daily_partitions
from orchestration.resources import HttpClientResource, IcebergStoreResource
from pipelines.raw.cot_positioning.config import COT_POSITIONING_SETTINGS
from pipelines.raw.cot_positioning.run import run_cot_positioning


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="http")
def raw_cot_positioning(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    cftc_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    metrics = run_cot_positioning(
        logger=context.log,
        settings=COT_POSITIONING_SETTINGS,
        store=iceberg_store.create(),
        client=cftc_client.create(),
        run_date=run_date,
    )
    return MaterializeResult(metadata=metrics)


raw_cot_positioning_job = define_asset_job("raw_cot_positioning_job", selection=[raw_cot_positioning])
raw_cot_positioning_schedule = build_schedule_from_partitioned_job(raw_cot_positioning_job, hour_of_day=13)
