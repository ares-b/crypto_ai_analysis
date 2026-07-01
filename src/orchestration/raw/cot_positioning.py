from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.cot_positioning.config import COT_POSITIONING_SETTINGS
from pipelines.raw.cot_positioning.run import cot_positioning_quality_subjects, run_cot_positioning
from orchestration.partitions import DEPLOY_DATE
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_C
from orchestration.resources import HttpClientResource, IcebergStoreResource

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


@raw_asset(
    name="raw_cot_positioning",
    source="cftc",
    partitions_def=daily_partitions,
    subjects=cot_positioning_quality_subjects(settings=COT_POSITIONING_SETTINGS),
)
def raw_cot_positioning(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    cftc_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    result = run_cot_positioning(
        logger=context.log,
        settings=COT_POSITIONING_SETTINGS,
        store=iceberg_store.create(),
        client=cftc_client.create(),
        run_date=run_date,
    )
    return to_materialize_result(context, result)
# 21:00 UTC - CFTC publishes CoT Fridays at 20:30 UTC
raw_cot_positioning_job = define_asset_job("raw_cot_positioning_job", selection=[raw_cot_positioning], tags=TIER_C)
raw_cot_positioning_schedule = build_schedule_from_partitioned_job(raw_cot_positioning_job, hour_of_day=21)

JOBS = [raw_cot_positioning_job]
SCHEDULES = [raw_cot_positioning_schedule]

ASSETS = [raw_cot_positioning]
