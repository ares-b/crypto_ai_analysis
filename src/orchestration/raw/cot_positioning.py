from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.partitions import DEPLOY_DATE

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
from pipelines.raw.cot_positioning.config import COT_POSITIONING_SETTINGS
from pipelines.raw.cot_positioning.run import run_cot_positioning
from orchestration._runtime import DEFAULT_RETRY_POLICY, TIER_C
from orchestration.resources import HttpClientResource, IcebergStoreResource


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=daily_partitions, group_name="raw", compute_kind="python", tags={"source": "cftc"}, retry_policy=DEFAULT_RETRY_POLICY)
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
# 21:00 UTC - CFTC publishes CoT Fridays at 20:30 UTC
raw_cot_positioning_job = define_asset_job("raw_cot_positioning_job", selection=[raw_cot_positioning], tags=TIER_C)
raw_cot_positioning_schedule = build_schedule_from_partitioned_job(raw_cot_positioning_job, hour_of_day=21)

JOBS = [raw_cot_positioning_job]
SCHEDULES = [raw_cot_positioning_schedule]

ASSETS = [raw_cot_positioning]
