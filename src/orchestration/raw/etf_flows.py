from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.etf_flows.config import ETF_FLOWS_SETTINGS
from pipelines.raw.etf_flows.run import etf_flows_quality_subjects, run_etf_flows
from orchestration.partitions import DEPLOY_DATE
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_C
from orchestration.resources import HttpClientResource, IcebergStoreResource

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


@raw_asset(
    name="raw_etf_flows",
    source="farside",
    partitions_def=daily_partitions,
    subjects=etf_flows_quality_subjects(settings=ETF_FLOWS_SETTINGS),
)
def raw_etf_flows(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    farside_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    result = run_etf_flows(
        logger=context.log,
        settings=ETF_FLOWS_SETTINGS,
        store=iceberg_store.create(),
        client=farside_client.create(),
        run_date=run_date,
    )
    return to_materialize_result(context, result)
# 23:00 UTC - Farside updates ~1h after US market close (21:00 UTC)
raw_etf_flows_job = define_asset_job("raw_etf_flows_job", selection=[raw_etf_flows], tags=TIER_C)
raw_etf_flows_schedule = build_schedule_from_partitioned_job(raw_etf_flows_job, hour_of_day=23)

JOBS = [raw_etf_flows_job]
SCHEDULES = [raw_etf_flows_schedule]

ASSETS = [raw_etf_flows]
