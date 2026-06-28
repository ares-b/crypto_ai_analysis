from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.partitions import DEPLOY_DATE

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
from pipelines.raw.etf_flows.config import ETF_FLOWS_SETTINGS
from pipelines.raw.etf_flows.run import run_etf_flows
from orchestration._runtime import DEFAULT_RETRY_POLICY, TIER_C
from orchestration.resources import HttpClientResource, IcebergStoreResource


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=daily_partitions, group_name="raw", compute_kind="python", tags={"source": "farside"}, retry_policy=DEFAULT_RETRY_POLICY)
def raw_etf_flows(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    farside_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    metrics = run_etf_flows(
        logger=context.log,
        settings=ETF_FLOWS_SETTINGS,
        store=iceberg_store.create(),
        client=farside_client.create(),
        run_date=run_date,
    )
    return MaterializeResult(metadata=metrics)
# 23:00 UTC - Farside updates ~1h after US market close (21:00 UTC)
raw_etf_flows_job = define_asset_job("raw_etf_flows_job", selection=[raw_etf_flows], tags=TIER_C)
raw_etf_flows_schedule = build_schedule_from_partitioned_job(raw_etf_flows_job, hour_of_day=23)

JOBS = [raw_etf_flows_job]
SCHEDULES = [raw_etf_flows_schedule]

ASSETS = [raw_etf_flows]
