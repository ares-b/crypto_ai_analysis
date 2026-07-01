from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.exchange_flows.config import EXCHANGE_FLOW_SETTINGS
from pipelines.raw.exchange_flows.run import exchange_flows_quality_subjects, run_exchange_flows
from orchestration.partitions import DEPLOY_DATE
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_C
from orchestration.resources import CryptoQuantClientResource, IcebergStoreResource

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


@raw_asset(
    name="raw_exchange_flows",
    source="cryptoquant",
    partitions_def=daily_partitions,
    subjects=exchange_flows_quality_subjects(settings=EXCHANGE_FLOW_SETTINGS),
)
def raw_exchange_flows(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    cryptoquant_client: CryptoQuantClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    result = run_exchange_flows(
        logger=context.log,
        settings=EXCHANGE_FLOW_SETTINGS,
        store=iceberg_store.create(),
        client=cryptoquant_client.create(),
        since=run_date,
        until=run_date,
    )
    return to_materialize_result(context, result)
# 08:00 UTC - CryptoQuant on-chain settled ~06:00 UTC
raw_exchange_flows_job = define_asset_job("raw_exchange_flows_job", selection=[raw_exchange_flows], tags=TIER_C)
raw_exchange_flows_schedule = build_schedule_from_partitioned_job(raw_exchange_flows_job, hour_of_day=8)

JOBS = [raw_exchange_flows_job]
SCHEDULES = [raw_exchange_flows_schedule]

ASSETS = [raw_exchange_flows]
