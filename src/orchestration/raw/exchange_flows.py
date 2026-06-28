from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.partitions import DEPLOY_DATE

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
from pipelines.raw.exchange_flows.config import EXCHANGE_FLOW_SETTINGS
from pipelines.raw.exchange_flows.run import run_exchange_flows
from orchestration._runtime import DEFAULT_RETRY_POLICY, TIER_C
from orchestration.resources import CryptoQuantClientResource, IcebergStoreResource


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=daily_partitions, group_name="raw", compute_kind="python", tags={"source": "cryptoquant"}, retry_policy=DEFAULT_RETRY_POLICY)
def raw_exchange_flows(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    cryptoquant_client: CryptoQuantClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    metrics = run_exchange_flows(
        logger=context.log,
        settings=EXCHANGE_FLOW_SETTINGS,
        store=iceberg_store.create(),
        client=cryptoquant_client.create(),
        since=run_date,
        until=run_date,
    )
    return MaterializeResult(metadata=metrics)
# 08:00 UTC - CryptoQuant on-chain settled ~06:00 UTC
raw_exchange_flows_job = define_asset_job("raw_exchange_flows_job", selection=[raw_exchange_flows], tags=TIER_C)
raw_exchange_flows_schedule = build_schedule_from_partitioned_job(raw_exchange_flows_job, hour_of_day=8)

JOBS = [raw_exchange_flows_job]
SCHEDULES = [raw_exchange_flows_schedule]

ASSETS = [raw_exchange_flows]
