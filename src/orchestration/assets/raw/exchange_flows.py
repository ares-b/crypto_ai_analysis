from datetime import date

from dagster import AssetExecutionContext, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.assets.raw import daily_partitions
from orchestration.resources import CryptoQuantClientResource, IcebergStoreResource
from pipelines.raw.exchange_flows.config import EXCHANGE_FLOW_SETTINGS
from pipelines.raw.exchange_flows.run import run_exchange_flows


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="http")
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


raw_exchange_flows_job = define_asset_job("raw_exchange_flows_job", selection=[raw_exchange_flows])
raw_exchange_flows_schedule = build_schedule_from_partitioned_job(raw_exchange_flows_job, hour_of_day=14)
