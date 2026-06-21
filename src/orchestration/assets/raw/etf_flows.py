from datetime import date

from dagster import AssetExecutionContext, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.assets.raw import daily_partitions
from orchestration.resources import HttpClientResource, IcebergStoreResource
from pipelines.raw.etf_flows.config import ETF_FLOWS_SETTINGS
from pipelines.raw.etf_flows.run import run_etf_flows


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="http")
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


raw_etf_flows_job = define_asset_job("raw_etf_flows_job", selection=[raw_etf_flows])
raw_etf_flows_schedule = build_schedule_from_partitioned_job(raw_etf_flows_job, hour_of_day=5)
