from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.onchain_metrics.config import ONCHAIN_SETTINGS
from pipelines.raw.onchain_metrics.run import onchain_quality_subjects, run_onchain_metrics
from orchestration.partitions import DEPLOY_DATE
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_B
from orchestration.resources import HttpClientResource, IcebergStoreResource

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


@raw_asset(
    name="raw_onchain_metrics",
    source="coinmetrics",
    partitions_def=daily_partitions,
    subjects=onchain_quality_subjects(settings=ONCHAIN_SETTINGS),
)
def raw_onchain_metrics(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    coinmetrics_client: HttpClientResource,
    blockchain_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    result = run_onchain_metrics(
        logger=context.log,
        settings=ONCHAIN_SETTINGS,
        store=iceberg_store.create(),
        coinmetrics_client=coinmetrics_client.create(),
        blockchain_client=blockchain_client.create(),
        run_date=run_date,
    )
    return to_materialize_result(context, result)
# 10:00 UTC - CoinMetrics daily metrics available ~10:00 UTC after overnight processing
raw_onchain_metrics_job = define_asset_job("raw_onchain_metrics_job", selection=[raw_onchain_metrics], tags=TIER_B)
raw_onchain_metrics_schedule = build_schedule_from_partitioned_job(raw_onchain_metrics_job, hour_of_day=10)

JOBS = [raw_onchain_metrics_job]
SCHEDULES = [raw_onchain_metrics_schedule]

ASSETS = [raw_onchain_metrics]
