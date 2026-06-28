from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.partitions import DEPLOY_DATE

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")
from pipelines.raw.onchain_metrics.config import ONCHAIN_SETTINGS
from pipelines.raw.onchain_metrics.run import run_onchain_metrics
from orchestration._runtime import DEFAULT_RETRY_POLICY, TIER_B
from orchestration.resources import HttpClientResource, IcebergStoreResource


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=daily_partitions, group_name="raw", compute_kind="python", tags={"source": "coinmetrics"}, retry_policy=DEFAULT_RETRY_POLICY)
def raw_onchain_metrics(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    coinmetrics_client: HttpClientResource,
    blockchain_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    metrics = run_onchain_metrics(
        logger=context.log,
        settings=ONCHAIN_SETTINGS,
        store=iceberg_store.create(),
        coinmetrics_client=coinmetrics_client.create(),
        blockchain_client=blockchain_client.create(),
        run_date=run_date,
    )
    return MaterializeResult(metadata=metrics)
# 10:00 UTC - CoinMetrics daily metrics available ~10:00 UTC after overnight processing
raw_onchain_metrics_job = define_asset_job("raw_onchain_metrics_job", selection=[raw_onchain_metrics], tags=TIER_B)
raw_onchain_metrics_schedule = build_schedule_from_partitioned_job(raw_onchain_metrics_job, hour_of_day=10)

JOBS = [raw_onchain_metrics_job]
SCHEDULES = [raw_onchain_metrics_schedule]

ASSETS = [raw_onchain_metrics]
