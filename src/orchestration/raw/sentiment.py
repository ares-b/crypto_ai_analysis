from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

daily_partitions = DailyPartitionsDefinition(start_date="2026-06-14", timezone="UTC")
from pipelines.raw.sentiment_index.config import SENTIMENT_SETTINGS
from pipelines.raw.sentiment_index.run import run_sentiment_index


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=daily_partitions, group_name="raw", compute_kind="python", tags={"source": "alternative.me"})
def raw_sentiment_index(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    fear_greed_client: HttpClientResource,
    deribit_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    metrics = run_sentiment_index(
        logger=context.log,
        settings=SENTIMENT_SETTINGS,
        store=iceberg_store.create(),
        run_date=run_date,
        fear_greed_client=fear_greed_client.create(),
        deribit_client=deribit_client.create() if SENTIMENT_SETTINGS.include_dvol else None,
    )
    return MaterializeResult(metadata=metrics)
# 01:00 UTC - Alternative.me updates around midnight UTC
raw_sentiment_index_job = define_asset_job("raw_sentiment_index_job", selection=[raw_sentiment_index])
raw_sentiment_index_schedule = build_schedule_from_partitioned_job(raw_sentiment_index_job, hour_of_day=1)

JOBS = [raw_sentiment_index_job]
SCHEDULES = [raw_sentiment_index_schedule]

ASSETS = [raw_sentiment_index]
