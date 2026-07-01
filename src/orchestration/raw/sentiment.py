from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.sentiment_index.config import SENTIMENT_SETTINGS
from pipelines.raw.sentiment_index.run import run_sentiment_index, sentiment_quality_subjects
from orchestration.partitions import DEPLOY_DATE
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_C
from orchestration.resources import HttpClientResource, IcebergStoreResource

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


@raw_asset(
    name="raw_sentiment_index",
    source="alternative.me",
    partitions_def=daily_partitions,
    subjects=sentiment_quality_subjects(settings=SENTIMENT_SETTINGS),
)
def raw_sentiment_index(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    fear_greed_client: HttpClientResource,
    deribit_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    result = run_sentiment_index(
        logger=context.log,
        settings=SENTIMENT_SETTINGS,
        store=iceberg_store.create(),
        run_date=run_date,
        fear_greed_client=fear_greed_client.create(),
        deribit_client=deribit_client.create() if SENTIMENT_SETTINGS.include_dvol else None,
    )
    return to_materialize_result(context, result)
# 01:00 UTC - Alternative.me updates around midnight UTC
raw_sentiment_index_job = define_asset_job("raw_sentiment_index_job", selection=[raw_sentiment_index], tags=TIER_C)
raw_sentiment_index_schedule = build_schedule_from_partitioned_job(raw_sentiment_index_job, hour_of_day=1)

JOBS = [raw_sentiment_index_job]
SCHEDULES = [raw_sentiment_index_schedule]

ASSETS = [raw_sentiment_index]
