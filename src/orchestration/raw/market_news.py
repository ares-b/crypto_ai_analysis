from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, build_schedule_from_partitioned_job, define_asset_job

from pipelines.raw.market_news.config import MARKET_NEWS_SETTINGS
from pipelines.raw.market_news.run import market_news_quality_subjects, run_market_news
from orchestration.partitions import DEPLOY_DATE
from orchestration._asset import raw_asset, to_materialize_result
from orchestration._runtime import TIER_B
from orchestration.resources import HttpClientResource, IcebergStoreResource

daily_partitions = DailyPartitionsDefinition(start_date=DEPLOY_DATE, timezone="UTC")


@raw_asset(
    name="raw_market_news",
    source="rss",
    partitions_def=daily_partitions,
    subjects=market_news_quality_subjects(settings=MARKET_NEWS_SETTINGS),
)
def raw_market_news(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    market_news_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    result = run_market_news(
        logger=context.log,
        settings=MARKET_NEWS_SETTINGS,
        store=iceberg_store.create(),
        client=market_news_client.create(),
        run_date=run_date,
    )
    return to_materialize_result(context, result)
# 04:00 UTC - no time constraint; avoid peak hours
raw_market_news_job = define_asset_job("raw_market_news_job", selection=[raw_market_news], tags=TIER_B)
raw_market_news_schedule = build_schedule_from_partitioned_job(raw_market_news_job, hour_of_day=4)

JOBS = [raw_market_news_job]
SCHEDULES = [raw_market_news_schedule]

ASSETS = [raw_market_news]
