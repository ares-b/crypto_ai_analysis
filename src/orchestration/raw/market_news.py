from datetime import date

from dagster import AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

daily_partitions = DailyPartitionsDefinition(start_date="2026-06-20", timezone="UTC")
from pipelines.raw.market_news.config import MARKET_NEWS_SETTINGS
from pipelines.raw.market_news.run import run_market_news


@asset(key_prefix=["crypto-ai-analysis"], partitions_def=daily_partitions, group_name="raw", compute_kind="python", tags={"source": "rss"})
def raw_market_news(
    context: AssetExecutionContext,
    iceberg_store: IcebergStoreResource,
    market_news_client: HttpClientResource,
) -> MaterializeResult:
    run_date = date.fromisoformat(context.partition_key)
    metrics = run_market_news(
        logger=context.log,
        settings=MARKET_NEWS_SETTINGS,
        store=iceberg_store.create(),
        client=market_news_client.create(),
        run_date=run_date,
    )
    return MaterializeResult(metadata=metrics)
# 04:00 UTC - no time constraint; avoid peak hours
raw_market_news_job = define_asset_job("raw_market_news_job", selection=[raw_market_news])
raw_market_news_schedule = build_schedule_from_partitioned_job(raw_market_news_job, hour_of_day=4)

JOBS = [raw_market_news_job]
SCHEDULES = [raw_market_news_schedule]

ASSETS = [raw_market_news]
