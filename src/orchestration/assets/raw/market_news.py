from datetime import date

from dagster import AssetExecutionContext, MaterializeResult, asset, build_schedule_from_partitioned_job, define_asset_job

from orchestration.assets.raw import daily_partitions
from orchestration.resources import HttpClientResource, IcebergStoreResource
from pipelines.raw.market_news.config import MARKET_NEWS_SETTINGS
from pipelines.raw.market_news.run import run_market_news


@asset(partitions_def=daily_partitions, group_name="raw", compute_kind="http")
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


raw_market_news_job = define_asset_job("raw_market_news_job", selection=[raw_market_news])
raw_market_news_schedule = build_schedule_from_partitioned_job(raw_market_news_job, hour_of_day=12)
