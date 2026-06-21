from dagster import Definitions

from orchestration.assets.raw.candles import (
    binance_candles_daily,
    binance_candles_weekly,
    daily_candles_schedule,
    raw_daily_candles_job,
    raw_weekly_candles_job,
    weekly_candles_schedule,
)
from orchestration.assets.raw.cot_positioning import (
    raw_cot_positioning,
    raw_cot_positioning_job,
    raw_cot_positioning_schedule,
)
from orchestration.assets.raw.etf_flows import (
    raw_etf_flows,
    raw_etf_flows_job,
    raw_etf_flows_schedule,
)
from orchestration.assets.raw.exchange_flows import (
    raw_exchange_flows,
    raw_exchange_flows_job,
    raw_exchange_flows_schedule,
)
from orchestration.assets.raw.futures import (
    raw_funding_rates,
    raw_funding_rates_job,
    raw_funding_rates_schedule,
    raw_futures_metrics,
    raw_futures_metrics_job,
    raw_futures_metrics_schedule,
    raw_long_short_ratio,
    raw_long_short_ratio_job,
    raw_long_short_ratio_schedule,
)
from orchestration.assets.raw.macro import (
    raw_macro_calendar,
    raw_macro_calendar_job,
    raw_macro_calendar_schedule,
    raw_macro_series,
    raw_macro_series_job,
    raw_macro_series_schedule,
)
from orchestration.assets.raw.market_metrics import (
    raw_market_metrics,
    raw_market_metrics_job,
    raw_market_metrics_schedule,
    raw_stablecoin_supply,
    raw_stablecoin_supply_job,
    raw_stablecoin_supply_schedule,
)
from orchestration.assets.raw.market_news import (
    raw_market_news,
    raw_market_news_job,
    raw_market_news_schedule,
)
from orchestration.assets.raw.onchain_metrics import (
    raw_onchain_metrics,
    raw_onchain_metrics_job,
    raw_onchain_metrics_schedule,
)
from orchestration.assets.raw.sentiment import (
    raw_sentiment_index,
    raw_sentiment_index_job,
    raw_sentiment_index_schedule,
)
from orchestration.resources import (
    BinanceClientResource,
    CryptoQuantClientResource,
    HttpClientResource,
    IcebergStoreResource,
)
from pipelines.raw.cot_positioning.config import COT_POSITIONING_SETTINGS
from pipelines.raw.etf_flows.config import ETF_FLOWS_SETTINGS
from pipelines.raw.macro_calendar.config import MACRO_CALENDAR_SETTINGS
from pipelines.raw.market_metrics.config import MARKET_METRIC_SETTINGS
from pipelines.raw.onchain_metrics.config import ONCHAIN_SETTINGS
from pipelines.raw.sentiment_index.config import SENTIMENT_SETTINGS

defs = Definitions(
    assets=[
        binance_candles_daily,
        binance_candles_weekly,
        raw_cot_positioning,
        raw_etf_flows,
        raw_exchange_flows,
        raw_funding_rates,
        raw_futures_metrics,
        raw_long_short_ratio,
        raw_macro_calendar,
        raw_macro_series,
        raw_market_metrics,
        raw_market_news,
        raw_onchain_metrics,
        raw_sentiment_index,
        raw_stablecoin_supply,
    ],
    jobs=[
        raw_daily_candles_job,
        raw_weekly_candles_job,
        raw_cot_positioning_job,
        raw_etf_flows_job,
        raw_exchange_flows_job,
        raw_funding_rates_job,
        raw_futures_metrics_job,
        raw_long_short_ratio_job,
        raw_macro_calendar_job,
        raw_macro_series_job,
        raw_market_metrics_job,
        raw_market_news_job,
        raw_onchain_metrics_job,
        raw_sentiment_index_job,
        raw_stablecoin_supply_job,
    ],
    schedules=[
        daily_candles_schedule,
        weekly_candles_schedule,
        raw_cot_positioning_schedule,
        raw_etf_flows_schedule,
        raw_exchange_flows_schedule,
        raw_funding_rates_schedule,
        raw_futures_metrics_schedule,
        raw_long_short_ratio_schedule,
        raw_macro_calendar_schedule,
        raw_macro_series_schedule,
        raw_market_metrics_schedule,
        raw_market_news_schedule,
        raw_onchain_metrics_schedule,
        raw_sentiment_index_schedule,
        raw_stablecoin_supply_schedule,
    ],
    resources={
        "iceberg_store": IcebergStoreResource(),
        "binance_client": BinanceClientResource(),
        "cftc_client": HttpClientResource(base_url=COT_POSITIONING_SETTINGS.cftc_base_url),
        "farside_client": HttpClientResource(base_url=ETF_FLOWS_SETTINGS.farside_url),
        "fred_client": HttpClientResource(base_url=MACRO_CALENDAR_SETTINGS.fred_base_url),
        "coingecko_client": HttpClientResource(base_url=MARKET_METRIC_SETTINGS.coingecko_base_url),
        "market_news_client": HttpClientResource(),
        "coinmetrics_client": HttpClientResource(base_url=ONCHAIN_SETTINGS.coinmetrics_base_url),
        "blockchain_client": HttpClientResource(base_url=ONCHAIN_SETTINGS.blockchain_base_url),
        "fear_greed_client": HttpClientResource(base_url=SENTIMENT_SETTINGS.fear_greed_base_url),
        "deribit_client": HttpClientResource(base_url=SENTIMENT_SETTINGS.deribit_base_url),
        "cryptoquant_client": CryptoQuantClientResource(),
    },
)
