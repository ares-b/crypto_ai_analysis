import os

from pydantic import BaseModel, ConfigDict, Field

DEFAULT_SERIES_IDS: tuple[str, ...] = (
    "DGS10",
    "DGS2",
    "DTWEXBGS",
    "CPIAUCSL",
    "PCEPI",
    "UNRATE",
    "FEDFUNDS",
    "M2SL",
)


class MacroSeriesSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.macro_series"
    fred_base_url: str = "https://api.stlouisfed.org"
    fred_api_key: str = Field(default_factory=lambda: os.environ.get("FRED_API_KEY", ""))
    series_ids: tuple[str, ...] = DEFAULT_SERIES_IDS
    incremental_lookback_days: int = 14
    request_timeout_seconds: float = 30.0


MACRO_SERIES_SETTINGS = MacroSeriesSettings()
