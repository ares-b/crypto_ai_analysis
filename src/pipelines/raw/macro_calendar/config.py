import os

from pydantic import BaseModel, ConfigDict, Field


class MacroCalendarSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.macro_calendar"
    fred_base_url: str = "https://api.stlouisfed.org"
    fred_api_key: str = Field(default_factory=lambda: os.environ.get("FRED_API_KEY", ""))
    incremental_lookback_days: int = 30
    request_timeout_seconds: float = 30.0


MACRO_CALENDAR_SETTINGS = MacroCalendarSettings()
