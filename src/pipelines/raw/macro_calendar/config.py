import os

from pydantic import BaseModel, ConfigDict, Field


class MacroCalendarSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    table_name: str = "raw.macro_calendar"
    fred_base_url: str = "https://api.stlouisfed.org"
    fred_api_key: str = Field(default_factory=lambda: os.environ.get("FRED_API_KEY", ""))
    incremental_lookback_days: int = 30
    request_timeout_seconds: float = 30.0
    # Curated high-impact releases (FRED release_id, name). Fetched one at a time via
    # /fred/release/dates; the all-releases /fred/releases/dates endpoint is too large.
    releases: tuple[tuple[int, str], ...] = (
        (10, "Consumer Price Index"),
        (50, "Employment Situation"),
        (53, "Gross Domestic Product"),
        (54, "Personal Income and Outlays"),
        (18, "H.15 Selected Interest Rates"),
    )


MACRO_CALENDAR_SETTINGS = MacroCalendarSettings()
