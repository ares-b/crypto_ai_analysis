from datetime import date
from unittest.mock import MagicMock

from core.http import HttpError
from pipelines.raw.macro_calendar.config import MacroCalendarSettings
from pipelines.raw.macro_calendar.run import _LIMIT, fetch_macro_calendar, run_macro_calendar
from tests.conftest import MemoryStore

# Single curated release for deterministic counts.
SETTINGS = MacroCalendarSettings(releases=((10, "Consumer Price Index"),))
RUN_DATE = date(2024, 6, 1)


def _make_release_date(date_str: str = "2024-06-01") -> dict:
    # release_id / release_name are injected by fetch from settings.releases.
    return {"date": date_str}


class TestFetchRows:
    def test_single_page(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"release_dates": [_make_release_date()]}

        rows = fetch_macro_calendar(client, settings=SETTINGS, since=None)

        assert len(rows) == 1
        assert rows[0].title == "Consumer Price Index"
        assert rows[0].event_id == "10:2024-06-01"

    def test_uses_singular_release_endpoint(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"release_dates": []}

        fetch_macro_calendar(client, settings=SETTINGS, since=None)

        path = client.get_json.call_args[0][0]
        params = client.get_json.call_args[1]["params"]
        assert path == "/fred/release/dates"
        assert params["release_id"] == 10

    def test_pagination_triggers_next_request(self, logger):
        client = MagicMock()
        full_batch = [_make_release_date("2024-06-01") for _ in range(_LIMIT)]
        client.get_json.side_effect = [
            {"release_dates": full_batch},
            {"release_dates": [_make_release_date("2024-04-01")]},
        ]

        rows = fetch_macro_calendar(client, settings=SETTINGS, since=None)

        assert client.get_json.call_count == 2
        assert len(rows) == _LIMIT + 1

    def test_since_passed_to_api(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"release_dates": []}

        fetch_macro_calendar(client, settings=SETTINGS, since=date(2024, 1, 1))

        params = client.get_json.call_args[1]["params"]
        assert params["realtime_start"] == "2024-01-01"

    def test_without_since_omits_param(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"release_dates": []}

        fetch_macro_calendar(client, settings=SETTINGS, since=None)

        params = client.get_json.call_args[1]["params"]
        assert "realtime_start" not in params

    def test_bad_release_id_skipped(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(400, "bad release")

        rows = fetch_macro_calendar(client, settings=SETTINGS, since=None)

        assert rows == []


class TestRunMacroCalendar:
    def test_rows_written_to_store(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"release_dates": [_make_release_date()]}
        store = MemoryStore()

        metrics = run_macro_calendar(
            store=store, logger=logger, run_date=RUN_DATE,
            client=client, settings=SETTINGS, since=None,
        )

        assert metrics["rows_affected"] == 1
        assert SETTINGS.table_name in store._tables

    def test_http_error_returns_zero(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(429, "test")
        store = MemoryStore()

        metrics = run_macro_calendar(
            store=store, logger=logger, run_date=RUN_DATE,
            client=client, settings=SETTINGS, since=None,
        )

        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_empty_rows_skips_upsert(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"release_dates": []}
        store = MemoryStore()

        metrics = run_macro_calendar(
            store=store, logger=logger, run_date=RUN_DATE,
            client=client, settings=SETTINGS, since=None,
        )

        assert metrics["rows_affected"] == 0
        assert store._tables == {}
