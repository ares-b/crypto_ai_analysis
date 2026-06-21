from datetime import date
from unittest.mock import MagicMock, call

from core.http import HttpError
from pipelines.raw.macro_series.config import MacroSeriesSettings
from pipelines.raw.macro_series.run import fetch_macro_series, run_macro_series
from tests.conftest import MemoryStore

SETTINGS = MacroSeriesSettings(series_ids=("DGS10", "DGS2"))
RUN_DATE = date(2024, 6, 1)


def _make_observations(series_id: str) -> dict:
    return {
        "observations": [
            {"date": "2024-06-01", "value": "4.5" if series_id == "DGS10" else "4.8"}
        ]
    }


class TestFetchRows:
    def test_fetches_each_series(self, logger):
        client = MagicMock()
        client.get_json.side_effect = [_make_observations("DGS10"), _make_observations("DGS2")]

        rows = fetch_macro_series(client, settings=SETTINGS, since=None)

        assert client.get_json.call_count == 2
        assert len(rows) == 2
        series_ids = {r.series_id for r in rows}
        assert series_ids == {"DGS10", "DGS2"}

    def test_since_passed_to_api(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"observations": []}

        fetch_macro_series(client, settings=SETTINGS, since=date(2024, 1, 1))

        params = client.get_json.call_args[1]["params"]
        assert params["observation_start"] == "2024-01-01"

    def test_without_since_omits_param(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"observations": []}

        fetch_macro_series(client, settings=SETTINGS, since=None)

        params = client.get_json.call_args[1]["params"]
        assert "observation_start" not in params

    def test_empty_observations(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"observations": []}

        rows = fetch_macro_series(client, settings=SETTINGS, since=None)

        assert rows == []


class TestRunMacroSeries:
    def test_rows_written_to_store(self, logger):
        client = MagicMock()
        client.get_json.side_effect = [_make_observations("DGS10"), _make_observations("DGS2")]
        store = MemoryStore()

        metrics = run_macro_series(
            store=store, logger=logger, run_date=RUN_DATE,
            client=client, settings=SETTINGS, since=None,
        )

        assert metrics["rows_affected"] == 2
        assert SETTINGS.table_name in store._tables

    def test_http_error_returns_zero(self, logger):
        client = MagicMock()
        client.get_json.side_effect = HttpError(500, "test")
        store = MemoryStore()

        metrics = run_macro_series(
            store=store, logger=logger, run_date=RUN_DATE,
            client=client, settings=SETTINGS, since=None,
        )

        assert metrics["rows_affected"] == 0
        assert store._tables == {}

    def test_empty_rows_skips_upsert(self, logger):
        client = MagicMock()
        client.get_json.return_value = {"observations": []}
        store = MemoryStore()

        metrics = run_macro_series(
            store=store, logger=logger, run_date=RUN_DATE,
            client=client, settings=SETTINGS, since=None,
        )

        assert metrics["rows_affected"] == 0
        assert store._tables == {}
