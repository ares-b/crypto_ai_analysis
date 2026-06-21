from datetime import date
from unittest.mock import MagicMock

from core.http import HttpError
from pipelines.raw.etf_flows.config import EtfFlowsSettings
from pipelines.raw.etf_flows.run import _parse_farside, fetch_etf_flows, run_etf_flows
from tests.conftest import MemoryStore

SETTINGS = EtfFlowsSettings(incremental_lookback_days=7)
RUN_DATE = date(2024, 6, 1)

_VALID_HTML = """
<table>
  <tr><th>Date</th><th>GBTC</th><th>Total</th></tr>
  <tr><td>01 Jun 2024</td><td>-100</td><td>200</td></tr>
  <tr><td>31 May 2024</td><td>50</td><td>150</td></tr>
</table>
"""

_NO_TOTAL_HTML = """
<table>
  <tr><th>Date</th><th>GBTC</th></tr>
  <tr><td>01 Jun 2024</td><td>100</td></tr>
</table>
"""


class TestParseFarside:
    def test_parses_valid_html(self):
        flows = _parse_farside(_VALID_HTML)
        assert len(flows) == 2
        assert flows[0].net_flow_usd == 200_000_000.0
        assert flows[0].flow_date.year == 2024

    def test_missing_total_column_returns_empty(self):
        assert _parse_farside(_NO_TOTAL_HTML) == []

    def test_empty_html_returns_empty(self):
        assert _parse_farside("") == []

    def test_skips_unparseable_rows(self):
        html = """
        <table>
          <tr><th>Date</th><th>Total</th></tr>
          <tr><td>bad-date</td><td>100</td></tr>
          <tr><td>01 Jun 2024</td><td>-</td></tr>
          <tr><td>02 Jun 2024</td><td>300</td></tr>
        </table>
        """
        flows = _parse_farside(html)
        assert len(flows) == 1
        assert flows[0].net_flow_usd == 300_000_000.0


class TestFetchRows:
    def test_parses_client_text(self):
        client = MagicMock()
        client.get_text.return_value = _VALID_HTML

        rows = fetch_etf_flows(client)

        assert len(rows) == 2
        client.get_text.assert_called_once()


class TestRunEtfFlows:
    def test_writes_window_rows(self):
        client = MagicMock()
        client.get_text.return_value = _VALID_HTML
        store = MemoryStore()

        metrics = run_etf_flows(
            store=store, logger=__import__("logging").getLogger("test"),
            run_date=RUN_DATE, client=client, settings=SETTINGS,
        )

        assert metrics["rows_affected"] >= 1
        assert metrics["fetch_status"] == "ok"
        assert SETTINGS.table_name in store._tables

    def test_http_error_returns_zero(self):
        client = MagicMock()
        client.get_text.side_effect = HttpError(503, "test")
        store = MemoryStore()

        metrics = run_etf_flows(
            store=store, logger=__import__("logging").getLogger("test"),
            run_date=RUN_DATE, client=client, settings=SETTINGS,
        )

        assert metrics["rows_affected"] == 0
        assert metrics["fetch_status"] == "http_error"
        assert store._tables == {}

    def test_empty_source_returns_zero(self):
        client = MagicMock()
        client.get_text.return_value = ""
        store = MemoryStore()

        metrics = run_etf_flows(
            store=store, logger=__import__("logging").getLogger("test"),
            run_date=RUN_DATE, client=client, settings=SETTINGS,
        )

        assert metrics["rows_affected"] == 0
        assert metrics["fetch_status"] == "empty_source"

    def test_metrics_shape(self):
        client = MagicMock()
        client.get_text.return_value = _VALID_HTML
        store = MemoryStore()

        metrics = run_etf_flows(
            store=store, logger=__import__("logging").getLogger("test"),
            run_date=RUN_DATE, client=client, settings=SETTINGS,
        )

        assert "rows_affected" in metrics
        assert "available_days" in metrics
        assert "written_days" in metrics
        assert "fetch_status" in metrics
