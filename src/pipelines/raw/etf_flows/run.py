import logging
from datetime import UTC, date, datetime, timedelta
from html.parser import HTMLParser

from core.http import HttpClient, HttpError
from core.quality import QualitySubject, RunResult
from core.storage import Store
from pipelines import MetricValue
from pipelines.quality import check_frame

from .config import EtfFlowsSettings
from .models import EtfFlow, EtfFlowRow, build_rows


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._rows: list[list[str]] = []
        self._current_row: list[str] = []
        self._in_cell = False
        self._cell_buf = ""

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag == "tr":
            self._current_row = []
        elif tag in ("td", "th"):
            self._in_cell = True
            self._cell_buf = ""

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_buf += data

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._in_cell:
            self._current_row.append(self._cell_buf.strip())
            self._in_cell = False
        elif tag == "tr" and self._current_row:
            self._rows.append(self._current_row)
            self._current_row = []

    @property
    def rows(self) -> list[list[str]]:
        return self._rows


def _parse_farside(html: str) -> list[EtfFlow]:
    parser = _TableParser()
    parser.feed(html)
    table_rows = parser.rows

    # The page has several tables (nav, etc.); find the data header row that has
    # both Date and Total, then parse the rows after it.
    date_idx = total_idx = header_idx = None
    for i, row in enumerate(table_rows):
        cells = [c.lower().strip() for c in row]
        if "date" in cells and "total" in cells:
            date_idx, total_idx, header_idx = cells.index("date"), cells.index("total"), i
            break
    if header_idx is None:
        return []

    flows: list[EtfFlow] = []
    for row in table_rows[header_idx + 1:]:
        if len(row) <= max(date_idx, total_idx):
            continue
        try:
            flow_date = datetime.strptime(row[date_idx].strip(), "%d %b %Y").date()
            raw = row[total_idx].replace(",", "").replace("$", "").strip()
            if not raw or raw in ("-", "N/A", ""):
                continue
            # Farside publishes values in USD millions
            flows.append(EtfFlow(flow_date=flow_date, net_flow_usd=float(raw) * 1_000_000))
        except (ValueError, IndexError):
            continue

    return flows


def fetch_etf_flows(client: HttpClient) -> list[EtfFlowRow]:
    html = client.get_text()
    return build_rows(_parse_farside(html), source_updated_at=datetime.now(UTC))


def etf_flows_quality_subjects(*, settings: EtfFlowsSettings) -> list[QualitySubject]:
    return [(settings.table_name, EtfFlowRow.quality_checks())]


def run_etf_flows(
    *,
    store: Store,
    logger: logging.Logger,
    run_date: date,
    client: HttpClient,
    settings: EtfFlowsSettings,
) -> RunResult:
    try:
        rows = fetch_etf_flows(client)
    except HttpError as exc:
        logger.warning(f"[etf_flows] source unavailable for {run_date.isoformat()}: {exc}")
        return RunResult({"rows_affected": 0, "available_days": 0, "written_days": 0, "fetch_status": "http_error"})
    if not rows:
        logger.warning(f"[etf_flows] source returned no parsable rows for {run_date.isoformat()}")
        return RunResult({"rows_affected": 0, "available_days": 0, "written_days": 0, "fetch_status": "empty_source"})

    window_start = run_date - timedelta(days=settings.incremental_lookback_days)
    window_rows = [row for row in rows if window_start <= row.date <= run_date]
    day_rows = [row for row in window_rows if row.date == run_date]
    rows_affected = 0
    quality_metrics: dict[str, MetricValue] = {}
    reports = []
    if window_rows:
        frame = EtfFlowRow.to_frame(window_rows)
        report = check_frame(frame, EtfFlowRow.quality_checks(), logger=logger, table=settings.table_name)
        reports.append(report)
        quality_metrics = report.to_metrics()
        if report.ok:
            rows_affected = store.upsert(settings.table_name, frame).rows_affected
    if not day_rows:
        logger.warning(f"[etf_flows] no flow published for {run_date.isoformat()}")
    else:
        logger.info(f"[etf_flows] {run_date.isoformat()} net_flow_usd={day_rows[0].net_flow_usd}")
    return RunResult({
        "rows_affected": rows_affected,
        "available_days": len(rows),
        "written_days": len(window_rows),
        "fetch_status": "ok",
        **quality_metrics,
    }, tuple(reports))
