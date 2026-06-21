from datetime import UTC, date, datetime

from pipelines.raw.etf_flows.models import EtfFlow, EtfFlowRow, build_rows

_NOW = datetime(2024, 6, 1, tzinfo=UTC)


class TestBuildRows:
    def test_maps_flow_fields(self):
        flows = [EtfFlow(flow_date=date(2024, 6, 1), net_flow_usd=1_000_000.0)]
        rows = build_rows(flows, source_updated_at=_NOW)

        assert len(rows) == 1
        assert rows[0].date == date(2024, 6, 1)
        assert rows[0].net_flow_usd == 1_000_000.0
        assert rows[0].source_updated_at == _NOW

    def test_empty_flows(self):
        assert build_rows([], source_updated_at=_NOW) == []

    def test_to_frame(self):
        flows = [EtfFlow(flow_date=date(2024, 6, 1), net_flow_usd=500_000.0)]
        rows = build_rows(flows, source_updated_at=_NOW)
        frame = EtfFlowRow.to_frame(rows)

        assert "date" in frame.columns
        assert "net_flow_usd" in frame.columns
        assert len(frame) == 1
