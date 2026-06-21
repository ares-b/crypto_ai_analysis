from datetime import UTC, date, datetime

from pipelines.raw.sentiment_index.models import (
    DeribitDvolRow,
    DeribitPutCallRow,
    SentimentRow,
    build_dvol_rows,
    build_rows,
)

_NOW = datetime(2024, 6, 1, tzinfo=UTC)


def _make_fear_greed_item(ts: int, value: int = 65, label: str = "Greed") -> dict:
    return {"timestamp": str(ts), "value": str(value), "value_classification": label}


class TestBuildRows:
    def test_valid_items(self):
        items = [_make_fear_greed_item(1717200000, 65), _make_fear_greed_item(1717113600, 42)]
        rows = build_rows(items, source_updated_at=_NOW)
        assert len(rows) == 2
        assert all(isinstance(r, SentimentRow) for r in rows)

    def test_sorted_by_date(self):
        # Older date first, newer second
        items = [
            _make_fear_greed_item(1717200000, 65),  # 2024-06-01
            _make_fear_greed_item(1717113600, 42),  # 2024-05-31
        ]
        rows = build_rows(items, source_updated_at=_NOW)
        assert rows[0].date < rows[1].date

    def test_empty_returns_empty(self):
        assert build_rows([], source_updated_at=_NOW) == []

    def test_to_frame(self):
        rows = build_rows([_make_fear_greed_item(1717200000)], source_updated_at=_NOW)
        frame = SentimentRow.to_frame(rows)
        assert "fear_greed_value" in frame.columns
        assert len(frame) == 1


class TestBuildDvolRows:
    def test_valid(self):
        dvol = {date(2024, 6, 1): 55.0, date(2024, 5, 31): 60.0}
        rows = build_dvol_rows(dvol, source_updated_at=_NOW)
        assert len(rows) == 2
        assert rows[0].date == date(2024, 5, 31)  # sorted ascending
        assert rows[0].dvol == 60.0

    def test_empty_returns_empty(self):
        assert build_dvol_rows({}, source_updated_at=_NOW) == []

    def test_to_frame(self):
        dvol = {date(2024, 6, 1): 55.0}
        rows = build_dvol_rows(dvol, source_updated_at=_NOW)
        frame = DeribitDvolRow.to_frame(rows)
        assert "dvol" in frame.columns
        assert len(frame) == 1


class TestDeribitPutCallRow:
    def test_to_frame(self):
        row = DeribitPutCallRow(
            date=date(2024, 6, 1),
            put_oi=1000.0,
            call_oi=2000.0,
            put_call_ratio=0.5,
            source_updated_at=_NOW,
        )
        frame = DeribitPutCallRow.to_frame([row])
        assert "put_call_ratio" in frame.columns
        assert len(frame) == 1
