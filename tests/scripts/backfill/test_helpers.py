import argparse
import logging
from datetime import UTC, date, datetime

import polars as pl

from core.quality import not_null, unique
from scripts.backfill import helpers
from tests.conftest import MemoryStore

_ID = ("instrument", "open_time")
_CHECKS = [not_null(*_ID), unique(*_ID)]
_LOGGER = logging.getLogger("test")


def _frame(dates):
    return pl.DataFrame({"instrument": ["BTC"] * len(dates), "open_time": dates, "v": list(range(len(dates)))})


class TestSelectNew:
    def test_empty_existing_returns_all(self):
        assert helpers.select_new(_frame([date(2024, 6, 1)]), pl.DataFrame(), _ID).height == 1

    def test_filters_present(self):
        frame = _frame([date(2024, 6, 1), date(2024, 6, 2)])
        existing = _frame([date(2024, 6, 1)]).select(_ID)
        out = helpers.select_new(frame, existing, _ID)
        assert out["open_time"].to_list() == [date(2024, 6, 2)]


class TestCommit:
    def test_appends_only_new_rows(self):
        store = MemoryStore()
        store.append("raw.x", _frame([date(2024, 6, 1)]))
        n = helpers.commit(store, "raw.x", _frame([date(2024, 6, 1), date(2024, 6, 2)]), _ID,
                           checks=_CHECKS, logger=_LOGGER, dry_run=False)
        assert n == 1
        assert store._tables["raw.x"]["open_time"].to_list() == [date(2024, 6, 1), date(2024, 6, 2)]

    def test_dry_run_writes_nothing(self):
        store = MemoryStore()
        n = helpers.commit(store, "raw.x", _frame([date(2024, 6, 1)]), _ID,
                           checks=_CHECKS, logger=_LOGGER, dry_run=True)
        assert n == 0 and "raw.x" not in store._tables

    def test_error_check_aborts_before_write(self):
        store = MemoryStore()
        dupes = _frame([date(2024, 6, 1), date(2024, 6, 1)])
        import pytest

        from core.quality import QualityError
        with pytest.raises(QualityError):
            helpers.commit(store, "raw.x", dupes, _ID, checks=_CHECKS, logger=_LOGGER, dry_run=False)
        assert "raw.x" not in store._tables


class TestWindow:
    def test_dates_to_utc_datetimes(self):
        args = argparse.Namespace(start=date(2024, 6, 1), end=date(2024, 6, 3))
        start, end = helpers.window(args)
        assert start == datetime(2024, 6, 1, tzinfo=UTC)
        assert end == datetime(2024, 6, 3, tzinfo=UTC)
