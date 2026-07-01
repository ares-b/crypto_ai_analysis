import logging

import polars as pl

from core.quality import in_range, not_null, unique
from core.quality.result import Severity
from pipelines.quality import check_frame

_LOGGER = logging.getLogger("test")


class TestCheckFrame:
    def test_passes_clean_frame(self):
        frame = pl.DataFrame({"id": [1, 2], "v": [10, 20]})
        report = check_frame(frame, [not_null("id"), unique("id")], logger=_LOGGER, table="raw.x")
        assert report.ok
        assert report.subject == "raw.x"

    def test_error_failure_does_not_raise_but_marks_not_ok(self):
        frame = pl.DataFrame({"id": [1, 1]})
        report = check_frame(frame, [unique("id")], logger=_LOGGER, table="raw.x")
        assert not report.ok
        assert report.error_failures

    def test_warn_failure_does_not_raise(self):
        frame = pl.DataFrame({"v": [999]})
        report = check_frame(frame, [in_range("v", 0, 100, severity=Severity.WARN)], logger=_LOGGER, table="raw.x")
        assert report.ok and report.to_metrics()["quality_failed_warns"] == 1
