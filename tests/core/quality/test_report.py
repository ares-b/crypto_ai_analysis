import polars as pl
import pytest

from core.quality import QualityError, in_range, not_null, unique, validate
from core.quality.result import Severity
from core.iceberg import IcebergRecord


class TestReport:
    def _frame(self):
        return pl.DataFrame({"id": [1, 1], "v": [10, 999]})

    def test_ok_false_on_error_failure(self):
        report = validate(self._frame(), [unique("id")])
        assert not report.ok and len(report.error_failures) == 1

    def test_ok_true_when_only_warns_fail(self):
        report = validate(self._frame(), [in_range("v", 0, 100, severity=Severity.WARN)])
        assert report.ok and len(report.failures) == 1

    def test_raise_for_status_raises_on_error(self):
        report = validate(self._frame(), [unique("id")])
        with pytest.raises(QualityError, match="unique"):
            report.raise_for_status()

    def test_raise_for_status_silent_on_warn(self):
        report = validate(self._frame(), [in_range("v", 0, 100, severity=Severity.WARN)])
        report.raise_for_status()

    def test_to_metrics_counts(self):
        report = validate(
            self._frame(),
            [unique("id"), in_range("v", 0, 100, severity=Severity.WARN), not_null("id")],
        )
        m = report.to_metrics()
        assert m["quality_checks_total"] == 3
        assert m["quality_failed_errors"] == 1
        assert m["quality_failed_warns"] == 1


class TestFromSpec:
    def test_baseline_from_identity(self):
        class _Row(IcebergRecord, table="raw.qtest", identity=("a", "b"), sort=("a",)):
            a: int
            b: str

        checks = _Row.quality_checks()
        names = {c.name for c in checks}
        assert names == {"not_null(a, b)", "unique(a, b)"}
