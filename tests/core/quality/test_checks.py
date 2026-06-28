from datetime import date

import polars as pl

from core.quality import (
    accepted_values,
    expression,
    in_range,
    matches_regex,
    not_empty,
    not_null,
    row_count_between,
    time_in_window,
    unique,
    validate,
)
from core.quality.result import Severity


def _result(frame, check):
    return validate(frame, [check]).results[0]


class TestRowChecks:
    def test_not_null_flags_nulls(self):
        r = _result(pl.DataFrame({"a": [1, None, 3]}), not_null("a"))
        assert not r.passed and r.failed_rows == 1

    def test_not_null_passes_clean(self):
        assert _result(pl.DataFrame({"a": [1, 2]}), not_null("a")).passed

    def test_not_null_multi_column(self):
        r = _result(pl.DataFrame({"a": [1, None], "b": [None, 2]}), not_null("a", "b"))
        assert r.failed_rows == 2

    def test_in_range_bounds_inclusive(self):
        r = _result(pl.DataFrame({"v": [0, 50, 100, 101]}), in_range("v", 0, 100))
        assert r.failed_rows == 1 and r.sample == [{"v": 101}]

    def test_in_range_one_sided(self):
        assert _result(pl.DataFrame({"v": [-1, 0, 5]}), in_range("v", 0, None)).failed_rows == 1

    def test_in_range_treats_null_as_fail(self):
        assert _result(pl.DataFrame({"v": [None, 5]}), in_range("v", 0, 10)).failed_rows == 1

    def test_accepted_values(self):
        r = _result(pl.DataFrame({"s": ["a", "b", "z"]}), accepted_values("s", ["a", "b"]))
        assert r.failed_rows == 1

    def test_matches_regex(self):
        r = _result(pl.DataFrame({"s": ["btc-usd", "bad"]}), matches_regex("s", r"^[a-z]+-[a-z]+$"))
        assert r.failed_rows == 1

    def test_time_in_window(self):
        f = pl.DataFrame({"d": [date(2024, 1, 1), date(2024, 6, 1), date(2025, 1, 1)]})
        r = _result(f, time_in_window("d", date(2024, 1, 1), date(2024, 12, 31)))
        assert r.failed_rows == 1

    def test_expression_escape_hatch(self):
        f = pl.DataFrame({"h": [10, 5], "l": [1, 9]})
        r = _result(f, expression("h>=l", pl.col("h") >= pl.col("l")))
        assert r.failed_rows == 1


class TestFrameChecks:
    def test_not_empty(self):
        assert not _result(pl.DataFrame(), not_empty()).passed
        assert _result(pl.DataFrame({"a": [1]}), not_empty()).passed

    def test_unique_flags_dupes(self):
        r = _result(pl.DataFrame({"k": [1, 1, 2]}), unique("k"))
        assert r.failed_rows == 2 and not r.passed

    def test_unique_composite(self):
        f = pl.DataFrame({"a": [1, 1, 1], "b": ["x", "x", "y"]})
        assert _result(f, unique("a", "b")).failed_rows == 2

    def test_row_count_between(self):
        assert _result(pl.DataFrame({"a": [1, 2]}), row_count_between(3, None)).failed_rows == 2
        assert _result(pl.DataFrame({"a": [1, 2]}), row_count_between(1, 5)).passed


class TestRunnerBatching:
    def test_batched_counts_match_individual(self):
        f = pl.DataFrame({"a": [1, None, 3], "v": [10, 200, 30]})
        checks = [not_null("a"), in_range("v", 0, 100)]
        batched = {r.name: r.failed_rows for r in validate(f, checks).results}
        individual = {c.name: validate(f, [c]).results[0].failed_rows for c in checks}
        assert batched == individual

    def test_empty_frame_no_crash(self):
        f = pl.DataFrame({"a": []}, schema={"a": pl.Int64})
        report = validate(f, [not_null("a"), in_range("a", 0, 1), not_empty()])
        assert report.results[0].passed  # no rows to violate not_null
        assert not report.results[2].passed  # not_empty fails

    def test_severity_preserved(self):
        f = pl.DataFrame({"v": [200]})
        r = validate(f, [in_range("v", 0, 100, severity=Severity.WARN)]).results[0]
        assert r.severity is Severity.WARN

    def test_no_columns_raises_clear_error(self):
        import pytest
        with pytest.raises(ValueError, match="at least one column"):
            not_null()
        with pytest.raises(ValueError, match="at least one column"):
            unique()

    def test_duplicate_check_names_do_not_collide(self):
        f = pl.DataFrame({"v": [5, 50, 150]})
        # Same column, same generated name; positional aliasing must keep both.
        results = validate(f, [in_range("v", 0, 100), in_range("v", 0, 10)]).results
        assert [r.failed_rows for r in results] == [1, 2]
