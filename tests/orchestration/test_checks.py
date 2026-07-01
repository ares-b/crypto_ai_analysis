import re
from types import SimpleNamespace

import pytest
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, MaterializeResult

from core.quality import CheckResult, Report, RunResult, Severity, not_null, unique
from orchestration._asset import to_materialize_result
from orchestration.checks import build_check_specs, to_check_results

_KEY = AssetKey(["crypto-ai-analysis", "x"])
_VALID = re.compile(r"^[A-Za-z0-9_]+$")


def _report(subject: str, *results: CheckResult) -> Report:
    return Report(results=tuple(results), subject=subject)


def _ok(name: str, severity: Severity = Severity.ERROR) -> CheckResult:
    return CheckResult(name=name, severity=severity, passed=True, failed_rows=0, total_rows=3)


def _ctx() -> SimpleNamespace:
    """Stand-in for AssetExecutionContext; to_materialize_result only reads asset_key."""
    return SimpleNamespace(asset_key=_KEY)


class TestBuildCheckSpecs:
    def test_names_are_dagster_valid_and_unique(self):
        subjects = [("raw.candles", [not_null("a", "b"), unique("a")])]
        specs = build_check_specs(_KEY, subjects)
        names = [s.name for s in specs]
        assert len(names) == 2
        assert all(_VALID.match(n) for n in names)
        assert len(set(names)) == len(names)

    def test_duplicate_check_names_disambiguated(self):
        subjects = [("raw.t", [not_null("a"), not_null("a")])]
        names = [s.name for s in build_check_specs(_KEY, subjects)]
        assert len(set(names)) == 2

    def test_error_checks_blocking_warn_not(self):
        subjects = [("raw.t", [not_null("a"), not_null("b", severity=Severity.WARN)])]
        specs = {s.description: s for s in build_check_specs(_KEY, subjects)}
        assert specs["not_null(a)"].blocking is True
        assert specs["not_null(b)"].blocking is False

    def test_multi_table_names_namespaced(self):
        subjects = [("raw.a", [unique("k")]), ("raw.b", [unique("k")])]
        names = [s.name for s in build_check_specs(_KEY, subjects)]
        assert len(set(names)) == 2  # same check, different table -> distinct


class TestToCheckResults:
    def test_result_names_match_declared_specs(self):
        # The naming used at emit time must equal the names declared at build time,
        # or Dagster rejects the result. Guard the two code paths agree.
        subjects = [("raw.t", [not_null("a"), unique("a")])]
        spec_names = {s.name for s in build_check_specs(_KEY, subjects)}
        report = _report("raw.t", _ok("not_null(a)"), _ok("unique(a)"))
        emitted_names = {r.check_name for r in to_check_results(_KEY, (report,))}
        assert emitted_names == spec_names

    def test_severity_mapped(self):
        report = _report("raw.t", CheckResult(name="x", severity=Severity.WARN, passed=False,
                                              failed_rows=1, total_rows=2, sample=[{"x": 1}]))
        check = next(iter(to_check_results(_KEY, (report,))))
        assert check.severity is AssetCheckSeverity.WARN
        assert check.passed is False

    def test_no_reports_emits_nothing(self):
        assert list(to_check_results(_KEY, ())) == []


class TestToMaterializeResult:
    def test_carries_metrics_and_one_check_result_per_check(self):
        report = _report("raw.t", _ok("not_null(a)"), _ok("unique(a)"))
        result = RunResult({"rows_affected": 3}, (report,))
        mat = to_materialize_result(_ctx(), result)
        assert isinstance(mat, MaterializeResult)
        assert mat.metadata == {"rows_affected": 3}
        assert len(mat.check_results) == 2
        assert all(isinstance(c, AssetCheckResult) for c in mat.check_results)

    def test_no_reports_still_materializes_without_checks(self):
        mat = to_materialize_result(_ctx(), RunResult({"rows_affected": 0}))
        assert isinstance(mat, MaterializeResult)
        assert not mat.check_results


def _report_from_subjects(subjects):
    """Build a passing report per subject, mirroring what validate() emits at runtime."""
    return tuple(
        _report(table, *[_ok(c.name, c.severity) for c in checks])
        for table, checks in subjects
    )


class TestRealPipelineAlignment:
    """The names declared from quality_subjects() must equal those emitted from the
    reports a run produces, for real pipeline models (single- and multi-table)."""

    def test_candles(self):
        from pipelines.raw.candles.config import DAILY_CANDLES
        from pipelines.raw.candles.run import quality_subjects

        subjects = quality_subjects(settings=DAILY_CANDLES)
        spec_names = {s.name for s in build_check_specs(_KEY, subjects)}
        reports = _report_from_subjects(subjects)
        emitted = {r.check_name for r in to_check_results(_KEY, reports)}
        assert emitted == spec_names and spec_names

    def test_sentiment_multi_table(self):
        from pipelines.raw.sentiment_index.config import SENTIMENT_SETTINGS
        from pipelines.raw.sentiment_index.run import sentiment_quality_subjects

        subjects = sentiment_quality_subjects(settings=SENTIMENT_SETTINGS)
        assert len(subjects) == 3
        spec_names = {s.name for s in build_check_specs(_KEY, subjects)}
        reports = _report_from_subjects(subjects)
        emitted = {r.check_name for r in to_check_results(_KEY, reports)}
        assert emitted == spec_names


class TestMaterializeRealAsset:
    """Full chain through the real Dagster runtime: checks are evaluated alongside
    the materialization and accepted (names match declared specs)."""

    def test_candles_emits_passing_checks(self, mocker):
        from datetime import UTC, datetime

        import dagster as dg

        from orchestration.partitions import DEPLOY_DATE
        from orchestration.raw.candles import binance_candles_daily
        from orchestration.resources import BinanceClientResource, IcebergStoreResource
        from tests.conftest import MemoryStore

        partition = DEPLOY_DATE
        open_ms = int(datetime.fromisoformat(partition).replace(tzinfo=UTC).timestamp() * 1000)
        kline = [open_ms, "1", "2", "0.5", "1.5", "100",
                 open_ms + 86_400_000 - 1, "150", 10, "60", "90", "0"]

        client = mocker.Mock(**{"get_klines.return_value": [kline]})

        # ConfigurableResource subclasses so Dagster binds them; create() returns the fakes.
        class FakeBinance(BinanceClientResource):
            def create(self):
                return client

        class FakeStore(IcebergStoreResource):
            def create(self):
                return MemoryStore()

        result = dg.materialize(
            [binance_candles_daily],
            partition_key=partition,
            resources={"binance_client": FakeBinance(), "iceberg_store": FakeStore()},
            raise_on_error=True,
        )
        assert result.success
        evals = result.get_asset_check_evaluations()
        assert len(evals) == 7
        assert all(e.passed for e in evals)


@pytest.mark.parametrize("subject", ["raw.candles", "raw.futures.funding"])
def test_slug_roundtrip_stable(subject):
    # Same subject + checks must produce identical names across calls (determinism).
    subjects = [(subject, [not_null("a"), unique("a")])]
    first = [s.name for s in build_check_specs(_KEY, subjects)]
    second = [s.name for s in build_check_specs(_KEY, subjects)]
    assert first == second
