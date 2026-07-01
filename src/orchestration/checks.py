import re
from collections.abc import Iterator, Sequence

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetKey,
)

from core.quality import QualitySubject, Report, Severity

_SAMPLE_LIMIT = 3
# Dagster check names must match ^[A-Za-z0-9_]+$; the human-readable original
# check name is kept in the spec description and result metadata.
_INVALID = re.compile(r"[^A-Za-z0-9_]+")


def _slug(value: str) -> str:
    return _INVALID.sub("_", value).strip("_")


def _names(subject: str, names: Sequence[str]) -> list[str]:
    """Stable, unique, Dagster-valid check names for one subject. Deterministic
    given order, so spec-build and result-emit produce identical names."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in names:
        base = f"{_slug(subject)}__{_slug(name)}"
        n = seen.get(base, 0)
        seen[base] = n + 1
        out.append(base if n == 0 else f"{base}_{n + 1}")
    return out


def _severity(severity: Severity) -> AssetCheckSeverity:
    return AssetCheckSeverity.ERROR if severity is Severity.ERROR else AssetCheckSeverity.WARN


def build_check_specs(
    asset_key: AssetKey, subjects: Sequence[QualitySubject]
) -> list[AssetCheckSpec]:
    """core.quality subjects -> dagster check declarations. ERROR checks are
    blocking so a failure fails the run; the pipeline also gates the write."""
    specs: list[AssetCheckSpec] = []
    for table, checks in subjects:
        for name, check in zip(_names(table, [c.name for c in checks]), checks, strict=True):
            specs.append(
                AssetCheckSpec(
                    name=name,
                    asset=asset_key,
                    blocking=check.severity is Severity.ERROR,
                    description=check.name,
                )
            )
    return specs


def to_check_results(
    asset_key: AssetKey, reports: Sequence[Report]
) -> Iterator[AssetCheckResult]:
    """core.quality reports -> dagster check results. Subjects with no frame this
    run (e.g. empty response) simply emit no results for that table."""
    for report in reports:
        names = _names(report.subject, [r.name for r in report.results])
        for name, result in zip(names, report.results, strict=True):
            metadata = {
                "table": report.subject,
                "failed_rows": result.failed_rows,
                "total_rows": result.total_rows,
            }
            if result.sample:
                metadata["sample"] = str(result.sample[:_SAMPLE_LIMIT])
            yield AssetCheckResult(
                asset_key=asset_key,
                check_name=name,
                passed=result.passed,
                severity=_severity(result.severity),
                metadata=metadata,
            )
