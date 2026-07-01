from dataclasses import dataclass, field
from enum import Enum

MetricValue = str | int | float | None


class Severity(Enum):
    ERROR = "error"  # blocks the write
    WARN = "warn"    # surfaced, write proceeds


class QualityError(Exception):
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: Severity
    passed: bool
    failed_rows: int
    total_rows: int
    sample: list = field(default_factory=list)

    @property
    def message(self) -> str:
        if self.passed:
            return f"{self.name}: ok ({self.total_rows} rows)"
        detail = f"{self.failed_rows}/{self.total_rows} rows"
        if self.sample:
            detail += f" e.g. {self.sample}"
        return f"{self.name} [{self.severity.value}]: {detail}"


@dataclass(frozen=True)
class Report:
    results: tuple[CheckResult, ...]
    # Dataset the checks ran against (table name). Used to namespace asset checks.
    subject: str = ""

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    @property
    def error_failures(self) -> list[CheckResult]:
        return [r for r in self.failures if r.severity is Severity.ERROR]

    @property
    def ok(self) -> bool:
        return not self.error_failures

    def raise_for_status(self) -> None:
        errors = self.error_failures
        if errors:
            raise QualityError("; ".join(r.message for r in errors))

    def to_metrics(self, *, prefix: str = "quality") -> dict[str, MetricValue]:
        warns = [r for r in self.failures if r.severity is Severity.WARN]
        return {
            f"{prefix}_checks_total": len(self.results),
            f"{prefix}_failed_errors": len(self.error_failures),
            f"{prefix}_failed_warns": len(warns),
        }


class RunResult(dict):
    """Run metrics mapping that also carries the quality reports behind it.

    Subclasses dict so existing callers keep `result["rows_affected"]`; the
    orchestration layer reads `result.reports` to emit Dagster asset checks
    without changing the metrics contract.
    """

    def __init__(self, metrics: dict[str, MetricValue], reports: tuple[Report, ...] = ()):
        super().__init__(metrics)
        self.reports: tuple[Report, ...] = tuple(reports)
