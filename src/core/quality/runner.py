from collections.abc import Sequence

import polars as pl

from core.quality.checks import Check
from core.quality.result import CheckResult, Report

_SAMPLE_SIZE = 3


def validate(frame: pl.DataFrame, checks: Sequence[Check]) -> Report:
    """Run all checks against a frame, collecting every result (no short-circuit).

    Row-level checks are evaluated in a single scan; frame-level checks run on their
    own. Pure: no logging, no I/O.
    """
    total = frame.height
    expr_checks = [c for c in checks if c.keep_expr is not None]

    # Alias positionally, not by name: two checks may share a name without colliding.
    failed_counts = [0] * len(expr_checks)
    if expr_checks and total:
        row = frame.select(
            [(~c.keep_expr).sum().alias(f"_c{i}") for i, c in enumerate(expr_checks)]
        ).row(0)
        failed_counts = [int(v or 0) for v in row]

    results: list[CheckResult] = []
    expr_idx = 0
    for check in checks:
        if check.keep_expr is not None:
            failed = failed_counts[expr_idx]
            expr_idx += 1
            sample = (
                frame.filter(~check.keep_expr).select(check.columns).head(_SAMPLE_SIZE).to_dicts()
                if failed and check.columns else []
            )
            passed = failed == 0
        else:
            passed, failed, sample = check.frame_eval(frame)
        results.append(
            CheckResult(
                name=check.name,
                severity=check.severity,
                passed=passed,
                failed_rows=failed,
                total_rows=total,
                sample=sample,
            )
        )
    return Report(results=tuple(results))
