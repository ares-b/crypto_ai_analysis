import logging
from collections.abc import Sequence

import polars as pl

from core.quality import Check, Report, validate


def check_frame(frame: pl.DataFrame, checks: Sequence[Check], *, logger: logging.Logger,
                table: str) -> Report:
    """Validate a frame before write: log failures, raise on any ERROR.

    Returns the report so callers can fold report.to_metrics() into asset metadata.
    """
    report = validate(frame, checks)
    for result in report.failures:
        logger.warning(
            "quality_check_failed",
            extra={"table": table, "check": result.name, "severity": result.severity.value,
                   "failed_rows": result.failed_rows, "total_rows": result.total_rows,
                   "sample": result.sample},
        )
    report.raise_for_status()
    return report
