import dataclasses
import logging
from collections.abc import Sequence

import polars as pl

from core.quality import Check, Report, validate


def check_frame(frame: pl.DataFrame, checks: Sequence[Check], *, logger: logging.Logger,
                table: str) -> Report:
    """Validate a frame before write: log failures, tag the report with its table.

    Does not raise: the caller decides whether to gate the write on `report.ok`
    and surfaces per-check results as Dagster asset checks. Backfill keeps its own
    validate + raise_for_status path for batch aborts.
    """
    report = dataclasses.replace(validate(frame, checks), subject=table)
    for result in report.failures:
        logger.warning(
            "quality_check_failed",
            extra={"table": table, "check": result.name, "severity": result.severity.value,
                   "failed_rows": result.failed_rows, "total_rows": result.total_rows,
                   "sample": result.sample},
        )
    return report
