import argparse
import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime

import polars as pl

from core.quality import Check, Report, validate
from core.storage import Store


def add_common_args(parser: argparse.ArgumentParser, *, default_start: str) -> None:
    parser.add_argument("--start", type=date.fromisoformat, default=date.fromisoformat(default_start))
    parser.add_argument("--end", type=date.fromisoformat, default=date.today(),
                        help="exclusive upper bound (default: today); set to the live Dagster start")
    parser.add_argument("--dry-run", action="store_true", help="fetch + validate, write nothing")


def window(args: argparse.Namespace) -> tuple[datetime, datetime]:
    start = datetime.combine(args.start, datetime.min.time(), tzinfo=UTC)
    end = datetime.combine(args.end, datetime.min.time(), tzinfo=UTC)
    return start, end


def _log_report(logger: logging.Logger, table: str, report: Report) -> None:
    for result in report.results:
        if result.passed:
            continue
        logger.warning(
            "quality_check_failed",
            extra={"table": table, "check": result.name, "severity": result.severity.value,
                   "failed_rows": result.failed_rows, "total_rows": result.total_rows,
                   "sample": result.sample},
        )


def _key_series(frame: pl.DataFrame, identity_cols: tuple[str, ...]) -> pl.Series:
    return frame.select(
        pl.concat_str([pl.col(c).cast(pl.Utf8) for c in identity_cols], separator="|").alias("__k")
    )["__k"]


def select_new(frame: pl.DataFrame, existing: pl.DataFrame, identity_cols: tuple[str, ...]) -> pl.DataFrame:
    if frame.is_empty() or existing.is_empty():
        return frame
    existing_keys = set(_key_series(existing, identity_cols).to_list())
    return frame.filter(~_key_series(frame, identity_cols).is_in(existing_keys))


def commit(
    store: Store | None,
    table: str,
    frame: pl.DataFrame,
    identity_cols: tuple[str, ...],
    *,
    checks: Sequence[Check],
    logger: logging.Logger,
    dry_run: bool,
) -> int:
    """Validate, then append only rows not already stored.

    ERROR-severity failures abort before any write. Append (no MERGE) is safe after
    select_new dedups against the table.
    """
    report = validate(frame, checks)
    _log_report(logger, table, report)
    report.raise_for_status()

    if dry_run:
        logger.info(f"[{table}] dry-run: {frame.height} rows validated, not written")
        return 0
    existing = store.read(table, columns=list(identity_cols))
    new = select_new(frame, existing, identity_cols)
    logger.info(f"[{table}] fetched={frame.height} already_present={frame.height - new.height} new={new.height}")
    if new.is_empty():
        logger.info(f"[{table}] nothing new, skipping write")
        return 0
    return store.append(table, new).rows_affected
