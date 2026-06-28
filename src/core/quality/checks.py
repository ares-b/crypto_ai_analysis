from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import NamedTuple

import polars as pl

from core.quality.result import Severity


class QualityCheckOutcome(NamedTuple):
    passed: bool
    failed_rows: int
    sample: list


FrameEval = Callable[[pl.DataFrame], QualityCheckOutcome]


@dataclass(frozen=True)
class Check:
    name: str
    severity: Severity
    # Row-level checks set keep_expr (rows where it is False fail); frame-level
    # checks (unique, not_empty, row count) set frame_eval instead.
    keep_expr: pl.Expr | None = None
    columns: tuple[str, ...] = ()
    frame_eval: FrameEval | None = None


def expression(name: str, keep_expr: pl.Expr, *, severity: Severity = Severity.ERROR,
               columns: tuple[str, ...] = ()) -> Check:
    return Check(name=name, severity=severity, keep_expr=keep_expr, columns=columns)


def not_null(*cols: str, severity: Severity = Severity.ERROR) -> Check:
    if not cols:
        raise ValueError("not_null requires at least one column")
    keep = pl.all_horizontal([pl.col(c).is_not_null() for c in cols])
    return Check(name=f"not_null({', '.join(cols)})", severity=severity, keep_expr=keep, columns=cols)


def in_range(col: str, low: float | None, high: float | None, *, inclusive: bool = True,
             allow_null: bool = False, severity: Severity = Severity.ERROR) -> Check:
    bounds = pl.lit(True)
    if low is not None:
        bounds = bounds & (pl.col(col) >= low if inclusive else pl.col(col) > low)
    if high is not None:
        bounds = bounds & (pl.col(col) <= high if inclusive else pl.col(col) < high)
    keep = (pl.col(col).is_null() | bounds) if allow_null else (pl.col(col).is_not_null() & bounds)
    return Check(name=f"in_range({col})", severity=severity, keep_expr=keep, columns=(col,))


def accepted_values(col: str, values: Sequence, *, severity: Severity = Severity.ERROR) -> Check:
    keep = pl.col(col).is_in(list(values))
    return Check(name=f"accepted_values({col})", severity=severity, keep_expr=keep, columns=(col,))


def matches_regex(col: str, pattern: str, *, severity: Severity = Severity.ERROR) -> Check:
    keep = pl.col(col).str.contains(pattern)
    return Check(name=f"matches_regex({col})", severity=severity, keep_expr=keep, columns=(col,))


def time_in_window(col: str, start: date | datetime, end: date | datetime, *,
                   severity: Severity = Severity.ERROR) -> Check:
    keep = pl.col(col).is_between(start, end)
    return Check(name=f"time_in_window({col})", severity=severity, keep_expr=keep, columns=(col,))


def not_empty(*, severity: Severity = Severity.ERROR) -> Check:
    def _eval(frame: pl.DataFrame) -> QualityCheckOutcome:
        return QualityCheckOutcome(frame.height > 0, 0, [])
    return Check(name="not_empty", severity=severity, frame_eval=_eval)


def unique(*cols: str, severity: Severity = Severity.ERROR) -> Check:
    if not cols:
        raise ValueError("unique requires at least one column")

    def _eval(frame: pl.DataFrame) -> QualityCheckOutcome:
        dup_mask = frame.select(cols).is_duplicated()
        dupes = int(dup_mask.sum())
        sample = frame.filter(dup_mask).select(cols).head(3).to_dicts() if dupes else []
        return QualityCheckOutcome(dupes == 0, dupes, sample)
    return Check(name=f"unique({', '.join(cols)})", severity=severity, frame_eval=_eval)


def row_count_between(low: int | None, high: int | None, *, severity: Severity = Severity.ERROR) -> Check:
    def _eval(frame: pl.DataFrame) -> QualityCheckOutcome:
        h = frame.height
        ok = (low is None or h >= low) and (high is None or h <= high)
        return QualityCheckOutcome(ok, 0 if ok else h, [])
    return Check(name=f"row_count_between({low}, {high})", severity=severity, frame_eval=_eval)


def from_spec(record_cls, *, extra: Sequence[Check] = ()) -> list[Check]:
    """Baseline checks every stored model gets: identity not-null and unique."""
    ids = record_cls.TABLE_SPEC.identity_columns
    base: list[Check] = [not_null(*ids), unique(*ids)] if ids else []
    return [*base, *extra]
