from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import polars as pl


@dataclass(frozen=True)
class WriteResult:
    rows_inserted: int
    rows_updated: int
    write_duration_seconds: float

    @property
    def rows_affected(self) -> int:
        return self.rows_inserted + self.rows_updated


@runtime_checkable
class Store(Protocol):
    """Engine-agnostic table store.

    Tables are referenced by ``"namespace.name"`` (e.g. ``"raw.binance_candles"``).
    """

    def create_all(self) -> None:
        """Create every namespace/table declared in the schema registry."""
        ...

    def read(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
    ) -> pl.DataFrame:
        """Return the full table as a Polars frame (empty frame if absent)."""
        ...

    def upsert(self, table: str, frame: pl.DataFrame) -> WriteResult:
        """Merge rows on the table's identity columns (insert or replace)."""
        ...
