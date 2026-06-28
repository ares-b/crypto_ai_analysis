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
    # Table names: "namespace.name" (e.g. "raw.binance_candles").
    def create_all(self) -> None: ...

    def read(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
    ) -> pl.DataFrame:
        # Returns empty frame when table does not exist yet.
        ...

    def upsert(self, table: str, frame: pl.DataFrame) -> WriteResult:
        ...

    def append(self, table: str, frame: pl.DataFrame) -> WriteResult:
        ...
