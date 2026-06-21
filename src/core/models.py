from __future__ import annotations

from typing import Self

import polars as pl
from pydantic import BaseModel, ConfigDict


class StoreRow(BaseModel):

    model_config = ConfigDict(frozen=True)

    @classmethod
    def to_frame(cls, rows: list[Self]) -> pl.DataFrame:
        return pl.DataFrame([row.model_dump() for row in rows])
