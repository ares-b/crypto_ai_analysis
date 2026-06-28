from typing import Self

import polars as pl
from pydantic import BaseModel, ConfigDict


class Record(BaseModel):

    model_config = ConfigDict(frozen=True)

    @classmethod
    def to_frame(cls, rows: list[Self]) -> pl.DataFrame:
        # infer_schema_length=None scans all rows so columns whose leading values
        # are null (optional fields) still get the correct dtype.
        return pl.DataFrame([row.model_dump() for row in rows], infer_schema_length=None)
