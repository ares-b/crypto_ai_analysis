from typing import ClassVar, Self

import polars as pl
from pydantic import BaseModel, ConfigDict

from core.storage._spec import TableSpec, build_iceberg_schema


class StoreRow(BaseModel):

    model_config = ConfigDict(frozen=True)

    @classmethod
    def to_frame(cls, rows: list[Self]) -> pl.DataFrame:
        return pl.DataFrame([row.model_dump() for row in rows])


class IcebergRow(StoreRow):
    
    TABLE_SPEC: ClassVar[TableSpec]

    _iceberg_table: ClassVar[str | None] = None
    _iceberg_identity: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(
        cls,
        table: str | None = None,
        identity: tuple[str, ...] = (),
        **kwargs: object,
    ) -> None:
        super().__init_subclass__(**kwargs)
        cls._iceberg_table = table
        cls._iceberg_identity = identity

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: object) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        if cls._iceberg_table is not None:
            namespace, name = cls._iceberg_table.split(".", 1)
            cls.TABLE_SPEC = TableSpec(
                identifier=(namespace, name),
                schema=build_iceberg_schema(
                    {k: v.annotation for k, v in cls.model_fields.items()}
                ),
                identity_columns=cls._iceberg_identity,
            )
