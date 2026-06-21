from __future__ import annotations

from typing import ClassVar

from core.models import StoreRow
from ._spec import TableSpec, build_iceberg_schema


class IcebergRow(StoreRow):
    """StoreRow that auto-derives its Iceberg TableSpec from Pydantic field annotations.

    Usage:
        class MyRow(IcebergRow, table="ns.name", identity=("id_col",)):
            id_col: str
            value: float | None
    """

    TABLE_SPEC: ClassVar[TableSpec]

    # Staging area: __init_subclass__ runs before model_fields is populated,
    # so we stash the kwargs here and consume them in __pydantic_init_subclass__.
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
