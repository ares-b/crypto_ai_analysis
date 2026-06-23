from typing import ClassVar

from core.iceberg.table import TableSpec
from core.models import Record


class IcebergRecord(Record):

    TABLE_SPEC: ClassVar[TableSpec]

    _iceberg_table: ClassVar[str | None] = None
    _iceberg_identity: ClassVar[tuple[str, ...]] = ()
    _iceberg_partition: ClassVar[tuple[str, ...]] = ()
    _iceberg_sort: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(
        cls,
        table: str | None = None,
        identity: tuple[str, ...] = (),
        partition: tuple[str, ...] = (),
        sort: tuple[str, ...] = (),
        **kwargs: object,
    ) -> None:
        super().__init_subclass__(**kwargs)
        cls._iceberg_table = table
        cls._iceberg_identity = identity
        cls._iceberg_partition = partition
        cls._iceberg_sort = sort

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: object) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        if cls._iceberg_table is not None:
            namespace, name = cls._iceberg_table.split(".", 1)
            cls.TABLE_SPEC = TableSpec.build(
                identifier=(namespace, name),
                annotations={k: v.annotation for k, v in cls.model_fields.items()},
                identity=cls._iceberg_identity,
                partition=cls._iceberg_partition,
                sort=cls._iceberg_sort,
            )
