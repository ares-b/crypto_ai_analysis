import re
import types as _types
from dataclasses import dataclass
from datetime import date, datetime
from typing import Union, get_args, get_origin

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform, MonthTransform, YearTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IcebergType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

_PYTHON_TO_ICEBERG: dict[type, IcebergType] = {
    str: StringType(),
    int: LongType(),
    float: DoubleType(),
    bool: BooleanType(),
    datetime: TimestamptzType(),
    date: DateType(),
}

_TRANSFORMS = {
    "months": MonthTransform(),
    "years": YearTransform(),
    "days": DayTransform(),
    "identity": IdentityTransform(),
}

_PARTITION_FIELD_ID_OFFSET = 1000


def _unwrap_optional(annotation: type) -> tuple[type, bool]:
    # Two union forms: typing.Optional[T] (get_origin → Union) and T | None (types.UnionType).
    is_union = get_origin(annotation) is Union or isinstance(annotation, _types.UnionType)
    if is_union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0], False
    return annotation, True


def build_iceberg_schema(field_annotations: dict[str, type]) -> Schema:
    # Field IDs are 1-based and stable as long as field order doesn't change.
    fields = []
    for i, (name, annotation) in enumerate(field_annotations.items(), start=1):
        inner, required = _unwrap_optional(annotation)
        iceberg_type = _PYTHON_TO_ICEBERG.get(inner)
        if iceberg_type is None:
            raise TypeError(
                f"No Iceberg type mapping for {inner!r} (field {name!r}). "
                f"Supported: {set(_PYTHON_TO_ICEBERG)}"
            )
        fields.append(NestedField(i, name, iceberg_type, required=required))
    return Schema(*fields)


def _parse_partition_expr(expr: str, schema: Schema, field_id: int) -> PartitionField:
    match = re.fullmatch(r"(\w+)\((\w+)\)", expr.strip())
    if match:
        transform_name, col_name = match.group(1), match.group(2)
    else:
        transform_name, col_name = "identity", expr.strip()

    transform = _TRANSFORMS.get(transform_name)
    if transform is None:
        raise ValueError(
            f"Unknown partition transform {transform_name!r}; known: {sorted(_TRANSFORMS)}"
        )
    source_id = schema.find_field(col_name).field_id
    return PartitionField(
        source_id=source_id,
        field_id=field_id,
        transform=transform,
        name=f"{col_name}_{transform_name}",
    )


def build_partition_spec(exprs: tuple[str, ...], schema: Schema) -> PartitionSpec | None:
    if not exprs:
        return None
    return PartitionSpec(
        *[
            _parse_partition_expr(expr, schema, _PARTITION_FIELD_ID_OFFSET + i)
            for i, expr in enumerate(exprs)
        ]
    )


def build_sort_order(field_names: tuple[str, ...], schema: Schema) -> SortOrder | None:
    if not field_names:
        return None
    return SortOrder(
        *[
            SortField(
                source_id=schema.find_field(name).field_id,
                transform=IdentityTransform(),
                direction=SortDirection.ASC,
                null_order=NullOrder.NULLS_FIRST,
            )
            for name in field_names
        ]
    )


@dataclass(frozen=True)
class TableSpec:
    identifier: tuple[str, str]
    schema: Schema
    identity_columns: tuple[str, ...]
    partition_spec: PartitionSpec | None = None
    sort_order: SortOrder | None = None

    @classmethod
    def build(
        cls,
        identifier: tuple[str, str],
        annotations: dict[str, type],
        identity: tuple[str, ...],
        partition: tuple[str, ...],
        sort: tuple[str, ...],
    ) -> "TableSpec":
        schema = build_iceberg_schema(annotations)
        return cls(
            identifier=identifier,
            schema=schema,
            identity_columns=identity,
            partition_spec=build_partition_spec(partition, schema),
            sort_order=build_sort_order(sort, schema),
        )

    @property
    def namespace(self) -> str:
        return self.identifier[0]

    @property
    def name(self) -> str:
        return self.identifier[1]
