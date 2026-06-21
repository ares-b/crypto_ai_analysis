from __future__ import annotations

import types as _types
from dataclasses import dataclass
from datetime import date, datetime
from typing import Union, get_args, get_origin

import pyarrow as pa
import polars as pl
from pyiceberg.schema import Schema
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


@dataclass(frozen=True)
class TableSpec:
    identifier: tuple[str, str]
    schema: Schema
    identity_columns: tuple[str, ...]

    @property
    def namespace(self) -> str:
        return self.identifier[0]

    @property
    def name(self) -> str:
        return self.identifier[1]


def coerce_to_schema(frame: pl.DataFrame, arrow_schema: pa.Schema) -> pa.Table:
    """Cast a Polars frame to match an Arrow schema before writing to Iceberg.

    Raises ``ValueError`` on missing columns; casts on minor type mismatches.
    """
    arrow_table = frame.to_arrow()
    if arrow_table.schema.equals(arrow_schema, check_metadata=False):
        return arrow_table

    columns: list[pa.ChunkedArray] = []
    for field in arrow_schema:
        if field.name not in arrow_table.schema.names:
            raise ValueError(
                f"column {field.name!r} required by Iceberg schema but missing from frame"
            )
        col = arrow_table.column(field.name)
        columns.append(col.cast(field.type) if col.type != field.type else col)

    return pa.table(
        {field.name: columns[i] for i, field in enumerate(arrow_schema)},
        schema=arrow_schema,
    )
