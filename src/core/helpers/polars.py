import polars as pl
import pyarrow as pa


def cast_frame_to_arrow(frame: pl.DataFrame, schema: pa.Schema) -> pa.Table:
    arrow_table = frame.to_arrow()
    if arrow_table.schema.equals(schema, check_metadata=False):
        return arrow_table

    extra = set(arrow_table.schema.names) - set(schema.names)
    if extra:
        raise ValueError(f"frame has columns absent from Iceberg schema: {sorted(extra)}")

    columns: list[pa.ChunkedArray] = []
    for field in schema:
        if field.name not in arrow_table.schema.names:
            raise ValueError(
                f"column {field.name!r} required by Iceberg schema but missing from frame"
            )
        col = arrow_table.column(field.name)
        columns.append(col.cast(field.type) if col.type != field.type else col)

    return pa.table(
        {field.name: columns[i] for i, field in enumerate(schema)},
        schema=schema,
    )
