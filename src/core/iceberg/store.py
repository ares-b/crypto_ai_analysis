import os
from collections.abc import Sequence
from pathlib import Path
from time import perf_counter
from typing import Self

import polars as pl
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortDirection
from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.helpers.polars import cast_frame_to_arrow
from core.iceberg.table import TableSpec
from core.storage import Store, WriteResult


class IcebergCatalogSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    uri: str = Field(default_factory=lambda: os.environ.get("ICEBERG_CATALOG_URI", ""))
    warehouse: str = Field(default_factory=lambda: os.environ.get("ICEBERG_CATALOG_WAREHOUSE", ""))
    catalog_name: str = Field(
        default_factory=lambda: os.environ.get("ICEBERG_CATALOG_NAME", "lakehouse")
    )
    token_file: str = Field(
        default_factory=lambda: os.environ.get(
            "ICEBERG_CATALOG_TOKEN_FILE", "/var/run/secrets/tokens/catalog"
        )
    )

    @model_validator(mode="after")
    def _validate_required(self) -> Self:
        if not self.uri:
            raise ValueError(
                "ICEBERG_CATALOG_URI is required. "
                "Set it to the REST catalog endpoint "
                "(e.g. http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog)."
            )
        if not self.warehouse:
            raise ValueError(
                "ICEBERG_CATALOG_WAREHOUSE is required. "
                "Set it to the warehouse name registered in the catalog (e.g. 'crypto-ai-analysis')."
            )
        return self


class IcebergStore(Store):
    def __init__(self, catalog: object, specs: Sequence[TableSpec]) -> None:
        self._catalog = catalog
        self._specs: dict[str, TableSpec] = {f"{s.namespace}.{s.name}": s for s in specs}

    @classmethod
    def from_config(
        cls,
        specs: Sequence[TableSpec],
        *,
        name: str,
        properties: dict[str, str],
    ) -> Self:
        return cls(load_catalog(name, **properties), specs)

    @classmethod
    def from_env(cls, specs: Sequence[TableSpec]) -> Self:
        # Token read fresh per call: Dagster creates a new store per asset run, token TTL ~1h.
        settings = IcebergCatalogSettings()
        store = cls.from_config(
            specs,
            name=settings.catalog_name,
            properties={
                "type": "rest",
                "uri": settings.uri,
                "warehouse": settings.warehouse,
                "token": cls._read_token(settings),
            },
        )
        store.create_all()
        return store

    @staticmethod
    def _read_token(settings: IcebergCatalogSettings) -> str:
        # ICEBERG_CATALOG_TOKEN takes precedence for local dev/CI without k8s SA token file.
        static = os.environ.get("ICEBERG_CATALOG_TOKEN")
        if static:
            return static
        try:
            return Path(settings.token_file).read_text().strip()
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"SA token file not found at {settings.token_file!r}. "
                "In production, ensure the pod mounts the projected SA token at that path. "
                "For local dev or CI, set ICEBERG_CATALOG_TOKEN to a static bearer token."
            ) from exc

    def create_all(self) -> None:
        namespaces = {s.namespace for s in self._specs.values()}
        for namespace in namespaces:
            self._catalog.create_namespace_if_not_exists(namespace)
        seen: set[str] = set()
        for spec in self._specs.values():
            key = f"{spec.namespace}.{spec.name}"
            if key in seen:
                continue
            seen.add(key)
            if not self._catalog.table_exists(spec.identifier):
                create_kwargs: dict[str, object] = {"schema": spec.schema}
                if spec.partition_spec is not None:
                    create_kwargs["partition_spec"] = spec.partition_spec
                if spec.sort_order is not None:
                    create_kwargs["sort_order"] = spec.sort_order
                self._catalog.create_table(spec.identifier, **create_kwargs)
            else:
                iceberg_table = self._catalog.load_table(spec.identifier)
                self._reconcile(iceberg_table, spec)
            self._sync_metadata(spec.identifier)

    def read(self, table: str, *, columns: Sequence[str] | None = None) -> pl.DataFrame:
        spec = self._resolve(table)
        try:
            iceberg_table = self._catalog.load_table(spec.identifier)
        except NoSuchTableError:
            frame = pl.from_arrow(spec.schema.empty_table())
            return frame.select(list(columns)) if columns is not None else frame

        scan = (
            iceberg_table.scan(selected_fields=tuple(columns))
            if columns is not None
            else iceberg_table.scan()
        )
        return pl.from_arrow(scan.to_arrow())

    def upsert(self, table: str, frame: pl.DataFrame) -> WriteResult:
        if frame.is_empty():
            return WriteResult(rows_inserted=0, rows_updated=0, write_duration_seconds=0.0)

        spec = self._resolve(table)
        iceberg_table = self._catalog.load_table(spec.identifier)
        arrow = cast_frame_to_arrow(frame, iceberg_table.schema().as_arrow())

        started_at = perf_counter()
        result = iceberg_table.upsert(arrow, join_cols=list(spec.identity_columns))
        self._sync_metadata(spec.identifier)

        return WriteResult(
            rows_inserted=result.rows_inserted,
            rows_updated=result.rows_updated,
            write_duration_seconds=perf_counter() - started_at,
        )

    def append(self, table: str, frame: pl.DataFrame) -> WriteResult:
        if frame.is_empty():
            return WriteResult(rows_inserted=0, rows_updated=0, write_duration_seconds=0.0)

        spec = self._resolve(table)
        iceberg_table = self._catalog.load_table(spec.identifier)
        arrow = cast_frame_to_arrow(frame, iceberg_table.schema().as_arrow())

        started_at = perf_counter()
        # Plain append: no full-table read for MERGE. Far cheaper for bulk backfills.
        iceberg_table.append(arrow)
        self._sync_metadata(spec.identifier)

        return WriteResult(
            rows_inserted=frame.height,
            rows_updated=0,
            write_duration_seconds=perf_counter() - started_at,
        )

    def _resolve(self, table: str) -> TableSpec:
        spec = self._specs.get(table)
        if spec is None:
            known = ", ".join(sorted(k for k in self._specs if "." in k))
            raise KeyError(f"unknown table {table!r}; known: {known}")
        return spec

    @staticmethod
    def _reconcile(table: Table, spec: TableSpec) -> None:
        if not IcebergStore._partition_spec_matches(table, spec):
            IcebergStore._apply_partition_spec(table, spec)
        if not IcebergStore._sort_order_matches(table, spec):
            IcebergStore._apply_sort_order(table, spec)

    @staticmethod
    def _partition_spec_matches(table: Table, spec: TableSpec) -> bool:
        current = table.spec().fields
        desired = spec.partition_spec.fields if spec.partition_spec is not None else []
        if len(current) != len(desired):
            return False
        table_schema = table.schema()
        for c, d in zip(current, desired):
            c_name = table_schema.find_field(c.source_id).name
            d_name = spec.schema.find_field(d.source_id).name
            if c_name != d_name or type(c.transform) is not type(d.transform):
                return False
        return True

    @staticmethod
    def _sort_order_matches(table: Table, spec: TableSpec) -> bool:
        current = table.sort_order().fields
        desired = spec.sort_order.fields if spec.sort_order is not None else []
        if len(current) != len(desired):
            return False
        table_schema = table.schema()
        for c, d in zip(current, desired):
            c_name = table_schema.find_field(c.source_id).name
            d_name = spec.schema.find_field(d.source_id).name
            if c_name != d_name or c.direction != d.direction or c.null_order != d.null_order:
                return False
        return True

    @staticmethod
    def _apply_partition_spec(table: Table, spec: TableSpec) -> None:
        update = table.update_spec()
        for field in table.spec().fields:
            update.remove_field(field.name)
        if spec.partition_spec is not None:
            for field in spec.partition_spec.fields:
                col_name = spec.schema.find_field(field.source_id).name
                update.add_field(col_name, field.transform, field.name)
        update.commit()

    @staticmethod
    def _apply_sort_order(table: Table, spec: TableSpec) -> None:
        update = table.update_sort_order()
        if spec.sort_order is not None:
            for field in spec.sort_order.fields:
                col_name = spec.schema.find_field(field.source_id).name
                if field.direction == SortDirection.ASC:
                    update.asc(col_name, field.transform, field.null_order)
                else:
                    update.desc(col_name, field.transform, field.null_order)
        update.commit()

    def _sync_metadata(self, identifier: tuple[str, str]) -> None:
        # S3 object stores have no atomic rename; copy to stable path so external tools skip metadata listing.
        iceberg_table = self._catalog.load_table(identifier)
        current = iceberg_table.metadata_location
        stable = self._stable_metadata_path(current)
        if current == stable:
            return

        src = iceberg_table.io.new_input(current)
        dst = iceberg_table.io.new_output(stable)
        with src.open(seekable=False) as source, dst.create(overwrite=True) as target:
            while chunk := source.read(1024 * 1024):
                target.write(chunk)

    @staticmethod
    def _stable_metadata_path(metadata_location: str) -> str:
        directory, _, _ = metadata_location.rpartition("/")
        if not directory:
            raise ValueError(f"invalid Iceberg metadata location: {metadata_location!r}")
        return f"{directory}/current.metadata.json"
