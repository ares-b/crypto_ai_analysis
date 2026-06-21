from __future__ import annotations

import os
from collections.abc import Sequence
from time import perf_counter

import polars as pl

from ._base import Store, WriteResult
from ._spec import TableSpec, coerce_to_schema


class IcebergStore(Store):
    """Production Iceberg table store backed by a pyiceberg catalog.

    Use :meth:`from_env` for CLI/init scripts, :meth:`from_config` when you need
    explicit catalog properties (tests, multi-tenant setups).
    """

    def __init__(self, catalog: object, specs: Sequence[TableSpec]) -> None:
        self._catalog = catalog
        self._specs: dict[str, TableSpec] = {
            f"{s.namespace}.{s.name}": s for s in specs
        } | {s.name: s for s in specs}

    @classmethod
    def from_config(
        cls,
        specs: Sequence[TableSpec],
        *,
        name: str,
        properties: dict[str, str],
    ) -> IcebergStore:
        cls._set_s3_checksum_defaults()
        from pyiceberg.catalog import load_catalog

        return cls(load_catalog(name, **properties), specs)

    @classmethod
    def from_env(cls, specs: Sequence[TableSpec]) -> IcebergStore:
        """Build from ``ICEBERG_*`` env vars and ensure all tables exist.

        Calls :meth:`create_all` so callers can write immediately after construction.
        """
        store = cls.from_config(
            specs,
            name=os.getenv("ICEBERG_CATALOG_NAME", "dca"),
            properties=cls._catalog_properties(
                catalog_uri=os.environ["ICEBERG_CATALOG_URI"],
                warehouse=os.environ["ICEBERG_WAREHOUSE"],
                s3_endpoint=os.environ["ICEBERG_S3_ENDPOINT"],
                s3_access_key_id=os.environ["ICEBERG_S3_ACCESS_KEY_ID"],
                s3_secret_access_key=os.environ["ICEBERG_S3_SECRET_ACCESS_KEY"],
                s3_region=os.getenv("ICEBERG_S3_REGION", "garage"),
            ),
        )
        store.create_all()
        return store

    @staticmethod
    def _catalog_properties(
        *,
        catalog_uri: str,
        warehouse: str,
        s3_endpoint: str,
        s3_access_key_id: str,
        s3_secret_access_key: str,
        s3_region: str = "garage",
    ) -> dict[str, str]:
        return {
            "type": "sql",
            "uri": catalog_uri,
            "warehouse": warehouse,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key_id,
            "s3.secret-access-key": s3_secret_access_key,
            "s3.region": s3_region,
            "s3.path-style-access": "true",
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        }

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
                self._catalog.create_table(spec.identifier, schema=spec.schema)
            self._sync_metadata(spec.identifier)

    def read(self, table: str, *, columns: Sequence[str] | None = None) -> pl.DataFrame:
        from pyiceberg.exceptions import NoSuchTableError

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
        arrow = coerce_to_schema(frame, iceberg_table.schema().as_arrow())

        started_at = perf_counter()
        result = iceberg_table.upsert(arrow, join_cols=list(spec.identity_columns))
        self._sync_metadata(spec.identifier)

        return WriteResult(
            rows_inserted=result.rows_inserted,
            rows_updated=result.rows_updated,
            write_duration_seconds=perf_counter() - started_at,
        )

    def _resolve(self, table: str) -> TableSpec:
        spec = self._specs.get(table)
        if spec is None:
            known = ", ".join(sorted(k for k in self._specs if "." in k))
            raise KeyError(f"unknown table {table!r}; known: {known}")
        return spec

    def _sync_metadata(self, identifier: tuple[str, str]) -> None:
        """Copy the versioned metadata file to a stable ``current.metadata.json`` path.

        Garage (and some S3-compatible stores) do not support atomic renames, so
        pyiceberg leaves metadata at a UUID-suffixed path after each commit.
        Copying to a fixed location lets external tools discover the current
        snapshot without listing the metadata directory.
        """
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

    @staticmethod
    def _set_s3_checksum_defaults() -> None:
        # Garage returns checksum headers that break botocore range-read validation
        os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
        os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
