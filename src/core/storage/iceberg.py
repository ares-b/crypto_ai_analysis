import os
from collections.abc import Sequence
from pathlib import Path
from time import perf_counter
from typing import Self

import polars as pl
from pyiceberg.catalog import load_catalog
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ._base import Store, WriteResult
from ._spec import TableSpec, coerce_to_schema


class IcebergCatalogSettings(BaseModel):
    """Iceberg REST catalog connection config.

    Reads from ICEBERG_CATALOG_* env vars by default; fields can be overridden
    directly for tests or local dev. Raises ValueError on missing required
    fields so callers get an actionable message, not a raw KeyError.
    """

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
    """Production Iceberg table store backed by an Iceberg REST catalog.

    Storage credentials are vended by the catalog via remote signing; this client
    needs no S3 keys. Use from_env() in production and from_config() for explicit
    catalog properties (integration tests, local dev with a real catalog).
    """

    def __init__(self, catalog: object, specs: Sequence[TableSpec]) -> None:
        self._catalog = catalog
        self._specs: dict[str, TableSpec] = (
            {f"{s.namespace}.{s.name}": s for s in specs} | {s.name: s for s in specs}
        )

    @classmethod
    def from_config(
        cls,
        specs: Sequence[TableSpec],
        *,
        name: str,
        properties: dict[str, str],
    ) -> Self:
        cls._set_s3_checksum_defaults()
        return cls(load_catalog(name, **properties), specs)

    @classmethod
    def from_env(cls, specs: Sequence[TableSpec]) -> Self:
        """Build from ICEBERG_CATALOG_* env vars and ensure all tables exist.

        Token is read fresh on every call. Dagster creates a new IcebergStore
        per asset run (via IcebergStoreResource.create()), so the token is
        always within its ~1h TTL for any single run.
        """
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
        """Read the SA bearer token.

        ICEBERG_CATALOG_TOKEN env var takes precedence; use it for local dev or CI
        where the k8s projected token file is not available.
        In production, the token is a projected k8s ServiceAccount token
        auto-rotated by kubelet every ~1h.
        """
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
        """Copy versioned metadata to a stable current.metadata.json path.

        Garage has no atomic rename. This stable path lets external tools
        discover the current snapshot without listing the metadata directory.
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
        # Garage returns checksum headers that break botocore range-read validation.
        os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
        os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
