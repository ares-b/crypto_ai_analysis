from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from core.iceberg.store import IcebergCatalogSettings, IcebergStore




class TestIcebergCatalogSettings:
    def test_reads_env_vars(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_WAREHOUSE", "crypto-ai-analysis")
        monkeypatch.delenv("ICEBERG_CATALOG_NAME", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN_FILE", raising=False)

        s = IcebergCatalogSettings()

        assert s.uri == "http://catalog:8181/catalog"
        assert s.warehouse == "crypto-ai-analysis"
        assert s.catalog_name == "lakehouse"
        assert s.token_file == "/var/run/secrets/tokens/catalog"

    def test_catalog_name_from_env(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_WAREHOUSE", "wh")
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "custom-catalog")
        assert IcebergCatalogSettings().catalog_name == "custom-catalog"

    def test_token_file_from_env(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_WAREHOUSE", "wh")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN_FILE", "/mnt/token")
        assert IcebergCatalogSettings().token_file == "/mnt/token"

    def test_explicit_values_override_env(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://env-uri/catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_WAREHOUSE", "env-wh")
        s = IcebergCatalogSettings(uri="http://explicit/catalog", warehouse="explicit-wh")
        assert s.uri == "http://explicit/catalog"
        assert s.warehouse == "explicit-wh"

    def test_missing_uri_raises_validation_error(self, monkeypatch):
        monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
        monkeypatch.setenv("ICEBERG_CATALOG_WAREHOUSE", "wh")
        with pytest.raises(ValidationError, match="ICEBERG_CATALOG_URI is required"):
            IcebergCatalogSettings()

    def test_missing_warehouse_raises_validation_error(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        monkeypatch.delenv("ICEBERG_CATALOG_WAREHOUSE", raising=False)
        with pytest.raises(ValidationError, match="ICEBERG_CATALOG_WAREHOUSE is required"):
            IcebergCatalogSettings()

    def test_empty_uri_raises_validation_error(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "")
        monkeypatch.setenv("ICEBERG_CATALOG_WAREHOUSE", "wh")
        with pytest.raises(ValidationError, match="ICEBERG_CATALOG_URI is required"):
            IcebergCatalogSettings()

    def test_is_frozen(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        monkeypatch.setenv("ICEBERG_CATALOG_WAREHOUSE", "wh")
        s = IcebergCatalogSettings()
        with pytest.raises((ValidationError, TypeError)):
            s.uri = "changed"  # type: ignore[misc]




class TestReadToken:
    def test_env_var_takes_precedence_over_file(self, monkeypatch, tmp_path):
        token_path = tmp_path / "token"
        token_path.write_text("file-token")
        settings = IcebergCatalogSettings(
            uri="http://catalog:8181/catalog",
            warehouse="wh",
            token_file=str(token_path),
        )
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "env-token")
        assert IcebergStore._read_token(settings) == "env-token"

    def test_reads_token_from_file_and_strips_whitespace(self, monkeypatch, tmp_path):
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        token_path = tmp_path / "token"
        token_path.write_text("  sa-token-abc  \n")
        settings = IcebergCatalogSettings(
            uri="http://catalog:8181/catalog",
            warehouse="wh",
            token_file=str(token_path),
        )
        assert IcebergStore._read_token(settings) == "sa-token-abc"

    def test_missing_file_raises_runtime_error(self, monkeypatch, tmp_path):
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        settings = IcebergCatalogSettings(
            uri="http://catalog:8181/catalog",
            warehouse="wh",
            token_file=str(tmp_path / "nonexistent"),
        )
        with pytest.raises(RuntimeError, match="ICEBERG_CATALOG_TOKEN"):
            IcebergStore._read_token(settings)

    def test_missing_file_error_includes_path(self, monkeypatch, tmp_path):
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        token_file = str(tmp_path / "nonexistent")
        settings = IcebergCatalogSettings(
            uri="http://catalog:8181/catalog",
            warehouse="wh",
            token_file=token_file,
        )
        with pytest.raises(RuntimeError, match=token_file):
            IcebergStore._read_token(settings)

    def test_missing_file_wraps_file_not_found(self, monkeypatch, tmp_path):
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        settings = IcebergCatalogSettings(
            uri="http://catalog:8181/catalog",
            warehouse="wh",
            token_file=str(tmp_path / "nonexistent"),
        )
        with pytest.raises(RuntimeError) as exc_info:
            IcebergStore._read_token(settings)
        assert isinstance(exc_info.value.__cause__, FileNotFoundError)




def _mock_catalog() -> MagicMock:
    catalog = MagicMock()
    catalog.table_exists.return_value = True
    return catalog


class TestFromEnv:
    def _setup_env(self, monkeypatch, *, uri="http://catalog:8181/catalog", warehouse="wh"):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", uri)
        monkeypatch.setenv("ICEBERG_CATALOG_WAREHOUSE", warehouse)
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "test-bearer-token")
        monkeypatch.delenv("ICEBERG_CATALOG_NAME", raising=False)

    def test_calls_load_catalog_with_rest_properties(self, monkeypatch):
        self._setup_env(monkeypatch)
        with patch("core.iceberg.store.load_catalog", return_value=_mock_catalog()) as mock_load:
            IcebergStore.from_env([])

        mock_load.assert_called_once_with(
            "lakehouse",
            type="rest",
            uri="http://catalog:8181/catalog",
            warehouse="wh",
            token="test-bearer-token",
        )

    def test_custom_catalog_name(self, monkeypatch):
        self._setup_env(monkeypatch)
        monkeypatch.setenv("ICEBERG_CATALOG_NAME", "my-catalog")
        with patch("core.iceberg.store.load_catalog", return_value=_mock_catalog()) as mock_load:
            IcebergStore.from_env([])
        assert mock_load.call_args[0][0] == "my-catalog"

    def test_create_all_called(self, monkeypatch):
        self._setup_env(monkeypatch)
        catalog = _mock_catalog()
        with patch("core.iceberg.store.load_catalog", return_value=catalog):
            IcebergStore.from_env([])

    def test_missing_uri_raises_before_catalog(self, monkeypatch):
        monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
        monkeypatch.setenv("ICEBERG_CATALOG_WAREHOUSE", "wh")
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "tok")
        with patch("core.iceberg.store.load_catalog") as mock_load:
            with pytest.raises(ValidationError, match="ICEBERG_CATALOG_URI"):
                IcebergStore.from_env([])
        mock_load.assert_not_called()

    def test_missing_warehouse_raises_before_catalog(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://catalog:8181/catalog")
        monkeypatch.delenv("ICEBERG_CATALOG_WAREHOUSE", raising=False)
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN", "tok")
        with patch("core.iceberg.store.load_catalog") as mock_load:
            with pytest.raises(ValidationError, match="ICEBERG_CATALOG_WAREHOUSE"):
                IcebergStore.from_env([])
        mock_load.assert_not_called()

    def test_missing_token_file_raises_before_catalog(self, monkeypatch, tmp_path):
        self._setup_env(monkeypatch)
        monkeypatch.delenv("ICEBERG_CATALOG_TOKEN", raising=False)
        monkeypatch.setenv("ICEBERG_CATALOG_TOKEN_FILE", str(tmp_path / "nonexistent"))
        with patch("core.iceberg.store.load_catalog") as mock_load:
            with pytest.raises(RuntimeError, match="ICEBERG_CATALOG_TOKEN"):
                IcebergStore.from_env([])
        mock_load.assert_not_called()

    def test_token_not_logged(self, monkeypatch, caplog):
        self._setup_env(monkeypatch)
        with patch("core.iceberg.store.load_catalog", return_value=_mock_catalog()):
            with caplog.at_level(logging.DEBUG):
                IcebergStore.from_env([])
        assert "test-bearer-token" not in caplog.text




class TestStableMetadataPath:
    def test_returns_current_metadata_json(self):
        loc = "s3://bucket/warehouse/ns/tbl/metadata/v1.metadata.json"
        stable = IcebergStore._stable_metadata_path(loc)
        assert stable == "s3://bucket/warehouse/ns/tbl/metadata/current.metadata.json"

    def test_empty_location_raises(self):
        with pytest.raises(ValueError, match="invalid Iceberg metadata location"):
            IcebergStore._stable_metadata_path("no-slash-here")

    def test_same_path_short_circuits_sync(self):
        catalog = MagicMock()
        iceberg_table = MagicMock()
        stable_loc = "s3://bucket/meta/current.metadata.json"
        iceberg_table.metadata_location = stable_loc
        catalog.load_table.return_value = iceberg_table

        store = IcebergStore(catalog, [])
        store._sync_metadata(("ns", "tbl"))

        iceberg_table.io.new_input.assert_not_called()
