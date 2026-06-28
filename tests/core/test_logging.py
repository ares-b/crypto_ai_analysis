import logging

from core.logging import (
    SecretMaskingFilter,
    _discover_secret_values,
    get_logger,
    install_secret_masking,
)


class TestRedact:
    def test_masks_query_api_key(self):
        f = SecretMaskingFilter()
        out = f.redact("GET https://api.stlouisfed.org/x?api_key=abc123&file_type=json")
        assert "abc123" not in out
        assert "api_key=***" in out
        assert "file_type=json" in out

    def test_masks_token_and_secret_params(self):
        f = SecretMaskingFilter()
        out = f.redact("uri?token=tok_9&secret=sek_8&key=k7")
        assert "tok_9" not in out and "sek_8" not in out and "k7" not in out

    def test_masks_bearer(self):
        f = SecretMaskingFilter()
        out = f.redact("Authorization: Bearer abc.def-123")
        assert "abc.def-123" not in out
        assert "Bearer ***" in out

    def test_masks_literal_secret_values(self):
        f = SecretMaskingFilter(("supersecretvalue",))
        assert "supersecretvalue" not in f.redact("leaked supersecretvalue here")

    def test_leaves_clean_text(self):
        f = SecretMaskingFilter()
        assert f.redact("nothing to hide here") == "nothing to hide here"


class TestFilter:
    def test_filter_rewrites_record(self):
        f = SecretMaskingFilter()
        rec = logging.LogRecord("t", logging.INFO, __file__, 1, "go to x?api_key=%s", ("zzz",), None)
        assert f.filter(rec) is True
        assert "zzz" not in rec.getMessage()
        assert "api_key=***" in rec.getMessage()

    def test_filter_scrubs_exception_traceback(self):
        f = SecretMaskingFilter()
        try:
            raise ValueError("boom at https://api.x/y?api_key=leakme")
        except ValueError:
            import sys

            rec = logging.LogRecord("t", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
        assert f.filter(rec) is True
        assert rec.exc_info is None  # rendered + cached so it is not re-rendered unmasked
        assert "leakme" not in rec.exc_text
        assert "api_key=***" in rec.exc_text

    def test_already_filtered_record_skipped(self):
        f = SecretMaskingFilter()
        rec = logging.LogRecord("t", logging.INFO, __file__, 1, "x?api_key=secretval", (), None)
        assert f.filter(rec) is True
        rec.msg = "x?api_key=secretval"  # simulate a second handler seeing the flagged record
        assert f.filter(rec) is True
        assert rec.getMessage() == "x?api_key=secretval"  # flag short-circuits re-redaction


class TestDiscoverSecretValues:
    def test_picks_secret_named_vars(self, monkeypatch):
        monkeypatch.setenv("SOME_API_KEY", "longsecretvalue1")
        monkeypatch.setenv("DB_PASSWORD", "anotherlongsecret")
        values = _discover_secret_values()
        assert "longsecretvalue1" in values and "anotherlongsecret" in values

    def test_skips_paths_ids_and_short_values(self, monkeypatch):
        monkeypatch.setenv("CATALOG_TOKEN_FILE", "/var/run/secrets/token")  # path suffix
        monkeypatch.setenv("CLIENT_ID", "public-client-id-value")          # _ID suffix
        monkeypatch.setenv("API_KEY", "short")                              # too short
        values = _discover_secret_values()
        assert "/var/run/secrets/token" not in values
        assert "public-client-id-value" not in values
        assert "short" not in values

    def test_skips_non_secret_names(self, monkeypatch):
        monkeypatch.setenv("REGULAR_CONFIG", "not-a-secret-value-here")
        assert "not-a-secret-value-here" not in _discover_secret_values()


class TestGetLogger:
    def test_returns_logger(self):
        assert isinstance(get_logger("test.x"), logging.Logger)

    def test_installs_masking_on_httpx(self):
        get_logger("test.y")
        httpx = logging.getLogger("httpx")
        assert any(isinstance(flt, SecretMaskingFilter) for flt in httpx.filters)


class TestInstallIdempotent:
    def test_repeat_install_does_not_stack_filters(self):
        httpx = logging.getLogger("httpx")
        httpx.filters = [flt for flt in httpx.filters if not isinstance(flt, SecretMaskingFilter)]

        for _ in range(4):
            install_secret_masking()

        maskers = [flt for flt in httpx.filters if isinstance(flt, SecretMaskingFilter)]
        assert len(maskers) == 1
