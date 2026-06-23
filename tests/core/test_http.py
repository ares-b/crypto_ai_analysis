
from unittest.mock import MagicMock, patch

import httpx
import pytest

from core.http import HttpClient, HttpError


def _make_response(status: int, *, json=None, text: str = "", headers: dict | None = None) -> MagicMock:
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.is_success = 200 <= status < 300
    r.headers = headers or {}
    r.json.return_value = json if json is not None else {}
    r.text = text
    return r


def _client(max_retries: int = 3) -> tuple[HttpClient, MagicMock]:
    with patch("httpx.Client") as mock_cls:
        mock_session = MagicMock()
        mock_cls.return_value = mock_session
        c = HttpClient("https://api.example.com", max_retries=max_retries)
        c._session = mock_session
    return c, mock_session


class TestBuildUrl:
    def test_absolute_path_returned_as_is(self):
        c, _ = _client()
        assert c._build_url("https://other.com/path") == "https://other.com/path"

    def test_relative_path_joined_with_base(self):
        c, _ = _client()
        assert c._build_url("/data") == "https://api.example.com/data"

    def test_empty_path_returns_base(self):
        c, _ = _client()
        assert c._build_url("") == "https://api.example.com"

    def test_no_base_url_returns_path(self):
        c = HttpClient("")
        assert c._build_url("/data") == "/data"

    def test_trailing_slash_stripped_from_base(self):
        c = HttpClient("https://api.example.com/")
        assert c._build_url("/data") == "https://api.example.com/data"


class TestRequest:
    def test_success_on_first_attempt(self):
        c, session = _client()
        session.get.return_value = _make_response(200, json={"ok": True})
        resp = c._request("/data")
        assert resp.status_code == 200

    def test_non_success_raises_http_error(self):
        c, session = _client()
        session.get.return_value = _make_response(404)
        with pytest.raises(HttpError) as exc_info:
            c._request("/data")
        assert exc_info.value.status_code == 404

    def test_transport_error_retries(self, mocker):
        mocker.patch("time.sleep")
        c, session = _client(max_retries=3)
        session.get.side_effect = [
            httpx.TransportError("timeout"),
            httpx.TransportError("timeout"),
            _make_response(200),
        ]
        resp = c._request("/data")
        assert resp.status_code == 200
        assert session.get.call_count == 3

    def test_transport_error_exhausted_raises(self, mocker):
        mocker.patch("time.sleep")
        c, session = _client(max_retries=2)
        session.get.side_effect = httpx.TransportError("timeout")
        with pytest.raises(HttpError) as exc_info:
            c._request("/data")
        assert exc_info.value.status_code == 0

    def test_retryable_status_retries_with_retry_after(self, mocker):
        mocker.patch("time.sleep")
        c, session = _client(max_retries=2)
        session.get.side_effect = [
            _make_response(429, headers={"Retry-After": "5"}),
            _make_response(200),
        ]
        resp = c._request("/data")
        assert resp.status_code == 200
        assert session.get.call_count == 2

    def test_retryable_status_on_last_attempt_raises(self, mocker):
        mocker.patch("time.sleep")
        c, session = _client(max_retries=1)
        session.get.return_value = _make_response(503)
        with pytest.raises(HttpError) as exc_info:
            c._request("/data")
        assert exc_info.value.status_code == 503

    def test_params_forwarded(self):
        c, session = _client()
        session.get.return_value = _make_response(200)
        c._request("/data", params={"key": "val"})
        session.get.assert_called_once_with("https://api.example.com/data", params={"key": "val"})


class TestGetJsonText:
    def test_get_json_returns_parsed(self):
        c, session = _client()
        session.get.return_value = _make_response(200, json={"result": 42})
        assert c.get_json("/data") == {"result": 42}

    def test_get_text_returns_string(self):
        c, session = _client()
        session.get.return_value = _make_response(200, text="hello")
        assert c.get_text("/data") == "hello"


class TestContextManager:
    def test_enter_returns_self(self):
        c, _ = _client()
        assert c.__enter__() is c

    def test_exit_calls_close(self, mocker):
        c, session = _client()
        c.__exit__(None, None, None)
        session.close.assert_called_once()

    def test_close_calls_session_close(self):
        c, session = _client()
        c.close()
        session.close.assert_called_once()
