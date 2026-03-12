"""Focused tests for http_client.fetch_page failure boundaries and timeout."""

from unittest.mock import patch, MagicMock

import pytest
from bs4 import BeautifulSoup
import requests

from http_client import fetch_page, HEADERS, DEFAULT_TIMEOUT


@patch("http_client.requests.get")
def test_fetch_page_success_uses_headers_and_timeout(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "<html><body>ok</body></html>"
    mock_get.return_value = mock_resp

    url = "https://example.com"
    soup = fetch_page(url)

    assert isinstance(soup, BeautifulSoup)

    # verify request parameters at HTTP boundary
    args, kwargs = mock_get.call_args
    assert args[0] == url
    assert kwargs["headers"] == HEADERS
    assert kwargs["timeout"] == DEFAULT_TIMEOUT


@patch("http_client.requests.get")
def test_fetch_page_non_200_raises_runtime_error(mock_get):
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "error"
    mock_get.return_value = mock_resp

    url = "https://example.com"
    with pytest.raises(RuntimeError) as exc_info:
        fetch_page(url)

    msg = str(exc_info.value)
    assert "Failed to fetch" in msg
    assert "status=500" in msg


@patch("http_client.requests.get")
def test_fetch_page_timeout_is_wrapped_as_runtime_error(mock_get):
    url = "https://example.com"
    mock_get.side_effect = requests.Timeout("request timed out")

    with pytest.raises(RuntimeError) as exc_info:
        fetch_page(url)

    msg = str(exc_info.value)
    assert "Failed to fetch" in msg
    assert "timeout" in msg


@patch("http_client.requests.get")
def test_fetch_page_request_exception_is_wrapped_as_runtime_error(mock_get):
    url = "https://example.com"
    mock_get.side_effect = requests.RequestException("network issue")

    with pytest.raises(RuntimeError) as exc_info:
        fetch_page(url)

    msg = str(exc_info.value)
    assert "Failed to fetch" in msg
    assert "request error" in msg
