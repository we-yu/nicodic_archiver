from unittest.mock import Mock, patch

import pytest
import requests

from http_client import HEADERS, REQUEST_TIMEOUT_SECONDS, fetch_page


def test_fetch_page_returns_soup_and_sets_timeout():
    response = Mock(status_code=200, text="<html><title>ok</title></html>")

    with patch("http_client.requests.get", return_value=response) as mock_get:
        soup = fetch_page("https://example.com/page")

    assert soup.title.string == "ok"
    mock_get.assert_called_once_with(
        "https://example.com/page",
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )


def test_fetch_page_raises_runtime_error_for_non_200_status():
    response = Mock(status_code=404, text="not found")

    with patch("http_client.requests.get", return_value=response):
        with pytest.raises(
            RuntimeError,
            match=r"Failed to fetch https://example.com/page \(status=404\)",
        ):
            fetch_page("https://example.com/page")


def test_fetch_page_wraps_timeout_error_with_stable_message():
    with patch("http_client.requests.get", side_effect=requests.Timeout("timed out")):
        with pytest.raises(
            RuntimeError,
            match=r"Failed to fetch https://example.com/page \(timeout=10s\)",
        ) as exc_info:
            fetch_page("https://example.com/page")

    assert isinstance(exc_info.value.__cause__, requests.Timeout)


def test_fetch_page_wraps_request_exception_with_runtime_error():
    with patch(
        "http_client.requests.get",
        side_effect=requests.ConnectionError("connection dropped"),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            fetch_page("https://example.com/page")

    message = str(exc_info.value)
    assert "Failed to fetch https://example.com/page" in message
    assert "ConnectionError" in message
    assert "connection dropped" in message
    assert isinstance(exc_info.value.__cause__, requests.ConnectionError)
