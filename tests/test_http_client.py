from unittest.mock import Mock, patch

import pytest
import requests

from http_client import HEADERS, REQUEST_TIMEOUT_SECONDS, fetch_page
from http_client import resolve_id_article_url


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


def test_resolve_id_article_url_uses_effective_a_url_after_redirect():
    response = Mock(
        status_code=200,
        text="<html></html>",
        url=(
            "https://dic.nicovideo.jp/a/"
            "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93"
        ),
    )

    with patch("http_client.requests.get", return_value=response):
        resolved = resolve_id_article_url(
            "https://dic.nicovideo.jp/id/5364158"
        )

    assert resolved == (
        "https://dic.nicovideo.jp/a/"
        "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93"
    )


def test_resolve_id_article_url_falls_back_to_rel_canonical_a_url():
    response = Mock(
        status_code=200,
        text=(
            "<html><head>"
            '<link rel="canonical" href="/a/%E3%81%8A%E3%81%9D%E6%9D%BE'
            '%E3%81%95%E3%82%93">'
            '<meta property="og:url" '
            'content="https://dic.nicovideo.jp/id/5364158">'
            "</head></html>"
        ),
        url="https://dic.nicovideo.jp/id/5364158",
    )

    with patch("http_client.requests.get", return_value=response):
        resolved = resolve_id_article_url(
            "https://dic.nicovideo.jp/id/5364158"
        )

    assert resolved == (
        "https://dic.nicovideo.jp/a/"
        "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93"
    )


def test_resolve_id_article_url_ignores_og_url_id_without_a_target():
    response = Mock(
        status_code=200,
        text=(
            "<html><head>"
            '<meta property="og:url" '
            'content="https://dic.nicovideo.jp/id/5364158">'
            "</head></html>"
        ),
        url="https://dic.nicovideo.jp/id/5364158",
    )

    with patch("http_client.requests.get", return_value=response):
        resolved = resolve_id_article_url(
            "https://dic.nicovideo.jp/id/5364158"
        )

    assert resolved is None
