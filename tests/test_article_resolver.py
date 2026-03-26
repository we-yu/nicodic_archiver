from unittest.mock import patch

from bs4 import BeautifulSoup
import pytest

from article_resolver import resolve_article_input


def test_resolve_article_input_succeeds_for_full_article_url():
    soup = BeautifulSoup(
        """
        <html><head>
        <meta property="og:title" content="Fooとは">
        <meta property="og:url" content="https://dic.nicovideo.jp/a/12345">
        </head></html>
        """,
        "lxml",
    )

    with patch("article_resolver.fetch_page", return_value=soup) as mock_fetch:
        result = resolve_article_input(" https://dic.nicovideo.jp/a/Foo ")

    mock_fetch.assert_called_once_with("https://dic.nicovideo.jp/a/Foo")
    assert result == {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        },
        "title": "Foo",
        "matched_by": "article_url",
        "normalized_input": "https://dic.nicovideo.jp/a/Foo",
    }


def test_resolve_article_input_succeeds_for_exact_title_first_page_only():
    search_soup = BeautifulSoup(
        """
        <html><body>
        <a href="/a/12345">Foo</a>
        <a href="/a/99999">Foo Extra</a>
        </body></html>
        """,
        "lxml",
    )
    article_soup = BeautifulSoup(
        """
        <html><head>
        <meta property="og:title" content="Fooとは">
        <meta property="og:url" content="https://dic.nicovideo.jp/a/12345">
        </head></html>
        """,
        "lxml",
    )

    with patch(
        "article_resolver.fetch_page",
        side_effect=[search_soup, article_soup],
    ) as mock_fetch:
        result = resolve_article_input("Foo")

    assert mock_fetch.call_count == 2
    assert result["ok"] is True
    assert result["canonical_target"] == {
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
    }
    assert result["title"] == "Foo"
    assert result["matched_by"] == "exact_title"
    assert result["normalized_input"] == "Foo"


def test_resolve_article_input_rejects_invalid_url_shape():
    result = resolve_article_input("https://example.com/a/12345")

    assert result == {
        "ok": False,
        "failure_kind": "invalid_input",
        "normalized_input": "https://example.com/a/12345",
    }


def test_resolve_article_input_returns_not_found_for_missing_exact_title():
    search_soup = BeautifulSoup(
        """
        <html><body>
        <a href="/a/12345">Other</a>
        </body></html>
        """,
        "lxml",
    )

    with patch("article_resolver.fetch_page", return_value=search_soup):
        result = resolve_article_input("Foo")

    assert result == {
        "ok": False,
        "failure_kind": "not_found",
        "normalized_input": "Foo",
    }


def test_resolve_article_input_returns_ambiguous_for_multiple_exact_titles():
    search_soup = BeautifulSoup(
        """
        <html><body>
        <a href="/a/12345">Foo</a>
        <a href="/a/67890">Foo</a>
        </body></html>
        """,
        "lxml",
    )

    with patch("article_resolver.fetch_page", return_value=search_soup):
        result = resolve_article_input("Foo")

    assert result == {
        "ok": False,
        "failure_kind": "ambiguous",
        "normalized_input": "Foo",
    }


def test_resolve_article_input_returns_not_found_when_title_search_is_404():
    with patch(
        "article_resolver.fetch_page",
        side_effect=RuntimeError(
            "Failed to fetch https://dic.nicovideo.jp/search/Foo (status=404)"
        ),
    ):
        result = resolve_article_input("Foo")

    assert result == {
        "ok": False,
        "failure_kind": "not_found",
        "normalized_input": "Foo",
    }


def test_resolve_article_input_still_raises_unexpected_title_search_errors():
    with patch(
        "article_resolver.fetch_page",
        side_effect=RuntimeError(
            "Failed to fetch https://dic.nicovideo.jp/search/Foo "
            "(timeout=10s)"
        ),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            resolve_article_input("Foo")

    assert "timeout=10s" in str(exc_info.value)
