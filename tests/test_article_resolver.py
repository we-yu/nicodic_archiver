from unittest.mock import patch

from bs4 import BeautifulSoup

from article_resolver import resolve_article_input


def test_resolve_article_input_from_article_url_success():
    result = resolve_article_input(" https://dic.nicovideo.jp/a/12345 ")

    assert result["ok"] is True
    assert result["matched_by"] == "article_url"
    assert result["normalized_input"] == "https://dic.nicovideo.jp/a/12345"
    assert result["canonical_target"] == {
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
    }
    assert "title" in result


def test_resolve_article_input_empty_returns_invalid_input():
    result = resolve_article_input("   ")

    assert result == {
        "ok": False,
        "error_type": "invalid_input",
        "normalized_input": "",
    }


@patch("article_resolver.fetch_page")
def test_resolve_article_input_title_exact_success_first_page_only(mock_fetch_page):
    html = """
    <html><body>
      <a href="/a/12345">Foo</a>
      <a href="/a/99999">Bar</a>
    </body></html>
    """
    mock_fetch_page.return_value = BeautifulSoup(html, "lxml")

    result = resolve_article_input("Foo")

    assert result["ok"] is True
    assert result["matched_by"] == "title_exact"
    assert result["normalized_input"] == "Foo"
    assert result["title"] == "Foo"
    assert result["canonical_target"] == {
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
    }
    mock_fetch_page.assert_called_once()


@patch("article_resolver.fetch_page")
def test_resolve_article_input_title_exact_not_found(mock_fetch_page):
    html = """
    <html><body>
      <a href="/a/12345">Another</a>
    </body></html>
    """
    mock_fetch_page.return_value = BeautifulSoup(html, "lxml")

    result = resolve_article_input("Foo")

    assert result == {
        "ok": False,
        "error_type": "not_found",
        "normalized_input": "Foo",
    }


@patch("article_resolver.fetch_page")
def test_resolve_article_input_title_exact_ambiguous(mock_fetch_page):
    html = """
    <html><body>
      <a href="/a/12345">Foo</a>
      <a href="/a/54321">Foo</a>
    </body></html>
    """
    mock_fetch_page.return_value = BeautifulSoup(html, "lxml")

    result = resolve_article_input("Foo")

    assert result == {
        "ok": False,
        "error_type": "ambiguous",
        "normalized_input": "Foo",
    }
