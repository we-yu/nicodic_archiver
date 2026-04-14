import pytest
from bs4 import BeautifulSoup

from orchestrator import ArticleRedirectedError, fetch_article_metadata


def test_fetch_article_metadata_detects_meta_refresh_redirect():
    html = """
    <html><head>
    <meta http-equiv="refresh" content="0;URL=https://dic.nicovideo.jp/a/12345">
    </head><body></body></html>
    """
    soup = BeautifulSoup(html, "lxml")
    with pytest.raises(ArticleRedirectedError) as exc_info:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("orchestrator.fetch_page", lambda url: soup)
            fetch_article_metadata("https://dic.nicovideo.jp/a/old")

    assert exc_info.value.redirect_url == "https://dic.nicovideo.jp/a/12345"


def test_fetch_article_metadata_detects_location_replace_redirect():
    html = """
    <html><head>
    <script>
    location.replace("https://dic.nicovideo.jp/a/99999");
    </script>
    </head><body></body></html>
    """
    soup = BeautifulSoup(html, "lxml")
    with pytest.raises(ArticleRedirectedError) as exc_info:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr("orchestrator.fetch_page", lambda url: soup)
            fetch_article_metadata("https://dic.nicovideo.jp/a/old")

    assert exc_info.value.redirect_url == "https://dic.nicovideo.jp/a/99999"
