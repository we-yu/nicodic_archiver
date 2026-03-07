"""Unit tests for orchestrator: URL building, metadata fetch, pagination."""
from unittest.mock import patch, MagicMock

from bs4 import BeautifulSoup

from orchestrator import (
    build_bbs_base_url,
    fetch_article_metadata,
    collect_all_responses,
)


# ----- build_bbs_base_url -----


def test_build_bbs_base_url_normal():
    url = "https://dic.nicovideo.jp/a/12345"
    assert build_bbs_base_url(url) == "https://dic.nicovideo.jp/b/a/12345/"


def test_build_bbs_base_url_with_trailing_slash():
    url = "https://dic.nicovideo.jp/a/12345/"
    assert build_bbs_base_url(url) == "https://dic.nicovideo.jp/b/a/12345/"


# ----- fetch_article_metadata (mocked fetch) -----


def test_fetch_article_metadata_mocked():
    html = """
    <html><head>
    <meta property="og:title" content="Fooとは">
    <meta property="og:url" content="https://dic.nicovideo.jp/a/12345">
    </head></html>
    """
    soup = BeautifulSoup(html, "lxml")
    with patch("orchestrator.fetch_page", return_value=soup):
        article_id, article_type, title = fetch_article_metadata(
            "https://dic.nicovideo.jp/a/12345"
        )
    assert article_id == "12345"
    assert article_type == "a"
    assert title == "Foo"


def test_fetch_article_metadata_missing_meta():
    soup = BeautifulSoup("<html><body></body></html>", "lxml")
    with patch("orchestrator.fetch_page", return_value=soup):
        article_id, article_type, title = fetch_article_metadata(
            "https://dic.nicovideo.jp/a/999"
        )
    assert article_id == "unknown"
    assert article_type == "a"
    assert title == "unknown"


# ----- collect_all_responses: pagination and stopping -----


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_stops_on_empty_page(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 1}, {"res_no": 2}],
        [],
    ]
    result = collect_all_responses("https://dic.nicovideo.jp/b/a/12345/")
    assert result == [{"res_no": 1}, {"res_no": 2}]
    assert mock_fetch.call_count == 2
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/1-")
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/3-")
    mock_sleep.assert_called_once_with(1)


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_stops_on_fetch_error(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.side_effect = RuntimeError("Failed to fetch (status=404)")
    result = collect_all_responses("https://dic.nicovideo.jp/b/a/12345/")
    assert result == []
    mock_fetch.assert_called_once()
    mock_parse.assert_not_called()
    mock_sleep.assert_not_called()


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_single_page(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.return_value = []
    result = collect_all_responses("https://dic.nicovideo.jp/b/a/12345/")
    assert result == []
    mock_fetch.assert_called_once_with("https://dic.nicovideo.jp/b/a/12345/1-")
    mock_sleep.assert_not_called()
