"""Unit tests for orchestrator: URL building, metadata, pagination, orchestration."""
from unittest.mock import patch, MagicMock

import pytest
from bs4 import BeautifulSoup

from orchestrator import (
    build_bbs_base_url,
    fetch_article_metadata,
    collect_all_responses,
    run_scrape,
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


# ----- run_scrape orchestration flow -----


def test_run_scrape_happy_path_orchestrates_dependencies_correctly():
    article_url = "https://dic.nicovideo.jp/a/12345"

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ) as mock_meta:
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ) as mock_build:
            with patch(
                "orchestrator.collect_all_responses",
                return_value=[{"res_no": 1}],
            ) as mock_collect:
                with patch("orchestrator.save_json") as mock_save_json:
                    conn = MagicMock()
                    with patch("orchestrator.init_db", return_value=conn) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch("orchestrator.print") as mock_print:
                                run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with("https://dic.nicovideo.jp/b/a/12345/")

    mock_save_json.assert_called_once_with(
        "12345",
        "a",
        "Title",
        article_url,
        [{"res_no": 1}],
    )

    mock_init.assert_called_once_with()
    mock_save_db.assert_called_once_with(
        conn,
        "12345",
        "a",
        "Title",
        article_url,
        [{"res_no": 1}],
    )
    conn.close.assert_called_once_with()

    # Final status message
    mock_print.assert_any_call("Saved to SQLite")


def test_run_scrape_propagates_error_from_metadata_and_does_not_init_db():
    article_url = "https://dic.nicovideo.jp/a/12345"

    with patch(
        "orchestrator.fetch_article_metadata",
        side_effect=RuntimeError("network error"),
    ) as mock_meta:
        with patch("orchestrator.init_db") as mock_init:
            with pytest.raises(RuntimeError):
                run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_init.assert_not_called()
