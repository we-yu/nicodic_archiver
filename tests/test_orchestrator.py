"""Unit tests for orchestrator: URL building, metadata, pagination, orchestration."""
from unittest.mock import patch, MagicMock

import pytest
from bs4 import BeautifulSoup

from orchestrator import (
    ArticleNotFoundError,
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


def test_fetch_article_metadata_raises_when_article_meta_missing():
    soup = BeautifulSoup("<html><body></body></html>", "lxml")
    with patch("orchestrator.fetch_page", return_value=soup):
        with pytest.raises(
            ArticleNotFoundError,
            match=r"Article not found: https://dic.nicovideo.jp/a/999",
        ):
            fetch_article_metadata("https://dic.nicovideo.jp/a/999")


def test_fetch_article_metadata_wraps_404_as_article_not_found():
    with patch(
        "orchestrator.fetch_page",
        side_effect=RuntimeError(
            "Failed to fetch https://dic.nicovideo.jp/a/999 (status=404)"
        ),
    ):
        with pytest.raises(
            ArticleNotFoundError,
            match=r"Article not found: https://dic.nicovideo.jp/a/999",
        ):
            fetch_article_metadata("https://dic.nicovideo.jp/a/999")


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
    result, interrupted = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert result == [{"res_no": 1}, {"res_no": 2}]
    assert interrupted is False
    assert mock_fetch.call_count == 2
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/1-")
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/3-")
    mock_sleep.assert_called_once_with(1)


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_returns_empty_for_missing_bbs(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.side_effect = RuntimeError(
        "Failed to fetch https://dic.nicovideo.jp/b/a/12345/1- (status=404)"
    )
    result, interrupted = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert result == []
    assert interrupted is False
    mock_fetch.assert_called_once()
    mock_parse.assert_not_called()
    mock_sleep.assert_not_called()


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_propagates_first_page_non_404_fetch_error(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.side_effect = RuntimeError("network unavailable")

    with pytest.raises(RuntimeError, match=r"network unavailable"):
        collect_all_responses("https://dic.nicovideo.jp/b/a/12345/")

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
    result, interrupted = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert result == []
    assert interrupted is False
    mock_fetch.assert_called_once_with("https://dic.nicovideo.jp/b/a/12345/1-")
    mock_sleep.assert_not_called()


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_sets_interrupted_on_later_page_error(
    mock_fetch, mock_parse, mock_sleep
):
    # 1ページ目は成功し、2ページ目でエラーが発生するケース
    mock_fetch.side_effect = [
        MagicMock(),
        RuntimeError("temporary network issue"),
    ]
    mock_parse.return_value = [{"res_no": 1}]

    result, interrupted = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )

    assert result == [{"res_no": 1}]
    assert interrupted is True
    assert mock_fetch.call_count == 2
    mock_sleep.assert_called_once_with(1)


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
                return_value=([{"res_no": 1}], False),
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


def test_run_scrape_article_not_found_skips_save_path():
    article_url = "https://dic.nicovideo.jp/a/999"

    with patch(
        "orchestrator.fetch_article_metadata",
        side_effect=ArticleNotFoundError(f"Article not found: {article_url}"),
    ) as mock_meta:
        with patch("orchestrator.collect_all_responses") as mock_collect:
            with patch("orchestrator.save_json") as mock_save_json:
                with patch("orchestrator.init_db") as mock_init:
                    with patch("orchestrator.save_to_db") as mock_save_db:
                        with patch("orchestrator.print") as mock_print:
                            run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_collect.assert_not_called()
    mock_save_json.assert_not_called()
    mock_init.assert_not_called()
    mock_save_db.assert_not_called()
    mock_print.assert_any_call(f"Article not found: {article_url}")


def test_run_scrape_saves_empty_result_for_zero_response_case():
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
                return_value=([], False),
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
    mock_save_json.assert_called_once_with("12345", "a", "Title", article_url, [])
    mock_init.assert_called_once_with()
    mock_save_db.assert_called_once_with(
        conn,
        "12345",
        "a",
        "Title",
        article_url,
        [],
    )
    conn.close.assert_called_once_with()
    mock_print.assert_any_call("No BBS responses found; saving empty result")
    mock_print.assert_any_call("Saved to SQLite")


def test_run_scrape_logs_and_saves_partial_on_later_page_interruption():
    article_url = "https://dic.nicovideo.jp/a/12345"

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ) as mock_meta:
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ) as mock_build:
            partial = [{"res_no": 1}]
            with patch(
                "orchestrator.collect_all_responses",
                return_value=(partial, True),
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
        partial,
    )
    mock_init.assert_called_once_with()
    mock_save_db.assert_called_once_with(
        conn,
        "12345",
        "a",
        "Title",
        article_url,
        partial,
    )
    conn.close.assert_called_once_with()
    # later-page interruption 向けのログが出ていること
    joined_calls = " ".join(
        " ".join(map(str, c.args)) for c in mock_print.call_args_list
    )
    assert "BBS fetch interrupted; saving partial responses" in joined_calls
