"""Unit tests for orchestrator: URL building, metadata, pagination, orchestration."""
import os
from unittest.mock import patch, MagicMock

import pytest
from bs4 import BeautifulSoup

from orchestrator import (
    ArticleMetadataResult,
    ArticleNotFoundError,
    DEFAULT_SCRAPE_PAGE_DELAY_SECONDS,
    QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP,
    RedirectArticleError,
    build_bbs_base_url,
    fetch_article_metadata_record,
    fetch_article_metadata,
    collect_all_responses,
    drain_queue_requests,
    extract_redirect_target_url,
    get_scrape_delay_seconds,
    get_max_saved_res_no,
    is_redirect_article_page,
    load_saved_responses,
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
    <meta itemprop="datePublished" content="2024-01-02T03:04:05+09:00">
    <meta itemprop="dateModified" content="2025-02-03T04:05:06+09:00">
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


def test_fetch_article_metadata_record_reads_article_dates():
    html = """
    <html><head>
    <meta property="og:title" content="Fooとは">
    <meta property="og:url" content="https://dic.nicovideo.jp/a/12345">
    <meta itemprop="datePublished" content="2024-01-02T03:04:05+09:00">
    <meta itemprop="dateModified" content="2025-02-03T04:05:06+09:00">
    </head></html>
    """
    soup = BeautifulSoup(html, "lxml")

    with patch("orchestrator.fetch_page", return_value=soup):
        record = fetch_article_metadata_record(
            "https://dic.nicovideo.jp/a/12345"
        )

    assert record == {
        "article_id": "12345",
        "article_type": "a",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "title": "Foo",
        "published_at": "2024-01-02T03:04:05+09:00",
        "modified_at": "2025-02-03T04:05:06+09:00",
    }


def test_fetch_article_metadata_record_prefers_canonical_a_over_og_id():
    html = """
    <html><head>
    <meta property="og:title" content="おそ松さんとは">
    <link rel="canonical"
     href="/a/%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93">
    <meta property="og:url" content="https://dic.nicovideo.jp/id/5364158">
    </head></html>
    """
    soup = BeautifulSoup(html, "lxml")

    with patch("orchestrator.fetch_page", return_value=soup):
        record = fetch_article_metadata_record(
            "https://dic.nicovideo.jp/id/5364158"
        )

    assert record == {
        "article_id": "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93",
        "article_type": "a",
        "article_url": (
            "https://dic.nicovideo.jp/a/"
            "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93"
        ),
        "title": "おそ松さん",
        "published_at": None,
        "modified_at": None,
    }


def test_fetch_article_metadata_preserves_canonical_article_url():
    canonical_url = (
        "https://dic.nicovideo.jp/a/"
        "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93"
    )

    with patch(
        "orchestrator.fetch_article_metadata_record",
        return_value={
            "article_id": "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93",
            "article_type": "a",
            "article_url": canonical_url,
            "title": "おそ松さん",
            "published_at": None,
            "modified_at": None,
        },
    ):
        result = fetch_article_metadata("https://dic.nicovideo.jp/id/5364158")

    assert result.article_url == canonical_url


def test_run_scrape_respects_existing_fetch_metadata_patch_seam():
    article_url = "https://dic.nicovideo.jp/a/12345"

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ) as mock_meta:
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ):
            with patch("orchestrator.get_max_saved_res_no", return_value=None):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=([], False, False),
                ):
                    conn = MagicMock()
                    with patch(
                        "orchestrator.init_db",
                        return_value=conn,
                    ):
                        with patch(
                            "orchestrator.save_to_db"
                        ) as mock_save_db:
                            with patch("orchestrator.print"):
                                run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    assert mock_save_db.call_args.kwargs == {}


def test_run_scrape_legacy_tuple_metadata_uses_input_a_identity():
    article_url = "https://dic.nicovideo.jp/a/12345"
    responses = [{"res_no": 1}]

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ):
        with patch("orchestrator.get_max_saved_res_no", return_value=None):
            with patch(
                "orchestrator.collect_all_responses",
                return_value=(responses, False, False),
            ) as mock_collect:
                conn = MagicMock()
                with patch("orchestrator.init_db", return_value=conn):
                    with patch("orchestrator.save_to_db") as mock_save_db:
                        with patch("orchestrator.print"):
                            run_scrape(article_url)

    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/12345/",
        response_cap=None,
        progress_reporter=None,
    )
    assert mock_save_db.call_args.args[1:5] == (
        "12345",
        "a",
        "Title",
        article_url,
    )


def test_run_scrape_passes_metadata_kwargs_only_when_present():
    article_url = "https://dic.nicovideo.jp/a/12345"

    patched_result = ArticleMetadataResult(
        "12345",
        "a",
        "Title",
        published_at="2024-01-02T03:04:05+09:00",
        modified_at="2025-02-03T04:05:06+09:00",
    )

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=patched_result,
    ):
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ):
            with patch("orchestrator.get_max_saved_res_no", return_value=None):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=([], False, False),
                ):
                    conn = MagicMock()
                    with patch(
                        "orchestrator.init_db",
                        return_value=conn,
                    ):
                        with patch(
                            "orchestrator.save_to_db"
                        ) as mock_save_db:
                            with patch("orchestrator.print"):
                                run_scrape(article_url)

    assert mock_save_db.call_args.kwargs == {
        "published_at": "2024-01-02T03:04:05+09:00",
        "modified_at": "2025-02-03T04:05:06+09:00",
    }


def test_run_scrape_uses_canonical_article_url_for_bbs_collection():
    article_url = "https://dic.nicovideo.jp/id/5364158"
    canonical_url = (
        "https://dic.nicovideo.jp/a/"
        "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93"
    )

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=ArticleMetadataResult(
            "5364158",
            "id",
            "おそ松さん",
            article_url=canonical_url,
        ),
    ) as mock_meta:
        with patch("orchestrator.get_max_saved_res_no", return_value=None):
            with patch(
                "orchestrator.collect_all_responses",
                return_value=([], False, False),
            ) as mock_collect:
                conn = MagicMock()
                with patch("orchestrator.init_db", return_value=conn):
                    with patch("orchestrator.save_to_db"):
                        with patch("orchestrator.print"):
                            run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/"
        "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93/",
        response_cap=None,
        progress_reporter=None,
    )


def test_run_scrape_persists_final_canonical_a_identity_at_save_boundary():
    article_url = "https://dic.nicovideo.jp/id/5364158"
    canonical_url = (
        "https://dic.nicovideo.jp/a/"
        "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93"
    )
    responses = [{"res_no": 1}]

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=ArticleMetadataResult(
            "5364158",
            "id",
            "おそ松さん",
            article_url=canonical_url,
        ),
    ):
        with patch("orchestrator.get_max_saved_res_no", return_value=None):
            with patch(
                "orchestrator.collect_all_responses",
                return_value=(responses, False, False),
            ):
                conn = MagicMock()
                with patch("orchestrator.init_db", return_value=conn):
                    with patch("orchestrator.save_to_db") as mock_save_db:
                        with patch("orchestrator.print"):
                            run_scrape(article_url)

    save_args = mock_save_db.call_args.args
    assert save_args[1] == "%E3%81%8A%E3%81%9D%E6%9D%BE%E3%81%95%E3%82%93"
    assert save_args[2] == "a"
    assert save_args[4] == canonical_url
    assert save_args[5] == responses


def test_run_scrape_does_not_write_json_artifact(tmp_path, monkeypatch):
    """run_scrape must not create any JSON file (JSON always-on write off)."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    article_url = "https://dic.nicovideo.jp/a/12345"

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=ArticleMetadataResult("12345", "a", "Foo"),
    ):
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ):
            with patch(
                "orchestrator.get_max_saved_res_no", return_value=None
            ):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=(
                        [{"res_no": 1, "id_hash": None,
                          "poster_name": None, "posted_at": None,
                          "content": "hi", "content_html": None}],
                        False,
                        False,
                    ),
                ):
                    conn = MagicMock()
                    with patch("orchestrator.init_db", return_value=conn):
                        with patch("orchestrator.save_to_db"):
                            with patch("orchestrator.print"):
                                run_scrape(article_url)

    json_files = list(tmp_path.rglob("*.json"))
    assert json_files == []


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


def test_extract_redirect_target_url_from_meta_refresh_page():
    soup = BeautifulSoup(
        """
        <html><head>
        <meta http-equiv="refresh"
              content="0; url=https://dic.nicovideo.jp/a/redirected">
        </head></html>
        """,
        "lxml",
    )

    assert extract_redirect_target_url(
        "https://dic.nicovideo.jp/a/original",
        soup,
    ) == "https://dic.nicovideo.jp/a/redirected"
    assert is_redirect_article_page(
        "https://dic.nicovideo.jp/a/original",
        soup,
    ) is True


def test_extract_redirect_target_url_from_location_replace_script():
    soup = BeautifulSoup(
        """
        <html><head>
        <script>
        location.replace('/a/redirected-script');
        </script>
        </head></html>
        """,
        "lxml",
    )

    assert extract_redirect_target_url(
        "https://dic.nicovideo.jp/a/original",
        soup,
    ) == "https://dic.nicovideo.jp/a/redirected-script"


def test_fetch_article_metadata_raises_redirect_article_error():
    soup = BeautifulSoup(
        """
        <html><head>
        <title>旧記事とは - ニコニコ大百科</title>
        <meta http-equiv="refresh"
              content="0; url=https://dic.nicovideo.jp/a/new-article">
        </head></html>
        """,
        "lxml",
    )

    with patch("orchestrator.fetch_page", return_value=soup):
        with pytest.raises(RedirectArticleError) as exc_info:
            fetch_article_metadata("https://dic.nicovideo.jp/a/old-article")

    assert exc_info.value.redirect_target_url == (
        "https://dic.nicovideo.jp/a/new-article"
    )
    assert exc_info.value.article_title == "旧記事"


# ----- collect_all_responses: pagination and stopping -----


def test_get_scrape_delay_seconds_defaults_to_five_when_unset():
    with patch.dict(os.environ, {}, clear=True):
        assert get_scrape_delay_seconds() == 5.0


def test_get_scrape_delay_seconds_uses_valid_env_value():
    with patch.dict(
        os.environ,
        {"SCRAPE_PAGE_DELAY_SECONDS": "2.5"},
        clear=True,
    ):
        assert get_scrape_delay_seconds() == 2.5


@pytest.mark.parametrize(
    "raw_value",
    ["", "abc", "-1", "NaN", "inf", "-inf"],
)
def test_get_scrape_delay_seconds_falls_back_for_invalid_values(raw_value):
    with patch.dict(
        os.environ,
        {"SCRAPE_PAGE_DELAY_SECONDS": raw_value},
        clear=True,
    ):
        assert get_scrape_delay_seconds() == DEFAULT_SCRAPE_PAGE_DELAY_SECONDS


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
    result, interrupted, cap_reached = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert result == [{"res_no": 1}, {"res_no": 2}]
    assert interrupted is False
    assert cap_reached is False
    assert mock_fetch.call_count == 2
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/1-")
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/3-")
    mock_sleep.assert_called_once_with(5.0)


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_resumes_from_anchor_and_filters_existing_first_page(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 4}, {"res_no": 5}],
        [{"res_no": 6}],
        [],
    ]

    result, interrupted, cap_reached = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/",
        start=4,
        max_saved_res_no=4,
    )

    assert result == [{"res_no": 5}, {"res_no": 6}]
    assert interrupted is False
    assert cap_reached is False
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/4-")
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/6-")
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/7-")
    assert mock_sleep.call_count == 2


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_resume_can_return_zero_new_without_failure(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 4}],
        [],
    ]

    result, interrupted, cap_reached = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/",
        start=4,
        max_saved_res_no=4,
    )

    assert result == []
    assert interrupted is False
    assert cap_reached is False
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/4-")
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/5-")
    mock_sleep.assert_called_once_with(5.0)


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_returns_empty_for_missing_bbs(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.side_effect = RuntimeError(
        "Failed to fetch https://dic.nicovideo.jp/b/a/12345/1- (status=404)"
    )
    result, interrupted, cap_reached = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert result == []
    assert interrupted is False
    assert cap_reached is False
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
    result, interrupted, cap_reached = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert result == []
    assert interrupted is False
    assert cap_reached is False
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

    result, interrupted, cap_reached = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )

    assert result == [{"res_no": 1}]
    assert interrupted is True
    assert cap_reached is False
    assert mock_fetch.call_count == 2
    mock_sleep.assert_called_once_with(5.0)


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
@patch("orchestrator.RESPONSE_CAP", 3)
def test_collect_all_responses_stops_at_cap_and_sets_cap_reached(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 1}, {"res_no": 2}],
        [{"res_no": 3}, {"res_no": 4}],
    ]
    result, interrupted, cap_reached = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert len(result) == 3
    assert result == [{"res_no": 1}, {"res_no": 2}, {"res_no": 3}]
    assert interrupted is False
    assert cap_reached is True
    assert mock_fetch.call_count == 2
    mock_sleep.assert_called_once_with(5.0)


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_uses_configured_sleep_delay(
    mock_fetch,
    mock_parse,
    mock_sleep,
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [[{"res_no": 1}], []]

    with patch.dict(
        os.environ,
        {"SCRAPE_PAGE_DELAY_SECONDS": "2.5"},
        clear=True,
    ):
        result, interrupted, cap_reached = collect_all_responses(
            "https://dic.nicovideo.jp/b/a/12345/"
        )

    assert result == [{"res_no": 1}]
    assert interrupted is False
    assert cap_reached is False
    mock_sleep.assert_called_once_with(2.5)


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
            with patch("orchestrator.get_max_saved_res_no", return_value=None):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=([{"res_no": 1}], False, False),
                ) as mock_collect:
                    conn = MagicMock()
                    with patch(
                        "orchestrator.init_db",
                        return_value=conn,
                    ) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch("orchestrator.print") as mock_print:
                                ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/12345/",
        response_cap=None,
        progress_reporter=None,
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
    assert ok


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
            with patch("orchestrator.init_db") as mock_init:
                with patch("orchestrator.save_to_db") as mock_save_db:
                    with patch("orchestrator.print") as mock_print:
                        ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_collect.assert_not_called()
    mock_init.assert_not_called()
    mock_save_db.assert_not_called()
    mock_print.assert_any_call(f"Article not found: {article_url}")
    assert not ok
    assert ok.outcome == "fail_article_not_found"


def test_run_scrape_redirect_article_handoffs_without_archive_migration():
    article_url = "https://dic.nicovideo.jp/a/old-article"

    with patch(
        "orchestrator.fetch_article_metadata",
        side_effect=RedirectArticleError(
            article_url,
            "https://dic.nicovideo.jp/a/new-article",
            "旧記事",
        ),
    ) as mock_meta:
        with patch("orchestrator.collect_all_responses") as mock_collect:
            with patch("orchestrator.init_db") as mock_init:
                with patch("orchestrator.save_to_db") as mock_save_db:
                    with patch("orchestrator.print") as mock_print:
                        ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_collect.assert_not_called()
    mock_init.assert_not_called()
    mock_save_db.assert_not_called()
    mock_print.assert_any_call(
        "Redirect detected: https://dic.nicovideo.jp/a/old-article -> "
        "https://dic.nicovideo.jp/a/new-article"
    )
    assert ok
    assert ok.outcome == "redirect_handoff"
    assert ok.redirect_target_url == "https://dic.nicovideo.jp/a/new-article"


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
            with patch("orchestrator.get_max_saved_res_no", return_value=None):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=([], False, False),
                ) as mock_collect:
                    conn = MagicMock()
                    with patch(
                        "orchestrator.init_db",
                        return_value=conn,
                    ) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch("orchestrator.print") as mock_print:
                                ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/12345/",
        response_cap=None,
        progress_reporter=None,
    )
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
    assert ok


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
            with patch("orchestrator.get_max_saved_res_no", return_value=None):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=(partial, True, False),
                ) as mock_collect:
                    conn = MagicMock()
                    with patch(
                        "orchestrator.init_db",
                        return_value=conn,
                    ) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch("orchestrator.print") as mock_print:
                                ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/12345/",
        response_cap=None,
        progress_reporter=None,
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
    assert ok


def test_run_scrape_denylist_skips_collection_and_save():
    article_url = "https://dic.nicovideo.jp/a/480340"

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("480340", "a", "Title"),
    ) as mock_meta:
        with patch("orchestrator.build_bbs_base_url") as mock_build:
            with patch("orchestrator.collect_all_responses") as mock_collect:
                with patch("orchestrator.init_db") as mock_init:
                    with patch("orchestrator.save_to_db") as mock_save_db:
                        with patch("orchestrator.print") as mock_print:
                            ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_not_called()
    mock_collect.assert_not_called()
    mock_init.assert_not_called()
    mock_save_db.assert_not_called()
    mock_print.assert_any_call("Skipping article (high-volume).")
    assert not ok
    assert ok.outcome == "skip_denylist"


def test_run_scrape_cap_reached_saves_partial_and_logs():
    article_url = "https://dic.nicovideo.jp/a/12345"
    partial = [{"res_no": i} for i in range(1, 4)]

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ) as mock_meta:
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ) as mock_build:
            with patch("orchestrator.get_max_saved_res_no", return_value=None):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=(partial, False, True),
                ) as mock_collect:
                    conn = MagicMock()
                    with patch(
                        "orchestrator.init_db",
                        return_value=conn,
                    ) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch("orchestrator.print") as mock_print:
                                ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/12345/",
        response_cap=None,
        progress_reporter=None,
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
    joined_calls = " ".join(
        " ".join(map(str, c.args)) for c in mock_print.call_args_list
    )
    assert "Response cap reached; saving partial responses" in joined_calls
    assert "3 items" in joined_calls
    assert ok


@pytest.mark.parametrize(
    ("scenario_name", "collected", "expected_message", "expected_responses"),
    [
        (
            "normal_save_path",
            ([{"res_no": 1}], False, False),
            None,
            [{"res_no": 1}],
        ),
        (
            "empty_result",
            ([], False, False),
            "No BBS responses found; saving empty result",
            [],
        ),
        (
            "later_page_interruption",
            ([{"res_no": 1}], True, False),
            "BBS fetch interrupted; saving partial responses",
            [{"res_no": 1}],
        ),
        (
            "cap_reached",
            ([{"res_no": 1}, {"res_no": 2}, {"res_no": 3}], False, True),
            "Response cap reached; saving partial responses",
            [{"res_no": 1}, {"res_no": 2}, {"res_no": 3}],
        ),
    ],
)
def test_run_scrape_representative_save_path_regression(
    scenario_name, collected, expected_message, expected_responses
):
    article_url = "https://dic.nicovideo.jp/a/12345"
    scenario_messages = [
        "No BBS responses found; saving empty result",
        "BBS fetch interrupted; saving partial responses",
        "Response cap reached; saving partial responses",
    ]

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ) as mock_meta:
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ) as mock_build:
            with patch("orchestrator.get_max_saved_res_no", return_value=None):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=collected,
                ) as mock_collect:
                    conn = MagicMock()
                    with patch(
                        "orchestrator.init_db",
                        return_value=conn,
                    ) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch("orchestrator.print") as mock_print:
                                ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/12345/",
        response_cap=None,
        progress_reporter=None,
    )
    mock_init.assert_called_once_with()
    mock_save_db.assert_called_once_with(
        conn,
        "12345",
        "a",
        "Title",
        article_url,
        expected_responses,
    )
    conn.close.assert_called_once_with()
    mock_print.assert_any_call("Saved to SQLite")
    assert ok

    if expected_message is None:
        joined_calls = " ".join(
            " ".join(map(str, c.args)) for c in mock_print.call_args_list
        )
        assert not any(message in joined_calls for message in scenario_messages)
    else:
        joined_calls = " ".join(
            " ".join(map(str, c.args)) for c in mock_print.call_args_list
        )
        assert expected_message in joined_calls


@pytest.mark.parametrize(
    ("scenario_name", "metadata_side_effect", "metadata_value", "expected_message"),
    [
        (
            "article_not_found",
            ArticleNotFoundError("Article not found: https://dic.nicovideo.jp/a/999"),
            None,
            "Article not found: https://dic.nicovideo.jp/a/999",
        ),
        (
            "known_high_volume_skip",
            None,
            ("480340", "a", "Title"),
            "Skipping article (high-volume).",
        ),
    ],
)
def test_run_scrape_representative_skip_path_regression(
    scenario_name, metadata_side_effect, metadata_value, expected_message
):
    article_url = (
        "https://dic.nicovideo.jp/a/999"
        if scenario_name == "article_not_found"
        else "https://dic.nicovideo.jp/a/480340"
    )

    fetch_kwargs = {"side_effect": metadata_side_effect}
    if metadata_side_effect is None:
        fetch_kwargs = {"return_value": metadata_value}

    with patch("orchestrator.fetch_article_metadata", **fetch_kwargs) as mock_meta:
        with patch("orchestrator.build_bbs_base_url") as mock_build:
            with patch("orchestrator.collect_all_responses") as mock_collect:
                with patch("orchestrator.init_db") as mock_init:
                    with patch("orchestrator.save_to_db") as mock_save_db:
                        with patch("orchestrator.print") as mock_print:
                            ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_not_called()
    mock_collect.assert_not_called()
    mock_init.assert_not_called()
    mock_save_db.assert_not_called()
    mock_print.assert_any_call(expected_message)
    assert not ok
    if scenario_name == "article_not_found":
        assert ok.outcome == "fail_article_not_found"
    else:
        assert ok.outcome == "skip_denylist"


def test_get_max_saved_res_no_returns_none_when_article_has_no_saved_responses():
    with patch("orchestrator.init_db") as mock_init:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = (None,)
        conn.cursor.return_value = cur
        mock_init.return_value = conn

        result = get_max_saved_res_no("12345", "a")

    assert result is None
    cur.execute.assert_called_once()
    conn.close.assert_called_once_with()


def test_load_saved_responses_returns_response_dicts_in_res_no_order():
    with patch("orchestrator.init_db") as mock_init:
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = [
            (1, "abc123", "Alice", "2025-01-01 00:00", "Hello", "<p>Hello</p>"),
            (2, None, None, None, "World", None),
        ]
        conn.cursor.return_value = cur
        mock_init.return_value = conn

        result = load_saved_responses("12345", "a")

    assert result == [
        {
            "res_no": 1,
            "id_hash": "abc123",
            "poster_name": "Alice",
            "posted_at": "2025-01-01 00:00",
            "content": "Hello",
            "content_html": "<p>Hello</p>",
        },
        {
            "res_no": 2,
            "id_hash": None,
            "poster_name": None,
            "posted_at": None,
            "content": "World",
            "content_html": None,
        },
    ]
    conn.close.assert_called_once_with()


def test_run_scrape_saved_article_resumes_and_saves_only_new_items():
    article_url = "https://dic.nicovideo.jp/a/12345"
    saved_responses = [
        {
            "res_no": 61,
            "id_hash": "old61",
            "poster_name": "Alice",
            "posted_at": "2025-01-01 00:00",
            "content": "Old 61",
            "content_html": "<p>Old 61</p>",
        },
        {
            "res_no": 65,
            "id_hash": "old65",
            "poster_name": "Bob",
            "posted_at": "2025-01-01 00:01",
            "content": "Old 65",
            "content_html": "<p>Old 65</p>",
        },
    ]
    new_responses = [
        {
            "res_no": 66,
            "id_hash": "new66",
            "poster_name": "Carol",
            "posted_at": "2025-01-01 00:02",
            "content": "New 66",
            "content_html": "<p>New 66</p>",
        }
    ]

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ) as mock_meta:
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ) as mock_build:
            with patch("orchestrator.get_max_saved_res_no", return_value=65):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=(new_responses, False, False),
                ) as mock_collect:
                    with patch(
                        "orchestrator.load_saved_responses",
                        return_value=saved_responses,
                    ) as mock_saved:
                        conn = MagicMock()
                        with patch(
                            "orchestrator.init_db",
                            return_value=conn,
                        ) as mock_init:
                            with patch(
                                "orchestrator.save_to_db",
                            ) as mock_save_db:
                                with patch(
                                    "orchestrator.print",
                                ) as mock_print:
                                    ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/12345/",
        start=61,
        max_saved_res_no=65,
        response_cap=None,
        progress_reporter=None,
    )
    mock_saved.assert_called_once_with("12345", "a")
    mock_init.assert_called_once_with()
    mock_save_db.assert_called_once_with(
        conn,
        "12345",
        "a",
        "Title",
        article_url,
        new_responses,
    )
    conn.close.assert_called_once_with()
    mock_print.assert_any_call(
        "Saved article detected; resuming from max_saved_res_no=65"
    )
    assert ok


def test_run_scrape_saved_article_zero_new_is_success_without_writing():
    article_url = "https://dic.nicovideo.jp/a/12345"

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ) as mock_meta:
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ) as mock_build:
            with patch("orchestrator.get_max_saved_res_no", return_value=65):
                with patch(
                    "orchestrator.collect_all_responses",
                    return_value=([], False, False),
                ) as mock_collect:
                    with patch(
                        "orchestrator.load_saved_responses",
                    ) as mock_saved:
                        with patch("orchestrator.init_db") as mock_init:
                            with patch(
                                "orchestrator.save_to_db",
                            ) as mock_save_db:
                                with patch(
                                    "orchestrator.print",
                                ) as mock_print:
                                    ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/12345/",
        start=61,
        max_saved_res_no=65,
        response_cap=None,
        progress_reporter=None,
    )
    mock_saved.assert_not_called()
    mock_init.assert_not_called()
    mock_save_db.assert_not_called()
    mock_print.assert_any_call(
        "No new BBS responses found; article already up to date"
    )
    assert ok


def test_drain_queue_requests_dequeues_when_run_scrape_is_success():
    queued = [
        {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
            "title": "Title",
            "enqueued_at": "2026-01-01 00:00:00",
        }
    ]
    conn = MagicMock()

    with patch("orchestrator.init_db", return_value=conn) as mock_init:
        with patch("orchestrator.list_queue_requests", return_value=queued):
            with patch("orchestrator.run_scrape", return_value=True) as mock_run:
                with patch(
                    "orchestrator.dequeue_canonical_target",
                    return_value=True,
                ) as mock_dequeue:
                    result = drain_queue_requests()

    mock_init.assert_called_once_with()
    mock_run.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        response_cap=QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP,
    )
    mock_dequeue.assert_called_once_with(conn, "12345", "a")
    conn.close.assert_called_once_with()
    assert result == {
        "processed": 1,
        "dequeued": 1,
        "remaining": 0,
        "errors": 0,
    }


def test_drain_queue_requests_cap_reached_path_is_success_and_dequeues():
    queued = [
        {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
            "title": "Title",
            "enqueued_at": "2026-01-01 00:00:00",
        }
    ]
    conn = MagicMock()

    with patch("orchestrator.init_db", return_value=conn):
        with patch("orchestrator.list_queue_requests", return_value=queued):
            with patch("orchestrator.run_scrape", return_value=True):
                with patch(
                    "orchestrator.dequeue_canonical_target",
                    return_value=True,
                ) as mock_dequeue:
                    result = drain_queue_requests()

    mock_dequeue.assert_called_once_with(conn, "12345", "a")
    assert result["dequeued"] == 1
    assert result["errors"] == 0


def test_drain_queue_requests_unexpected_failure_keeps_request_queued():
    queued = [
        {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
            "title": "Title",
            "enqueued_at": "2026-01-01 00:00:00",
        }
    ]
    conn = MagicMock()

    with patch("orchestrator.init_db", return_value=conn):
        with patch("orchestrator.list_queue_requests", return_value=queued):
            with patch(
                "orchestrator.run_scrape",
                side_effect=RuntimeError("boom"),
            ) as mock_run:
                with patch(
                    "orchestrator.dequeue_canonical_target",
                ) as mock_dequeue:
                    result = drain_queue_requests()

    mock_run.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        response_cap=QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP,
    )
    mock_dequeue.assert_not_called()
    assert result == {
        "processed": 1,
        "dequeued": 0,
        "remaining": 1,
        "errors": 1,
    }
