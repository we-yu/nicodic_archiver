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
    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert result == [{"res_no": 1}, {"res_no": 2}]
    assert interrupted is False
    assert cap_reached is False
    assert empty_note is None
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
    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert result == []
    assert interrupted is False
    assert cap_reached is False
    assert empty_note is None
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
    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert result == []
    assert interrupted is False
    assert cap_reached is False
    assert empty_note is None
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

    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )

    assert result == [{"res_no": 1}]
    assert interrupted is True
    assert cap_reached is False
    assert empty_note is None
    assert mock_fetch.call_count == 2
    mock_sleep.assert_called_once_with(1)


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
    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/"
    )
    assert len(result) == 3
    assert result == [{"res_no": 1}, {"res_no": 2}, {"res_no": 3}]
    assert interrupted is False
    assert cap_reached is True
    assert empty_note is None
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
                return_value=([{"res_no": 1}], False, False, None),
            ) as mock_collect:
                with patch("orchestrator.save_json") as mock_save_json:
                    conn = MagicMock()
                    with patch("orchestrator.init_db", return_value=conn) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch(
                                "orchestrator.get_max_saved_res_no",
                                return_value=None,
                            ):
                                with patch("orchestrator.print") as mock_print:
                                    ok = run_scrape(article_url)

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
    assert ok is True


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
                            ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_collect.assert_not_called()
    mock_save_json.assert_not_called()
    mock_init.assert_not_called()
    mock_save_db.assert_not_called()
    mock_print.assert_any_call(f"Article not found: {article_url}")
    assert ok is False


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
                return_value=([], False, False, None),
            ) as mock_collect:
                with patch("orchestrator.save_json") as mock_save_json:
                    conn = MagicMock()
                    with patch("orchestrator.init_db", return_value=conn) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch(
                                "orchestrator.get_max_saved_res_no",
                                return_value=None,
                            ):
                                with patch("orchestrator.print") as mock_print:
                                    ok = run_scrape(article_url)

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
    assert ok is True


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
                return_value=(partial, True, False, None),
            ) as mock_collect:
                with patch("orchestrator.save_json") as mock_save_json:
                    conn = MagicMock()
                    with patch("orchestrator.init_db", return_value=conn) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch(
                                "orchestrator.get_max_saved_res_no",
                                return_value=None,
                            ):
                                with patch("orchestrator.print") as mock_print:
                                    ok = run_scrape(article_url)

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
    assert ok is True


def test_run_scrape_denylist_skips_collection_and_save():
    article_url = "https://dic.nicovideo.jp/a/480340"

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("480340", "a", "Title"),
    ) as mock_meta:
        with patch("orchestrator.build_bbs_base_url") as mock_build:
            with patch("orchestrator.collect_all_responses") as mock_collect:
                with patch("orchestrator.save_json") as mock_save_json:
                    with patch("orchestrator.init_db") as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch("orchestrator.print") as mock_print:
                                ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_not_called()
    mock_collect.assert_not_called()
    mock_save_json.assert_not_called()
    mock_init.assert_not_called()
    mock_save_db.assert_not_called()
    mock_print.assert_any_call("Skipping article (high-volume).")
    assert ok is False


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
            with patch(
                "orchestrator.collect_all_responses",
                return_value=(partial, False, True, None),
            ) as mock_collect:
                with patch("orchestrator.save_json") as mock_save_json:
                    conn = MagicMock()
                    with patch("orchestrator.init_db", return_value=conn) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch(
                                "orchestrator.get_max_saved_res_no",
                                return_value=None,
                            ):
                                with patch("orchestrator.print") as mock_print:
                                    ok = run_scrape(article_url)

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
    joined_calls = " ".join(
        " ".join(map(str, c.args)) for c in mock_print.call_args_list
    )
    assert "Response cap reached; saving partial responses" in joined_calls
    assert "3 items" in joined_calls
    assert ok is True


@pytest.mark.parametrize(
    ("scenario_name", "collected", "expected_message", "expected_responses"),
    [
        (
            "normal_save_path",
            ([{"res_no": 1}], False, False, None),
            None,
            [{"res_no": 1}],
        ),
        (
            "empty_result",
            ([], False, False, None),
            "No BBS responses found; saving empty result",
            [],
        ),
        (
            "later_page_interruption",
            ([{"res_no": 1}], True, False, None),
            "BBS fetch interrupted; saving partial responses",
            [{"res_no": 1}],
        ),
        (
            "cap_reached",
            (
                [{"res_no": 1}, {"res_no": 2}, {"res_no": 3}],
                False,
                True,
                None,
            ),
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
            with patch(
                "orchestrator.collect_all_responses",
                return_value=collected,
            ) as mock_collect:
                with patch("orchestrator.save_json") as mock_save_json:
                    conn = MagicMock()
                    with patch("orchestrator.init_db", return_value=conn) as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch(
                                "orchestrator.get_max_saved_res_no",
                                return_value=None,
                            ):
                                with patch("orchestrator.print") as mock_print:
                                    ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_called_once_with(article_url)
    mock_collect.assert_called_once_with("https://dic.nicovideo.jp/b/a/12345/")
    mock_save_json.assert_called_once_with(
        "12345",
        "a",
        "Title",
        article_url,
        expected_responses,
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
    assert ok is True

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
                with patch("orchestrator.save_json") as mock_save_json:
                    with patch("orchestrator.init_db") as mock_init:
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch("orchestrator.print") as mock_print:
                                ok = run_scrape(article_url)

    mock_meta.assert_called_once_with(article_url)
    mock_build.assert_not_called()
    mock_collect.assert_not_called()
    mock_save_json.assert_not_called()
    mock_init.assert_not_called()
    mock_save_db.assert_not_called()
    mock_print.assert_any_call(expected_message)
    assert ok is False


# ----- collect_all_responses: incremental (saved article) -----


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_incremental_skips_pages_until_anchor_page(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 1}, {"res_no": 2}],
        [{"res_no": 3}, {"res_no": 4}],
        [{"res_no": 5}, {"res_no": 6}],
        [],
    ]
    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/",
        max_saved_res_no=5,
    )
    assert result == [{"res_no": 6}]
    assert interrupted is False
    assert cap_reached is False
    assert empty_note is None
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/1-")
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/3-")
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/5-")
    mock_fetch.assert_any_call("https://dic.nicovideo.jp/b/a/12345/7-")


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_incremental_filters_resume_page_and_fetches_following_pages(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 1}, {"res_no": 2}, {"res_no": 3}],
        [{"res_no": 4}, {"res_no": 5}],
    ]
    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/",
        max_saved_res_no=2,
    )
    assert result == [{"res_no": 3}, {"res_no": 4}, {"res_no": 5}]
    assert empty_note is None


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_incremental_resume_page_zero_new_still_fetches_later_pages(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 1}, {"res_no": 2}],
        [{"res_no": 3}],
        [],
    ]
    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/",
        max_saved_res_no=2,
    )
    assert result == [{"res_no": 3}]
    assert empty_note is None


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_incremental_thread_ends_before_anchor_returns_empty_note(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 1}, {"res_no": 2}],
        [],
    ]
    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/",
        max_saved_res_no=10,
    )
    assert result == []
    assert interrupted is False
    assert cap_reached is False
    assert empty_note == "no_new"


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_incremental_first_eligible_page_all_new_when_min_gt_saved(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 1}, {"res_no": 2}],
        [{"res_no": 3}, {"res_no": 4}],
        [],
    ]
    result, interrupted, cap_reached, empty_note = collect_all_responses(
        "https://dic.nicovideo.jp/b/a/12345/",
        max_saved_res_no=2,
    )
    assert result == [{"res_no": 3}, {"res_no": 4}]
    assert empty_note is None


def test_run_scrape_saved_passes_max_saved_and_merges_json():
    article_url = "https://dic.nicovideo.jp/a/12345"
    merged = [{"res_no": 1}, {"res_no": 2}]
    new_only = [{"res_no": 2}]

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ):
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ):
            with patch(
                "orchestrator.collect_all_responses",
                return_value=(new_only, False, False, None),
            ) as mock_collect:
                with patch("orchestrator.save_json") as mock_save_json:
                    conn = MagicMock()
                    with patch("orchestrator.init_db", return_value=conn):
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch(
                                "orchestrator.get_max_saved_res_no",
                                return_value=1,
                            ):
                                with patch(
                                    "orchestrator.fetch_responses_as_save_format",
                                    return_value=merged,
                                ):
                                    ok = run_scrape(article_url)

    mock_collect.assert_called_once_with(
        "https://dic.nicovideo.jp/b/a/12345/",
        max_saved_res_no=1,
    )
    mock_save_json.assert_called_once_with(
        "12345",
        "a",
        "Title",
        article_url,
        merged,
    )
    mock_save_db.assert_called_once_with(
        conn,
        "12345",
        "a",
        "Title",
        article_url,
        new_only,
    )
    assert ok is True


def test_run_scrape_saved_zero_new_prints_success_and_writes_merged_json():
    article_url = "https://dic.nicovideo.jp/a/12345"
    merged = [{"res_no": 1}]

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Title"),
    ):
        with patch(
            "orchestrator.build_bbs_base_url",
            return_value="https://dic.nicovideo.jp/b/a/12345/",
        ):
            with patch(
                "orchestrator.collect_all_responses",
                return_value=([], False, False, "no_new"),
            ):
                with patch("orchestrator.save_json") as mock_save_json:
                    conn = MagicMock()
                    with patch("orchestrator.init_db", return_value=conn):
                        with patch("orchestrator.save_to_db") as mock_save_db:
                            with patch(
                                "orchestrator.get_max_saved_res_no",
                                return_value=1,
                            ):
                                with patch(
                                    "orchestrator.fetch_responses_as_save_format",
                                    return_value=merged,
                                ):
                                    with patch("orchestrator.print") as mock_print:
                                        ok = run_scrape(article_url)

    mock_save_json.assert_called_once_with(
        "12345",
        "a",
        "Title",
        article_url,
        merged,
    )
    mock_save_db.assert_called_once_with(
        conn,
        "12345",
        "a",
        "Title",
        article_url,
        [],
    )
    mock_print.assert_any_call("No new responses since last save.")
    assert ok is True
