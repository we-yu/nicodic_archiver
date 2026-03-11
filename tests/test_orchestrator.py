"""Unit tests for orchestrator orchestration flow and pagination behavior."""
from unittest.mock import MagicMock, patch

from bs4 import BeautifulSoup
import pytest

from orchestrator import (
    build_bbs_base_url,
    collect_all_responses,
    fetch_article_metadata,
    run_scrape,
)


ARTICLE_URL = "https://dic.nicovideo.jp/a/12345"
BBS_BASE_URL = "https://dic.nicovideo.jp/b/a/12345/"


# ----- build_bbs_base_url -----


def test_build_bbs_base_url_normal():
    assert build_bbs_base_url(ARTICLE_URL) == BBS_BASE_URL


def test_build_bbs_base_url_with_trailing_slash():
    assert build_bbs_base_url(f"{ARTICLE_URL}/") == BBS_BASE_URL


# ----- fetch_article_metadata (mocked fetch) -----


def test_fetch_article_metadata_reads_title_id_and_type_from_page():
    html = """
    <html><head>
    <meta property="og:title" content="Fooとは">
    <meta property="og:url" content="https://dic.nicovideo.jp/a/12345">
    </head></html>
    """
    soup = BeautifulSoup(html, "lxml")
    with patch("orchestrator.fetch_page", return_value=soup):
        article_id, article_type, title = fetch_article_metadata(ARTICLE_URL)
    assert article_id == "12345"
    assert article_type == "a"
    assert title == "Foo"


def test_fetch_article_metadata_keeps_full_title_and_trims_og_url_slash():
    html = """
    <html><head>
    <meta property="og:title" content="Foo Bar Encyclopedia Entry">
    <meta property="og:url" content="https://dic.nicovideo.jp/a/12345/">
    </head></html>
    """
    soup = BeautifulSoup(html, "lxml")
    with patch("orchestrator.fetch_page", return_value=soup):
        article_id, article_type, title = fetch_article_metadata(ARTICLE_URL)

    assert article_id == "12345"
    assert article_type == "a"
    assert title == "Foo Bar Encyclopedia Entry"


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
def test_collect_all_responses_collects_multiple_pages_until_empty(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.return_value = MagicMock()
    mock_parse.side_effect = [
        [{"res_no": 1}, {"res_no": 2}],
        [{"res_no": 3}],
        [],
    ]

    result = collect_all_responses(BBS_BASE_URL)

    assert result == [{"res_no": 1}, {"res_no": 2}, {"res_no": 3}]
    assert mock_fetch.call_count == 3
    mock_fetch.assert_any_call(f"{BBS_BASE_URL}1-")
    mock_fetch.assert_any_call(f"{BBS_BASE_URL}3-")
    mock_fetch.assert_any_call(f"{BBS_BASE_URL}4-")
    assert mock_sleep.call_count == 2


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
    result = collect_all_responses(BBS_BASE_URL)
    assert result == [{"res_no": 1}, {"res_no": 2}]
    assert mock_fetch.call_count == 2
    mock_fetch.assert_any_call(f"{BBS_BASE_URL}1-")
    mock_fetch.assert_any_call(f"{BBS_BASE_URL}3-")
    mock_sleep.assert_called_once_with(1)


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_returns_partial_results_on_late_fetch_error(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.side_effect = [
        MagicMock(),
        RuntimeError("Failed to fetch (status=404)"),
    ]
    mock_parse.return_value = [{"res_no": 1}, {"res_no": 2}]

    result = collect_all_responses(BBS_BASE_URL)

    assert result == [{"res_no": 1}, {"res_no": 2}]
    assert mock_fetch.call_count == 2
    mock_parse.assert_called_once()
    mock_sleep.assert_called_once_with(1)


@patch("orchestrator.time.sleep")
@patch("orchestrator.parse_responses")
@patch("orchestrator.fetch_page")
def test_collect_all_responses_stops_on_fetch_error(
    mock_fetch, mock_parse, mock_sleep
):
    mock_fetch.side_effect = RuntimeError("Failed to fetch (status=404)")
    result = collect_all_responses(BBS_BASE_URL)
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
    result = collect_all_responses(BBS_BASE_URL)
    assert result == []
    mock_fetch.assert_called_once_with(f"{BBS_BASE_URL}1-")
    mock_sleep.assert_not_called()


# ----- run_scrape orchestration flow -----


def test_run_scrape_coordinates_metadata_collection_and_persistence(capsys):
    responses = [{"res_no": 1, "content": "hello", "content_html": "<p>hello</p>"}]
    conn = MagicMock()
    events = []

    def log_and_return(name, value):
        def _side_effect(*args, **kwargs):
            events.append((name, args, kwargs))
            return value

        return _side_effect

    with patch(
        "orchestrator.fetch_article_metadata",
        side_effect=log_and_return(
            "fetch_article_metadata", ("12345", "a", "Foo")
        ),
    ) as mock_metadata, patch(
        "orchestrator.build_bbs_base_url",
        side_effect=log_and_return("build_bbs_base_url", BBS_BASE_URL),
    ) as mock_build_bbs, patch(
        "orchestrator.collect_all_responses",
        side_effect=log_and_return("collect_all_responses", responses),
    ) as mock_collect, patch(
        "orchestrator.save_json",
        side_effect=log_and_return("save_json", None),
    ) as mock_save_json, patch(
        "orchestrator.init_db",
        side_effect=log_and_return("init_db", conn),
    ) as mock_init_db, patch(
        "orchestrator.save_to_db",
        side_effect=log_and_return("save_to_db", None),
    ) as mock_save_to_db:
        run_scrape(ARTICLE_URL)

    mock_metadata.assert_called_once_with(ARTICLE_URL)
    mock_build_bbs.assert_called_once_with(ARTICLE_URL)
    mock_collect.assert_called_once_with(BBS_BASE_URL)
    mock_save_json.assert_called_once_with(
        "12345", "a", "Foo", ARTICLE_URL, responses
    )
    mock_init_db.assert_called_once_with()
    mock_save_to_db.assert_called_once_with(
        conn, "12345", "a", "Foo", ARTICLE_URL, responses
    )
    conn.close.assert_called_once_with()

    assert [event[0] for event in events] == [
        "fetch_article_metadata",
        "build_bbs_base_url",
        "collect_all_responses",
        "save_json",
        "init_db",
        "save_to_db",
    ]
    assert "Saved to SQLite" in capsys.readouterr().out


def test_run_scrape_persists_empty_response_sets():
    conn = MagicMock()

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Foo"),
    ), patch(
        "orchestrator.build_bbs_base_url",
        return_value=BBS_BASE_URL,
    ), patch(
        "orchestrator.collect_all_responses",
        return_value=[],
    ), patch("orchestrator.save_json") as mock_save_json, patch(
        "orchestrator.init_db",
        return_value=conn,
    ), patch("orchestrator.save_to_db") as mock_save_to_db:
        run_scrape(ARTICLE_URL)

    mock_save_json.assert_called_once_with("12345", "a", "Foo", ARTICLE_URL, [])
    mock_save_to_db.assert_called_once_with(
        conn, "12345", "a", "Foo", ARTICLE_URL, []
    )
    conn.close.assert_called_once_with()


def test_run_scrape_aborts_before_db_work_when_json_save_fails():
    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=("12345", "a", "Foo"),
    ), patch(
        "orchestrator.build_bbs_base_url",
        return_value=BBS_BASE_URL,
    ), patch(
        "orchestrator.collect_all_responses",
        return_value=[{"res_no": 1}],
    ), patch(
        "orchestrator.save_json",
        side_effect=RuntimeError("disk full"),
    ), patch("orchestrator.init_db") as mock_init_db, patch(
        "orchestrator.save_to_db"
    ) as mock_save_to_db:
        with pytest.raises(RuntimeError, match="disk full"):
            run_scrape(ARTICLE_URL)

    mock_init_db.assert_not_called()
    mock_save_to_db.assert_not_called()
