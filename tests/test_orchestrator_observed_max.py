"""Scrape-flow persistence of the observed board max response number.

These tests exercise the real storage helper against a temp DB (no mocked
target write), unlike test_orchestrator.py which mocks that collaborator.
"""

import sqlite3
from unittest.mock import patch

from orchestrator import ArticleMetadataResult, run_scrape
from storage import (
    DEFAULT_DB_PATH,
    init_db,
    register_target,
    update_target_observed_max_res_no,
)


def _read_target_observed_max(article_id, article_type="a"):
    conn = sqlite3.connect(DEFAULT_DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT observed_max_res_no, observed_max_res_no_source
            FROM target
            WHERE article_id=? AND article_type=?
            """,
            (article_id, article_type),
        )
        return cur.fetchone()
    finally:
        conn.close()


def test_run_scrape_persists_observed_max_to_target(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    register_target(
        conn, "5364158", "a", "https://dic.nicovideo.jp/a/5364158", title="T",
    )
    conn.close()

    canonical_url = "https://dic.nicovideo.jp/a/5364158"
    responses = [{"res_no": 1}, {"res_no": 250}, {"res_no": 37}]

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=ArticleMetadataResult(
            "5364158", "a", "Title", article_url=canonical_url,
        ),
    ):
        with patch("orchestrator.get_max_saved_res_no", return_value=None):
            with patch(
                "orchestrator.collect_all_responses",
                return_value=(responses, False, False),
            ):
                with patch("orchestrator.print"):
                    run_scrape(canonical_url)

    row = _read_target_observed_max("5364158")
    assert row[0] == 250
    assert row[1] == "bbs_page_scrape"


def test_run_scrape_does_not_lower_existing_observed_max(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    register_target(
        conn, "5364158", "a", "https://dic.nicovideo.jp/a/5364158", title="T",
    )
    update_target_observed_max_res_no(
        conn, "5364158", "a", 999, source="article_top_preview",
    )
    conn.close()

    canonical_url = "https://dic.nicovideo.jp/a/5364158"
    responses = [{"res_no": 1}, {"res_no": 10}]

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=ArticleMetadataResult(
            "5364158", "a", "Title", article_url=canonical_url,
        ),
    ):
        with patch("orchestrator.get_max_saved_res_no", return_value=None):
            with patch(
                "orchestrator.collect_all_responses",
                return_value=(responses, False, False),
            ):
                with patch("orchestrator.print"):
                    run_scrape(canonical_url)

    row = _read_target_observed_max("5364158")
    assert row[0] == 999
    assert row[1] == "article_top_preview"


def test_run_scrape_already_up_to_date_persists_saved_rows_fallback(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    register_target(
        conn, "5364158", "a", "https://dic.nicovideo.jp/a/5364158", title="T",
    )
    conn.close()

    canonical_url = "https://dic.nicovideo.jp/a/5364158"

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=ArticleMetadataResult(
            "5364158", "a", "Title", article_url=canonical_url,
        ),
    ):
        with patch("orchestrator.get_max_saved_res_no", return_value=120):
            with patch(
                "orchestrator.collect_all_responses",
                return_value=([], False, False),
            ):
                with patch("orchestrator.print"):
                    run_scrape(canonical_url)

    row = _read_target_observed_max("5364158")
    assert row[0] == 120
    assert row[1] == "saved_rows_fallback"


def test_run_scrape_already_up_to_date_does_not_downgrade_observed(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    register_target(
        conn, "5364158", "a", "https://dic.nicovideo.jp/a/5364158", title="T",
    )
    update_target_observed_max_res_no(
        conn, "5364158", "a", 999, source="article_top_preview",
    )
    conn.close()

    canonical_url = "https://dic.nicovideo.jp/a/5364158"

    with patch(
        "orchestrator.fetch_article_metadata",
        return_value=ArticleMetadataResult(
            "5364158", "a", "Title", article_url=canonical_url,
        ),
    ):
        with patch("orchestrator.get_max_saved_res_no", return_value=120):
            with patch(
                "orchestrator.collect_all_responses",
                return_value=([], False, False),
            ):
                with patch("orchestrator.print"):
                    run_scrape(canonical_url)

    row = _read_target_observed_max("5364158")
    assert row[0] == 999
    assert row[1] == "article_top_preview"
