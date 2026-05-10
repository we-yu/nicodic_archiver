"""TASK044-focused tests: numeric target registry identity and denylist."""

from unittest.mock import patch

from archive_read import query_registered_articles
from collection_policy import find_denylisted_article_id
from storage import init_db, register_target
from target_list import register_target_url


def test_denylist_blocks_plain_id_url_237789():
    assert (
        find_denylisted_article_id(
            article_url="https://dic.nicovideo.jp/id/237789",
        )
        == "237789"
    )


def test_denylist_blocks_numeric_article_id_237789():
    assert (
        find_denylisted_article_id(
            article_id="237789",
            article_url="https://dic.nicovideo.jp/a/4294967295",
        )
        == "237789"
    )


def test_registered_articles_default_sort_tiebreaks_on_target_row_id(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(conn, "1", "a", "https://dic.nicovideo.jp/a/q", title="Q")
        register_target(conn, "2", "a", "https://dic.nicovideo.jp/a/r", title="R")
        cur = conn.cursor()
        same_ts = "2026-01-01T00:00:00+00:00"
        cur.execute(
            "UPDATE target SET created_at=? WHERE article_id=?",
            (same_ts, "1"),
        )
        cur.execute(
            "UPDATE target SET created_at=? WHERE article_id=?",
            (same_ts, "2"),
        )
        conn.commit()
    finally:
        conn.close()

    result = query_registered_articles()
    assert [row["article_id"] for row in result["rows"]] == ["2", "1"]


@patch("target_list.resolve_article_input")
def test_register_target_url_observes_post_resolve_denylist(mock_res, tmp_path):
    mock_res.return_value = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/unique-test-article",
            "article_id": "237789",
            "article_type": "a",
        },
        "title": "X",
        "matched_by": "article_url",
        "normalized_input": "u",
    }
    target_db = tmp_path / "t.db"
    out = register_target_url(
        "https://dic.nicovideo.jp/a/unique-test-article",
        str(target_db),
    )
    assert out == "denylisted"
