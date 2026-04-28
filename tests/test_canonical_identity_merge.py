import sqlite3

from canonical_identity_merge import merge_canonical_a_identity_groups
from storage import init_db, register_target, save_to_db


def _response(res_no, text):
    return {
        "res_no": res_no,
        "id_hash": f"id-{res_no}",
        "poster_name": f"user-{res_no}",
        "posted_at": f"2025-01-01 00:0{res_no}",
        "content": text,
        "content_html": f"<p>{text}</p>",
    }


def _open_counts(db_path):
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        article_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM responses")
        response_count = cur.fetchone()[0]
        return article_count, response_count
    finally:
        conn.close()


def test_merge_canonical_identities_dry_run_keeps_db_unchanged(tmp_path):
    db_path = tmp_path / "merge.db"
    conn = init_db(str(db_path))
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Article Title",
            "https://dic.nicovideo.jp/a/slug-title",
            [_response(1, "old-1"), _response(2, "old-2")],
        )
        save_to_db(
            conn,
            "slug-title",
            "a",
            "Article Title",
            "https://dic.nicovideo.jp/a/slug-title",
            [_response(2, "new-2"), _response(3, "new-3")],
        )
    finally:
        conn.close()

    before_counts = _open_counts(str(db_path))

    result = merge_canonical_a_identity_groups(str(db_path))

    assert result["dry_run"] is True
    assert result["group_count"] == 1
    assert result["copied_response_count"] == 1
    assert result["skipped_existing_response_count"] == 1
    assert result["groups"][0]["keep_identity"] == {
        "article_id": "slug-title",
        "article_type": "a",
    }
    assert result["groups"][0]["source_identities"] == [
        {"article_id": "12345", "article_type": "a"}
    ]
    assert _open_counts(str(db_path)) == before_counts


def test_merge_canonical_identities_apply_copies_missing_and_cleans_old_rows(
    tmp_path,
):
    db_path = tmp_path / "merge.db"
    conn = init_db(str(db_path))
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Article Title",
            "https://dic.nicovideo.jp/a/slug-title",
            [_response(1, "old-1"), _response(2, "old-2")],
        )
        save_to_db(
            conn,
            "slug-title",
            "a",
            "Article Title",
            "https://dic.nicovideo.jp/a/slug-title",
            [_response(2, "new-2"), _response(3, "new-3")],
        )
    finally:
        conn.close()

    result = merge_canonical_a_identity_groups(str(db_path), apply=True)

    assert result["dry_run"] is False
    assert result["copied_response_count"] == 1
    assert result["cleaned_article_count"] == 1
    assert result["cleaned_response_count"] == 2
    assert result["groups"][0]["cleanup_performed"] is True

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT article_id FROM articles ORDER BY article_id ASC"
        )
        assert cur.fetchall() == [("slug-title",)]

        cur.execute(
            """
            SELECT article_id, res_no, content_text
            FROM responses
            ORDER BY article_id ASC, res_no ASC
            """
        )
        assert cur.fetchall() == [
            ("slug-title", 1, "old-1"),
            ("slug-title", 2, "new-2"),
            ("slug-title", 3, "new-3"),
        ]
    finally:
        conn.close()


def test_merge_canonical_identities_apply_rekeys_and_dedupes_target_rows(
    tmp_path,
):
    db_path = tmp_path / "merge.db"
    conn = init_db(str(db_path))
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Article Title",
            "https://dic.nicovideo.jp/a/slug-title",
            [_response(1, "old-1")],
        )
        save_to_db(
            conn,
            "slug-title",
            "a",
            "Article Title",
            "https://dic.nicovideo.jp/a/slug-title",
            [_response(2, "new-2")],
        )
        register_target(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/slug-title",
        )
    finally:
        conn.close()

    result = merge_canonical_a_identity_groups(str(db_path), apply=True)

    assert result["target_rekey_count"] == 1
    assert result["target_deleted_count"] == 0

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT article_id, is_active FROM target ORDER BY article_id ASC"
        )
        assert cur.fetchall() == [("slug-title", 1)]
    finally:
        conn.close()


def test_merge_canonical_identities_apply_deletes_duplicate_target_rows(tmp_path):
    db_path = tmp_path / "merge.db"
    conn = init_db(str(db_path))
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Article Title",
            "https://dic.nicovideo.jp/a/slug-title",
            [_response(1, "old-1")],
        )
        save_to_db(
            conn,
            "slug-title",
            "a",
            "Article Title",
            "https://dic.nicovideo.jp/a/slug-title",
            [_response(2, "new-2")],
        )
        register_target(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/slug-title",
        )
        register_target(
            conn,
            "slug-title",
            "a",
            "https://dic.nicovideo.jp/a/slug-title",
        )
    finally:
        conn.close()

    result = merge_canonical_a_identity_groups(str(db_path), apply=True)

    assert result["target_rekey_count"] == 0
    assert result["target_deleted_count"] == 1

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT article_id FROM target ORDER BY article_id ASC")
        assert cur.fetchall() == [("slug-title",)]
    finally:
        conn.close()
