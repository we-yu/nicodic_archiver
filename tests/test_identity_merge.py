"""Focused tests for identity_merge (canonical URL identity merge seam).

These tests use isolated SQLite DBs in tmp_path. They never touch the
runtime DB. The tests cover:
- detection of duplicate canonical_url groups for article_type='a'
- keep-identity selection (slug match preferred, never 'id' type)
- dry-run safety (no DB writes)
- response transfer with INSERT OR IGNORE de-duplication
- cleanup of old numeric source rows only after verification
- non-destructive target normalization (deactivation)
- explicit DB path requirement (no implicit runtime default)
"""
import sqlite3

import pytest

from identity_merge import (
    apply_canonical_url_merge,
    choose_keep_identity,
    find_canonical_url_duplicate_groups,
    merge_canonical_url_identities,
    plan_canonical_url_merge,
)
from storage import init_db, register_target


def _seed_articles_row(
    conn, article_id, article_type, title, canonical_url
):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO articles (article_id, article_type, title, canonical_url)
        VALUES (?, ?, ?, ?)
        """,
        (article_id, article_type, title, canonical_url),
    )


def _seed_response_row(
    conn, article_id, article_type, res_no, content_text
):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO responses
        (article_id, article_type, res_no, content_text)
        VALUES (?, ?, ?, ?)
        """,
        (article_id, article_type, res_no, content_text),
    )


def _build_dup_group_db(tmp_path):
    db_path = tmp_path / "merge.db"
    conn = init_db(str(db_path))
    canonical_url = "https://dic.nicovideo.jp/a/foo-slug"

    _seed_articles_row(conn, "12345", "a", "Foo (old)", canonical_url)
    _seed_articles_row(conn, "foo-slug", "a", "Foo", canonical_url)

    _seed_response_row(conn, "12345", "a", 1, "OLD-1")
    _seed_response_row(conn, "12345", "a", 2, "OLD-2-collide")
    _seed_response_row(conn, "12345", "a", 3, "OLD-3")

    _seed_response_row(conn, "foo-slug", "a", 2, "KEEP-2")

    register_target(conn, "12345", "a", canonical_url)
    register_target(conn, "foo-slug", "a", canonical_url)

    conn.commit()
    return conn, str(db_path), canonical_url


def test_find_groups_returns_only_a_type_dups(tmp_path):
    db_path = tmp_path / "groups.db"
    conn = init_db(str(db_path))
    try:
        canonical_url = "https://dic.nicovideo.jp/a/dup-slug"
        _seed_articles_row(conn, "1", "a", "A1", canonical_url)
        _seed_articles_row(conn, "dup-slug", "a", "A2", canonical_url)

        _seed_articles_row(
            conn,
            "999",
            "a",
            "Solo",
            "https://dic.nicovideo.jp/a/solo",
        )
        conn.commit()

        groups = find_canonical_url_duplicate_groups(conn)
    finally:
        conn.close()

    assert len(groups) == 1
    assert groups[0]["canonical_url"] == canonical_url
    article_ids = sorted(row["article_id"] for row in groups[0]["rows"])
    assert article_ids == ["1", "dup-slug"]


def test_find_groups_does_not_include_id_type_rows(tmp_path):
    db_path = tmp_path / "no_id.db"
    conn = init_db(str(db_path))
    try:
        canonical_url = "https://dic.nicovideo.jp/a/foo-slug"
        _seed_articles_row(conn, "12345", "a", "Foo", canonical_url)
        _seed_articles_row(
            conn,
            "5364158",
            "id",
            "Foo (id legacy)",
            canonical_url,
        )
        conn.commit()

        groups = find_canonical_url_duplicate_groups(conn)
    finally:
        conn.close()

    assert groups == []


def test_choose_keep_identity_prefers_slug_match(tmp_path):
    canonical_url = "https://dic.nicovideo.jp/a/foo-slug"
    group = {
        "canonical_url": canonical_url,
        "rows": [
            {
                "article_id": "12345",
                "article_type": "a",
                "canonical_url": canonical_url,
            },
            {
                "article_id": "foo-slug",
                "article_type": "a",
                "canonical_url": canonical_url,
            },
        ],
    }
    keep = choose_keep_identity(group)
    assert keep["article_id"] == "foo-slug"
    assert keep["article_type"] == "a"


def test_choose_keep_identity_returns_none_when_no_slug_row(tmp_path):
    canonical_url = "https://dic.nicovideo.jp/a/foo-slug"
    group = {
        "canonical_url": canonical_url,
        "rows": [
            {
                "article_id": "12345",
                "article_type": "a",
                "canonical_url": canonical_url,
            },
            {
                "article_id": "67890",
                "article_type": "a",
                "canonical_url": canonical_url,
            },
        ],
    }
    assert choose_keep_identity(group) is None


def test_dry_run_apply_returns_plan_without_writes(tmp_path):
    conn, db_path, canonical_url = _build_dup_group_db(tmp_path)
    try:
        plan = plan_canonical_url_merge(conn)
        summary = apply_canonical_url_merge(conn, dry_run=True)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        assert cur.fetchone()[0] == 2
        cur.execute(
            "SELECT COUNT(*) FROM responses WHERE article_id=?",
            ("12345",),
        )
        assert cur.fetchone()[0] == 3
        cur.execute(
            "SELECT COUNT(*) FROM responses WHERE article_id=?",
            ("foo-slug",),
        )
        assert cur.fetchone()[0] == 1

    finally:
        conn.close()

    assert summary["dry_run"] is True
    assert len(summary["groups"]) == 1
    group = summary["groups"][0]
    assert group["keep_identity"] == {
        "article_id": "foo-slug",
        "article_type": "a",
    }
    assert plan[0]["keep_identity"] == group["keep_identity"]
    assert len(group["sources"]) == 1
    src = group["sources"][0]
    assert src["article_id"] == "12345"
    assert src["transferred"] == 2
    assert src["verification"] == "dry_run"
    assert src["deleted_articles"] == 0
    assert src["deleted_responses"] == 0


def test_apply_transfers_only_missing_responses_then_cleans_source(tmp_path):
    conn, db_path, canonical_url = _build_dup_group_db(tmp_path)
    try:
        summary = apply_canonical_url_merge(conn, dry_run=False)
        cur = conn.cursor()

        cur.execute(
            "SELECT res_no, content_text FROM responses "
            "WHERE article_id=? AND article_type=? ORDER BY res_no",
            ("foo-slug", "a"),
        )
        keep_rows = cur.fetchall()
        assert keep_rows == [
            (1, "OLD-1"),
            (2, "KEEP-2"),
            (3, "OLD-3"),
        ]

        cur.execute(
            "SELECT COUNT(*) FROM responses WHERE article_id=?",
            ("12345",),
        )
        assert cur.fetchone()[0] == 0

        cur.execute(
            "SELECT COUNT(*) FROM articles WHERE article_id=?",
            ("12345",),
        )
        assert cur.fetchone()[0] == 0

        cur.execute(
            "SELECT COUNT(*) FROM articles WHERE article_id=?",
            ("foo-slug",),
        )
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()

    assert summary["dry_run"] is False
    assert summary["groups"][0]["sources"][0]["verification"] == "ok"
    assert summary["groups"][0]["sources"][0]["transferred"] == 2
    assert summary["groups"][0]["sources"][0]["deleted_articles"] == 1
    assert summary["groups"][0]["sources"][0]["deleted_responses"] == 3


def test_apply_does_not_introduce_id_type_rows(tmp_path):
    conn, db_path, canonical_url = _build_dup_group_db(tmp_path)
    try:
        apply_canonical_url_merge(conn, dry_run=False)
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM articles WHERE article_type='id'"
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT COUNT(*) FROM responses WHERE article_type='id'"
        )
        assert cur.fetchone()[0] == 0
    finally:
        conn.close()


def test_apply_deactivates_old_numeric_target_row(tmp_path):
    conn, db_path, canonical_url = _build_dup_group_db(tmp_path)
    try:
        apply_canonical_url_merge(conn, dry_run=False)
        cur = conn.cursor()

        cur.execute(
            "SELECT is_active FROM target "
            "WHERE article_id=? AND article_type=?",
            ("12345", "a"),
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

        cur.execute(
            "SELECT is_active FROM target "
            "WHERE article_id=? AND article_type=?",
            ("foo-slug", "a"),
        )
        keep_row = cur.fetchone()
        assert keep_row is not None
        assert keep_row[0] == 1
    finally:
        conn.close()


def test_apply_skips_groups_without_safe_keep_identity(tmp_path):
    db_path = tmp_path / "no_safe.db"
    conn = init_db(str(db_path))
    try:
        canonical_url = "https://dic.nicovideo.jp/a/foo-slug"
        _seed_articles_row(conn, "12345", "a", "Old", canonical_url)
        _seed_articles_row(conn, "67890", "a", "Older", canonical_url)
        _seed_response_row(conn, "12345", "a", 1, "OLD-1")
        _seed_response_row(conn, "67890", "a", 1, "OLDER-1")
        conn.commit()

        summary = apply_canonical_url_merge(conn, dry_run=False)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT COUNT(*) FROM responses")
        assert cur.fetchone()[0] == 2
    finally:
        conn.close()

    assert summary["groups"][0]["skip_reason"] == "no_safe_keep_identity"
    for src in summary["groups"][0]["sources"]:
        assert src["transferred"] == 0
        assert src["verification"] == "skipped"
        assert src["deleted_articles"] == 0


def test_merge_canonical_url_identities_requires_explicit_db_path(tmp_path):
    with pytest.raises(ValueError):
        merge_canonical_url_identities("", apply=False)


def test_merge_canonical_url_identities_rejects_missing_db_file(tmp_path):
    missing = tmp_path / "nope.db"
    with pytest.raises(FileNotFoundError):
        merge_canonical_url_identities(str(missing), apply=False)


def test_merge_canonical_url_identities_dry_run_default_does_not_write(
    tmp_path,
):
    conn, db_path, canonical_url = _build_dup_group_db(tmp_path)
    conn.close()

    summary = merge_canonical_url_identities(db_path)

    assert summary["dry_run"] is True

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        assert cur.fetchone()[0] == 2
        cur.execute(
            "SELECT COUNT(*) FROM responses WHERE article_id=?",
            ("12345",),
        )
        assert cur.fetchone()[0] == 3
    finally:
        conn.close()
