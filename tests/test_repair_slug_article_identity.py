"""Focused tests for tools.repair_slug_article_identity maintenance tool.

These tests use isolated SQLite DBs in tmp_path. They never touch the runtime DB.
Network resolution is mocked; tests must not depend on live network access.
"""

import sqlite3

import pytest

from storage import init_db, register_target
from tools.repair_slug_article_identity import (
    format_repair_summary_lines,
    plan_slug_article_identity_repair,
    repair_slug_article_identity,
)


def _seed_articles_row(conn, article_id, title, canonical_url):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO articles (article_id, article_type, title, canonical_url)
        VALUES (?, 'a', ?, ?)
        """,
        (article_id, title, canonical_url),
    )


def _seed_response_row(conn, article_id, res_no, content_text):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO responses (article_id, article_type, res_no, content_text)
        VALUES (?, 'a', ?, ?)
        """,
        (article_id, res_no, content_text),
    )


def _build_slug_group_db(tmp_path):
    db_path = tmp_path / "repair.db"
    conn = init_db(str(db_path))
    canonical_url = "https://dic.nicovideo.jp/a/foo-slug"
    slug_id = "foo-slug"
    numeric_id = "5560706"

    _seed_articles_row(conn, slug_id, "Foo", canonical_url)
    _seed_response_row(conn, slug_id, 1, "S1")
    _seed_response_row(conn, slug_id, 2, "S2")
    register_target(conn, slug_id, "a", canonical_url)

    conn.commit()
    return conn, str(db_path), canonical_url, slug_id, numeric_id


def test_requires_explicit_db_path():
    with pytest.raises(ValueError):
        repair_slug_article_identity("")


def test_rejects_missing_db_file(tmp_path):
    missing = tmp_path / "nope.db"
    with pytest.raises(FileNotFoundError):
        repair_slug_article_identity(str(missing))


def test_detection_finds_slug_group_and_plans_resolution_skipped_without_network(
    tmp_path,
):
    conn, db_path, canonical_url, slug_id, numeric_id = _build_slug_group_db(tmp_path)
    try:
        summary = plan_slug_article_identity_repair(conn, allow_network=False)
    finally:
        conn.close()

    assert summary["dry_run"] is True
    assert summary["allow_network"] is False
    assert summary["legacy_counts"]["articles"] == 1
    assert len(summary["groups"]) == 1
    assert summary["groups"][0]["canonical_url"] == canonical_url
    assert summary["groups"][0]["resolved_numeric_article_id"] is None
    assert summary["groups"][0]["resolved_by"] == "skipped_network_disallowed"


def test_dry_run_performs_no_writes(tmp_path, monkeypatch):
    conn, db_path, canonical_url, slug_id, numeric_id = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": numeric_id,
            "article_type": "a",
            "article_url": canonical_url,
            "title": "Foo",
            "published_at": None,
            "modified_at": None,
        },
    )

    summary = repair_slug_article_identity(db_path, apply=False, allow_network=True)
    assert summary["dry_run"] is True

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM responses")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT COUNT(*) FROM target")
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_apply_transfers_missing_responses_and_deactivates_slug_target(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, slug_id, numeric_id = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": numeric_id,
            "article_type": "a",
            "article_url": canonical_url,
            "title": "Foo",
            "published_at": None,
            "modified_at": None,
        },
    )

    summary = repair_slug_article_identity(db_path, apply=True, allow_network=True)
    assert summary["dry_run"] is False
    assert summary["groups"][0]["apply"]["status"] == "applied"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT res_no, content_text FROM responses "
            "WHERE article_id=? AND article_type='a' ORDER BY res_no",
            (numeric_id,),
        )
        assert cur.fetchall() == [(1, "S1"), (2, "S2")]
        cur.execute(
            "SELECT COUNT(*) FROM responses WHERE article_id=? AND article_type='a'",
            (slug_id,),
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT COUNT(*) FROM articles WHERE article_id=? AND article_type='a'",
            (slug_id,),
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT is_active FROM target WHERE article_id=? AND article_type='a'",
            (slug_id,),
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT is_active FROM target WHERE article_id=? AND article_type='a'",
            (numeric_id,),
        )
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_apply_does_not_insert_duplicate_res_no(tmp_path, monkeypatch):
    conn, db_path, canonical_url, slug_id, numeric_id = _build_slug_group_db(tmp_path)
    try:
        _seed_articles_row(conn, numeric_id, "Foo", canonical_url)
        _seed_response_row(conn, numeric_id, 2, "KEEP-2")
        register_target(conn, numeric_id, "a", canonical_url)
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": numeric_id,
            "article_type": "a",
            "article_url": canonical_url,
            "title": "Foo",
            "published_at": None,
            "modified_at": None,
        },
    )

    summary = repair_slug_article_identity(db_path, apply=True, allow_network=True)
    assert summary["dry_run"] is False

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT res_no, content_text FROM responses "
            "WHERE article_id=? AND article_type='a' ORDER BY res_no",
            (numeric_id,),
        )
        assert cur.fetchall() == [(1, "S1"), (2, "KEEP-2")]
    finally:
        conn.close()


def test_group_skipped_when_network_resolution_fails(tmp_path, monkeypatch):
    conn, db_path, canonical_url, slug_id, numeric_id = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": "not-numeric",
            "article_type": "a",
            "article_url": canonical_url,
            "title": "Foo",
            "published_at": None,
            "modified_at": None,
        },
    )

    summary = repair_slug_article_identity(db_path, apply=False, allow_network=True)
    assert summary["groups"][0]["resolved_numeric_article_id"] is None
    assert summary["groups"][0]["resolved_by"] == "network_failed"


def test_tool_never_creates_article_type_id_rows(tmp_path, monkeypatch):
    conn, db_path, canonical_url, slug_id, numeric_id = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": numeric_id,
            "article_type": "a",
            "article_url": canonical_url,
            "title": "Foo",
            "published_at": None,
            "modified_at": None,
        },
    )

    repair_slug_article_identity(db_path, apply=True, allow_network=True)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles WHERE article_type='id'")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM responses WHERE article_type='id'")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM target WHERE article_type='id'")
        assert cur.fetchone()[0] == 0
    finally:
        conn.close()


def test_summary_format_is_human_readable(tmp_path, monkeypatch):
    conn, db_path, canonical_url, slug_id, numeric_id = _build_slug_group_db(
        tmp_path
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": numeric_id,
            "article_type": "a",
            "article_url": canonical_url,
            "title": "Foo",
            "published_at": None,
            "modified_at": None,
        },
    )

    summary = repair_slug_article_identity(db_path, apply=False, allow_network=True)
    lines = format_repair_summary_lines(db_path, summary)
    assert lines[0].startswith("=== REPAIR SLUG ARTICLE IDENTITY ===")
    assert any("Mode: dry-run" in line for line in lines)
    assert any("Processed groups: 1" in line for line in lines)


def test_limit_restricts_processed_groups(tmp_path):
    conn, db_path, _, _, _ = _build_slug_group_db(tmp_path)
    url2 = "https://dic.nicovideo.jp/a/bar-slug"
    _seed_articles_row(conn, "bar-slug", "Bar", url2)
    conn.commit()
    conn.close()

    summary = repair_slug_article_identity(db_path, apply=False, limit=1)
    assert summary["total_detected_groups"] == 2
    assert summary["processed_groups"] == 1
    assert len(summary["groups"]) == 1


def test_limit_zero_processes_no_groups(tmp_path):
    conn, db_path, _, _, _ = _build_slug_group_db(tmp_path)
    conn.close()

    summary = repair_slug_article_identity(db_path, apply=False, limit=0)
    assert summary["processed_groups"] == 0
    assert len(summary["groups"]) == 0


def test_limit_applies_before_network_resolution(tmp_path, monkeypatch):
    conn, db_path, _, _, _ = _build_slug_group_db(tmp_path)
    conn.close()

    called: list[str] = []

    def _mock_fetch(url: str) -> dict:
        called.append(url)
        return {}

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        _mock_fetch,
    )

    summary = repair_slug_article_identity(
        db_path, apply=False, allow_network=True, limit=0
    )
    assert summary["processed_groups"] == 0
    assert called == []


def test_summary_only_suppresses_per_group_detail(tmp_path, monkeypatch):
    conn, db_path, canonical_url, _, numeric_id = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": numeric_id,
            "article_type": "a",
            "article_url": canonical_url,
            "title": "Foo",
            "published_at": None,
            "modified_at": None,
        },
    )

    summary = repair_slug_article_identity(db_path, apply=False, allow_network=True)
    lines = format_repair_summary_lines(db_path, summary, summary_only=True)
    assert not any("canonical_url:" in line for line in lines)
    assert any("Processed groups:" in line for line in lines)
    assert any("Resolved groups:" in line for line in lines)


def test_dry_run_with_limit_performs_no_writes(tmp_path, monkeypatch):
    conn, db_path, canonical_url, _, numeric_id = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": numeric_id,
            "article_type": "a",
            "article_url": canonical_url,
            "title": "Foo",
            "published_at": None,
            "modified_at": None,
        },
    )

    summary = repair_slug_article_identity(
        db_path, apply=False, allow_network=True, limit=1
    )
    assert summary["dry_run"] is True

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM responses")
        assert cur.fetchone()[0] == 2
    finally:
        conn.close()
