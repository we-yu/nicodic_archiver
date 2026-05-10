"""Focused tests for tools.repair_slug_article_identity maintenance tool.

These tests use isolated SQLite DBs in tmp_path. They never touch the runtime DB.
Network resolution is mocked; tests must not depend on live network access.
"""

import sqlite3

import pytest

from storage import init_db, register_target
from tools.repair_slug_article_identity import (
    UnresolvedNetworkError,
    format_repair_summary_lines,
    plan_slug_article_identity_repair,
    repair_slug_article_identity,
    write_unresolved_report,
)


def _insert_legacy_slug_target_row(conn, article_id, canonical_url):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO target (article_id, article_type, canonical_url, is_active)
        VALUES (?, 'a', ?, 1)
        """,
        (article_id, canonical_url),
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
    _insert_legacy_slug_target_row(conn, slug_id, canonical_url)

    conn.commit()
    return conn, str(db_path), canonical_url, slug_id, numeric_id


def _build_numeric_slug_group_db(tmp_path):
    db_path = tmp_path / "numeric-repair.db"
    conn = init_db(str(db_path))
    canonical_url = "https://dic.nicovideo.jp/a/999"
    legacy_numeric_slug_id = "999"
    resolved_numeric_id = "4734363"

    _seed_articles_row(conn, legacy_numeric_slug_id, "999", canonical_url)
    _seed_response_row(conn, legacy_numeric_slug_id, 1, "N1")
    register_target(conn, legacy_numeric_slug_id, "a", canonical_url)

    conn.commit()
    return (
        conn,
        str(db_path),
        canonical_url,
        legacy_numeric_slug_id,
        resolved_numeric_id,
    )


def _build_target_only_numeric_slug_db(
    tmp_path,
    *,
    slug_id="999",
):
    db_path = tmp_path / f"target-only-{slug_id}.db"
    conn = init_db(str(db_path))
    canonical_url = f"https://dic.nicovideo.jp/a/{slug_id}"

    _insert_legacy_slug_target_row(conn, slug_id, canonical_url)
    conn.commit()
    return conn, str(db_path), canonical_url, slug_id


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

    summary = repair_slug_article_identity(
        db_path,
        apply=False,
        allow_network=True,
        skip_unresolved=True,
        network_retry_delay_seconds=0,
    )
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


def test_network_fetch_failure_retries(tmp_path, monkeypatch):
    conn, db_path, canonical_url, _, numeric_id = _build_slug_group_db(tmp_path)
    conn.close()

    calls: list[str] = []

    def _mock_fetch(url: str) -> dict:
        calls.append(url)
        if len(calls) < 3:
            raise RuntimeError(f"temporary upstream failure status=500 {len(calls)}")
        return {
            "article_id": numeric_id,
            "article_type": "a",
            "title": "Foo",
        }

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        _mock_fetch,
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=False,
        allow_network=True,
        network_retries=2,
        network_retry_delay_seconds=0,
    )

    assert len(calls) == 3
    assert summary["groups"][0]["resolved_numeric_article_id"] == numeric_id
    assert summary["groups"][0]["attempts_made"] == 3


def test_network_failure_aborts_without_skip_unresolved(tmp_path, monkeypatch):
    conn, db_path, _, _, _ = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: (_ for _ in ()).throw(RuntimeError("status=500")),
    )

    with pytest.raises(UnresolvedNetworkError):
        repair_slug_article_identity(
            db_path,
            apply=False,
            allow_network=True,
            network_retries=1,
            network_retry_delay_seconds=0,
        )


def test_network_failure_is_skipped_with_skip_unresolved(tmp_path, monkeypatch):
    conn, db_path, canonical_url, slug_id, _ = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: (_ for _ in ()).throw(RuntimeError("status=500")),
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=False,
        allow_network=True,
        network_retries=1,
        network_retry_delay_seconds=0,
        skip_unresolved=True,
    )

    group = summary["groups"][0]
    assert group["canonical_url"] == canonical_url
    assert group["source_article_ids"] == [slug_id]
    assert group["resolved_numeric_article_id"] is None
    assert group["resolved_by"] == "network_failed"
    assert group["attempts_made"] == 2


def test_unresolved_report_is_written_with_expected_contents(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, slug_id, _ = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: (_ for _ in ()).throw(RuntimeError("status=500")),
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=False,
        allow_network=True,
        network_retries=1,
        network_retry_delay_seconds=0,
        skip_unresolved=True,
    )
    report_path = tmp_path / "unresolved.txt"
    write_unresolved_report(str(report_path), summary)

    text = report_path.read_text(encoding="utf-8")
    assert canonical_url in text
    assert f"legacy_article_id: {slug_id}" in text
    assert "article_type: a" in text
    assert "reason: network_failed" in text
    assert "error: status=500" in text
    assert "attempts_made: 2" in text


def test_skipped_unresolved_groups_are_not_modified_in_apply_mode(
    tmp_path,
    monkeypatch,
):
    conn, db_path, _, slug_id, _ = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: (_ for _ in ()).throw(RuntimeError("status=500")),
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retries=1,
        network_retry_delay_seconds=0,
        skip_unresolved=True,
    )

    assert summary["groups"][0]["apply"]["status"] == "skipped"
    assert summary["groups"][0]["apply"]["reason"] == "network_failed"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT article_id, title, canonical_url FROM articles ORDER BY article_id"
        )
        assert cur.fetchall() == [
            (slug_id, "Foo", "https://dic.nicovideo.jp/a/foo-slug")
        ]
        cur.execute(
            "SELECT article_id, article_type, res_no FROM responses ORDER BY res_no"
        )
        assert cur.fetchall() == [(slug_id, "a", 1), (slug_id, "a", 2)]
    finally:
        conn.close()


def test_numeric_only_slug_article_id_is_detected_and_planned(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id, resolved_numeric_id = (
        _build_numeric_slug_group_db(tmp_path)
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": resolved_numeric_id,
            "article_type": "a",
            "title": "999",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=False,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    group = summary["groups"][0]
    assert summary["legacy_counts"]["articles"] == 1
    assert summary["legacy_counts"]["responses"] == 1
    assert summary["legacy_counts"]["target"] == 1
    assert group["canonical_url"] == canonical_url
    assert group["source_article_ids"] == [legacy_id]
    assert group["existing_numeric_article_ids"] == []
    assert group["resolved_numeric_article_id"] == resolved_numeric_id
    assert group["resolved_by"] == "network"


def test_numeric_only_slug_article_id_is_applied_when_metadata_resolves(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id, resolved_numeric_id = (
        _build_numeric_slug_group_db(tmp_path)
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": resolved_numeric_id,
            "article_type": "a",
            "title": "999",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    assert summary["groups"][0]["apply"]["status"] == "applied"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT article_id, article_type, title, canonical_url "
            "FROM articles ORDER BY article_id"
        )
        assert cur.fetchall() == [
            (
                resolved_numeric_id,
                "a",
                "999",
                canonical_url,
            )
        ]
        cur.execute(
            "SELECT article_id, article_type, res_no, content_text "
            "FROM responses ORDER BY res_no"
        )
        assert cur.fetchall() == [
            (resolved_numeric_id, "a", 1, "N1")
        ]
        cur.execute(
            "SELECT article_id, article_type, canonical_url, is_active "
            "FROM target ORDER BY article_id"
        )
        assert set(cur.fetchall()) == {
            (legacy_id, "a", canonical_url, 0),
            (resolved_numeric_id, "a", canonical_url, 1),
        }
    finally:
        conn.close()


def test_numeric_only_slug_unresolved_case_is_skipped_safely(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id, _ = _build_numeric_slug_group_db(
        tmp_path
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: (_ for _ in ()).throw(RuntimeError("status=500")),
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retries=1,
        network_retry_delay_seconds=0,
        skip_unresolved=True,
    )

    group = summary["groups"][0]
    assert group["canonical_url"] == canonical_url
    assert group["resolved_numeric_article_id"] is None
    assert group["apply"]["status"] == "skipped"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT article_id, title, canonical_url FROM articles ORDER BY article_id"
        )
        assert cur.fetchall() == [(legacy_id, "999", canonical_url)]
    finally:
        conn.close()


def test_numeric_only_slug_same_resolved_id_is_noop_in_apply(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id, _ = _build_numeric_slug_group_db(
        tmp_path
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": legacy_id,
            "article_type": "a",
            "title": "999",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    group = summary["groups"][0]
    assert group["needs_repair"] is False
    assert group["apply"]["status"] == "skipped"
    assert group["apply"]["reason"] == "no_identity_change"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT article_id, title, canonical_url FROM articles ORDER BY article_id"
        )
        assert cur.fetchall() == [(legacy_id, "999", canonical_url)]
    finally:
        conn.close()


def test_numeric_only_slug_apply_overwrites_preexisting_numeric_title(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id, resolved_numeric_id = (
        _build_numeric_slug_group_db(tmp_path)
    )
    try:
        _seed_articles_row(conn, resolved_numeric_id, "Wrong title", canonical_url)
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": resolved_numeric_id,
            "article_type": "a",
            "title": "999",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    assert summary["groups"][0]["apply"]["status"] == "applied"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT article_id, title, canonical_url FROM articles ORDER BY article_id"
        )
        assert cur.fetchall() == [
            (resolved_numeric_id, "999", canonical_url)
        ]
        cur.execute(
            "SELECT article_id, article_type, canonical_url, is_active "
            "FROM target ORDER BY article_id"
        )
        assert set(cur.fetchall()) == {
            (legacy_id, "a", canonical_url, 0),
            (resolved_numeric_id, "a", canonical_url, 1),
        }
    finally:
        conn.close()


def test_numeric_only_slug_prefers_metadata_id_url_identity(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id, resolved_numeric_id = (
        _build_numeric_slug_group_db(tmp_path)
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": legacy_id,
            "article_type": "a",
            "article_url": f"https://dic.nicovideo.jp/id/{resolved_numeric_id}",
            "title": "999",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    assert summary["groups"][0]["apply"]["status"] == "applied"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT article_id, article_type, title, canonical_url "
            "FROM articles ORDER BY article_id"
        )
        assert cur.fetchall() == [
            (resolved_numeric_id, "a", "999", canonical_url)
        ]
        cur.execute(
            "SELECT article_id, article_type, canonical_url, is_active "
            "FROM target ORDER BY article_id"
        )
        assert set(cur.fetchall()) == {
            (legacy_id, "a", canonical_url, 0),
            (resolved_numeric_id, "a", canonical_url, 1),
        }
    finally:
        conn.close()


def test_metadata_record_with_article_id_but_missing_canonical_url_still_works(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, _, numeric_id = _build_slug_group_db(tmp_path)
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": numeric_id,
            "article_type": "a",
            "title": "Foo",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=False,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    group = summary["groups"][0]
    assert group["canonical_url"] == canonical_url
    assert group["resolved_numeric_article_id"] == numeric_id


def test_existing_legacy_non_digit_slug_behavior_still_passes(
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
            "title": "Foo",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    assert summary["groups"][0]["source_article_ids"] == [slug_id]
    assert summary["groups"][0]["resolved_numeric_article_id"] == numeric_id
    assert summary["groups"][0]["apply"]["status"] == "applied"
    lines = format_repair_summary_lines(db_path, summary)
    assert any("Unresolved groups: 0" in line for line in lines)


def test_target_only_numeric_slug_is_detected_and_planned(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id = _build_target_only_numeric_slug_db(
        tmp_path
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": legacy_id,
            "article_type": "a",
            "article_url": "https://dic.nicovideo.jp/id/4734363",
            "title": "999",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=False,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    group = summary["groups"][0]
    assert summary["legacy_counts"]["articles"] == 0
    assert summary["legacy_counts"]["responses"] == 0
    assert summary["legacy_counts"]["target"] == 1
    assert group["canonical_url"] == canonical_url
    assert group["source_article_ids"] == [legacy_id]
    assert group["existing_numeric_article_ids"] == []
    assert group["resolved_numeric_article_id"] == "4734363"
    assert group["resolved_by"] == "network"
    assert group["title"] == "999"


def test_target_only_numeric_slug_apply_normalizes_target_only(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id = _build_target_only_numeric_slug_db(
        tmp_path
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": legacy_id,
            "article_type": "a",
            "article_url": "https://dic.nicovideo.jp/id/4734363",
            "title": "999",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    assert summary["groups"][0]["apply"]["status"] == "applied"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM responses")
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT article_id, article_type, canonical_url, is_active "
            "FROM target ORDER BY article_id"
        )
        assert set(cur.fetchall()) == {
            (legacy_id, "a", canonical_url, 0),
            ("4734363", "a", canonical_url, 1),
        }
    finally:
        conn.close()


def test_target_only_numeric_slug_network_failure_skips_safely(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id = _build_target_only_numeric_slug_db(
        tmp_path
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: (_ for _ in ()).throw(RuntimeError("status=500")),
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retries=1,
        network_retry_delay_seconds=0,
        skip_unresolved=True,
    )

    group = summary["groups"][0]
    assert group["canonical_url"] == canonical_url
    assert group["resolved_numeric_article_id"] is None
    assert group["apply"]["status"] == "skipped"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM responses")
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT article_id, article_type, canonical_url, is_active "
            "FROM target"
        )
        assert cur.fetchall() == [(legacy_id, "a", canonical_url, 1)]
    finally:
        conn.close()


def test_target_only_numeric_slug_without_id_url_is_unresolved(
    tmp_path,
    monkeypatch,
):
    conn, db_path, canonical_url, legacy_id = _build_target_only_numeric_slug_db(
        tmp_path
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": legacy_id,
            "article_type": "a",
            "title": "999",
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=False,
        allow_network=True,
        network_retry_delay_seconds=0,
        skip_unresolved=True,
    )

    group = summary["groups"][0]
    assert group["canonical_url"] == canonical_url
    assert group["resolved_numeric_article_id"] is None
    assert group["resolved_by"] == "network_failed"


def test_target_only_numeric_slug_4294967295_model_is_protected(
    tmp_path,
    monkeypatch,
):
    slug_id = "4294967295"
    conn, db_path, canonical_url, legacy_id = _build_target_only_numeric_slug_db(
        tmp_path,
        slug_id=slug_id,
    )
    conn.close()

    monkeypatch.setattr(
        "tools.repair_slug_article_identity.fetch_article_metadata_record",
        lambda _url: {
            "article_id": legacy_id,
            "article_type": "a",
            "article_url": "https://dic.nicovideo.jp/id/237789",
            "title": slug_id,
        },
    )

    summary = repair_slug_article_identity(
        db_path,
        apply=True,
        allow_network=True,
        network_retry_delay_seconds=0,
    )

    assert summary["groups"][0]["resolved_numeric_article_id"] == "237789"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT article_id, article_type, canonical_url, is_active "
            "FROM target ORDER BY article_id"
        )
        assert set(cur.fetchall()) == {
            (legacy_id, "a", canonical_url, 0),
            ("237789", "a", canonical_url, 1),
        }
    finally:
        conn.close()
