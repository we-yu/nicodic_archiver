import importlib.util
import sqlite3
from pathlib import Path

import pytest

from storage import init_db, register_target


MODULE_PATH = Path(__file__).resolve().parent.parent / "tools" / "repair_slug_article_identity.py"
SPEC = importlib.util.spec_from_file_location(
    "repair_slug_article_identity",
    MODULE_PATH,
)
repair_tool = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(repair_tool)


def _seed_article(conn, article_id, title, canonical_url):
    conn.execute(
        """
        INSERT INTO articles (article_id, article_type, title, canonical_url)
        VALUES (?, 'a', ?, ?)
        """,
        (article_id, title, canonical_url),
    )


def _seed_response(conn, article_id, res_no, content_text):
    conn.execute(
        """
        INSERT INTO responses (article_id, article_type, res_no, content_text)
        VALUES (?, 'a', ?, ?)
        """,
        (article_id, res_no, content_text),
    )


def _seed_slug_group_db(tmp_path):
    db_path = tmp_path / "repair.db"
    conn = init_db(str(db_path))
    canonical_url = "https://dic.nicovideo.jp/a/foo-slug"
    slug_id = "foo-slug"
    numeric_id = "12345"

    _seed_article(conn, slug_id, "Foo", canonical_url)
    _seed_article(conn, numeric_id, "Foo numeric", canonical_url)
    _seed_response(conn, slug_id, 1, "slug-1")
    _seed_response(conn, slug_id, 2, "slug-2")
    _seed_response(conn, numeric_id, 2, "numeric-2")
    register_target(conn, slug_id, "a", canonical_url)
    conn.commit()
    conn.close()
    return str(db_path), canonical_url, slug_id, numeric_id


def test_detects_slug_article_id_rows(tmp_path):
    db_path, canonical_url, slug_id, numeric_id = _seed_slug_group_db(tmp_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        detection = repair_tool.detect_legacy_slug_identity_groups(conn)
    finally:
        conn.close()

    assert len(detection["groups"]) == 1
    group = detection["groups"][0]
    assert group["canonical_url"] == canonical_url
    assert [row["article_id"] for row in group["legacy_article_rows"]] == [slug_id]
    assert [row["article_id"] for row in group["numeric_article_rows"]] == [numeric_id]
    assert group["legacy_response_identities"] == [
        {"article_id": slug_id, "response_count": 2}
    ]


def test_dry_run_performs_no_writes(tmp_path):
    db_path, canonical_url, slug_id, numeric_id = _seed_slug_group_db(tmp_path)

    summary = repair_tool.repair_slug_article_identities(db_path)

    assert summary["dry_run"] is True
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles WHERE article_id=?", (slug_id,))
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM responses WHERE article_id=?", (slug_id,))
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT COUNT(*) FROM target WHERE article_id=?", (slug_id,))
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_apply_transfers_missing_responses_and_skips_duplicate_res_no(tmp_path):
    db_path, canonical_url, slug_id, numeric_id = _seed_slug_group_db(tmp_path)

    summary = repair_tool.repair_slug_article_identities(db_path, apply=True)

    assert summary["dry_run"] is False
    assert summary["transaction"] == "committed"
    group = summary["groups"][0]
    assert group["resolved_article_id"] == numeric_id
    assert group["response_actions"][0]["transferred_count"] == 1
    assert group["response_actions"][0]["duplicate_res_no_count"] == 1
    assert group["response_actions"][0]["verification"] == "ok"

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT res_no, content_text FROM responses "
            "WHERE article_id=? AND article_type='a' ORDER BY res_no ASC",
            (numeric_id,),
        )
        assert cur.fetchall() == [(1, "slug-1"), (2, "numeric-2")]
        cur.execute("SELECT COUNT(*) FROM responses WHERE article_id=?", (slug_id,))
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM articles WHERE article_id=?", (slug_id,))
        assert cur.fetchone()[0] == 0
    finally:
        conn.close()


def test_apply_skips_group_without_resolvable_numeric_id_and_reports_it(tmp_path):
    db_path = tmp_path / "unresolved.db"
    conn = init_db(str(db_path))
    canonical_url = "https://dic.nicovideo.jp/a/foo-slug"
    _seed_article(conn, "foo-slug", "Foo", canonical_url)
    _seed_response(conn, "foo-slug", 1, "slug-1")
    conn.commit()
    conn.close()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            repair_tool,
            "fetch_article_metadata_record",
            lambda canonical_url: {
                "article_id": None,
                "article_type": "a",
                "article_url": canonical_url,
            },
        )
        summary = repair_tool.repair_slug_article_identities(str(db_path), apply=True)

    assert summary["groups"][0]["skip_reason"] == "metadata_non_numeric_article_id"
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles WHERE article_id='foo-slug'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM responses WHERE article_id='foo-slug'")
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_apply_does_not_create_article_type_id_rows(tmp_path):
    db_path, canonical_url, slug_id, numeric_id = _seed_slug_group_db(tmp_path)

    repair_tool.repair_slug_article_identities(db_path, apply=True)

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


def test_apply_target_rows_are_normalized_without_delete_first(tmp_path):
    db_path, canonical_url, slug_id, numeric_id = _seed_slug_group_db(tmp_path)

    summary = repair_tool.repair_slug_article_identities(db_path, apply=True)

    target_actions = summary["groups"][0]["target_actions"]
    assert target_actions == [
        {"source_article_id": slug_id, "action": "updated_to_numeric"}
    ]

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM target")
        assert cur.fetchone()[0] == 1
        cur.execute(
            "SELECT article_id, canonical_url, is_active FROM target ORDER BY id ASC"
        )
        assert cur.fetchone() == (numeric_id, canonical_url, 1)
    finally:
        conn.close()


def test_apply_deletes_legacy_slug_target_after_verification_when_numeric_exists(
    tmp_path,
):
    db_path = tmp_path / "target_existing_numeric.db"
    conn = init_db(str(db_path))
    canonical_url = "https://dic.nicovideo.jp/a/foo-slug"
    _seed_article(conn, "foo-slug", "Foo", canonical_url)
    _seed_article(conn, "12345", "Foo", canonical_url)
    _seed_response(conn, "foo-slug", 1, "slug-1")
    register_target(conn, "foo-slug", "a", canonical_url)
    register_target(conn, "12345", "a", canonical_url)
    conn.commit()
    conn.close()

    summary = repair_tool.repair_slug_article_identities(str(db_path), apply=True)

    assert summary["groups"][0]["target_actions"] == [
        {
            "source_article_id": "foo-slug",
            "action": "deleted_after_verification",
        }
    ]

    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT article_id FROM target ORDER BY article_id ASC")
        assert cur.fetchall() == [("12345",)]
    finally:
        conn.close()


def test_detects_orphan_slug_response_identity_without_canonical_source(tmp_path):
    db_path = tmp_path / "orphan.db"
    conn = init_db(str(db_path))
    _seed_response(conn, "foo-slug", 1, "orphan")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        detection = repair_tool.detect_legacy_slug_identity_groups(conn)
    finally:
        conn.close()

    assert detection["groups"] == []
    assert detection["orphan_response_identities"] == [
        {
            "article_id": "foo-slug",
            "response_count": 1,
            "skip_reason": "missing_canonical_url_source",
        }
    ]


def test_cli_requires_explicit_db_path(capsys):
    with pytest.raises(SystemExit) as exc_info:
        repair_tool.main([])

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "--db" in captured.err


def test_cli_apply_flag_is_required_for_writes(tmp_path, capsys):
    db_path, canonical_url, slug_id, numeric_id = _seed_slug_group_db(tmp_path)

    exit_code = repair_tool.main(["--db", db_path])

    assert exit_code == 0
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles WHERE article_id=?", (slug_id,))
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM responses WHERE article_id=?", (slug_id,))
        assert cur.fetchone()[0] == 2
    finally:
        conn.close()

    captured = capsys.readouterr()
    assert "Mode: dry-run" in captured.out


def test_metadata_resolution_is_mockable_for_network_free_tests(tmp_path):
    db_path = tmp_path / "metadata.db"
    conn = init_db(str(db_path))
    canonical_url = "https://dic.nicovideo.jp/a/foo-slug"
    _seed_article(conn, "foo-slug", "Foo", canonical_url)
    conn.commit()
    conn.close()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            repair_tool,
            "fetch_article_metadata_record",
            lambda article_url: {
                "article_id": "5560706",
                "article_type": "a",
                "article_url": article_url,
            },
        )
        summary = repair_tool.repair_slug_article_identities(str(db_path), apply=True)

    assert summary["groups"][0]["resolved_article_id"] == "5560706"
    assert summary["groups"][0]["resolution_source"] == "metadata"
