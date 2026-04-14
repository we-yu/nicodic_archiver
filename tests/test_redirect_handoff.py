import sqlite3
from datetime import datetime, timezone

from target_list import handoff_redirected_target, list_active_target_urls
from target_list import register_target_url


def _read_table_count(db_path: str, table: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def test_handoff_redirected_target_deactivates_source_and_registers_redirect(tmp_path):
    db_path = tmp_path / "targets.db"
    register_target_url("https://dic.nicovideo.jp/a/111", str(db_path))

    detected_at = datetime.now(timezone.utc).isoformat()
    result = handoff_redirected_target(
        "111",
        "a",
        "https://dic.nicovideo.jp/a/222",
        target_db_path=str(db_path),
        detected_at=detected_at,
    )

    assert result["ok"] is True
    assert result["status"] == "handed_off"
    assert list_active_target_urls(str(db_path)) == ["https://dic.nicovideo.jp/a/222"]
    assert result["source_entry"]["is_active"] is False
    assert result["source_entry"]["is_redirected"] is True
    assert result["source_entry"]["redirect_url"] == "https://dic.nicovideo.jp/a/222"


def test_handoff_redirect_suppresses_duplicate_redirect_target(tmp_path):
    db_path = tmp_path / "targets.db"
    register_target_url("https://dic.nicovideo.jp/a/111", str(db_path))
    register_target_url("https://dic.nicovideo.jp/a/222", str(db_path))

    detected_at = datetime.now(timezone.utc).isoformat()
    result = handoff_redirected_target(
        "111",
        "a",
        "https://dic.nicovideo.jp/a/222",
        target_db_path=str(db_path),
        detected_at=detected_at,
    )

    assert result["ok"] is True
    assert result["redirect_register_status"] in {"duplicate", "reactivated"}
    assert list_active_target_urls(str(db_path)) == ["https://dic.nicovideo.jp/a/222"]


def test_handoff_redirect_does_not_write_archive_tables(tmp_path):
    db_path = tmp_path / "targets.db"
    register_target_url("https://dic.nicovideo.jp/a/111", str(db_path))

    before_articles = _read_table_count(str(db_path), "articles")
    before_responses = _read_table_count(str(db_path), "responses")

    detected_at = datetime.now(timezone.utc).isoformat()
    handoff_redirected_target(
        "111",
        "a",
        "https://dic.nicovideo.jp/a/222",
        target_db_path=str(db_path),
        detected_at=detected_at,
    )

    after_articles = _read_table_count(str(db_path), "articles")
    after_responses = _read_table_count(str(db_path), "responses")

    assert after_articles == before_articles
    assert after_responses == before_responses
