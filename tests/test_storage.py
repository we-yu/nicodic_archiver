"""Unit tests for storage layer (storage.py).

These tests run in a temp working directory so production `data/` is untouched.
"""

import json
import sqlite3

import storage
from storage import (
    admin_import_targets_from_txt,
    dequeue_canonical_target,
    enqueue_canonical_target,
    init_db,
    list_active_scrape_target_urls,
    list_queue_requests,
    register_scrape_target,
    save_json,
    save_to_db,
)


def _table_names(conn: sqlite3.Connection) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in cur.fetchall()}


def test_init_db_creates_data_dir_db_and_tables(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        db_path = tmp_path / "data" / "nicodic.db"
        assert (tmp_path / "data").is_dir()
        assert db_path.is_file()

        tables = _table_names(conn)
        assert "articles" in tables
        assert "responses" in tables
        assert "queue_requests" in tables
        assert "target" in tables
    finally:
        conn.close()


def test_save_to_db_inserts_article_and_responses_and_mapping(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        article_id = "12345"
        article_type = "a"
        title = "Some Title"
        article_url = "https://dic.nicovideo.jp/a/12345"
        responses = [
            {
                "res_no": 1,
                "id_hash": "id1",
                "poster_name": "Alice",
                "posted_at": "2025-01-01 00:00",
                "content": "TEXT-1",
                "content_html": "<div>HTML-1</div>",
            },
            {
                "res_no": 2,
                "id_hash": "id2",
                "poster_name": "Bob",
                "posted_at": "2025-01-01 00:01",
                "content": "TEXT-2",
                "content_html": "<div>HTML-2</div>",
            },
        ]

        save_to_db(conn, article_id, article_type, title, article_url, responses)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT COUNT(*) FROM responses")
        assert cur.fetchone()[0] == 2

        # Mapping protection: response.content -> content_text,
        # response.content_html -> content_html
        cur.execute(
            "SELECT res_no, content_text, content_html "
            "FROM responses "
            "WHERE article_id=? AND article_type=? "
            "ORDER BY res_no ASC",
            (article_id, article_type),
        )
        rows = cur.fetchall()
        assert rows == [
            (1, "TEXT-1", "<div>HTML-1</div>"),
            (2, "TEXT-2", "<div>HTML-2</div>"),
        ]
    finally:
        conn.close()


def test_save_to_db_insert_or_ignore_prevents_duplicate_growth(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        article_id = "12345"
        article_type = "a"
        title = "Some Title"
        article_url = "https://dic.nicovideo.jp/a/12345"
        responses = [
            {"res_no": 1, "content": "TEXT-1", "content_html": "<div>HTML-1</div>"},
            {"res_no": 2, "content": "TEXT-2", "content_html": "<div>HTML-2</div>"},
        ]

        save_to_db(conn, article_id, article_type, title, article_url, responses)
        save_to_db(conn, article_id, article_type, title, article_url, responses)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM articles")
        assert cur.fetchone()[0] == 1

        cur.execute("SELECT COUNT(*) FROM responses")
        assert cur.fetchone()[0] == 2
    finally:
        conn.close()


def test_save_json_writes_json_and_sanitizes_title_in_filename(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(storage.time, "time", lambda: 1700000000)

    article_id = "99999"
    article_type = "a"
    title = "A/B\\C"
    article_url = "https://dic.nicovideo.jp/a/99999"
    responses = [
        {
            "res_no": 1,
            "id_hash": "id1",
            "poster_name": "Alice",
            "posted_at": "2025-01-01 00:00",
            "content": "TEXT",
            "content_html": "<div>HTML</div>",
        }
    ]

    save_json(article_id, article_type, title, article_url, responses)

    # Filename behavior: / -> ／ and \ -> ＼
    expected_filename = f"{article_id}{article_type}_A／B＼C.json"
    output_path = tmp_path / "data" / expected_filename
    assert output_path.is_file()

    data = json.loads(output_path.read_text(encoding="utf-8"))
    for key in [
        "article_id",
        "article_type",
        "article_url",
        "title",
        "collected_at",
        "response_count",
        "responses",
    ]:
        assert key in data

    assert data["article_id"] == article_id
    assert data["article_type"] == article_type
    assert data["article_url"] == article_url
    assert data["title"] == title
    assert data["collected_at"] == 1700000000
    assert data["response_count"] == 1
    assert data["responses"] == responses


def test_enqueue_canonical_target_persists_minimal_queue_entry(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        canonical_target = {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        }

        result = enqueue_canonical_target(conn, canonical_target, title="First Title")

        assert result["status"] == "enqueued"
        assert result["queue_identity"] == {
            "article_id": "12345",
            "article_type": "a",
        }
        assert result["entry"]["article_url"] == canonical_target["article_url"]
        assert result["entry"]["article_id"] == canonical_target["article_id"]
        assert result["entry"]["article_type"] == canonical_target["article_type"]
        assert result["entry"]["title"] == "First Title"
        assert result["entry"]["enqueued_at"] is not None

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM queue_requests")
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_enqueue_canonical_target_suppresses_duplicates_as_success_class(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        canonical_target = {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        }

        first = enqueue_canonical_target(conn, canonical_target, title="First Title")
        second = enqueue_canonical_target(conn, canonical_target, title="First Title")

        assert first["status"] == "enqueued"
        assert second["status"] == "duplicate"
        assert second["queue_identity"] == {
            "article_id": "12345",
            "article_type": "a",
        }

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM queue_requests")
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()


def test_enqueue_canonical_target_is_persistent_across_connections(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    canonical_target = {
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
    }

    conn = init_db()
    try:
        first = enqueue_canonical_target(conn, canonical_target, title=None)
        assert first["status"] == "enqueued"
    finally:
        conn.close()

    conn = init_db()
    try:
        second = enqueue_canonical_target(conn, canonical_target, title=None)
        assert second["status"] == "duplicate"
        assert second["entry"]["title"] is None
    finally:
        conn.close()


def test_list_queue_requests_returns_fifo_order(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        enqueue_canonical_target(
            conn,
            {
                "article_url": "https://dic.nicovideo.jp/a/1",
                "article_id": "1",
                "article_type": "a",
            },
            title="One",
        )
        enqueue_canonical_target(
            conn,
            {
                "article_url": "https://dic.nicovideo.jp/a/2",
                "article_id": "2",
                "article_type": "a",
            },
            title="Two",
        )

        queued = list_queue_requests(conn)
        assert [item["article_id"] for item in queued] == ["1", "2"]
    finally:
        conn.close()


def test_dequeue_canonical_target_removes_only_requested_item(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        enqueue_canonical_target(
            conn,
            {
                "article_url": "https://dic.nicovideo.jp/a/1",
                "article_id": "1",
                "article_type": "a",
            },
        )
        enqueue_canonical_target(
            conn,
            {
                "article_url": "https://dic.nicovideo.jp/a/2",
                "article_id": "2",
                "article_type": "a",
            },
        )

        removed = dequeue_canonical_target(conn, "1", "a")
        assert removed is True

        queued = list_queue_requests(conn)
        assert [item["article_id"] for item in queued] == ["2"]
    finally:
        conn.close()


def test_register_scrape_target_inserts_and_duplicate_by_identity(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        assert (
            register_scrape_target(
                conn,
                "12345",
                "a",
                "https://dic.nicovideo.jp/a/12345",
            )
            == "added"
        )
        assert (
            register_scrape_target(
                conn,
                "12345",
                "a",
                "https://dic.nicovideo.jp/a/12345",
            )
            == "duplicate"
        )
    finally:
        conn.close()


def test_list_active_scrape_target_urls_is_read_only_stable_order(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_scrape_target(
            conn,
            "1",
            "a",
            "https://dic.nicovideo.jp/a/1",
        )
        register_scrape_target(
            conn,
            "2",
            "a",
            "https://dic.nicovideo.jp/a/2",
        )
        urls = list_active_scrape_target_urls(conn)
        assert urls == [
            "https://dic.nicovideo.jp/a/1",
            "https://dic.nicovideo.jp/a/2",
        ]
    finally:
        conn.close()


def test_list_active_scrape_target_urls_skips_inactive_rows(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_scrape_target(
            conn,
            "9",
            "a",
            "https://dic.nicovideo.jp/a/9",
        )
        conn.execute("UPDATE target SET is_active = 0 WHERE article_id = '9'")
        conn.commit()
        assert list_active_scrape_target_urls(conn) == []
    finally:
        conn.close()


def test_admin_import_targets_from_txt_one_shot_bounded(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    txt = tmp_path / "legacy.txt"
    txt.write_text(
        "https://dic.nicovideo.jp/a/10\n"
        "https://dic.nicovideo.jp/a/11\n"
        "not-a-url\n",
        encoding="utf-8",
    )
    conn = init_db()
    try:
        summary = admin_import_targets_from_txt(conn, str(txt))
        assert summary["added"] == 2
        assert summary["duplicate"] == 0
        assert summary["invalid"] == 1
    finally:
        conn.close()


def test_admin_import_marks_duplicate_when_identity_already_registered(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    txt = tmp_path / "legacy.txt"
    txt.write_text("https://dic.nicovideo.jp/a/10\n", encoding="utf-8")
    conn = init_db()
    try:
        register_scrape_target(
            conn,
            "10",
            "a",
            "https://dic.nicovideo.jp/a/10",
        )
        summary = admin_import_targets_from_txt(conn, str(txt))
        assert summary["added"] == 0
        assert summary["duplicate"] == 1
        assert summary["invalid"] == 0
    finally:
        conn.close()


def test_admin_import_targets_from_txt_missing_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        summary = admin_import_targets_from_txt(
            conn,
            str(tmp_path / "missing.txt"),
        )
        assert summary["error"] == "file_not_found"
    finally:
        conn.close()
