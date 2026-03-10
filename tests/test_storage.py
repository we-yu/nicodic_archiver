"""Unit tests for storage layer (storage.py).

These tests run in a temp working directory so production `data/` is untouched.
"""

import json
import sqlite3

import storage
from storage import init_db, save_json, save_to_db


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
