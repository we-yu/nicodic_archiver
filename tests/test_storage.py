"""Unit tests for storage layer (storage.py).

These tests run in a temp working directory so production `data/` is untouched.
"""

import json
import sqlite3

import storage
from storage import (
    append_scrape_run_observation,
    dequeue_canonical_target,
    enqueue_canonical_target,
    format_run_telemetry_csv_wide,
    get_target,
    init_db,
    list_queue_requests,
    list_targets,
    register_target,
    save_json,
    save_to_db,
    set_target_active_state,
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
        assert "scrape_run_observation" in tables
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


def test_register_target_persists_canonical_identity_once(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        first = register_target(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/12345",
        )
        second = register_target(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/12345",
        )

        assert first["status"] == "added"
        assert second["status"] == "duplicate"
        assert second["target_identity"] == {
            "article_id": "12345",
            "article_type": "a",
        }

        targets = list_targets(conn)
        assert len(targets) == 1
        assert targets[0]["canonical_url"] == "https://dic.nicovideo.jp/a/12345"
        assert targets[0]["is_active"] is True
    finally:
        conn.close()


def test_register_target_reactivates_inactive_target(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/12345",
        )

        cur = conn.cursor()
        cur.execute(
            "UPDATE target SET is_active=0 WHERE article_id=? AND article_type=?",
            ("12345", "a"),
        )
        conn.commit()

        result = register_target(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/12345",
        )

        assert result["status"] == "reactivated"
        assert list_targets(conn) == [
            {
                "id": 1,
                "article_id": "12345",
                "article_type": "a",
                "canonical_url": "https://dic.nicovideo.jp/a/12345",
                "is_active": True,
                "created_at": result["entry"]["created_at"],
            }
        ]
    finally:
        conn.close()


def test_list_targets_filters_out_inactive_rows_by_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            "1",
            "a",
            "https://dic.nicovideo.jp/a/1",
        )
        register_target(
            conn,
            "2",
            "a",
            "https://dic.nicovideo.jp/a/2",
        )

        cur = conn.cursor()
        cur.execute(
            "UPDATE target SET is_active=0 WHERE article_id=? AND article_type=?",
            ("2", "a"),
        )
        conn.commit()

        active_targets = list_targets(conn)
        all_targets = list_targets(conn, active_only=False)

        assert [item["article_id"] for item in active_targets] == ["1"]
        assert [item["article_id"] for item in all_targets] == ["1", "2"]
    finally:
        conn.close()


def test_get_target_returns_single_registry_entry(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/12345",
        )

        entry = get_target(conn, "12345", "a")

        assert entry is not None
        assert entry["article_id"] == "12345"
        assert entry["article_type"] == "a"
        assert entry["canonical_url"] == "https://dic.nicovideo.jp/a/12345"
        assert entry["is_active"] is True
    finally:
        conn.close()


def test_set_target_active_state_deactivates_and_reactivates_non_destructively(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/12345",
        )

        deactivated = set_target_active_state(conn, "12345", "a", False)
        reactivated = set_target_active_state(conn, "12345", "a", True)

        assert deactivated["found"] is True
        assert deactivated["status"] == "deactivated"
        assert deactivated["entry"]["is_active"] is False

        assert reactivated["found"] is True
        assert reactivated["status"] == "activated"
        assert reactivated["entry"]["is_active"] is True
    finally:
        conn.close()


def test_set_target_active_state_reports_not_found_without_writing(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        result = set_target_active_state(conn, "404", "a", False)

        assert result == {
            "found": False,
            "status": "not_found",
            "entry": None,
            "target_identity": {
                "article_id": "404",
                "article_type": "a",
            },
        }
    finally:
        conn.close()


def test_append_scrape_run_observation_csv_wide_has_run_columns(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        append_scrape_run_observation(
            conn,
            run_id="r1",
            run_started_at="2026-01-01T00:00:00+00:00",
            run_kind="batch",
            article_id="1",
            article_type="a",
            canonical_article_url="https://dic.nicovideo.jp/a/1",
            scrape_outcome="ok",
        )
        append_scrape_run_observation(
            conn,
            run_id="r2",
            run_started_at="2026-01-02T00:00:00+00:00",
            run_kind="batch",
            article_id="1",
            article_type="a",
            canonical_article_url="https://dic.nicovideo.jp/a/1",
            scrape_outcome="skip_denylist",
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM scrape_run_observation")
        assert cur.fetchone()[0] == 2
    finally:
        conn.close()

    conn = init_db()
    try:
        csv_text = format_run_telemetry_csv_wide(conn)
    finally:
        conn.close()

    assert "run0_saved_response_count_after_run" in csv_text
    assert "run1_skipped" in csv_text
    assert "skip_denylist" in csv_text
