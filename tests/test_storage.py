"""Unit tests for storage layer (storage.py).

These tests run in a temp working directory so production `data/` is untouched.
"""

import json
import sqlite3

import storage
from storage import (
    append_scrape_run_observation,
    compute_all_article_response_stats,
    dequeue_canonical_target,
    enqueue_canonical_target,
    format_response_stats_rebuild_lines,
    format_run_telemetry_csv_wide,
    get_target,
    init_db,
    list_queue_requests,
    list_targets,
    mark_target_redirected,
    open_readonly_db,
    rebuild_article_response_stats,
    rebuild_article_response_stats_for_db,
    register_target,
    save_json,
    save_to_db,
    set_target_active_state,
)


def _read_stats_row(conn, article_id, article_type):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT saved_response_count, saved_max_res_no
        FROM article_response_stats
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    return cur.fetchone()


def _make_response(res_no):
    return {
        "res_no": res_no,
        "id_hash": f"h{res_no}",
        "poster_name": "P",
        "posted_at": "2025-01-01 00:00",
        "content": f"c{res_no}",
        "content_html": f"<p>c{res_no}</p>",
    }


def _table_names(conn: sqlite3.Connection) -> set[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in cur.fetchall()}


def test_open_readonly_db_returns_none_without_creating_file(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)

    conn = open_readonly_db()

    assert conn is None
    assert not (tmp_path / "data").exists()


def test_open_readonly_db_uses_query_only_on_existing_db(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    writer = init_db()
    writer.close()

    conn = open_readonly_db()
    assert conn is not None
    try:
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 1
    finally:
        conn.close()


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
        assert "article_response_stats" in tables
    finally:
        conn.close()


def _index_names(conn: sqlite3.Connection) -> set[str]:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND name NOT LIKE 'sqlite_%'"
    )
    return {row[0] for row in cur.fetchall()}


def test_init_db_creates_supporting_indexes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        indexes = _index_names(conn)
        assert "idx_articles_type_canonical_url" in indexes
        assert "idx_target_active_created_at" in indexes
    finally:
        conn.close()


def test_init_db_supporting_indexes_are_idempotent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    conn.close()
    conn = init_db()
    try:
        indexes = _index_names(conn)
        assert "idx_articles_type_canonical_url" in indexes
        assert "idx_target_active_created_at" in indexes
    finally:
        conn.close()


def test_save_to_db_updates_response_summary(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Title",
            "https://dic.nicovideo.jp/a/12345",
            [_make_response(1), _make_response(2)],
        )
        assert _read_stats_row(conn, "12345", "a") == (2, 2)
    finally:
        conn.close()


def test_save_to_db_summary_grows_with_more_responses(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        url = "https://dic.nicovideo.jp/a/12345"
        save_to_db(conn, "12345", "a", "Title", url, [_make_response(1)])
        assert _read_stats_row(conn, "12345", "a") == (1, 1)

        save_to_db(
            conn,
            "12345",
            "a",
            "Title",
            url,
            [_make_response(2), _make_response(3)],
        )
        assert _read_stats_row(conn, "12345", "a") == (3, 3)
    finally:
        conn.close()


def test_save_to_db_summary_does_not_overcount_duplicates(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        url = "https://dic.nicovideo.jp/a/12345"
        responses = [_make_response(1), _make_response(2)]
        save_to_db(conn, "12345", "a", "Title", url, responses)
        save_to_db(conn, "12345", "a", "Title", url, responses)
        assert _read_stats_row(conn, "12345", "a") == (2, 2)
    finally:
        conn.close()


def test_save_to_db_summary_zero_response_checked_article(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        save_to_db(
            conn,
            "7711002",
            "a",
            "Checked Zero",
            "https://dic.nicovideo.jp/a/7711002",
            [],
            latest_scraped_at="2026-06-07T08:09:10+00:00",
        )
        # Raw summary: count 0, max NULL. The 0-display rule lives in the
        # read layer (keyed on last_scraped_at), not the stored summary.
        assert _read_stats_row(conn, "7711002", "a") == (0, None)
    finally:
        conn.close()


def test_compute_all_article_response_stats_groups_by_identity(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        save_to_db(
            conn,
            "111",
            "a",
            "A",
            "https://dic.nicovideo.jp/a/111",
            [_make_response(1), _make_response(2)],
        )
        save_to_db(
            conn,
            "222",
            "a",
            "B",
            "https://dic.nicovideo.jp/a/222",
            [_make_response(5)],
        )
        computed = dict(
            ((aid, atype), (count, max_res))
            for (aid, atype, count, max_res) in (
                compute_all_article_response_stats(conn)
            )
        )
        assert computed[("111", "a")] == (2, 2)
        assert computed[("222", "a")] == (1, 5)
    finally:
        conn.close()


def test_rebuild_article_response_stats_dry_run_does_not_write(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        save_to_db(
            conn,
            "111",
            "a",
            "A",
            "https://dic.nicovideo.jp/a/111",
            [_make_response(1)],
        )
        conn.execute("DELETE FROM article_response_stats")
        conn.commit()

        summary = rebuild_article_response_stats(conn, dry_run=True)
        assert summary["dry_run"] is True
        assert summary["computed_articles"] == 1
        assert summary["existing_summary_rows"] == 0
        assert summary["written_rows"] == 0
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM article_response_stats")
        assert cur.fetchone()[0] == 0
    finally:
        conn.close()


def test_rebuild_article_response_stats_apply_writes_expected_rows(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        save_to_db(
            conn,
            "111",
            "a",
            "A",
            "https://dic.nicovideo.jp/a/111",
            [_make_response(1), _make_response(7)],
        )
        conn.execute("DELETE FROM article_response_stats")
        conn.commit()

        summary = rebuild_article_response_stats(conn, dry_run=False)
        assert summary["dry_run"] is False
        assert summary["computed_articles"] == 1
        assert _read_stats_row(conn, "111", "a") == (2, 7)
    finally:
        conn.close()


def test_rebuild_article_response_stats_for_db_dry_run_default(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "data" / "nicodic.db"
    conn = init_db()
    try:
        save_to_db(
            conn,
            "111",
            "a",
            "A",
            "https://dic.nicovideo.jp/a/111",
            [_make_response(1)],
        )
        conn.execute("DELETE FROM article_response_stats")
        conn.commit()
    finally:
        conn.close()

    summary = rebuild_article_response_stats_for_db(str(db_path))
    assert summary["dry_run"] is True
    assert summary["computed_articles"] == 1

    check = init_db(str(db_path))
    try:
        cur = check.cursor()
        cur.execute("SELECT COUNT(*) FROM article_response_stats")
        # dry-run wrote nothing; init_db re-open does not backfill
        assert cur.fetchone()[0] == 0
    finally:
        check.close()

    lines = format_response_stats_rebuild_lines(str(db_path), summary)
    assert any("Mode: dry-run" in line for line in lines)


def test_rebuild_article_response_stats_for_db_apply_writes(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "data" / "nicodic.db"
    conn = init_db()
    try:
        save_to_db(
            conn,
            "111",
            "a",
            "A",
            "https://dic.nicovideo.jp/a/111",
            [_make_response(1), _make_response(2)],
        )
        conn.execute("DELETE FROM article_response_stats")
        conn.commit()
    finally:
        conn.close()

    summary = rebuild_article_response_stats_for_db(str(db_path), apply=True)
    assert summary["dry_run"] is False

    check = init_db(str(db_path))
    try:
        assert _read_stats_row(check, "111", "a") == (2, 2)
    finally:
        check.close()


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


def test_save_to_db_followup_with_empty_preserves_prior_responses(
    tmp_path, monkeypatch,
):
    """Zero-response scrape pass must not delete existing archive responses."""
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        article_id = "12345"
        article_type = "a"
        url = "https://dic.nicovideo.jp/a/12345"
        responses = [
            {
                "res_no": 1,
                "id_hash": "a",
                "poster_name": "P1",
                "posted_at": "2025-01-01 00:00",
                "content": "C1",
                "content_html": "<p>c1</p>",
            },
        ]
        save_to_db(conn, article_id, article_type, "T", url, responses)
        save_to_db(
            conn,
            article_id,
            article_type,
            "T",
            url,
            [],
            latest_scraped_at="2099-12-31T00:00:00+00:00",
        )

        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM responses "
            "WHERE article_id=? AND article_type=?",
            (article_id, article_type),
        )
        assert cur.fetchone()[0] == 1

        cur.execute(
            "SELECT latest_scraped_at FROM articles "
            "WHERE article_id=? AND article_type=?",
            (article_id, article_type),
        )
        assert cur.fetchone()[0] == "2099-12-31T00:00:00+00:00"
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


def test_save_to_db_persists_bounded_article_metadata(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Some Title",
            "https://dic.nicovideo.jp/a/12345",
            [],
            published_at="2024-01-02T03:04:05+09:00",
            modified_at="2025-02-03T04:05:06+09:00",
            latest_scraped_at="2026-03-04T05:06:07+09:00",
        )

        cur = conn.cursor()
        cur.execute(
            "SELECT published_at, modified_at, latest_scraped_at "
            "FROM articles WHERE article_id=? AND article_type=?",
            ("12345", "a"),
        )
        row = cur.fetchone()

        assert row == (
            "2024-01-02T03:04:05+09:00",
            "2025-02-03T04:05:06+09:00",
            "2026-03-04T05:06:07+09:00",
        )
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


def test_register_target_rejects_non_numeric_article_id_for_type_a(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        try:
            register_target(
                conn,
                "sluglike",
                "a",
                "https://dic.nicovideo.jp/a/sluglike",
            )
        except ValueError as exc:
            assert "digits-only" in str(exc)
        else:
            raise AssertionError("expected ValueError")
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
        targets = list_targets(conn)
        assert len(targets) == 1
        assert targets[0]["article_id"] == "12345"
        assert targets[0]["article_type"] == "a"
        assert targets[0]["canonical_url"] == "https://dic.nicovideo.jp/a/12345"
        assert targets[0]["is_active"] is True
        assert targets[0]["is_redirected"] is False
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
        assert entry["is_redirected"] is False
        assert entry["redirect_target_url"] is None
        assert entry["redirect_detected_at"] is None
    finally:
        conn.close()


def test_mark_target_redirected_persists_redirect_state_and_deactivates(
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

        result = mark_target_redirected(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/67890",
            "2026-04-14T00:00:00+00:00",
        )

        assert result["found"] is True
        assert result["status"] == "redirected"
        assert result["entry"]["is_active"] is False
        assert result["entry"]["is_redirected"] is True
        assert result["entry"]["redirect_target_url"] == (
            "https://dic.nicovideo.jp/a/67890"
        )
        assert result["entry"]["redirect_detected_at"] == (
            "2026-04-14T00:00:00+00:00"
        )
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
