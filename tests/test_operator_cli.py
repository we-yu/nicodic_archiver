from operator_cli import (
    export_archive_for_operator,
    inspect_target_for_operator,
    list_archives_for_operator,
    list_targets_for_operator,
)
from storage import init_db, save_to_db
from target_list import deactivate_target, register_target_url


def test_list_targets_for_operator_shows_status_and_count(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    target_db_path = tmp_path / "targets.db"

    register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))
    register_target_url("https://dic.nicovideo.jp/id/99999", str(target_db_path))
    deactivate_target("99999", "id", str(target_db_path))

    assert list_targets_for_operator(str(target_db_path), active_only=False) is True

    out = capsys.readouterr().out
    assert "=== TARGET REGISTRY ===" in out
    assert "Count: 2" in out
    assert "active   12345 a" in out
    assert "inactive 99999 id" in out


def test_inspect_target_for_operator_returns_false_for_missing_target(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    target_db_path = tmp_path / "targets.db"

    assert inspect_target_for_operator("404", "a", str(target_db_path)) is False

    out = capsys.readouterr().out
    assert "Target not found in registry" in out
    assert "ID: 404" in out


def test_list_archives_for_operator_shows_saved_archive_summary(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Archive Title",
            "https://dic.nicovideo.jp/a/12345",
            [
                {
                    "res_no": 1,
                    "id_hash": "abc123",
                    "poster_name": "Alice",
                    "posted_at": "2025-01-01 00:00",
                    "content": "hello",
                    "content_html": "<p>hello</p>",
                }
            ],
        )
    finally:
        conn.close()

    assert list_archives_for_operator() is True

    out = capsys.readouterr().out
    assert "=== SAVED ARCHIVES ===" in out
    assert "Count: 1" in out
    assert "12345 a | title=Archive Title | responses=1" in out


def test_export_archive_for_operator_writes_requested_output_file(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Archive Title",
            "https://dic.nicovideo.jp/a/12345",
            [
                {
                    "res_no": 1,
                    "id_hash": "abc123",
                    "poster_name": "Alice",
                    "posted_at": "2025-01-01 00:00",
                    "content": "hello",
                    "content_html": "<p>hello</p>",
                }
            ],
        )
    finally:
        conn.close()

    output_path = tmp_path / "exports" / "archive.md"

    assert export_archive_for_operator(
        "12345",
        "a",
        "md",
        output_path=str(output_path),
    ) is True

    out = capsys.readouterr().out
    assert "Archive export written" in out
    assert output_path.is_file()
    content = output_path.read_text(encoding="utf-8")
    assert "# Archive Title" in content
    assert "### Response 1" in content
