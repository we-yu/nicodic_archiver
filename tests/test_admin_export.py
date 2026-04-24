from io import StringIO

from admin_export import run_admin_export
from storage import init_db, save_to_db


def _seed_admin_archives(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "First Title",
            "https://dic.nicovideo.jp/a/12345",
            [
                {
                    "res_no": 1,
                    "id_hash": "abc123",
                    "poster_name": "Alice",
                    "posted_at": "2025-01-01 00:00",
                    "content": "First response",
                    "content_html": "<p>First response</p>",
                }
            ],
        )
        save_to_db(
            conn,
            "99999",
            "a",
            "12345",
            "https://dic.nicovideo.jp/a/99999",
            [],
        )
    finally:
        conn.close()


def _run_export(args):
    stdout = StringIO()
    stderr = StringIO()
    exit_code = run_admin_export(args, stdout=stdout, stderr=stderr)
    return exit_code, stdout.getvalue(), stderr.getvalue()


def test_admin_export_defaults_to_txt_for_title_input(tmp_path, monkeypatch):
    _seed_admin_archives(tmp_path, monkeypatch)

    exit_code, out, err = _run_export(["First Title"])

    assert exit_code == 0
    assert "=== ARTICLE META ===" in out
    assert "Title: First Title" in out
    assert "exporting 12345 a as 12345a_First Title.txt" in err


def test_admin_export_supports_id_input(tmp_path, monkeypatch):
    _seed_admin_archives(tmp_path, monkeypatch)

    exit_code, out, err = _run_export(["--id", "12345"])

    assert exit_code == 0
    assert "Title: First Title" in out
    assert "12345a_First Title.txt" in err


def test_admin_export_supports_numeric_title_with_title_flag(
    tmp_path,
    monkeypatch,
):
    _seed_admin_archives(tmp_path, monkeypatch)

    exit_code, out, err = _run_export(["--title", "12345"])

    assert exit_code == 0
    assert "Title: 12345" in out
    assert "99999a_12345.txt" in err


def test_admin_export_supports_markdown_format(tmp_path, monkeypatch):
    _seed_admin_archives(tmp_path, monkeypatch)

    exit_code, out, err = _run_export(["First Title", "--md"])

    assert exit_code == 0
    assert "# First Title" in out
    assert "## Responses" in out
    assert "12345a_First Title.md" in err


def test_admin_export_supports_csv_format(tmp_path, monkeypatch):
    _seed_admin_archives(tmp_path, monkeypatch)

    exit_code, out, err = _run_export(["First Title", "--csv"])

    assert exit_code == 0
    assert "article_id,article_type,article_title" in out
    assert "12345,a,First Title" in out
    assert "12345a_First Title.csv" in err
