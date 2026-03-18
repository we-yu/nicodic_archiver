from cli import export_all_articles, list_articles
from storage import init_db, save_to_db


def _seed_archives(tmp_path, monkeypatch):
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
            "Second Title",
            "https://dic.nicovideo.jp/a/99999",
            [],
        )
    finally:
        conn.close()


def test_list_articles_outputs_required_fields_for_saved_articles(
    tmp_path, monkeypatch, capsys
):
    _seed_archives(tmp_path, monkeypatch)

    assert list_articles() is True

    out = capsys.readouterr().out
    assert "=== SAVED ARTICLES ===" in out
    assert "12345 a | title=First Title" in out
    assert "created_at=" in out
    assert "response_count=1" in out
    assert "99999 a | title=Second Title" in out
    assert "response_count=0" in out


def test_list_articles_handles_empty_db_as_success(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    conn.close()

    assert list_articles() is True

    out = capsys.readouterr().out
    assert "No saved articles found." in out


def test_export_all_articles_outputs_sectioned_txt_archives(
    tmp_path, monkeypatch, capsys
):
    _seed_archives(tmp_path, monkeypatch)

    assert export_all_articles("txt") is True

    out = capsys.readouterr().out
    assert "=== ARTICLE EXPORT 1/2 ===" in out
    assert "ID: 12345" in out
    assert "Title: First Title" in out
    assert "URL: https://dic.nicovideo.jp/a/12345" in out
    assert "Exported At:" in out
    assert "=== ARTICLE EXPORT 2/2 ===" in out
    assert "ID: 99999" in out
    assert "Title: Second Title" in out


def test_export_all_articles_handles_empty_db_as_success(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    conn.close()

    assert export_all_articles("txt") is True

    out = capsys.readouterr().out
    assert "No saved articles found." in out


def test_export_all_articles_rejects_unsupported_format(
    tmp_path, monkeypatch, capsys
):
    _seed_archives(tmp_path, monkeypatch)

    assert export_all_articles("md") is False

    out = capsys.readouterr().out
    assert "Unsupported export format: md" in out
