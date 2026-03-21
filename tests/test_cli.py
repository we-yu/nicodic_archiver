from unittest.mock import patch

from cli import export_all_articles, export_article, list_articles
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


def test_export_article_outputs_txt_for_saved_article(tmp_path, monkeypatch, capsys):
    _seed_archives(tmp_path, monkeypatch)

    assert export_article("12345", "a", "txt") is True

    out = capsys.readouterr().out
    assert "=== ARTICLE META ===" in out
    assert "ID: 12345" in out
    assert "Type: a" in out
    assert "Title: First Title" in out
    assert "=== RESPONSES ===" in out
    assert ">1 Alice 2025-01-01 00:00 ID: abc123" in out


def test_export_article_outputs_md_for_saved_article(tmp_path, monkeypatch, capsys):
    _seed_archives(tmp_path, monkeypatch)

    assert export_article("12345", "a", "md") is True

    out = capsys.readouterr().out
    assert "# First Title" in out
    assert "- Article ID: 12345" in out
    assert "## Responses" in out
    assert "### Response 1" in out


@patch("cli.read_article_summaries")
def test_list_articles_uses_summary_read_seam(mock_read_summaries, capsys):
    mock_read_summaries.return_value = [
        {
            "article_id": "12345",
            "article_type": "a",
            "title": "First Title",
            "created_at": "2025-01-01T00:00:00+00:00",
            "response_count": 1,
        }
    ]

    assert list_articles() is True

    mock_read_summaries.assert_called_once_with()
    out = capsys.readouterr().out
    assert "12345 a | title=First Title" in out


@patch("cli.read_article_archive")
def test_export_article_uses_archive_read_seam(mock_read_archive, capsys):
    mock_read_archive.return_value = {
        "article_id": "12345",
        "article_type": "a",
        "title": "First Title",
        "url": "https://dic.nicovideo.jp/a/12345",
        "created_at": "2025-01-01T00:00:00+00:00",
        "responses": [(1, "Alice", "2025-01-01 00:00", "abc123", "Hello")],
    }

    assert export_article("12345", "a", "txt") is True

    mock_read_archive.assert_called_once_with("12345", "a")
    out = capsys.readouterr().out
    assert "Title: First Title" in out
    assert "Hello" in out
