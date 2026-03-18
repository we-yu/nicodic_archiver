from cli import export_article
from storage import init_db, save_to_db


def _seed_article_archive(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Some Title",
            "https://dic.nicovideo.jp/a/12345",
            [
                {
                    "res_no": 1,
                    "id_hash": "abc123",
                    "poster_name": "Alice",
                    "posted_at": "2025-01-01 00:00",
                    "content": "Hello world",
                    "content_html": "<p>Hello world</p>",
                },
                {
                    "res_no": 2,
                    "id_hash": None,
                    "poster_name": None,
                    "posted_at": None,
                    "content": "Second post",
                    "content_html": "<p>Second post</p>",
                },
            ],
        )
    finally:
        conn.close()


def test_export_article_txt_outputs_human_readable_text(tmp_path, monkeypatch, capsys):
    _seed_article_archive(tmp_path, monkeypatch)

    assert export_article("12345", "a", "txt") is True

    out = capsys.readouterr().out
    assert "=== ARTICLE META ===" in out
    assert "ID: 12345" in out
    assert "Type: a" in out
    assert "Title: Some Title" in out
    assert "=== RESPONSES ===" in out
    assert ">1 Alice 2025-01-01 00:00 ID: abc123" in out
    assert "Second post" in out


def test_export_article_md_outputs_human_readable_markdown(
    tmp_path, monkeypatch, capsys
):
    _seed_article_archive(tmp_path, monkeypatch)

    assert export_article("12345", "a", "md") is True

    out = capsys.readouterr().out
    assert "# Some Title" in out
    assert "- Article ID: 12345" in out
    assert "- Article Type: a" in out
    assert "## Responses" in out
    assert "### Response 1" in out
    assert "- Poster: Alice" in out
    assert "### Response 2" in out
    assert "- Poster: unknown" in out


def test_export_article_returns_false_for_missing_article(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    conn.close()

    assert export_article("99999", "a", "txt") is False

    out = capsys.readouterr().out
    assert "Article not found in DB" in out


def test_export_article_returns_false_for_unsupported_format(
    tmp_path, monkeypatch, capsys
):
    _seed_article_archive(tmp_path, monkeypatch)

    assert export_article("12345", "a", "html") is False

    out = capsys.readouterr().out
    assert "Unsupported export format: html" in out
