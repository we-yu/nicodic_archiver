from archive_read import get_saved_article_txt, has_saved_article
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
    finally:
        conn.close()


def test_has_saved_article_returns_true_for_saved_article(tmp_path, monkeypatch):
    _seed_archives(tmp_path, monkeypatch)

    assert has_saved_article("12345", "a") is True


def test_has_saved_article_returns_false_for_missing_article(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    conn.close()

    assert has_saved_article("99999", "a") is False


def test_get_saved_article_txt_returns_bounded_txt_content(
    tmp_path,
    monkeypatch,
):
    _seed_archives(tmp_path, monkeypatch)

    content = get_saved_article_txt("12345", "a")

    assert content is not None
    assert "=== ARTICLE META ===" in content
    assert "ID: 12345" in content
    assert "Title: First Title" in content
    assert ">1 Alice 2025-01-01 00:00 ID: abc123" in content
    assert "First response" in content


def test_get_saved_article_txt_returns_none_for_missing_article(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    conn.close()

    assert get_saved_article_txt("99999", "a") is None
