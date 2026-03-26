from archive_read import (
    get_saved_article_summary,
    get_saved_article_summary_by_exact_title,
    get_saved_article_txt,
    has_saved_article,
)
from storage import init_db, save_to_db


def _seed_archive(tmp_path, monkeypatch):
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


def test_has_saved_article_returns_true_for_existing_article(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)
    assert has_saved_article("12345", "a") is True


def test_has_saved_article_returns_false_for_missing_article(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)
    assert has_saved_article("99999", "a") is False


def test_get_saved_article_txt_returns_content_for_existing_article(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_txt("12345", "a")

    assert result["found"] is True
    assert result["article_id"] == "12345"
    assert result["article_type"] == "a"
    assert "=== ARTICLE META ===" in result["content"]
    assert "Title: First Title" in result["content"]
    assert ">1 Alice 2025-01-01 00:00 ID: abc123" in result["content"]


def test_get_saved_article_txt_returns_missing_shape_for_missing_article(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_txt("99999", "a")

    assert result == {
        "found": False,
        "content": None,
        "article_id": "99999",
        "article_type": "a",
    }


def test_get_saved_article_summary_returns_bounded_metadata_for_existing_article(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_summary("12345", "a")

    assert result == {
        "found": True,
        "article_id": "12345",
        "article_type": "a",
        "title": "First Title",
        "url": "https://dic.nicovideo.jp/a/12345",
        "created_at": result["created_at"],
        "response_count": 1,
    }
    assert result["created_at"]


def test_get_saved_article_summary_returns_missing_shape_for_missing_article(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_summary("99999", "a")

    assert result == {
        "found": False,
        "article_id": "99999",
        "article_type": "a",
        "title": None,
        "url": None,
        "created_at": None,
        "response_count": 0,
    }


def test_get_saved_article_summary_by_exact_title_returns_existing_article(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_summary_by_exact_title("First Title")

    assert result == {
        "found": True,
        "article_id": "12345",
        "article_type": "a",
        "title": "First Title",
        "url": "https://dic.nicovideo.jp/a/12345",
        "created_at": result["created_at"],
        "response_count": 1,
    }
    assert result["created_at"]


def test_get_saved_article_summary_by_exact_title_returns_missing_shape(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_summary_by_exact_title("Missing Title")

    assert result == {
        "found": False,
        "article_id": None,
        "article_type": None,
        "title": None,
        "url": None,
        "created_at": None,
        "response_count": 0,
    }


def test_get_saved_article_summary_by_exact_title_returns_ascii_case_insensitive_hit(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        save_to_db(
            conn,
            "5587284",
            "id",
            "G123",
            "https://dic.nicovideo.jp/id/5587284",
            [
                {
                    "res_no": 1,
                    "id_hash": "g123001",
                    "poster_name": "Alice",
                    "posted_at": "2025-01-01 00:00",
                    "content": "First response",
                    "content_html": "<p>First response</p>",
                }
            ],
        )
    finally:
        conn.close()

    result = get_saved_article_summary_by_exact_title("g123")

    assert result == {
        "found": True,
        "article_id": "5587284",
        "article_type": "id",
        "title": "G123",
        "url": "https://dic.nicovideo.jp/id/5587284",
        "created_at": result["created_at"],
        "response_count": 1,
    }
    assert result["created_at"]
