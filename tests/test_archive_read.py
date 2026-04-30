import csv
from io import StringIO
import sqlite3
from unittest.mock import patch

from archive_read import (
    get_registered_article_listing,
    get_registered_articles_csv,
    get_saved_article_export,
    get_saved_article_summary,
    get_saved_article_summary_by_exact_title,
    get_saved_article_summary_by_id,
    get_saved_article_txt,
    has_saved_article,
    list_registered_articles,
    write_scrape_targets_txt,
)
from storage import init_db, register_target, save_to_db


def _seed_archive(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            "12345",
            "a",
            "https://dic.nicovideo.jp/a/12345",
        )
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
    assert "Article ID: 12345" in result["content"]
    assert "Canonical URL: https://dic.nicovideo.jp/a/12345" in result["content"]
    assert "1 Alice 2025-01-01 00:00 ID: abc123" in result["content"]
    assert "Created:" not in result["content"]


def test_get_saved_article_txt_keeps_reply_markers_in_response_body(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Reply Title",
            "https://dic.nicovideo.jp/a/12345",
            [
                {
                    "res_no": 1,
                    "id_hash": "abc123",
                    "poster_name": "Alice",
                    "posted_at": "2025-01-01 00:00",
                    "content": ">>123\nreply body",
                    "content_html": "<p>&gt;&gt;123</p><p>reply body</p>",
                }
            ],
            modified_at="2025-01-02T00:00:00+09:00",
        )
    finally:
        conn.close()

    result = get_saved_article_txt("12345", "a")

    assert "Last Modified: 2025-01-02T00:00:00+09:00" in result["content"]
    assert "1 Alice 2025-01-01 00:00 ID: abc123" in result["content"]
    assert ">>123\nreply body" in result["content"]


def test_get_saved_article_export_returns_markdown_for_existing_article(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_export("12345", "a", "md")

    assert result["found"] is True
    assert result["format"] == "md"
    assert "# First Title" in result["content"]
    assert "- Article ID: 12345" in result["content"]
    assert "## Responses" in result["content"]
    assert "### 1" in result["content"]


def test_get_saved_article_export_returns_csv_rows_with_stable_header(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_export("12345", "a", "csv")
    rows = list(csv.DictReader(StringIO(result["content"])))

    assert result["found"] is True
    assert result["format"] == "csv"
    assert rows == [
        {
            "article_id": "12345",
            "article_type": "a",
            "storage_article_key": "12345",
            "article_title": "First Title",
            "canonical_url": "https://dic.nicovideo.jp/a/12345",
            "res_no": "1",
            "poster_name": "Alice",
            "poster_id": "abc123",
            "posted_at": "2025-01-01 00:00",
            "content_text": "First response",
            "content_html": "<p>First response</p>",
        }
    ]


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
        "published_at": None,
        "modified_at": None,
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
        "published_at": None,
        "modified_at": None,
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
        "published_at": None,
        "modified_at": None,
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
        "published_at": None,
        "modified_at": None,
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
        "published_at": None,
        "modified_at": None,
        "response_count": 1,
    }
    assert result["created_at"]


def test_archive_read_tolerates_db_without_metadata_columns(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    conn = sqlite3.connect(data_dir / "nicodic.db")
    try:
        conn.execute(
            """
            CREATE TABLE articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id TEXT NOT NULL,
                article_type TEXT NOT NULL,
                title TEXT NOT NULL,
                canonical_url TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(article_id, article_type)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id TEXT NOT NULL,
                article_type TEXT NOT NULL,
                res_no INTEGER NOT NULL,
                id_hash TEXT,
                poster_name TEXT,
                posted_at TEXT,
                content_text TEXT,
                UNIQUE(article_id, article_type, res_no)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO articles (article_id, article_type, title, canonical_url)
            VALUES (?, ?, ?, ?)
            """,
            (
                "12345",
                "a",
                "First Title",
                "https://dic.nicovideo.jp/a/12345",
            ),
        )
        conn.execute(
            """
            INSERT INTO responses (
                article_id, article_type, res_no, id_hash,
                poster_name, posted_at, content_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "12345",
                "a",
                1,
                "abc123",
                "Alice",
                "2025-01-01 00:00",
                "First response",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    summary = get_saved_article_summary("12345", "a")
    txt_result = get_saved_article_txt("12345", "a")

    assert summary["found"] is True
    assert summary["published_at"] is None
    assert summary["modified_at"] is None
    assert txt_result["found"] is True
    assert "Title: First Title" in txt_result["content"]


def test_archive_read_does_not_call_init_db_on_read_path(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    with patch(
        "archive_read.sqlite3.connect",
        wraps=sqlite3.connect,
    ) as mock_connect:
        result = get_saved_article_summary("12345", "a")

    mock_connect.assert_called_once()
    assert result["found"] is True


def test_get_saved_article_summary_by_id_returns_first_match(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_summary_by_id("12345")

    assert result["found"] is True
    assert result["article_id"] == "12345"
    assert result["article_type"] == "a"
    assert result["title"] == "First Title"
    assert result["response_count"] == 1


def test_get_saved_article_summary_by_id_returns_missing_shape(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = get_saved_article_summary_by_id("99999")

    assert result["found"] is False
    assert result["article_id"] == "99999"
    assert result["article_type"] is None


def test_list_registered_articles_returns_expected_columns(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    articles = list_registered_articles()

    assert len(articles) == 1
    row = articles[0]
    assert row["article_id"] == "12345"
    assert row["article_id_display"] == "12345"
    assert row["article_type"] == "a"
    assert row["title"] == "First Title"
    assert row["canonical_url"] == "https://dic.nicovideo.jp/a/12345"
    assert row["saved_response_count"] == 1
    assert row["latest_scraped_max_res_no"] == 1
    assert "last_scraped_at" in row
    assert row["created_at"]
    assert row["is_pending_initial_scrape"] is False


def test_get_registered_article_listing_defaults_to_recently_registered_first(
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
        register_target(
            conn,
            "99999",
            "a",
            "https://dic.nicovideo.jp/a/99999",
        )
        conn.execute(
            "UPDATE target SET created_at=? WHERE article_id=? AND article_type='a'",
            ("2026-01-01 00:00:00", "12345"),
        )
        conn.execute(
            "UPDATE target SET created_at=? WHERE article_id=? AND article_type='a'",
            ("2026-01-02 00:00:00", "99999"),
        )
        save_to_db(
            conn,
            "12345",
            "a",
            "First Title",
            "https://dic.nicovideo.jp/a/12345",
            [],
        )
        save_to_db(
            conn,
            "99999",
            "a",
            "Second Title",
            "https://dic.nicovideo.jp/a/99999",
            [],
        )
        conn.commit()
    finally:
        conn.close()

    listing = get_registered_article_listing()

    assert [row["article_id"] for row in listing["rows"]] == ["99999", "12345"]
    assert listing["sort_by"] == "created_at"
    assert listing["sort_dir"] == "desc"


def test_get_registered_article_listing_filters_sorts_and_pages(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        for article_id, title in (("200", "Beta"), ("100", "Alpha")):
            register_target(
                conn,
                article_id,
                "a",
                f"https://dic.nicovideo.jp/a/{article_id}",
            )
            save_to_db(
                conn,
                article_id,
                "a",
                title,
                f"https://dic.nicovideo.jp/a/{article_id}",
                [{"res_no": 1, "content": title, "content_html": f"<p>{title}</p>"}],
            )
        conn.commit()
    finally:
        conn.close()

    listing = get_registered_article_listing(
        query="a",
        sort_by="title",
        sort_dir="asc",
        page=1,
        per_page=100,
    )

    assert [row["title"] for row in listing["rows"]] == ["Alpha", "Beta"]
    csv_result = get_registered_articles_csv(
        query="a",
        sort_by="title",
        sort_dir="asc",
        page=1,
        per_page=100,
    )
    assert "Title,Article ID,Type,Canonical URL" in csv_result["content"]


def test_list_registered_articles_marks_pending_first_scrape_rows(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            "pending-title",
            "a",
            "https://dic.nicovideo.jp/a/pending-title",
        )
        conn.commit()
    finally:
        conn.close()

    row = list_registered_articles()[0]
    assert row["article_id_display"] == ""
    assert row["is_pending_initial_scrape"] is True


def test_get_saved_article_export_uses_human_facing_identity_contract_for_slug(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF",
            "a",
            "https://dic.nicovideo.jp/a/%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF",
        )
        save_to_db(
            conn,
            "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF",
            "a",
            "たつきショック",
            "https://dic.nicovideo.jp/a/%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF",
            [],
        )
    finally:
        conn.close()

    result = get_saved_article_export(
        "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF",
        "a",
        "txt",
    )

    assert "Article ID: unavailable" in result["content"]
    assert "Storage Key: たつきショック" in result["content"]
    assert result["filename"].endswith(".txt")
    assert "%E3%81%9F" not in result["filename"]


def test_list_registered_articles_returns_empty_list_when_no_db(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    # No data/ dir created — DB does not exist
    articles = list_registered_articles()
    assert articles == []


def test_write_scrape_targets_txt_creates_artifact(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    write_scrape_targets_txt(data_dir=str(tmp_path / "data"))

    artifact = tmp_path / "data" / "scrape_targets.txt"
    assert artifact.is_file()
    content = artifact.read_text(encoding="utf-8")
    assert "scrape_targets:" in content
    assert "First Title" in content
    assert "responses=1" in content
    assert "max_res_no=1" in content


def test_write_scrape_targets_txt_empty_when_no_db(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    data_dir = tmp_path / "data"

    write_scrape_targets_txt(data_dir=str(data_dir))

    artifact = data_dir / "scrape_targets.txt"
    assert artifact.is_file()
    content = artifact.read_text(encoding="utf-8")
    assert "count: 0" in content
