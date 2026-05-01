import csv
from io import StringIO
import sqlite3
from unittest.mock import patch

from archive_read import (
    REGISTERED_ARTICLE_COLUMNS,
    export_registered_articles_csv,
    get_saved_article_export,
    get_saved_article_summary,
    get_saved_article_summary_by_exact_title,
    get_saved_article_summary_by_id,
    get_saved_article_txt,
    has_saved_article,
    list_registered_articles,
    query_registered_articles,
    write_scrape_targets_txt,
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
            "article_title": "First Title",
            "article_url": "https://dic.nicovideo.jp/a/12345",
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
    assert row["article_type"] == "a"
    assert row["title"] == "First Title"
    assert row["canonical_url"] == "https://dic.nicovideo.jp/a/12345"
    assert row["saved_response_count"] == 1
    assert row["latest_scraped_max_res_no"] == 1
    assert "last_scraped_at" in row
    assert row["article_id"] == "12345"
    assert "created_at" in row


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


def test_query_registered_articles_returns_paginated_result(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = query_registered_articles(per_page=100, page=1)

    assert result["total"] == 1
    assert result["page"] == 1
    assert result["per_page"] == 100
    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["article_id"] == "12345"
    assert row["article_type"] == "a"
    assert row["title"] == "First Title"
    assert row["saved_response_count"] == 1


def test_query_registered_articles_default_sort_is_created_at_desc(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = query_registered_articles()

    assert result["total"] >= 1
    assert result["rows"][0]["article_id"] == "12345"


def test_query_registered_articles_filters_by_search_title(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = query_registered_articles(search="First")

    assert result["total"] == 1
    assert result["rows"][0]["title"] == "First Title"


def test_query_registered_articles_filters_by_search_article_id(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = query_registered_articles(search="12345")

    assert result["total"] == 1
    assert result["rows"][0]["article_id"] == "12345"


def test_query_registered_articles_no_match_returns_empty(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = query_registered_articles(search="zzz_no_match_xyz")

    assert result["total"] == 0
    assert result["rows"] == []


def test_query_registered_articles_paginate_false_returns_all(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = query_registered_articles(paginate=False)

    assert result["total"] == 1
    assert len(result["rows"]) == 1


def test_query_registered_articles_invalid_sort_falls_back(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = query_registered_articles(sort_by="__invalid__")

    assert result["total"] == 1


def test_query_registered_articles_no_db_returns_empty(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)

    result = query_registered_articles()

    assert result["rows"] == []
    assert result["total"] == 0


def test_registered_article_columns_keys_are_consistent():
    keys = [col["key"] for col in REGISTERED_ARTICLE_COLUMNS]
    assert "article_id" in keys
    assert "article_type" in keys
    assert "title" in keys
    assert "canonical_url" in keys
    assert "created_at" in keys
    assert "saved_response_count" in keys
    assert "last_scraped_at" in keys
    headers = [col["csv_header"] for col in REGISTERED_ARTICLE_COLUMNS]
    assert len(headers) == len(set(headers)), "csv_header values must be unique"


def test_export_registered_articles_csv_returns_csv_with_header(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    csv_text = export_registered_articles_csv()

    reader = list(csv.DictReader(StringIO(csv_text)))
    assert len(reader) == 1
    assert reader[0]["article_id"] == "12345"
    assert reader[0]["title"] == "First Title"
    assert reader[0]["article_type"] == "a"


def test_export_registered_articles_csv_no_db_returns_header_only(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)

    csv_text = export_registered_articles_csv()

    lines = csv_text.strip().splitlines()
    assert len(lines) == 1
    assert "article_id" in lines[0]


def test_txt_archive_decodes_url_encoded_article_id(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    encoded_id = "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF"
    try:
        # Legacy / regression simulation: older DBs may have url-encoded
        # slug values stored as articles.article_id for article_type='a'.
        # New saves must reject that at the save boundary, so we seed rows
        # directly here to keep read behavior covered.
        canonical_url = (
            "https://dic.nicovideo.jp/a/%E3%81%9F%E3%81%A4"
            "%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF"
        )
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO articles (article_id, article_type, title, canonical_url)
            VALUES (?, ?, ?, ?)
            """,
            (encoded_id, "a", "たつきショック", canonical_url),
        )
        cur.execute(
            """
            INSERT INTO responses
            (article_id, article_type, res_no, content_text)
            VALUES (?, ?, ?, ?)
            """,
            (encoded_id, "a", 1, "test"),
        )
        conn.commit()
    finally:
        conn.close()

    from archive_read import get_saved_article_export
    result = get_saved_article_export(encoded_id, "a", "txt")

    assert result["found"]
    assert "ID: たつきショック" in result["content"]
    assert f"ID: {encoded_id}" not in result["content"]


# --- Registered Articles: URL-encoded article_id display decoding ---

_ENCODED_JP = (
    "%E3%81%82%E3%81%8B%E3%82%8A%E5%85%88%E7%94%9F"
)
_DECODED_JP = "あかり先生"


def _seed_encoded_article(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        # Seed a legacy slug-identity row directly (save_to_db rejects now).
        canonical_url = f"https://dic.nicovideo.jp/a/{_ENCODED_JP}"
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO articles (article_id, article_type, title, canonical_url)
            VALUES (?, ?, ?, ?)
            """,
            (_ENCODED_JP, "a", _DECODED_JP, canonical_url),
        )
        cur.execute(
            """
            INSERT INTO responses
            (article_id, article_type, res_no, id_hash, poster_name, posted_at,
             content_text, content_html)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _ENCODED_JP,
                "a",
                1,
                "x1",
                "Bob",
                "2025-06-01 00:00",
                "hello",
                "<p>hello</p>",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_list_registered_articles_decodes_url_encoded_article_id(
    tmp_path,
    monkeypatch,
):
    _seed_encoded_article(tmp_path, monkeypatch)

    articles = list_registered_articles()

    assert len(articles) == 1
    assert articles[0]["article_id"] == _DECODED_JP
    assert articles[0]["canonical_url"] == (
        f"https://dic.nicovideo.jp/a/{_ENCODED_JP}"
    )


def test_query_registered_articles_decodes_url_encoded_article_id(
    tmp_path,
    monkeypatch,
):
    _seed_encoded_article(tmp_path, monkeypatch)

    result = query_registered_articles()

    assert result["total"] == 1
    assert result["rows"][0]["article_id"] == _DECODED_JP
    assert result["rows"][0]["canonical_url"] == (
        f"https://dic.nicovideo.jp/a/{_ENCODED_JP}"
    )


def test_query_registered_articles_search_by_raw_encoded_id_still_works(
    tmp_path,
    monkeypatch,
):
    _seed_encoded_article(tmp_path, monkeypatch)

    result = query_registered_articles(search=_ENCODED_JP)

    assert result["total"] == 1
    assert result["rows"][0]["article_id"] == _DECODED_JP


def test_export_registered_articles_csv_decodes_url_encoded_article_id(
    tmp_path,
    monkeypatch,
):
    _seed_encoded_article(tmp_path, monkeypatch)

    csv_text = export_registered_articles_csv()

    reader = list(csv.DictReader(StringIO(csv_text)))
    assert len(reader) == 1
    assert reader[0]["article_id"] == _DECODED_JP
    assert reader[0]["canonical_url"] == (
        f"https://dic.nicovideo.jp/a/{_ENCODED_JP}"
    )
