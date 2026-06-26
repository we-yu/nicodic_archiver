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
import archive_read
from storage import init_db, open_readonly_db, register_target, save_to_db


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
        conn.execute(
            """
            UPDATE target
            SET created_at=?
            WHERE article_id=? AND article_type=?
            """,
            ("2026-01-01T00:00:00+00:00", "12345", "a"),
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


def _seed_target_slug_saved_numeric_article(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    encoded_slug = "%E3%83%86%E3%82%B9%E3%83%88%E8%A8%98%E4%BA%8B"
    canonical_url = f"https://dic.nicovideo.jp/a/{encoded_slug}"
    try:
        register_target(conn, "4470620", "a", canonical_url)
        conn.execute(
            """
            UPDATE target
            SET created_at=?
            WHERE article_id=? AND article_type=?
            """,
            ("2026-04-01T00:00:00+00:00", "4470620", "a"),
        )
        save_to_db(
            conn,
            "4470620",
            "a",
            "Saved Numeric Title",
            canonical_url,
            [
                {
                    "res_no": 1,
                    "id_hash": "abc123",
                    "poster_name": "Alice",
                    "posted_at": "2025-01-01 00:00",
                    "content": "First response",
                    "content_html": "<p>First response</p>",
                },
                {
                    "res_no": 2,
                    "id_hash": "def456",
                    "poster_name": "Bob",
                    "posted_at": "2025-01-01 00:05",
                    "content": "Second response",
                    "content_html": "<p>Second response</p>",
                },
            ],
            latest_scraped_at="2026-04-02T00:00:00+00:00",
        )
    finally:
        conn.close()

    return {
        "encoded_slug": encoded_slug,
        "decoded_slug": "テスト記事",
        "canonical_url": canonical_url,
        "saved_article_id": "4470620",
    }


def _seed_pending_target(
    tmp_path,
    monkeypatch,
    article_id="8880001",
    article_type="a",
    canonical_url="https://dic.nicovideo.jp/a/pending-slug",
    title="Pending Display Title",
    created_at="2026-02-01T00:00:00+00:00",
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            article_id,
            article_type,
            canonical_url,
            title=title,
        )
        conn.execute(
            """
            UPDATE target
            SET created_at=?
            WHERE article_id=? AND article_type=?
            """,
            (created_at, article_id, article_type),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_checked_zero_board(
    tmp_path,
    monkeypatch,
    *,
    article_id="7711002",
    latest_scraped_at="2026-06-07T08:09:10+00:00",
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            article_id,
            "a",
            f"https://dic.nicovideo.jp/a/{article_id}",
            title="Checked Zero Posts",
        )
        conn.execute(
            """
            UPDATE target
            SET created_at=?
            WHERE article_id=? AND article_type=?
            """,
            ("2026-05-01T01:02:03+00:00", article_id, "a"),
        )
        save_to_db(
            conn,
            article_id,
            "a",
            "Checked Zero Posts",
            f"https://dic.nicovideo.jp/a/{article_id}",
            [],
            latest_scraped_at=latest_scraped_at,
        )
    finally:
        conn.close()


def _seed_registered_sort_case(
    tmp_path,
    monkeypatch,
    article_id,
    *,
    created_at,
    response_numbers=None,
    latest_scraped_at=None,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            article_id,
            "a",
            f"https://dic.nicovideo.jp/a/{article_id}",
            title=f"Title {article_id}",
        )
        conn.execute(
            """
            UPDATE target
            SET created_at=?
            WHERE article_id=? AND article_type=?
            """,
            (created_at, article_id, "a"),
        )
        conn.commit()
        if response_numbers:
            save_to_db(
                conn,
                article_id,
                "a",
                f"Title {article_id}",
                f"https://dic.nicovideo.jp/a/{article_id}",
                [
                    {
                        "res_no": res_no,
                        "id_hash": f"id{res_no}",
                        "poster_name": f"Poster {res_no}",
                        "posted_at": "2025-01-01 00:00",
                        "content": f"Response {res_no}",
                        "content_html": f"<p>Response {res_no}</p>",
                    }
                    for res_no in response_numbers
                ],
                latest_scraped_at=latest_scraped_at,
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

    with patch("storage.init_db") as mock_init:
        with patch(
            "archive_read.open_readonly_db",
            wraps=open_readonly_db,
        ) as mock_ro:
            result = get_saved_article_summary("12345", "a")

    mock_init.assert_not_called()
    mock_ro.assert_called_once()
    assert result["found"] is True


def test_open_readonly_db_does_not_create_missing_db(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    conn = open_readonly_db()

    assert conn is None
    assert not (tmp_path / "data" / "nicodic.db").exists()


def test_open_readonly_db_opens_existing_db_read_only(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _seed_archive(tmp_path, monkeypatch)

    conn = open_readonly_db()
    assert conn is not None
    try:
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 1
    finally:
        conn.close()


def test_registered_articles_column_label_saved_max_res_no():
    from archive_read import REGISTERED_ARTICLE_COLUMNS

    col = next(c for c in REGISTERED_ARTICLE_COLUMNS if c["key"] == "saved_max_res_no")
    assert col["label"] == "Saved Max Res No"


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
    assert row["saved_max_res_no"] == 1
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
    assert "saved_max_res_no=1" in content


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


def test_query_registered_articles_includes_active_pending_targets(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="8880001",
        canonical_url="https://dic.nicovideo.jp/a/pending-slug",
        created_at="2026-02-02T00:00:00+00:00",
    )

    result = query_registered_articles()

    assert result["total"] == 2
    assert result["rows"][0]["article_id"] == "8880001"
    assert result["rows"][0]["title"] == "Pending Display Title"
    assert result["rows"][0]["saved_response_count"] == 0
    assert result["rows"][0]["saved_max_res_no"] is None
    assert result["rows"][0]["last_scraped_at"] is None


def test_query_registered_completed_zero_board_shows_checked_state(
    tmp_path,
    monkeypatch,
):
    _seed_checked_zero_board(tmp_path, monkeypatch)

    result = query_registered_articles(search="7711002", paginate=False)

    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["saved_response_count"] == 0
    assert row["saved_max_res_no"] == 0
    assert row["last_scraped_at"] == "2026-06-07T08:09:10+00:00"


def test_query_registered_followup_empty_scrape_keeps_prior_responses(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    aid = "3322110"
    url = f"https://dic.nicovideo.jp/a/{aid}"
    conn = init_db()
    try:
        register_target(conn, aid, "a", url)
        responses = [
            {
                "res_no": 1,
                "id_hash": "h1",
                "poster_name": "A",
                "posted_at": "2025-01-01",
                "content": "c1",
                "content_html": "<p>c1</p>",
            },
            {
                "res_no": 2,
                "id_hash": "h2",
                "poster_name": "B",
                "posted_at": "2025-01-02",
                "content": "c2",
                "content_html": "<p>c2</p>",
            },
        ]
        save_to_db(
            conn,
            aid,
            "a",
            "Kept Rows",
            url,
            responses,
            latest_scraped_at="2026-01-01T01:01:01+00:00",
        )
        save_to_db(
            conn,
            aid,
            "a",
            "Kept Rows",
            url,
            [],
            latest_scraped_at="2026-06-06T06:06:06+00:00",
        )
    finally:
        conn.close()

    result = query_registered_articles(search=aid, paginate=False)
    row = result["rows"][0]
    assert row["saved_response_count"] == 2
    assert row["saved_max_res_no"] == 2
    assert row["last_scraped_at"] == "2026-06-06T06:06:06+00:00"


def test_query_registered_articles_search_finds_pending_target_by_article_id(
    tmp_path,
    monkeypatch,
):
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="8880001",
        canonical_url="https://dic.nicovideo.jp/a/pending-slug",
    )

    result = query_registered_articles(search="8880001")

    assert result["total"] == 1
    assert result["rows"][0]["article_id"] == "8880001"


def test_query_registered_articles_search_does_not_match_canonical_url_only(
    tmp_path,
    monkeypatch,
):
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        canonical_url="https://dic.nicovideo.jp/a/pending-slug",
    )

    result = query_registered_articles(search="dic.nicovideo.jp/a/pending-slug")

    assert result["total"] == 0


def test_query_registered_articles_uses_target_created_at_for_default_order(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="8880003",
        title="Older Pending",
        canonical_url="https://dic.nicovideo.jp/a/older-pending",
        created_at="2026-01-01T00:00:00+00:00",
    )

    conn = init_db()
    try:
        conn.execute(
            """
            UPDATE target
            SET created_at=?
            WHERE article_id=? AND article_type=?
            """,
            ("2026-03-01T00:00:00+00:00", "12345", "a"),
        )
        conn.commit()
    finally:
        conn.close()

    result = query_registered_articles()

    assert [row["article_id"] for row in result["rows"]] == [
        "12345",
        "8880003",
    ]


def test_query_registered_articles_matches_saved_numeric_article_by_canonical_url(
    tmp_path,
    monkeypatch,
):
    seeded = _seed_target_slug_saved_numeric_article(tmp_path, monkeypatch)

    result = query_registered_articles()

    assert result["total"] == 1
    row = result["rows"][0]
    assert row["article_id"] == "4470620"
    assert row["article_type"] == "a"
    assert row["title"] == "Saved Numeric Title"
    assert row["canonical_url"] == seeded["canonical_url"]
    assert row["saved_response_count"] == 2
    assert row["saved_max_res_no"] == 2
    assert row["last_scraped_at"] == "2026-04-02T00:00:00+00:00"


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


def test_query_registered_articles_search_matches_visible_title_and_article_id(
    tmp_path,
    monkeypatch,
):
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="991100",
        title="Pending Display Title",
        canonical_url="https://dic.nicovideo.jp/a/pending-a",
    )
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="880001",
        title="Title 99 Visible",
        canonical_url="https://dic.nicovideo.jp/a/pending-b",
        created_at="2026-02-02T00:00:00+00:00",
    )

    result = query_registered_articles(search="99", paginate=False)

    assert [row["article_id"] for row in result["rows"]] == [
        "880001",
        "991100",
    ]


def test_query_registered_articles_search_matches_visible_japanese_title(
    tmp_path,
    monkeypatch,
):
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="771100",
        title="連合艦隊これくしょん",
        canonical_url="https://dic.nicovideo.jp/a/hidden-encoded-match",
    )
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="771101",
        title="連合船隊これくしょん",
        canonical_url="https://dic.nicovideo.jp/a/%E8%89%A6-only-hit",
        created_at="2026-02-02T00:00:00+00:00",
    )

    result = query_registered_articles(search="艦", paginate=False)

    assert [row["article_id"] for row in result["rows"]] == ["771100"]


def test_query_registered_articles_search_escapes_like_wildcards(
    tmp_path,
    monkeypatch,
):
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="wild-1",
        article_type="id",
        title="Rate 100%_艦 Visible",
        canonical_url="https://dic.nicovideo.jp/id/wild-1",
    )
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="plain-2",
        article_type="id",
        title="Rate 100X艦 Visible",
        canonical_url="https://dic.nicovideo.jp/id/plain-2",
        created_at="2026-02-02T00:00:00+00:00",
    )

    percent_result = query_registered_articles(search="%", paginate=False)
    underscore_result = query_registered_articles(search="_", paginate=False)

    assert [row["article_id"] for row in percent_result["rows"]] == ["wild-1"]
    assert [row["article_id"] for row in underscore_result["rows"]] == ["wild-1"]


def test_query_registered_articles_sorts_article_id_numerically(
    tmp_path,
    monkeypatch,
):
    for article_id in ("1", "2", "10", "100"):
        _seed_registered_sort_case(
            tmp_path,
            monkeypatch,
            article_id,
            created_at="2026-01-01T00:00:00+00:00",
        )

    result = query_registered_articles(
        sort_by="article_id",
        sort_order="asc",
        paginate=False,
    )

    assert [row["article_id"] for row in result["rows"]] == [
        "1",
        "2",
        "10",
        "100",
    ]


def test_query_registered_articles_sorts_saved_response_count_numerically(
    tmp_path,
    monkeypatch,
):
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "11",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=[1, 2],
        latest_scraped_at="2026-01-01T00:00:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "12",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=list(range(1, 11)),
        latest_scraped_at="2026-01-01T00:00:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "13",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=[1],
        latest_scraped_at="2026-01-01T00:00:00+00:00",
    )

    result = query_registered_articles(
        sort_by="saved_response_count",
        sort_order="asc",
        paginate=False,
    )

    assert [row["article_id"] for row in result["rows"]] == [
        "13",
        "11",
        "12",
    ]


def test_query_registered_articles_sorts_max_res_no_numerically(
    tmp_path,
    monkeypatch,
):
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "21",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=[2],
        latest_scraped_at="2026-01-01T00:00:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "22",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=[10],
        latest_scraped_at="2026-01-01T00:00:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "23",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=[100],
        latest_scraped_at="2026-01-01T00:00:00+00:00",
    )

    result = query_registered_articles(
        sort_by="saved_max_res_no",
        sort_order="asc",
        paginate=False,
    )

    assert [row["article_id"] for row in result["rows"]] == [
        "21",
        "22",
        "23",
    ]


def test_query_registered_articles_sorts_created_at_as_datetime(
    tmp_path,
    monkeypatch,
):
    # Both timestamps are UTC so text-sort and datetime-sort agree.
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "31",
        created_at="2025-12-31T15:30:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "32",
        created_at="2025-12-31T23:00:00+00:00",
    )

    result = query_registered_articles(
        sort_by="created_at",
        sort_order="desc",
        paginate=False,
    )

    assert [row["article_id"] for row in result["rows"]] == ["32", "31"]


def test_query_registered_articles_sorts_last_scraped_as_datetime(
    tmp_path,
    monkeypatch,
):
    # Both scraped timestamps are UTC so text-sort and datetime-sort agree.
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "41",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=[1],
        latest_scraped_at="2025-12-31T15:30:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "42",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=[1],
        latest_scraped_at="2025-12-31T23:00:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "43",
        created_at="2026-01-01T00:00:00+00:00",
    )

    result = query_registered_articles(
        sort_by="last_scraped_at",
        sort_order="desc",
        paginate=False,
    )

    assert [row["article_id"] for row in result["rows"]] == [
        "42",
        "41",
        "43",
    ]


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


def test_query_registered_fast_path_fetches_stats_only_for_page_identities(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="8880002",
        canonical_url="https://dic.nicovideo.jp/a/pending-two",
        created_at="2026-02-02T00:00:00+00:00",
    )

    captured = []
    real_fetch = archive_read._registered_fetch_response_stats

    def spy_fetch(conn, identities):
        captured.append(list(identities))
        return real_fetch(conn, identities)

    with patch(
        "archive_read._registered_fetch_response_stats",
        side_effect=spy_fetch,
    ):
        query_registered_articles(
            sort_by="created_at",
            per_page=1,
            page=1,
        )

    assert len(captured) == 1
    assert len(captured[0]) == 1


def test_query_registered_fast_path_page_choice_ignores_off_page_responses(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        register_target(
            conn,
            "100",
            "a",
            "https://dic.nicovideo.jp/a/100",
            title="Old Many",
        )
        conn.execute(
            """
            UPDATE target
            SET created_at=?
            WHERE article_id=? AND article_type=?
            """,
            ("2026-01-01T00:00:00+00:00", "100", "a"),
        )
        save_to_db(
            conn,
            "100",
            "a",
            "Old Many",
            "https://dic.nicovideo.jp/a/100",
            [
                {
                    "res_no": res_no,
                    "id_hash": f"h{res_no}",
                    "poster_name": "A",
                    "posted_at": "2025-01-01",
                    "content": f"c{res_no}",
                    "content_html": f"<p>c{res_no}</p>",
                }
                for res_no in range(1, 51)
            ],
            latest_scraped_at="2026-01-02T00:00:00+00:00",
        )
        register_target(
            conn,
            "200",
            "a",
            "https://dic.nicovideo.jp/a/200",
            title="New Empty",
        )
        conn.execute(
            """
            UPDATE target
            SET created_at=?
            WHERE article_id=? AND article_type=?
            """,
            ("2026-06-01T00:00:00+00:00", "200", "a"),
        )
    finally:
        conn.close()

    result = query_registered_articles(
        sort_by="created_at",
        sort_order="desc",
        per_page=1,
        page=1,
    )

    assert result["rows"][0]["article_id"] == "200"
    assert result["rows"][0]["saved_response_count"] == 0
    assert result["rows"][0]["saved_max_res_no"] is None


def test_query_registered_fast_path_includes_page_response_stats(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = query_registered_articles(
        sort_by="created_at",
        sort_order="desc",
        per_page=1,
        page=1,
    )

    row = result["rows"][0]
    assert row["saved_response_count"] == 1
    assert row["saved_max_res_no"] == 1


def test_query_registered_legacy_sort_alias_maps_to_saved_max_res_no(
    tmp_path,
    monkeypatch,
):
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "21",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=[2],
        latest_scraped_at="2026-01-01T00:00:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "22",
        created_at="2026-01-01T00:00:00+00:00",
        response_numbers=[10],
        latest_scraped_at="2026-01-01T00:00:00+00:00",
    )

    legacy = query_registered_articles(
        sort_by="latest_scraped_max_res_no",
        sort_order="asc",
        paginate=False,
    )
    current = query_registered_articles(
        sort_by="saved_max_res_no",
        sort_order="asc",
        paginate=False,
    )

    assert [row["article_id"] for row in legacy["rows"]] == [
        row["article_id"] for row in current["rows"]
    ]


def test_query_registered_fast_path_reads_display_stats_from_summary(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    conn = init_db()
    try:
        conn.execute(
            """
            UPDATE article_response_stats
            SET saved_response_count=99, saved_max_res_no=99
            WHERE article_id=? AND article_type=?
            """,
            ("12345", "a"),
        )
        conn.commit()
    finally:
        conn.close()

    result = query_registered_articles(sort_by="created_at")

    row = result["rows"][0]
    assert row["saved_response_count"] == 99
    assert row["saved_max_res_no"] == 99


def test_query_registered_aggregate_sort_reads_stats_from_summary(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    conn = init_db()
    try:
        conn.execute(
            """
            UPDATE article_response_stats
            SET saved_response_count=77, saved_max_res_no=88
            WHERE article_id=? AND article_type=?
            """,
            ("12345", "a"),
        )
        conn.commit()
    finally:
        conn.close()

    count_sorted = query_registered_articles(
        sort_by="saved_response_count",
        paginate=False,
    )
    max_sorted = query_registered_articles(
        sort_by="saved_max_res_no",
        paginate=False,
    )

    assert count_sorted["rows"][0]["saved_response_count"] == 77
    assert max_sorted["rows"][0]["saved_max_res_no"] == 88


def test_query_registered_fast_path_falls_back_when_summary_row_missing(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    conn = init_db()
    try:
        conn.execute("DELETE FROM article_response_stats")
        conn.commit()
    finally:
        conn.close()

    result = query_registered_articles(sort_by="created_at")

    row = result["rows"][0]
    assert row["saved_response_count"] == 1
    assert row["saved_max_res_no"] == 1


def test_query_registered_does_not_crash_when_summary_table_absent(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    conn = init_db()
    try:
        conn.execute("DROP TABLE article_response_stats")
        conn.commit()
    finally:
        conn.close()

    fast = query_registered_articles(sort_by="created_at")
    aggregate = query_registered_articles(sort_by="saved_response_count")

    assert fast["rows"][0]["saved_response_count"] == 1
    assert fast["rows"][0]["saved_max_res_no"] == 1
    assert aggregate["rows"][0]["saved_response_count"] == 1


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


def test_export_registered_articles_csv_includes_pending_target_rows(
    tmp_path,
    monkeypatch,
):
    _seed_pending_target(
        tmp_path,
        monkeypatch,
        article_id="8899001",
        title="Pending CSV Row Title",
        canonical_url="https://dic.nicovideo.jp/a/pending-for-csv",
    )

    csv_text = export_registered_articles_csv()

    reader = list(csv.DictReader(StringIO(csv_text)))
    assert len(reader) == 1
    assert reader[0]["article_id"] == "8899001"
    assert reader[0]["title"] == "Pending CSV Row Title"
    assert reader[0]["saved_response_count"] == "0"


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
        canonical_url = f"https://dic.nicovideo.jp/a/{_ENCODED_JP}"
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO target
            (article_id, article_type, canonical_url, is_active)
            VALUES (?, 'a', ?, 1)
            """,
            (_ENCODED_JP, canonical_url),
        )
        # Legacy slug target row (read-compat only); archive rows use raw SQL.
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


def _seed_encoded_pending_target(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        canonical_url = f"https://dic.nicovideo.jp/a/{_ENCODED_JP}"
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO target
            (article_id, article_type, canonical_url, is_active)
            VALUES (?, 'a', ?, 1)
            """,
            (_ENCODED_JP, canonical_url),
        )
        conn.execute(
            """
            UPDATE target
            SET created_at=?
            WHERE article_id=? AND article_type=?
            """,
            ("2026-05-01T00:00:00+00:00", _ENCODED_JP, "a"),
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


def test_query_registered_articles_search_by_raw_encoded_id_no_longer_matches(
    tmp_path,
    monkeypatch,
):
    _seed_encoded_article(tmp_path, monkeypatch)

    result = query_registered_articles(search=_ENCODED_JP)

    assert result["total"] == 0


def test_query_registered_articles_search_finds_encoded_pending_target_by_decoded_term(
    tmp_path,
    monkeypatch,
):
    _seed_encoded_pending_target(tmp_path, monkeypatch)

    result = query_registered_articles(search=_DECODED_JP, paginate=False)

    assert result["total"] == 1
    assert result["rows"][0]["article_id"] == _DECODED_JP
    assert result["rows"][0]["title"] == _DECODED_JP


def test_query_registered_articles_search_encoded_pending_by_encoded_term_no_match(
    tmp_path,
    monkeypatch,
):
    _seed_encoded_pending_target(tmp_path, monkeypatch)

    result = query_registered_articles(search=_ENCODED_JP, paginate=False)

    assert result["total"] == 0


def test_query_registered_articles_saved_title_search_still_works_with_search_variants(
    tmp_path,
    monkeypatch,
):
    _seed_archive(tmp_path, monkeypatch)

    result = query_registered_articles(search="First", paginate=False)

    assert result["total"] == 1
    assert result["rows"][0]["title"] == "First Title"


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


# --- MainTask049: index and query-shape regression guards ---


def test_query_registered_canonical_url_fallback_returns_saved_stats(
    tmp_path,
    monkeypatch,
):
    """Slug target with numeric saved article matched via canonical_url fallback."""
    seeded = _seed_target_slug_saved_numeric_article(tmp_path, monkeypatch)

    result = query_registered_articles()

    assert result["total"] == 1
    row = result["rows"][0]
    assert row["article_id"] == seeded["saved_article_id"]
    assert row["title"] == "Saved Numeric Title"
    assert row["saved_response_count"] == 2
    assert row["saved_max_res_no"] == 2


def test_query_registered_canonical_url_fallback_visible_for_all_sort_keys(
    tmp_path,
    monkeypatch,
):
    """URL fallback target appears correctly under every supported sort key."""
    _seed_target_slug_saved_numeric_article(tmp_path, monkeypatch)

    for sort_key in (
        "created_at",
        "last_scraped_at",
        "title",
        "article_id",
        "saved_response_count",
        "saved_max_res_no",
    ):
        result = query_registered_articles(sort_by=sort_key, paginate=False)
        assert result["total"] == 1, f"expected 1 row for sort_by={sort_key!r}"
        assert result["rows"][0]["saved_response_count"] == 2, (
            f"saved_response_count wrong for sort_by={sort_key!r}"
        )


def test_query_registered_default_created_at_desc_text_order(
    tmp_path,
    monkeypatch,
):
    """Default created_at DESC text sort keeps UTC ISO strings in correct order."""
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "51",
        created_at="2026-03-01T00:00:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "52",
        created_at="2026-06-01T00:00:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "53",
        created_at="2025-12-01T00:00:00+00:00",
    )

    result = query_registered_articles(
        sort_by="created_at",
        sort_order="desc",
        paginate=False,
    )

    assert [row["article_id"] for row in result["rows"]] == ["52", "51", "53"]


def test_query_registered_default_created_at_asc_text_order(
    tmp_path,
    monkeypatch,
):
    """Default created_at ASC text sort keeps UTC ISO strings in correct order."""
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "61",
        created_at="2026-06-01T00:00:00+00:00",
    )
    _seed_registered_sort_case(
        tmp_path,
        monkeypatch,
        "62",
        created_at="2025-01-01T00:00:00+00:00",
    )

    result = query_registered_articles(
        sort_by="created_at",
        sort_order="asc",
        paginate=False,
    )

    assert [row["article_id"] for row in result["rows"]] == ["62", "61"]


def test_query_registered_summary_table_used_not_responses_for_stats_sort(
    tmp_path,
    monkeypatch,
):
    """aggregate sort reads from article_response_stats, not responses directly."""
    _seed_archive(tmp_path, monkeypatch)

    conn = init_db()
    try:
        conn.execute(
            """
            UPDATE article_response_stats
            SET saved_response_count=55, saved_max_res_no=77
            WHERE article_id=? AND article_type=?
            """,
            ("12345", "a"),
        )
        conn.commit()
    finally:
        conn.close()

    count_sorted = query_registered_articles(
        sort_by="saved_response_count",
        paginate=False,
    )
    max_sorted = query_registered_articles(
        sort_by="saved_max_res_no",
        paginate=False,
    )

    assert count_sorted["rows"][0]["saved_response_count"] == 55
    assert max_sorted["rows"][0]["saved_max_res_no"] == 77


def test_query_registered_legacy_alias_gives_same_order_as_saved_max_res_no(
    tmp_path,
    monkeypatch,
):
    """latest_scraped_max_res_no alias produces the same results as saved_max_res_no."""
    for article_id, res_nos in (("71", [1, 5]), ("72", [1, 2, 3])):
        _seed_registered_sort_case(
            tmp_path,
            monkeypatch,
            article_id,
            created_at="2026-01-01T00:00:00+00:00",
            response_numbers=res_nos,
            latest_scraped_at="2026-01-02T00:00:00+00:00",
        )

    legacy = query_registered_articles(
        sort_by="latest_scraped_max_res_no",
        sort_order="desc",
        paginate=False,
    )
    direct = query_registered_articles(
        sort_by="saved_max_res_no",
        sort_order="desc",
        paginate=False,
    )

    assert [r["article_id"] for r in legacy["rows"]] == [
        r["article_id"] for r in direct["rows"]
    ]
    assert legacy["rows"][0]["saved_max_res_no"] == 5
