import sqlite3

from unittest.mock import patch

import cli as cli_module


def _init_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
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
    cur.execute(
        """
        CREATE TABLE responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id TEXT NOT NULL,
            article_type TEXT NOT NULL,
            res_no INTEGER NOT NULL,
            id_hash TEXT,
            poster_name TEXT,
            posted_at TEXT,
            content_html TEXT,
            content_text TEXT,
            scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(article_id, article_type, res_no)
        )
        """
    )
    conn.commit()


def test_export_article_txt_renders_article_and_responses(capsys):
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO articles (
            article_id, article_type, title, canonical_url, created_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        ("12345", "a", "Title", "https://dic.nicovideo.jp/a/12345", "2026-01-01"),
    )
    cur.execute(
        """
        INSERT INTO responses (article_id, article_type, res_no, id_hash, poster_name,
                               posted_at, content_html, content_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "12345",
            "a",
            1,
            "abc",
            "name",
            "2026-01-01",
            "<div>html</div>",
            "text",
        ),
    )
    conn.commit()

    with patch("cli.sqlite3.connect", return_value=conn):
        ok = cli_module.export_article("12345", "a", "txt")

    assert ok is True
    out = capsys.readouterr().out
    assert "=== ARTICLE ===" in out
    assert "ID: 12345" in out
    assert "Type: a" in out
    assert "Title: Title" in out
    assert "=== RESPONSES ===" in out
    assert ">1 name 2026-01-01 ID: abc" in out
    assert "text" in out
    assert "[html]" in out
    assert "<div>html</div>" in out


def test_export_article_md_renders_markdown(capsys):
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO articles (
            article_id, article_type, title, canonical_url, created_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        ("12345", "a", "Title", "https://dic.nicovideo.jp/a/12345", "2026-01-01"),
    )
    cur.execute(
        """
        INSERT INTO responses (
            article_id, article_type, res_no, content_html, content_text
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        ("12345", "a", 1, "<div>html</div>", "text"),
    )
    conn.commit()

    with patch("cli.sqlite3.connect", return_value=conn):
        ok = cli_module.export_article("12345", "a", "md")

    assert ok is True
    out = capsys.readouterr().out
    assert out.startswith("# Title")
    assert "## Responses" in out
    assert "```html" in out
    assert "<div>html</div>" in out


def test_export_article_missing_returns_false_and_prints(capsys):
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)

    with patch("cli.sqlite3.connect", return_value=conn):
        ok = cli_module.export_article("nope", "a", "txt")

    assert ok is False
    out = capsys.readouterr().out
    assert "Article not found in DB" in out


def test_export_article_unsupported_format_returns_false(capsys):
    ok = cli_module.export_article("12345", "a", "csv")
    assert ok is False
    out = capsys.readouterr().out
    assert "Unsupported format" in out
