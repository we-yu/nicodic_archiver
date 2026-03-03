"""SQLite persistence layer for nicodic_archiver."""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator


@contextmanager
def _connect(db_path: str) -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: str) -> None:
    """Create database tables if they do not exist.

    Tables:
        ``responses`` – one row per BBS response.
        ``scrape_state`` – tracks the highest response number fetched per article.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS responses (
                article     TEXT    NOT NULL,
                no          INTEGER NOT NULL,
                user_id     TEXT,
                body        TEXT,
                date        TEXT,
                raw_json    TEXT,
                PRIMARY KEY (article, no)
            );

            CREATE TABLE IF NOT EXISTS scrape_state (
                article         TEXT    PRIMARY KEY,
                last_no         INTEGER NOT NULL DEFAULT 0,
                last_scraped_at TEXT
            );
            """
        )


def upsert_responses(
    db_path: str, article_slug: str, responses: list[dict]
) -> int:
    """Insert or replace response rows.

    Args:
        db_path: Path to the SQLite file.
        article_slug: Article identifier.
        responses: List of response dicts from the API.

    Returns:
        Number of rows written.
    """
    if not responses:
        return 0

    rows = [
        (
            article_slug,
            r["no"],
            r.get("userId"),
            r.get("body"),
            r.get("date"),
            json.dumps(r, ensure_ascii=False),
        )
        for r in responses
    ]

    with _connect(db_path) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO responses (article, no, user_id, body, date, raw_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    return len(rows)


def update_scrape_state(db_path: str, article_slug: str, last_no: int) -> None:
    """Record the highest response number fetched for *article_slug*."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO scrape_state (article, last_no, last_scraped_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(article) DO UPDATE SET
                last_no         = excluded.last_no,
                last_scraped_at = excluded.last_scraped_at
            """,
            (article_slug, last_no),
        )


def get_last_no(db_path: str, article_slug: str) -> int:
    """Return the highest response number stored for *article_slug* (0 if none)."""
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT last_no FROM scrape_state WHERE article = ?",
            (article_slug,),
        ).fetchone()
    return int(row["last_no"]) if row else 0


def get_responses(db_path: str, article_slug: str) -> list[dict]:
    """Return all stored responses for *article_slug* ordered by number."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT no, user_id, body, date, raw_json FROM responses "
            "WHERE article = ? ORDER BY no ASC",
            (article_slug,),
        ).fetchall()
    return [dict(r) for r in rows]


def list_articles(db_path: str) -> list[str]:
    """Return all article slugs that have stored responses."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT article FROM responses ORDER BY article"
        ).fetchall()
    return [r["article"] for r in rows]
