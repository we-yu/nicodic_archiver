import json
import os
import sqlite3
import time


def init_db():
    """
    SQLite初期化（テーブル作成）。
    既存の場合は何もしない。
    """

    os.makedirs("data", exist_ok=True)

    conn = sqlite3.connect("data/nicodic.db")
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id TEXT NOT NULL,
        article_type TEXT NOT NULL,
        title TEXT NOT NULL,
        canonical_url TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(article_id, article_type)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS responses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id TEXT NOT NULL,
        article_type TEXT NOT NULL,
        res_no INTEGER NOT NULL,
        id_hash TEXT,
        poster_name TEXT,
        posted_at TEXT,
        content_html TEXT,
        content_text TEXT,
        res_hidden INTEGER DEFAULT 0,
        idhash_hidden INTEGER DEFAULT 0,
        good_count INTEGER,
        bad_count INTEGER,
        scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(article_id, article_type, res_no)
    )
    """)

    conn.commit()
    return conn


def get_max_saved_res_no(
    article_id: str,
    article_type: str,
    conn=None,
) -> int | None:
    """Return max res_no in DB for the article, or None if none stored."""

    def _query(c):
        cur = c.cursor()
        cur.execute(
            """
            SELECT MAX(res_no) FROM responses
            WHERE article_id=? AND article_type=?
            """,
            (article_id, article_type),
        )
        row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    if conn is not None:
        return _query(conn)

    os.makedirs("data", exist_ok=True)
    own = sqlite3.connect("data/nicodic.db")
    try:
        return _query(own)
    finally:
        own.close()


def fetch_responses_as_save_format(conn, article_id: str, article_type: str) -> list:
    """Load all stored responses for JSON export (ordered by res_no)."""

    cur = conn.cursor()
    cur.execute(
        """
        SELECT res_no, id_hash, poster_name, posted_at, content_text, content_html
        FROM responses
        WHERE article_id=? AND article_type=?
        ORDER BY res_no ASC
        """,
        (article_id, article_type),
    )
    out = []
    for row in cur.fetchall():
        res_no, id_hash, poster_name, posted_at, content_text, content_html = row
        out.append(
            {
                "res_no": res_no,
                "id_hash": id_hash,
                "poster_name": poster_name,
                "posted_at": posted_at,
                "content": content_text,
                "content_html": content_html or "",
            }
        )
    return out


def save_to_db(conn, article_id, article_type, title, article_url, responses):
    """
    記事およびレスをSQLiteへ保存。
    INSERT OR IGNORE で重複回避。
    """

    cur = conn.cursor()

    # 記事メタ保存
    cur.execute("""
        INSERT OR IGNORE INTO articles
        (article_id, article_type, title, canonical_url)
        VALUES (?, ?, ?, ?)
    """, (article_id, article_type, title, article_url))

    # レス保存
    for r in responses:
        cur.execute("""
            INSERT OR IGNORE INTO responses
            (article_id, article_type, res_no, id_hash, poster_name, posted_at,
                    content_text, content_html)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            article_id,
            article_type,
            r["res_no"],
            r.get("id_hash"),
            r.get("poster_name"),
            r.get("posted_at"),
            r.get("content"),
            r.get("content_html"),
        ))

    conn.commit()


def save_json(article_id, article_type, title, article_url, responses):
    """
    取得結果をJSONとして保存（保険用途）。
    """

    os.makedirs("data", exist_ok=True)

    safe_title = title.replace("/", "／").replace("\\", "＼")
    filename = f"{article_id}{article_type}_{safe_title}.json"
    output_path = os.path.join("data", filename)

    data = {
        "article_id": article_id,
        "article_type": article_type,
        "article_url": article_url,
        "title": title,
        "collected_at": int(time.time()),
        "response_count": len(responses),
        "responses": responses
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("Saved JSON:", output_path)
