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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS article_request_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_url TEXT NOT NULL,
        article_id TEXT NOT NULL,
        article_type TEXT NOT NULL,
        enqueued_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(article_id, article_type)
    )
    """)

    conn.commit()
    return conn


def enqueue_article_request(canonical_target):
    """Persist one canonical article target as a minimal queue request."""

    article_url = canonical_target["article_url"]
    article_id = canonical_target["article_id"]
    article_type = canonical_target["article_type"]

    conn = init_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO article_request_queue
            (article_url, article_id, article_type)
            VALUES (?, ?, ?)
            """,
            (article_url, article_id, article_type),
        )
        inserted = cur.rowcount == 1
        conn.commit()

        cur.execute(
            """
            SELECT article_url, article_id, article_type, enqueued_at
            FROM article_request_queue
            WHERE article_id=? AND article_type=?
            """,
            (article_id, article_type),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    queued_article_url, queued_article_id, queued_article_type, enqueued_at = row

    return {
        "status": "enqueued" if inserted else "duplicate",
        "article_url": queued_article_url,
        "article_id": queued_article_id,
        "article_type": queued_article_type,
        "enqueued_at": enqueued_at,
    }


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
