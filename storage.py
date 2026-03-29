import json
import os
import sqlite3
import time
from pathlib import Path


DEFAULT_DB_PATH = "data/nicodic.db"


def _target_row_to_entry(row):
    return {
        "id": row[0],
        "article_id": row[1],
        "article_type": row[2],
        "canonical_url": row[3],
        "is_active": bool(row[4]),
        "created_at": row[5],
    }


def init_db(db_path: str = DEFAULT_DB_PATH):
    """
    SQLite初期化（テーブル作成）。
    既存の場合は何もしない。
    """

    db_dir = Path(db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
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
    CREATE TABLE IF NOT EXISTS queue_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id TEXT NOT NULL,
        article_type TEXT NOT NULL,
        article_url TEXT NOT NULL,
        title TEXT,
        enqueued_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(article_id, article_type)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS target (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id TEXT NOT NULL,
        article_type TEXT NOT NULL,
        canonical_url TEXT NOT NULL,
        is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(article_id, article_type)
    )
    """)

    conn.commit()
    return conn


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


def enqueue_canonical_target(conn, canonical_target, title=None):
    """
    Enqueue resolved canonical target as a minimal persistent queue request.

    canonical_target requires:
      - article_url
      - article_id
      - article_type
    """

    article_url = canonical_target["article_url"]
    article_id = canonical_target["article_id"]
    article_type = canonical_target["article_type"]

    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO queue_requests
        (article_id, article_type, article_url, title)
        VALUES (?, ?, ?, ?)
        """,
        (article_id, article_type, article_url, title),
    )
    inserted = cur.rowcount == 1
    conn.commit()

    cur.execute(
        """
        SELECT article_url, article_id, article_type, title, enqueued_at
        FROM queue_requests
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    row = cur.fetchone()
    entry = {
        "article_url": row[0],
        "article_id": row[1],
        "article_type": row[2],
        "title": row[3],
        "enqueued_at": row[4],
    }

    if inserted:
        status = "enqueued"
    else:
        status = "duplicate"

    return {
        "status": status,
        "entry": entry,
        "queue_identity": {
            "article_id": article_id,
            "article_type": article_type,
        },
    }


def list_queue_requests(conn, limit=None):
    """Load persisted queue requests in FIFO order."""

    cur = conn.cursor()
    query = """
        SELECT article_url, article_id, article_type, title, enqueued_at
        FROM queue_requests
        ORDER BY id ASC
    """
    params = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)

    cur.execute(query, params)
    rows = cur.fetchall()
    return [
        {
            "article_url": article_url,
            "article_id": article_id,
            "article_type": article_type,
            "title": title,
            "enqueued_at": enqueued_at,
        }
        for (article_url, article_id, article_type, title, enqueued_at) in rows
    ]


def dequeue_canonical_target(conn, article_id, article_type):
    """Remove one queued request by canonical target identity."""

    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM queue_requests
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    conn.commit()
    return cur.rowcount > 0


def register_target(conn, article_id, article_type, canonical_url):
    """Register or reactivate one canonical scrape target."""

    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, article_id, article_type, canonical_url, is_active, created_at
        FROM target
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    existing_row = cur.fetchone()

    if existing_row is None:
        cur.execute(
            """
            INSERT INTO target
            (article_id, article_type, canonical_url, is_active)
            VALUES (?, ?, ?, 1)
            """,
            (article_id, article_type, canonical_url),
        )
        status = "added"
    else:
        existing_entry = _target_row_to_entry(existing_row)
        status = "duplicate"
        if not existing_entry["is_active"]:
            status = "reactivated"

        cur.execute(
            """
            UPDATE target
            SET canonical_url=?, is_active=1
            WHERE article_id=? AND article_type=?
            """,
            (canonical_url, article_id, article_type),
        )

    conn.commit()

    cur.execute(
        """
        SELECT id, article_id, article_type, canonical_url, is_active, created_at
        FROM target
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    entry = _target_row_to_entry(cur.fetchone())

    return {
        "status": status,
        "entry": entry,
        "target_identity": {
            "article_id": article_id,
            "article_type": article_type,
        },
    }


def list_targets(conn, active_only=True):
    """Load registered scrape targets in insertion order."""

    cur = conn.cursor()
    query = (
        "SELECT id, article_id, article_type, canonical_url, is_active, "
        "created_at FROM target"
    )
    params = ()
    if active_only:
        query += " WHERE is_active=1"
    query += " ORDER BY id ASC"

    cur.execute(query, params)
    return [_target_row_to_entry(row) for row in cur.fetchall()]
