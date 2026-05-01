import csv
from datetime import datetime, timezone
import json
import os
import sqlite3
import time
from io import StringIO
from pathlib import Path


DEFAULT_DB_PATH = "data/nicodic.db"


def validate_saved_article_identity(article_id: str, article_type: str) -> None:
    """
    Validate saved archive identity at the persistence boundary.

    For canonical NicoNicoPedia article rows (article_type='a'), the saved
    article_id must be a non-empty digits-only string (numeric article ID),
    never a URL-encoded '/a/<slug>' value.
    """
    if article_type != "a":
        return
    if not isinstance(article_id, str):
        raise ValueError("saved article_id must be a string for article_type='a'")
    if not article_id:
        raise ValueError("saved article_id must be non-empty for article_type='a'")
    if not article_id.isdigit():
        raise ValueError(
            "saved article_id must be digits-only for article_type='a'"
        )


def _target_row_to_entry(row):
    return {
        "id": row[0],
        "article_id": row[1],
        "article_type": row[2],
        "canonical_url": row[3],
        "is_active": bool(row[4]),
        "created_at": row[5],
        "is_redirected": bool(row[6]),
        "redirect_target_url": row[7],
        "redirect_detected_at": row[8],
    }


def _list_column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cur.fetchall()}


def _ensure_target_redirect_columns(conn: sqlite3.Connection) -> None:
    column_names = _list_column_names(conn, "target")
    required_columns = {
        "is_redirected": (
            "ALTER TABLE target ADD COLUMN is_redirected "
            "INTEGER NOT NULL DEFAULT 0 CHECK (is_redirected IN (0, 1))"
        ),
        "redirect_target_url": (
            "ALTER TABLE target ADD COLUMN redirect_target_url TEXT"
        ),
        "redirect_detected_at": (
            "ALTER TABLE target ADD COLUMN redirect_detected_at TEXT"
        ),
    }

    cur = conn.cursor()
    changed = False
    for column_name, statement in required_columns.items():
        if column_name in column_names:
            continue
        cur.execute(statement)
        changed = True

    if changed:
        conn.commit()


def _ensure_article_metadata_columns(conn: sqlite3.Connection) -> None:
    column_names = _list_column_names(conn, "articles")
    required_columns = {
        "published_at": "ALTER TABLE articles ADD COLUMN published_at TEXT",
        "modified_at": "ALTER TABLE articles ADD COLUMN modified_at TEXT",
        (
            "latest_scraped_at"
        ): "ALTER TABLE articles ADD COLUMN latest_scraped_at TEXT",
    }

    cur = conn.cursor()
    changed = False
    for column_name, statement in required_columns.items():
        if column_name in column_names:
            continue
        cur.execute(statement)
        changed = True

    if changed:
        conn.commit()


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
        is_redirected INTEGER NOT NULL DEFAULT 0 CHECK (is_redirected IN (0, 1)),
        redirect_target_url TEXT,
        redirect_detected_at TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(article_id, article_type)
    )
    """)

    _ensure_article_metadata_columns(conn)
    _ensure_target_redirect_columns(conn)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrape_run_observation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        run_started_at TEXT NOT NULL,
        run_kind TEXT NOT NULL,
        skipped INTEGER NOT NULL CHECK (skipped IN (0, 1)),
        article_id TEXT NOT NULL,
        article_type TEXT NOT NULL,
        canonical_article_url TEXT,
        saved_response_count_after_run INTEGER NOT NULL,
        latest_total_response_count_ref INTEGER,
        scrape_ok INTEGER NOT NULL CHECK (scrape_ok IN (0, 1)),
        scrape_outcome TEXT NOT NULL,
        observed_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """)

    conn.commit()
    return conn


def save_to_db(
    conn,
    article_id,
    article_type,
    title,
    article_url,
    responses,
    *,
    published_at: str | None = None,
    modified_at: str | None = None,
    latest_scraped_at: str | None = None,
):
    """
    記事およびレスをSQLiteへ保存。
    INSERT OR IGNORE で重複回避。
    """

    validate_saved_article_identity(article_id, article_type)
    cur = conn.cursor()

    # 記事メタ保存
    cur.execute("""
        INSERT OR IGNORE INTO articles
        (article_id, article_type, title, canonical_url)
        VALUES (?, ?, ?, ?)
    """, (article_id, article_type, title, article_url))

    if latest_scraped_at is None:
        latest_scraped_at = datetime.now(timezone.utc).isoformat()

    cur.execute(
        """
        UPDATE articles
        SET title = ?,
            canonical_url = ?,
            published_at = COALESCE(?, published_at),
            modified_at = COALESCE(?, modified_at),
            latest_scraped_at = ?
        WHERE article_id = ? AND article_type = ?
        """,
        (
            title,
            article_url,
            published_at,
            modified_at,
            latest_scraped_at,
            article_id,
            article_type,
        ),
    )

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


def save_json(
    article_id,
    article_type,
    title,
    article_url,
    responses,
    announce=True,
):
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

    if announce:
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
        SELECT id, article_id, article_type, canonical_url, is_active,
               created_at, is_redirected, redirect_target_url,
               redirect_detected_at
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
            SET canonical_url=?,
                is_active=1,
                is_redirected=0,
                redirect_target_url=NULL,
                redirect_detected_at=NULL
            WHERE article_id=? AND article_type=?
            """,
            (canonical_url, article_id, article_type),
        )

    conn.commit()

    cur.execute(
        """
        SELECT id, article_id, article_type, canonical_url, is_active,
               created_at, is_redirected, redirect_target_url,
               redirect_detected_at
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
        "created_at, is_redirected, redirect_target_url, "
        "redirect_detected_at FROM target"
    )
    params = ()
    if active_only:
        query += " WHERE is_active=1"
    query += " ORDER BY id ASC"

    cur.execute(query, params)
    return [_target_row_to_entry(row) for row in cur.fetchall()]


def get_target(conn, article_id, article_type):
    """Load one registered target by canonical identity."""

    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, article_id, article_type, canonical_url, is_active,
               created_at, is_redirected, redirect_target_url,
               redirect_detected_at
        FROM target
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return _target_row_to_entry(row)


def set_target_active_state(conn, article_id, article_type, is_active):
    """Activate or deactivate one registered target without deleting it."""

    current_entry = get_target(conn, article_id, article_type)
    if current_entry is None:
        return {
            "found": False,
            "status": "not_found",
            "entry": None,
            "target_identity": {
                "article_id": article_id,
                "article_type": article_type,
            },
        }

    desired_state = 1 if is_active else 0
    current_state = 1 if current_entry["is_active"] else 0

    if current_state == desired_state:
        status = "unchanged"
    else:
        cur = conn.cursor()
        if is_active:
            cur.execute(
                """
                UPDATE target
                SET is_active=?,
                    is_redirected=0,
                    redirect_target_url=NULL,
                    redirect_detected_at=NULL
                WHERE article_id=? AND article_type=?
                """,
                (desired_state, article_id, article_type),
            )
        else:
            cur.execute(
                """
                UPDATE target
                SET is_active=?
                WHERE article_id=? AND article_type=?
                """,
                (desired_state, article_id, article_type),
            )
        conn.commit()
        status = "activated" if is_active else "deactivated"

    return {
        "found": True,
        "status": status,
        "entry": get_target(conn, article_id, article_type),
        "target_identity": {
            "article_id": article_id,
            "article_type": article_type,
        },
    }


def mark_target_redirected(
    conn,
    article_id,
    article_type,
    redirect_target_url,
    redirect_detected_at=None,
):
    """Persist minimal redirect state and deactivate the old scrape target."""

    current_entry = get_target(conn, article_id, article_type)
    if current_entry is None:
        return {
            "found": False,
            "status": "not_found",
            "entry": None,
            "target_identity": {
                "article_id": article_id,
                "article_type": article_type,
            },
        }

    detected_at = redirect_detected_at or datetime.now(
        timezone.utc
    ).isoformat()
    if (
        current_entry["is_redirected"]
        and current_entry["redirect_target_url"] == redirect_target_url
        and not current_entry["is_active"]
    ):
        status = "unchanged"
    else:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE target
            SET is_active=0,
                is_redirected=1,
                redirect_target_url=?,
                redirect_detected_at=?
            WHERE article_id=? AND article_type=?
            """,
            (
                redirect_target_url,
                detected_at,
                article_id,
                article_type,
            ),
        )
        conn.commit()
        status = "redirected"

    return {
        "found": True,
        "status": status,
        "entry": get_target(conn, article_id, article_type),
        "target_identity": {
            "article_id": article_id,
            "article_type": article_type,
        },
    }


_RUN_KINDS = frozenset({"batch", "periodic_batch"})
_OUTCOMES = frozenset(
    {
        "ok",
        "redirect_handoff",
        "skip_denylist",
        "fail_article_not_found",
        "fail_false",
        "fail_exception",
    }
)


def read_saved_response_observation_stats(conn, article_id, article_type):
    """Read-only counts for telemetry (OUT path; no writes)."""

    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*), MAX(res_no)
        FROM responses
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    row = cur.fetchone()
    return int(row[0]), row[1]


def append_scrape_run_observation(
    conn,
    *,
    run_id,
    run_started_at,
    run_kind,
    article_id,
    article_type,
    canonical_article_url,
    scrape_outcome,
):
    """
    IN: append one per-run per-article observation (append-only row).

    Reads current response stats from ``responses``; does not mutate them.
    """

    if run_kind not in _RUN_KINDS:
        raise ValueError(f"invalid run_kind: {run_kind!r}")
    if scrape_outcome not in _OUTCOMES:
        raise ValueError(f"invalid scrape_outcome: {scrape_outcome!r}")

    skipped = 1 if scrape_outcome == "skip_denylist" else 0
    scrape_ok = 1 if scrape_outcome in {"ok", "redirect_handoff"} else 0
    saved_n, max_res = read_saved_response_observation_stats(
        conn,
        article_id,
        article_type,
    )

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO scrape_run_observation (
            run_id,
            run_started_at,
            run_kind,
            skipped,
            article_id,
            article_type,
            canonical_article_url,
            saved_response_count_after_run,
            latest_total_response_count_ref,
            scrape_ok,
            scrape_outcome
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            run_started_at,
            run_kind,
            skipped,
            article_id,
            article_type,
            canonical_article_url,
            saved_n,
            max_res,
            scrape_ok,
            scrape_outcome,
        ),
    )
    conn.commit()


def list_scrape_run_observations(conn):
    """OUT: all observations in stable chronological order (read-only)."""

    cur = conn.cursor()
    cur.execute(
        """
        SELECT run_id, run_started_at, run_kind, skipped, article_id,
               article_type, canonical_article_url,
               saved_response_count_after_run,
               latest_total_response_count_ref, scrape_ok, scrape_outcome,
               observed_at
        FROM scrape_run_observation
        ORDER BY run_started_at ASC, id ASC
        """
    )
    rows = []
    for row in cur.fetchall():
        rows.append(
            {
                "run_id": row[0],
                "run_started_at": row[1],
                "run_kind": row[2],
                "skipped": row[3],
                "article_id": row[4],
                "article_type": row[5],
                "canonical_article_url": row[6],
                "saved_response_count_after_run": row[7],
                "latest_total_response_count_ref": row[8],
                "scrape_ok": row[9],
                "scrape_outcome": row[10],
                "observed_at": row[11],
            }
        )
    return rows


def format_run_telemetry_csv_wide(conn):
    """
    Read-only derived CSV: one row per article, repeated columns per run.

    Not a source of truth; for human inspection only.
    """

    observations = list_scrape_run_observations(conn)
    buf = StringIO()
    writer = csv.writer(buf, lineterminator="\n")

    if not observations:
        writer.writerow(
            ["article_id", "article_type", "canonical_article_url"],
        )
        return buf.getvalue()

    run_order = []
    seen_runs = set()
    for obs in observations:
        rid = obs["run_id"]
        if rid not in seen_runs:
            seen_runs.add(rid)
            run_order.append(rid)

    articles = {}
    for obs in observations:
        key = (obs["article_id"], obs["article_type"])
        if key not in articles:
            articles[key] = {
                "canonical_article_url": obs["canonical_article_url"] or "",
                "by_run": {},
            }
        articles[key]["by_run"][obs["run_id"]] = obs

    header = ["article_id", "article_type", "canonical_article_url"]
    for i in range(len(run_order)):
        prefix = f"run{i}"
        header.extend(
            [
                f"{prefix}_run_id",
                f"{prefix}_run_started_at",
                f"{prefix}_run_kind",
                f"{prefix}_saved_response_count_after_run",
                f"{prefix}_latest_total_response_count_ref",
                f"{prefix}_skipped",
                f"{prefix}_scrape_ok",
                f"{prefix}_scrape_outcome",
            ],
        )
    writer.writerow(header)

    for (article_id, article_type) in sorted(articles.keys()):
        entry = articles[(article_id, article_type)]
        row = [article_id, article_type, entry["canonical_article_url"]]
        for rid in run_order:
            obs = entry["by_run"].get(rid)
            if obs is None:
                row.extend(["", "", "", "", "", "", "", ""])
                continue
            row.extend(
                [
                    obs["run_id"],
                    obs["run_started_at"],
                    obs["run_kind"],
                    obs["saved_response_count_after_run"],
                    (
                        ""
                        if obs["latest_total_response_count_ref"] is None
                        else obs["latest_total_response_count_ref"]
                    ),
                    obs["skipped"],
                    obs["scrape_ok"],
                    obs["scrape_outcome"],
                ],
            )
        writer.writerow(row)

    return buf.getvalue()
