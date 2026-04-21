import sqlite3
import csv
from io import StringIO
from pathlib import Path

from storage import DEFAULT_DB_PATH

_RESPONSE_COLUMNS_TXT = (
    "res_no, poster_name, posted_at, id_hash, content_text"
)


def _open_archive_read_conn() -> sqlite3.Connection | None:
    db_path = Path(DEFAULT_DB_PATH)
    if not db_path.exists():
        return None
    return sqlite3.connect(str(db_path))


def _article_select_columns(conn) -> str:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(articles)")
    column_names = {row[1] for row in cur.fetchall()}

    published_expr = "published_at" if "published_at" in column_names else "NULL"
    modified_expr = "modified_at" if "modified_at" in column_names else "NULL"
    return (
        "title, canonical_url, created_at, "
        f"{published_expr} AS published_at, "
        f"{modified_expr} AS modified_at"
    )


def _title_lookup_select_columns(conn) -> str:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(articles)")
    column_names = {row[1] for row in cur.fetchall()}

    published_expr = "published_at" if "published_at" in column_names else "NULL"
    modified_expr = "modified_at" if "modified_at" in column_names else "NULL"
    return (
        "article_id, article_type, title, canonical_url, created_at, "
        f"{published_expr} AS published_at, "
        f"{modified_expr} AS modified_at"
    )


def _count_saved_responses(cur, article_id, article_type):
    cur.execute(
        """
        SELECT COUNT(*)
        FROM responses
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    return cur.fetchone()[0]


def _build_saved_article_summary(
    article_id,
    article_type,
    title,
    url,
    created_at,
    published_at,
    modified_at,
    response_count,
):
    return {
        "found": True,
        "article_id": article_id,
        "article_type": article_type,
        "title": title,
        "url": url,
        "created_at": created_at,
        "published_at": published_at,
        "modified_at": modified_at,
        "response_count": response_count,
    }


def _find_saved_article_by_title_lookup(cur, title):
    select_columns = _title_lookup_select_columns(cur.connection)
    cur.execute(
        f"""
        SELECT {select_columns}
        FROM articles
        WHERE title=?
        ORDER BY created_at ASC, article_id ASC, article_type ASC
        LIMIT 1
        """,
        (title,),
    )
    article = cur.fetchone()
    if article is not None:
        return article

    cur.execute(
        f"""
        SELECT {select_columns}
        FROM articles
        WHERE title = ? COLLATE NOCASE
        ORDER BY created_at ASC, article_id ASC, article_type ASC
        LIMIT 1
        """,
        (title,),
    )
    return cur.fetchone()


def has_saved_article(article_id, article_type):
    """Return True when the article exists in saved archive."""

    conn = _open_archive_read_conn()
    if conn is None:
        return False
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM articles
            WHERE article_id=? AND article_type=?
            LIMIT 1
            """,
            (article_id, article_type),
        )
        return cur.fetchone() is not None
    except sqlite3.OperationalError:
        return False
    finally:
        conn.close()


def read_article_archive(article_id, article_type, last_n=None):
    conn = _open_archive_read_conn()
    if conn is None:
        return None
    cur = conn.cursor()

    try:
        article_columns = _article_select_columns(conn)
        cur.execute(
            f"""
            SELECT {article_columns}
            FROM articles
            WHERE article_id=? AND article_type=?
            """,
            (article_id, article_type),
        )

        article = cur.fetchone()
        if not article:
            return None

        title, url, created_at, published_at, modified_at = article

        if last_n:
            cur.execute(
                f"""
                SELECT {_RESPONSE_COLUMNS_TXT}
                FROM responses
                WHERE article_id=? AND article_type=?
                ORDER BY res_no DESC
                LIMIT ?
                """,
                (article_id, article_type, last_n),
            )
            rows = cur.fetchall()
            rows.reverse()
        else:
            cur.execute(
                f"""
                SELECT {_RESPONSE_COLUMNS_TXT}
                FROM responses
                WHERE article_id=? AND article_type=?
                ORDER BY res_no ASC
                """,
                (article_id, article_type),
            )
            rows = cur.fetchall()
    except sqlite3.OperationalError:
        return None
    finally:
        if conn is not None:
            conn.close()

    return {
        "article_id": article_id,
        "article_type": article_type,
        "title": title,
        "url": url,
        "created_at": created_at,
        "published_at": published_at,
        "modified_at": modified_at,
        "responses": rows,
    }


def _csv_response_columns(conn: sqlite3.Connection) -> str:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(responses)")
    column_names = {row[1] for row in cur.fetchall()}
    if "content_html" in column_names:
        return f"{_RESPONSE_COLUMNS_TXT}, content_html"
    return _RESPONSE_COLUMNS_TXT


def _read_article_archive_for_csv_export(
    article_id: str,
    article_type: str,
    last_n: int | None = None,
) -> dict | None:
    """Like read_article_archive but responses may include content_html (CSV only)."""

    conn = _open_archive_read_conn()
    if conn is None:
        return None
    cur = conn.cursor()

    try:
        article_columns = _article_select_columns(conn)
        cur.execute(
            f"""
            SELECT {article_columns}
            FROM articles
            WHERE article_id=? AND article_type=?
            """,
            (article_id, article_type),
        )

        article = cur.fetchone()
        if not article:
            return None

        title, url, created_at, published_at, modified_at = article

        response_columns = _csv_response_columns(conn)
        if last_n:
            cur.execute(
                f"""
                SELECT {response_columns}
                FROM responses
                WHERE article_id=? AND article_type=?
                ORDER BY res_no DESC
                LIMIT ?
                """,
                (article_id, article_type, last_n),
            )
            rows = cur.fetchall()
            rows.reverse()
        else:
            cur.execute(
                f"""
                SELECT {response_columns}
                FROM responses
                WHERE article_id=? AND article_type=?
                ORDER BY res_no ASC
                """,
                (article_id, article_type),
            )
            rows = cur.fetchall()
    except sqlite3.OperationalError:
        return None
    finally:
        if conn is not None:
            conn.close()

    return {
        "article_id": article_id,
        "article_type": article_type,
        "title": title,
        "url": url,
        "created_at": created_at,
        "published_at": published_at,
        "modified_at": modified_at,
        "responses": rows,
    }


def get_saved_article_summary(article_id, article_type):
    """Return bounded metadata for non-CLI consumers checking archive status."""

    conn = _open_archive_read_conn()
    if conn is None:
        return {
            "found": False,
            "article_id": article_id,
            "article_type": article_type,
            "title": None,
            "url": None,
            "created_at": None,
            "published_at": None,
            "modified_at": None,
            "response_count": 0,
        }
    cur = conn.cursor()

    try:
        article_columns = _article_select_columns(conn)
        cur.execute(
            f"""
            SELECT {article_columns}
            FROM articles
            WHERE article_id=? AND article_type=?
            """,
            (article_id, article_type),
        )

        article = cur.fetchone()
        if not article:
            return {
                "found": False,
                "article_id": article_id,
                "article_type": article_type,
                "title": None,
                "url": None,
                "created_at": None,
                "published_at": None,
                "modified_at": None,
                "response_count": 0,
            }

        title, url, created_at, published_at, modified_at = article
        response_count = _count_saved_responses(cur, article_id, article_type)
    except sqlite3.OperationalError:
        return {
            "found": False,
            "article_id": article_id,
            "article_type": article_type,
            "title": None,
            "url": None,
            "created_at": None,
            "published_at": None,
            "modified_at": None,
            "response_count": 0,
        }
    finally:
        if conn is not None:
            conn.close()

    return _build_saved_article_summary(
        article_id,
        article_type,
        title,
        url,
        created_at,
        published_at,
        modified_at,
        response_count,
    )


def get_saved_article_summary_by_exact_title(title):
    """Return bounded metadata for a saved-title lookup."""

    conn = _open_archive_read_conn()
    if conn is None:
        return {
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
    cur = conn.cursor()

    try:
        article = _find_saved_article_by_title_lookup(cur, title)
        if not article:
            return {
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

        (
            article_id,
            article_type,
            saved_title,
            url,
            created_at,
            published_at,
            modified_at,
        ) = article
        response_count = _count_saved_responses(cur, article_id, article_type)
    except sqlite3.OperationalError:
        return {
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
    finally:
        if conn is not None:
            conn.close()

    return _build_saved_article_summary(
        article_id,
        article_type,
        saved_title,
        url,
        created_at,
        published_at,
        modified_at,
        response_count,
    )


def _article_date_line(archive) -> str | None:
    if archive.get("modified_at"):
        return f"Last Modified: {archive['modified_at']}"
    if archive.get("published_at"):
        return f"Published: {archive['published_at']}"
    return None


def _render_txt_archive(archive):
    lines = [
        "=== ARTICLE META ===",
        f"ID: {archive['article_id']}",
        f"Type: {archive['article_type']}",
        f"Title: {archive['title']}",
        f"URL: {archive['url']}",
        "",
        "=== RESPONSES ===",
    ]

    date_line = _article_date_line(archive)
    if date_line is not None:
        lines.insert(5, date_line)

    for response in archive["responses"]:
        res_no, poster_name, posted_at, id_hash, content_text = response[:5]
        poster_name = poster_name or "unknown"
        posted_at = posted_at or "unknown"
        id_hash = id_hash or "unknown"

        lines.append(f"{res_no} {poster_name} {posted_at} ID: {id_hash}")
        lines.append(content_text or "")
        lines.append("----")

    return "\n".join(lines)


def _render_md_archive(archive: dict) -> str:
    title = archive.get("title") or "unknown"
    lines = [f"# {title}", ""]
    lines.append("## Meta")
    lines.append(f"- ID: {archive.get('article_id')}")
    lines.append(f"- Type: {archive.get('article_type')}")
    lines.append(f"- URL: {archive.get('url')}")
    date_line = _article_date_line(archive)
    if date_line is not None:
        lines.append(f"- {date_line}")
    lines.append("")
    lines.append("## Responses")
    lines.append("")

    for response in archive.get("responses", []):
        res_no, poster_name, posted_at, id_hash, content_text = response[:5]
        poster_name = poster_name or "unknown"
        posted_at = posted_at or "unknown"
        id_hash = id_hash or "unknown"
        lines.append(f"### {res_no}. {poster_name}")
        lines.append(f"- Posted at: {posted_at}")
        lines.append(f"- Poster ID: {id_hash}")
        lines.append("")
        lines.append(content_text or "")
        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines).rstrip("\n") + "\n"


CSV_EXPORT_FIELDS: tuple[str, ...] = (
    "article_id",
    "article_type",
    "article_title",
    "article_url",
    "res_no",
    "poster_name",
    "poster_id",
    "posted_at",
    "content_text",
    "content_html",
)


def _csv_record_for_response(archive: dict, response: tuple) -> dict[str, object]:
    res_no, poster_name, posted_at, id_hash, content_text = response[:5]
    content_html = response[5] if len(response) > 5 else ""
    return {
        "article_id": archive.get("article_id") or "",
        "article_type": archive.get("article_type") or "",
        "article_title": archive.get("title") or "",
        "article_url": archive.get("url") or "",
        "res_no": res_no,
        "poster_name": poster_name or "",
        "poster_id": id_hash or "",
        "posted_at": posted_at or "",
        "content_text": content_text or "",
        "content_html": content_html or "",
    }


def _render_csv_archive(archive: dict) -> str:
    buf = StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=list(CSV_EXPORT_FIELDS),
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    for response in archive.get("responses", []):
        writer.writerow(_csv_record_for_response(archive, response))
    return buf.getvalue()


def get_saved_article_export(article_id: str, article_type: str, fmt: str) -> dict:
    """
    Return bounded one-article export payload for non-CLI consumers.

    Supported formats: txt / md / csv
    """

    if fmt == "csv":
        archive = _read_article_archive_for_csv_export(article_id, article_type)
    else:
        archive = read_article_archive(article_id, article_type)
    if not archive:
        return {
            "found": False,
            "content": None,
            "article_id": article_id,
            "article_type": article_type,
            "format": fmt,
        }

    renderer_by_format = {
        "txt": _render_txt_archive,
        "md": _render_md_archive,
        "csv": _render_csv_archive,
    }
    renderer = renderer_by_format.get(fmt)
    if renderer is None:
        return {
            "found": False,
            "content": None,
            "article_id": article_id,
            "article_type": article_type,
            "format": fmt,
        }

    return {
        "found": True,
        "content": renderer(archive),
        "article_id": article_id,
        "article_type": article_type,
        "title": archive.get("title"),
        "format": fmt,
    }


def get_saved_article_txt(article_id, article_type):
    """
    Return bounded one-article TXT payload for non-CLI consumers.

    Return shape:
      {"found": True, "content": str, "article_id": str, "article_type": str}
      {"found": False, "content": None, "article_id": str, "article_type": str}
    """

    result = get_saved_article_export(article_id, article_type, "txt")
    payload = {
        "found": result["found"],
        "content": result["content"],
        "article_id": result["article_id"],
        "article_type": result["article_type"],
    }
    if result["found"]:
        payload["title"] = result.get("title")
    return payload


def read_article_summaries():
    conn = _open_archive_read_conn()
    if conn is None:
        return []
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                a.article_id,
                a.article_type,
                a.title,
                a.canonical_url,
                a.created_at,
                COUNT(r.id) AS response_count
            FROM articles AS a
            LEFT JOIN responses AS r
                ON a.article_id = r.article_id
                AND a.article_type = r.article_type
            GROUP BY
                a.article_id,
                a.article_type,
                a.title,
                a.canonical_url,
                a.created_at
            ORDER BY a.created_at ASC, a.article_id ASC, a.article_type ASC
            """
        )

        rows = cur.fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()

    return [
        {
            "article_id": article_id,
            "article_type": article_type,
            "title": title or "unknown",
            "url": canonical_url or "unknown",
            "created_at": created_at or "unknown",
            "response_count": response_count,
        }
        for (
            article_id,
            article_type,
            title,
            canonical_url,
            created_at,
            response_count,
        ) in rows
    ]
