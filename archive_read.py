import csv
from io import StringIO
import sqlite3
from pathlib import Path

from storage import DEFAULT_DB_PATH


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
                """
                SELECT res_no, poster_name, posted_at, id_hash, content_text
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
                """
                SELECT res_no, poster_name, posted_at, id_hash, content_text
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

    for (
        res_no,
        poster_name,
        posted_at,
        id_hash,
        content_text,
    ) in archive["responses"]:
        poster_name = poster_name or "unknown"
        posted_at = posted_at or "unknown"
        id_hash = id_hash or "unknown"

        lines.append(f"{res_no} {poster_name} {posted_at} ID: {id_hash}")
        lines.append(content_text or "")
        lines.append("----")

    return "\n".join(lines)


def _escape_md_response_text(content_text):
    escaped_lines = []
    for line in (content_text or "").splitlines():
        if line.startswith(">"):
            line = "\\" + line
        escaped_lines.append(f"{line}  ")
    return "\n".join(escaped_lines).rstrip()


def _render_md_archive(archive):
    lines = [
        f"# {archive['title']}",
        "",
        f"- ID: {archive['article_id']}",
        f"- Type: {archive['article_type']}",
        f"- URL: {archive['url']}",
    ]

    date_line = _article_date_line(archive)
    if date_line is not None:
        lines.append(f"- {date_line}")

    lines.extend(["", "## Responses", ""])

    for response in archive["responses"]:
        res_no, poster_name, posted_at, id_hash, content_text = response
        lines.extend(
            [
                f"### {res_no}",
                f"- Name: {poster_name or 'unknown'}",
                f"- Posted At: {posted_at or 'unknown'}",
                f"- Poster ID: {id_hash or 'unknown'}",
                "",
                _escape_md_response_text(content_text),
                "",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def _csv_field_names():
    return [
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
    ]


def _read_archive_csv_rows(article_id, article_type):
    conn = _open_archive_read_conn()
    if conn is None:
        return []
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                res_no,
                poster_name,
                posted_at,
                id_hash,
                content_text,
                content_html
            FROM responses
            WHERE article_id=? AND article_type=?
            ORDER BY res_no ASC
            """,
            (article_id, article_type),
        )
        return cur.fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def _render_csv_archive(archive):
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=_csv_field_names())
    writer.writeheader()

    for row in _read_archive_csv_rows(
        archive["article_id"],
        archive["article_type"],
    ):
        res_no, poster_name, posted_at, id_hash, content_text, content_html = row
        writer.writerow(
            {
                "article_id": archive["article_id"],
                "article_type": archive["article_type"],
                "article_title": archive["title"],
                "article_url": archive["url"],
                "res_no": res_no,
                "poster_name": poster_name or "",
                "poster_id": id_hash or "",
                "posted_at": posted_at or "",
                "content_text": content_text or "",
                "content_html": content_html or "",
            }
        )

    return output.getvalue()


def _render_article_export(archive, requested_format):
    renderers = {
        "txt": _render_txt_archive,
        "md": _render_md_archive,
        "csv": _render_csv_archive,
    }
    return renderers.get(requested_format, _render_txt_archive)(archive)


def get_saved_article_export(article_id, article_type, requested_format="txt"):
    """
    Return bounded one-article export payload for non-CLI consumers.

    Return shape:
      {"found": True, "content": str, "article_id": str, "article_type": str}
      {"found": False, "content": None, "article_id": str, "article_type": str}
    """

    archive = read_article_archive(article_id, article_type)
    if not archive:
        return {
            "found": False,
            "content": None,
            "article_id": article_id,
            "article_type": article_type,
        }

    return {
        "found": True,
        "content": _render_article_export(archive, requested_format),
        "article_id": article_id,
        "article_type": article_type,
        "title": archive["title"],
        "format": requested_format,
    }


def get_saved_article_txt(article_id, article_type):
    return get_saved_article_export(article_id, article_type, "txt")


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
