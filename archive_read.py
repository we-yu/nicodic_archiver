import csv
from datetime import datetime, timezone
from io import StringIO
import sqlite3
from pathlib import Path
from urllib.parse import unquote

from storage import DEFAULT_DB_PATH

# --- Registered Articles query configuration ---

# Allowed sort columns; validated on every request (server-side sort).
REGISTERED_SORT_ALLOWLIST = frozenset({
    "title",
    "article_id",
    "created_at",
    "saved_response_count",
    "latest_scraped_max_res_no",
    "last_scraped_at",
})

# Search target columns. Add entries here to extend user-facing search
# without scattering conditions across query/render code.
REGISTERED_SEARCH_COLUMNS = ("title", "article_id")

DEFAULT_REGISTERED_SORT_BY = "created_at"
DEFAULT_REGISTERED_SORT_ORDER = "desc"
DEFAULT_REGISTERED_PER_PAGE = 100
ALLOWED_REGISTERED_PER_PAGE = (100, 200, 500, 1000)

# Single-source column definition — web table headers and CSV headers
# both derive from this list. Update here to change both at once.
REGISTERED_ARTICLE_COLUMNS = [
    {"key": "article_id", "label": "Article ID",
     "csv_header": "article_id"},
    {"key": "article_type", "label": "Type",
     "csv_header": "article_type"},
    {"key": "title", "label": "Title",
     "csv_header": "title"},
    {"key": "canonical_url", "label": "Canonical URL",
     "csv_header": "canonical_url"},
    {"key": "created_at", "label": "Created At",
     "csv_header": "created_at"},
    {"key": "saved_response_count", "label": "Saved Responses",
     "csv_header": "saved_response_count"},
    {"key": "latest_scraped_max_res_no", "label": "Max Res No",
     "csv_header": "latest_scraped_max_res_no"},
    {"key": "last_scraped_at", "label": "Last Scraped",
     "csv_header": "last_scraped_at"},
]


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
    display_id = unquote(archive['article_id'])
    lines = [
        "=== ARTICLE META ===",
        f"ID: {display_id}",
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
    display_id = unquote(archive['article_id'])
    lines = [
        f"# {archive['title']}",
        "",
        f"- ID: {display_id}",
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


def get_saved_article_summary_by_id(article_id):
    """Return bounded metadata for the first saved article matching article_id."""

    conn = _open_archive_read_conn()
    _not_found = {
        "found": False,
        "article_id": article_id,
        "article_type": None,
        "title": None,
        "url": None,
        "created_at": None,
        "published_at": None,
        "modified_at": None,
        "response_count": 0,
    }
    if conn is None:
        return _not_found
    cur = conn.cursor()

    try:
        select_columns = _title_lookup_select_columns(conn)
        cur.execute(
            f"""
            SELECT {select_columns}
            FROM articles
            WHERE article_id=?
            ORDER BY created_at ASC, article_type ASC
            LIMIT 1
            """,
            (article_id,),
        )
        article = cur.fetchone()
        if not article:
            return _not_found

        (
            found_id,
            article_type,
            title,
            url,
            created_at,
            published_at,
            modified_at,
        ) = article
        response_count = _count_saved_responses(
            cur, found_id, article_type
        )
    except sqlite3.OperationalError:
        return _not_found
    finally:
        conn.close()

    return _build_saved_article_summary(
        found_id,
        article_type,
        title,
        url,
        created_at,
        published_at,
        modified_at,
        response_count,
    )


def list_registered_articles():
    """Return per-article summary list for registered article table view."""

    conn = _open_archive_read_conn()
    if conn is None:
        return []
    cur = conn.cursor()

    try:
        cur.execute("PRAGMA table_info(articles)")
        column_names = {row[1] for row in cur.fetchall()}
        has_scraped_at = "latest_scraped_at" in column_names

        if has_scraped_at:
            scraped_at_sel = (
                ", a.latest_scraped_at AS last_scraped_at"
            )
            scraped_at_grp = ", a.latest_scraped_at"
        else:
            scraped_at_sel = ", NULL AS last_scraped_at"
            scraped_at_grp = ""

        cur.execute(
            f"""
            SELECT
                a.article_id,
                a.article_type,
                a.title,
                a.canonical_url,
                a.created_at,
                COUNT(r.id) AS saved_response_count,
                MAX(r.res_no) AS latest_scraped_max_res_no
                {scraped_at_sel}
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
                {scraped_at_grp}
            ORDER BY
                a.created_at ASC, a.article_id ASC, a.article_type ASC
            """
        )
        rows = cur.fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()

    return [
        {
            "article_id": unquote(article_id or ""),
            "article_type": article_type,
            "title": title or "",
            "canonical_url": canonical_url or "",
            "created_at": created_at or "",
            "saved_response_count": saved_response_count,
            "latest_scraped_max_res_no": latest_scraped_max_res_no,
            "last_scraped_at": last_scraped_at,
        }
        for (
            article_id,
            article_type,
            title,
            canonical_url,
            created_at,
            saved_response_count,
            latest_scraped_max_res_no,
            last_scraped_at,
        ) in rows
    ]


def _registered_sql_schema(conn):
    """Return (has_scraped_at, scraped_sel, scraped_grp) for articles."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(articles)")
    cols = {row[1] for row in cur.fetchall()}
    has = "latest_scraped_at" in cols
    sel = (
        ", a.latest_scraped_at AS last_scraped_at"
        if has
        else ", NULL AS last_scraped_at"
    )
    grp = ", a.latest_scraped_at" if has else ""
    return has, sel, grp


def _registered_sort_sql_expr(sort_by):
    """Return SQL ORDER BY expression for a validated sort_by key."""
    _map = {
        "title": "a.title",
        "article_id": "a.article_id",
        "created_at": "a.created_at",
        "saved_response_count": "saved_response_count",
        "latest_scraped_max_res_no": "latest_scraped_max_res_no",
        "last_scraped_at": "last_scraped_at",
    }
    return _map.get(sort_by, "a.created_at")


def _registered_row_to_dict(row):
    (
        article_id,
        article_type,
        title,
        canonical_url,
        created_at,
        saved_response_count,
        latest_scraped_max_res_no,
        last_scraped_at,
    ) = row
    return {
        "article_id": unquote(article_id or ""),
        "article_type": article_type or "",
        "title": title or "",
        "canonical_url": canonical_url or "",
        "created_at": created_at or "",
        "saved_response_count": saved_response_count,
        "latest_scraped_max_res_no": latest_scraped_max_res_no,
        "last_scraped_at": last_scraped_at,
    }


def query_registered_articles(
    *,
    sort_by=DEFAULT_REGISTERED_SORT_BY,
    sort_order=DEFAULT_REGISTERED_SORT_ORDER,
    search=None,
    page=1,
    per_page=DEFAULT_REGISTERED_PER_PAGE,
    paginate=True,
):
    """Query registered articles with sort, search, and pagination.

    Returns:
        {"rows": [...], "total": int, "page": int, "per_page": int}

    Set paginate=False to return all matching rows without LIMIT/OFFSET;
    that is the internal all-records export route.
    """
    if sort_by not in REGISTERED_SORT_ALLOWLIST:
        sort_by = DEFAULT_REGISTERED_SORT_BY
    order_dir = "ASC" if sort_order == "asc" else "DESC"
    conn = _open_archive_read_conn()
    if conn is None:
        return {"rows": [], "total": 0, "page": page, "per_page": per_page}

    try:
        _has_scraped, scraped_sel, scraped_grp = _registered_sql_schema(conn)
        sort_expr = _registered_sort_sql_expr(sort_by)

        where_sql = ""
        where_params: list = []
        if search:
            conds = [
                f"a.{col} LIKE ?" for col in REGISTERED_SEARCH_COLUMNS
            ]
            where_sql = "WHERE (" + " OR ".join(conds) + ")"
            where_params = [
                f"%{search}%" for _ in REGISTERED_SEARCH_COLUMNS
            ]

        count_sql = f"SELECT COUNT(*) FROM articles AS a {where_sql}"
        data_sql = f"""
            SELECT
                a.article_id,
                a.article_type,
                a.title,
                a.canonical_url,
                a.created_at,
                COUNT(r.id) AS saved_response_count,
                MAX(r.res_no) AS latest_scraped_max_res_no
                {scraped_sel}
            FROM articles AS a
            LEFT JOIN responses AS r
                ON a.article_id = r.article_id
                AND a.article_type = r.article_type
            {where_sql}
            GROUP BY
                a.article_id, a.article_type, a.title,
                a.canonical_url, a.created_at{scraped_grp}
            ORDER BY
                {sort_expr} {order_dir},
                a.article_id ASC, a.article_type ASC
        """

        cur = conn.cursor()
        cur.execute(count_sql, where_params)
        total = cur.fetchone()[0]

        if paginate:
            offset = (page - 1) * per_page
            cur.execute(
                data_sql + " LIMIT ? OFFSET ?",
                where_params + [per_page, offset],
            )
        else:
            cur.execute(data_sql, where_params)

        rows = [_registered_row_to_dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        return {"rows": [], "total": 0, "page": page, "per_page": per_page}
    finally:
        conn.close()

    return {"rows": rows, "total": total, "page": page, "per_page": per_page}


def _render_registered_list_csv(rows):
    """Render registered article rows as CSV using shared column def."""
    output = StringIO()
    headers = [col["csv_header"] for col in REGISTERED_ARTICLE_COLUMNS]
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    for row in rows:
        writer.writerow({
            col["csv_header"]: str(row.get(col["key"]) or "")
            for col in REGISTERED_ARTICLE_COLUMNS
        })
    return output.getvalue()


def export_registered_articles_csv(
    sort_by=DEFAULT_REGISTERED_SORT_BY,
    sort_order=DEFAULT_REGISTERED_SORT_ORDER,
    search=None,
):
    """Export all registered articles as CSV for internal CLI use.

    All-records route (no pagination). For the user-facing web CSV
    (current-page only), use query_registered_articles() +
    _render_registered_list_csv() via web_app.py.
    """
    result = query_registered_articles(
        sort_by=sort_by,
        sort_order=sort_order,
        search=search,
        paginate=False,
    )
    return _render_registered_list_csv(result["rows"])


def write_scrape_targets_txt(data_dir="data"):
    """Write a human-readable targets snapshot to scrape_targets.txt."""

    articles = list_registered_articles()
    ts = datetime.now(timezone.utc).isoformat()
    lines = [
        "# scrape_targets: registered article snapshot",
        f"# generated_at: {ts}",
        f"# count: {len(articles)}",
        "",
    ]
    for row in articles:
        last_scraped = row.get("last_scraped_at") or "-"
        max_res = row["latest_scraped_max_res_no"]
        lines.append(
            f"type={row['article_type']}"
            f" | responses={row['saved_response_count']}"
            f" | max_res_no={max_res if max_res is not None else '-'}"
            f" | last_scraped={last_scraped}"
            f" | title={row['title']}"
        )
    lines.append("")

    output_path = Path(data_dir) / "scrape_targets.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
