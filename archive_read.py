import csv
from datetime import datetime, timezone
from io import StringIO
import sqlite3
from pathlib import Path
from urllib.parse import unquote, urlparse

from storage import DEFAULT_DB_PATH, open_readonly_db

# --- Registered Articles query configuration ---

# Allowed sort columns; validated on every request (server-side sort).
REGISTERED_SORT_ALLOWLIST = frozenset({
    "title",
    "article_id",
    "created_at",
    "saved_response_count",
    "observed_max_res_no",
    "last_scraped_at",
})

# Sorts that order on target/article fields only; stats are fetched after paging.
# observed_max_res_no is a plain target column, so it sorts page-first.
REGISTERED_PAGE_FIRST_SORTS = frozenset({
    "created_at",
    "last_scraped_at",
    "title",
    "article_id",
    "observed_max_res_no",
})

# Search only fields users can understand from the visible table.
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
    # observed_max_res_no is the board-level max observed during registration
    # or scraping (monotonic). saved_max_res_no is kept internally only.
    {"key": "observed_max_res_no", "label": "Observed Max Res No",
     "csv_header": "observed_max_res_no"},
    {"key": "last_scraped_at", "label": "Last Scraped",
     "csv_header": "last_scraped_at"},
]


def _open_archive_read_conn() -> sqlite3.Connection | None:
    return open_readonly_db(DEFAULT_DB_PATH)


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
    result = query_registered_articles(paginate=False)
    return result["rows"]


def _registered_has_last_scraped_column(conn):
    """Return whether the optional articles.latest_scraped_at column exists."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(articles)")
    cols = {row[1] for row in cur.fetchall()}
    return "latest_scraped_at" in cols


def _registered_has_target_title_column(conn):
    """Return whether optional target.title column exists (older DB compat)."""

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(target)")
    cols = {row[1] for row in cur.fetchall()}
    return "title" in cols


def _registered_has_observed_max_column(conn):
    """Return whether optional target.observed_max_res_no exists (DB compat)."""

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(target)")
    cols = {row[1] for row in cur.fetchall()}
    return "observed_max_res_no" in cols


def _registered_fallback_title(article_id, canonical_url):
    parsed = urlparse(canonical_url or "")
    path_parts = [part for part in parsed.path.split("/") if part]
    if path_parts:
        fallback = unquote(path_parts[-1]).strip()
        if fallback:
            return fallback

    fallback = unquote(article_id or "").strip()
    if fallback:
        return fallback

    return ""


def _registered_uses_page_first_sort(sort_by: str) -> bool:
    return sort_by in REGISTERED_PAGE_FIRST_SORTS


def _registered_build_resolved_targets_cte(
    *,
    matched_title_sql: str,
    matched_last_scraped_expr: str,
    observed_max_expr: str = "NULL",
) -> str:
    return f"""
            WITH url_fallback_articles AS (
                SELECT
                    a.article_type,
                    a.canonical_url,
                    MIN(a.id) AS selected_article_row_id
                FROM articles AS a
                WHERE a.canonical_url IS NOT NULL
                GROUP BY a.article_type, a.canonical_url
            ),
            resolved_targets AS (
                SELECT
                    t.id AS target_row_id,
                    t.article_id AS target_article_id,
                    t.article_type AS target_article_type,
                    t.canonical_url AS target_canonical_url,
                    t.created_at AS target_created_at,
                    {observed_max_expr} AS observed_max_res_no,
                    COALESCE(a_exact.article_id, a_url.article_id)
                        AS matched_article_id,
                    COALESCE(a_exact.article_type, a_url.article_type)
                        AS matched_article_type,
                    {matched_title_sql}
                        AS matched_title,
                    {matched_last_scraped_expr} AS matched_last_scraped_at
                FROM target AS t
                LEFT JOIN articles AS a_exact
                    ON t.article_id = a_exact.article_id
                    AND t.article_type = a_exact.article_type
                LEFT JOIN url_fallback_articles AS ufa
                    ON a_exact.id IS NULL
                    AND ufa.article_type = t.article_type
                    AND ufa.canonical_url = t.canonical_url
                LEFT JOIN articles AS a_url
                    ON a_url.id = ufa.selected_article_row_id
                WHERE t.is_active = 1
            )
        """


def _registered_build_search_clause(search):
    if not search:
        return "", []
    conds = []
    for col in REGISTERED_SEARCH_COLUMNS:
        expr = _registered_search_sql_expr(col)
        conds.append(f"{expr} LIKE ? ESCAPE '\\'")
    where_sql = "WHERE (" + " OR ".join(conds) + ")"
    escaped_search = _escape_registered_like_term(search)
    where_params = [
        f"%{escaped_search}%" for _ in REGISTERED_SEARCH_COLUMNS
    ]
    return where_sql, where_params


def _registered_has_article_response_stats_table(conn):
    """Return whether the materialized summary table exists (older DB compat)."""

    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name='article_response_stats'"
    )
    return cur.fetchone() is not None


def _registered_unique_identities(identities):
    unique = []
    seen = set()
    for article_id, article_type in identities:
        if article_id is None or article_type is None:
            continue
        key = (article_id, article_type)
        if key in seen:
            continue
        seen.add(key)
        unique.append(key)
    return unique


def _registered_stats_from_summary(conn, unique):
    placeholders = ",".join(["(?, ?)"] * len(unique))
    params = [part for pair in unique for part in pair]
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT article_id, article_type, saved_response_count, saved_max_res_no
        FROM article_response_stats
        WHERE (article_id, article_type) IN ({placeholders})
        """,
        params,
    )
    return {
        (row[0], row[1]): {"count": int(row[2]), "max_res": row[3]}
        for row in cur.fetchall()
    }


def _registered_stats_from_responses(conn, unique):
    """Bounded fallback: aggregate responses for the given identities only.

    Bounded to the page identities (at most one page worth), so this never
    performs an unbounded full-table aggregation.
    """

    placeholders = ",".join(["(?, ?)"] * len(unique))
    params = [part for pair in unique for part in pair]
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT article_id, article_type, COUNT(id), MAX(res_no)
        FROM responses
        WHERE (article_id, article_type) IN ({placeholders})
        GROUP BY article_id, article_type
        """,
        params,
    )
    return {
        (row[0], row[1]): {"count": int(row[2]), "max_res": row[3]}
        for row in cur.fetchall()
    }


def _registered_fetch_response_stats(conn, identities):
    """Read saved stats for explicit page identities from the summary table.

    Falls back to a bounded responses aggregation only for identities that are
    missing from the summary (e.g. a not-yet-backfilled DB).
    """

    unique = _registered_unique_identities(identities)
    if not unique:
        return {}

    if _registered_has_article_response_stats_table(conn):
        stats = _registered_stats_from_summary(conn, unique)
    else:
        stats = {}

    missing = [key for key in unique if key not in stats]
    if missing:
        stats.update(_registered_stats_from_responses(conn, missing))
    return stats


def _registered_display_saved_max_res_no(
    matched_last_scraped_at,
    saved_response_count,
    saved_max_res_no,
):
    if (
        matched_last_scraped_at
        and saved_response_count == 0
    ):
        return 0
    return saved_max_res_no


def _registered_page_shell_row_to_dict(shell_row, stats_map):
    (
        article_id,
        article_type,
        title,
        canonical_url,
        created_at,
        matched_article_id,
        matched_article_type,
        last_scraped_at,
        observed_max_res_no,
    ) = shell_row
    stat = None
    if matched_article_id is not None and matched_article_type is not None:
        stat = stats_map.get((matched_article_id, matched_article_type))
    saved_response_count = stat["count"] if stat else 0
    saved_max_res_no = stat["max_res"] if stat else None
    display_max = _registered_display_saved_max_res_no(
        last_scraped_at,
        saved_response_count,
        saved_max_res_no,
    )
    return _registered_row_to_dict(
        (
            article_id,
            article_type,
            title,
            canonical_url,
            created_at,
            saved_response_count,
            display_max,
            last_scraped_at,
            observed_max_res_no,
        )
    )


def _registered_target_scoped_response_stats_sql() -> str:
    return """
            SELECT
                r.article_id,
                r.article_type,
                COUNT(r.id) AS saved_response_count,
                MAX(r.res_no) AS saved_max_res_no
            FROM responses AS r
            INNER JOIN (
                SELECT DISTINCT
                    matched_article_id AS article_id,
                    matched_article_type AS article_type
                FROM resolved_targets
                WHERE matched_article_id IS NOT NULL
                  AND matched_article_type IS NOT NULL
            ) AS target_ids
                ON r.article_id = target_ids.article_id
                AND r.article_type = target_ids.article_type
            GROUP BY r.article_id, r.article_type
        """


def _registered_saved_max_res_no_display_sql() -> str:
    """SQL: MAX(saved res_no), or 0 when scrape completed with zero responses."""
    return (
        "CASE WHEN rt.matched_last_scraped_at IS NOT NULL "
        "AND COALESCE(rs.saved_response_count, 0) = 0 "
        "THEN 0 "
        "ELSE rs.saved_max_res_no END"
    )


def _registered_order_by_clause(sort_by, order_dir, *, max_res_sql: str):
    """Return a typed ORDER BY clause for a validated sort key."""
    numeric_article_id = (
        "rt.target_article_id != '' "
        "AND rt.target_article_id NOT GLOB '*[^0-9]*'"
    )
    sort_terms = {
        "title": [
            (
                "COALESCE(NULLIF(rt.matched_title, ''), rt.target_article_id) "
                f"COLLATE NOCASE {order_dir}"
            ),
        ],
        "article_id": [
            (
                "CASE WHEN "
                f"{numeric_article_id} THEN 0 ELSE 1 END ASC"
            ),
            (
                "CASE WHEN "
                f"{numeric_article_id} "
                "THEN CAST(rt.target_article_id AS INTEGER) END "
                f"{order_dir}"
            ),
            f"rt.target_article_id COLLATE NOCASE {order_dir}",
        ],
        "created_at": [
            "CASE WHEN rt.target_created_at IS NULL THEN 1 ELSE 0 END ASC",
            f"julianday(rt.target_created_at) {order_dir}",
        ],
        "saved_response_count": [
            f"COALESCE(rs.saved_response_count, 0) {order_dir}",
        ],
        "observed_max_res_no": [
            "CASE WHEN rt.observed_max_res_no IS NULL THEN 1 ELSE 0 END ASC",
            f"rt.observed_max_res_no {order_dir}",
        ],
        "last_scraped_at": [
            "CASE WHEN rt.matched_last_scraped_at IS NULL THEN 1 ELSE 0 END ASC",
            f"julianday(rt.matched_last_scraped_at) {order_dir}",
        ],
    }
    selected = sort_terms.get(sort_by, sort_terms[DEFAULT_REGISTERED_SORT_BY])
    return ",\n                ".join(
        selected + [
            "rt.target_row_id DESC",
            "rt.target_article_type ASC",
        ]
    )


def _registered_search_sql_expr(column):
    mapping = {
        "title": (
            "registered_visible_title("
            "rt.target_article_id, rt.target_canonical_url, rt.matched_title)"
        ),
        "article_id": "registered_visible_article_id(rt.target_article_id)",
    }
    return mapping[column]


def _escape_registered_like_term(term):
    """Escape LIKE wildcard characters so user input is treated literally."""
    escaped = term.replace("\\", "\\\\")
    escaped = escaped.replace("%", "\\%")
    return escaped.replace("_", "\\_")


def _registered_visible_article_id(value):
    return unquote(value or "").strip()


def _registered_visible_title(article_id, canonical_url, matched_title):
    title = (matched_title or "").strip()
    if title:
        return title
    return _registered_fallback_title(article_id, canonical_url)


def _register_registered_search_functions(conn):
    conn.create_function(
        "registered_visible_article_id",
        1,
        _registered_visible_article_id,
    )
    conn.create_function(
        "registered_visible_title",
        3,
        _registered_visible_title,
    )


def _registered_row_to_dict(row):
    (
        article_id,
        article_type,
        title,
        canonical_url,
        created_at,
        saved_response_count,
        saved_max_res_no,
        last_scraped_at,
        observed_max_res_no,
    ) = row
    aid = (article_id or "").strip()
    if aid.isdigit():
        display_article_id = aid
    else:
        display_article_id = unquote(aid)
    display_title = title or _registered_fallback_title(article_id, canonical_url)
    return {
        "article_id": display_article_id,
        "article_type": article_type or "",
        "title": display_title,
        "canonical_url": canonical_url or "",
        "created_at": created_at or "",
        "saved_response_count": saved_response_count or 0,
        # saved_max_res_no is kept internally for diagnostics; not displayed.
        "saved_max_res_no": saved_max_res_no,
        "observed_max_res_no": observed_max_res_no,
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
    # Legacy saved-stat sort aliases now map to the user-facing observed max.
    if sort_by in ("latest_scraped_max_res_no", "saved_max_res_no"):
        sort_by = "observed_max_res_no"
    if sort_by not in REGISTERED_SORT_ALLOWLIST:
        sort_by = DEFAULT_REGISTERED_SORT_BY
    order_dir = "ASC" if sort_order == "asc" else "DESC"
    conn = _open_archive_read_conn()
    if conn is None:
        return {"rows": [], "total": 0, "page": page, "per_page": per_page}

    try:
        _register_registered_search_functions(conn)
        has_last_scraped = _registered_has_last_scraped_column(conn)
        has_target_title = _registered_has_target_title_column(conn)
        matched_last_scraped_expr = (
            "COALESCE(a_exact.latest_scraped_at, a_url.latest_scraped_at)"
            if has_last_scraped
            else "NULL"
        )
        if has_target_title:
            matched_title_sql = (
                "COALESCE("
                "NULLIF(TRIM(COALESCE(t.title, '')), ''), "
                "a_exact.title, a_url.title)"
            )
        else:
            matched_title_sql = "COALESCE(a_exact.title, a_url.title)"
        observed_max_expr = (
            "t.observed_max_res_no"
            if _registered_has_observed_max_column(conn)
            else "NULL"
        )
        base_cte_sql = _registered_build_resolved_targets_cte(
            matched_title_sql=matched_title_sql,
            matched_last_scraped_expr=matched_last_scraped_expr,
            observed_max_expr=observed_max_expr,
        )
        where_sql, where_params = _registered_build_search_clause(search)
        max_res_sql = _registered_saved_max_res_no_display_sql()
        order_by_sql = _registered_order_by_clause(
            sort_by, order_dir, max_res_sql=max_res_sql,
        )

        cur = conn.cursor()
        if where_sql:
            cur.execute(
                base_cte_sql + f"""
                SELECT COUNT(*)
                FROM resolved_targets AS rt
                {where_sql}
                """,
                where_params,
            )
            total = cur.fetchone()[0]
        else:
            # Fast path for default no-search page: count active targets directly.
            cur.execute(
                """
                SELECT COUNT(*)
                FROM target
                WHERE is_active = 1
                """
            )
            total = cur.fetchone()[0]

        if _registered_uses_page_first_sort(sort_by):
            page_sql = base_cte_sql + f"""
                SELECT
                    rt.target_article_id,
                    rt.target_article_type,
                    rt.matched_title,
                    rt.target_canonical_url,
                    rt.target_created_at,
                    rt.matched_article_id,
                    rt.matched_article_type,
                    rt.matched_last_scraped_at,
                    rt.observed_max_res_no
                FROM resolved_targets AS rt
                {where_sql}
                ORDER BY
                    {order_by_sql}
            """
            if paginate:
                offset = (page - 1) * per_page
                cur.execute(
                    page_sql + " LIMIT ? OFFSET ?",
                    where_params + [per_page, offset],
                )
            else:
                cur.execute(page_sql, where_params)

            shell_rows = cur.fetchall()
            stats_map = _registered_fetch_response_stats(
                conn,
                [
                    (row[5], row[6])
                    for row in shell_rows
                ],
            )
            rows = [
                _registered_page_shell_row_to_dict(shell_row, stats_map)
                for shell_row in shell_rows
            ]
        else:
            if _registered_has_article_response_stats_table(conn):
                stats_source_sql = "article_response_stats"
            else:
                stats_source_sql = (
                    "(" + _registered_target_scoped_response_stats_sql() + ")"
                )
            data_sql = base_cte_sql + f"""
                SELECT
                    rt.target_article_id,
                    rt.target_article_type,
                    rt.matched_title,
                    rt.target_canonical_url,
                    rt.target_created_at,
                    COALESCE(rs.saved_response_count, 0) AS saved_response_count,
                    ({max_res_sql}) AS saved_max_res_no,
                    rt.matched_last_scraped_at AS last_scraped_at,
                    rt.observed_max_res_no AS observed_max_res_no
                FROM resolved_targets AS rt
                LEFT JOIN {stats_source_sql} AS rs
                    ON rt.matched_article_id = rs.article_id
                    AND rt.matched_article_type = rs.article_type
                {where_sql}
                ORDER BY
                    {order_by_sql}
            """
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
            col["csv_header"]: (
                "" if row.get(col["key"]) is None else str(row.get(col["key"]))
            )
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
        saved_max = row["saved_max_res_no"]
        lines.append(
            f"type={row['article_type']}"
            f" | responses={row['saved_response_count']}"
            f" | saved_max_res_no={saved_max if saved_max is not None else '-'}"
            f" | last_scraped={last_scraped}"
            f" | title={row['title']}"
        )
    lines.append("")

    output_path = Path(data_dir) / "scrape_targets.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
