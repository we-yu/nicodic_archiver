import csv
from datetime import datetime, timezone
from io import StringIO
import re
import sqlite3
from pathlib import Path
from urllib.parse import unquote, urlparse

from storage import DEFAULT_DB_PATH


REGISTERED_ARTICLE_DEFAULT_PER_PAGE = 100
REGISTERED_ARTICLE_ALLOWED_PER_PAGE = (100, 200, 500, 1000)
REGISTERED_ARTICLE_DEFAULT_SORT_BY = "created_at"
REGISTERED_ARTICLE_DEFAULT_SORT_DIR = "desc"
REGISTERED_ARTICLE_TABLE_COLUMNS = (
    {"key": "title", "label": "Title"},
    {"key": "article_id_display", "label": "Article ID"},
    {"key": "article_type", "label": "Type"},
    {"key": "canonical_url", "label": "Canonical URL"},
    {"key": "saved_response_count", "label": "Saved Responses"},
    {"key": "latest_scraped_max_res_no", "label": "Max Res No"},
    {"key": "last_scraped_at", "label": "Last Scraped"},
    {"key": "created_at", "label": "Registered At"},
)
REGISTERED_ARTICLE_SORT_ALLOWLIST = {
    "title": lambda row: row["title"].casefold(),
    "article_id": lambda row: row["article_id"].casefold(),
    "created_at": lambda row: row["created_at"] or "",
    "saved_response_count": lambda row: row["saved_response_count"],
    "latest_scraped_max_res_no": lambda row: (
        row["latest_scraped_max_res_no"]
        if row["latest_scraped_max_res_no"] is not None
        else -1
    ),
    "last_scraped_at": lambda row: row["last_scraped_at"] or "",
}
REGISTERED_ARTICLE_SEARCH_FIELDS = (
    lambda row: row["title"],
    lambda row: row["article_id"],
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


def _decode_human_text(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    decoded = unquote(text).strip()
    return decoded or text


def _storage_article_key(article_id: str) -> str:
    return _decode_human_text(article_id) or "unknown"


def _human_article_id(article_id: str) -> str:
    text = (article_id or "").strip()
    return text if text.isdigit() else ""


def _humanize_canonical_target_title(canonical_url: str | None) -> str:
    parsed = urlparse(canonical_url or "")
    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts:
        return "unknown"
    return _decode_human_text(path_parts[-1]) or "unknown"


def _human_registered_title(title: str | None, canonical_url: str | None) -> str:
    text = (title or "").strip()
    if text:
        return text
    return _humanize_canonical_target_title(canonical_url)


def _export_meta_lines(archive, *, markdown: bool) -> list[str]:
    article_id_text = _human_article_id(archive["article_id"]) or "unavailable"
    lines = [
        ("Article ID", article_id_text),
        ("Article Type", archive["article_type"]),
        ("Canonical URL", archive["url"]),
        ("Storage Key", _storage_article_key(archive["article_id"])),
        ("Title", archive["title"]),
    ]

    if markdown:
        return [f"- {label}: {value}" for label, value in lines]

    return [f"{label}: {value}" for label, value in lines]


def _sanitize_export_filename_part(value: str) -> str:
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value)
    text = re.sub(r"\s+", " ", text).strip(" .")
    return text or "article"


def build_human_export_filename(
    article_id: str,
    article_type: str,
    title: str | None,
    canonical_url: str | None,
    requested_format: str,
) -> str:
    display_article_id = _human_article_id(article_id)
    title_part = _sanitize_export_filename_part(
        _human_registered_title(title, canonical_url)
    )

    if display_article_id:
        base_identifier = f"{display_article_id}{article_type}"
    else:
        base_identifier = _sanitize_export_filename_part(
            f"{article_type}_{_humanize_canonical_target_title(canonical_url)}"
        )

    if title_part.casefold() == base_identifier.casefold():
        stem = base_identifier
    else:
        stem = f"{base_identifier}_{title_part}"
    return f"{stem}.{requested_format}"


def _render_txt_archive(archive):
    lines = [
        "=== ARTICLE META ===",
        *_export_meta_lines(archive, markdown=False),
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
        *_export_meta_lines(archive, markdown=True),
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
        "storage_article_key",
        "article_title",
        "canonical_url",
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
                "article_id": _human_article_id(archive["article_id"]),
                "article_type": archive["article_type"],
                "storage_article_key": _storage_article_key(
                    archive["article_id"]
                ),
                "article_title": archive["title"],
                "canonical_url": archive["url"],
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
        "canonical_url": archive["url"],
        "filename": build_human_export_filename(
            article_id,
            article_type,
            archive["title"],
            archive["url"],
            requested_format,
        ),
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

    return get_registered_article_listing()["rows"]


def _fetch_registered_article_rows() -> list[dict]:
    conn = _open_archive_read_conn()
    if conn is None:
        return []
    cur = conn.cursor()

    try:
        cur.execute("PRAGMA table_info(articles)")
        article_column_names = {row[1] for row in cur.fetchall()}
        has_scraped_at = "latest_scraped_at" in article_column_names

        scraped_at_sel = (
            "a.latest_scraped_at AS last_scraped_at"
            if has_scraped_at
            else "NULL AS last_scraped_at"
        )

        cur.execute(
            f"""
            SELECT
                t.article_id,
                t.article_type,
                a.title,
                t.canonical_url,
                t.created_at,
                COALESCE(r.saved_response_count, 0) AS saved_response_count,
                r.latest_scraped_max_res_no,
                {scraped_at_sel}
            FROM target AS t
            LEFT JOIN articles AS a
                ON t.article_id = a.article_id
                AND t.article_type = a.article_type
            LEFT JOIN (
                SELECT
                    article_id,
                    article_type,
                    COUNT(*) AS saved_response_count,
                    MAX(res_no) AS latest_scraped_max_res_no
                FROM responses
                GROUP BY article_id, article_type
            ) AS r
                ON t.article_id = r.article_id
                AND t.article_type = r.article_type
            ORDER BY t.created_at DESC, t.article_id ASC, t.article_type ASC
            """
        )
        rows = cur.fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()

    registered_rows = []
    for (
        article_id,
        article_type,
        title,
        canonical_url,
        created_at,
        saved_response_count,
        latest_scraped_max_res_no,
        last_scraped_at,
    ) in rows:
        registered_rows.append(
            {
                "article_id": article_id,
                "article_id_display": _human_article_id(article_id),
                "article_type": article_type,
                "title": _human_registered_title(title, canonical_url),
                "canonical_url": canonical_url or "",
                "saved_response_count": saved_response_count or 0,
                "latest_scraped_max_res_no": latest_scraped_max_res_no,
                "last_scraped_at": last_scraped_at,
                "created_at": created_at or "",
                "is_pending_initial_scrape": (
                    (saved_response_count or 0) == 0 and not last_scraped_at
                ),
            }
        )
    return registered_rows


def _normalize_registered_sort_by(sort_by: str | None) -> str:
    if sort_by in REGISTERED_ARTICLE_SORT_ALLOWLIST:
        return sort_by
    return REGISTERED_ARTICLE_DEFAULT_SORT_BY


def _normalize_registered_sort_dir(sort_dir: str | None) -> str:
    if sort_dir in {"asc", "desc"}:
        return sort_dir
    return REGISTERED_ARTICLE_DEFAULT_SORT_DIR


def _normalize_registered_per_page(per_page: int | str | None) -> int:
    try:
        parsed = int(per_page)
    except (TypeError, ValueError):
        return REGISTERED_ARTICLE_DEFAULT_PER_PAGE

    if parsed in REGISTERED_ARTICLE_ALLOWED_PER_PAGE:
        return parsed
    return REGISTERED_ARTICLE_DEFAULT_PER_PAGE


def _normalize_registered_page(page: int | str | None) -> int:
    try:
        parsed = int(page)
    except (TypeError, ValueError):
        return 1
    return max(parsed, 1)


def _matches_registered_article_query(row: dict, query: str) -> bool:
    normalized = query.casefold()
    if not normalized:
        return True
    return any(
        normalized in str(field(row) or "").casefold()
        for field in REGISTERED_ARTICLE_SEARCH_FIELDS
    )


def _sort_registered_article_rows(
    rows: list[dict],
    sort_by: str,
    sort_dir: str,
) -> list[dict]:
    sort_key = REGISTERED_ARTICLE_SORT_ALLOWLIST[sort_by]
    secondary_key = lambda row: (
        row["created_at"] or "",
        row["article_id"],
        row["article_type"],
    )
    prepared = sorted(rows, key=secondary_key, reverse=True)
    return sorted(
        prepared,
        key=sort_key,
        reverse=(sort_dir == "desc"),
    )


def get_registered_article_listing(
    *,
    query: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    page: int | str | None = None,
    per_page: int | str | None = None,
) -> dict:
    normalized_query = (query or "").strip()
    normalized_sort_by = _normalize_registered_sort_by(sort_by)
    normalized_sort_dir = _normalize_registered_sort_dir(sort_dir)
    normalized_per_page = _normalize_registered_per_page(per_page)
    requested_page = _normalize_registered_page(page)

    filtered_rows = [
        row
        for row in _fetch_registered_article_rows()
        if _matches_registered_article_query(row, normalized_query)
    ]
    sorted_rows = _sort_registered_article_rows(
        filtered_rows,
        normalized_sort_by,
        normalized_sort_dir,
    )

    total_count = len(sorted_rows)
    total_pages = max(1, (total_count + normalized_per_page - 1) // normalized_per_page)
    normalized_page = min(requested_page, total_pages)
    offset = (normalized_page - 1) * normalized_per_page
    paged_rows = sorted_rows[offset : offset + normalized_per_page]

    return {
        "rows": paged_rows,
        "total_count": total_count,
        "page": normalized_page,
        "per_page": normalized_per_page,
        "total_pages": total_pages,
        "sort_by": normalized_sort_by,
        "sort_dir": normalized_sort_dir,
        "query": normalized_query,
        "columns": REGISTERED_ARTICLE_TABLE_COLUMNS,
        "allowed_per_page": REGISTERED_ARTICLE_ALLOWED_PER_PAGE,
        "sort_allowlist": tuple(REGISTERED_ARTICLE_SORT_ALLOWLIST.keys()),
    }


def render_registered_articles_csv(rows: list[dict]) -> str:
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow([column["label"] for column in REGISTERED_ARTICLE_TABLE_COLUMNS])
    for row in rows:
        writer.writerow(
            [row.get(column["key"], "") for column in REGISTERED_ARTICLE_TABLE_COLUMNS]
        )
    return output.getvalue()


def get_registered_articles_csv(
    *,
    query: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
    page: int | str | None = None,
    per_page: int | str | None = None,
) -> dict:
    listing = get_registered_article_listing(
        query=query,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        per_page=per_page,
    )
    return {
        "content": render_registered_articles_csv(listing["rows"]),
        "listing": listing,
        "filename": "registered_articles_current_page.csv",
    }


def get_all_registered_articles_csv() -> dict:
    rows = _sort_registered_article_rows(
        _fetch_registered_article_rows(),
        REGISTERED_ARTICLE_DEFAULT_SORT_BY,
        REGISTERED_ARTICLE_DEFAULT_SORT_DIR,
    )
    return {
        "content": render_registered_articles_csv(rows),
        "row_count": len(rows),
        "filename": "registered_articles_all.csv",
    }


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
