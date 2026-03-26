import sqlite3


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
    response_count,
):
    return {
        "found": True,
        "article_id": article_id,
        "article_type": article_type,
        "title": title,
        "url": url,
        "created_at": created_at,
        "response_count": response_count,
    }


def _find_saved_article_by_title_lookup(cur, title):
    cur.execute(
        """
        SELECT article_id, article_type, title, canonical_url, created_at
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
        """
        SELECT article_id, article_type, title, canonical_url, created_at
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

    conn = sqlite3.connect("data/nicodic.db")
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM articles
        WHERE article_id=? AND article_type=?
        LIMIT 1
        """,
        (article_id, article_type),
    )
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def read_article_archive(article_id, article_type, last_n=None):
    conn = sqlite3.connect("data/nicodic.db")
    cur = conn.cursor()

    cur.execute(
        """
        SELECT title, canonical_url, created_at
        FROM articles
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )

    article = cur.fetchone()
    if not article:
        conn.close()
        return None

    title, url, created_at = article

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

    conn.close()

    return {
        "article_id": article_id,
        "article_type": article_type,
        "title": title,
        "url": url,
        "created_at": created_at,
        "responses": rows,
    }


def get_saved_article_summary(article_id, article_type):
    """Return bounded metadata for non-CLI consumers checking archive status."""

    conn = sqlite3.connect("data/nicodic.db")
    cur = conn.cursor()

    cur.execute(
        """
        SELECT title, canonical_url, created_at
        FROM articles
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )

    article = cur.fetchone()
    if not article:
        conn.close()
        return {
            "found": False,
            "article_id": article_id,
            "article_type": article_type,
            "title": None,
            "url": None,
            "created_at": None,
            "response_count": 0,
        }

    title, url, created_at = article
    response_count = _count_saved_responses(cur, article_id, article_type)
    conn.close()

    return _build_saved_article_summary(
        article_id,
        article_type,
        title,
        url,
        created_at,
        response_count,
    )


def get_saved_article_summary_by_exact_title(title):
    """Return bounded metadata for a saved-title lookup."""

    conn = sqlite3.connect("data/nicodic.db")
    cur = conn.cursor()

    article = _find_saved_article_by_title_lookup(cur, title)

    if not article:
        conn.close()
        return {
            "found": False,
            "article_id": None,
            "article_type": None,
            "title": None,
            "url": None,
            "created_at": None,
            "response_count": 0,
        }

    article_id, article_type, saved_title, url, created_at = article
    response_count = _count_saved_responses(cur, article_id, article_type)
    conn.close()

    return _build_saved_article_summary(
        article_id,
        article_type,
        saved_title,
        url,
        created_at,
        response_count,
    )


def _render_txt_archive(archive):
    lines = [
        "=== ARTICLE META ===",
        f"ID: {archive['article_id']}",
        f"Type: {archive['article_type']}",
        f"Title: {archive['title']}",
        f"URL: {archive['url']}",
        f"Created: {archive['created_at']}",
        "",
        "=== RESPONSES ===",
    ]

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

        lines.append(f">{res_no} {poster_name} {posted_at} ID: {id_hash}")
        lines.append(content_text or "")
        lines.append("----")

    return "\n".join(lines)


def get_saved_article_txt(article_id, article_type):
    """
    Return bounded one-article TXT payload for non-CLI consumers.

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
        "content": _render_txt_archive(archive),
        "article_id": article_id,
        "article_type": article_type,
    }


def read_article_summaries():
    conn = sqlite3.connect("data/nicodic.db")
    cur = conn.cursor()

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
