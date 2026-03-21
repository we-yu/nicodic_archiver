import sqlite3


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
