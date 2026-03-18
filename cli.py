import sqlite3
from datetime import datetime, timezone


def _load_article_archive(article_id, article_type, last_n=None):
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


def _render_md_archive(archive):
    lines = [
        f"# {archive['title']}",
        "",
        f"- Article ID: {archive['article_id']}",
        f"- Article Type: {archive['article_type']}",
        f"- URL: {archive['url']}",
        f"- Created: {archive['created_at']}",
        "",
        "## Responses",
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

        lines.extend(
            [
                "",
                f"### Response {res_no}",
                f"- Poster: {poster_name}",
                f"- Posted At: {posted_at}",
                f"- ID Hash: {id_hash}",
                "",
                content_text or "",
            ]
        )

    return "\n".join(lines)


def export_article(article_id, article_type, output_format):
    archive = _load_article_archive(article_id, article_type)
    if not archive:
        print("Article not found in DB")
        return False

    if output_format == "txt":
        print(_render_txt_archive(archive))
        return True

    if output_format == "md":
        print(_render_md_archive(archive))
        return True

    print(f"Unsupported export format: {output_format}")
    return False


def _load_article_summaries():
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


def list_articles():
    summaries = _load_article_summaries()
    if not summaries:
        print("No saved articles found.")
        return True

    print("=== SAVED ARTICLES ===")
    for summary in summaries:
        print(
            f"{summary['article_id']} {summary['article_type']} | "
            f"title={summary['title']} | "
            f"created_at={summary['created_at']} | "
            f"response_count={summary['response_count']}"
        )

    return True


def export_all_articles(output_format):
    if output_format != "txt":
        print(f"Unsupported export format: {output_format}")
        return False

    summaries = _load_article_summaries()
    if not summaries:
        print("No saved articles found.")
        return True

    exported_at = datetime.now(timezone.utc).isoformat()

    for index, summary in enumerate(summaries, start=1):
        archive = _load_article_archive(
            summary["article_id"],
            summary["article_type"],
        )

        archive_title = summary["title"]
        archive_url = summary["url"]
        archive_created_at = summary["created_at"]
        archive_responses = []

        if archive:
            archive_title = archive["title"] or archive_title
            archive_url = archive["url"] or archive_url
            archive_created_at = archive["created_at"] or archive_created_at
            archive_responses = archive["responses"]

        print(f"=== ARTICLE EXPORT {index}/{len(summaries)} ===")
        print(f"ID: {summary['article_id']}")
        print(f"Type: {summary['article_type']}")
        print(f"Title: {archive_title or 'unknown'}")
        print(f"URL: {archive_url or 'unknown'}")
        print(f"Created: {archive_created_at or 'unknown'}")
        print(f"Exported At: {exported_at}")
        print(f"Response Count: {summary['response_count']}")
        print("")
        print("=== RESPONSES ===")

        for (
            res_no,
            poster_name,
            posted_at,
            id_hash,
            content_text,
        ) in archive_responses:
            poster_name = poster_name or "unknown"
            posted_at = posted_at or "unknown"
            id_hash = id_hash or "unknown"

            print(f">{res_no} {poster_name} {posted_at} ID: {id_hash}")
            print(content_text or "")
            print("----")

        if index != len(summaries):
            print("")

    return True


def inspect_article(article_id, article_type, last_n=None):
    """
    DB内の記事・レスをCLI表示する。
    """

    archive = _load_article_archive(article_id, article_type, last_n=last_n)
    if not archive:
        print("Article not found in DB")
        return

    print("=== ARTICLE META ===")
    print("ID:", article_id)
    print("Type:", article_type)
    print("Title:", archive["title"])
    print("URL:", archive["url"])
    print("Created:", archive["created_at"])

    print("\n=== RESPONSES ===")
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

        print(f"＞{res_no}　{poster_name}　{posted_at} ID: {id_hash}")
        print(content_text or "")
        print("----")
