import sqlite3


def inspect_article(article_id, article_type, last_n=None):
    """
    DB内の記事・レスをCLI表示する。
    """

    conn = sqlite3.connect("data/nicodic.db")
    cur = conn.cursor()

    cur.execute("""
        SELECT title, canonical_url, created_at
        FROM articles
        WHERE article_id=? AND article_type=?
    """, (article_id, article_type))

    article = cur.fetchone()
    if not article:
        print("Article not found in DB")
        conn.close()
        return

    title, url, created_at = article

    print("=== ARTICLE META ===")
    print("ID:", article_id)
    print("Type:", article_type)
    print("Title:", title)
    print("URL:", url)
    print("Created:", created_at)

    if last_n:
        cur.execute("""
            SELECT res_no, poster_name, posted_at, id_hash, content_text
            FROM responses
            WHERE article_id=? AND article_type=?
            ORDER BY res_no DESC
            LIMIT ?
        """, (article_id, article_type, last_n))
        rows = cur.fetchall()
        rows.reverse()
    else:
        cur.execute("""
            SELECT res_no, poster_name, posted_at, id_hash, content_text
            FROM responses
            WHERE article_id=? AND article_type=?
            ORDER BY res_no ASC
        """, (article_id, article_type))
        rows = cur.fetchall()

    print("\n=== RESPONSES ===")
    for res_no, poster_name, posted_at, id_hash, content_text in rows:
        poster_name = poster_name or "unknown"
        posted_at = posted_at or "unknown"
        id_hash = id_hash or "unknown"

        print(f"＞{res_no}　{poster_name}　{posted_at} ID: {id_hash}")
        print(content_text or "")
        print("----")

    conn.close()


def export_article(article_id: str, article_type: str, fmt: str) -> bool:
    """
    Export one saved article archive (article + responses) as text-based output.

    Args:
        article_id: DB-side article identifier.
        article_type: DB-side discriminator (e.g. "a" for /a/<id>).
        fmt: "txt" or "md".

    Returns:
        True if exported, False if not found or unsupported format.
    """
    fmt = (fmt or "").lower()
    if fmt not in {"txt", "md"}:
        print(f"Unsupported format: {fmt}")
        return False

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
        print("Article not found in DB")
        conn.close()
        return False

    title, url, created_at = article

    cur.execute(
        """
        SELECT res_no, poster_name, posted_at, id_hash, content_text, content_html
        FROM responses
        WHERE article_id=? AND article_type=?
        ORDER BY res_no ASC
        """,
        (article_id, article_type),
    )
    rows = cur.fetchall()
    conn.close()

    if fmt == "txt":
        _print_export_txt(article_id, article_type, title, url, created_at, rows)
        return True

    _print_export_md(article_id, article_type, title, url, created_at, rows)
    return True


def _print_export_txt(
    article_id: str,
    article_type: str,
    title: str,
    url: str | None,
    created_at: str | None,
    rows: list[tuple],
) -> None:
    print("=== ARTICLE ===")
    print(f"ID: {article_id}")
    print(f"Type: {article_type}")
    print(f"Title: {title}")
    print(f"URL: {url or ''}")
    print(f"Created: {created_at or ''}")
    print("")
    print("=== RESPONSES ===")

    for res_no, poster_name, posted_at, id_hash, content_text, content_html in rows:
        poster_name = poster_name or "unknown"
        posted_at = posted_at or "unknown"
        id_hash = id_hash or "unknown"
        content_text = content_text or ""
        content_html = content_html or ""

        print(f">{res_no} {poster_name} {posted_at} ID: {id_hash}")
        if content_text:
            print(content_text)
        print("")
        if content_html:
            print("[html]")
            print(content_html)
            print("[/html]")
        print("----")


def _print_export_md(
    article_id: str,
    article_type: str,
    title: str,
    url: str | None,
    created_at: str | None,
    rows: list[tuple],
) -> None:
    print(f"# {title}")
    print("")
    print(f"- ID: `{article_id}`")
    print(f"- Type: `{article_type}`")
    print(f"- URL: {url or ''}")
    print(f"- Created: {created_at or ''}")
    print("")
    print("## Responses")
    print("")

    for res_no, poster_name, posted_at, id_hash, content_text, content_html in rows:
        poster_name = poster_name or "unknown"
        posted_at = posted_at or "unknown"
        id_hash = id_hash or "unknown"
        content_text = content_text or ""
        content_html = content_html or ""

        print(f"### {res_no}")
        print("")
        print(f"- name: {poster_name}")
        print(f"- posted_at: {posted_at}")
        print(f"- id: {id_hash}")
        print("")
        if content_text:
            print(content_text)
            print("")
        if content_html:
            print("```html")
            print(content_html)
            print("```")
            print("")
