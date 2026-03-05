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
