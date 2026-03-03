import requests
import json
import os
import sys
import time
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import sqlite3


# ============================================================
# 基本設定
# ============================================================

# シンプルなUA指定（現状は固定値）
HEADERS = {"User-Agent": "Mozilla/5.0"}


# ============================================================
# HTTP取得層
# ============================================================

def fetch_page(url: str) -> BeautifulSoup:
    """
    指定URLを取得し BeautifulSoup を返す。
    200以外は例外送出。
    ※ 現状ロジック変更なし
    """
    res = requests.get(url, headers=HEADERS)

    if res.status_code != 200:
        raise RuntimeError(f"Failed to fetch {url} (status={res.status_code})")

    return BeautifulSoup(res.text, "lxml")


# ============================================================
# URL変換系
# ============================================================

def build_bbs_base_url(article_url: str) -> str:
    """
    記事URLから掲示板ベースURLを生成する。
    /a/xxx -> /b/a/xxx/
    """
    parsed = urlparse(article_url)
    path_parts = parsed.path.strip("/").split("/")

    article_type = path_parts[0]
    article_id = path_parts[1]

    return f"{parsed.scheme}://{parsed.netloc}/b/{article_type}/{article_id}/"


# ============================================================
# パース層
# ============================================================

def parse_responses(soup: BeautifulSoup) -> list:
    """
    掲示板HTMLからレス情報を抽出する。
    抽出対象:
      - レス番号
      - 投稿者名
      - 投稿日時
      - ID
      - 本文テキスト
      - 本文HTML（将来用保険）
    """

    responses = []
    res_heads = soup.find_all("dt", class_="st-bbs_reshead")

    for head in res_heads:

        res_no_raw = head.get("data-res_no")
        id_hash = head.get("data-id_hash")

        # 投稿者名
        name_tag = head.find("span", class_="st-bbs_name")
        poster_name = name_tag.get_text(strip=True) if name_tag else None

        # 投稿日時
        time_tag = head.find("span", class_="bbs_resInfo_resTime")
        posted_at = time_tag.get_text(strip=True) if time_tag else None

        body = head.find_next_sibling("dd", class_="st-bbs_resbody")

        content_text = ""
        content_html = ""

        if body:
            inner = body.find("div", class_="bbs_resbody_inner")

            if inner:

                # 本文以外の領域削除
                for cls in [
                    "st-bbs_contents-oekaki",
                    "st-bbs_contentsTitle",
                    "st-bbsArea_buttons",
                    "st-bbs_referLabel",   # ← 追加
                ]:
                    for tag in inner.find_all(class_=cls):
                        tag.decompose()

                # 画像や入力欄は本文扱いしない
                for tag in inner.find_all(["input", "img"]):
                    tag.decompose()

                # <br> を改行へ変換
                for br in inner.find_all("br"):
                    br.replace_with("\n")

                content_html = str(inner)

                # テキスト抽出（separator未使用）
                content_text = inner.get_text()
                content_text = content_text.strip()

                # 過剰改行圧縮
                while "\n\n\n" in content_text:
                    content_text = content_text.replace("\n\n\n", "\n\n")

        if not res_no_raw:
            continue

        responses.append({
            "res_no": int(res_no_raw),
            "id_hash": id_hash,
            "poster_name": poster_name,
            "posted_at": posted_at,
            "content": content_text,
            "content_html": content_html,
        })

    return responses


# ============================================================
# 掲示板ページ収集
# ============================================================

def collect_all_responses(bbs_base_url: str) -> list:
    """
    ページネーションを辿り全レス収集。
    404または空ページで終了。
    """

    all_responses = []
    start = 1

    while True:

        page_url = f"{bbs_base_url}{start}-"
        print("Fetching:", page_url)

        try:
            soup = fetch_page(page_url)
        except RuntimeError as e:
            print(e)
            break

        page_responses = parse_responses(soup)

        if not page_responses:
            break

        all_responses.extend(page_responses)

        print("Page collected:", len(page_responses))
        print("Total collected:", len(all_responses))

        start += len(page_responses)

        # 過度アクセス回避
        time.sleep(1)

    return all_responses


# ============================================================
# 記事メタ取得
# ============================================================

def fetch_article_metadata(article_url: str):
    """
    記事ページから以下を取得:
      - article_id
      - article_type
      - title
    """

    soup = fetch_page(article_url)

    title_tag = soup.find("meta", property="og:title")
    title = title_tag["content"].split("とは")[0] if title_tag else "unknown"

    og_url = soup.find("meta", property="og:url")
    article_id = og_url["content"].rstrip("/").split("/")[-1] if og_url else "unknown"

    parsed = urlparse(article_url)
    article_type = parsed.path.strip("/").split("/")[0]

    return article_id, article_type, title


# ============================================================
# DB層
# ============================================================

def init_db():
    """
    SQLite初期化（テーブル作成）。
    既存の場合は何もしない。
    """

    os.makedirs("data", exist_ok=True)

    conn = sqlite3.connect("data/nicodic.db")
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id TEXT NOT NULL,
        article_type TEXT NOT NULL,
        title TEXT NOT NULL,
        canonical_url TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(article_id, article_type)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS responses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id TEXT NOT NULL,
        article_type TEXT NOT NULL,
        res_no INTEGER NOT NULL,
        id_hash TEXT,
        poster_name TEXT,
        posted_at TEXT,
        content_html TEXT,
        content_text TEXT,
        res_hidden INTEGER DEFAULT 0,
        idhash_hidden INTEGER DEFAULT 0,
        good_count INTEGER,
        bad_count INTEGER,
        scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(article_id, article_type, res_no)
    )
    """)

    conn.commit()
    return conn


def save_to_db(conn, article_id, article_type, title, article_url, responses):
    """
    記事およびレスをSQLiteへ保存。
    INSERT OR IGNORE で重複回避。
    """

    cur = conn.cursor()

    # 記事メタ保存
    cur.execute("""
        INSERT OR IGNORE INTO articles
        (article_id, article_type, title, canonical_url)
        VALUES (?, ?, ?, ?)
    """, (article_id, article_type, title, article_url))

    # レス保存
    for r in responses:
        cur.execute("""
            INSERT OR IGNORE INTO responses
            (article_id, article_type, res_no, id_hash, poster_name, posted_at,
                    content_text, content_html)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            article_id,
            article_type,
            r["res_no"],
            r.get("id_hash"),
            r.get("poster_name"),
            r.get("posted_at"),
            r.get("content"),
            r.get("content_html"),
        ))

    conn.commit()


# ============================================================
# JSONバックアップ
# ============================================================

def save_json(article_id, article_type, title, article_url, responses):
    """
    取得結果をJSONとして保存（保険用途）。
    """

    os.makedirs("data", exist_ok=True)

    safe_title = title.replace("/", "／").replace("\\", "＼")
    filename = f"{article_id}{article_type}_{safe_title}.json"
    output_path = os.path.join("data", filename)

    data = {
        "article_id": article_id,
        "article_type": article_type,
        "article_url": article_url,
        "title": title,
        "collected_at": int(time.time()),
        "response_count": len(responses),
        "responses": responses
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("Saved JSON:", output_path)


# ============================================================
# inspectコマンド
# ============================================================

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


# ============================================================
# エントリポイント
# ============================================================

def main():
    """
    CLIエントリポイント。
    - 通常: 記事URL指定でスクレイプ実行
    - inspect: DB内容表示
    """

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py <article_url>")
        print("  python main.py inspect <article_id> <article_type> [--last N]")
        sys.exit(1)

    # inspectモード
    if sys.argv[1] == "inspect":

        if len(sys.argv) < 4:
            print("Usage: inspect <article_id> <article_type> [--last N]")
            sys.exit(1)

        article_id = sys.argv[2]
        article_type = sys.argv[3]

        last_n = None
        if "--last" in sys.argv:
            idx = sys.argv.index("--last")
            last_n = int(sys.argv[idx + 1])

        inspect_article(article_id, article_type, last_n)
        return

    # 通常スクレイプモード
    article_url = sys.argv[1]

    article_id, article_type, title = fetch_article_metadata(article_url)
    bbs_base_url = build_bbs_base_url(article_url)

    responses = collect_all_responses(bbs_base_url)

    save_json(article_id, article_type, title, article_url, responses)

    conn = init_db()
    save_to_db(conn, article_id, article_type, title, article_url, responses)
    conn.close()

    print("Saved to SQLite")


if __name__ == "__main__":
    main()

print(;

