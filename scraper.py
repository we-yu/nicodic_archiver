"""
Scrape orchestration: article metadata, BBS response collection, and persist.
Uses http_client, parser, and storage; no CLI.
"""
import time
from urllib.parse import urlparse

from http_client import fetch_page
from parser import parse_responses
from storage import init_db, save_json, save_to_db


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


def scrape_article(article_url: str):
    """
    記事URLからメタ取得・BBS全レス収集・JSON/DB保存まで一括実行。
    保存順・表示・sleepは従来どおり。
    """
    article_id, article_type, title = fetch_article_metadata(article_url)
    bbs_base_url = build_bbs_base_url(article_url)

    responses = collect_all_responses(bbs_base_url)

    save_json(article_id, article_type, title, article_url, responses)

    conn = init_db()
    save_to_db(conn, article_id, article_type, title, article_url, responses)
    conn.close()

    print("Saved to SQLite")
