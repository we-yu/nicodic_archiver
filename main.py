import sys
import time
from urllib.parse import urlparse
from cli import inspect_article
from http_client import fetch_page
from parser import parse_responses
from storage import init_db, save_json, save_to_db


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
