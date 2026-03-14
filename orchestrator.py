import time
from urllib.parse import urlparse

from http_client import fetch_page
from parser import parse_responses
from storage import init_db, save_json, save_to_db

# 取得レス数の上限。未知の high-volume 記事に対する bounded protection。
RESPONSE_CAP = 1_000_000

# 仮置き。known high-volume article を skip するための seed。
DENYLIST_ARTICLE_IDS = frozenset({"480340", "237789"})


class ArticleNotFoundError(RuntimeError):
    """記事ページが見つからない場合の orchestration 用例外。"""


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


def collect_all_responses(bbs_base_url: str) -> tuple[list, bool, bool]:
    """
    ページネーションを辿り全レス収集。
    404または空ページで終了。RESPONSE_CAP に達した場合も終了。

    Returns:
        (all_responses, interrupted, cap_reached)
        interrupted=True は「途中ページでの取得エラーにより中断した」ことを表す。
        cap_reached=True は「取得上限に到達した」ことを表す。
    """

    all_responses = []
    start = 1
    interrupted = False
    cap_reached = False

    while True:

        page_url = f"{bbs_base_url}{start}-"
        print("Fetching:", page_url)

        try:
            soup = fetch_page(page_url)
        except RuntimeError as e:
            # 1ページ目の404は「掲示板が存在しない」ケースとして扱い、
            # empty-result として返す（中断フラグは立てない）。
            if start == 1 and "status=404" in str(e):
                print("No BBS found:", bbs_base_url)
                return [], False, False

            # 1ページ目のその他エラーは従来どおり上位へ伝播させる。
            if start == 1:
                raise

            # 2ページ目以降のエラーは「later-page interruption」として扱う。
            print("Later-page fetch interrupted:", page_url)
            print(e)
            interrupted = True
            break

        page_responses = parse_responses(soup)

        if not page_responses:
            if start == 1:
                print("No responses found:", bbs_base_url)
            break

        all_responses.extend(page_responses)

        if len(all_responses) >= RESPONSE_CAP:
            all_responses = all_responses[:RESPONSE_CAP]
            cap_reached = True
            print("Total collected:", len(all_responses))
            break

        print("Page collected:", len(page_responses))
        print("Total collected:", len(all_responses))

        start += len(page_responses)

        # 過度アクセス回避
        time.sleep(1)

    return all_responses, interrupted, cap_reached


def fetch_article_metadata(article_url: str):
    """
    記事ページから以下を取得:
      - article_id
      - article_type
      - title
    """

    try:
        soup = fetch_page(article_url)
    except RuntimeError as exc:
        if "status=404" in str(exc):
            raise ArticleNotFoundError(f"Article not found: {article_url}") from exc
        raise

    title_tag = soup.find("meta", property="og:title")
    title = title_tag["content"].split("とは")[0] if title_tag else "unknown"

    og_url = soup.find("meta", property="og:url")
    if og_url is None:
        raise ArticleNotFoundError(f"Article not found: {article_url}")

    article_id = og_url["content"].rstrip("/").split("/")[-1]

    parsed = urlparse(article_url)
    article_type = parsed.path.strip("/").split("/")[0]

    return article_id, article_type, title


def run_scrape(article_url: str):
    try:
        article_id, article_type, title = fetch_article_metadata(article_url)

    except ArticleNotFoundError:
        print(f"Article not found: {article_url}")
        return

    if article_id in DENYLIST_ARTICLE_IDS:
        print("Skipping article (high-volume).")
        return

    bbs_base_url = build_bbs_base_url(article_url)

    responses, interrupted, cap_reached = collect_all_responses(bbs_base_url)

    # empty-result / later-page interruption / cap reached を区別して扱う。
    if not responses and not interrupted and not cap_reached:
        # 掲示板は存在するがレスが0件、あるいは掲示板自体が存在しないケース。
        print("No BBS responses found; saving empty result")
    elif interrupted and responses:
        # 途中ページでのエラーにより中断したが、一部レスは取得済み。
        print(
            f"BBS fetch interrupted; saving partial responses "
            f"({len(responses)} items) for: {article_url}"
        )
    elif cap_reached and responses:
        # 取得上限に到達。その時点までの responses を保存する。
        print(
            f"Response cap reached; saving partial responses "
            f"({len(responses)} items) for: {article_url}"
        )

    save_json(article_id, article_type, title, article_url, responses)

    conn = init_db()
    save_to_db(conn, article_id, article_type, title, article_url, responses)
    conn.close()

    print("Saved to SQLite")
