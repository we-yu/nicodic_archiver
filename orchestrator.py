import time
from urllib.parse import urlparse

from http_client import fetch_page
from parser import parse_responses
from storage import (
    dequeue_canonical_target,
    init_db,
    list_queue_requests,
    save_json,
    save_to_db,
)

# 取得レス数の上限。未知の high-volume 記事に対する bounded protection。
RESPONSE_CAP = 1_000_000
QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP = 10_800

# 仮置き。known high-volume article を skip するための seed。
DENYLIST_ARTICLE_IDS = frozenset({"480340", "237789"})

BBS_PAGE_SIZE = 30


def get_containing_page_start(res_no: int) -> int:
    """Return the BBS page start that contains the given response number."""
    return ((res_no - 1) // BBS_PAGE_SIZE) * BBS_PAGE_SIZE + 1


class ArticleNotFoundError(RuntimeError):
    """記事ページが見つからない場合の orchestration 用例外。"""


def get_max_saved_res_no(article_id: str, article_type: str) -> int | None:
    """Return the highest saved response number for the article, if any."""

    conn = init_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT MAX(res_no)
            FROM responses
            WHERE article_id=? AND article_type=?
            """,
            (article_id, article_type),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return row[0]
    finally:
        conn.close()


def load_saved_responses(article_id: str, article_type: str) -> list[dict]:
    """Load saved responses in res_no order for JSON refresh after resume."""

    conn = init_db()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT res_no, id_hash, poster_name, posted_at, content_text,
                   content_html
            FROM responses
            WHERE article_id=? AND article_type=?
            ORDER BY res_no ASC
            """,
            (article_id, article_type),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "res_no": res_no,
            "id_hash": id_hash,
            "poster_name": poster_name,
            "posted_at": posted_at,
            "content": content_text,
            "content_html": content_html,
        }
        for (
            res_no,
            id_hash,
            poster_name,
            posted_at,
            content_text,
            content_html,
        ) in rows
    ]


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


def collect_all_responses(
    bbs_base_url: str,
    start: int = 1,
    max_saved_res_no: int | None = None,
    response_cap: int | None = None,
) -> tuple[list, bool, bool]:
    """
    ページネーションを辿り全レス収集。
    404または空ページで終了。RESPONSE_CAP に達した場合も終了。

    Returns:
        (all_responses, interrupted, cap_reached)
        interrupted=True は「途中ページでの取得エラーにより中断した」ことを表す。
        cap_reached=True は「取得上限に到達した」ことを表す。
    """

    all_responses = []
    next_start = start
    interrupted = False
    cap_reached = False
    first_request = True
    effective_cap = response_cap if response_cap is not None else RESPONSE_CAP

    while True:

        page_url = f"{bbs_base_url}{next_start}-"
        print("Fetching:", page_url)

        try:
            soup = fetch_page(page_url)
        except RuntimeError as e:
            # 1ページ目の404は「掲示板が存在しない」ケースとして扱い、
            # empty-result として返す（中断フラグは立てない）。
            if first_request and "status=404" in str(e):
                print("No BBS found:", bbs_base_url)
                return [], False, False

            # 1ページ目のその他エラーは従来どおり上位へ伝播させる。
            if first_request:
                raise

            # 2ページ目以降のエラーは「later-page interruption」として扱う。
            print("Later-page fetch interrupted:", page_url)
            print(e)
            interrupted = True
            break

        raw_page_responses = parse_responses(soup)
        page_responses = raw_page_responses

        if max_saved_res_no is not None and first_request:
            page_responses = [
                response
                for response in raw_page_responses
                if response["res_no"] > max_saved_res_no
            ]

        if not raw_page_responses:
            if first_request:
                print("No responses found:", bbs_base_url)
            break

        all_responses.extend(page_responses)

        if len(all_responses) >= effective_cap:
            all_responses = all_responses[:effective_cap]
            cap_reached = True
            print("Total collected:", len(all_responses))
            break

        print("Page collected:", len(page_responses))
        print("Total collected:", len(all_responses))

        next_start += len(raw_page_responses)
        first_request = False

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


def run_scrape(article_url: str, response_cap: int | None = None) -> bool:
    try:
        article_id, article_type, title = fetch_article_metadata(article_url)

    except ArticleNotFoundError:
        print(f"Article not found: {article_url}")
        return False

    if article_id in DENYLIST_ARTICLE_IDS:
        print("Skipping article (high-volume).")
        return False

    max_saved_res_no = get_max_saved_res_no(article_id, article_type)
    bbs_base_url = build_bbs_base_url(article_url)

    if max_saved_res_no is None:
        responses, interrupted, cap_reached = collect_all_responses(
            bbs_base_url,
            response_cap=response_cap,
        )
    else:
        resume_start = get_containing_page_start(max_saved_res_no)
        print(
            f"Saved article detected; resuming from max_saved_res_no="
            f"{max_saved_res_no}"
        )
        responses, interrupted, cap_reached = collect_all_responses(
            bbs_base_url,
            start=resume_start,
            max_saved_res_no=max_saved_res_no,
            response_cap=response_cap,
        )

    if max_saved_res_no is not None and not responses and not interrupted:
        print("No new BBS responses found; article already up to date")
        return True

    json_responses = responses
    if max_saved_res_no is not None:
        json_responses = load_saved_responses(article_id, article_type) + responses

    # empty-result / later-page interruption / cap reached を区別して扱う。
    if (
        max_saved_res_no is None
        and not responses
        and not interrupted
        and not cap_reached
    ):
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

    save_json(article_id, article_type, title, article_url, json_responses)

    conn = init_db()
    save_to_db(conn, article_id, article_type, title, article_url, responses)
    conn.close()

    print("Saved to SQLite")
    return True


def drain_queue_requests(max_requests: int | None = None) -> dict:
    """
    Execute persisted queued requests in bounded single-process order.

    Success-class terminal outcomes are dequeued.
    Unexpected failures keep requests queued.
    """

    conn = init_db()
    try:
        queued = list_queue_requests(conn, limit=max_requests)
        dequeued_count = 0
        error_count = 0

        for request in queued:
            article_url = request["article_url"]
            article_id = request["article_id"]
            article_type = request["article_type"]

            try:
                ok = run_scrape(
                    article_url,
                    response_cap=QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP,
                )
            except Exception as exc:
                print(
                    f"Queue drain failed unexpectedly for {article_url}: "
                    f"{type(exc).__name__}: {exc}"
                )
                error_count += 1
                continue

            if ok:
                removed = dequeue_canonical_target(conn, article_id, article_type)
                if removed:
                    dequeued_count += 1
                continue

            error_count += 1

        return {
            "processed": len(queued),
            "dequeued": dequeued_count,
            "remaining": len(queued) - dequeued_count,
            "errors": error_count,
        }
    finally:
        conn.close()
