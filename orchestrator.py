import re
import time
from urllib.parse import urljoin
from urllib.parse import urlparse

from http_client import fetch_page
from parser import parse_responses
from storage import (
    dequeue_canonical_target,
    init_db,
    list_queue_requests,
    save_to_db,
)
from target_list import parse_target_identity

# 取得レス数の上限。未知の high-volume 記事に対する bounded protection。
RESPONSE_CAP = 1_000_000
QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP = 10_800

# 仮置き。known high-volume article を skip するための seed。
DENYLIST_ARTICLE_IDS = frozenset({"480340", "237789"})

BBS_PAGE_SIZE = 30


class ArticleMetadataResult:
    def __init__(
        self,
        article_id: str,
        article_type: str,
        title: str,
        published_at: str | None = None,
        modified_at: str | None = None,
    ) -> None:
        self.article_id = article_id
        self.article_type = article_type
        self.title = title
        self.published_at = published_at
        self.modified_at = modified_at

    def __iter__(self):
        yield self.article_id
        yield self.article_type
        yield self.title


class ScrapeResult:
    """Truthiness matches legacy bool return; ``outcome`` supports telemetry."""

    __slots__ = (
        "ok",
        "outcome",
        "display_status",
        "article_title",
        "collected_response_count",
        "observed_max_res_no",
        "failure_page",
        "failure_cause",
        "short_reason",
        "redirect_target_url",
    )

    def __init__(
        self,
        ok: bool,
        outcome: str = "ok",
        display_status: str | None = None,
        article_title: str = "unknown",
        collected_response_count: int = 0,
        observed_max_res_no: int | None = None,
        failure_page: str | None = None,
        failure_cause: str | None = None,
        short_reason: str | None = None,
        redirect_target_url: str | None = None,
    ) -> None:
        self.ok = ok
        self.outcome = outcome
        self.display_status = display_status or ("success" if ok else "fail")
        self.article_title = article_title
        self.collected_response_count = collected_response_count
        self.observed_max_res_no = observed_max_res_no
        self.failure_page = failure_page
        self.failure_cause = failure_cause
        self.short_reason = short_reason
        self.redirect_target_url = redirect_target_url

    def __bool__(self) -> bool:
        return self.ok


def get_containing_page_start(res_no: int) -> int:
    """Return the BBS page start that contains the given response number."""
    return ((res_no - 1) // BBS_PAGE_SIZE) * BBS_PAGE_SIZE + 1


class ArticleNotFoundError(RuntimeError):
    """記事ページが見つからない場合の orchestration 用例外。"""


class RedirectArticleError(RuntimeError):
    """記事ページが redirect article だった場合の orchestration 用例外。"""

    def __init__(
        self,
        article_url: str,
        redirect_target_url: str,
        article_title: str = "unknown",
    ) -> None:
        self.article_url = article_url
        self.redirect_target_url = redirect_target_url
        self.article_title = article_title
        super().__init__(
            f"Redirect detected: {article_url} -> {redirect_target_url}"
        )


_LOCATION_REPLACE_RE = re.compile(
    r"location\.replace\(\s*(['\"])(.*?)\1\s*\)",
    re.IGNORECASE,
)


def _extract_page_title(soup) -> str:
    meta_title = soup.find("meta", property="og:title")
    if meta_title is not None:
        title = meta_title.get("content", "").strip()
        if title:
            return title.split("とは")[0]

    title_tag = soup.find("title")
    if title_tag is None:
        return "unknown"

    title = title_tag.get_text(" ", strip=True)
    if not title:
        return "unknown"

    return title.split("とは")[0]


def _extract_itemprop_datetime(soup, itemprop: str) -> str | None:
    tag = soup.find(attrs={"itemprop": itemprop})
    if tag is None:
        return None

    content = tag.get("content", "").strip()
    if content:
        return content

    text = tag.get_text(" ", strip=True)
    return text or None


def fetch_article_metadata_record(article_url: str) -> dict:
    try:
        soup = fetch_page(article_url)
    except RuntimeError as exc:
        if "status=404" in str(exc):
            raise ArticleNotFoundError(f"Article not found: {article_url}") from exc
        raise

    redirect_target_url = extract_redirect_target_url(article_url, soup)
    if redirect_target_url is not None:
        raise RedirectArticleError(
            article_url,
            redirect_target_url,
            _extract_page_title(soup),
        )

    title_tag = soup.find("meta", property="og:title")
    title = title_tag["content"].split("とは")[0] if title_tag else "unknown"

    og_url = soup.find("meta", property="og:url")
    if og_url is None:
        raise ArticleNotFoundError(f"Article not found: {article_url}")

    article_id = og_url["content"].rstrip("/").split("/")[-1]
    parsed = urlparse(article_url)
    article_type = parsed.path.strip("/").split("/")[0]

    return {
        "article_id": article_id,
        "article_type": article_type,
        "title": title,
        "published_at": _extract_itemprop_datetime(soup, "datePublished"),
        "modified_at": _extract_itemprop_datetime(soup, "dateModified"),
    }


def _normalize_redirect_target_url(
    article_url: str,
    candidate_url: str,
) -> str | None:
    raw_target = candidate_url.strip().strip('"\'')
    if not raw_target:
        return None

    resolved_target = urljoin(article_url, raw_target)
    target_identity = parse_target_identity(resolved_target)
    if target_identity is None:
        return None
    return target_identity["canonical_url"]


def _extract_meta_refresh_target(article_url: str, soup) -> str | None:
    for meta_tag in soup.find_all("meta"):
        http_equiv = meta_tag.get("http-equiv", "").strip().lower()
        if http_equiv != "refresh":
            continue

        content = meta_tag.get("content", "")
        match = re.search(r"url\s*=\s*([^;]+)", content, re.IGNORECASE)
        if match is None:
            continue

        redirect_target = _normalize_redirect_target_url(
            article_url,
            match.group(1),
        )
        if redirect_target is not None:
            return redirect_target

    return None


def _extract_location_replace_target(article_url: str, soup) -> str | None:
    for script_tag in soup.find_all("script"):
        script_text = script_tag.get_text(" ", strip=True)
        if not script_text:
            continue

        match = _LOCATION_REPLACE_RE.search(script_text)
        if match is None:
            continue

        redirect_target = _normalize_redirect_target_url(
            article_url,
            match.group(2),
        )
        if redirect_target is not None:
            return redirect_target

    return None


def extract_redirect_target_url(article_url: str, soup) -> str | None:
    redirect_target = _extract_meta_refresh_target(article_url, soup)
    if redirect_target is not None:
        return redirect_target
    return _extract_location_replace_target(article_url, soup)


def is_redirect_article_page(article_url: str, soup) -> bool:
    return extract_redirect_target_url(article_url, soup) is not None


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


def _display_target_label(title: str, article_id: str, article_url: str) -> str:
    if title and title != "unknown":
        return title
    if article_id:
        return article_id
    return article_url


def _error_status_text(error_text: str) -> str:
    marker = "status="
    if marker not in error_text:
        return "interrupted"

    status_part = error_text.split(marker, maxsplit=1)[1]
    status = status_part.split(")", maxsplit=1)[0].split()[0]
    return status


def _observed_max_res_no(responses: list[dict]) -> int | None:
    if not responses:
        return None
    return max(response["res_no"] for response in responses)


def collect_all_responses(
    bbs_base_url: str,
    start: int = 1,
    max_saved_res_no: int | None = None,
    response_cap: int | None = None,
    progress_reporter=None,
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
        if progress_reporter is None:
            print("Fetching:", page_url)

        try:
            soup = fetch_page(page_url)
        except RuntimeError as e:
            # 1ページ目の404は「掲示板が存在しない」ケースとして扱い、
            # empty-result として返す（中断フラグは立てない）。
            if first_request and "status=404" in str(e):
                if progress_reporter is None:
                    print("No BBS found:", bbs_base_url)
                return [], False, False

            # 1ページ目のその他エラーは従来どおり上位へ伝播させる。
            if first_request:
                raise

            # 2ページ目以降のエラーは「later-page interruption」として扱う。
            if progress_reporter is None:
                print("Later-page fetch interrupted:", page_url)
                print(e)
            else:
                progress_reporter.later_page_interrupted(
                    page_url,
                    _error_status_text(str(e)),
                    len(all_responses),
                )
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
                if progress_reporter is None:
                    print("No responses found:", bbs_base_url)
            break

        all_responses.extend(page_responses)

        if len(all_responses) >= effective_cap:
            all_responses = all_responses[:effective_cap]
            cap_reached = True
            if progress_reporter is None:
                print("Total collected:", len(all_responses))
            else:
                progress_reporter.response_cap_reached(len(all_responses))
            break

        if progress_reporter is None:
            print("Page collected:", len(page_responses))
            print("Total collected:", len(all_responses))
        else:
            progress_reporter.page_progress(
                page_url,
                len(page_responses),
                len(all_responses),
            )

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
    record = fetch_article_metadata_record(article_url)
    return ArticleMetadataResult(
        record["article_id"],
        record["article_type"],
        record["title"],
        published_at=record.get("published_at"),
        modified_at=record.get("modified_at"),
    )


def run_scrape(
    article_url: str,
    response_cap: int | None = None,
    progress_reporter=None,
    target_index: int | None = None,
    target_total: int | None = None,
) -> ScrapeResult:
    try:
        metadata_result = fetch_article_metadata(article_url)
        article_id, article_type, title = metadata_result

    except RedirectArticleError as exc:
        if progress_reporter is None:
            print(
                f"Redirect detected: {article_url} -> "
                f"{exc.redirect_target_url}"
            )
        return ScrapeResult(
            True,
            "redirect_handoff",
            "success",
            article_title=exc.article_title,
            collected_response_count=0,
            observed_max_res_no=None,
            failure_page=article_url,
            failure_cause="redirect_detected",
            short_reason="redirect_handoff",
            redirect_target_url=exc.redirect_target_url,
        )

    except ArticleNotFoundError:
        if progress_reporter is None:
            print(f"Article not found: {article_url}")
        else:
            progress_reporter.finish_target(
                "fail",
                article_url,
                0,
                article_url,
                reason=f"url={article_url} reason=article_not_found",
            )
        return ScrapeResult(
            False,
            "fail_article_not_found",
            "fail",
            article_title="unknown",
            collected_response_count=0,
            observed_max_res_no=None,
            failure_page="unknown",
            failure_cause="article_not_found",
            short_reason="article_not_found",
        )

    display_label = _display_target_label(title, article_id, article_url)
    target_ref = article_id or article_url

    if (
        progress_reporter is not None
        and target_index is not None
        and target_total is not None
    ):
        progress_reporter.start_target(
            target_index,
            target_total,
            display_label,
            article_url,
        )

    if article_id in DENYLIST_ARTICLE_IDS:
        if progress_reporter is None:
            print("Skipping article (high-volume).")
        else:
            progress_reporter.finish_target(
                "fail",
                display_label,
                0,
                target_ref,
                reason="reason=skip_denylist",
            )
        return ScrapeResult(
            False,
            "skip_denylist",
            "fail",
            article_title=title,
            collected_response_count=0,
            observed_max_res_no=None,
            failure_page="unknown",
            failure_cause="skip_denylist",
            short_reason="skip_denylist",
        )

    max_saved_res_no = get_max_saved_res_no(article_id, article_type)
    bbs_base_url = build_bbs_base_url(article_url)

    if max_saved_res_no is None:
        responses, interrupted, cap_reached = collect_all_responses(
            bbs_base_url,
            response_cap=response_cap,
            progress_reporter=progress_reporter,
        )
    else:
        resume_start = get_containing_page_start(max_saved_res_no)
        if progress_reporter is None:
            print(
                f"Saved article detected; resuming from max_saved_res_no="
                f"{max_saved_res_no}"
            )
        responses, interrupted, cap_reached = collect_all_responses(
            bbs_base_url,
            start=resume_start,
            max_saved_res_no=max_saved_res_no,
            response_cap=response_cap,
            progress_reporter=progress_reporter,
        )

    if max_saved_res_no is not None and not responses and not interrupted:
        if progress_reporter is None:
            print("No new BBS responses found; article already up to date")
        else:
            progress_reporter.finish_target(
                "success",
                display_label,
                max_saved_res_no,
                target_ref,
                reason="reason=already_up_to_date",
            )
        return ScrapeResult(
            True,
            "ok",
            "success",
            article_title=title,
            collected_response_count=0,
            observed_max_res_no=max_saved_res_no,
            short_reason="already_up_to_date",
        )

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
        if progress_reporter is None:
            print("No BBS responses found; saving empty result")
    elif interrupted and responses:
        # 途中ページでのエラーにより中断したが、一部レスは取得済み。
        if progress_reporter is None:
            print(
                f"BBS fetch interrupted; saving partial responses "
                f"({len(responses)} items) for: {article_url}"
            )
    elif cap_reached and responses:
        # 取得上限に到達。その時点までの responses を保存する。
        if progress_reporter is None:
            print(
                f"Response cap reached; saving partial responses "
                f"({len(responses)} items) for: {article_url}"
            )

    conn = init_db()
    save_kwargs = {}
    published_at = getattr(metadata_result, "published_at", None)
    modified_at = getattr(metadata_result, "modified_at", None)
    if published_at is not None:
        save_kwargs["published_at"] = published_at
    if modified_at is not None:
        save_kwargs["modified_at"] = modified_at

    save_to_db(
        conn,
        article_id,
        article_type,
        title,
        article_url,
        responses,
        **save_kwargs,
    )
    conn.close()

    if progress_reporter is None:
        print("Saved to SQLite")
    else:
        display_status = "partial" if interrupted or cap_reached else "success"
        progress_reporter.finish_target(
            display_status,
            display_label,
            len(json_responses),
            target_ref,
        )
    return ScrapeResult(
        True,
        "ok",
        "partial" if interrupted or cap_reached else "success",
        article_title=title,
        collected_response_count=len(responses),
        observed_max_res_no=_observed_max_res_no(json_responses),
        failure_page="unknown",
        failure_cause=(
            "later_page_interrupted"
            if interrupted
            else "response_cap_reached"
            if cap_reached
            else None
        ),
        short_reason=(
            "later_page_interrupted"
            if interrupted
            else "response_cap_reached"
            if cap_reached
            else None
        ),
    )


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
