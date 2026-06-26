from urllib.parse import quote, urlparse

from article_page_identity import (
    ArticleIdMismatchError,
    resolve_registration_identity_from_html,
)
from http_client import fetch_page
from parser import extract_observed_max_res_no_from_article_top


NICO_TOP_URL = "https://dic.nicovideo.jp"


def normalize_article_input(raw_input: str) -> str:
    return raw_input.strip()


def _build_success_result(
    article_url: str,
    article_id: str,
    article_type: str,
    title: str,
    matched_by: str,
    normalized_input: str,
    observed_max_res_no: int | None = None,
    observed_max_res_no_source: str | None = None,
) -> dict:
    result = {
        "ok": True,
        "canonical_target": {
            "article_url": article_url,
            "article_id": article_id,
            "article_type": article_type,
        },
        "title": title,
        "matched_by": matched_by,
        "normalized_input": normalized_input,
    }
    if observed_max_res_no is not None:
        result["observed_max_res_no"] = observed_max_res_no
        result["observed_max_res_no_source"] = (
            observed_max_res_no_source or "article_top_preview"
        )
    return result


def _build_failure_result(failure_kind: str, normalized_input: str) -> dict:
    return {
        "ok": False,
        "failure_kind": failure_kind,
        "normalized_input": normalized_input,
    }


def _is_not_found_runtime_error(exc: RuntimeError) -> bool:
    return "status=404" in str(exc)


def _looks_like_url_input(normalized_input: str) -> bool:
    parsed = urlparse(normalized_input)
    return bool(parsed.scheme or parsed.netloc)


def _normalize_candidate_url(candidate_url: str) -> str | None:
    parsed = urlparse(candidate_url)

    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.netloc != "dic.nicovideo.jp":
        return None

    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) != 2:
        return None

    article_type, article_slug = path_parts
    if not article_type or not article_slug:
        return None

    return f"{parsed.scheme}://{parsed.netloc}/{article_type}/{article_slug}"


def _id_url_numeric_from_normalized_url(normalized_url: str) -> str | None:
    path_parts = [p for p in urlparse(normalized_url).path.split("/") if p]
    if (
        len(path_parts) == 2
        and path_parts[0] == "id"
        and path_parts[1].isdigit()
    ):
        return path_parts[1]
    return None


def _resolve_from_article_url(
    article_url: str,
    normalized_input: str,
    matched_by: str,
) -> dict:
    id_candidate = _id_url_numeric_from_normalized_url(article_url)

    try:
        soup = fetch_page(article_url)
    except RuntimeError as exc:
        if _is_not_found_runtime_error(exc):
            return _build_failure_result("not_found", normalized_input)
        raise

    try:
        canonical_target, title = resolve_registration_identity_from_html(
            soup,
            article_url,
            input_id_numeric_candidate=id_candidate,
        )
    except ArticleIdMismatchError:
        return _build_failure_result("id_mismatch", normalized_input)
    except ValueError:
        return _build_failure_result("not_found", normalized_input)

    observed_max_res_no = extract_observed_max_res_no_from_article_top(soup)

    return _build_success_result(
        article_url=canonical_target["article_url"],
        article_id=canonical_target["article_id"],
        article_type=canonical_target["article_type"],
        title=title,
        matched_by=matched_by,
        normalized_input=normalized_input,
        observed_max_res_no=observed_max_res_no,
        observed_max_res_no_source="article_top_preview",
    )


def _build_direct_article_url_from_title(normalized_title: str) -> str:
    """Encode the trimmed title into a dic /a/<slug> article URL."""

    slug = quote(normalized_title, safe="")
    return f"{NICO_TOP_URL}/a/{slug}"


def _resolve_from_exact_title(normalized_title: str) -> dict:
    article_url = _build_direct_article_url_from_title(normalized_title)
    return _resolve_from_article_url(
        article_url,
        normalized_input=normalized_title,
        matched_by="exact_title",
    )


def resolve_article_input(article_input: str) -> dict:
    normalized_input = normalize_article_input(article_input)
    if not normalized_input:
        return _build_failure_result("invalid_input", normalized_input)

    if _looks_like_url_input(normalized_input):
        normalized_url = _normalize_candidate_url(normalized_input)
        if normalized_url is None:
            return _build_failure_result("invalid_input", normalized_input)

        return _resolve_from_article_url(
            normalized_url,
            normalized_input=normalized_input,
            matched_by="article_url",
        )

    return _resolve_from_exact_title(normalized_input)
