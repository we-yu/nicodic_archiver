from urllib.parse import quote, urlparse

from http_client import fetch_page


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
) -> dict:
    return {
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


def _build_failure_result(failure_kind: str, normalized_input: str) -> dict:
    return {
        "ok": False,
        "failure_kind": failure_kind,
        "normalized_input": normalized_input,
    }


def _looks_like_url_input(normalized_input: str) -> bool:
    parsed = urlparse(normalized_input)
    return bool(parsed.scheme or parsed.netloc)


def _extract_title_from_soup(soup) -> str | None:
    title_tag = soup.find("meta", property="og:title")
    if title_tag is None:
        return None

    title = title_tag.get("content", "").strip()
    if not title:
        return None

    return title.split("とは")[0]


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


def _extract_canonical_target_from_url(article_url: str) -> dict | None:
    normalized_url = _normalize_candidate_url(article_url)
    if normalized_url is None:
        return None

    parsed = urlparse(normalized_url)
    article_type, article_id = [part for part in parsed.path.split("/") if part]

    return {
        "article_url": normalized_url,
        "article_id": article_id,
        "article_type": article_type,
    }


def _resolve_from_article_url(
    article_url: str,
    normalized_input: str,
    matched_by: str,
) -> dict:
    try:
        soup = fetch_page(article_url)
    except RuntimeError as exc:
        if "status=404" in str(exc):
            return _build_failure_result("not_found", normalized_input)
        raise

    og_url = soup.find("meta", property="og:url")
    canonical_url = og_url.get("content", "").strip() if og_url else ""
    canonical_target = _extract_canonical_target_from_url(canonical_url)
    title = _extract_title_from_soup(soup)

    if canonical_target is None or title is None:
        return _build_failure_result("not_found", normalized_input)

    return _build_success_result(
        article_url=canonical_target["article_url"],
        article_id=canonical_target["article_id"],
        article_type=canonical_target["article_type"],
        title=title,
        matched_by=matched_by,
        normalized_input=normalized_input,
    )


def _build_title_search_url(normalized_title: str) -> str:
    return f"{NICO_TOP_URL}/search/{quote(normalized_title)}"


def _extract_exact_title_candidate_urls(soup, normalized_title: str) -> list[str]:
    candidate_urls = []
    seen_urls = set()

    for anchor in soup.find_all("a", href=True):
        candidate_title = anchor.get_text(strip=True)
        if candidate_title != normalized_title:
            continue

        href = anchor["href"].strip()
        if href.startswith("/"):
            href = f"{NICO_TOP_URL}{href}"

        normalized_url = _normalize_candidate_url(href)
        if normalized_url is None:
            continue
        if normalized_url in seen_urls:
            continue

        seen_urls.add(normalized_url)
        candidate_urls.append(normalized_url)

    return candidate_urls


def _resolve_from_exact_title(normalized_title: str) -> dict:
    search_url = _build_title_search_url(normalized_title)
    soup = fetch_page(search_url)
    candidate_urls = _extract_exact_title_candidate_urls(soup, normalized_title)

    if not candidate_urls:
        return _build_failure_result("not_found", normalized_title)
    if len(candidate_urls) >= 2:
        return _build_failure_result("ambiguous", normalized_title)

    return _resolve_from_article_url(
        candidate_urls[0],
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
