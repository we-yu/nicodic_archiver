from urllib.parse import quote, urlparse

from http_client import fetch_page


def normalize_article_input(raw_input: str) -> str:
    """Normalize user-provided article input at a single seam."""

    return raw_input.strip()


def _parse_article_top_url(candidate: str) -> dict | None:
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.netloc != "dic.nicovideo.jp":
        return None

    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) != 2:
        return None

    article_type, article_id = parts
    if not article_type or not article_id:
        return None

    return {
        "article_url": (
            f"{parsed.scheme}://{parsed.netloc}/{article_type}/{article_id}"
        ),
        "article_id": article_id,
        "article_type": article_type,
    }


def _build_search_url(title: str) -> str:
    encoded = quote(title, safe="")
    return f"https://dic.nicovideo.jp/search/{encoded}"


def _extract_candidates_from_search_page(soup) -> list[dict]:
    candidates = []
    seen = set()
    for link in soup.find_all("a"):
        href = link.get("href")
        if not href:
            continue

        parsed = urlparse(href)
        if parsed.netloc and parsed.netloc != "dic.nicovideo.jp":
            continue
        if parsed.scheme and parsed.scheme not in {"http", "https"}:
            continue

        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) != 2:
            continue
        article_type, article_id = parts
        if not article_type or not article_id:
            continue

        title = link.get_text(strip=True)
        key = (article_type, article_id, title)
        if key in seen:
            continue
        seen.add(key)

        candidates.append(
            {
                "article_url": f"https://dic.nicovideo.jp/{article_type}/{article_id}",
                "article_id": article_id,
                "article_type": article_type,
                "title": title,
            }
        )
    return candidates


def resolve_article_input(article_input: str) -> dict:
    """
    Resolve user input into canonical article target.

    Success envelope:
      {
        "ok": True,
        "canonical_target": {"article_url", "article_id", "article_type"},
        "title": str,
        "matched_by": "article_url" | "title_exact",
        "normalized_input": str,
      }

    Failure envelope:
      {
        "ok": False,
        "error_type": "invalid_input" | "not_found" | "ambiguous",
        "normalized_input": str,
      }
    """

    normalized_input = normalize_article_input(article_input)
    if not normalized_input:
        return {
            "ok": False,
            "error_type": "invalid_input",
            "normalized_input": normalized_input,
        }

    parsed_url = _parse_article_top_url(normalized_input)
    if parsed_url is not None:
        return {
            "ok": True,
            "canonical_target": parsed_url,
            "title": normalized_input,
            "matched_by": "article_url",
            "normalized_input": normalized_input,
        }

    # Bounded title resolution: fetch first search page only, exact title match.
    soup = fetch_page(_build_search_url(normalized_input))
    candidates = _extract_candidates_from_search_page(soup)
    exact = [c for c in candidates if c["title"] == normalized_input]

    if len(exact) == 1:
        selected = exact[0]
        return {
            "ok": True,
            "canonical_target": {
                "article_url": selected["article_url"],
                "article_id": selected["article_id"],
                "article_type": selected["article_type"],
            },
            "title": selected["title"],
            "matched_by": "title_exact",
            "normalized_input": normalized_input,
        }

    if len(exact) == 0:
        return {
            "ok": False,
            "error_type": "not_found",
            "normalized_input": normalized_input,
        }

    return {
        "ok": False,
        "error_type": "ambiguous",
        "normalized_input": normalized_input,
    }
