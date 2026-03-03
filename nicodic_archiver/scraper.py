"""HTTP fetching layer for NicoNico Dictionary BBS responses."""

import requests

from .config import API_BASE_URL, API_PAGE_LIMIT


def _responses_url(article_slug: str) -> str:
    return f"{API_BASE_URL}/topic/article:{article_slug}/responses"


def fetch_responses(
    article_slug: str,
    from_no: int = 1,
    limit: int = API_PAGE_LIMIT,
    session: requests.Session | None = None,
) -> dict:
    """Fetch a single page of responses from the NicoNico Dictionary BBS API.

    Args:
        article_slug: Article identifier (e.g. ``"vocaloid"``).
        from_no: 1-indexed response number to start from (inclusive).
        limit: Number of responses to fetch (max ``API_PAGE_LIMIT``).
        session: Optional :class:`requests.Session` to reuse.

    Returns:
        Parsed JSON dict containing ``"response"`` list and
        ``"totalResponseCount"`` integer.
    """
    requester = session or requests
    url = _responses_url(article_slug)
    params = {"from": from_no, "limit": limit, "dir": "asc"}
    resp = requester.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_responses(
    article_slug: str,
    session: requests.Session | None = None,
) -> list[dict]:
    """Fetch every response for *article_slug* by paging through the API.

    Returns:
        Flat list of response dicts ordered by response number ascending.
    """
    all_responses: list[dict] = []
    from_no = 1

    while True:
        data = fetch_responses(article_slug, from_no=from_no, session=session)
        page = data.get("response", [])
        if not page:
            break
        all_responses.extend(page)
        total = data.get("totalResponseCount", 0)
        if len(all_responses) >= total:
            break
        from_no = all_responses[-1]["no"] + 1

    return all_responses


def fetch_new_responses(
    article_slug: str,
    last_no: int,
    session: requests.Session | None = None,
) -> list[dict]:
    """Fetch responses with a response number greater than *last_no*.

    This is the differential-scraping entry point: pass in the highest
    response number already stored and only the newer responses are returned.

    Args:
        article_slug: Article identifier.
        last_no: The highest response number already stored.
                 Pass ``0`` to fetch from the very beginning.

    Returns:
        List of new response dicts (may be empty when nothing is new).
    """
    return fetch_all_responses_from(article_slug, from_no=last_no + 1, session=session)


def fetch_all_responses_from(
    article_slug: str,
    from_no: int,
    session: requests.Session | None = None,
) -> list[dict]:
    """Fetch all responses starting from *from_no*."""
    all_responses: list[dict] = []

    while True:
        data = fetch_responses(article_slug, from_no=from_no, session=session)
        page = data.get("response", [])
        if not page:
            break
        all_responses.extend(page)
        total = data.get("totalResponseCount", 0)
        fetched_up_to = all_responses[-1]["no"]
        if fetched_up_to >= total:
            break
        from_no = fetched_up_to + 1

    return all_responses
