import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


# シンプルなUA指定（現状は固定値）
HEADERS = {"User-Agent": "Mozilla/5.0"}

# HTTP層の境界として最低限の保護を行う
REQUEST_TIMEOUT_SECONDS = 10


def _fetch_response(url: str) -> requests.Response:
    try:
        return requests.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.Timeout as e:
        raise RuntimeError(
            f"Failed to fetch {url} (timeout={REQUEST_TIMEOUT_SECONDS}s)"
        ) from e
    except requests.RequestException as e:
        raise RuntimeError(
            f"Failed to fetch {url} ({type(e).__name__}: {e})"
        ) from e


def _normalize_dic_url(candidate_url: str) -> str | None:
    parsed = urlparse(candidate_url)
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

    return f"https://dic.nicovideo.jp/{article_type}/{article_id}"


def _is_article_type(article_url: str | None, article_type: str) -> bool:
    if article_url is None:
        return False

    normalized_url = _normalize_dic_url(article_url)
    if normalized_url is None:
        return False

    parts = [part for part in urlparse(normalized_url).path.split("/") if part]
    return len(parts) == 2 and parts[0] == article_type


def _extract_canonical_a_url(source_url: str, response_text: str) -> str | None:
    soup = BeautifulSoup(response_text, "lxml")
    canonical_tag = soup.find(
        "link",
        rel=lambda value: value and "canonical" in value,
    )
    if canonical_tag is None:
        return None

    href = canonical_tag.get("href", "").strip()
    if not href:
        return None

    canonical_url = _normalize_dic_url(urljoin(source_url, href))
    if not _is_article_type(canonical_url, "a"):
        return None

    return canonical_url


def resolve_id_article_url(article_url: str) -> str | None:
    normalized_url = _normalize_dic_url(article_url)
    if not _is_article_type(normalized_url, "id"):
        return None

    response = _fetch_response(normalized_url)
    effective_url = _normalize_dic_url(response.url)
    if _is_article_type(effective_url, "a"):
        return effective_url

    return _extract_canonical_a_url(normalized_url, response.text)


def fetch_page(url: str) -> BeautifulSoup:
    """
    指定URLを取得し BeautifulSoup を返す。
    200以外、timeout、request error は RuntimeError を送出する。
    """
    res = _fetch_response(url)

    if res.status_code != 200:
        raise RuntimeError(f"Failed to fetch {url} (status={res.status_code})")

    return BeautifulSoup(res.text, "lxml")
