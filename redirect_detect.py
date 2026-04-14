import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


_META_REFRESH_RE = re.compile(r"^\s*\d+\s*;\s*url\s*=\s*(.+)\s*$", re.IGNORECASE)
_LOCATION_REPLACE_RE = re.compile(
    r"location\.replace\(\s*(['\"])(.*?)\1\s*\)",
    re.IGNORECASE,
)


def _normalize_redirect_url(candidate: str, base_url: str) -> str | None:
    raw = (candidate or "").strip().strip("'\"")
    if not raw:
        return None

    resolved = urljoin(base_url, raw)
    parsed = urlparse(resolved)
    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.netloc != "dic.nicovideo.jp":
        return None

    path = parsed.path.rstrip("/")
    if not path.startswith(("/a/", "/id/")):
        return None

    return f"{parsed.scheme}://{parsed.netloc}{path}"


def extract_redirect_url_from_soup(
    soup: BeautifulSoup,
    *,
    base_url: str,
) -> str | None:
    """
    Detect NicoNicoPedia article redirect and extract redirect target URL.

    Supported signals (bounded):
    - <meta http-equiv="refresh" content="0;URL=...">
    - location.replace("...") in <script>
    """

    meta = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
    if meta is not None:
        content = meta.get("content") or ""
        match = _META_REFRESH_RE.match(content)
        if match:
            normalized = _normalize_redirect_url(match.group(1), base_url)
            if normalized:
                return normalized

    scripts = soup.find_all("script")
    for script in scripts:
        text = script.string or script.get_text() or ""
        match = _LOCATION_REPLACE_RE.search(text)
        if not match:
            continue
        normalized = _normalize_redirect_url(match.group(2), base_url)
        if normalized:
            return normalized

    return None
