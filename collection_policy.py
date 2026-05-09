import re
from urllib.parse import urlparse


DENYLISTED_ARTICLES = {
    "480340": {
        "title": ">>3が理解できることが不幸",
        "canonical_url": (
            "https://dic.nicovideo.jp/a/"
            "%3E%3E3%E3%81%8C%E7%90%86%E8%A7%A3%E3%81%A7"
            "%E3%81%8D%E3%82%8B%E3%81%93%E3%81%A8%E3%81%8C"
            "%E4%B8%8D%E5%B9%B8"
        ),
        "id_url": "https://dic.nicovideo.jp/id/480340",
    },
    "237789": {
        "title": "4294967295",
        "canonical_url": "https://dic.nicovideo.jp/a/4294967295",
        "id_url": "https://dic.nicovideo.jp/id/237789",
    },
}

DENYLIST_ARTICLE_IDS = frozenset(DENYLISTED_ARTICLES)

_ID_URL_RE = re.compile(r"^/id/([0-9]+)$")

_DENYLIST_URL_TO_ID = {
    article["canonical_url"]: article_id
    for article_id, article in DENYLISTED_ARTICLES.items()
}
_DENYLIST_URL_TO_ID.update(
    {
        article["id_url"]: article_id
        for article_id, article in DENYLISTED_ARTICLES.items()
    }
)


def _numeric_id_from_url(article_url: str | None) -> str | None:
    if not article_url:
        return None

    exact_match = _DENYLIST_URL_TO_ID.get(article_url)
    if exact_match is not None:
        return exact_match

    parsed = urlparse(article_url)
    match = _ID_URL_RE.match(parsed.path)
    if match is None:
        return None

    return match.group(1)


def find_denylisted_article_id(
    *,
    article_id: str | None = None,
    article_url: str | None = None,
) -> str | None:
    if article_id and article_id.isdigit() and article_id in DENYLIST_ARTICLE_IDS:
        return article_id

    url_article_id = _numeric_id_from_url(article_url)
    if url_article_id in DENYLIST_ARTICLE_IDS:
        return url_article_id

    return None
