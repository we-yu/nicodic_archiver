import requests
from bs4 import BeautifulSoup


# シンプルなUA指定（現状は固定値）
HEADERS = {"User-Agent": "Mozilla/5.0"}

# HTTP層の境界として最低限の保護を行う
REQUEST_TIMEOUT_SECONDS = 10


def fetch_page(url: str) -> BeautifulSoup:
    """
    指定URLを取得し BeautifulSoup を返す。
    200以外、timeout、request error は RuntimeError を送出する。
    """
    try:
        res = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.Timeout as e:
        raise RuntimeError(
            f"Failed to fetch {url} (timeout={REQUEST_TIMEOUT_SECONDS}s)"
        ) from e
    except requests.RequestException as e:
        raise RuntimeError(
            f"Failed to fetch {url} ({type(e).__name__}: {e})"
        ) from e

    if res.status_code != 200:
        raise RuntimeError(f"Failed to fetch {url} (status={res.status_code})")

    return BeautifulSoup(res.text, "lxml")
