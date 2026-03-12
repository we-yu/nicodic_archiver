import requests
from bs4 import BeautifulSoup


# シンプルなUA指定（現状は固定値）
HEADERS = {"User-Agent": "Mozilla/5.0"}

# タイムアウト（秒）。HTTP層の境界として最低限の保護を行う。
DEFAULT_TIMEOUT = 10


def fetch_page(url: str) -> BeautifulSoup:
    """
    指定URLを取得し BeautifulSoup を返す。
    200以外、あるいはHTTPエラー時は RuntimeError を送出する。
    """
    try:
        res = requests.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT)
    except requests.Timeout as e:
        raise RuntimeError(f"Failed to fetch {url} (timeout)") from e
    except requests.RequestException as e:
        # requests 層のその他のエラーも RuntimeError にラップする。
        raise RuntimeError(f"Failed to fetch {url} (request error)") from e

    if res.status_code != 200:
        raise RuntimeError(f"Failed to fetch {url} (status={res.status_code})")

    return BeautifulSoup(res.text, "lxml")
