import requests
from bs4 import BeautifulSoup


# シンプルなUA指定（現状は固定値）
HEADERS = {"User-Agent": "Mozilla/5.0"}


def fetch_page(url: str) -> BeautifulSoup:
    """
    指定URLを取得し BeautifulSoup を返す。
    200以外は例外送出。
    ※ 現状ロジック変更なし
    """
    res = requests.get(url, headers=HEADERS)

    if res.status_code != 200:
        raise RuntimeError(f"Failed to fetch {url} (status={res.status_code})")

    return BeautifulSoup(res.text, "lxml")

