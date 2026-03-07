from bs4 import BeautifulSoup

import orchestrator


def test_build_bbs_base_url():
    article_url = "https://dic.nicovideo.jp/a/プロイセン(APヘタリア)"

    result = orchestrator.build_bbs_base_url(article_url)

    assert result == "https://dic.nicovideo.jp/b/a/プロイセン(APヘタリア)/"


def test_fetch_article_metadata_uses_fetch_page(monkeypatch):
    called = {}
    html = """
    <html>
      <head>
        <meta property="og:title" content="テスト記事とは (単語記事)" />
        <meta property="og:url" content="https://dic.nicovideo.jp/a/4470620" />
      </head>
    </html>
    """

    def fake_fetch_page(url):
        called["url"] = url
        return BeautifulSoup(html, "lxml")

    monkeypatch.setattr(orchestrator, "fetch_page", fake_fetch_page)

    result = orchestrator.fetch_article_metadata(
        "https://dic.nicovideo.jp/a/4470620"
    )

    assert called["url"] == "https://dic.nicovideo.jp/a/4470620"
    assert result == ("4470620", "a", "テスト記事")


def test_collect_all_responses_paginates(monkeypatch):
    fetched_urls = []
    sleep_calls = []
    page_responses = {
        "page1": [{"res_no": 1}, {"res_no": 2}],
        "page2": [{"res_no": 3}],
        "page3": [],
    }
    page_order = iter(["page1", "page2", "page3"])

    def fake_fetch_page(url):
        fetched_urls.append(url)
        return next(page_order)

    def fake_parse_responses(page):
        return page_responses[page]

    def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(orchestrator, "fetch_page", fake_fetch_page)
    monkeypatch.setattr(orchestrator, "parse_responses", fake_parse_responses)
    monkeypatch.setattr(orchestrator.time, "sleep", fake_sleep)

    result = orchestrator.collect_all_responses(
        "https://dic.nicovideo.jp/b/a/4470620/"
    )

    assert fetched_urls == [
        "https://dic.nicovideo.jp/b/a/4470620/1-",
        "https://dic.nicovideo.jp/b/a/4470620/3-",
        "https://dic.nicovideo.jp/b/a/4470620/4-",
    ]
    assert sleep_calls == [1, 1]
    assert result == [{"res_no": 1}, {"res_no": 2}, {"res_no": 3}]
