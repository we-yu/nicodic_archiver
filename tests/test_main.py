import main


def test_main_dispatches_to_inspect(monkeypatch):
    calls = {}

    def fake_inspect_article(article_id, article_type, last_n=None):
        calls["inspect"] = (article_id, article_type, last_n)

    def fake_run_scrape(article_url):
        calls["run_scrape"] = article_url

    monkeypatch.setattr(main, "inspect_article", fake_inspect_article)
    monkeypatch.setattr(main, "run_scrape", fake_run_scrape)
    monkeypatch.setattr(
        main.sys,
        "argv",
        ["main.py", "inspect", "4470620", "a", "--last", "10"],
    )

    main.main()

    assert calls["inspect"] == ("4470620", "a", 10)
    assert "run_scrape" not in calls


def test_main_dispatches_to_run_scrape(monkeypatch):
    calls = {}

    def fake_inspect_article(article_id, article_type, last_n=None):
        calls["inspect"] = (article_id, article_type, last_n)

    def fake_run_scrape(article_url):
        calls["run_scrape"] = article_url

    monkeypatch.setattr(main, "inspect_article", fake_inspect_article)
    monkeypatch.setattr(main, "run_scrape", fake_run_scrape)
    monkeypatch.setattr(
        main.sys,
        "argv",
        ["main.py", "https://dic.nicovideo.jp/a/4470620"],
    )

    main.main()

    assert calls["run_scrape"] == "https://dic.nicovideo.jp/a/4470620"
    assert "inspect" not in calls
