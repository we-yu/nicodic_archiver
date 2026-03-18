from target_list import add_target_url, load_target_urls, validate_article_url


def test_load_target_urls_reads_plain_text_targets_stably(tmp_path):
    target_file = tmp_path / "targets.txt"
    target_file.write_text(
        "\n"
        "# temporary scrape targets\n"
        "https://dic.nicovideo.jp/a/12345\n"
        "\n"
        "https://dic.nicovideo.jp/a/99999\n",
        encoding="utf-8",
    )

    assert load_target_urls(str(target_file)) == [
        "https://dic.nicovideo.jp/a/12345",
        "https://dic.nicovideo.jp/a/99999",
    ]


def test_load_target_urls_ignores_duplicate_lines_while_preserving_order(tmp_path):
    target_file = tmp_path / "targets.txt"
    target_file.write_text(
        "https://dic.nicovideo.jp/a/12345\n"
        "https://dic.nicovideo.jp/a/12345\n"
        "https://dic.nicovideo.jp/a/77777\n",
        encoding="utf-8",
    )

    assert load_target_urls(str(target_file)) == [
        "https://dic.nicovideo.jp/a/12345",
        "https://dic.nicovideo.jp/a/77777",
    ]


def test_validate_article_url_accepts_minimal_nicodic_article_url():
    ok, reason = validate_article_url(
        "https://dic.nicovideo.jp/a/12345",
    )
    assert ok is True
    assert reason == "ok"


def test_validate_article_url_rejects_non_article_paths():
    ok, reason = validate_article_url(
        "https://dic.nicovideo.jp/b/a/12345/",
    )
    assert ok is False
    assert reason in {"invalid_path", "unexpected_article_type"}


def test_add_target_url_appends_new_target(tmp_path):
    target_file = tmp_path / "targets.txt"
    added, reason = add_target_url(
        "https://dic.nicovideo.jp/a/12345",
        str(target_file),
    )
    assert added is True
    assert reason == "added"
    assert load_target_urls(str(target_file)) == [
        "https://dic.nicovideo.jp/a/12345"
    ]


def test_add_target_url_does_not_append_duplicates(tmp_path):
    target_file = tmp_path / "targets.txt"
    target_file.write_text(
        "https://dic.nicovideo.jp/a/12345\n",
        encoding="utf-8",
    )
    added, reason = add_target_url(
        "https://dic.nicovideo.jp/a/12345",
        str(target_file),
    )
    assert added is False
    assert reason == "duplicate"
    assert (
        target_file.read_text(encoding="utf-8")
        == "https://dic.nicovideo.jp/a/12345\n"
    )


def test_add_target_url_rejects_invalid_url_and_does_not_write(tmp_path):
    target_file = tmp_path / "targets.txt"
    added, reason = add_target_url("not-a-url", str(target_file))
    assert added is False
    assert reason != "added"
    assert not target_file.exists()
