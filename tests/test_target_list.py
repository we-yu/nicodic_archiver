from target_list import add_target_url, load_target_urls, validate_target_url


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


def test_load_target_urls_trims_whitespace_and_ignores_comment_lines(tmp_path):
    target_file = tmp_path / "targets.txt"
    target_file.write_text(
        "  # comment line to ignore  \n"
        "  https://dic.nicovideo.jp/a/12345  \n"
        "\n"
        "\thttps://dic.nicovideo.jp/a/77777\t\n",
        encoding="utf-8",
    )

    assert load_target_urls(str(target_file)) == [
        "https://dic.nicovideo.jp/a/12345",
        "https://dic.nicovideo.jp/a/77777",
    ]


def test_validate_target_url_accepts_minimally_valid_nicopedia_article_url():
    assert validate_target_url("https://dic.nicovideo.jp/a/12345") is True


def test_validate_target_url_rejects_non_article_shape():
    assert validate_target_url("https://dic.nicovideo.jp/a/12345/extra") is False
    assert validate_target_url("https://example.com/a/12345") is False
    assert validate_target_url("not-a-url") is False


def test_add_target_url_appends_valid_target_to_text_list(tmp_path):
    target_file = tmp_path / "targets.txt"

    result = add_target_url("https://dic.nicovideo.jp/a/12345", str(target_file))

    assert result == "added"
    assert target_file.read_text(encoding="utf-8") == (
        "https://dic.nicovideo.jp/a/12345\n"
    )


def test_add_target_url_does_not_append_duplicate_target(tmp_path):
    target_file = tmp_path / "targets.txt"
    target_file.write_text("https://dic.nicovideo.jp/a/12345\n", encoding="utf-8")

    result = add_target_url("https://dic.nicovideo.jp/a/12345", str(target_file))

    assert result == "duplicate"
    assert target_file.read_text(encoding="utf-8") == (
        "https://dic.nicovideo.jp/a/12345\n"
    )


def test_add_target_url_appends_without_inserting_blank_line(tmp_path):
    target_file = tmp_path / "targets.txt"
    target_file.write_text("https://dic.nicovideo.jp/a/12345\n", encoding="utf-8")

    result = add_target_url("https://dic.nicovideo.jp/a/77777", str(target_file))

    assert result == "added"
    assert target_file.read_text(encoding="utf-8") == (
        "https://dic.nicovideo.jp/a/12345\n"
        "https://dic.nicovideo.jp/a/77777\n"
    )


def test_add_target_url_rejects_invalid_target_without_writing(tmp_path):
    target_file = tmp_path / "targets.txt"

    result = add_target_url("not-a-url", str(target_file))

    assert result == "invalid"
    assert target_file.exists() is False
