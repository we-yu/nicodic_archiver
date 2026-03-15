from target_list import load_target_urls


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
