from target_list import (
    import_targets_from_text_file,
    list_active_target_urls,
    register_target_url,
    validate_target_url,
)


def test_list_active_target_urls_reads_registered_targets_stably(tmp_path):
    target_db_path = tmp_path / "targets.db"

    register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))
    register_target_url("https://dic.nicovideo.jp/id/99999", str(target_db_path))

    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/12345",
        "https://dic.nicovideo.jp/id/99999",
    ]


def test_validate_target_url_accepts_minimally_valid_nicopedia_article_url():
    assert validate_target_url("https://dic.nicovideo.jp/a/12345") is True


def test_validate_target_url_rejects_non_article_shape():
    assert validate_target_url("https://dic.nicovideo.jp/a/12345/extra") is False
    assert validate_target_url("https://example.com/a/12345") is False
    assert validate_target_url("not-a-url") is False


def test_register_target_url_inserts_valid_target_into_registry(tmp_path):
    target_db_path = tmp_path / "targets.db"

    result = register_target_url(
        "https://dic.nicovideo.jp/a/12345",
        str(target_db_path),
    )

    assert result == "added"
    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/12345",
    ]


def test_register_target_url_suppresses_duplicate_identity(tmp_path):
    target_db_path = tmp_path / "targets.db"
    register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))

    result = register_target_url(
        "https://dic.nicovideo.jp/a/12345",
        str(target_db_path),
    )

    assert result == "duplicate"
    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/12345",
    ]


def test_register_target_url_rejects_invalid_target_without_writing(tmp_path):
    target_db_path = tmp_path / "targets.db"

    result = register_target_url("not-a-url", str(target_db_path))

    assert result == "invalid"
    assert target_db_path.exists() is False


def test_import_targets_from_text_file_is_one_shot_and_non_automatic(tmp_path):
    target_db_path = tmp_path / "targets.db"
    source_file = tmp_path / "targets.txt"
    source_file.write_text(
        "\n"
        "# legacy targets\n"
        "https://dic.nicovideo.jp/a/12345\n"
        "https://dic.nicovideo.jp/a/12345\n"
        "not-a-url\n"
        "https://dic.nicovideo.jp/id/777\n",
        encoding="utf-8",
    )

    result = import_targets_from_text_file(
        str(source_file),
        str(target_db_path),
    )

    assert result == {
        "source_path": str(source_file),
        "target_db_path": str(target_db_path),
        "processed": 4,
        "added": 2,
        "duplicate": 1,
        "reactivated": 0,
        "invalid": 1,
    }
    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/12345",
        "https://dic.nicovideo.jp/id/777",
    ]
