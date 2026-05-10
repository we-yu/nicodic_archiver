from unittest.mock import patch

from target_list import (
    deactivate_target,
    handoff_redirected_target,
    import_targets_from_text_file,
    inspect_registered_target,
    list_registered_targets,
    list_active_target_urls,
    parse_target_identity,
    reactivate_target,
    register_target_url,
    validate_target_url,
)


def _mock_resolve_for_slug_numeric_pair(canonical_url: str, numeric_id: str):
    return {
        "ok": True,
        "canonical_target": {
            "article_url": canonical_url,
            "article_id": numeric_id,
            "article_type": "a",
        },
        "title": "Sample",
        "matched_by": "article_url",
        "normalized_input": canonical_url,
    }


def test_list_active_target_urls_reads_registered_targets_stably(tmp_path):
    target_db_path = tmp_path / "targets.db"

    with patch(
        "target_list.resolve_article_input",
        side_effect=[
            _mock_resolve_for_slug_numeric_pair(
                "https://dic.nicovideo.jp/a/12345",
                "11111",
            ),
            _mock_resolve_for_slug_numeric_pair(
                "https://dic.nicovideo.jp/a/osomatsu-san",
                "22222",
            ),
        ],
    ):
        register_target_url(
            "https://dic.nicovideo.jp/a/12345",
            str(target_db_path),
        )
        register_target_url(
            "https://dic.nicovideo.jp/id/99999",
            str(target_db_path),
        )

    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/12345",
        "https://dic.nicovideo.jp/a/osomatsu-san",
    ]


def test_validate_target_url_accepts_minimally_valid_nicopedia_article_url():
    assert validate_target_url("https://dic.nicovideo.jp/a/12345") is True


def test_validate_target_url_stays_syntax_only_for_id_input():
    ok = validate_target_url("https://dic.nicovideo.jp/id/5364158")

    assert ok is True


def test_parse_target_identity_remains_pure_for_id_input():
    identity = parse_target_identity("https://dic.nicovideo.jp/id/5364158")

    assert identity == {
        "article_id": "5364158",
        "article_type": "id",
        "canonical_url": "https://dic.nicovideo.jp/id/5364158",
    }


def test_validate_target_url_rejects_non_article_shape():
    assert validate_target_url("https://dic.nicovideo.jp/a/12345/extra") is False
    assert validate_target_url("https://example.com/a/12345") is False
    assert validate_target_url("not-a-url") is False


def test_register_target_url_inserts_valid_target_into_registry(tmp_path):
    target_db_path = tmp_path / "targets.db"

    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve_for_slug_numeric_pair(
            "https://dic.nicovideo.jp/a/12345",
            "5502789",
        ),
    ):
        result = register_target_url(
            "https://dic.nicovideo.jp/a/12345",
            str(target_db_path),
        )

    assert result == "added"
    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/12345",
    ]


def test_register_target_url_stores_resolved_a_target_for_id_input(tmp_path):
    target_db_path = tmp_path / "targets.db"

    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve_for_slug_numeric_pair(
            "https://dic.nicovideo.jp/a/osomatsu-san",
            "33333",
        ),
    ):
        result = register_target_url(
            "https://dic.nicovideo.jp/id/5364158",
            str(target_db_path),
        )

    assert result == "added"
    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/osomatsu-san",
    ]


def test_register_target_url_rejects_unresolved_id_input(tmp_path):
    target_db_path = tmp_path / "targets.db"

    with patch(
        "target_list.resolve_article_input",
        return_value={
            "ok": False,
            "failure_kind": "not_found",
            "normalized_input": "x",
        },
    ):
        result = register_target_url(
            "https://dic.nicovideo.jp/id/5364158",
            str(target_db_path),
        )

    assert result == "resolution_failure"
    assert target_db_path.exists() is False


def test_register_target_url_rejects_denylisted_numeric_id_input(tmp_path):
    target_db_path = tmp_path / "targets.db"

    result = register_target_url(
        "https://dic.nicovideo.jp/id/480340",
        str(target_db_path),
    )

    assert result == "denylisted"
    assert target_db_path.exists() is False


def test_register_target_url_rejects_denylisted_canonical_slug_url(tmp_path):
    target_db_path = tmp_path / "targets.db"

    result = register_target_url(
        "https://dic.nicovideo.jp/a/"
        "%3E%3E3%E3%81%8C%E7%90%86%E8%A7%A3%E3%81%A7"
        "%E3%81%8D%E3%82%8B%E3%81%93%E3%81%A8%E3%81%8C"
        "%E4%B8%8D%E5%B9%B8",
        str(target_db_path),
    )

    assert result == "denylisted"
    assert target_db_path.exists() is False


def test_register_target_url_denylisted_after_numeric_resolution(tmp_path):
    """Denylist (237789) must apply when only metadata reveals the numeric ID."""

    target_db_path = tmp_path / "targets.db"
    deny_url = (
        "https://dic.nicovideo.jp/a/4294967295"
    )

    with patch(
        "target_list.resolve_article_input",
        return_value={
            "ok": True,
            "canonical_target": {
                "article_url": deny_url,
                "article_id": "237789",
                "article_type": "a",
            },
            "title": "4294967295",
            "matched_by": "article_url",
            "normalized_input": deny_url,
        },
    ):
        result = register_target_url(
            deny_url,
            str(target_db_path),
        )

    assert result == "denylisted"


def test_register_target_url_suppresses_duplicate_identity(tmp_path):
    target_db_path = tmp_path / "targets.db"
    mock = _mock_resolve_for_slug_numeric_pair(
        "https://dic.nicovideo.jp/a/12345",
        "5502789",
    )
    with patch("target_list.resolve_article_input", return_value=mock):
        register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))

    with patch("target_list.resolve_article_input", return_value=mock):
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

    def _side_effect(article_input: str):
        if "a/12345" in article_input:
            return _mock_resolve_for_slug_numeric_pair(
                "https://dic.nicovideo.jp/a/12345",
                "5502789",
            )
        return _mock_resolve_for_slug_numeric_pair(
            "https://dic.nicovideo.jp/a/777-title",
            "8888888",
        )

    with patch(
        "target_list.resolve_article_input",
        side_effect=_side_effect,
    ):
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
        "denylisted": 0,
        "reactivated": 0,
        "invalid": 1,
        "resolution_failure": 0,
    }
    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/12345",
        "https://dic.nicovideo.jp/a/777-title",
    ]


def test_import_targets_from_text_file_reports_denylisted_skip(tmp_path):
    target_db_path = tmp_path / "targets.db"
    source_file = tmp_path / "targets.txt"
    source_file.write_text(
        "https://dic.nicovideo.jp/id/480340\n"
        "https://dic.nicovideo.jp/a/12345\n",
        encoding="utf-8",
    )

    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve_for_slug_numeric_pair(
            "https://dic.nicovideo.jp/a/12345",
            "5502789",
        ),
    ):
        result = import_targets_from_text_file(
            str(source_file),
            str(target_db_path),
        )

    assert result == {
        "source_path": str(source_file),
        "target_db_path": str(target_db_path),
        "processed": 2,
        "added": 1,
        "duplicate": 0,
        "denylisted": 1,
        "reactivated": 0,
        "invalid": 0,
        "resolution_failure": 0,
    }
    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/12345",
    ]


def test_list_registered_targets_includes_inactive_entries_when_requested(tmp_path):
    target_db_path = tmp_path / "targets.db"

    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve_for_slug_numeric_pair(
            "https://dic.nicovideo.jp/a/12345",
            "5502789",
        ),
    ):
        register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))

    deactivate_target("5502789", "a", str(target_db_path))

    all_entries = list_registered_targets(str(target_db_path), active_only=False)
    active_entries = list_registered_targets(str(target_db_path), active_only=True)

    assert len(all_entries) == 1
    assert all_entries[0]["is_active"] is False
    assert active_entries == []


def test_inspect_registered_target_returns_entry_by_identity(tmp_path):
    target_db_path = tmp_path / "targets.db"
    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve_for_slug_numeric_pair(
            "https://dic.nicovideo.jp/a/12345",
            "5502789",
        ),
    ):
        register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))

    entry = inspect_registered_target("5502789", "a", str(target_db_path))

    assert entry is not None
    assert entry["article_id"] == "5502789"
    assert entry["article_type"] == "a"
    assert entry["canonical_url"] == "https://dic.nicovideo.jp/a/12345"


def test_deactivate_and_reactivate_target_return_operator_facing_result(tmp_path):
    target_db_path = tmp_path / "targets.db"
    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve_for_slug_numeric_pair(
            "https://dic.nicovideo.jp/a/12345",
            "5502789",
        ),
    ):
        register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))

    deactivated = deactivate_target("5502789", "a", str(target_db_path))
    reactivated = reactivate_target("5502789", "a", str(target_db_path))

    assert deactivated["status"] == "deactivated"
    assert deactivated["entry"]["is_active"] is False
    assert reactivated["status"] == "activated"
    assert reactivated["entry"]["is_active"] is True


def test_handoff_redirected_target_deactivates_old_and_registers_new(tmp_path):
    target_db_path = tmp_path / "targets.db"
    with patch(
        "target_list.resolve_article_input",
        side_effect=[
            _mock_resolve_for_slug_numeric_pair(
                "https://dic.nicovideo.jp/a/12345",
                "5502789",
            ),
            _mock_resolve_for_slug_numeric_pair(
                "https://dic.nicovideo.jp/a/67890",
                "5502790",
            ),
        ],
    ):
        register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))
        result = handoff_redirected_target(
            "5502789",
            "a",
            "https://dic.nicovideo.jp/a/67890",
            str(target_db_path),
        )

    assert result["status"] == "redirected"
    assert result["entry"]["is_active"] is False
    assert result["entry"]["is_redirected"] is True
    assert result["entry"]["redirect_target_url"] == (
        "https://dic.nicovideo.jp/a/67890"
    )
    assert result["register_status"] == "added"
    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/67890",
    ]


def test_handoff_redirected_target_suppresses_duplicate_new_target(tmp_path):
    target_db_path = tmp_path / "targets.db"
    with patch(
        "target_list.resolve_article_input",
        side_effect=[
            _mock_resolve_for_slug_numeric_pair(
                "https://dic.nicovideo.jp/a/12345",
                "5502789",
            ),
            _mock_resolve_for_slug_numeric_pair(
                "https://dic.nicovideo.jp/a/67890",
                "5502790",
            ),
            _mock_resolve_for_slug_numeric_pair(
                "https://dic.nicovideo.jp/a/67890",
                "5502790",
            ),
        ],
    ):
        register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))
        register_target_url("https://dic.nicovideo.jp/a/67890", str(target_db_path))

        result = handoff_redirected_target(
            "5502789",
            "a",
            "https://dic.nicovideo.jp/a/67890",
            str(target_db_path),
        )

    assert result["status"] == "redirected"
    assert result["register_status"] == "duplicate"
    assert list_active_target_urls(str(target_db_path)) == [
        "https://dic.nicovideo.jp/a/67890",
    ]
