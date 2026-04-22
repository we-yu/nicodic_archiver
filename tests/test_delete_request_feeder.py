import json
import sqlite3
from unittest.mock import patch

from delete_request_feeder import (
    append_batch_targets,
    classify_delete_request_url,
    extract_delete_request_urls,
    format_delete_request_feed_inspect_lines,
    normalize_supported_delete_request_input,
    resolve_internal_article_id_input,
    run_delete_request_feeder,
    sanitize_delete_request_candidate,
)


def test_extract_delete_request_urls_reads_explicit_urls_only():
    text = (
        "【掲示板URL】https://dic.nicovideo.jp/a/j-pop\n"
        "補足 https://dic.nicovideo.jp/t/a/%E5%A5%B3%E6%80%A7】"
    )

    assert extract_delete_request_urls(text) == [
        "https://dic.nicovideo.jp/a/j-pop",
        "https://dic.nicovideo.jp/t/a/%E5%A5%B3%E6%80%A7",
    ]


def test_classify_and_normalize_supported_delete_request_urls():
    cases = [
        (
            "https://dic.nicovideo.jp/a/j-pop",
            "article_direct",
            "https://dic.nicovideo.jp/a/j-pop",
        ),
        (
            "https://dic.nicovideo.jp/b/a/%E3%81%A4%E3%81%91%E9%BA%BA/1-",
            "article_board",
            "https://dic.nicovideo.jp/a/%E3%81%A4%E3%81%91%E9%BA%BA",
        ),
        (
            "https://dic.nicovideo.jp/t/b/a/%E8%B2%A1%E5%8B%99"
            "%E7%9C%81/691-?from=a_bbslook_5020395",
            "article_thread_board",
            "https://dic.nicovideo.jp/a/%E8%B2%A1%E5%8B%99%E7%9C%81",
        ),
        (
            "https://dic.nicovideo.jp/t/a/%E5%A5%B3%E6%80%A7",
            "article_thread_direct",
            "https://dic.nicovideo.jp/a/%E5%A5%B3%E6%80%A7",
        ),
    ]

    for raw_url, category, expected in cases:
        assert classify_delete_request_url(raw_url) == category
        assert normalize_supported_delete_request_input(raw_url, category) == expected


def test_classify_rejects_unsupported_delete_request_categories():
    assert classify_delete_request_url("https://dic.nicovideo.jp/v/sm12825985") == (
        "video"
    )
    assert classify_delete_request_url("https://dic.nicovideo.jp/u/688493") == (
        "user"
    )
    assert classify_delete_request_url(
        "https://dic.nicovideo.jp/l/%E3%82%B3%E3%83%BC%E3%83%AB"
    ) == "live"
    assert classify_delete_request_url("https://dic.nicovideo.jp/b/c/co2078137/") == (
        "community_board"
    )
    assert classify_delete_request_url("https://dic.nicovideo.jp/a/") == (
        "malformed"
    )


def test_sanitize_delete_request_candidate_strips_control_contamination():
    assert sanitize_delete_request_candidate(
        " \r\nhttps://dic.nicovideo.jp/a/j-pop%0D%0A\n"
    ) == "https://dic.nicovideo.jp/a/j-pop"
    assert sanitize_delete_request_candidate("\r\n\x01\x02") is None


def test_resolve_internal_article_id_input_uses_saved_article_row(tmp_path):
    archive_db_path = tmp_path / "archive.db"
    conn = sqlite3.connect(archive_db_path)
    conn.execute(
        """
        CREATE TABLE articles (
            article_id TEXT,
            article_type TEXT,
            title TEXT,
            canonical_url TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO articles (article_id, article_type, title, canonical_url)
        VALUES (?, ?, ?, ?)
        """,
        (
            "5728993",
            "a",
            "J-POP",
            "https://dic.nicovideo.jp/a/j-pop",
        ),
    )
    conn.commit()
    conn.close()

    assert resolve_internal_article_id_input(
        "5728993",
        archive_db_path=str(archive_db_path),
    ) == "https://dic.nicovideo.jp/a/j-pop"


def test_run_delete_request_feeder_updates_state_and_deduplicates(tmp_path):
    state_path = tmp_path / "feed_state.json"
    responses = [
        {
            "res_no": 12,
            "body": (
                "【掲示板URL】https://dic.nicovideo.jp/a/j-pop\n"
                "重複 https://dic.nicovideo.jp/a/j-pop"
            ),
        }
    ]

    with patch(
        "delete_request_feeder._load_delete_request_responses",
        return_value=responses,
    ), patch(
        "delete_request_feeder.resolve_article_input",
        return_value={
            "ok": True,
            "canonical_target": {
                "article_url": "https://dic.nicovideo.jp/a/j-pop",
                "article_id": "j-pop",
                "article_type": "a",
            },
        },
    ), patch(
        "delete_request_feeder.register_target_url",
        return_value="added",
    ) as register_mock:
        summary = run_delete_request_feeder(
            "targets.db",
            archive_db_path="archive.db",
            state_path=str(state_path),
        )

    assert summary["handed_off_candidates"] == 1
    assert summary["queued_target_urls"] == ["https://dic.nicovideo.jp/a/j-pop"]
    assert register_mock.call_count == 1
    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["last_processed_res_no"] == 12


def test_run_delete_request_feeder_skips_malformed_candidate_without_abort(
    tmp_path,
):
    state_path = tmp_path / "feed_state.json"
    responses = [
        {
            "res_no": 20,
            "body": "https://dic.nicovideo.jp/a/%0D%0A",
        }
    ]

    with patch(
        "delete_request_feeder._load_delete_request_responses",
        return_value=responses,
    ), patch(
        "delete_request_feeder.resolve_article_input",
    ) as resolve_mock, patch(
        "delete_request_feeder.register_target_url",
    ) as register_mock:
        summary = run_delete_request_feeder(
            "targets.db",
            archive_db_path="archive.db",
            state_path=str(state_path),
        )

    resolve_mock.assert_not_called()
    register_mock.assert_not_called()
    assert summary["skipped_invalid_candidates"] == 1
    assert summary["handed_off_candidates"] == 0


def test_run_delete_request_feeder_continues_after_candidate_resolver_failure(
    tmp_path,
):
    state_path = tmp_path / "feed_state.json"
    responses = [
        {
            "res_no": 21,
            "body": (
                "https://dic.nicovideo.jp/a/bad-one\n"
                "https://dic.nicovideo.jp/a/good-one"
            ),
        }
    ]

    with patch(
        "delete_request_feeder._load_delete_request_responses",
        return_value=responses,
    ), patch(
        "delete_request_feeder.resolve_article_input",
        side_effect=[ValueError("resolver broke"), {
            "ok": True,
            "canonical_target": {
                "article_url": "https://dic.nicovideo.jp/a/good-one",
                "article_id": "good-one",
                "article_type": "a",
            },
        }],
    ), patch(
        "delete_request_feeder.register_target_url",
        return_value="added",
    ) as register_mock:
        summary = run_delete_request_feeder(
            "targets.db",
            archive_db_path="archive.db",
            state_path=str(state_path),
        )

    assert summary["skipped_resolution_failures"] == 1
    assert summary["queued_target_urls"] == [
        "https://dic.nicovideo.jp/a/good-one"
    ]
    assert register_mock.call_count == 1


def test_run_delete_request_feeder_continues_after_upstream_fetch_failure(
    tmp_path,
):
    state_path = tmp_path / "feed_state.json"
    responses = [
        {
            "res_no": 22,
            "body": (
                "https://dic.nicovideo.jp/a/slow-one\n"
                "https://dic.nicovideo.jp/a/good-two"
            ),
        }
    ]

    with patch(
        "delete_request_feeder._load_delete_request_responses",
        return_value=responses,
    ), patch(
        "delete_request_feeder.resolve_article_input",
        side_effect=[RuntimeError("Failed to fetch upstream (status=500)"), {
            "ok": True,
            "canonical_target": {
                "article_url": "https://dic.nicovideo.jp/a/good-two",
                "article_id": "good-two",
                "article_type": "a",
            },
        }],
    ), patch(
        "delete_request_feeder.register_target_url",
        return_value="added",
    ):
        summary = run_delete_request_feeder(
            "targets.db",
            archive_db_path="archive.db",
            state_path=str(state_path),
        )

    assert summary["skipped_resolution_failures"] == 1
    assert summary["processed_candidates"] == 2
    assert summary["registered_candidates"] == 1


def test_run_delete_request_feeder_continues_after_registration_failure(
    tmp_path,
):
    state_path = tmp_path / "feed_state.json"
    responses = [
        {
            "res_no": 23,
            "body": (
                "https://dic.nicovideo.jp/a/bad-register\n"
                "https://dic.nicovideo.jp/a/good-three"
            ),
        }
    ]

    with patch(
        "delete_request_feeder._load_delete_request_responses",
        return_value=responses,
    ), patch(
        "delete_request_feeder.resolve_article_input",
        side_effect=[
            {
                "ok": True,
                "canonical_target": {
                    "article_url": "https://dic.nicovideo.jp/a/bad-register",
                    "article_id": "bad-register",
                    "article_type": "a",
                },
            },
            {
                "ok": True,
                "canonical_target": {
                    "article_url": "https://dic.nicovideo.jp/a/good-three",
                    "article_id": "good-three",
                    "article_type": "a",
                },
            },
        ],
    ), patch(
        "delete_request_feeder.register_target_url",
        side_effect=[sqlite3.OperationalError("readonly"), "added"],
    ):
        summary = run_delete_request_feeder(
            "targets.db",
            archive_db_path="archive.db",
            state_path=str(state_path),
        )

    assert summary["skipped_registration_failures"] == 1
    assert summary["queued_target_urls"] == [
        "https://dic.nicovideo.jp/a/good-three"
    ]


def test_append_batch_targets_appends_only_new_urls_at_tail():
    assert append_batch_targets(
        [
            "https://dic.nicovideo.jp/a/base-1",
            "https://dic.nicovideo.jp/a/base-2",
        ],
        [
            "https://dic.nicovideo.jp/a/base-2",
            "https://dic.nicovideo.jp/a/new-3",
        ],
    ) == [
        "https://dic.nicovideo.jp/a/base-1",
        "https://dic.nicovideo.jp/a/base-2",
        "https://dic.nicovideo.jp/a/new-3",
    ]


def test_format_delete_request_feed_inspect_lines_is_stdout_friendly():
    lines = format_delete_request_feed_inspect_lines(
        {
            "candidates": [
                {
                    "res_no": 3,
                    "raw_url": "https://dic.nicovideo.jp/a/j-pop",
                    "category": "article_direct",
                    "accepted": True,
                    "normalized_input": "https://dic.nicovideo.jp/a/j-pop",
                },
                {
                    "res_no": 4,
                    "raw_url": "https://dic.nicovideo.jp/v/sm9",
                    "category": "video",
                    "accepted": False,
                    "normalized_input": None,
                },
            ],
            "summary": {
                "checked_from_res_no": 1,
                "checked_to_res_no": 4,
                "responses_checked": 2,
                "extracted_candidates": 2,
                "handed_off_candidates": 0,
                "updated_last_processed_res_no": 4,
            },
        }
    )

    assert lines[0].startswith("ACCEPT ")
    assert lines[1].startswith("REJECT ")
    assert lines[2].startswith("SUMMARY ")
    assert "processed_candidates=0" in lines[2]
    assert "skipped_invalid_candidates=0" in lines[2]
    assert "skipped_registration_failures=0" in lines[2]
