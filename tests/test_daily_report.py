"""Focused offline tests for TASK052 daily Slack runtime report."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from daily_report import (
    aggregate_batch_logs_for_day,
    attempt_daily_runtime_report,
    attach_sources_to_targets,
    format_daily_report_message,
    load_last_sent_report_date,
    parse_completed_batch_log,
    query_targets_created_on_day,
    report_date_for_attempt,
    save_last_sent_report_date,
)
from storage import init_db, register_target
from target_addition_log import (
    append_target_added_event,
    parse_target_addition_line,
    read_target_addition_events,
)
from target_list import register_target_url


def _completed_batch_log(
    *,
    ended_at: str,
    processed: int = 10,
    hit: int = 2,
    warn: int = 1,
    fail: int = 0,
    new_responses: int = 5,
) -> str:
    return "\n".join(
        [
            "BATCH_RUN_START",
            "  run_id=abc123",
            "BATCH_RUN_END",
            "  run_id=abc123",
            f"  ended_at={ended_at}",
            f"  processed_targets={processed}",
            "  final_status=success",
            "BATCH_DIGEST",
            f"  H={hit}",
            "  OK0=0",
            f"  W={warn}",
            f"  F={fail}",
            "  S=0",
            f"  NEW={new_responses}",
            "  UOBS=0",
            "",
        ]
    )


def _seed_target(
    db_path: Path,
    article_id: str,
    *,
    title: str = "",
    created_at: str,
    canonical_url: str | None = None,
):
    conn = init_db(str(db_path))
    try:
        url = canonical_url or (
            f"https://dic.nicovideo.jp/a/{article_id}"
        )
        register_target(conn, article_id, "a", url, title=title or None)
        conn.execute(
            "UPDATE target SET created_at=? WHERE article_id=?",
            (created_at, article_id),
        )
        conn.commit()
    finally:
        conn.close()


def _mock_resolve(article_id: str, title: str = "Sample"):
    url = f"https://dic.nicovideo.jp/a/{article_id}"
    return {
        "ok": True,
        "canonical_target": {
            "article_url": url,
            "article_id": article_id,
            "article_type": "a",
        },
        "title": title,
        "matched_by": "article_url",
        "normalized_input": url,
        "observed_max_res_no": None,
    }


def test_parse_one_completed_batch_log():
    text = _completed_batch_log(
        ended_at="2026-07-09T12:00:00+00:00",
        processed=42,
        hit=3,
        warn=2,
        fail=1,
        new_responses=9,
    )
    parsed = parse_completed_batch_log(text)
    assert parsed is not None
    assert parsed["processed_targets"] == 42
    assert parsed["hit"] == 3
    assert parsed["warn"] == 2
    assert parsed["fail"] == 1
    assert parsed["new_responses"] == 9
    assert parsed["ended_at"].date() == date(2026, 7, 9)


def test_aggregate_multiple_completed_batch_logs(tmp_path):
    log_dir = tmp_path / "batch_runs"
    log_dir.mkdir()
    (log_dir / "batch_a.log").write_text(
        _completed_batch_log(
            ended_at="2026-07-09T01:00:00+00:00",
            processed=10,
            hit=1,
            warn=0,
            fail=0,
            new_responses=4,
        ),
        encoding="utf-8",
    )
    (log_dir / "batch_b.log").write_text(
        _completed_batch_log(
            ended_at="2026-07-09T23:00:00+00:00",
            processed=20,
            hit=2,
            warn=1,
            fail=3,
            new_responses=8,
        ),
        encoding="utf-8",
    )
    totals = aggregate_batch_logs_for_day(log_dir, date(2026, 7, 9))
    assert totals["completed_runs"] == 2
    assert totals["processed_targets"] == 30
    assert totals["hit"] == 3
    assert totals["warn"] == 1
    assert totals["fail"] == 3
    assert totals["new_responses"] == 12


def test_aggregate_excludes_runs_outside_report_utc_date(tmp_path):
    log_dir = tmp_path / "batch_runs"
    log_dir.mkdir()
    (log_dir / "batch_in.log").write_text(
        _completed_batch_log(ended_at="2026-07-09T00:00:00+00:00"),
        encoding="utf-8",
    )
    (log_dir / "batch_out.log").write_text(
        _completed_batch_log(ended_at="2026-07-10T00:00:00+00:00"),
        encoding="utf-8",
    )
    (log_dir / "batch_prev.log").write_text(
        _completed_batch_log(ended_at="2026-07-08T23:59:59+00:00"),
        encoding="utf-8",
    )
    totals = aggregate_batch_logs_for_day(log_dir, date(2026, 7, 9))
    assert totals["completed_runs"] == 1


def test_aggregate_ignores_incomplete_current_batch_log(tmp_path):
    log_dir = tmp_path / "batch_runs"
    log_dir.mkdir()
    (log_dir / "batch_incomplete.log").write_text(
        "BATCH_RUN_START\n  run_id=live\n",
        encoding="utf-8",
    )
    (log_dir / "batch_ok.log").write_text(
        _completed_batch_log(ended_at="2026-07-09T05:00:00+00:00"),
        encoding="utf-8",
    )
    totals = aggregate_batch_logs_for_day(log_dir, date(2026, 7, 9))
    assert totals["completed_runs"] == 1


def test_aggregate_contains_malformed_batch_log(tmp_path):
    log_dir = tmp_path / "batch_runs"
    log_dir.mkdir()
    (log_dir / "batch_bad.log").write_text(
        "BATCH_RUN_END\n  ended_at=not-a-date\nBATCH_DIGEST\n  H=1\n",
        encoding="utf-8",
    )
    (log_dir / "batch_ok.log").write_text(
        _completed_batch_log(
            ended_at="2026-07-09T05:00:00+00:00",
            processed=7,
        ),
        encoding="utf-8",
    )
    totals = aggregate_batch_logs_for_day(log_dir, date(2026, 7, 9))
    assert totals["completed_runs"] == 1
    assert totals["processed_targets"] == 7


def test_query_targets_created_on_report_day(tmp_path):
    db_path = tmp_path / "targets.db"
    _seed_target(
        db_path,
        "100",
        title="In Day",
        created_at="2026-07-09 12:00:00",
    )
    _seed_target(
        db_path,
        "200",
        title="Next Day",
        created_at="2026-07-10 00:00:00",
    )
    _seed_target(
        db_path,
        "050",
        title="Prev Day",
        created_at="2026-07-08 23:59:59",
    )
    rows = query_targets_created_on_day(db_path, date(2026, 7, 9))
    assert [row["article_id"] for row in rows] == ["100"]
    assert rows[0]["title"] == "In Day"


def test_query_targets_respects_sqlite_created_at_semantics(tmp_path):
    db_path = tmp_path / "targets.db"
    # Production insert path uses SQLite CURRENT_TIMESTAMP style.
    _seed_target(
        db_path,
        "301",
        title="SQLite UTC",
        created_at="2026-07-09 00:00:00",
    )
    # ISO-with-offset also accepted (test/seed style).
    _seed_target(
        db_path,
        "302",
        title="ISO UTC",
        created_at="2026-07-09T23:59:59+00:00",
    )
    rows = query_targets_created_on_day(db_path, date(2026, 7, 9))
    assert [row["article_id"] for row in rows] == ["301", "302"]


def test_target_reporting_independent_of_completed_runs(tmp_path):
    db_path = tmp_path / "targets.db"
    log_dir = tmp_path / "batch_runs"
    log_dir.mkdir()
    _seed_target(
        db_path,
        "400",
        title="Web Only",
        created_at="2026-07-09 08:00:00",
    )
    metrics = aggregate_batch_logs_for_day(log_dir, date(2026, 7, 9))
    targets = query_targets_created_on_day(db_path, date(2026, 7, 9))
    message = format_daily_report_message(
        date(2026, 7, 9),
        metrics,
        attach_sources_to_targets(targets, date(2026, 7, 9)),
    )
    assert "Runs 0" in message
    assert "New targets 1" in message
    assert "Web Only" in message


def test_append_target_added_event_after_first_add(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TARGET_ADDITION_LOG_DIR",
        str(tmp_path / "additions"),
    )
    db_path = tmp_path / "targets.db"
    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve("501", title="First"),
    ):
        status = register_target_url(
            "https://dic.nicovideo.jp/a/501",
            str(db_path),
            source="web_user",
        )
    assert status == "added"
    events = read_target_addition_events(
        date.today(),
        log_dir=tmp_path / "additions",
    )
    # Event day is UTC today from append timestamp.
    utc_day = datetime.now(timezone.utc).date()
    events = read_target_addition_events(
        utc_day,
        log_dir=tmp_path / "additions",
    )
    assert len(events) == 1
    assert events[0]["article_id"] == "501"
    assert events[0]["source"] == "web_user"
    assert events[0]["title"] == "First"


def test_no_event_for_duplicate_registration(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TARGET_ADDITION_LOG_DIR",
        str(tmp_path / "additions"),
    )
    db_path = tmp_path / "targets.db"
    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve("502"),
    ):
        assert register_target_url(
            "https://dic.nicovideo.jp/a/502",
            str(db_path),
            source="web_user",
        ) == "added"
        assert register_target_url(
            "https://dic.nicovideo.jp/a/502",
            str(db_path),
            source="web_user",
        ) == "duplicate"
    utc_day = datetime.now(timezone.utc).date()
    events = read_target_addition_events(
        utc_day,
        log_dir=tmp_path / "additions",
    )
    assert len(events) == 1


def test_no_event_for_reactivation(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TARGET_ADDITION_LOG_DIR",
        str(tmp_path / "additions"),
    )
    db_path = tmp_path / "targets.db"
    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve("503"),
    ):
        assert register_target_url(
            "https://dic.nicovideo.jp/a/503",
            str(db_path),
            source="operator",
        ) == "added"
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute(
                "UPDATE target SET is_active=0 WHERE article_id=?",
                ("503",),
            )
            conn.commit()
        finally:
            conn.close()
        assert register_target_url(
            "https://dic.nicovideo.jp/a/503",
            str(db_path),
            source="operator",
        ) == "reactivated"
    utc_day = datetime.now(timezone.utc).date()
    events = read_target_addition_events(
        utc_day,
        log_dir=tmp_path / "additions",
    )
    assert len(events) == 1


def test_event_log_failure_does_not_fail_registration(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv(
        "TARGET_ADDITION_LOG_DIR",
        str(tmp_path / "additions"),
    )
    db_path = tmp_path / "targets.db"
    with patch(
        "target_list.resolve_article_input",
        return_value=_mock_resolve("504"),
    ), patch(
        "target_addition_log.Path.open",
        side_effect=OSError("disk full"),
    ):
        with pytest.warns(RuntimeWarning):
            status = register_target_url(
                "https://dic.nicovideo.jp/a/504",
                str(db_path),
                source="web_user",
            )
    assert status == "added"
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT article_id FROM target WHERE article_id=?",
            ("504",),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None


def test_malformed_event_line_containment(tmp_path):
    day = date(2026, 7, 9)
    path = (
        tmp_path
        / f"target_additions_{day.isoformat()}.jsonl"
    )
    path.write_text(
        "\n".join(
            [
                "{not-json",
                json.dumps(
                    {
                        "ts": "2026-07-09T01:00:00Z",
                        "event": "target_added",
                        "source": "web_user",
                        "article_id": "600",
                        "title": "Ok",
                    }
                ),
                json.dumps({"event": "other", "article_id": "601"}),
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    events = read_target_addition_events(day, log_dir=tmp_path)
    assert len(events) == 1
    assert events[0]["article_id"] == "600"
    assert parse_target_addition_line("{bad") is None


def test_source_matching_and_unknown_and_duplicates(tmp_path):
    day = date(2026, 7, 9)
    path = (
        tmp_path
        / f"target_additions_{day.isoformat()}.jsonl"
    )
    lines = [
        json.dumps(
            {
                "ts": "2026-07-09T01:00:00Z",
                "event": "target_added",
                "source": "delete_feeder",
                "article_id": "701",
                "article_type": "a",
            }
        ),
        json.dumps(
            {
                "ts": "2026-07-09T02:00:00Z",
                "event": "target_added",
                "source": "hot_word",
                "article_id": "701",
                "article_type": "a",
            }
        ),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    targets = [
        {
            "article_id": "701",
            "article_type": "a",
            "title": "Matched",
            "canonical_url": "https://dic.nicovideo.jp/a/701",
        },
        {
            "article_id": "702",
            "article_type": "a",
            "title": "No Event",
            "canonical_url": "https://dic.nicovideo.jp/a/702",
        },
    ]
    enriched = attach_sources_to_targets(
        targets,
        day,
        log_dir=tmp_path,
    )
    assert len(enriched) == 2
    assert enriched[0]["source"] == "delete_feeder"
    assert enriched[1]["source"] == "unknown"


def test_source_match_prefers_article_id_and_type(tmp_path):
    day = date(2026, 7, 9)
    path = (
        tmp_path
        / f"target_additions_{day.isoformat()}.jsonl"
    )
    lines = [
        json.dumps(
            {
                "ts": "2026-07-09T01:00:00Z",
                "event": "target_added",
                "source": "delete_feeder",
                "article_id": "800",
                "article_type": "a",
            }
        ),
        json.dumps(
            {
                "ts": "2026-07-09T01:05:00Z",
                "event": "target_added",
                "source": "hot_word",
                "article_id": "800",
                "article_type": "u",
            }
        ),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    targets = [
        {
            "article_id": "800",
            "article_type": "a",
            "title": "Article A",
        },
        {
            "article_id": "800",
            "article_type": "u",
            "title": "Article U",
        },
    ]
    enriched = attach_sources_to_targets(
        targets,
        day,
        log_dir=tmp_path,
    )
    assert enriched[0]["source"] == "delete_feeder"
    assert enriched[1]["source"] == "hot_word"


def test_source_match_typed_event_does_not_cross_type(tmp_path):
    day = date(2026, 7, 9)
    path = (
        tmp_path
        / f"target_additions_{day.isoformat()}.jsonl"
    )
    path.write_text(
        json.dumps(
            {
                "ts": "2026-07-09T01:00:00Z",
                "event": "target_added",
                "source": "web_user",
                "article_id": "810",
                "article_type": "a",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    targets = [
        {
            "article_id": "810",
            "article_type": "a",
            "title": "Typed Match",
        },
        {
            "article_id": "810",
            "article_type": "u",
            "title": "Other Type",
        },
    ]
    enriched = attach_sources_to_targets(
        targets,
        day,
        log_dir=tmp_path,
    )
    assert enriched[0]["source"] == "web_user"
    assert enriched[1]["source"] == "unknown"


def test_source_match_untyped_event_is_safe_fallback(tmp_path):
    day = date(2026, 7, 9)
    path = (
        tmp_path
        / f"target_additions_{day.isoformat()}.jsonl"
    )
    path.write_text(
        json.dumps(
            {
                "ts": "2026-07-09T01:00:00Z",
                "event": "target_added",
                "source": "operator",
                "article_id": "820",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    targets = [
        {
            "article_id": "820",
            "article_type": "a",
            "title": "Legacy Event",
        },
    ]
    enriched = attach_sources_to_targets(
        targets,
        day,
        log_dir=tmp_path,
    )
    assert enriched[0]["source"] == "operator"
    # Target is retained even when matching via untyped fallback.
    assert enriched[0]["title"] == "Legacy Event"


def test_source_labels_for_major_paths(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "TARGET_ADDITION_LOG_DIR",
        str(tmp_path / "additions"),
    )
    db_path = tmp_path / "targets.db"
    cases = [
        ("801", "delete_feeder"),
        ("802", "hot_word"),
        ("803", "web_user"),
        ("804", "operator"),
    ]
    for article_id, source in cases:
        with patch(
            "target_list.resolve_article_input",
            return_value=_mock_resolve(article_id),
        ):
            assert register_target_url(
                f"https://dic.nicovideo.jp/a/{article_id}",
                str(db_path),
                source=source,
            ) == "added"
    utc_day = datetime.now(timezone.utc).date()
    events = {
        event["article_id"]: event["source"]
        for event in read_target_addition_events(
            utc_day,
            log_dir=tmp_path / "additions",
        )
    }
    assert events["801"] == "delete_feeder"
    assert events["802"] == "hot_word"
    assert events["803"] == "web_user"
    assert events["804"] == "operator"


def test_format_normal_slack_message():
    metrics = {
        "completed_runs": 8,
        "processed_targets": 52431,
        "new_responses": 612,
        "hit": 221,
        "warn": 4,
        "fail": 137,
    }
    targets = [
        {
            "article_id": "1",
            "title": "foo",
            "source": "delete_feeder",
        },
        {
            "article_id": "2",
            "title": "bar",
            "source": "delete_feeder",
        },
        {
            "article_id": "3",
            "title": "hoge",
            "source": "hot_word",
        },
        {
            "article_id": "4",
            "title": "piyo",
            "source": "web_user",
        },
    ]
    message = format_daily_report_message(
        date(2026, 7, 9),
        metrics,
        targets,
    )
    assert message.startswith(
        "📊 NicoArc daily report — 2026-07-09 UTC"
    )
    assert "Runs 8 | processed 52,431 | new responses 612" in message
    assert "HIT 221 / WARN 4 / FAIL 137" in message
    assert "New targets 4 (Delete 2 / HOT 1 / Web 1)" in message
    assert "1. foo — Delete Feeder" in message
    assert "4. piyo — Web input" in message


def test_format_zero_target_message():
    metrics = {
        "completed_runs": 8,
        "processed_targets": 52431,
        "new_responses": 612,
        "hit": 221,
        "warn": 4,
        "fail": 137,
    }
    message = format_daily_report_message(
        date(2026, 7, 9),
        metrics,
        [],
    )
    assert "New targets 0" in message
    assert "1." not in message


def test_format_zero_run_with_target():
    metrics = {
        "completed_runs": 0,
        "processed_targets": 0,
        "new_responses": 0,
        "hit": 0,
        "warn": 0,
        "fail": 0,
    }
    targets = [
        {
            "article_id": "1",
            "title": "foo",
            "source": "web_user",
        },
    ]
    message = format_daily_report_message(
        date(2026, 7, 9),
        metrics,
        targets,
    )
    assert "Runs 0 | processed 0 | new responses 0" in message
    assert "New targets 1 (Web 1)" in message
    assert "1. foo — Web input" in message


def test_format_ten_item_bound_and_overflow_and_truncation():
    metrics = {
        "completed_runs": 1,
        "processed_targets": 1,
        "new_responses": 0,
        "hit": 0,
        "warn": 0,
        "fail": 0,
    }
    long_title = "x" * 100
    targets = [
        {
            "article_id": str(i),
            "title": long_title if i == 0 else f"t{i}",
            "source": "operator",
        }
        for i in range(12)
    ]
    message = format_daily_report_message(
        date(2026, 7, 9),
        metrics,
        targets,
    )
    assert "New targets 12 (Other 12)" in message
    assert "10. " in message
    assert "11. " not in message
    assert "... and 2 more." in message
    assert "xxx..." in message
    assert long_title not in message


def test_same_report_date_not_sent_twice(tmp_path):
    state_path = tmp_path / "state.json"
    save_last_sent_report_date(date(2026, 7, 9), state_path)
    sent = []

    def fake_send(url, text, timeout_seconds):
        sent.append(text)

    result = attempt_daily_runtime_report(
        target_db_path=tmp_path / "targets.db",
        batch_log_dir=tmp_path / "batch_runs",
        state_path=state_path,
        now=datetime(2026, 7, 10, 0, 5, tzinfo=timezone.utc),
        send_fn=fake_send,
        enabled=True,
        webhook_url="https://hooks.example/x",
    )
    assert result["reason"] == "already_sent"
    assert sent == []
    assert load_last_sent_report_date(state_path) == date(2026, 7, 9)


def test_successful_send_updates_state(tmp_path):
    state_path = tmp_path / "state.json"
    (tmp_path / "batch_runs").mkdir()
    init_db(str(tmp_path / "targets.db")).close()
    sent = []

    def fake_send(url, text, timeout_seconds):
        sent.append(text)

    result = attempt_daily_runtime_report(
        target_db_path=tmp_path / "targets.db",
        batch_log_dir=tmp_path / "batch_runs",
        state_path=state_path,
        now=datetime(2026, 7, 10, 0, 5, tzinfo=timezone.utc),
        send_fn=fake_send,
        enabled=True,
        webhook_url="https://hooks.example/x",
    )
    assert result["sent"] is True
    assert result["reason"] == "sent"
    assert len(sent) == 1
    assert "2026-07-09 UTC" in sent[0]
    assert load_last_sent_report_date(state_path) == date(2026, 7, 9)


def test_failed_send_does_not_update_state_and_allows_retry(tmp_path):
    state_path = tmp_path / "state.json"
    (tmp_path / "batch_runs").mkdir()
    init_db(str(tmp_path / "targets.db")).close()

    def fail_send(url, text, timeout_seconds):
        raise OSError("network down")

    with pytest.warns(
        RuntimeWarning,
        match=r"daily_report_failed: OSError",
    ):
        first = attempt_daily_runtime_report(
            target_db_path=tmp_path / "targets.db",
            batch_log_dir=tmp_path / "batch_runs",
            state_path=state_path,
            now=datetime(2026, 7, 10, 0, 5, tzinfo=timezone.utc),
            send_fn=fail_send,
            enabled=True,
            webhook_url="https://hooks.example/secret",
        )
    assert first["reason"] == "failed"
    assert load_last_sent_report_date(state_path) is None

    sent = []

    def ok_send(url, text, timeout_seconds):
        sent.append(text)
        assert "secret" not in text

    second = attempt_daily_runtime_report(
        target_db_path=tmp_path / "targets.db",
        batch_log_dir=tmp_path / "batch_runs",
        state_path=state_path,
        now=datetime(2026, 7, 10, 1, 5, tzinfo=timezone.utc),
        send_fn=ok_send,
        enabled=True,
        webhook_url="https://hooks.example/secret",
    )
    assert second["sent"] is True
    assert len(sent) == 1
    assert load_last_sent_report_date(state_path) == date(2026, 7, 9)


def test_disabled_and_missing_webhook_perform_no_send(tmp_path):
    sent = []

    def fake_send(url, text, timeout_seconds):
        sent.append(text)

    disabled = attempt_daily_runtime_report(
        target_db_path=tmp_path / "targets.db",
        batch_log_dir=tmp_path / "batch_runs",
        state_path=tmp_path / "state.json",
        send_fn=fake_send,
        enabled=False,
        webhook_url="https://hooks.example/x",
    )
    missing = attempt_daily_runtime_report(
        target_db_path=tmp_path / "targets.db",
        batch_log_dir=tmp_path / "batch_runs",
        state_path=tmp_path / "state.json",
        send_fn=fake_send,
        enabled=True,
        webhook_url="",
    )
    assert disabled["reason"] == "disabled"
    assert missing["reason"] == "missing_webhook"
    assert sent == []


def test_daily_report_independent_of_issue_report_enabled(
    tmp_path,
    monkeypatch,
):
    """Daily report needs its own enable flag + webhook, not issue-report."""
    (tmp_path / "batch_runs").mkdir()
    init_db(str(tmp_path / "targets.db")).close()
    monkeypatch.setenv("NICOARC_DAILY_REPORT_ENABLED", "1")
    monkeypatch.setenv("NICOARC_ISSUE_REPORT_ENABLED", "0")
    monkeypatch.setenv(
        "NICOARC_ISSUE_REPORT_SLACK_WEBHOOK_URL",
        "https://hooks.example/daily",
    )
    sent = []

    def fake_send(url, text, timeout_seconds):
        sent.append({"url": url, "text": text})

    result = attempt_daily_runtime_report(
        target_db_path=tmp_path / "targets.db",
        batch_log_dir=tmp_path / "batch_runs",
        state_path=tmp_path / "state.json",
        now=datetime(2026, 7, 10, 0, 5, tzinfo=timezone.utc),
        send_fn=fake_send,
    )
    assert result["sent"] is True
    assert result["reason"] == "sent"
    assert len(sent) == 1
    assert "daily" in sent[0]["url"]
    assert "2026-07-09 UTC" in sent[0]["text"]


def test_report_date_for_attempt_is_previous_utc_day():
    assert report_date_for_attempt(
        datetime(2026, 7, 10, 0, 5, tzinfo=timezone.utc)
    ) == date(2026, 7, 9)


def test_append_event_direct_helper(tmp_path):
    append_target_added_event(
        article_id="900",
        title="Direct",
        source="import",
        log_dir=tmp_path,
        now=datetime(2026, 7, 9, 8, 0, tzinfo=timezone.utc),
    )
    events = read_target_addition_events(
        date(2026, 7, 9),
        log_dir=tmp_path,
    )
    assert events[0]["source"] == "import"


def test_daily_report_runs_before_both_feeders(tmp_path, monkeypatch):
    import main as main_module
    from orchestrator import ScrapeResult

    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    order: list[str] = []

    def report_side_effect(**kwargs):
        order.append("daily_report")
        return {
            "attempted": False,
            "sent": False,
            "skipped": True,
            "reason": "disabled",
            "report_date": None,
        }

    def delete_side_effect(*args, **kwargs):
        order.append("delete_feeder")
        return {
            "checked_from_res_no": 1,
            "checked_to_res_no": None,
            "responses_checked": 0,
            "extracted_candidates": 0,
            "handed_off_candidates": 0,
            "updated_last_processed_res_no": 0,
            "queued_target_urls": [],
            "added_targets": 0,
            "reactivated_targets": 0,
            "duplicate_targets": 0,
            "invalid_targets": 0,
        }

    def hot_side_effect(*args, **kwargs):
        order.append("hot_word")
        return {
            "source_url": "https://example.test",
            "recent_weeks": 12,
            "fetch_ok": True,
            "extracted_candidates": 0,
            "unique_candidates": 0,
            "candidate_urls": [],
            "added_targets": 0,
            "reactivated_targets": 0,
            "duplicate_targets": 0,
            "denylisted_candidates": 0,
            "invalid_candidates": 0,
            "resolution_failures": 0,
            "registration_failures": 0,
            "queued_target_urls": [],
        }

    with patch(
        "main.attempt_daily_runtime_report",
        side_effect=report_side_effect,
    ), patch(
        "main.run_delete_request_feeder",
        side_effect=delete_side_effect,
    ), patch(
        "main.run_hot_word_feeder",
        side_effect=hot_side_effect,
    ), patch(
        "main.list_active_target_urls",
        return_value=["https://dic.nicovideo.jp/a/1"],
    ), patch(
        "main.list_registered_targets",
        return_value=[
            {
                "canonical_url": "https://dic.nicovideo.jp/a/1",
                "article_id": "1",
            },
        ],
    ), patch(
        "main.run_scrape",
        return_value=ScrapeResult(True, "ok", article_title="One"),
    ):
        main_module.run_batch_scrape("targets.db")

    assert order == ["daily_report", "delete_feeder", "hot_word"]


def test_daily_report_failure_still_reaches_feeders(tmp_path, monkeypatch):
    import main as main_module
    from orchestrator import ScrapeResult

    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    calls: list[str] = []

    def boom(**kwargs):
        calls.append("daily_report")
        raise RuntimeError("report boom")

    def delete_side_effect(*args, **kwargs):
        calls.append("delete_feeder")
        return {
            "checked_from_res_no": 1,
            "checked_to_res_no": None,
            "responses_checked": 0,
            "extracted_candidates": 0,
            "handed_off_candidates": 0,
            "updated_last_processed_res_no": 0,
            "queued_target_urls": [],
            "added_targets": 0,
            "reactivated_targets": 0,
            "duplicate_targets": 0,
            "invalid_targets": 0,
        }

    def hot_side_effect(*args, **kwargs):
        calls.append("hot_word")
        return {
            "source_url": "https://example.test",
            "recent_weeks": 12,
            "fetch_ok": True,
            "extracted_candidates": 0,
            "unique_candidates": 0,
            "candidate_urls": [],
            "added_targets": 0,
            "reactivated_targets": 0,
            "duplicate_targets": 0,
            "denylisted_candidates": 0,
            "invalid_candidates": 0,
            "resolution_failures": 0,
            "registration_failures": 0,
            "queued_target_urls": [],
        }

    with patch(
        "main.attempt_daily_runtime_report",
        side_effect=boom,
    ), patch(
        "main.run_delete_request_feeder",
        side_effect=delete_side_effect,
    ), patch(
        "main.run_hot_word_feeder",
        side_effect=hot_side_effect,
    ), patch(
        "main.list_active_target_urls",
        return_value=["https://dic.nicovideo.jp/a/1"],
    ), patch(
        "main.list_registered_targets",
        return_value=[
            {
                "canonical_url": "https://dic.nicovideo.jp/a/1",
                "article_id": "1",
            },
        ],
    ), patch(
        "main.run_scrape",
        return_value=ScrapeResult(True, "ok", article_title="One"),
    ):
        status, failed = main_module.run_batch_scrape("targets.db")

    assert calls == ["daily_report", "delete_feeder", "hot_word"]
    assert status == "success"
    assert failed == 0
    logs = list(Path(tmp_path).glob("batch_*.log"))
    assert len(logs) == 1
    text = logs[0].read_text(encoding="utf-8")
    assert "DAILY_REPORT" in text
    assert "outcome=failed_outer" in text
    assert "report_date=unknown" in text
    assert "reason=RuntimeError" in text
    assert "report boom" not in text
    assert "traceback" not in text.lower()


def test_no_schema_migration_required_for_daily_report(tmp_path):
    db_path = tmp_path / "targets.db"
    conn = init_db(str(db_path))
    try:
        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(target)").fetchall()
        }
    finally:
        conn.close()
    assert "created_at" in cols
    assert "provenance" not in cols
    assert "source" not in cols
