from unittest.mock import patch

from verification_cli import (
    verify_kgs_batch,
    verify_kgs_fetch,
    verify_one_shot_batch,
    verify_one_shot_fetch,
    verify_telemetry_export,
)


class _DummyScrapeResult:
    def __init__(self, ok, outcome):
        self.ok = ok
        self.outcome = outcome

    def __bool__(self):
        return self.ok


def test_verify_one_shot_fetch_rejects_non_canonical_input(capsys):
    assert verify_one_shot_fetch("not-a-url") is False

    out = capsys.readouterr().out
    assert "Verification fetch rejected" in out
    assert "canonical Nicopedia article URL" in out


def test_verify_one_shot_fetch_prints_saved_summary_on_success(capsys):
    with patch(
        "verification_cli.run_scrape",
        return_value=_DummyScrapeResult(True, "ok"),
    ), patch(
        "verification_cli.get_saved_article_summary",
        return_value={
            "found": True,
            "article_id": "12345",
            "article_type": "a",
            "title": "Foo",
            "url": "https://dic.nicovideo.jp/a/12345",
            "created_at": "2026-01-01 00:00:00",
            "response_count": 12,
        },
    ):
        assert verify_one_shot_fetch("https://dic.nicovideo.jp/a/12345") is True

    out = capsys.readouterr().out
    assert "=== VERIFICATION FETCH ===" in out
    assert "Result: fetch completed" in out
    assert "Saved Responses: 12" in out


def test_verify_one_shot_batch_prints_summary_and_failure_count(capsys):
    def _run_batch_scrape(_target_db_path):
        return "partial_failure", 2

    assert verify_one_shot_batch("targets.db", _run_batch_scrape) is False

    out = capsys.readouterr().out
    assert "=== VERIFICATION BATCH RUN ===" in out
    assert "Final Status: partial_failure" in out
    assert "Failed Targets: 2" in out


def test_verify_telemetry_export_writes_output_file(tmp_path, capsys):
    db_path = str(tmp_path / "telemetry.db")
    output_path = tmp_path / "exports" / "telemetry.csv"

    assert verify_telemetry_export(db_path, output_path=str(output_path)) is True

    out = capsys.readouterr().out
    assert "Verification telemetry export written" in out
    assert output_path.is_file()
    content = output_path.read_text(encoding="utf-8")
    assert "article_id" in content


def test_verify_kgs_fetch_uses_isolated_state_and_stdout_guidance(
    tmp_path,
    capsys,
):
    state_dir = tmp_path / "smoke-state"

    with patch(
        "verification_cli.run_scrape",
        return_value=_DummyScrapeResult(True, "ok"),
    ), patch(
        "verification_cli.get_saved_article_summary",
        return_value={
            "found": True,
            "article_id": "12345",
            "article_type": "a",
            "title": "Foo",
            "url": "https://dic.nicovideo.jp/a/12345",
            "created_at": "2026-01-01 00:00:00",
            "response_count": 12,
        },
    ):
        assert verify_kgs_fetch(
            "https://dic.nicovideo.jp/a/12345",
            str(state_dir),
        ) is True

    out = capsys.readouterr().out
    assert "=== KGS LIVE SMOKE ===" in out
    assert "Mode: manual opt-in non-gating helper" in out
    assert "Isolated State Dir:" in out
    assert "KGS messages are stdout-only" in out
    assert "KGS Summary: pass" in out


def test_verify_kgs_fetch_runs_bounded_followup_when_requested(tmp_path, capsys):
    state_dir = tmp_path / "smoke-state"

    with patch(
        "verification_cli.run_scrape",
        side_effect=[
            _DummyScrapeResult(True, "ok"),
            _DummyScrapeResult(True, "ok"),
        ],
    ), patch(
        "verification_cli.get_saved_article_summary",
        return_value={
            "found": True,
            "article_id": "12345",
            "article_type": "a",
            "title": "Foo",
            "url": "https://dic.nicovideo.jp/a/12345",
            "created_at": "2026-01-01 00:00:00",
            "response_count": 12,
        },
    ), patch(
        "verification_cli._drop_latest_saved_responses",
        return_value=3,
    ) as mock_trim:
        assert verify_kgs_fetch(
            "https://dic.nicovideo.jp/a/12345",
            str(state_dir),
            followup_drop_last=3,
        ) is True

    mock_trim.assert_called_once()
    out = capsys.readouterr().out
    assert "Phase: bounded-follow-up" in out
    assert "Follow-Up Trimmed Responses: 3" in out
    assert "Result: bounded-follow-up passed" in out


def test_verify_kgs_batch_registers_target_and_runs_batch(tmp_path, capsys):
    state_dir = tmp_path / "smoke-state"

    def _run_batch_scrape(_target_db_path):
        return "success", 0

    with patch(
        "verification_cli.register_target_url",
        return_value="added",
    ), patch(
        "verification_cli.get_saved_article_summary",
        return_value={
            "found": True,
            "article_id": "12345",
            "article_type": "a",
            "title": "Foo",
            "url": "https://dic.nicovideo.jp/a/12345",
            "created_at": "2026-01-01 00:00:00",
            "response_count": 12,
        },
    ):
        assert verify_kgs_batch(
            "https://dic.nicovideo.jp/a/12345",
            str(state_dir),
            _run_batch_scrape,
        ) is True

    out = capsys.readouterr().out
    assert "Action: batch" in out
    assert "Phase: target-registration (added)" in out
    assert "=== KGS BATCH SUMMARY ===" in out
    assert "Final Status: success" in out
