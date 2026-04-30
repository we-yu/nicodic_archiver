"""Unit tests for main.py: CLI dispatch (inspect vs scrape, usage/exit)"""
from unittest.mock import call, patch

from pathlib import Path

import pytest

import main as main_module
from orchestrator import ScrapeResult
from storage import init_db


@patch("main.run_scrape")
def test_main_scrape_mode_calls_run_scrape(mock_run_scrape):
    with patch("sys.argv", ["main.py", "https://dic.nicovideo.jp/a/12345"]):
        main_module.main()
    mock_run_scrape.assert_called_once_with("https://dic.nicovideo.jp/a/12345")


@patch("main.inspect_article")
def test_main_inspect_mode_calls_inspect_article(mock_inspect):
    with patch("sys.argv", ["main.py", "inspect", "12345", "a"]):
        main_module.main()
    mock_inspect.assert_called_once_with("12345", "a", None)


@patch("main.export_article")
def test_main_export_mode_calls_export_article(mock_export):
    mock_export.return_value = True

    with patch(
        "sys.argv",
        ["main.py", "export", "12345", "a", "--format", "txt"],
    ):
        main_module.main()

    mock_export.assert_called_once_with("12345", "a", "txt")


@patch("main.list_articles")
def test_main_list_articles_calls_list_articles(mock_list_articles):
    with patch("sys.argv", ["main.py", "list-articles"]):
        main_module.main()

    mock_list_articles.assert_called_once_with()


@patch("main.export_all_articles", return_value=True)
def test_main_export_all_articles_calls_export_all_articles(mock_export_all):
    with patch(
        "sys.argv",
        ["main.py", "export-all-articles", "--format", "txt"],
    ):
        main_module.main()

    mock_export_all.assert_called_once_with("txt")


@patch("main.export_all_articles", return_value=False)
def test_main_export_all_articles_exits_non_zero_on_failure(mock_export_all):
    with patch(
        "sys.argv",
        ["main.py", "export-all-articles", "--format", "html"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    mock_export_all.assert_called_once_with("html")


@patch("main.register_target_url", return_value="added")
def test_main_add_target_calls_register_target_url(mock_add_target, capsys):
    with patch(
        "sys.argv",
        [
            "main.py",
            "add-target",
            "https://dic.nicovideo.jp/a/12345",
            "targets.db",
        ],
    ):
        main_module.main()

    mock_add_target.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        "targets.db",
    )
    out = capsys.readouterr().out
    assert "Added target: https://dic.nicovideo.jp/a/12345" in out


@patch("main.resolve_article_input")
def test_main_resolve_article_calls_resolver_and_prints_success(
    mock_resolve_article_input,
    capsys,
):
    mock_resolve_article_input.return_value = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        },
        "title": "Foo",
        "matched_by": "exact_title",
        "normalized_input": "Foo",
    }

    with patch("sys.argv", ["main.py", "resolve-article", "Foo"]):
        main_module.main()

    mock_resolve_article_input.assert_called_once_with("Foo")
    out = capsys.readouterr().out
    assert "Resolved article target" in out
    assert "Matched By: exact_title" in out
    assert "URL: https://dic.nicovideo.jp/a/12345" in out


@patch("main.resolve_article_input")
def test_main_resolve_article_exits_non_zero_on_resolution_failure(
    mock_resolve_article_input,
    capsys,
):
    mock_resolve_article_input.return_value = {
        "ok": False,
        "failure_kind": "not_found",
        "normalized_input": "Foo",
    }

    with patch("sys.argv", ["main.py", "resolve-article", "Foo"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    mock_resolve_article_input.assert_called_once_with("Foo")
    out = capsys.readouterr().out
    assert "Article resolution failed: not_found" in out
    assert "Input: Foo" in out


@patch("main.register_target_url", return_value="duplicate")
def test_main_add_target_reports_duplicate_without_error(mock_add_target, capsys):
    with patch(
        "sys.argv",
        [
            "main.py",
            "add-target",
            "https://dic.nicovideo.jp/a/12345",
            "targets.db",
        ],
    ):
        main_module.main()

    mock_add_target.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        "targets.db",
    )
    out = capsys.readouterr().out
    assert "Target already exists: https://dic.nicovideo.jp/a/12345" in out


@patch("main.register_target_url", return_value="invalid")
def test_main_add_target_exits_non_zero_for_invalid_url(mock_add_target, capsys):
    with patch(
        "sys.argv",
        ["main.py", "add-target", "not-a-url", "targets.db"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    mock_add_target.assert_called_once_with("not-a-url", "targets.db")
    out = capsys.readouterr().out
    assert "Invalid target URL: not-a-url" in out


@patch("main.register_target_url", return_value="reactivated")
def test_main_add_target_reports_reactivated_target(mock_add_target, capsys):
    with patch(
        "sys.argv",
        [
            "main.py",
            "add-target",
            "https://dic.nicovideo.jp/a/12345",
            "targets.db",
        ],
    ):
        main_module.main()

    mock_add_target.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        "targets.db",
    )
    out = capsys.readouterr().out
    assert "Reactivated target: https://dic.nicovideo.jp/a/12345" in out


@patch("main.export_article", return_value=False)
def test_main_export_mode_exits_non_zero_on_export_failure(mock_export):
    with patch(
        "sys.argv",
        ["main.py", "export", "12345", "a", "--format", "md"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    mock_export.assert_called_once_with("12345", "a", "md")


@patch("main.inspect_article")
def test_main_inspect_mode_with_last_n(mock_inspect):
    with patch("sys.argv", ["main.py", "inspect", "12345", "a", "--last", "10"]):
        main_module.main()
    mock_inspect.assert_called_once_with("12345", "a", 10)


@patch("main.list_active_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/12345",
    "https://dic.nicovideo.jp/a/99999",
])
def test_main_targets_mode_loads_and_prints_targets(mock_load_targets, capsys):
    with patch("sys.argv", ["main.py", "targets", "targets.db"]):
        main_module.main()

    mock_load_targets.assert_called_once_with("targets.db")
    out = capsys.readouterr().out
    assert "Loaded 2 active scrape target(s) from target registry targets.db" in out
    assert "https://dic.nicovideo.jp/a/12345" in out
    assert "https://dic.nicovideo.jp/a/99999" in out


def test_main_targets_without_path_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "targets"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: targets <target_db_path>" in out


def test_main_export_all_articles_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "export-all-articles"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: export-all-articles --format txt" in out


def test_main_add_target_without_required_args_exits_with_usage(capsys):
    with patch(
        "sys.argv",
        ["main.py", "add-target", "https://dic.nicovideo.jp/a/1"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: add-target <article_url> <target_db_path>" in out


def test_main_resolve_article_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "resolve-article"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: resolve-article <article_url_or_full_title>" in out


def test_main_export_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "export", "12345", "a"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: export <article_id> <article_type> --format txt|md" in out


@patch("main.serve_web_app")
def test_main_web_mode_calls_serve_web_app_with_defaults(mock_serve_web_app):
    with patch("sys.argv", ["main.py", "web"]):
        main_module.main()

    mock_serve_web_app.assert_called_once_with(
        host="127.0.0.1",
        port=8000,
        target_db_path=main_module.DEFAULT_TARGET_DB_PATH,
    )


@patch("main.serve_web_app")
def test_main_web_mode_allows_host_and_port_override(mock_serve_web_app):
    with patch(
        "sys.argv",
        [
            "main.py",
            "web",
            "--host",
            "0.0.0.0",
            "--port",
            "9001",
            "--target-db-path",
            "/runtime/data/custom.db",
        ],
    ):
        main_module.main()

    mock_serve_web_app.assert_called_once_with(
        host="0.0.0.0",
        port=9001,
        target_db_path="/runtime/data/custom.db",
    )


@patch("main.list_targets_for_operator")
def test_main_operator_target_list_calls_operator_helper(mock_list_targets):
    mock_list_targets.return_value = True

    with patch("sys.argv", ["main.py", "operator", "target", "list"]):
        main_module.main()

    mock_list_targets.assert_called_once_with(
        main_module.DEFAULT_TARGET_DB_PATH,
        active_only=False,
    )


@patch("main.inspect_target_for_operator")
def test_main_operator_target_inspect_calls_operator_helper(mock_inspect_target):
    mock_inspect_target.return_value = True

    with patch(
        "sys.argv",
        [
            "main.py",
            "operator",
            "target",
            "inspect",
            "12345",
            "a",
            "--db",
            "targets.db",
        ],
    ):
        main_module.main()

    mock_inspect_target.assert_called_once_with("12345", "a", "targets.db")


@patch("main.deactivate_target_for_operator")
def test_main_operator_target_deactivate_exits_non_zero_on_failure(
    mock_deactivate_target,
):
    mock_deactivate_target.return_value = False

    with patch(
        "sys.argv",
        ["main.py", "operator", "target", "deactivate", "12345", "a"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    mock_deactivate_target.assert_called_once_with(
        "12345",
        "a",
        main_module.DEFAULT_TARGET_DB_PATH,
    )


@patch("main.list_archives_for_operator")
def test_main_operator_archive_list_calls_operator_helper(mock_list_archives):
    mock_list_archives.return_value = True

    with patch("sys.argv", ["main.py", "operator", "archive", "list"]):
        main_module.main()

    mock_list_archives.assert_called_once_with()


@patch("main.inspect_archive_for_operator")
def test_main_operator_archive_inspect_calls_operator_helper(mock_inspect_archive):
    mock_inspect_archive.return_value = True

    with patch(
        "sys.argv",
        [
            "main.py",
            "operator",
            "archive",
            "inspect",
            "12345",
            "a",
            "--last",
            "5",
        ],
    ):
        main_module.main()

    mock_inspect_archive.assert_called_once_with("12345", "a", last_n=5)


@patch("main.export_archive_for_operator")
def test_main_operator_archive_export_calls_operator_helper(mock_export_archive):
    mock_export_archive.return_value = True

    with patch(
        "sys.argv",
        [
            "main.py",
            "operator",
            "archive",
            "export",
            "12345",
            "a",
            "--format",
            "md",
            "--output",
            "out.md",
        ],
    ):
        main_module.main()

    mock_export_archive.assert_called_once_with(
        "12345",
        "a",
        "md",
        output_path="out.md",
    )


@patch("main.export_registered_articles_csv_for_operator")
def test_main_operator_archive_export_registered_csv_calls_helper(
    mock_export_registered_csv,
):
    mock_export_registered_csv.return_value = True

    with patch(
        "sys.argv",
        [
            "main.py",
            "operator",
            "archive",
            "export-registered-csv",
            "--output",
            "registered.csv",
        ],
    ):
        main_module.main()

    mock_export_registered_csv.assert_called_once_with(
        output_path="registered.csv",
    )


def test_main_operator_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "operator"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Operator usage:" in out
    assert "operator target list" in out
    assert "operator archive export" in out
    assert "export-registered-csv" in out


@patch("main.verify_one_shot_fetch")
def test_main_verify_fetch_calls_verification_helper(mock_verify_fetch):
    mock_verify_fetch.return_value = True

    with patch(
        "sys.argv",
        ["main.py", "verify", "fetch", "https://dic.nicovideo.jp/a/12345"],
    ):
        main_module.main()

    mock_verify_fetch.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345"
    )


@patch("main.verify_kgs_fetch")
def test_main_verify_kgs_fetch_calls_verification_helper(mock_verify_kgs_fetch):
    mock_verify_kgs_fetch.return_value = True

    with patch(
        "sys.argv",
        [
            "main.py",
            "verify",
            "kgs",
            "fetch",
            "https://dic.nicovideo.jp/a/12345",
            "--state-dir",
            "runtime/smoke/custom",
            "--followup-drop-last",
            "2",
        ],
    ):
        main_module.main()

    mock_verify_kgs_fetch.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        "runtime/smoke/custom",
        followup_drop_last=2,
    )


@patch("main.verify_kgs_batch")
def test_main_verify_kgs_batch_calls_verification_helper(mock_verify_kgs_batch):
    mock_verify_kgs_batch.return_value = True

    with patch(
        "sys.argv",
        [
            "main.py",
            "verify",
            "kgs",
            "batch",
            "https://dic.nicovideo.jp/a/12345",
        ],
    ):
        main_module.main()

    args = mock_verify_kgs_batch.call_args.args
    assert args[0] == "https://dic.nicovideo.jp/a/12345"
    assert args[1] == "runtime/smoke/kgs"
    assert args[2] is main_module.run_batch_scrape


@patch("main.verify_registry_list")
def test_main_verify_registry_list_calls_verification_helper(
    mock_verify_registry_list,
):
    mock_verify_registry_list.return_value = True

    with patch("sys.argv", ["main.py", "verify", "registry", "list"]):
        main_module.main()

    mock_verify_registry_list.assert_called_once_with(
        main_module.DEFAULT_TARGET_DB_PATH,
        active_only=False,
    )


@patch("main.verify_one_shot_batch")
def test_main_verify_batch_calls_verification_helper(mock_verify_batch):
    mock_verify_batch.return_value = True

    with patch("sys.argv", ["main.py", "verify", "batch", "run"]):
        main_module.main()

    args = mock_verify_batch.call_args.args
    assert args[0] == main_module.DEFAULT_TARGET_DB_PATH
    assert args[1] is main_module.run_batch_scrape


@patch("main.verify_telemetry_export")
def test_main_verify_telemetry_export_calls_verification_helper(
    mock_verify_telemetry_export,
):
    mock_verify_telemetry_export.return_value = True

    with patch(
        "sys.argv",
        [
            "main.py",
            "verify",
            "telemetry",
            "export",
            "--db",
            "telemetry.db",
            "--output",
            "out.csv",
        ],
    ):
        main_module.main()

    mock_verify_telemetry_export.assert_called_once_with(
        "telemetry.db",
        output_path="out.csv",
    )


def test_main_verify_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "verify"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Verification usage:" in out
    assert "verify fetch" in out
    assert "verify kgs fetch" in out
    assert "verify telemetry export" in out


def test_main_too_few_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "python main.py <article_url>" in out
    assert "python main.py operator <target|archive> ..." in out
    assert "python main.py verify <fetch|kgs|registry|batch|telemetry> ..." in out
    assert "inspect" in out
    assert "export <article_id> <article_type> --format txt" in out
    assert "export <article_id> <article_type> --format md" in out
    assert "list-articles" in out
    assert "export-all-articles --format txt" in out
    assert "add-target <article_url> <target_db_path>" in out
    assert "import-targets <targets_txt_path> <target_db_path>" in out
    assert "resolve-article <article_url_or_full_title>" in out
    assert "targets <target_db_path>" in out
    assert "batch <target_db_path>" in out
    assert "periodic-once <target_db_path>" in out
    assert "inspect-delete-request-feed" in out
    assert "web [--host HOST] [--port PORT] [--target-db-path PATH]" in out
    assert (
        "periodic <target_db_path> <interval_seconds> [--max-runs N]" in out
    )
    assert "export-run-telemetry-csv" in out


@patch("main.inspect_delete_request_feed")
def test_main_inspect_delete_request_feed_prints_stdout_candidates(
    mock_inspect_delete_request_feed,
    capsys,
):
    mock_inspect_delete_request_feed.return_value = {
        "candidates": [
            {
                "res_no": 10,
                "raw_url": "https://dic.nicovideo.jp/a/j-pop",
                "category": "article_direct",
                "accepted": True,
                "normalized_input": "https://dic.nicovideo.jp/a/j-pop",
            }
        ],
        "summary": {
            "checked_from_res_no": 1,
            "checked_to_res_no": 10,
            "responses_checked": 1,
            "extracted_candidates": 1,
            "handed_off_candidates": 0,
            "updated_last_processed_res_no": 10,
        },
    }

    with patch(
        "sys.argv",
        [
            "main.py",
            "inspect-delete-request-feed",
            "--archive-db",
            "archive.db",
            "--state-path",
            "state.json",
            "--full-scan",
        ],
    ):
        main_module.main()

    mock_inspect_delete_request_feed.assert_called_once_with(
        archive_db_path="archive.db",
        state_path="state.json",
        full_scan=True,
    )
    out = capsys.readouterr().out
    assert "ACCEPT res_no=10" in out
    assert "SUMMARY checked_range=1-10" in out


@patch("main._record_scrape_run_observation")
@patch(
    "main.run_delete_request_feeder",
    return_value={
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
    },
)
@patch("main.list_active_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
])
@patch("main.run_scrape", return_value=ScrapeResult(True, "ok"))
def test_main_batch_records_telemetry_once_per_target(
    mock_scrape,
    mock_run_delete_request_feeder,
    mock_list,
    mock_record,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch", "targets.db"]):
        main_module.main()

    assert mock_record.call_count == 1


def test_main_export_run_telemetry_csv_prints_header(capsys, tmp_path, monkeypatch):
    dbp = str(tmp_path / "tel.db")
    monkeypatch.chdir(tmp_path)

    conn = init_db(dbp)
    conn.close()

    with patch(
        "sys.argv",
        ["main.py", "export-run-telemetry-csv", "--db", dbp],
    ):
        main_module.main()

    out = capsys.readouterr().out
    assert "article_id" in out
    assert "canonical_article_url" in out


@patch("main.import_targets_from_text_file")
def test_main_import_targets_reports_counts(mock_import_targets, capsys):
    mock_import_targets.return_value = {
        "source_path": "/runtime/targets/targets.txt",
        "target_db_path": "/app/data/nicodic.db",
        "processed": 3,
        "added": 2,
        "duplicate": 1,
        "reactivated": 0,
        "invalid": 0,
    }

    with patch(
        "sys.argv",
        [
            "main.py",
            "import-targets",
            "/runtime/targets/targets.txt",
            "/app/data/nicodic.db",
        ],
    ):
        main_module.main()

    mock_import_targets.assert_called_once_with(
        "/runtime/targets/targets.txt",
        "/app/data/nicodic.db",
    )
    out = capsys.readouterr().out
    assert "Imported 3 target line(s)" in out
    assert "added=2 duplicate=1 reactivated=0 invalid=0" in out


def test_main_import_targets_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "import-targets"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: import-targets <targets_txt_path> <target_db_path>" in out


def test_main_inspect_without_id_type_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "inspect"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "inspect <article_id> <article_type>" in out


@patch("main.list_active_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch(
    "main.run_delete_request_feeder",
    return_value={
        "checked_from_res_no": 1,
        "checked_to_res_no": 10,
        "responses_checked": 1,
        "extracted_candidates": 1,
        "handed_off_candidates": 1,
        "updated_last_processed_res_no": 10,
        "queued_target_urls": ["https://dic.nicovideo.jp/a/3"],
        "added_targets": 1,
        "reactivated_targets": 0,
        "duplicate_targets": 0,
        "invalid_targets": 0,
    },
)
@patch(
    "main.run_scrape",
    side_effect=[
        ScrapeResult(
            True,
            "ok",
            display_status="partial",
            article_title="First Title",
            collected_response_count=12,
            observed_max_res_no=12,
        ),
        ScrapeResult(
            True,
            "ok",
            article_title="Second Title",
            collected_response_count=7,
            observed_max_res_no=7,
        ),
        ScrapeResult(
            True,
            "ok",
            article_title="Third Title",
            collected_response_count=3,
            observed_max_res_no=3,
        ),
    ],
)
def test_main_batch_all_success_exits_zero(
    mock_run_scrape,
    mock_run_delete_request_feeder,
    mock_load_targets,
    tmp_path,
    capsys,
    monkeypatch,
):
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch", "targets.db"]):
        main_module.main()

    assert mock_run_scrape.call_count == 3
    mock_run_delete_request_feeder.assert_called_once()
    mock_run_scrape.assert_any_call("https://dic.nicovideo.jp/a/1")
    mock_run_scrape.assert_any_call("https://dic.nicovideo.jp/a/2")
    mock_run_scrape.assert_any_call("https://dic.nicovideo.jp/a/3")
    out = capsys.readouterr().out
    assert "[delete-request-feed] checked_range=1-10" in out
    assert "[OK] https://dic.nicovideo.jp/a/1" in out
    assert "[OK] https://dic.nicovideo.jp/a/2" in out
    assert "[OK] https://dic.nicovideo.jp/a/3" in out

    logs = list(Path(tmp_path).glob("batch_*.log"))
    assert len(logs) == 1
    text = logs[0].read_text(encoding="utf-8")
    assert "BATCH_RUN_START" in text
    assert "DELETE_REQUEST_FEED" in text
    assert "BATCH_RUN_END" in text
    assert "  target_db_path=targets.db" in text
    assert "  target_source=target_table" in text
    assert "  total_targets=3" in text
    assert "[PROGRESS = 1/3]" in text
    assert "[PROGRESS = 2/3]" in text
    assert "[PROGRESS = 3/3]" in text
    assert "  result=SUCCESS" in text
    assert "SUCCESS_PARTIAL" not in text
    assert "  target_url=https://dic.nicovideo.jp/a/1" in text
    assert "  target_url=https://dic.nicovideo.jp/a/2" in text
    assert "  target_url=https://dic.nicovideo.jp/a/3" in text
    assert "  article_title=First Title" in text
    assert "  collected_response_count=12" in text
    assert "  observed_max_res_no=12" in text
    assert "  article_title=Second Title" in text
    assert "  collected_response_count=7" in text
    assert "  observed_max_res_no=7" in text
    assert "  article_title=Third Title" in text
    assert "  collected_response_count=3" in text
    assert "  observed_max_res_no=3" in text
    assert "  success_targets=3" in text
    assert "  failed_targets=0" in text
    assert "  duration_seconds=" in text
    assert "  final_status=success" in text
    assert "FAILURE_DETAIL" not in text


@patch(
    "main.run_delete_request_feeder",
    return_value={
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
    },
)
@patch("main.list_active_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch(
    "main.run_scrape",
    side_effect=[
        ScrapeResult(
            False,
            "fail_article_not_found",
            article_title="unknown",
            collected_response_count=0,
            observed_max_res_no=None,
            failure_page="unknown",
            failure_cause="article_not_found",
            short_reason="article_not_found",
        ),
        ScrapeResult(
            True,
            "ok",
            article_title="Second Title",
            collected_response_count=7,
            observed_max_res_no=7,
        ),
    ],
)
def test_main_batch_failure_sets_nonzero_exit_and_continues(
    mock_run_scrape,
    mock_load_targets,
    mock_run_delete_request_feeder,
    tmp_path,
    capsys,
    monkeypatch,
):
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch", "targets.db"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    assert mock_run_scrape.call_count == 2
    out = capsys.readouterr().out
    assert "[FAIL] https://dic.nicovideo.jp/a/1" in out
    assert "[OK] https://dic.nicovideo.jp/a/2" in out

    logs = list(Path(tmp_path).glob("batch_*.log"))
    assert len(logs) == 1
    text = logs[0].read_text(encoding="utf-8")
    assert "[PROGRESS = 1/2]" in text
    assert "[PROGRESS = 2/2]" in text
    assert "  result=FAIL" in text
    assert "  target_url=https://dic.nicovideo.jp/a/1" in text
    assert "  article_title=unknown" in text
    assert "  FAILURE_DETAIL" in text
    assert "    progress=1/2" in text
    assert "    failure_page=unknown" in text
    assert "    failure_cause=article_not_found" in text
    assert "    short_reason=article_not_found" in text
    assert "  result=SUCCESS" in text
    assert "  target_url=https://dic.nicovideo.jp/a/2" in text
    assert "  article_title=Second Title" in text
    assert "  total_targets=2" in text
    assert "  success_targets=1" in text
    assert "  failed_targets=1" in text
    assert "  final_status=partial_failure" in text


@patch("main.handoff_redirected_target")
@patch(
    "main.run_delete_request_feeder",
    return_value={
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
    },
)
@patch("main.list_active_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch(
    "main.run_scrape",
    side_effect=[
        ScrapeResult(
            True,
            "redirect_handoff",
            article_title="Old Title",
            failure_page="https://dic.nicovideo.jp/a/1",
            failure_cause="redirect_detected",
            short_reason="redirect_handoff",
            redirect_target_url="https://dic.nicovideo.jp/a/9",
        ),
        ScrapeResult(
            True,
            "ok",
            article_title="Second Title",
            collected_response_count=7,
            observed_max_res_no=7,
        ),
    ],
)
def test_main_batch_redirect_handoff_is_success_class_and_logged(
    mock_run_scrape,
    mock_load_targets,
    mock_run_delete_request_feeder,
    mock_handoff_redirected_target,
    tmp_path,
    capsys,
    monkeypatch,
):
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    mock_handoff_redirected_target.return_value = {
        "found": True,
        "status": "redirected",
        "entry": {
            "article_id": "1",
            "article_type": "a",
            "canonical_url": "https://dic.nicovideo.jp/a/1",
            "is_active": False,
            "is_redirected": True,
            "redirect_target_url": "https://dic.nicovideo.jp/a/9",
            "redirect_detected_at": "2026-04-14T00:00:00+00:00",
        },
        "register_status": "added",
        "redirect_target": {
            "article_id": "9",
            "article_type": "a",
            "canonical_url": "https://dic.nicovideo.jp/a/9",
        },
    }

    with patch("sys.argv", ["main.py", "batch", "targets.db"]):
        main_module.main()

    out = capsys.readouterr().out
    assert "[OK] https://dic.nicovideo.jp/a/1" in out
    assert "redirected -> https://dic.nicovideo.jp/a/9; added" in out
    assert "[OK] https://dic.nicovideo.jp/a/2" in out
    mock_handoff_redirected_target.assert_called_once_with(
        "1",
        "a",
        "https://dic.nicovideo.jp/a/9",
        "targets.db",
    )

    logs = list(Path(tmp_path).glob("batch_*.log"))
    assert len(logs) == 1
    text = logs[0].read_text(encoding="utf-8")
    assert "  REDIRECT_DETAIL" in text
    assert "    source_target_url=https://dic.nicovideo.jp/a/1" in text
    assert "    redirect_target_url=https://dic.nicovideo.jp/a/9" in text
    assert "    source_status=redirected" in text
    assert "    register_status=added" in text
    assert "  success_targets=2" in text
    assert "  failed_targets=0" in text
    assert "  final_status=success" in text


@patch(
    "main.run_delete_request_feeder",
    return_value={
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
    },
)
@patch("main.list_active_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch(
    "main.run_scrape",
    side_effect=[RuntimeError("boom"), ScrapeResult(True, "ok")],
)
def test_main_batch_exception_sets_nonzero_exit_and_continues(
    mock_run_scrape,
    mock_load_targets,
    mock_run_delete_request_feeder,
    tmp_path,
    capsys,
    monkeypatch,
):
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch", "targets.db"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    assert mock_run_scrape.call_count == 2
    out = capsys.readouterr().out
    assert "[FAIL] https://dic.nicovideo.jp/a/1" in out
    assert "RuntimeError" in out
    assert "[OK] https://dic.nicovideo.jp/a/2" in out

    logs = list(Path(tmp_path).glob("batch_*.log"))
    assert len(logs) == 1
    text = logs[0].read_text(encoding="utf-8")
    assert "[PROGRESS = 1/2]" in text
    assert "[PROGRESS = 2/2]" in text
    assert "  result=FAIL" in text
    assert "  target_url=https://dic.nicovideo.jp/a/1" in text
    assert "  article_title=unknown" in text
    assert "    failure_page=unknown" in text
    assert "    failure_cause=RuntimeError" in text
    assert "    short_reason=RuntimeError: boom" in text
    assert "  result=SUCCESS" in text
    assert "  target_url=https://dic.nicovideo.jp/a/2" in text
    assert "  failed_targets=1" in text
    assert "  final_status=partial_failure" in text


def test_main_batch_without_path_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "batch"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: batch <target_db_path>" in out


@patch("main.run_periodic_once")
def test_main_periodic_once_calls_run_periodic_once(mock_run_periodic_once):
    with patch("sys.argv", ["main.py", "periodic-once", "targets.db"]):
        main_module.main()

    mock_run_periodic_once.assert_called_once_with("targets.db")


def test_main_periodic_once_without_path_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "periodic-once"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: periodic-once <target_db_path>" in out


@patch("main.run_periodic_scrape")
def test_run_periodic_once_delegates_to_single_periodic_cycle(
    mock_run_periodic_scrape,
):
    main_module.run_periodic_once("targets.db")

    mock_run_periodic_scrape.assert_called_once_with(
        "targets.db",
        0.0,
        max_runs=1,
    )


@patch("main._run_periodic_once_with_host_cron")
@patch("main.run_periodic_scrape")
def test_run_periodic_once_uses_host_cron_log_path_when_configured(
    mock_run_periodic_scrape,
    mock_host_cron_run,
    monkeypatch,
):
    monkeypatch.setenv("HOST_CRON_LOG_PATH", "/runtime/logs/host_cron.log")

    main_module.run_periodic_once("targets.db")

    mock_host_cron_run.assert_called_once_with(
        "targets.db",
        "/runtime/logs/host_cron.log",
    )
    mock_run_periodic_scrape.assert_not_called()


@patch(
    "main.run_batch_scrape",
    side_effect=[("success", 0), ("partial_failure", 1)],
)
@patch("main.time.sleep")
def test_main_periodic_runs_full_batch_per_cycle_and_honors_max_runs(
    mock_sleep, mock_run_batch, capsys
):
    with patch(
        "sys.argv",
        ["main.py", "periodic", "targets.db", "30", "--max-runs", "2"],
    ):
        main_module.main()

    assert mock_run_batch.call_args_list == [
        call("targets.db"),
        call("targets.db"),
    ]
    mock_sleep.assert_called_once_with(30.0)

    out = capsys.readouterr().out
    assert "[periodic] Run 1 starting" in out
    assert "[periodic] Run 1 finished with status=success failed_targets=0" in out
    assert "[periodic] Sleeping 30.0 second(s)" in out
    assert "[periodic] Run 2 starting" in out
    assert (
        "[periodic] Run 2 finished with status=partial_failure failed_targets=1"
        in out
    )


@patch(
    "main.run_batch_scrape",
    side_effect=[("failure", 2), ("success", 0)],
)
@patch("main.time.sleep")
def test_main_periodic_continues_after_failure_statuses(
    mock_sleep, mock_run_batch, capsys
):
    with patch(
        "sys.argv",
        ["main.py", "periodic", "targets.db", "5", "--max-runs", "2"],
    ):
        main_module.main()

    assert mock_run_batch.call_args_list == [
        call("targets.db"),
        call("targets.db"),
    ]
    mock_sleep.assert_called_once_with(5.0)

    out = capsys.readouterr().out
    assert "status=failure failed_targets=2" in out
    assert "status=success failed_targets=0" in out


@patch("main.run_batch_scrape", return_value=("success", 0))
@patch("main.time.sleep", side_effect=KeyboardInterrupt)
def test_main_periodic_exits_safely_on_ctrl_c_during_sleep(
    mock_sleep, mock_run_batch, capsys
):
    with patch("sys.argv", ["main.py", "periodic", "targets.db", "5"]):
        main_module.main()

    mock_run_batch.assert_called_once_with("targets.db")
    mock_sleep.assert_called_once_with(5.0)
    out = capsys.readouterr().out
    assert "Periodic execution interrupted. Exiting safely." in out


@patch("main.run_batch_scrape", side_effect=KeyboardInterrupt)
def test_main_periodic_exits_safely_on_ctrl_c_during_run(mock_run_batch, capsys):
    with patch("sys.argv", ["main.py", "periodic", "targets.db", "5"]):
        main_module.main()

    mock_run_batch.assert_called_once_with("targets.db")
    out = capsys.readouterr().out
    assert "Periodic execution interrupted. Exiting safely." in out


def test_main_periodic_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "periodic", "targets.db"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert (
        "Usage: periodic <target_db_path> <interval_seconds> [--max-runs N]"
        in out
    )
