"""Unit tests for main.py: CLI dispatch (inspect vs scrape, usage/exit)."""
from unittest.mock import call, patch

from pathlib import Path

import pytest

import main as main_module


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


@patch("main.register_scrape_target", return_value="added")
@patch("main.init_db")
@patch("main.resolve_article_input")
def test_main_add_target_registers_resolved_article(
    mock_resolve,
    mock_init_db,
    mock_register,
    capsys,
):
    class _Conn:
        def close(self):
            return None

    mock_init_db.return_value = _Conn()
    mock_resolve.return_value = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        },
        "title": "T",
        "matched_by": "article_url",
        "normalized_input": "https://dic.nicovideo.jp/a/12345",
    }

    with patch(
        "sys.argv",
        ["main.py", "add-target", "https://dic.nicovideo.jp/a/12345"],
    ):
        main_module.main()

    mock_register.assert_called_once_with(
        mock_init_db.return_value,
        "12345",
        "a",
        "https://dic.nicovideo.jp/a/12345",
    )
    out = capsys.readouterr().out
    assert "Registered scrape target: https://dic.nicovideo.jp/a/12345" in out


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


@patch("main.register_scrape_target", return_value="duplicate")
@patch("main.init_db")
@patch("main.resolve_article_input")
def test_main_add_target_reports_duplicate_without_error(
    mock_resolve,
    mock_init_db,
    mock_register,
    capsys,
):
    class _Conn:
        def close(self):
            return None

    mock_init_db.return_value = _Conn()
    mock_resolve.return_value = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        },
        "title": "T",
        "matched_by": "article_url",
        "normalized_input": "x",
    }

    with patch(
        "sys.argv",
        ["main.py", "add-target", "https://dic.nicovideo.jp/a/12345"],
    ):
        main_module.main()

    out = capsys.readouterr().out
    assert "Scrape target already registered: a/12345" in out


@patch("main.register_scrape_target", return_value="invalid")
@patch("main.init_db")
@patch("main.resolve_article_input")
def test_main_add_target_exits_non_zero_for_invalid_registry_url(
    mock_resolve,
    mock_init_db,
    mock_register,
    capsys,
):
    class _Conn:
        def close(self):
            return None

    mock_init_db.return_value = _Conn()
    mock_resolve.return_value = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        },
        "title": "T",
        "matched_by": "article_url",
        "normalized_input": "x",
    }

    with patch(
        "sys.argv",
        ["main.py", "add-target", "https://dic.nicovideo.jp/a/12345"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Invalid canonical URL for registry:" in out


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


@patch("main.list_active_scrape_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/12345",
    "https://dic.nicovideo.jp/a/99999",
])
@patch("main.init_db")
def test_main_targets_mode_loads_and_prints_targets(mock_init_db, mock_list, capsys):
    class _Conn:
        def close(self):
            return None

    mock_init_db.return_value = _Conn()

    with patch("sys.argv", ["main.py", "targets"]):
        main_module.main()

    mock_list.assert_called_once_with(mock_init_db.return_value)
    out = capsys.readouterr().out
    assert "Loaded 2 active scrape target(s) from sqlite target registry" in out
    assert "https://dic.nicovideo.jp/a/12345" in out
    assert "https://dic.nicovideo.jp/a/99999" in out


def test_main_export_all_articles_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "export-all-articles"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: export-all-articles --format txt" in out


def test_main_add_target_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "add-target"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: add-target <article_url_or_full_title>" in out


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

    mock_serve_web_app.assert_called_once_with(host="127.0.0.1", port=8000)


@patch("main.serve_web_app")
def test_main_web_mode_allows_host_and_port_override(mock_serve_web_app):
    with patch(
        "sys.argv",
        ["main.py", "web", "--host", "0.0.0.0", "--port", "9001"],
    ):
        main_module.main()

    mock_serve_web_app.assert_called_once_with(host="0.0.0.0", port=9001)


def test_main_too_few_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "python main.py <article_url>" in out
    assert "inspect" in out
    assert "export <article_id> <article_type> --format txt" in out
    assert "export <article_id> <article_type> --format md" in out
    assert "list-articles" in out
    assert "export-all-articles --format txt" in out
    assert "add-target <article_url_or_full_title>" in out
    assert "resolve-article <article_url_or_full_title>" in out
    assert "targets" in out
    assert "batch" in out
    assert "periodic-once" in out
    assert "import-targets-from-txt <targets_txt_path>" in out
    assert "web [--host HOST] [--port PORT]" in out
    assert "periodic <interval_seconds> [--max-runs N]" in out


def test_main_inspect_without_id_type_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "inspect"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "inspect <article_id> <article_type>" in out


@patch("main.list_active_scrape_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.init_db")
@patch("main.run_scrape", side_effect=[True, True])
def test_main_batch_all_success_exits_zero(
    mock_run_scrape,
    mock_init_db,
    mock_list,
    tmp_path,
    capsys,
    monkeypatch,
):
    class _Conn:
        def close(self):
            return None

    mock_init_db.return_value = _Conn()
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch"]):
        main_module.main()

    assert mock_run_scrape.call_count == 2
    mock_run_scrape.assert_any_call("https://dic.nicovideo.jp/a/1")
    mock_run_scrape.assert_any_call("https://dic.nicovideo.jp/a/2")
    out = capsys.readouterr().out
    assert "[OK] https://dic.nicovideo.jp/a/1" in out
    assert "[OK] https://dic.nicovideo.jp/a/2" in out

    logs = list(Path(tmp_path).glob("batch_*.log"))
    assert len(logs) == 1
    text = logs[0].read_text(encoding="utf-8")
    assert "BATCH_RUN_START" in text
    assert "BATCH_RUN_END" in text
    assert "target_source=sqlite_target_table" in text
    assert "total_targets=2" in text
    assert "failed_targets=0" in text
    assert "final_status=success" in text
    assert "\nFAIL\n" not in text


@patch("main.list_active_scrape_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.init_db")
@patch("main.run_scrape", side_effect=[False, True])
def test_main_batch_failure_sets_nonzero_exit_and_continues(
    mock_run_scrape,
    mock_init_db,
    mock_list,
    tmp_path,
    capsys,
    monkeypatch,
):
    class _Conn:
        def close(self):
            return None

    mock_init_db.return_value = _Conn()
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch"]):
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
    assert "failed_targets=1" in text
    assert "total_targets=2" in text
    assert "final_status=partial_failure" in text
    assert "FAIL\n" in text
    assert "target=https://dic.nicovideo.jp/a/1" in text
    assert "short_reason=run_scrape_returned_false" in text
    assert "target=https://dic.nicovideo.jp/a/2" not in text


@patch("main.list_active_scrape_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.init_db")
@patch("main.run_scrape", side_effect=[RuntimeError("boom"), True])
def test_main_batch_exception_sets_nonzero_exit_and_continues(
    mock_run_scrape,
    mock_init_db,
    mock_list,
    tmp_path,
    capsys,
    monkeypatch,
):
    class _Conn:
        def close(self):
            return None

    mock_init_db.return_value = _Conn()
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch"]):
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
    assert "failed_targets=1" in text
    assert "final_status=partial_failure" in text
    assert "target=https://dic.nicovideo.jp/a/1" in text
    assert "short_reason=RuntimeError: boom" in text
    assert "target=https://dic.nicovideo.jp/a/2" not in text


@patch("main.list_active_scrape_target_urls", return_value=[])
@patch("main.init_db")
def test_main_batch_empty_registry_exits_zero(
    mock_init_db,
    mock_list,
    tmp_path,
    monkeypatch,
    capsys,
):
    class _Conn:
        def close(self):
            return None

    mock_init_db.return_value = _Conn()
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch"]):
        main_module.main()

    out = capsys.readouterr().out
    assert "Loaded 0 active scrape target(s)" in out


@patch("main.run_periodic_once")
def test_main_periodic_once_calls_run_periodic_once(mock_run_periodic_once):
    with patch("sys.argv", ["main.py", "periodic-once"]):
        main_module.main()

    mock_run_periodic_once.assert_called_once_with()


@patch("main.run_periodic_scrape")
def test_run_periodic_once_delegates_to_single_periodic_cycle(
    mock_run_periodic_scrape,
):
    main_module.run_periodic_once()

    mock_run_periodic_scrape.assert_called_once_with(0.0, max_runs=1)


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
        ["main.py", "periodic", "30", "--max-runs", "2"],
    ):
        main_module.main()

    assert mock_run_batch.call_args_list == [call(), call()]
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
        ["main.py", "periodic", "5", "--max-runs", "2"],
    ):
        main_module.main()

    assert mock_run_batch.call_args_list == [call(), call()]
    mock_sleep.assert_called_once_with(5.0)

    out = capsys.readouterr().out
    assert "status=failure failed_targets=2" in out
    assert "status=success failed_targets=0" in out


@patch("main.run_batch_scrape", return_value=("success", 0))
@patch("main.time.sleep", side_effect=KeyboardInterrupt)
def test_main_periodic_exits_safely_on_ctrl_c_during_sleep(
    mock_sleep, mock_run_batch, capsys
):
    with patch("sys.argv", ["main.py", "periodic", "5"]):
        main_module.main()

    mock_run_batch.assert_called_once_with()
    mock_sleep.assert_called_once_with(5.0)
    out = capsys.readouterr().out
    assert "Periodic execution interrupted. Exiting safely." in out


@patch("main.run_batch_scrape", side_effect=KeyboardInterrupt)
def test_main_periodic_exits_safely_on_ctrl_c_during_run(mock_run_batch, capsys):
    with patch("sys.argv", ["main.py", "periodic", "5"]):
        main_module.main()

    mock_run_batch.assert_called_once_with()
    out = capsys.readouterr().out
    assert "Periodic execution interrupted. Exiting safely." in out


def test_main_periodic_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "periodic"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: periodic <interval_seconds> [--max-runs N]" in out


@patch("main.admin_import_targets_from_txt")
@patch("main.init_db")
def test_main_import_targets_from_txt_admin_path(
    mock_init_db,
    mock_import,
    capsys,
    tmp_path,
):
    class _Conn:
        def close(self):
            return None

    mock_init_db.return_value = _Conn()
    mock_import.return_value = {"added": 2, "duplicate": 1, "invalid": 0}

    p = tmp_path / "t.txt"
    p.write_text("https://dic.nicovideo.jp/a/1\n", encoding="utf-8")

    with patch("sys.argv", ["main.py", "import-targets-from-txt", str(p)]):
        main_module.main()

    mock_import.assert_called_once_with(mock_init_db.return_value, str(p))
    out = capsys.readouterr().out
    assert "Admin import complete: added=2 duplicate=1 invalid=0" in out
