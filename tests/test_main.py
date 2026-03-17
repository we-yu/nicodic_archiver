"""Unit tests for main.py: CLI dispatch (inspect vs scrape, usage/exit)."""
from unittest.mock import patch

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


@patch("main.inspect_article")
def test_main_inspect_mode_with_last_n(mock_inspect):
    with patch("sys.argv", ["main.py", "inspect", "12345", "a", "--last", "10"]):
        main_module.main()
    mock_inspect.assert_called_once_with("12345", "a", 10)


@patch("main.load_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/12345",
    "https://dic.nicovideo.jp/a/99999",
])
def test_main_targets_mode_loads_and_prints_targets(mock_load_targets, capsys):
    with patch("sys.argv", ["main.py", "targets", "targets.txt"]):
        main_module.main()

    mock_load_targets.assert_called_once_with("targets.txt")
    out = capsys.readouterr().out
    assert "Loaded 2 scrape target(s) from targets.txt" in out
    assert "https://dic.nicovideo.jp/a/12345" in out
    assert "https://dic.nicovideo.jp/a/99999" in out


def test_main_targets_without_path_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "targets"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: targets <target_list_path>" in out


def test_main_too_few_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "python main.py <article_url>" in out
    assert "inspect" in out
    assert "targets <target_list_path>" in out
    assert "batch <target_list_path>" in out


def test_main_inspect_without_id_type_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "inspect"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "inspect <article_id> <article_type>" in out


@patch("main.load_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.run_scrape", side_effect=[None, None])
def test_main_batch_all_success_exits_zero(
    mock_run_scrape, mock_load_targets, capsys
):
    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
        main_module.main()

    assert mock_run_scrape.call_count == 2
    mock_run_scrape.assert_any_call("https://dic.nicovideo.jp/a/1")
    mock_run_scrape.assert_any_call("https://dic.nicovideo.jp/a/2")
    out = capsys.readouterr().out
    assert "[OK] https://dic.nicovideo.jp/a/1" in out
    assert "[OK] https://dic.nicovideo.jp/a/2" in out
    assert "Batch final status: success" in out


@patch("main.load_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.run_scrape", side_effect=[False, True])
def test_main_batch_failure_sets_nonzero_exit_and_continues(
    mock_run_scrape, mock_load_targets, capsys
):
    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    assert mock_run_scrape.call_count == 2
    out = capsys.readouterr().out
    assert "[FAIL] https://dic.nicovideo.jp/a/1" in out
    assert "[OK] https://dic.nicovideo.jp/a/2" in out
    assert "Batch final status: partial_failure" in out


@patch("main.load_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.run_scrape", side_effect=[RuntimeError("boom"), True])
def test_main_batch_exception_sets_nonzero_exit_and_continues(
    mock_run_scrape, mock_load_targets, capsys
):
    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    assert mock_run_scrape.call_count == 2
    out = capsys.readouterr().out
    assert "[FAIL] https://dic.nicovideo.jp/a/1" in out
    assert "RuntimeError" in out
    assert "[OK] https://dic.nicovideo.jp/a/2" in out
    assert "Batch final status: partial_failure" in out


@patch("main.load_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.run_scrape", side_effect=[False, False])
def test_main_batch_all_failures_report_failure_status(
    mock_run_scrape, mock_load_targets, capsys, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)

    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    assert mock_run_scrape.call_count == 2
    out = capsys.readouterr().out
    assert "Batch final status: failure" in out

    log_files = list((tmp_path / "data" / "batch_runs").glob("*.log"))
    assert len(log_files) == 1

    log_text = log_files[0].read_text(encoding="utf-8")
    assert "failed_targets=2" in log_text
    assert "final_status=failure" in log_text
    assert "FAIL target=https://dic.nicovideo.jp/a/1 reason=returned_false" in log_text
    assert "FAIL target=https://dic.nicovideo.jp/a/2 reason=returned_false" in log_text


@patch("main.load_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.run_scrape", side_effect=[RuntimeError("boom"), None])
def test_main_batch_writes_log_summary_and_failure_details(
    mock_run_scrape, mock_load_targets, capsys, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)

    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    assert mock_load_targets.called
    assert mock_run_scrape.call_count == 2

    log_files = list((tmp_path / "data" / "batch_runs").glob("*.log"))
    assert len(log_files) == 1

    log_text = log_files[0].read_text(encoding="utf-8")
    assert "START run_id=batch-" in log_text
    assert "END run_id=batch-" in log_text
    assert "started_at=" in log_text
    assert "ended_at=" in log_text
    assert "total_targets=2" in log_text
    assert "failed_targets=1" in log_text
    assert "final_status=partial_failure" in log_text
    assert (
        "FAIL target=https://dic.nicovideo.jp/a/1 "
        "reason=RuntimeError: boom"
    ) in log_text
    assert "target=https://dic.nicovideo.jp/a/2" not in log_text


@patch("main.load_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.run_scrape", side_effect=[None, None])
def test_main_batch_writes_success_log_without_per_target_success_details(
    mock_run_scrape, mock_load_targets, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)

    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
        main_module.main()

    assert mock_load_targets.called
    assert mock_run_scrape.call_count == 2

    log_files = list((tmp_path / "data" / "batch_runs").glob("*.log"))
    assert len(log_files) == 1

    log_text = log_files[0].read_text(encoding="utf-8")
    assert "START run_id=batch-" in log_text
    assert "END run_id=batch-" in log_text
    assert "failed_targets=0" in log_text
    assert "final_status=success" in log_text
    assert "FAIL target=" not in log_text


def test_main_batch_without_path_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "batch"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: batch <target_list_path>" in out
