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


def test_main_export_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "export", "12345", "a"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: export <article_id> <article_type> --format txt|md" in out


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
    assert "add-target <article_url> <target_list_path>" in out
    assert "targets <target_list_path>" in out
    assert "batch <target_list_path>" in out
    assert (
        "periodic <target_list_path> <interval_seconds> [--max-runs N]" in out
    )


@patch("main.add_target_url", return_value=(True, "added"))
def test_main_add_target_calls_add_target_url(mock_add):
    with patch(
        "sys.argv",
        ["main.py", "add-target", "https://dic.nicovideo.jp/a/12345", "targets.txt"],
    ):
        main_module.main()

    mock_add.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        "targets.txt",
    )


@patch("main.add_target_url", return_value=(False, "duplicate"))
def test_main_add_target_failure_exits_nonzero(mock_add):
    with patch(
        "sys.argv",
        ["main.py", "add-target", "https://dic.nicovideo.jp/a/12345", "targets.txt"],
    ):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    mock_add.assert_called_once()


def test_main_add_target_missing_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "add-target"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: add-target <article_url> <target_list_path>" in out


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
@patch("main.run_scrape", side_effect=[True, True])
def test_main_batch_all_success_exits_zero(
    mock_run_scrape, mock_load_targets, tmp_path, capsys, monkeypatch
):
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
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
    assert "total_targets=2" in text
    assert "failed_targets=0" in text
    assert "final_status=success" in text
    assert "\nFAIL\n" not in text


@patch("main.load_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.run_scrape", side_effect=[False, True])
def test_main_batch_failure_sets_nonzero_exit_and_continues(
    mock_run_scrape, mock_load_targets, tmp_path, capsys, monkeypatch
):
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
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


@patch("main.load_target_urls", return_value=[
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
])
@patch("main.run_scrape", side_effect=[RuntimeError("boom"), True])
def test_main_batch_exception_sets_nonzero_exit_and_continues(
    mock_run_scrape, mock_load_targets, tmp_path, capsys, monkeypatch
):
    monkeypatch.setenv("BATCH_LOG_DIR", str(tmp_path))
    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
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


def test_main_batch_without_path_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "batch"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: batch <target_list_path>" in out


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
        ["main.py", "periodic", "targets.txt", "30", "--max-runs", "2"],
    ):
        main_module.main()

    assert mock_run_batch.call_args_list == [
        call("targets.txt"),
        call("targets.txt"),
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
        ["main.py", "periodic", "targets.txt", "5", "--max-runs", "2"],
    ):
        main_module.main()

    assert mock_run_batch.call_args_list == [
        call("targets.txt"),
        call("targets.txt"),
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
    with patch("sys.argv", ["main.py", "periodic", "targets.txt", "5"]):
        main_module.main()

    mock_run_batch.assert_called_once_with("targets.txt")
    mock_sleep.assert_called_once_with(5.0)
    out = capsys.readouterr().out
    assert "Periodic execution interrupted. Exiting safely." in out


@patch("main.run_batch_scrape", side_effect=KeyboardInterrupt)
def test_main_periodic_exits_safely_on_ctrl_c_during_run(mock_run_batch, capsys):
    with patch("sys.argv", ["main.py", "periodic", "targets.txt", "5"]):
        main_module.main()

    mock_run_batch.assert_called_once_with("targets.txt")
    out = capsys.readouterr().out
    assert "Periodic execution interrupted. Exiting safely." in out


def test_main_periodic_without_required_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "periodic", "targets.txt"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert (
        "Usage: periodic <target_list_path> <interval_seconds> [--max-runs N]"
        in out
    )
