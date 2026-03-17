"""Unit tests for main.py: CLI dispatch (inspect vs scrape, usage/exit)."""
from unittest.mock import call, patch

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


@patch(
    "main.load_target_urls",
    return_value=[
        "https://dic.nicovideo.jp/a/12345",
        "https://dic.nicovideo.jp/a/99999",
    ],
)
@patch("main.run_scrape")
def test_main_batch_mode_runs_targets_serially_and_succeeds(
    mock_run_scrape, mock_load_targets, capsys
):
    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
        main_module.main()

    mock_load_targets.assert_called_once_with("targets.txt")
    assert mock_run_scrape.call_args_list == [
        call("https://dic.nicovideo.jp/a/12345"),
        call("https://dic.nicovideo.jp/a/99999"),
    ]

    out = capsys.readouterr().out
    assert "[1/2] START https://dic.nicovideo.jp/a/12345" in out
    assert "[1/2] OK https://dic.nicovideo.jp/a/12345" in out
    assert "[2/2] START https://dic.nicovideo.jp/a/99999" in out
    assert "[2/2] OK https://dic.nicovideo.jp/a/99999" in out
    assert "Batch finished successfully: 2/2" in out


@patch(
    "main.load_target_urls",
    return_value=[
        "https://dic.nicovideo.jp/a/12345",
        "https://dic.nicovideo.jp/a/99999",
        "https://dic.nicovideo.jp/a/77777",
    ],
)
@patch(
    "main.run_scrape",
    side_effect=[None, RuntimeError("network boom"), None],
)
def test_main_batch_mode_continues_on_error_and_exits_non_zero(
    mock_run_scrape, mock_load_targets, capsys
):
    with patch("sys.argv", ["main.py", "batch", "targets.txt"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    mock_load_targets.assert_called_once_with("targets.txt")
    assert mock_run_scrape.call_args_list == [
        call("https://dic.nicovideo.jp/a/12345"),
        call("https://dic.nicovideo.jp/a/99999"),
        call("https://dic.nicovideo.jp/a/77777"),
    ]

    out = capsys.readouterr().out
    assert "[1/3] OK https://dic.nicovideo.jp/a/12345" in out
    assert "[2/3] FAILED https://dic.nicovideo.jp/a/99999: network boom" in out
    assert "[3/3] OK https://dic.nicovideo.jp/a/77777" in out
    assert "Batch finished with failures: 1/3" in out


def test_main_targets_without_path_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "targets"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: targets <target_list_path>" in out


def test_main_batch_without_path_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "batch"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "Usage: batch <target_list_path>" in out


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
