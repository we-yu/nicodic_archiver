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


def test_main_too_few_args_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "python main.py <article_url>" in out
    assert "target-list" in out
    assert "inspect" in out


@patch("main.run_scrape")
@patch("main.load_target_list")
def test_main_target_list_loads_file_and_runs_first_url(mock_load, mock_run_scrape):
    mock_load.return_value = [
        "https://dic.nicovideo.jp/a/1",
        "https://dic.nicovideo.jp/a/2",
    ]
    with patch("sys.argv", ["main.py", "--target-list", "/path/to/list.txt"]):
        main_module.main()
    mock_load.assert_called_once_with("/path/to/list.txt")
    mock_run_scrape.assert_called_once_with("https://dic.nicovideo.jp/a/1")


@patch("main.load_target_list")
def test_main_target_list_empty_exits_with_message(mock_load, capsys):
    mock_load.return_value = []
    with patch("sys.argv", ["main.py", "--target-list", "/path/to/empty.txt"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "empty" in out.lower() or "no valid" in out.lower()


def test_main_target_list_missing_file_arg_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "--target-list"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "--target-list" in out or "target-list" in out


def test_main_inspect_without_id_type_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "inspect"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "inspect <article_id> <article_type>" in out
