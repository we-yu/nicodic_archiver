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
    assert "inspect" in out


def test_main_inspect_without_id_type_exits_with_usage(capsys):
    with patch("sys.argv", ["main.py", "inspect"]):
        with pytest.raises(SystemExit) as exc_info:
            main_module.main()
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "inspect <article_id> <article_type>" in out
