"""Tests for plain-text target list loader."""
import tempfile
from pathlib import Path

from target_list import load_target_list


def test_load_target_list_empty_file():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        path = f.name
    try:
        assert load_target_list(path) == []
    finally:
        Path(path).unlink(missing_ok=True)


def test_load_target_list_one_url():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("https://dic.nicovideo.jp/a/12345\n")
        path = f.name
    try:
        assert load_target_list(path) == ["https://dic.nicovideo.jp/a/12345"]
    finally:
        Path(path).unlink(missing_ok=True)


def test_load_target_list_multiple_urls():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("https://dic.nicovideo.jp/a/1\n")
        f.write("https://dic.nicovideo.jp/a/2\n")
        f.write("https://dic.nicovideo.jp/a/3\n")
        path = f.name
    try:
        got = load_target_list(path)
        assert got == [
            "https://dic.nicovideo.jp/a/1",
            "https://dic.nicovideo.jp/a/2",
            "https://dic.nicovideo.jp/a/3",
        ]
    finally:
        Path(path).unlink(missing_ok=True)


def test_load_target_list_skips_comments_and_blank_lines():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("# comment\n")
        f.write("\n")
        f.write("https://dic.nicovideo.jp/a/42\n")
        f.write("  \n")
        f.write("# another\n")
        f.write("https://dic.nicovideo.jp/a/99\n")
        path = f.name
    try:
        got = load_target_list(path)
        assert got == [
            "https://dic.nicovideo.jp/a/42",
            "https://dic.nicovideo.jp/a/99",
        ]
    finally:
        Path(path).unlink(missing_ok=True)


def test_load_target_list_strips_lines():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("  https://dic.nicovideo.jp/a/1  \n")
        path = f.name
    try:
        assert load_target_list(path) == ["https://dic.nicovideo.jp/a/1"]
    finally:
        Path(path).unlink(missing_ok=True)
