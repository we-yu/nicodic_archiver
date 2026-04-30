from operator_cli import (
    export_registered_articles_csv_for_operator,
    export_archive_for_operator,
    inspect_target_for_operator,
    list_archives_for_operator,
    list_targets_for_operator,
    show_scraped_res_for_operator,
)
from storage import init_db, save_to_db
from target_list import deactivate_target, register_target_url
from unittest.mock import patch


def test_list_targets_for_operator_shows_status_and_count(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    target_db_path = tmp_path / "targets.db"

    register_target_url("https://dic.nicovideo.jp/a/12345", str(target_db_path))

    with patch(
        "target_list.resolve_id_article_url",
        return_value="https://dic.nicovideo.jp/a/99999-title",
    ):
        register_target_url(
            "https://dic.nicovideo.jp/id/99999",
            str(target_db_path),
        )

    deactivate_target("99999-title", "a", str(target_db_path))

    assert list_targets_for_operator(str(target_db_path), active_only=False) is True

    out = capsys.readouterr().out
    assert "=== TARGET REGISTRY ===" in out
    assert "Count: 2" in out
    assert "active   12345 a" in out
    assert "inactive 99999-title a" in out


def test_inspect_target_for_operator_returns_false_for_missing_target(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    target_db_path = tmp_path / "targets.db"

    assert inspect_target_for_operator("404", "a", str(target_db_path)) is False

    out = capsys.readouterr().out
    assert "Target not found in registry" in out
    assert "ID: 404" in out


def test_list_archives_for_operator_shows_saved_archive_summary(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Archive Title",
            "https://dic.nicovideo.jp/a/12345",
            [
                {
                    "res_no": 1,
                    "id_hash": "abc123",
                    "poster_name": "Alice",
                    "posted_at": "2025-01-01 00:00",
                    "content": "hello",
                    "content_html": "<p>hello</p>",
                }
            ],
        )
    finally:
        conn.close()

    assert list_archives_for_operator() is True

    out = capsys.readouterr().out
    assert "=== SAVED ARCHIVES ===" in out
    assert "Count: 1" in out
    assert "12345 a | title=Archive Title | responses=1" in out


def test_export_archive_for_operator_writes_requested_output_file(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)

    conn = init_db()
    try:
        save_to_db(
            conn,
            "12345",
            "a",
            "Archive Title",
            "https://dic.nicovideo.jp/a/12345",
            [
                {
                    "res_no": 1,
                    "id_hash": "abc123",
                    "poster_name": "Alice",
                    "posted_at": "2025-01-01 00:00",
                    "content": "hello",
                    "content_html": "<p>hello</p>",
                }
            ],
        )
    finally:
        conn.close()

    output_path = tmp_path / "exports" / "archive.md"

    assert export_archive_for_operator(
        "12345",
        "a",
        "md",
        output_path=str(output_path),
    ) is True

    out = capsys.readouterr().out
    assert "Archive export written" in out
    assert output_path.is_file()
    content = output_path.read_text(encoding="utf-8")
    assert "# Archive Title" in content
    assert "### Response 1" in content


def _seed_article(tmp_path, article_id="12345", article_type="a"):
    conn = init_db()
    try:
        save_to_db(
            conn,
            article_id,
            article_type,
            "管理者向けテスト記事",
            f"https://dic.nicovideo.jp/{article_type}/{article_id}",
            [
                {
                    "res_no": 1,
                    "id_hash": "abc",
                    "poster_name": "User",
                    "posted_at": "2025-01-01 00:00",
                    "content": "test content",
                    "content_html": "<p>test content</p>",
                }
            ],
        )
    finally:
        conn.close()


def test_show_scraped_res_for_operator_exports_by_title(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    _seed_article(tmp_path)

    result = show_scraped_res_for_operator(
        "管理者向けテスト記事", requested_format="txt"
    )

    assert result is True
    out, err = capsys.readouterr()
    assert "=== ARTICLE META ===" in out
    assert "ok:" in err
    assert "12345a_" in err
    assert ".txt" in err


def test_show_scraped_res_for_operator_exports_by_id(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    _seed_article(tmp_path)

    result = show_scraped_res_for_operator("12345", is_id=True)

    assert result is True
    out, err = capsys.readouterr()
    assert "=== ARTICLE META ===" in out
    assert "ok:" in err


def test_show_scraped_res_for_operator_exports_md_format(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    _seed_article(tmp_path)

    result = show_scraped_res_for_operator(
        "管理者向けテスト記事", requested_format="md"
    )

    assert result is True
    out, err = capsys.readouterr()
    assert "# 管理者向けテスト記事" in out
    assert ".md" in err


def test_show_scraped_res_for_operator_exports_csv_format(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    _seed_article(tmp_path)

    result = show_scraped_res_for_operator(
        "管理者向けテスト記事", requested_format="csv"
    )

    assert result is True
    out, err = capsys.readouterr()
    assert "article_id" in out
    assert ".csv" in err


def test_show_scraped_res_for_operator_returns_false_for_missing_title(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()

    result = show_scraped_res_for_operator("存在しない記事")

    assert result is False
    _, err = capsys.readouterr()
    assert "not found" in err


def test_export_registered_articles_csv_for_operator_writes_all_rows(
    tmp_path,
    capsys,
):
    output_path = tmp_path / "exports" / "registered.csv"

    with patch(
        "operator_cli.get_all_registered_articles_csv",
        return_value={
            "content": "Title,Article ID\nFoo,12345\n",
            "row_count": 1,
            "filename": "registered_articles_all.csv",
        },
    ) as mock_export:
        assert export_registered_articles_csv_for_operator(str(output_path)) is True

    mock_export.assert_called_once_with()
    out = capsys.readouterr().out
    assert "Registered articles CSV written" in out
    assert output_path.read_text(encoding="utf-8") == "Title,Article ID\nFoo,12345\n"


def test_show_scraped_res_for_operator_uses_export_filename_from_archive_read(
    capsys,
):
    with patch(
        "operator_cli.get_saved_article_summary_by_exact_title",
        return_value={
            "found": True,
            "article_id": "12345",
            "article_type": "a",
            "title": "管理者向けテスト記事",
        },
    ):
        with patch(
            "operator_cli.get_saved_article_export",
            return_value={
                "found": True,
                "content": "body",
                "filename": "12345a_管理者向けテスト記事.txt",
            },
        ):
            assert show_scraped_res_for_operator("管理者向けテスト記事") is True

    _, err = capsys.readouterr()
    assert "12345a_管理者向けテスト記事.txt" in err


def test_show_scraped_res_for_operator_returns_false_for_missing_id(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()

    result = show_scraped_res_for_operator("99999", is_id=True)

    assert result is False
    _, err = capsys.readouterr()
    assert "not found" in err
