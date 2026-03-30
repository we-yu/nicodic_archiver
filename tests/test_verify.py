"""Unit tests for tools.verify bounded verification helpers."""

from unittest.mock import patch

import tools.verify as verify_mod


def test_verify_targets_list_empty_db(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    db_path = str(tmp_path / "reg.db")

    code = verify_mod.dispatch_verify(["targets", "list", db_path])
    assert code == 0
    out = capsys.readouterr().out
    assert "TARGET REGISTRY" in out or "TARGET REGISTRY (verification)" in out
    assert "Count: 0" in out
    assert "(no rows)" in out


@patch("tools.verify.resolve_article_input")
@patch("tools.verify.archive_read.get_saved_article_summary")
def test_verify_article_check_prints_status(
    mock_summary,
    mock_resolve,
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    # Create a schema so archive_read queries are stable.
    from storage import init_db

    init_db()

    mock_resolve.return_value = {
        "ok": True,
        "canonical_target": {
            "article_id": "1",
            "article_type": "a",
            "article_url": "https://dic.nicovideo.jp/a/1",
        },
        "matched_by": "article_url",
        "title": "T",
        "normalized_input": "https://dic.nicovideo.jp/a/1",
    }
    mock_summary.return_value = {
        "found": False,
        "article_id": "1",
        "article_type": "a",
        "title": None,
        "url": None,
        "created_at": None,
        "response_count": 0,
    }

    code = verify_mod.dispatch_verify(
        ["article", "check", "https://dic.nicovideo.jp/a/1"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "Found: no" in out


@patch("tools.verify.resolve_article_input")
def test_verify_article_fetch_requires_isolated(
    mock_resolve,
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    code = verify_mod.dispatch_verify(
        ["article", "fetch", "https://dic.nicovideo.jp/a/1"]
    )
    assert code == 1
    mock_resolve.assert_not_called()


@patch("tools.verify.resolve_article_input")
@patch("tools.verify.orchestrator.run_scrape")
def test_verify_article_fetch_parses_isolated_dir(
    mock_run_scrape,
    mock_resolve,
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)

    mock_resolve.return_value = {
        "ok": True,
        "canonical_target": {
            "article_id": "1",
            "article_type": "a",
            "article_url": "https://dic.nicovideo.jp/a/1",
        },
        "matched_by": "article_url",
        "title": "T",
        "normalized_input": "https://dic.nicovideo.jp/a/1",
    }
    mock_run_scrape.return_value = None

    iso_dir = str(tmp_path / "iso_root")
    code = verify_mod.dispatch_verify(
        [
            "article",
            "fetch",
            "https://dic.nicovideo.jp/a/1",
            "--isolated",
            "--isolated-dir",
            iso_dir,
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "ISOLATED ARCHIVE STATE" in out
    mock_resolve.assert_called_once()


def test_verify_kgs_show_reads_kgs_file(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    kgs_path = tmp_path / "kgs.txt"
    kgs_path.write_text(
        "# comment\nhttps://dic.nicovideo.jp/a/12345\n",
        encoding="utf-8",
    )

    code = verify_mod.dispatch_verify(
        ["kgs", "show", "--kgs-file", str(kgs_path)],
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "KGS URL" in out
    assert "12345" in out


def test_verify_kgs_follow_up_requires_isolated(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    code = verify_mod.dispatch_verify(
        [
            "kgs",
            "follow-up",
            "--known-good-url",
            "https://dic.nicovideo.jp/a/1",
        ]
    )
    assert code == 1
