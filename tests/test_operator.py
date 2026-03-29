"""Tests for tools.operator bounded CLI dispatch."""

from tools import operator as operator_mod


def test_operator_target_list_empty_db(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    db = str(tmp_path / "reg.db")
    code = operator_mod.dispatch_operator(["target", "list", db])
    assert code == 0
    out = capsys.readouterr().out
    assert "TARGET REGISTRY" in out
    assert "(no rows)" in out


def test_operator_target_add_inspect_deactivate_flow(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    db = str(tmp_path / "reg.db")
    url = "https://dic.nicovideo.jp/a/12345"

    assert operator_mod.dispatch_operator(["target", "add", db, url]) == 0
    assert "Added new target row" in capsys.readouterr().out

    assert (
        operator_mod.dispatch_operator(
            ["target", "inspect", db, "12345", "a"],
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "canonical_url" in out
    assert url in out

    assert (
        operator_mod.dispatch_operator(
            ["target", "deactivate", db, "12345", "a"],
        )
        == 0
    )
    assert "inactive" in capsys.readouterr().out

    assert operator_mod.dispatch_operator(["target", "list", db]) == 0
    assert "(no rows)" in capsys.readouterr().out

    assert operator_mod.dispatch_operator(["target", "list", db, "--all"]) == 0
    assert "inactive" in capsys.readouterr().out

    assert (
        operator_mod.dispatch_operator(
            ["target", "reactivate", db, "12345", "a"],
        )
        == 0
    )
    assert "active" in capsys.readouterr().out


def test_operator_target_deactivate_missing_row(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db = str(tmp_path / "reg.db")
    code = operator_mod.dispatch_operator(
        ["target", "deactivate", db, "nope", "a"],
    )
    assert code == 1


def test_operator_unknown_realm_exits_nonzero(capsys):
    code = operator_mod.dispatch_operator(["widgets", "list"])
    assert code == 1
    assert "Unknown operator realm" in capsys.readouterr().out


def test_operator_help_zero(capsys):
    assert operator_mod.dispatch_operator(["help"]) == 0
    assert "operator target list" in capsys.readouterr().out


def test_operator_archive_inspect_bad_last_returns_one(capsys):
    code = operator_mod.dispatch_operator(
        ["archive", "inspect", "1", "a", "--last", "x"],
    )
    assert code == 1
    assert "integer" in capsys.readouterr().out
