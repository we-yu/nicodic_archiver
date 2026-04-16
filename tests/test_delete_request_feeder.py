import io
import json
from pathlib import Path
from unittest.mock import patch

import main as main_module
from delete_request_feeder import run_delete_request_feeder
from nicopedia_url import classify_and_normalize_nicopedia_url


def test_classify_and_normalize_accepts_and_normalizes_supported_urls():
    direct = classify_and_normalize_nicopedia_url("https://dic.nicovideo.jp/a/j-pop")
    assert direct.supported is True
    assert direct.category == "article_direct"
    assert direct.normalized_article_url == "https://dic.nicovideo.jp/a/j-pop"

    board = classify_and_normalize_nicopedia_url(
        "https://dic.nicovideo.jp/b/a/%E3%81%A4%E3%81%91%E9%BA%BA/1-"
    )
    assert board.supported is True
    assert board.category == "article_board"
    assert board.normalized_article_url == (
        "https://dic.nicovideo.jp/a/%E3%81%A4%E3%81%91%E9%BA%BA"
    )

    thread_board = classify_and_normalize_nicopedia_url(
        "https://dic.nicovideo.jp/t/b/a/%E3%81%84%E3%81%98%E3%82%81/6031-"
    )
    assert thread_board.supported is True
    assert thread_board.category == "article_thread_board"
    assert thread_board.normalized_article_url == (
        "https://dic.nicovideo.jp/a/%E3%81%84%E3%81%98%E3%82%81"
    )

    thread_direct = classify_and_normalize_nicopedia_url(
        "https://dic.nicovideo.jp/t/a/%E5%A5%B3%E6%80%A7"
    )
    assert thread_direct.supported is True
    assert thread_direct.category == "article_thread_direct"
    assert thread_direct.normalized_article_url == (
        "https://dic.nicovideo.jp/a/%E5%A5%B3%E6%80%A7"
    )


def test_classify_and_normalize_drops_query_and_fragment():
    url = classify_and_normalize_nicopedia_url(
        "https://dic.nicovideo.jp/a/%E6%9C%A8%E6%9D%91%E6%8B%93%E5%93%89#bbs"
    )
    assert url.supported is True
    assert url.normalized_article_url == (
        "https://dic.nicovideo.jp/a/%E6%9C%A8%E6%9D%91%E6%8B%93%E5%93%89"
    )

    url2 = classify_and_normalize_nicopedia_url(
        "https://dic.nicovideo.jp/t/b/a/%E8%B2%A1%E5%8B%99%E7%9C%81/691-"
        "?from=a_bbslook_5020395"
    )
    assert url2.supported is True
    assert url2.normalized_article_url == (
        "https://dic.nicovideo.jp/a/%E8%B2%A1%E5%8B%99%E7%9C%81"
    )


def test_classify_and_normalize_rejects_unsupported_and_malformed():
    video = classify_and_normalize_nicopedia_url("https://dic.nicovideo.jp/v/sm1")
    assert video.supported is False
    assert video.category == "video"

    user = classify_and_normalize_nicopedia_url("https://dic.nicovideo.jp/u/688493")
    assert user.supported is False
    assert user.category == "user"

    live = classify_and_normalize_nicopedia_url(
        "https://dic.nicovideo.jp/l/%E3%82%B3%E3%83%BC%E3%83%AB"
    )
    assert live.supported is False
    assert live.category == "live_or_other"

    community = classify_and_normalize_nicopedia_url(
        "https://dic.nicovideo.jp/b/c/co2078137/"
    )
    assert community.supported is False
    assert community.category == "community_board"

    malformed = classify_and_normalize_nicopedia_url("https://dic.nicovideo.jp/a/")
    assert malformed.supported is False
    assert malformed.category == "malformed"


def test_run_delete_request_feeder_extracts_and_handoffs_and_updates_state(tmp_path):
    target_db_path = tmp_path / "targets.db"
    state_path = Path(str(target_db_path)).with_suffix(".delete_request_feeder.json")

    archive = {
        "responses": [
            (
                10,
                "p",
                "t",
                "h",
                "【掲示板URL】https://dic.nicovideo.jp/b/a/%E3%81%A4%E3%81%91%E9%BA%BA/1-",
            ),
            (
                11,
                "p",
                "t",
                "h",
                "【掲示板URL】https://dic.nicovideo.jp/id/5728993",
            ),
            (12, "p", "t", "h", "【掲示板URL】https://dic.nicovideo.jp/v/sm1"),
        ]
    }

    resolved = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/j-pop",
            "article_id": "x",
            "article_type": "a",
        },
    }

    with patch(
        "delete_request_feeder.read_article_archive",
        return_value=archive,
    ):
        with patch(
            "delete_request_feeder.resolve_article_input",
            return_value=resolved,
        ):
            with patch(
                "delete_request_feeder.register_target_url",
                return_value="added",
            ) as mock_reg:
                buf = io.StringIO()
                result = run_delete_request_feeder(
                    str(target_db_path),
                    inspect=True,
                    stdout=buf,
                )

    assert result["ok"] is True
    assert result["checked_res_no_range"] == (1, 12)
    assert result["extracted_candidates"] == 2
    assert result["handoff_attempts"] == 2
    assert mock_reg.call_args_list[0].args[0].startswith(
        "https://dic.nicovideo.jp/a/"
    )
    assert mock_reg.call_args_list[1].args[0] == "https://dic.nicovideo.jp/a/j-pop"

    assert state_path.exists() is True
    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["last_processed_res_no"] == 12

    out = buf.getvalue()
    assert "res_no=10" in out
    assert "category=article_board" in out
    assert "res_no=11" in out
    assert "category=article_id" in out


def test_run_delete_request_feeder_is_incremental_by_last_processed_res_no(tmp_path):
    target_db_path = tmp_path / "targets.db"
    state_path = Path(str(target_db_path)).with_suffix(".delete_request_feeder.json")
    state_path.write_text('{"last_processed_res_no": 11}\n', encoding="utf-8")

    archive = {
        "responses": [
            (10, "p", "t", "h", "【掲示板URL】https://dic.nicovideo.jp/a/j-pop"),
            (12, "p", "t", "h", "【掲示板URL】https://dic.nicovideo.jp/a/j-pop"),
        ]
    }

    with patch("delete_request_feeder.read_article_archive", return_value=archive):
        with patch(
            "delete_request_feeder.register_target_url",
            return_value="duplicate",
        ) as mock_reg:
            result = run_delete_request_feeder(str(target_db_path))

    assert result["checked_res_no_range"] == (12, 12)
    assert result["extracted_candidates"] == 1
    assert mock_reg.call_count == 1
    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["last_processed_res_no"] == 12


def test_run_batch_scrape_runs_feeder_before_loading_targets(tmp_path):
    calls = []

    def _feeder(_target_db_path):
        calls.append("feeder")
        return {
            "ok": True,
            "checked_res_no_range": (1, 1),
            "extracted_candidates": 0,
            "handoff_attempts": 0,
            "updated_last_processed_res_no": 1,
        }

    def _list_targets(_target_db_path):
        calls.append("list_targets")
        return []

    with patch.object(main_module, "run_delete_request_feeder", side_effect=_feeder):
        with patch.object(
            main_module,
            "list_active_target_urls",
            side_effect=_list_targets,
        ):
            with patch.object(main_module, "_append_batch_run_start"):
                with patch.object(main_module, "_append_batch_run_end"):
                    status, failed = main_module.run_batch_scrape(
                        str(tmp_path / "t.db")
                    )

    assert (status, failed) == ("success", 0)
    assert calls[:2] == ["feeder", "list_targets"]
