from unittest.mock import patch

import web_action_log


def test_append_web_action_log_writes_block_with_blank_lines_and_human_title():
    captured = {}

    def fake_open(*args, **kwargs):
        class _F:
            def write(self, content):
                captured["content"] = content

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        return _F()

    with patch("web_action_log.open", fake_open):
        web_action_log.append_web_action_log(
            {
                "action_id": "a1",
                "action_kind": "download",
                "visitor_hint": "ra=1.2.3.4 ua=UA",
                "input_value": "https://dic.nicovideo.jp/a/%E3%83%86%E3%82%B9%E3%83%88",
                "resolved_title": "%E3%83%86%E3%82%B9%E3%83%88",
                "resolved_article_id": "12345",
                "resolved_article_type": "a",
                "resolved_canonical_url": "https://dic.nicovideo.jp/a/12345",
                "requested_format": "txt",
                "result_status": "ok",
            },
            log_path="data/_unit_test.log",
        )

    content = captured["content"]
    assert content.startswith("\nSTART web_action a1\n")
    assert content.endswith("\nEND web_action a1\n\n")
    assert "resolved_title: テスト\n" in content
