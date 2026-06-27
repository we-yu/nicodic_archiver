from compact_scrape_log import (
    BatchDigestRecorder,
    GROUP_PAGE_TOKENS,
    feeder_summary_compact,
    flush_page_tokens_by_group,
    format_page_ok_token,
    join_page_tokens,
)


def test_feeder_summary_compact_aliases_skip_fields():
    summary = {
        "checked_from_res_no": 1,
        "checked_to_res_no": 3,
        "responses_checked": 3,
        "extracted_candidates": 3,
        "processed_candidates": 3,
        "registered_candidates": 0,
        "handed_off_candidates": 3,
        "skipped_invalid_candidates": 0,
        "skipped_resolution_failures": 2,
        "skipped_denylisted_candidates": 0,
        "skipped_registration_failures": 0,
        "updated_last_processed_res_no": 9,
    }
    txt = feeder_summary_compact(summary)
    assert "skipped_resolution=2" in txt
    assert "skipped_registration=0" in txt


def test_page_tokens_group_about_eleven_wide():
    base = "https://dic.nicovideo.jp/b/a/694740/"
    urls = [f"{base}{n * 30 + 1}-" for n in range(25)]
    tokens = [format_page_ok_token(u) for u in urls]
    rows, tail = flush_page_tokens_by_group(
        tokens[:],
        group_size=GROUP_PAGE_TOKENS,
    )
    assert len(rows[0]) == GROUP_PAGE_TOKENS
    assert sum(len(row) for row in rows) + len(tail) == 25


def test_join_page_tokens_concatenates_tokens():
    a = "[1 OK]"
    b = "[31 OK]"
    assert join_page_tokens([a, b]) == "[1 OK][31 OK]"


def test_format_page_ok_token_strips_trailing_dash_from_board_page():
    url = "https://dic.nicovideo.jp/b/a/5492955/511-"
    assert format_page_ok_token(url) == "[511 OK]"


def test_batch_digest_recorder_render_includes_counters_and_sections():
    rec = BatchDigestRecorder()
    rec.add_finish_entry(
        had_step=True,
        prog_idx=1,
        prog_total=1,
        article_id_val="501",
        label="SKIPME",
        ref="501",
        status="fail",
        reason="reason=skip_denylist",
        stored_new=0,
        observed_after=None,
        interrupt_http=None,
    )
    lines = rec.render_block()
    assert lines[0] == "BATCH_DIGEST"
    assert any("H=" in ln for ln in lines)
    assert any("BATCH_DIGEST_ITEMS" in ln for ln in lines)
    assert any("SKIP " in ln for ln in lines)
