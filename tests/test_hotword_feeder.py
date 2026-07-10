"""Focused tests for the bounded HOT word target feeder.

These tests prove that only data-row rank cells from the
過去のHOTワードbest3 table are extracted, that the recent-week limit and
first-seen de-duplication work, and that fetch / candidate failures are
contained. No live network is used.
"""

from bs4 import BeautifulSoup

import hotword_feeder
from hotword_feeder import (
    extract_hot_word_candidates,
    format_hot_word_feed_inspect_lines,
    inspect_hot_word_feed,
    run_hot_word_feeder,
    scan_hot_word_feed,
)


# A best3 table whose recent rows mix relative /a/ links and absolute dic
# article URLs, with a duplicate article (AAA) and a duplicate absolute link
# (CCC) across weeks. Surrounding it are decoys that must NOT be extracted:
# header-cell rank links (1位/2位/3位), the 第○回 blomaga period link, a
# score-ranking table, a related-items list, and an unrelated /a/ table.
FIXTURE_HTML = """
<div id="article">
  <ul id="page-menu">
    <li><a href="#h2-1">過去のHOTワードbest3</a></li>
  </ul>
  <h2 id="h2-1">過去のHOTワードbest3<sub>(第○回)</sub></h2>
  <table>
    <tr>
      <th>集計期間</th>
      <th><a class="auto" href="/a/1%E4%BD%8D">1位</a></th>
      <th><a class="auto" href="/a/2%E4%BD%8D">2位</a></th>
      <th><a class="auto" href="/a/3%E4%BD%8D">3位</a></th>
    </tr>
    <tr>
      <td>
        <span><a href="https://ch.nicovideo.jp/x/ar1">第689回</a> -</span>
      </td>
      <td><a class="auto" href="/a/AAA">AAA</a></td>
      <td><a class="auto" href="/a/BBB">BBB</a></td>
      <td><a href="https://dic.nicovideo.jp/a/CCC" class="dic">C</a></td>
    </tr>
    <tr>
      <td>
        <span><a href="https://ch.nicovideo.jp/x/ar2">第688回</a> -</span>
      </td>
      <td><a class="auto" href="/a/AAA">AAA again</a></td>
      <td><a class="auto" href="/a/DDD">DDD</a></td>
      <td><a href="https://dic.nicovideo.jp/a/CCC" class="dic">C2</a></td>
    </tr>
    <tr>
      <td>
        <span><a href="https://ch.nicovideo.jp/x/ar3">第687回</a> -</span>
      </td>
      <td><a class="auto" href="/a/EEE">EEE</a></td>
      <td><a class="auto" href="/a/FFF">FFF</a></td>
      <td><a class="auto" href="/a/GGG">GGG</a></td>
    </tr>
  </table>
  <h2 id="h2-2">スコアランキング</h2>
  <table>
    <tr><th>順位</th><th>記事</th></tr>
    <tr>
      <td>1</td>
      <td><a class="auto" href="/a/SCORE_DECOY">score</a></td>
    </tr>
  </table>
  <h2 id="h2-5">関連項目</h2>
  <ul>
    <li><a class="auto" href="/a/RELATED_DECOY">related</a></li>
  </ul>
</div>
<table>
  <tr><td><a href="/a/OUTSIDE_DECOY">outside</a></td></tr>
</table>
"""


def _soup(html: str = FIXTURE_HTML) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def _u(slug: str) -> str:
    return f"https://dic.nicovideo.jp/a/{slug}"


def test_extracts_only_best3_rank_cells_with_dedup():
    result = extract_hot_word_candidates(_soup(), recent_weeks=12)
    assert result == [
        _u("AAA"),
        _u("BBB"),
        _u("CCC"),
        _u("DDD"),
        _u("EEE"),
        _u("FFF"),
        _u("GGG"),
    ]


def test_decoy_links_are_not_extracted():
    result = extract_hot_word_candidates(_soup(), recent_weeks=12)
    # Header rank cells (1位/2位/3位) are th cells -> excluded.
    assert _u("1%E4%BD%8D") not in result
    assert _u("2%E4%BD%8D") not in result
    assert _u("3%E4%BD%8D") not in result
    # Other-section and out-of-table decoys -> excluded.
    assert _u("SCORE_DECOY") not in result
    assert _u("RELATED_DECOY") not in result
    assert _u("OUTSIDE_DECOY") not in result
    # The 第○回 blomaga period link is not a dic article link -> excluded.
    assert all("ch.nicovideo.jp" not in url for url in result)


def test_recent_week_limit_bounds_rows():
    result = extract_hot_word_candidates(_soup(), recent_weeks=2)
    # Only the two most recent data rows are processed.
    assert result == [_u("AAA"), _u("BBB"), _u("CCC"), _u("DDD")]
    assert _u("EEE") not in result


def test_relative_and_absolute_links_both_supported():
    result = extract_hot_word_candidates(_soup(), recent_weeks=1)
    # Row 1 has relative /a/AAA, /a/BBB and absolute .../a/CCC.
    assert _u("AAA") in result
    assert _u("CCC") in result


def test_missing_section_returns_empty():
    html = "<div><h2>無関係な見出し</h2><table><tr><td>"
    html += '<a href="/a/NOPE">x</a></td></tr></table></div>'
    assert extract_hot_word_candidates(_soup(html), recent_weeks=12) == []


def test_scan_reports_counts_without_registering(monkeypatch):
    calls = []
    monkeypatch.setattr(
        hotword_feeder,
        "register_target_url",
        lambda url, db, **kwargs: calls.append(url) or "added",
    )
    scan = scan_hot_word_feed(
        source_url="http://example/test",
        recent_weeks=12,
        fetch=lambda url: _soup(),
    )
    assert scan["fetch_ok"] is True
    assert scan["extracted_candidates"] == 9
    assert scan["unique_candidates"] == 7
    assert scan["candidate_urls"][0] == _u("AAA")
    # scan must not register anything.
    assert calls == []


def test_run_registers_via_boundary_and_queues_new(monkeypatch):
    statuses = {
        _u("AAA"): "added",
        _u("BBB"): "reactivated",
        _u("CCC"): "duplicate",
        _u("DDD"): "denylisted",
        _u("EEE"): "resolution_failure",
        _u("FFF"): "invalid",
        _u("GGG"): "added",
    }
    seen = []

    def fake_register(url, db, **kwargs):
        seen.append((url, db))
        return statuses[url]

    monkeypatch.setattr(hotword_feeder, "register_target_url", fake_register)
    summary = run_hot_word_feeder(
        "data/nicodic.db",
        recent_weeks=12,
        fetch=lambda url: _soup(),
    )
    assert [u for u, _ in seen] == summary["candidate_urls"]
    assert summary["added_targets"] == 2
    assert summary["reactivated_targets"] == 1
    assert summary["duplicate_targets"] == 1
    assert summary["denylisted_candidates"] == 1
    assert summary["resolution_failures"] == 1
    assert summary["invalid_candidates"] == 1
    # Only added/reactivated targets are queued for the same shot.
    assert summary["queued_target_urls"] == [_u("AAA"), _u("BBB"), _u("GGG")]


def test_source_fetch_failure_is_contained(monkeypatch):
    register_calls = []
    monkeypatch.setattr(
        hotword_feeder,
        "register_target_url",
        lambda url, db, **kwargs: register_calls.append(url) or "added",
    )

    def boom(url):
        raise RuntimeError(f"Failed to fetch {url} (status=500)")

    summary = run_hot_word_feeder("data/nicodic.db", fetch=boom)
    assert summary["fetch_ok"] is False
    assert summary["candidate_urls"] == []
    assert summary["queued_target_urls"] == []
    assert register_calls == []


def test_candidate_level_failure_is_contained(monkeypatch):
    def flaky_register(url, db, **kwargs):
        if url == _u("CCC"):
            raise RuntimeError("boom")
        return "added"

    monkeypatch.setattr(hotword_feeder, "register_target_url", flaky_register)
    summary = run_hot_word_feeder(
        "data/nicodic.db",
        recent_weeks=12,
        fetch=lambda url: _soup(),
    )
    # The one failing candidate is counted, the run still completes.
    assert summary["registration_failures"] == 1
    assert summary["added_targets"] == 6
    assert _u("CCC") not in summary["queued_target_urls"]


def test_inspect_has_no_registration_side_effects(monkeypatch):
    def fail_if_called(url, db, **kwargs):
        raise AssertionError("inspect must not register")

    monkeypatch.setattr(hotword_feeder, "register_target_url", fail_if_called)
    scan = inspect_hot_word_feed(
        source_url="http://example/test",
        recent_weeks=12,
        fetch=lambda url: _soup(),
    )
    lines = format_hot_word_feed_inspect_lines(scan)
    assert "SOURCE http://example/test" in lines
    assert "RECENT_WEEKS 12" in lines
    assert "EXTRACTED 9" in lines
    assert "UNIQUE 7" in lines
    assert f"CANDIDATE {_u('AAA')}" in lines
    assert sum(1 for ln in lines if ln.startswith("CANDIDATE ")) == 7
