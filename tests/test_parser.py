"""Unit tests for parser.py: current HTML parsing behavior."""

from bs4 import BeautifulSoup

from parser import parse_responses


def test_parse_responses_normal_case():
    html = """
    <dl>
      <dt class="st-bbs_reshead" data-res_no="12" data-id_hash="abc123">
        <span class="st-bbs_name">Alice</span>
        <span class="bbs_resInfo_resTime">2026/03/07 12:34</span>
      </dt>
      <dd class="st-bbs_resbody">
        <div class="bbs_resbody_inner">
          Hello<br/>World
          <div class="st-bbs_contentsTitle">remove me</div>
          <div class="st-bbs_referLabel">remove label</div>
          <input value="ignored" />
          <img src="ignored.png" />
        </div>
      </dd>
    </dl>
    """
    soup = BeautifulSoup(html, "lxml")

    responses = parse_responses(soup)

    assert len(responses) == 1
    response = responses[0]
    assert response["res_no"] == 12
    assert response["id_hash"] == "abc123"
    assert response["poster_name"] == "Alice"
    assert response["posted_at"] == "2026/03/07 12:34"
    assert response["content"] == "Hello\nWorld"
    assert "Hello\nWorld" in response["content_html"]
    assert "remove me" not in response["content_html"]
    assert "remove label" not in response["content_html"]
    assert "<img" not in response["content_html"]
    assert "<input" not in response["content_html"]


def test_parse_responses_partial_html_and_missing_res_no():
    html = """
    <dl>
      <dt class="st-bbs_reshead" data-id_hash="skip-me">
        <span class="st-bbs_name">NoNumber</span>
      </dt>
      <dd class="st-bbs_resbody">
        <div class="bbs_resbody_inner">Should be skipped</div>
      </dd>

      <dt class="st-bbs_reshead" data-res_no="13" data-id_hash="def456">
      </dt>
      <dd class="st-bbs_resbody">
      </dd>
    </dl>
    """
    soup = BeautifulSoup(html, "lxml")

    responses = parse_responses(soup)

    assert len(responses) == 1
    response = responses[0]
    assert response["res_no"] == 13
    assert response["id_hash"] == "def456"
    assert response["poster_name"] is None
    assert response["posted_at"] is None
    assert response["content"] == ""
    assert response["content_html"] == ""
