from unittest.mock import patch

from queue_drain import QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP, drain_queue_requests
from storage import enqueue_canonical_target, init_db


def _enqueue_target(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    conn = init_db()
    try:
        enqueue_canonical_target(
            conn,
            {
                "article_url": "https://dic.nicovideo.jp/a/12345",
                "article_id": "12345",
                "article_type": "a",
            },
            title="First Title",
        )
    finally:
        conn.close()


def _queue_count():
    conn = init_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM queue_requests")
        return cur.fetchone()[0]
    finally:
        conn.close()


@patch("queue_drain.run_scrape_outcome")
def test_drain_queue_requests_dequeues_on_normal_success(
    mock_run_scrape_outcome,
    tmp_path,
    monkeypatch,
):
    _enqueue_target(tmp_path, monkeypatch)
    mock_run_scrape_outcome.return_value = {"ok": True, "cap_reached": False}

    result = drain_queue_requests()

    mock_run_scrape_outcome.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        response_cap=QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP,
    )
    assert result["processed"] == 1
    assert result["dequeued"] == 1
    assert result["kept"] == 0
    assert result["results"] == [
        {
            "article_id": "12345",
            "article_type": "a",
            "result": "dequeued",
            "cap_reached": False,
        }
    ]
    assert _queue_count() == 0


@patch("queue_drain.run_scrape_outcome")
def test_drain_queue_requests_dequeues_on_cap_reached_success_class(
    mock_run_scrape_outcome,
    tmp_path,
    monkeypatch,
):
    _enqueue_target(tmp_path, monkeypatch)
    mock_run_scrape_outcome.return_value = {"ok": True, "cap_reached": True}

    result = drain_queue_requests()

    mock_run_scrape_outcome.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        response_cap=QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP,
    )
    assert result["processed"] == 1
    assert result["dequeued"] == 1
    assert result["kept"] == 0
    assert result["results"] == [
        {
            "article_id": "12345",
            "article_type": "a",
            "result": "dequeued",
            "cap_reached": True,
        }
    ]
    assert _queue_count() == 0


@patch("queue_drain.run_scrape_outcome")
def test_drain_queue_requests_keeps_request_on_unexpected_failure(
    mock_run_scrape_outcome,
    tmp_path,
    monkeypatch,
):
    _enqueue_target(tmp_path, monkeypatch)
    mock_run_scrape_outcome.side_effect = RuntimeError("boom")

    result = drain_queue_requests()

    mock_run_scrape_outcome.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        response_cap=QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP,
    )
    assert result["processed"] == 1
    assert result["dequeued"] == 0
    assert result["kept"] == 1
    assert result["results"] == [
        {
            "article_id": "12345",
            "article_type": "a",
            "result": "kept",
            "detail": "RuntimeError: boom",
        }
    ]
    assert _queue_count() == 1
