"""Tests for nicodic_archiver.scraper."""

from unittest.mock import MagicMock, call, patch

import pytest

from nicodic_archiver.scraper import (
    fetch_all_responses,
    fetch_new_responses,
    fetch_responses,
)


def _make_api_response(responses: list[dict], total: int) -> dict:
    return {"response": responses, "totalResponseCount": total}


def _resp(no: int) -> dict:
    return {"no": no, "userId": "u1", "body": f"body {no}", "date": "2024-01-01T00:00:00+09:00"}


class TestFetchResponses:
    def test_calls_correct_url(self):
        mock_session = MagicMock()
        mock_session.get.return_value.json.return_value = _make_api_response([], 0)
        mock_session.get.return_value.raise_for_status.return_value = None

        fetch_responses("testarticle", from_no=1, limit=10, session=mock_session)

        mock_session.get.assert_called_once_with(
            "https://dic.nicovideo.jp/api/v1/topic/article:testarticle/responses",
            params={"from": 1, "limit": 10, "dir": "asc"},
            timeout=30,
        )

    def test_raises_on_http_error(self):
        mock_session = MagicMock()
        mock_session.get.return_value.raise_for_status.side_effect = Exception("404")

        with pytest.raises(Exception, match="404"):
            fetch_responses("missing", session=mock_session)


class TestFetchAllResponses:
    def test_single_page(self):
        responses = [_resp(i) for i in range(1, 4)]
        mock_session = MagicMock()
        mock_session.get.return_value.json.return_value = _make_api_response(responses, 3)
        mock_session.get.return_value.raise_for_status.return_value = None

        result = fetch_all_responses("art", session=mock_session)

        assert len(result) == 3
        assert result[0]["no"] == 1

    def test_multiple_pages(self):
        page1 = [_resp(i) for i in range(1, 3)]
        page2 = [_resp(i) for i in range(3, 5)]

        responses_iter = iter([
            _make_api_response(page1, 4),
            _make_api_response(page2, 4),
        ])

        mock_session = MagicMock()
        mock_session.get.return_value.raise_for_status.return_value = None
        mock_session.get.return_value.json.side_effect = lambda: next(responses_iter)

        result = fetch_all_responses("art", session=mock_session)

        assert len(result) == 4
        assert mock_session.get.call_count == 2

    def test_empty_result(self):
        mock_session = MagicMock()
        mock_session.get.return_value.json.return_value = _make_api_response([], 0)
        mock_session.get.return_value.raise_for_status.return_value = None

        result = fetch_all_responses("empty", session=mock_session)

        assert result == []


class TestFetchNewResponses:
    def test_fetches_from_last_no_plus_one(self):
        mock_session = MagicMock()
        mock_session.get.return_value.json.return_value = _make_api_response([], 50)
        mock_session.get.return_value.raise_for_status.return_value = None

        fetch_new_responses("art", last_no=10, session=mock_session)

        call_params = mock_session.get.call_args[1]["params"]
        assert call_params["from"] == 11

    def test_last_no_zero_fetches_from_one(self):
        mock_session = MagicMock()
        mock_session.get.return_value.json.return_value = _make_api_response([], 0)
        mock_session.get.return_value.raise_for_status.return_value = None

        fetch_new_responses("art", last_no=0, session=mock_session)

        call_params = mock_session.get.call_args[1]["params"]
        assert call_params["from"] == 1

    def test_returns_only_new_responses(self):
        new = [_resp(11), _resp(12)]
        mock_session = MagicMock()
        mock_session.get.return_value.json.return_value = _make_api_response(new, 12)
        mock_session.get.return_value.raise_for_status.return_value = None

        result = fetch_new_responses("art", last_no=10, session=mock_session)

        assert [r["no"] for r in result] == [11, 12]
