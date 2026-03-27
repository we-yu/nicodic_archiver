from io import BytesIO
from unittest.mock import Mock, patch

from web_app import application, check_article_status


def _run_wsgi_request(method, path="/", body=""):
    encoded_body = body.encode("utf-8")
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    response = application(
        {
            "REQUEST_METHOD": method,
            "PATH_INFO": path,
            "CONTENT_LENGTH": str(len(encoded_body)),
            "CONTENT_TYPE": "application/x-www-form-urlencoded",
            "wsgi.input": BytesIO(encoded_body),
        },
        start_response,
    )
    captured["body"] = b"".join(response).decode("utf-8")
    return captured


def _headers_as_dict(response):
    return dict(response["headers"])


def test_check_article_status_returns_resolution_failure():
    with patch(
        "web_app.get_saved_article_summary_by_exact_title",
        return_value={
            "found": False,
            "article_id": None,
            "article_type": None,
            "title": None,
            "url": None,
            "created_at": None,
            "response_count": 0,
        },
    ):
        with patch(
            "web_app.resolve_article_input",
            return_value={
                "ok": False,
                "failure_kind": "ambiguous",
                "normalized_input": "Foo",
            },
        ):
            result = check_article_status("Foo")

    assert result == {
        "status": "resolution_failure",
        "input": "Foo",
        "failure_kind": "ambiguous",
        "message": "Could not resolve the input (ambiguous).",
    }


def test_check_article_status_returns_saved_result_for_local_exact_title_match():
    with patch(
        "web_app.get_saved_article_summary_by_exact_title",
        return_value={
            "found": True,
            "article_id": "12345",
            "article_type": "a",
            "title": "Foo",
            "url": "https://dic.nicovideo.jp/a/12345",
            "created_at": "2026-03-25T00:00:00+00:00",
            "response_count": 42,
        },
    ):
        with patch("web_app.resolve_article_input") as mock_resolve:
            result = check_article_status("Foo")

    mock_resolve.assert_not_called()
    assert result == {
        "status": "saved",
        "input": "Foo",
        "title": "Foo",
        "matched_by": "local_exact_title",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }


def test_check_article_status_returns_saved_result_after_resolution():
    resolution = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        },
        "title": "Foo",
        "matched_by": "exact_title",
        "normalized_input": "Foo",
    }
    summary = {
        "found": True,
        "article_id": "12345",
        "article_type": "a",
        "title": "Foo",
        "url": "https://dic.nicovideo.jp/a/12345",
        "created_at": "2026-03-25T00:00:00+00:00",
        "response_count": 42,
    }

    with patch(
        "web_app.get_saved_article_summary_by_exact_title",
        return_value={
            "found": False,
            "article_id": None,
            "article_type": None,
            "title": None,
            "url": None,
            "created_at": None,
            "response_count": 0,
        },
    ):
        with patch("web_app.resolve_article_input", return_value=resolution):
            with patch("web_app.get_saved_article_summary", return_value=summary):
                result = check_article_status("Foo")

    assert result == {
        "status": "saved",
        "input": "Foo",
        "title": "Foo",
        "matched_by": "exact_title",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }


def test_check_article_status_returns_unsaved_result():
    resolution = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        },
        "title": "Foo",
        "matched_by": "article_url",
        "normalized_input": "https://dic.nicovideo.jp/a/Foo",
    }
    summary = {
        "found": False,
        "article_id": "12345",
        "article_type": "a",
        "title": None,
        "url": None,
        "created_at": None,
        "response_count": 0,
    }

    with patch("web_app.resolve_article_input", return_value=resolution):
        with patch("web_app.get_saved_article_summary", return_value=summary):
            result = check_article_status("https://dic.nicovideo.jp/a/Foo")

    assert result == {
        "status": "unsaved",
        "input": "https://dic.nicovideo.jp/a/Foo",
        "title": "Foo",
        "matched_by": "article_url",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "message": "Resolved article, but no saved archive was found yet.",
    }


def test_check_article_status_returns_internal_error_on_unexpected_exception():
    with patch(
        "web_app.get_saved_article_summary_by_exact_title",
        return_value={
            "found": False,
            "article_id": None,
            "article_type": None,
            "title": None,
            "url": None,
            "created_at": None,
            "response_count": 0,
        },
    ):
        with patch(
            "web_app.resolve_article_input",
            side_effect=RuntimeError("boom"),
        ):
            result = check_article_status("Foo")

    assert result == {
        "status": "internal_error",
        "input": "Foo",
        "error_kind": "RuntimeError",
        "message": "Internal error while checking article status.",
    }


def test_check_article_status_url_input_still_uses_resolver_path():
    resolution = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        },
        "title": "Foo",
        "matched_by": "article_url",
        "normalized_input": "https://dic.nicovideo.jp/a/g123",
    }
    summary = {
        "found": True,
        "article_id": "12345",
        "article_type": "a",
        "title": "Foo",
        "url": "https://dic.nicovideo.jp/a/12345",
        "created_at": "2026-03-25T00:00:00+00:00",
        "response_count": 42,
    }

    with patch("web_app.get_saved_article_summary_by_exact_title") as mock_local:
        with patch("web_app.resolve_article_input", return_value=resolution):
            with patch("web_app.get_saved_article_summary", return_value=summary):
                result = check_article_status("https://dic.nicovideo.jp/a/g123")

    mock_local.assert_not_called()
    assert result["status"] == "saved"
    assert result["matched_by"] == "article_url"


def test_application_get_renders_form_and_message_area():
    response = _run_wsgi_request("GET")

    assert response["status"] == "200 OK"
    assert "Article name or article URL" in response["body"]
    assert ">Submit</button>" in response["body"]
    assert "Submit an article name or article URL" in response["body"]


def test_application_post_renders_saved_result_message():
    result = {
        "status": "saved",
        "input": "Foo",
        "title": "Foo",
        "matched_by": "exact_title",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }

    with patch("web_app.check_article_status", return_value=result):
        response = _run_wsgi_request("POST", body="article_input=Foo")

    assert response["status"] == "200 OK"
    assert "Saved archive found for the resolved article." in response["body"]
    assert "Canonical target: a/12345" in response["body"]
    assert "Saved response count: 42" in response["body"]
    assert "Download TXT" in response["body"]


def test_application_post_renders_unsaved_enqueue_action():
    result = {
        "status": "unsaved",
        "input": "Foo",
        "title": "Foo",
        "matched_by": "exact_title",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "message": "Resolved article, but no saved archive was found yet.",
    }

    with patch("web_app.check_article_status", return_value=result):
        response = _run_wsgi_request("POST", body="article_input=Foo")

    assert response["status"] == "200 OK"
    assert "Resolved article, but no saved archive was found yet." in response["body"]
    assert "Enqueue" in response["body"]


def test_application_post_download_txt_returns_attachment_response():
    status_result = {
        "status": "saved",
        "input": "Foo",
        "title": "Foo",
        "matched_by": "exact_title",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }

    with patch("web_app.check_article_status", return_value=status_result):
        with patch(
            "web_app.get_saved_article_txt",
            return_value={
                "found": True,
                "content": "txt payload",
                "article_id": "12345",
                "article_type": "a",
            },
        ):
            response = _run_wsgi_request(
                "POST",
                body="action=download_txt&article_input=Foo",
            )

    headers = _headers_as_dict(response)
    assert response["status"] == "200 OK"
    assert response["body"] == "txt payload"
    assert headers["Content-Type"] == "text/plain; charset=utf-8"
    assert headers["Content-Disposition"] == (
        'attachment; filename="nicodic_a_12345.txt"'
    )


def test_application_post_enqueue_returns_success_message_for_duplicate_queue():
    status_result = {
        "status": "unsaved",
        "input": "Foo",
        "title": "Foo",
        "matched_by": "exact_title",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "message": "Resolved article, but no saved archive was found yet.",
    }
    mock_conn = Mock()

    with patch("web_app.check_article_status", return_value=status_result):
        with patch("web_app.init_db", return_value=mock_conn):
            with patch(
                "web_app.enqueue_canonical_target",
                return_value={
                    "status": "duplicate",
                    "entry": {
                        "article_url": "https://dic.nicovideo.jp/a/12345",
                        "article_id": "12345",
                        "article_type": "a",
                        "title": "Foo",
                        "enqueued_at": "2026-03-27T00:00:00+00:00",
                    },
                    "queue_identity": {
                        "article_id": "12345",
                        "article_type": "a",
                    },
                },
            ):
                response = _run_wsgi_request(
                    "POST",
                    body="action=enqueue&article_input=Foo",
                )

    assert response["status"] == "200 OK"
    assert "Queue request accepted; article was already queued." in response["body"]
    assert "Queue status: <strong>duplicate</strong>" in response["body"]
    assert "message-area queued" in response["body"]
    mock_conn.close.assert_called_once_with()


def test_application_post_enqueue_recheck_can_return_saved_result():
    result = {
        "status": "saved",
        "input": "Foo",
        "title": "Foo",
        "matched_by": "exact_title",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }

    with patch("web_app.check_article_status", return_value=result):
        response = _run_wsgi_request(
            "POST",
            body="action=enqueue&article_input=Foo",
        )

    assert response["status"] == "200 OK"
    assert (
        "Saved archive found during enqueue recheck. Download TXT instead."
        in response["body"]
    )
    assert "Download TXT" in response["body"]


def test_application_post_renders_title_resolution_failure_message():
    result = {
        "status": "resolution_failure",
        "input": "Foo",
        "failure_kind": "not_found",
        "message": "Could not resolve the input (not_found).",
    }

    with patch("web_app.check_article_status", return_value=result):
        response = _run_wsgi_request("POST", body="article_input=Foo")

    assert response["status"] == "200 OK"
    assert "Could not resolve the input (not_found)." in response["body"]
    assert "Resolution status: <strong>not_found</strong>" in response["body"]


def test_application_returns_not_found_for_unknown_path():
    response = _run_wsgi_request("GET", path="/missing")

    assert response["status"] == "404 Not Found"
    assert response["body"] == "Not Found"
