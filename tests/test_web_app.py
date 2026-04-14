from io import BytesIO
from unittest.mock import patch

import web_app
from web_app import application, check_article_status, create_app


def _run_wsgi_request(
    method,
    path="/",
    body="",
    query_string="",
    app=None,
    extra_environ=None,
):
    encoded_body = body.encode("utf-8")
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": query_string,
        "CONTENT_LENGTH": str(len(encoded_body)),
        "CONTENT_TYPE": "application/x-www-form-urlencoded",
        "wsgi.input": BytesIO(encoded_body),
    }
    if extra_environ:
        environ.update(extra_environ)

    response = (app or application)(environ, start_response)
    captured["body"] = b"".join(response).decode("utf-8")
    return captured


def _header_map(response):
    return dict(response["headers"])


def test_check_article_status_returns_saved_result_for_local_title_lookup():
    with patch(
        "web_app.get_saved_article_summary_by_exact_title",
        return_value={
            "found": True,
            "article_id": "5587284",
            "article_type": "id",
            "title": "G123",
            "url": "https://dic.nicovideo.jp/id/5587284",
            "created_at": "2026-03-25T00:00:00+00:00",
            "response_count": 42,
        },
    ):
        with patch("web_app.resolve_article_input") as mock_resolve:
            result = check_article_status("g123")

    mock_resolve.assert_not_called()
    assert result == {
        "status": "saved",
        "input": "g123",
        "title": "G123",
        "matched_by": "local_title_lookup",
        "article_url": "https://dic.nicovideo.jp/id/5587284",
        "article_id": "5587284",
        "article_type": "id",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }


def test_check_article_status_classifies_temporary_fetch_failure():
    empty_summary = {
        "found": False,
        "article_id": None,
        "article_type": None,
        "title": None,
        "url": None,
        "created_at": None,
        "response_count": 0,
    }

    with patch(
        "web_app.get_saved_article_summary_by_exact_title",
        return_value=empty_summary,
    ):
        with patch(
            "web_app.resolve_article_input",
            side_effect=RuntimeError(
                "Failed to fetch https://dic.nicovideo.jp/a/Foo (timeout=10s)"
            ),
        ):
            result = check_article_status("Foo")

    assert result["status"] == "resolution_failure"
    assert result["failure_kind"] == "temporary_fetch_failure"


def test_application_get_renders_externalized_title_and_waiting_state():
    response = _run_wsgi_request("GET")

    assert response["status"] == "200 OK"
    assert web_app.UI_TEXTS["page_title"] in response["body"]
    assert web_app.UI_TEXTS["input_placeholder"] in response["body"]
    assert "data-archive-check-form" in response["body"]
    assert "data-busy-message" in response["body"]
    assert "Saved articles will download as TXT." in response["body"]


def test_application_post_saved_result_autodownloads_without_buttons():
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
    assert "Saved article found. TXT download will start automatically." in (
        response["body"]
    )
    assert "Article title:</strong> Foo" in response["body"]
    assert "Article ID:</strong> 12345" in response["body"]
    assert "Saved response count:</strong> 42" in response["body"]
    assert "data-auto-download-form" in response["body"]
    assert "Download TXT" not in response["body"]
    assert "Add To Target Registry" not in response["body"]
    assert "Matched by" not in response["body"]


def test_application_post_registers_unsaved_article_and_logs_action(tmp_path):
    result = {
        "status": "unsaved",
        "input": "https://dic.nicovideo.jp/a/%E3%83%8B%E3%82%B3",
        "title": "ニコニコ大百科",
        "matched_by": "article_url",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "message": "Resolved article, but no saved archive was found yet.",
    }
    log_path = tmp_path / "web_action.log"

    with patch("web_app.check_article_status", return_value=result):
        with patch("web_app.register_target_url", return_value="added"):
            response = _run_wsgi_request(
                "POST",
                body="article_input=Foo",
                app=create_app(
                    target_db_path="/runtime/data/custom.db",
                    web_action_log_path=str(log_path),
                ),
                extra_environ={
                    "REMOTE_ADDR": "127.0.0.1",
                    "HTTP_USER_AGENT": "pytest-agent",
                },
            )

    assert response["status"] == "200 OK"
    assert "Article registered for archive checking." in response["body"]
    assert "Article title:</strong> ニコニコ大百科" in response["body"]
    assert "Saved response count" not in response["body"]

    log_text = log_path.read_text(encoding="utf-8")
    assert log_text.startswith("\nWEB_ACTION_START\n")
    assert log_text.endswith("WEB_ACTION_END\n\n")
    assert "action_kind=registration" in log_text
    assert "result_status=added" in log_text
    assert "resolved_title=ニコニコ大百科" in log_text
    assert "%E3%83%8B%E3%82%B3" not in log_text


def test_download_endpoint_returns_txt_and_logs_download(tmp_path):
    log_path = tmp_path / "web_action.log"

    with patch(
        "web_app.get_saved_article_txt",
        return_value={
            "found": True,
            "content": "=== ARTICLE META ===\nTitle: Foo",
            "article_id": "12345",
            "article_type": "a",
        },
    ):
        response = _run_wsgi_request(
            "GET",
            path="/download",
            query_string=(
                "article_id=12345&article_type=a&article_input=Foo"
                "&resolved_title=Foo&article_url="
                "https%3A%2F%2Fdic.nicovideo.jp%2Fa%2F12345"
                "&requested_format=txt"
            ),
            app=create_app(web_action_log_path=str(log_path)),
            extra_environ={
                "REMOTE_ADDR": "127.0.0.1",
                "HTTP_USER_AGENT": "pytest-agent",
            },
        )

    assert response["status"] == "200 OK"
    headers = _header_map(response)
    assert headers["Content-Type"] == "text/plain; charset=utf-8"
    assert "attachment; filename=\"12345a.txt\"" in headers[
        "Content-Disposition"
    ]
    assert "=== ARTICLE META ===" in response["body"]

    log_text = log_path.read_text(encoding="utf-8")
    assert "action_kind=download" in log_text
    assert "requested_format=txt" in log_text
    assert "result_status=success" in log_text


def test_application_post_error_result_is_short_and_logged(tmp_path):
    result = {
        "status": "resolution_failure",
        "input": "https://example.com/nope",
        "failure_kind": "invalid_input",
        "message": "Could not resolve the input.",
    }
    log_path = tmp_path / "web_action.log"

    with patch("web_app.check_article_status", return_value=result):
        response = _run_wsgi_request(
            "POST",
            body="article_input=https%3A%2F%2Fexample.com%2Fnope",
            app=create_app(web_action_log_path=str(log_path)),
            extra_environ={
                "REMOTE_ADDR": "127.0.0.1",
                "HTTP_USER_AGENT": "pytest-agent",
            },
        )

    assert response["status"] == "200 OK"
    assert "Enter an article name or a valid Nicopedia article URL." in (
        response["body"]
    )
    assert "Reference ID:</strong>" in response["body"]
    assert "Matched by" not in response["body"]
    assert "Resolution status" not in response["body"]

    log_text = log_path.read_text(encoding="utf-8")
    assert log_text.startswith("\nWEB_ACTION_START\n")
    assert log_text.endswith("WEB_ACTION_END\n\n")
    assert "action_kind=failed_action" in log_text
    assert "error_code=invalid_input" in log_text


def test_application_returns_not_found_for_unknown_path():
    response = _run_wsgi_request("GET", path="/missing")

    assert response["status"] == "404 Not Found"
    assert response["body"] == "Not Found"
