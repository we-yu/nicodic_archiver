from io import BytesIO
from unittest.mock import patch

from web_app import application, check_article_status
import web_app


def _run_wsgi_request(method, path="/", body="", query_string="", app=None):
    encoded_body = body.encode("utf-8")
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    response = (app or application)(
        {
            "REQUEST_METHOD": method,
            "PATH_INFO": path,
            "QUERY_STRING": query_string,
            "CONTENT_LENGTH": str(len(encoded_body)),
            "CONTENT_TYPE": "application/x-www-form-urlencoded",
            "wsgi.input": BytesIO(encoded_body),
        },
        start_response,
    )
    captured["body"] = b"".join(response).decode("utf-8")
    return captured


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
    assert "NicoNicoPedia Archive Checker" in response["body"]
    assert "記事名 / 記事URL" in response["body"]
    assert "例:" in response["body"]
    assert "Submit" in response["body"]


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
    headers = dict(response["headers"])
    assert headers["Content-Type"] == "text/html; charset=utf-8"
    assert "保存済みの記事が見つかりました" in response["body"]
    assert "Title:" in response["body"]
    assert "Article ID:" in response["body"]
    assert "/download?" in response["body"]


@patch("web_app.append_web_action_log")
@patch(
    "web_app.get_saved_article_txt",
    return_value={
        "found": True,
        "content": "=== ARTICLE META ===\nTitle: Foo",
        "article_id": "12345",
        "article_type": "a",
    },
)
def test_application_download_returns_attachment_and_logs_action(
    mock_get_txt,
    mock_append_log,
):
    response = _run_wsgi_request(
        "GET",
        path="/download",
        query_string=(
            "article_id=12345&article_type=a&format=txt&action_id=aid1"
            "&input=Foo&title=Foo&url=https%3A%2F%2Fdic.nicovideo.jp%2Fa%2F12345"
        ),
    )

    assert response["status"] == "200 OK"
    headers = dict(response["headers"])
    assert headers["Content-Type"] == "text/plain; charset=utf-8"
    assert "attachment; filename=" in headers["Content-Disposition"]
    assert "=== ARTICLE META ===" in response["body"]
    mock_get_txt.assert_called_once_with("12345", "a")
    assert mock_append_log.call_count == 1
    logged = mock_append_log.call_args[0][0]
    assert logged["action_kind"] == "download"
    assert logged["result_status"] == "ok"


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
    assert "記事が見つかりませんでした" in response["body"]


def test_application_post_renders_unsaved_result_with_target_action_only():
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
        with patch("web_app.register_target_url", return_value="added"):
            response = _run_wsgi_request("POST", body="article_input=Foo")

    assert response["status"] == "200 OK"
    assert "取得対象として登録しました" in response["body"]
    assert "Download TXT" not in response["body"]
    assert "Add To Target Registry" not in response["body"]
    assert "<form method=\"post\" action=\"/action\">" not in response["body"]


@patch("web_app.append_web_action_log")
@patch("web_app.register_target_url", return_value="added")
@patch("web_app.check_article_status")
def test_application_post_unsaved_registers_and_logs_action(
    mock_check_status,
    mock_add_target_url,
    mock_append_log,
):
    mock_check_status.return_value = {
        "status": "unsaved",
        "input": "Foo",
        "title": "Foo",
        "matched_by": "exact_title",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "message": "Resolved article, but no saved archive was found yet.",
    }

    response = _run_wsgi_request("POST", body="article_input=Foo")

    assert response["status"] == "200 OK"
    assert "取得対象として登録しました" in response["body"]
    mock_add_target_url.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        web_app.DEFAULT_TARGET_DB_PATH,
    )
    assert mock_append_log.call_count == 1
    logged = mock_append_log.call_args[0][0]
    assert logged["action_kind"] == "registration"
    assert logged["result_status"] == "ok"
    assert logged["resolved_title"] == "Foo"
    assert logged["resolved_article_id"] == "12345"
    assert logged["resolved_article_type"] == "a"
    assert logged["resolved_canonical_url"] == "https://dic.nicovideo.jp/a/12345"
    assert logged["requested_format"] == "txt"


def test_application_returns_not_found_for_unknown_path():
    response = _run_wsgi_request("GET", path="/missing")

    assert response["status"] == "404 Not Found"
    assert response["body"] == "Not Found"
