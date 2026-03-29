from io import BytesIO
from unittest.mock import patch

from web_app import application, check_article_status, create_app


def _run_wsgi_request(method, path="/", body="", app=None):
    encoded_body = body.encode("utf-8")
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    response = (app or application)(
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
    assert "Article name or article URL" in response["body"]
    assert "Submit" in response["body"]
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
    assert "Add To Target Registry" in response["body"]


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
        response = _run_wsgi_request("POST", body="article_input=Foo")

    assert response["status"] == "200 OK"
    assert "Add To Target Registry" in response["body"]
    assert "Enqueue Request" not in response["body"]


@patch("web_app.register_target_url", return_value="added")
@patch("web_app.check_article_status")
def test_application_action_add_target_uses_canonical_url(
    mock_check_status,
    mock_add_target_url,
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

    response = _run_wsgi_request(
        "POST",
        path="/action",
        body="article_input=Foo&action=add_target",
    )

    assert response["status"] == "200 OK"
    assert (
        "Canonical article URL was added to the target registry."
        in response["body"]
    )
    mock_add_target_url.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        "data/nicodic.db",
    )


@patch("web_app.register_target_url", return_value="added")
@patch("web_app.check_article_status")
def test_application_action_add_target_uses_custom_runtime_target_path(
    mock_check_status,
    mock_add_target_url,
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

    response = _run_wsgi_request(
        "POST",
        path="/action",
        body="article_input=Foo&action=add_target",
        app=create_app(target_db_path="/runtime/data/custom.db"),
    )

    assert response["status"] == "200 OK"
    mock_add_target_url.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        "/runtime/data/custom.db",
    )


@patch("web_app.register_target_url", return_value="duplicate")
@patch("web_app.check_article_status")
def test_application_action_add_target_duplicate_is_bounded(
    mock_check_status,
    mock_add_target_url,
):
    mock_check_status.return_value = {
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

    response = _run_wsgi_request(
        "POST",
        path="/action",
        body="article_input=Foo&action=add_target",
    )

    assert response["status"] == "200 OK"
    assert (
        "Canonical article URL is already active in the target registry."
        in response["body"]
    )
    mock_add_target_url.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        "data/nicodic.db",
    )


@patch("web_app.register_target_url", return_value="reactivated")
@patch("web_app.check_article_status")
def test_application_action_add_target_reactivated_is_bounded(
    mock_check_status,
    mock_add_target_url,
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

    response = _run_wsgi_request(
        "POST",
        path="/action",
        body="article_input=Foo&action=add_target",
    )

    assert response["status"] == "200 OK"
    assert (
        "Canonical article URL was reactivated in the target registry."
        in response["body"]
    )
    mock_add_target_url.assert_called_once_with(
        "https://dic.nicovideo.jp/a/12345",
        "data/nicodic.db",
    )


@patch("web_app.check_article_status")
def test_application_action_add_target_skips_write_for_unresolved_input(
    mock_check_status,
):
    mock_check_status.return_value = {
        "status": "resolution_failure",
        "input": "Foo",
        "failure_kind": "not_found",
        "message": "Could not resolve the input (not_found).",
    }

    with patch("web_app.register_target_url") as mock_add_target_url:
        response = _run_wsgi_request(
            "POST",
            path="/action",
            body="article_input=Foo&action=add_target",
        )

    assert response["status"] == "200 OK"
    assert "Action requires a saved or unsaved resolved article." in response["body"]
    mock_add_target_url.assert_not_called()


@patch("web_app.get_saved_article_txt")
@patch("web_app.check_article_status")
def test_application_action_download_txt_for_saved(
    mock_check_status,
    mock_get_saved_article_txt,
):
    mock_check_status.return_value = {
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
    mock_get_saved_article_txt.return_value = {
        "found": True,
        "content": "=== ARTICLE META ===\nTitle: Foo",
        "article_id": "12345",
        "article_type": "a",
    }

    response = _run_wsgi_request(
        "POST",
        path="/action",
        body="article_input=Foo&action=download_txt",
    )

    assert response["status"] == "200 OK"
    headers = dict(response["headers"])
    assert headers["Content-Type"] == "text/plain; charset=utf-8"
    assert "attachment; filename=\"12345a.txt\"" in headers["Content-Disposition"]
    assert "=== ARTICLE META ===" in response["body"]
    mock_check_status.assert_called_once_with("Foo")
    mock_get_saved_article_txt.assert_called_once_with("12345", "a")


@patch("web_app.check_article_status")
def test_application_action_enqueue_request_is_not_supported_from_web(
    mock_check_status,
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

    with patch("web_app.register_target_url") as mock_add_target_url:
        response = _run_wsgi_request(
            "POST",
            path="/action",
            body="article_input=Foo&action=enqueue_request",
        )

    assert response["status"] == "200 OK"
    assert "Unsupported action." in response["body"]
    mock_add_target_url.assert_not_called()


def test_application_returns_not_found_for_unknown_path():
    response = _run_wsgi_request("GET", path="/missing")

    assert response["status"] == "404 Not Found"
    assert response["body"] == "Not Found"
