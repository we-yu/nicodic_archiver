from io import BytesIO
from unittest.mock import patch

from web_app import app


def _call_app(method: str, path: str, body: bytes = b""):
    status_holder = {}
    headers_holder = {}

    def start_response(status, headers):
        status_holder["status"] = status
        headers_holder["headers"] = dict(headers)

    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "wsgi.input": BytesIO(body),
        "CONTENT_LENGTH": str(len(body)),
        "CONTENT_TYPE": "application/x-www-form-urlencoded",
    }
    chunks = app(environ, start_response)
    content = b"".join(chunks)
    return status_holder["status"], headers_holder["headers"], content


def test_web_app_get_root_renders_form_and_message_area():
    status, headers, body = _call_app("GET", "/")
    text = body.decode("utf-8")

    assert status.startswith("200")
    assert headers["Content-Type"].startswith("text/html")
    assert 'name="article_input"' in text
    assert "<button" in text
    assert 'id="message"' in text


@patch("web_app.resolve_article_input")
def test_web_app_post_resolve_handles_resolution_failure(mock_resolve):
    mock_resolve.return_value = {
        "ok": False,
        "failure_kind": "invalid_input",
        "normalized_input": "",
    }

    status, _, body = _call_app("POST", "/resolve", body=b"article_input=")
    text = body.decode("utf-8")

    assert status.startswith("200")
    assert "Resolution failed: invalid_input" in text


@patch("web_app.has_saved_article", return_value=True)
@patch("web_app.resolve_article_input")
def test_web_app_post_resolve_success_saved(mock_resolve, mock_has_saved):
    mock_resolve.return_value = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/12345",
            "article_id": "12345",
            "article_type": "a",
        },
        "title": "Foo",
        "matched_by": "article_url",
        "normalized_input": "https://dic.nicovideo.jp/a/12345",
    }

    status, _, body = _call_app(
        "POST",
        "/resolve",
        body=b"article_input=https%3A%2F%2Fdic.nicovideo.jp%2Fa%2F12345",
    )
    text = body.decode("utf-8")

    assert status.startswith("200")
    assert "Saved article detected." in text
    assert "title=Foo" in text
    assert "url=https://dic.nicovideo.jp/a/12345" in text
    mock_has_saved.assert_called_once_with("12345", "a")


@patch("web_app.has_saved_article", return_value=False)
@patch("web_app.resolve_article_input")
def test_web_app_post_resolve_success_unsaved(mock_resolve, mock_has_saved):
    mock_resolve.return_value = {
        "ok": True,
        "canonical_target": {
            "article_url": "https://dic.nicovideo.jp/a/99999",
            "article_id": "99999",
            "article_type": "a",
        },
        "title": "Bar",
        "matched_by": "exact_title",
        "normalized_input": "Bar",
    }

    status, _, body = _call_app("POST", "/resolve", body=b"article_input=Bar")
    text = body.decode("utf-8")

    assert status.startswith("200")
    assert "No saved archive yet." in text
    assert "title=Bar" in text
    mock_has_saved.assert_called_once_with("99999", "a")


@patch("web_app.resolve_article_input", side_effect=RuntimeError("boom"))
def test_web_app_post_resolve_unexpected_error_returns_500(mock_resolve):
    status, _, body = _call_app("POST", "/resolve", body=b"article_input=Foo")
    text = body.decode("utf-8")

    assert status.startswith("500")
    assert "Internal error: RuntimeError" in text
    mock_resolve.assert_called_once()
