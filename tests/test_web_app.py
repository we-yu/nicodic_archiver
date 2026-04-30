from io import BytesIO
import sqlite3
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


def test_check_article_status_restores_saved_parity_for_decoded_url_input():
    with patch(
        "web_app.resolve_article_input",
        return_value={
            "ok": True,
            "normalized_input": "https://dic.nicovideo.jp/a/たつきショック",
            "matched_by": "article_url",
            "title": "たつきショック",
            "canonical_target": {
                "article_url": "https://dic.nicovideo.jp/id/5502789",
                "article_id": "5502789",
                "article_type": "id",
            },
        },
    ):
        with patch(
            "web_app.get_saved_article_summary",
            return_value={
                "found": False,
                "article_id": "5502789",
                "article_type": "id",
                "title": None,
                "url": None,
                "created_at": None,
                "published_at": None,
                "modified_at": None,
                "response_count": 0,
            },
        ):
            with patch(
                "web_app.get_saved_article_summary_by_exact_title",
                return_value={
                    "found": True,
                    "article_id": "5502789",
                    "article_type": "a",
                    "title": "たつきショック",
                    "url": "https://dic.nicovideo.jp/a/たつきショック",
                    "created_at": "2026-03-25T00:00:00+00:00",
                    "published_at": None,
                    "modified_at": None,
                    "response_count": 42,
                },
            ):
                result = check_article_status(
                    "https://dic.nicovideo.jp/a/たつきショック"
                )

    assert result["status"] == "saved"
    assert result["matched_by"] == "article_url"
    assert result["article_id"] == "5502789"
    assert result["article_type"] == "a"
    assert result["article_url"] == "https://dic.nicovideo.jp/a/たつきショック"


def test_check_article_status_restores_saved_parity_for_encoded_url_input():
    with patch(
        "web_app.resolve_article_input",
        return_value={
            "ok": True,
            "normalized_input": (
                "https://dic.nicovideo.jp/a/"
                "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF"
            ),
            "matched_by": "article_url",
            "title": "たつきショック",
            "canonical_target": {
                "article_url": "https://dic.nicovideo.jp/id/5502789",
                "article_id": "5502789",
                "article_type": "id",
            },
        },
    ):
        with patch(
            "web_app.get_saved_article_summary",
            return_value={
                "found": False,
                "article_id": "5502789",
                "article_type": "id",
                "title": None,
                "url": None,
                "created_at": None,
                "published_at": None,
                "modified_at": None,
                "response_count": 0,
            },
        ):
            with patch(
                "web_app.get_saved_article_summary_by_exact_title",
                return_value={
                    "found": True,
                    "article_id": "5502789",
                    "article_type": "a",
                    "title": "たつきショック",
                    "url": "https://dic.nicovideo.jp/a/たつきショック",
                    "created_at": "2026-03-25T00:00:00+00:00",
                    "published_at": None,
                    "modified_at": None,
                    "response_count": 42,
                },
            ):
                result = check_article_status(
                    "https://dic.nicovideo.jp/a/"
                    "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF"
                )

    assert result["status"] == "saved"
    assert result["matched_by"] == "article_url"
    assert result["article_id"] == "5502789"
    assert result["article_type"] == "a"
    assert result["article_url"] == "https://dic.nicovideo.jp/a/たつきショック"


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


def test_check_article_status_tolerates_old_schema_saved_lookup(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    conn = sqlite3.connect(data_dir / "nicodic.db")
    try:
        conn.execute(
            """
            CREATE TABLE articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id TEXT NOT NULL,
                article_type TEXT NOT NULL,
                title TEXT NOT NULL,
                canonical_url TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(article_id, article_type)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id TEXT NOT NULL,
                article_type TEXT NOT NULL,
                res_no INTEGER NOT NULL,
                id_hash TEXT,
                poster_name TEXT,
                posted_at TEXT,
                content_text TEXT,
                UNIQUE(article_id, article_type, res_no)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO articles (article_id, article_type, title, canonical_url)
            VALUES (?, ?, ?, ?)
            """,
            (
                "5502789",
                "a",
                "たつきショック",
                "https://dic.nicovideo.jp/a/%E3%81%9F%E3%81%A4%E3%81%8D"
                "%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF",
            ),
        )
        conn.execute(
            """
            INSERT INTO responses (article_id, article_type, res_no, content_text)
            VALUES (?, ?, ?, ?)
            """,
            ("5502789", "a", 1, "saved"),
        )
        conn.commit()
    finally:
        conn.close()

    with patch("web_app.resolve_article_input") as mock_resolve:
        result = check_article_status("たつきショック")

    mock_resolve.assert_not_called()
    assert result["status"] == "saved"
    assert result["title"] == "たつきショック"
    assert result["response_count"] == 1


def test_check_article_status_tolerates_old_schema_saved_lookup_for_url(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    conn = sqlite3.connect(data_dir / "nicodic.db")
    try:
        conn.execute(
            """
            CREATE TABLE articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id TEXT NOT NULL,
                article_type TEXT NOT NULL,
                title TEXT NOT NULL,
                canonical_url TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(article_id, article_type)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id TEXT NOT NULL,
                article_type TEXT NOT NULL,
                res_no INTEGER NOT NULL,
                id_hash TEXT,
                poster_name TEXT,
                posted_at TEXT,
                content_text TEXT,
                UNIQUE(article_id, article_type, res_no)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO articles (article_id, article_type, title, canonical_url)
            VALUES (?, ?, ?, ?)
            """,
            (
                "5502789",
                "a",
                "たつきショック",
                "https://dic.nicovideo.jp/a/%E3%81%9F%E3%81%A4%E3%81%8D"
                "%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF",
            ),
        )
        conn.execute(
            """
            INSERT INTO responses (article_id, article_type, res_no, content_text)
            VALUES (?, ?, ?, ?)
            """,
            ("5502789", "a", 1, "saved"),
        )
        conn.commit()
    finally:
        conn.close()

    with patch(
        "web_app.resolve_article_input",
        return_value={
            "ok": True,
            "normalized_input": "https://dic.nicovideo.jp/a/たつきショック",
            "matched_by": "article_url",
            "title": "たつきショック",
            "canonical_target": {
                "article_id": "5502789",
                "article_type": "a",
                "article_url": "https://dic.nicovideo.jp/a/たつきショック",
            },
        },
    ) as mock_resolve:
        result = check_article_status(
            "https://dic.nicovideo.jp/a/たつきショック"
        )

    mock_resolve.assert_called_once()
    assert result["status"] == "saved"
    assert result["matched_by"] == "article_url"
    assert result["response_count"] == 1


def test_application_get_renders_externalized_title_and_waiting_state():
    response = _run_wsgi_request("GET")

    assert response["status"] == "200 OK"
    assert web_app.UI_TEXTS["page_title"] in response["body"]
    assert web_app.UI_TEXTS["input_placeholder"] in response["body"]
    assert "data-archive-check-form" in response["body"]
    assert "data-busy-message" in response["body"]
    assert "Saved articles will download as TXT." in response["body"]
    assert "overflow-wrap: anywhere;" in response["body"]


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
    assert 'name="requested_format" value="txt"' in response["body"]


def test_saved_title_input_with_md_selection_keeps_saved_download_flow():
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
            body="article_input=Foo&requested_format=md",
        )

    assert response["status"] == "200 OK"
    assert "Saved article found. Markdown download will start" in (
        response["body"]
    )
    assert 'name="requested_format" value="md"' in response["body"]


def test_saved_title_input_with_csv_selection_keeps_saved_download_flow():
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
            body="article_input=Foo&requested_format=csv",
        )

    assert response["status"] == "200 OK"
    assert "Saved article found. CSV download will start automatically." in (
        response["body"]
    )
    assert 'name="requested_format" value="csv"' in response["body"]


def test_saved_result_encoded_url_input_autodownload_ok(
):
    result = {
        "status": "saved",
        "input": (
            "https://dic.nicovideo.jp/a/"
            "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF"
        ),
        "title": "たつきショック",
        "matched_by": "article_url",
        "article_url": "https://dic.nicovideo.jp/a/たつきショック",
        "article_id": "5502789",
        "article_type": "a",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }

    with patch("web_app.check_article_status", return_value=result):
        response = _run_wsgi_request(
            "POST",
            body=(
                "article_input=https%3A%2F%2Fdic.nicovideo.jp%2Fa%2F"
                "%25E3%2581%259F%25E3%2581%25A4%25E3%2581%258D%25E3%2582%25B7"
                "%25E3%2583%25A7%25E3%2583%2583%25E3%2582%25AF"
            ),
        )

    assert response["status"] == "200 OK"
    assert "Saved article found. TXT download will start automatically." in (
        response["body"]
    )
    assert "data-auto-download-form" in response["body"]
    assert 'name="article_id" value="5502789"' in response["body"]
    assert 'name="article_type" value="a"' in response["body"]
    assert 'name="resolved_title" value="たつきショック"' in response["body"]
    assert response["body"].count('name="article_input"') == 1


def test_saved_result_decoded_url_input_autodownload_ok(
):
    result = {
        "status": "saved",
        "input": "https://dic.nicovideo.jp/a/たつきショック",
        "title": "たつきショック",
        "matched_by": "article_url",
        "article_url": "https://dic.nicovideo.jp/a/たつきショック",
        "article_id": "5502789",
        "article_type": "a",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }

    with patch("web_app.check_article_status", return_value=result):
        response = _run_wsgi_request(
            "POST",
            body=(
                "article_input=https%3A%2F%2Fdic.nicovideo.jp%2Fa%2F"
                "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF"
            ),
        )

    assert response["status"] == "200 OK"
    assert "Saved article found. TXT download will start automatically." in (
        response["body"]
    )
    assert "data-auto-download-form" in response["body"]
    assert 'name="article_id" value="5502789"' in response["body"]
    assert 'name="article_type" value="a"' in response["body"]
    assert 'name="resolved_title" value="たつきショック"' in response["body"]
    assert response["body"].count('name="article_input"') == 1


def test_saved_decoded_url_input_with_csv_selection_keeps_download_flow():
    result = {
        "status": "saved",
        "input": "https://dic.nicovideo.jp/a/たつきショック",
        "title": "たつきショック",
        "matched_by": "article_url",
        "article_url": "https://dic.nicovideo.jp/a/たつきショック",
        "article_id": "5502789",
        "article_type": "a",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }

    with patch("web_app.check_article_status", return_value=result):
        response = _run_wsgi_request(
            "POST",
            body=(
                "article_input=https%3A%2F%2Fdic.nicovideo.jp%2Fa%2F"
                "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF"
                "&requested_format=csv"
            ),
        )

    assert response["status"] == "200 OK"
    assert 'name="requested_format" value="csv"' in response["body"]


def test_saved_encoded_url_input_with_csv_selection_keeps_download_flow():
    result = {
        "status": "saved",
        "input": (
            "https://dic.nicovideo.jp/a/"
            "%E3%81%9F%E3%81%A4%E3%81%8D%E3%82%B7%E3%83%A7%E3%83%83%E3%82%AF"
        ),
        "title": "たつきショック",
        "matched_by": "article_url",
        "article_url": "https://dic.nicovideo.jp/a/たつきショック",
        "article_id": "5502789",
        "article_type": "a",
        "response_count": 42,
        "message": "Saved archive found for the resolved article.",
    }

    with patch("web_app.check_article_status", return_value=result):
        response = _run_wsgi_request(
            "POST",
            body=(
                "article_input=https%3A%2F%2Fdic.nicovideo.jp%2Fa%2F"
                "%25E3%2581%259F%25E3%2581%25A4%25E3%2581%258D%25E3%2582%25B7"
                "%25E3%2583%25A7%25E3%2583%2583%25E3%2582%25AF"
                "&requested_format=csv"
            ),
        )

    assert response["status"] == "200 OK"
    assert 'name="requested_format" value="csv"' in response["body"]


def test_application_post_registration_write_failure_returns_bounded_error():
    result = {
        "status": "unsaved",
        "input": "https://dic.nicovideo.jp/a/%E6%9C%AA%E4%BF%9D%E5%AD%98",
        "title": "未保存記事",
        "matched_by": "article_url",
        "article_url": "https://dic.nicovideo.jp/a/%E6%9C%AA%E4%BF%9D%E5%AD%98",
        "article_id": "12345",
        "article_type": "a",
        "message": "Resolved article, but no saved archive was found yet.",
    }

    with patch("web_app.check_article_status", return_value=result):
        with patch(
            "web_app.register_target_url",
            side_effect=sqlite3.OperationalError(
                "attempt to write a readonly database"
            ),
        ):
            response = _run_wsgi_request(
                "POST",
                body=(
                    "article_input=https%3A%2F%2Fdic.nicovideo.jp%2Fa%2F"
                    "%E6%9C%AA%E4%BF%9D%E5%AD%98"
                ),
            )

    assert response["status"] == "200 OK"
    assert "An unexpected internal error occurred." in response["body"]
    assert "Reference ID:</strong>" in response["body"]


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


def test_application_post_unsaved_result_stays_200_when_log_write_fails():
    result = {
        "status": "unsaved",
        "input": "Foo",
        "title": "ニコニコ大百科",
        "matched_by": "exact_title",
        "article_url": "https://dic.nicovideo.jp/a/12345",
        "article_id": "12345",
        "article_type": "a",
        "message": "Resolved article, but no saved archive was found yet.",
    }

    with patch("web_app.check_article_status", return_value=result):
        with patch("web_app.register_target_url", return_value="added"):
            with patch(
                "web_app._append_web_action_log_lines",
                side_effect=PermissionError("denied"),
            ):
                response = _run_wsgi_request("POST", body="article_input=Foo")

    assert response["status"] == "200 OK"
    assert "Article registered for archive checking." in response["body"]
    assert "Article title:</strong> ニコニコ大百科" in response["body"]


def test_application_post_error_result_stays_200_when_log_write_fails():
    result = {
        "status": "resolution_failure",
        "input": "https://example.com/nope",
        "failure_kind": "invalid_input",
        "message": "Could not resolve the input.",
    }

    with patch("web_app.check_article_status", return_value=result):
        with patch(
            "web_app._append_web_action_log_lines",
            side_effect=PermissionError("denied"),
        ):
            response = _run_wsgi_request(
                "POST",
                body="article_input=https%3A%2F%2Fexample.com%2Fnope",
            )

    assert response["status"] == "200 OK"
    assert "Enter an article name or a valid Nicopedia article URL." in (
        response["body"]
    )
    assert "Reference ID:</strong>" in response["body"]


def test_download_endpoint_returns_txt_and_logs_download(tmp_path):
    log_path = tmp_path / "web_action.log"

    with patch(
        "web_app.get_saved_article_export",
        return_value={
            "found": True,
            "content": "=== ARTICLE META ===\nTitle: Foo",
            "article_id": "12345",
            "article_type": "a",
            "title": "たつきショック",
            "format": "txt",
        },
    ):
        response = _run_wsgi_request(
            "GET",
            path="/download",
            query_string=(
                "article_id=12345&article_type=a"
                "&resolved_title=Foo&requested_format=txt"
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
    assert "filename=\"12345a_article.txt\"" in headers[
        "Content-Disposition"
    ]
    assert "filename*=UTF-8''12345a_%E3%81%9F%E3%81%A4" in headers[
        "Content-Disposition"
    ]
    assert "=== ARTICLE META ===" in response["body"]

    log_text = log_path.read_text(encoding="utf-8")
    assert "action_kind=download" in log_text
    assert "requested_format=txt" in log_text
    assert "result_status=success" in log_text


def test_download_endpoint_returns_csv_content_type(tmp_path):
    log_path = tmp_path / "web_action.log"

    with patch(
        "web_app.get_saved_article_export",
        return_value={
            "found": True,
            "content": "article_id,res_no\n12345,1\n",
            "article_id": "12345",
            "article_type": "a",
            "title": "たつきショック",
            "format": "csv",
        },
    ):
        response = _run_wsgi_request(
            "GET",
            path="/download",
            query_string=(
                "article_id=12345&article_type=a"
                "&resolved_title=Foo&requested_format=csv"
            ),
            app=create_app(web_action_log_path=str(log_path)),
        )

    assert response["status"] == "200 OK"
    headers = _header_map(response)
    assert headers["Content-Type"] == "text/csv; charset=utf-8"
    assert "filename=\"12345a_article.csv\"" in headers[
        "Content-Disposition"
    ]


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


def test_top_page_includes_registered_list_link():
    response = _run_wsgi_request("GET")

    assert response["status"] == "200 OK"
    assert 'href="/registered"' in response["body"]
    assert 'target="_blank"' in response["body"]
    assert "登録済み記事一覧" in response["body"]


def _mock_query_result(rows):
    """Build a query_registered_articles return value for mocking."""
    return {
        "rows": rows,
        "total": len(rows),
        "page": 1,
        "per_page": 100,
    }


def _make_reg_row(
    article_id="12345",
    article_type="a",
    title="テスト記事",
    canonical_url="https://dic.nicovideo.jp/a/12345",
    saved_response_count=42,
    latest_scraped_max_res_no=50,
    last_scraped_at="2026-01-01T00:00:00+00:00",
    created_at="2026-01-01T00:00:00+00:00",
):
    return {
        "article_id": article_id,
        "article_type": article_type,
        "title": title,
        "canonical_url": canonical_url,
        "saved_response_count": saved_response_count,
        "latest_scraped_max_res_no": latest_scraped_max_res_no,
        "last_scraped_at": last_scraped_at,
        "created_at": created_at,
    }


def test_registered_page_renders_html_table_with_expected_columns():
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([_make_reg_row()]),
    ):
        response = _run_wsgi_request("GET", path="/registered")

    assert response["status"] == "200 OK"
    assert "<table" in response["body"]
    assert "テスト記事" in response["body"]
    assert "https://dic.nicovideo.jp/a/12345" in response["body"]
    assert ">42<" in response["body"]
    assert ">50<" in response["body"]
    assert "2026-01-01T00:00:00+00:00" in response["body"]
    assert "12345" in response["body"]


def test_registered_page_shows_article_id_column():
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([_make_reg_row(article_id="99887")]),
    ):
        response = _run_wsgi_request("GET", path="/registered")

    assert ">99887<" in response["body"]
    assert "Article ID" in response["body"]


def test_registered_page_canonical_url_is_clickable_link():
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([_make_reg_row()]),
    ):
        response = _run_wsgi_request("GET", path="/registered")

    assert 'href="https://dic.nicovideo.jp/a/12345"' in response["body"]
    assert 'target="_blank"' in response["body"]


def test_registered_page_highlights_not_scraped_rows():
    unscrapped = _make_reg_row(
        title="未スクレイプ",
        saved_response_count=0,
        latest_scraped_max_res_no=None,
        last_scraped_at=None,
    )
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([unscrapped]),
    ):
        response = _run_wsgi_request("GET", path="/registered")

    assert "not-scraped" in response["body"]


def test_registered_page_scraped_rows_have_no_special_class():
    scraped = _make_reg_row(saved_response_count=5)
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([scraped]),
    ):
        response = _run_wsgi_request("GET", path="/registered")

    assert 'class="not-scraped"' not in response["body"]


def test_registered_page_shows_sort_links_in_headers():
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([_make_reg_row()]),
    ):
        response = _run_wsgi_request("GET", path="/registered")

    assert "sort_by=title" in response["body"]
    assert "sort_by=created_at" in response["body"]
    assert "sort_by=article_id" in response["body"]


def test_registered_page_renders_empty_table_when_no_articles():
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([]),
    ):
        response = _run_wsgi_request("GET", path="/registered")

    assert response["status"] == "200 OK"
    assert "<table" in response["body"]
    assert "Count: 0" in response["body"]


def test_registered_page_lists_multiple_articles():
    articles = [
        _make_reg_row(
            article_id="1",
            article_type="a",
            title="記事A",
            canonical_url="https://dic.nicovideo.jp/a/1",
            saved_response_count=10,
            latest_scraped_max_res_no=10,
            last_scraped_at=None,
        ),
        _make_reg_row(
            article_id="2",
            article_type="id",
            title="記事B",
            canonical_url="https://dic.nicovideo.jp/id/2",
            saved_response_count=5,
            latest_scraped_max_res_no=None,
            last_scraped_at=None,
        ),
    ]
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result(articles),
    ):
        response = _run_wsgi_request("GET", path="/registered")

    assert response["status"] == "200 OK"
    assert "記事A" in response["body"]
    assert "記事B" in response["body"]
    assert "Count: 2" in response["body"]


def test_registered_page_csv_download_returns_csv_content_type():
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([_make_reg_row()]),
    ):
        response = _run_wsgi_request("GET", path="/registered/csv")

    assert response["status"] == "200 OK"
    headers = _header_map(response)
    assert "text/csv" in headers["Content-Type"]
    assert "article_id" in response["body"]
    assert "テスト記事" in response["body"]


def test_registered_page_csv_contains_all_column_headers():
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([_make_reg_row()]),
    ):
        response = _run_wsgi_request("GET", path="/registered/csv")

    first_line = response["body"].splitlines()[0]
    assert "article_id" in first_line
    assert "article_type" in first_line
    assert "title" in first_line
    assert "canonical_url" in first_line


def test_registered_csv_includes_csv_download_link_on_page():
    with patch(
        "web_app.query_registered_articles",
        return_value=_mock_query_result([_make_reg_row()]),
    ):
        response = _run_wsgi_request("GET", path="/registered")

    assert "/registered/csv" in response["body"]
