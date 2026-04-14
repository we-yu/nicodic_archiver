import os
from html import escape
from urllib.parse import quote
from urllib.parse import parse_qs
from urllib.parse import urlencode
from urllib.parse import urlparse
from wsgiref.simple_server import make_server

from archive_read import (
    get_saved_article_txt,
    get_saved_article_summary,
    get_saved_article_summary_by_exact_title,
)
from article_resolver import resolve_article_input
from target_list import register_target_url
from web_action_log import append_web_action_log
from web_ui_text import DOWNLOAD_FORMATS, UI_TEXT


DEFAULT_TARGET_DB_PATH = os.environ.get("TARGET_DB_PATH", "data/nicodic.db")


def _normalize_web_input(article_input: str) -> str:
    return article_input.strip()


def _looks_like_url_input(article_input: str) -> bool:
    parsed = urlparse(article_input)
    return bool(parsed.scheme or parsed.netloc)


def check_article_status(article_input: str) -> dict:
    """Resolve input and return a bounded archive status result for the web UI."""

    try:
        normalized_input = _normalize_web_input(article_input)
        if normalized_input and not _looks_like_url_input(normalized_input):
            archive_summary = get_saved_article_summary_by_exact_title(
                normalized_input
            )
            if archive_summary["found"]:
                return {
                    "status": "saved",
                    "input": normalized_input,
                    "title": archive_summary["title"],
                    "matched_by": "local_title_lookup",
                    "article_url": archive_summary["url"],
                    "article_id": archive_summary["article_id"],
                    "article_type": archive_summary["article_type"],
                    "response_count": archive_summary["response_count"],
                    "message": "Saved archive found for the resolved article.",
                }

        resolution = resolve_article_input(article_input)
        if not resolution["ok"]:
            failure_kind = resolution["failure_kind"]
            return {
                "status": "resolution_failure",
                "input": resolution["normalized_input"],
                "failure_kind": failure_kind,
                "message": f"Could not resolve the input ({failure_kind}).",
            }

        canonical_target = resolution["canonical_target"]
        archive_summary = get_saved_article_summary(
            canonical_target["article_id"],
            canonical_target["article_type"],
        )

        if archive_summary["found"]:
            return {
                "status": "saved",
                "input": resolution["normalized_input"],
                "title": archive_summary["title"] or resolution["title"],
                "matched_by": resolution["matched_by"],
                "article_url": canonical_target["article_url"],
                "article_id": canonical_target["article_id"],
                "article_type": canonical_target["article_type"],
                "response_count": archive_summary["response_count"],
                "message": "Saved archive found for the resolved article.",
            }

        return {
            "status": "unsaved",
            "input": resolution["normalized_input"],
            "title": resolution["title"],
            "matched_by": resolution["matched_by"],
            "article_url": canonical_target["article_url"],
            "article_id": canonical_target["article_id"],
            "article_type": canonical_target["article_type"],
            "message": "Resolved article, but no saved archive was found yet.",
        }
    except Exception as exc:
        return {
            "status": "internal_error",
            "input": _normalize_web_input(article_input),
            "error_kind": type(exc).__name__,
            "message": "Internal error while checking article status.",
        }


def _read_post_form(environ: dict) -> dict:
    content_length = environ.get("CONTENT_LENGTH", "0").strip()
    try:
        body_size = int(content_length)
    except ValueError:
        body_size = 0

    body = environ["wsgi.input"].read(body_size).decode("utf-8")
    form_data = parse_qs(body, keep_blank_values=True)
    return {key: values[0] if values else "" for key, values in form_data.items()}


def _make_visitor_hint(environ: dict) -> str:
    remote_addr = environ.get("REMOTE_ADDR", "") or ""
    user_agent = environ.get("HTTP_USER_AGENT", "") or ""
    remote_addr = remote_addr.strip()
    user_agent = user_agent.strip()
    return f"ra={remote_addr} ua={user_agent}"


def _make_action_id(environ: dict) -> str:
    request_id = environ.get("HTTP_X_REQUEST_ID", "") or ""
    request_id = request_id.strip()
    if request_id:
        return request_id
    method = (environ.get("REQUEST_METHOD", "") or "").strip()
    nonce = os.urandom(6).hex()
    return f"wa_{os.getpid()}_{method}_{nonce}"


def _error_code_for_check_result(result: dict) -> str:
    status = result.get("status")
    if status == "resolution_failure":
        kind = result.get("failure_kind") or "unknown"
        if kind in {
            "not_found",
            "invalid_input",
            "ambiguous",
            "could_not_resolve",
            "upstream_failure",
            "timeout",
        }:
            return kind
        return "resolution_failure"
    if status == "internal_error":
        return "internal_error"
    return "unknown_error"


def _user_error_message(result: dict) -> str:
    status = result.get("status")
    if status == "resolution_failure":
        kind = result.get("failure_kind") or "unknown"
        if kind in {"not_found", "article_not_found", "404"}:
            return "記事が見つかりませんでした。入力を確認してください。"
        if kind in {"invalid_input"}:
            return "入力が不正です。記事名またはURLを確認してください。"
        if kind in {"ambiguous"}:
            return "候補が複数あり解決できませんでした。URLで指定してください。"
        if kind in {"could_not_resolve"}:
            return "記事を解決できませんでした。URLで指定してください。"
        if kind in {"timeout", "upstream_failure"}:
            return "一時的に取得に失敗しました。時間をおいて再試行してください。"
        return "入力を解決できませんでした。記事名またはURLを確認してください。"
    if status == "internal_error":
        return "内部エラーが発生しました。時間をおいて再試行してください。"
    return "不明なエラーが発生しました。"


def _render_message_area(
    result: dict | None,
) -> str:
    if result is None:
        return (
            '<section class="message-area empty">'
            f"<h2>{escape(UI_TEXT['result_heading'])}</h2>"
            f"<p>{escape(UI_TEXT['result_empty'])}</p>"
            "</section>"
        )

    status = escape(result["status"])
    message = escape(result["message"])
    lines = [
        f'<section class="message-area {status}">',
        f"<h2>{escape(UI_TEXT['result_heading'])}</h2>",
        f'<p class="status-line">{message}</p>',
    ]

    if result["status"] in {"saved", "registered"}:
        lines.extend(
            [
                f"<p>Title: <strong>{escape(result['title'])}</strong></p>",
                f"<p>Article ID: <strong>{escape(result['article_id'])}</strong></p>",
                f"<p>URL: {escape(result['article_url'])}</p>",
            ]
        )

    if result["status"] == "saved":
        lines.append(
            "<p>Saved responses: "
            f"<strong>{escape(str(result['response_count']))}</strong></p>"
        )
        fmt = escape(result.get("requested_format_display_name", ""))
        lines.append(f"<p>{fmt} をダウンロードしました。</p>")

    if result["status"] == "registered":
        lines.append("<p>取得対象として登録しました。後で再実行してください。</p>")

    lines.append("</section>")
    return "".join(lines)


def _render_page(
    article_input: str,
    result: dict | None = None,
    *,
    is_working: bool = False,
    auto_download_url: str | None = None,
) -> bytes:
    safe_input = escape(article_input)
    message_area = _render_message_area(result)
    headline = escape(UI_TEXT["headline"])
    page_title = escape(UI_TEXT["page_title"])
    lede = escape(UI_TEXT["lede"])
    label = escape(UI_TEXT["form_label"])
    placeholder = escape(UI_TEXT["form_placeholder"])
    submit = escape(UI_TEXT["submit"])
    working_text = escape(UI_TEXT["working"])
    working_class = " working" if is_working else ""
    disabled = " disabled" if is_working else ""
    download_script = ""
    if auto_download_url:
        safe_url = escape(auto_download_url, quote=True)
        download_script = (
            "<script>"
            "window.addEventListener('load', () => {"
            f"window.location.assign('{safe_url}');"
            "});"
            "</script>"
        )
    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{page_title}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4efe5;
      --panel: #fffaf2;
      --ink: #1f2430;
      --accent: #0f766e;
      --border: #d9ccb4;
      --muted: #6b7280;
      --saved: #14532d;
      --unsaved: #92400e;
      --failure: #9f1239;
      --error: #991b1b;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top, rgba(15, 118, 110, 0.12), transparent 35%),
        linear-gradient(180deg, #efe6d7 0%, var(--bg) 100%);
    }}
    main {{
      max-width: 760px;
      margin: 0 auto;
      padding: 48px 20px 64px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 24px;
      box-shadow: 0 20px 40px rgba(31, 36, 48, 0.08);
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: clamp(2rem, 4vw, 3rem);
      line-height: 1.05;
    }}
    p {{ line-height: 1.5; }}
    .lede {{ color: var(--muted); margin: 0 0 24px; }}
    form {{ display: grid; gap: 12px; }}
    label {{ font-weight: 700; }}
    input[type=\"text\"] {{
      width: 100%;
      padding: 14px 16px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: #fff;
      font: inherit;
    }}
    .working-line {{
      color: var(--muted);
      display: none;
      margin: 0;
    }}
    .panel.working .working-line {{
      display: block;
    }}
    .panel.working button {{
      opacity: 0.72;
      cursor: wait;
    }}
    button {{
      width: fit-content;
      padding: 12px 18px;
      border: 0;
      border-radius: 999px;
      background: var(--accent);
      color: #fff;
      font: inherit;
      cursor: pointer;
    }}
    .message-area {{
      margin-top: 24px;
      padding: 18px;
      border-radius: 14px;
      border: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.72);
    }}
    .message-area h2 {{ margin-top: 0; font-size: 1.2rem; }}
    .message-area.empty {{ color: var(--muted); }}
    .message-area.saved .status-line {{ color: var(--saved); }}
    .message-area.unsaved .status-line {{ color: var(--unsaved); }}
    .message-area.resolution_failure .status-line {{ color: var(--failure); }}
    .message-area.internal_error .status-line {{ color: var(--error); }}
    @media (max-width: 640px) {{
      main {{ padding: 24px 14px 36px; }}
      .panel {{ padding: 18px; }}
      button {{ width: 100%; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class=\"panel{working_class}\">
      <h1>{headline}</h1>
      <p class=\"lede\">{lede}</p>
      <p class=\"working-line\">{working_text}</p>
      <form method=\"post\" action=\"/\">
        <label for=\"article_input\">{label}</label>
        <input
          id=\"article_input\"
          name=\"article_input\"
          type=\"text\"
          value=\"{safe_input}\"
          placeholder=\"{placeholder}\"
          {disabled}
        >
        <button type=\"submit\"{disabled}>{submit}</button>
      </form>
      {message_area}
    </section>
  </main>
  <script>
    const form = document.querySelector('form[action="/"]');
    if (form) {{
      form.addEventListener("submit", () => {{
        const panel = document.querySelector(".panel");
        if (panel) panel.classList.add("working");
        const input = document.getElementById("article_input");
        const btn = form.querySelector('button[type="submit"]');
        if (input) input.disabled = true;
        if (btn) btn.disabled = true;
      }});
    }}
  </script>
  {download_script}
</body>
</html>
"""
    return html.encode("utf-8")


def _build_download_filename(article_id: str, article_type: str, ext: str) -> str:
    base = f"{article_id}{article_type}"
    safe = quote(base, safe="")
    return f"{safe}.{ext}"


def _log_web_action(
    environ: dict,
    *,
    action_kind: str,
    input_value: str,
    requested_format: str,
    result_status: str,
    resolved_title: str | None = None,
    resolved_article_id: str | None = None,
    resolved_article_type: str | None = None,
    resolved_canonical_url: str | None = None,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> None:
    append_web_action_log(
        {
            "action_id": _make_action_id(environ),
            "action_kind": action_kind,
            "visitor_hint": _make_visitor_hint(environ),
            "input_value": input_value,
            "resolved_title": resolved_title or "",
            "resolved_article_id": resolved_article_id or "",
            "resolved_article_type": resolved_article_type or "",
            "resolved_canonical_url": resolved_canonical_url or "",
            "requested_format": requested_format,
            "result_status": result_status,
            "error_code": error_code,
            "error_detail": error_detail,
        }
    )


def create_app(target_db_path: str = DEFAULT_TARGET_DB_PATH):
    def app(environ, start_response):
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "/")

        if method == "GET" and path == "/":
            body = _render_page("")
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        if method == "GET" and path == "/download":
            query = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
            article_id = (query.get("article_id") or [""])[0]
            article_type = (query.get("article_type") or [""])[0]
            fmt_key = (query.get("format") or ["txt"])[0]
            action_id = (query.get("action_id") or [""])[0]
            input_value = (query.get("input") or [""])[0]
            resolved_title = (query.get("title") or [""])[0]
            resolved_url = (query.get("url") or [""])[0]

            fmt = DOWNLOAD_FORMATS.get(fmt_key)
            if not fmt or not article_id or not article_type:
                _log_web_action(
                    environ,
                    action_kind="download",
                    input_value=input_value,
                    requested_format=fmt_key,
                    result_status="failed",
                    error_code="invalid_download_request",
                    error_detail=environ.get("QUERY_STRING", ""),
                )
                error_result = {
                    "status": "error",
                    "message": "ダウンロード要求が不正です。",
                }
                body = _render_page("", error_result)
                start_response(
                    "200 OK",
                    [
                        ("Content-Type", "text/html; charset=utf-8"),
                        ("Content-Length", str(len(body))),
                    ],
                )
                return [body]

            txt_result = get_saved_article_txt(article_id, article_type)
            if not txt_result["found"]:
                _log_web_action(
                    {**environ, "HTTP_X_REQUEST_ID": action_id},
                    action_kind="download",
                    input_value=input_value,
                    requested_format=fmt_key,
                    result_status="failed",
                    resolved_title=resolved_title,
                    resolved_article_id=article_id,
                    resolved_article_type=article_type,
                    resolved_canonical_url=resolved_url,
                    error_code="saved_not_found",
                    error_detail="saved article missing for download",
                )
                error_result = {
                    "status": "error",
                    "message": "保存済み記事が見つかりませんでした。",
                }
                body = _render_page("", error_result)
                start_response(
                    "200 OK",
                    [
                        ("Content-Type", "text/html; charset=utf-8"),
                        ("Content-Length", str(len(body))),
                    ],
                )
                return [body]

            _log_web_action(
                {**environ, "HTTP_X_REQUEST_ID": action_id},
                action_kind="download",
                input_value=input_value,
                requested_format=fmt_key,
                result_status="ok",
                resolved_title=resolved_title,
                resolved_article_id=article_id,
                resolved_article_type=article_type,
                resolved_canonical_url=resolved_url,
            )
            filename = _build_download_filename(
                article_id,
                article_type,
                fmt["file_ext"],
            )
            body = txt_result["content"].encode("utf-8")
            start_response(
                "200 OK",
                [
                    ("Content-Type", fmt["content_type"]),
                    (
                        "Content-Disposition",
                        f'attachment; filename="{filename}"',
                    ),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        if method == "POST" and path == "/":
            form = _read_post_form(environ)
            article_input = form.get("article_input", "")
            fmt_key = "txt"
            fmt = DOWNLOAD_FORMATS[fmt_key]
            check_result = check_article_status(article_input)

            if check_result["status"] == "saved":
                action_id = _make_action_id(environ)
                download_url = "/download?" + urlencode(
                    {
                        "article_id": check_result["article_id"],
                        "article_type": check_result["article_type"],
                        "format": fmt_key,
                        "action_id": action_id,
                        "input": article_input,
                        "title": check_result["title"],
                        "url": check_result["article_url"],
                    }
                )
                saved = {
                    "status": "saved",
                    "message": "保存済みの記事が見つかりました。",
                    "input": check_result["input"],
                    "title": check_result["title"],
                    "article_url": check_result["article_url"],
                    "article_id": check_result["article_id"],
                    "article_type": check_result["article_type"],
                    "response_count": check_result["response_count"],
                    "requested_format_display_name": fmt["display_name"],
                }
                body = _render_page(
                    article_input,
                    saved,
                    auto_download_url=download_url,
                )
                start_response(
                    "200 OK",
                    [
                        ("Content-Type", "text/html; charset=utf-8"),
                        ("Content-Length", str(len(body))),
                    ],
                )
                return [body]

            if check_result["status"] == "unsaved":
                add_result = register_target_url(
                    check_result["article_url"],
                    target_db_path,
                )
                result_status = "ok"
                if add_result not in {"added", "reactivated", "duplicate"}:
                    result_status = "failed"

                _log_web_action(
                    environ,
                    action_kind="registration",
                    input_value=article_input,
                    requested_format=fmt_key,
                    result_status=result_status,
                    resolved_title=check_result.get("title"),
                    resolved_article_id=check_result.get("article_id"),
                    resolved_article_type=check_result.get("article_type"),
                    resolved_canonical_url=check_result.get("article_url"),
                    error_code=None if result_status == "ok" else "registration_failed",
                    error_detail=None if result_status == "ok" else str(add_result),
                )

                if result_status != "ok":
                    error_result = {
                        "status": "error",
                        "message": "取得対象の登録に失敗しました。",
                    }
                    body = _render_page(article_input, error_result)
                    start_response(
                        "200 OK",
                        [
                            ("Content-Type", "text/html; charset=utf-8"),
                            ("Content-Length", str(len(body))),
                        ],
                    )
                    return [body]

                registered = {
                    "status": "registered",
                    "message": "取得対象として登録しました。",
                    "input": check_result["input"],
                    "title": check_result["title"],
                    "article_url": check_result["article_url"],
                    "article_id": check_result["article_id"],
                    "article_type": check_result["article_type"],
                }
                body = _render_page(article_input, registered)
                start_response(
                    "200 OK",
                    [
                        ("Content-Type", "text/html; charset=utf-8"),
                        ("Content-Length", str(len(body))),
                    ],
                )
                return [body]

            error_message = _user_error_message(check_result)
            error_code = _error_code_for_check_result(check_result)
            _log_web_action(
                environ,
                action_kind="failed",
                input_value=article_input,
                requested_format=fmt_key,
                result_status="failed",
                resolved_title=check_result.get("title"),
                resolved_article_id=check_result.get("article_id"),
                resolved_article_type=check_result.get("article_type"),
                resolved_canonical_url=check_result.get("article_url"),
                error_code=error_code,
                error_detail=check_result.get("message", ""),
            )
            error_result = {"status": "error", "message": error_message}
            body = _render_page(article_input, error_result)
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        if path != "/":
            body = b"Not Found"
            start_response(
                "404 Not Found",
                [
                    ("Content-Type", "text/plain; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        body = b"Method Not Allowed"
        start_response(
            "405 Method Not Allowed",
            [
                ("Content-Type", "text/plain; charset=utf-8"),
                ("Content-Length", str(len(body))),
            ],
        )
        return [body]

    return app


application = create_app()


def serve_web_app(
    host: str = "127.0.0.1",
    port: int = 8000,
    target_db_path: str = DEFAULT_TARGET_DB_PATH,
) -> None:
    app = create_app(target_db_path=target_db_path)
    with make_server(host, port, app) as server:
        print(f"Serving web app at http://{host}:{port}")
        server.serve_forever()
