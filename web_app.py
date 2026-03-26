from html import escape
from urllib.parse import parse_qs
from urllib.parse import urlparse
from wsgiref.simple_server import make_server

from archive_read import (
    get_saved_article_summary,
    get_saved_article_summary_by_exact_title,
)
from article_resolver import resolve_article_input


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


def _read_post_input(environ: dict) -> str:
    content_length = environ.get("CONTENT_LENGTH", "0").strip()
    try:
        body_size = int(content_length)
    except ValueError:
        body_size = 0

    body = environ["wsgi.input"].read(body_size).decode("utf-8")
    form_data = parse_qs(body, keep_blank_values=True)
    return form_data.get("article_input", [""])[0]


def _render_message_area(result: dict | None) -> str:
    if result is None:
        return (
            '<section class="message-area empty">'
            "<h2>Result</h2>"
            "<p>"
            "Submit an article name or article URL to check whether it "
            "is already saved."
            "</p>"
            "</section>"
        )

    status = escape(result["status"])
    message = escape(result["message"])
    lines = [
        f'<section class="message-area {status}">',
        "<h2>Result</h2>",
        f'<p class="status-line">{message}</p>',
    ]

    if result["status"] == "resolution_failure":
        lines.append(
            "<p>Resolution status: "
            f"<strong>{escape(result['failure_kind'])}</strong></p>"
        )

    if result["status"] == "internal_error":
        lines.append(
            "<p>Error type: "
            f"<strong>{escape(result['error_kind'])}</strong></p>"
        )

    if result["status"] in {"saved", "unsaved"}:
        lines.extend(
            [
                f"<p>Title: <strong>{escape(result['title'])}</strong></p>",
                (
                    "<p>Canonical target: "
                    f"{escape(result['article_type'])}/"
                    f"{escape(result['article_id'])}</p>"
                ),
                f"<p>Matched by: {escape(result['matched_by'])}</p>",
                f"<p>URL: {escape(result['article_url'])}</p>",
            ]
        )

    if result["status"] == "saved":
        lines.append(
            "<p>Saved response count: "
            f"{escape(str(result['response_count']))}</p>"
        )

    lines.append("</section>")
    return "".join(lines)


def _render_page(article_input: str, result: dict | None = None) -> bytes:
    safe_input = escape(article_input)
    message_area = _render_message_area(result)
    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>Nicodic Archive Checker</title>
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
    <section class=\"panel\">
      <h1>Archive Checker</h1>
      <p class=\"lede\">
        Enter an article name or article URL to see whether a saved
        archive already exists.
      </p>
      <form method=\"post\" action=\"/\">
        <label for=\"article_input\">Article name or article URL</label>
        <input
          id=\"article_input\"
          name=\"article_input\"
          type=\"text\"
          value=\"{safe_input}\"
          placeholder=\"https://dic.nicovideo.jp/a/... or exact title\"
        >
        <button type=\"submit\">Check archive status</button>
      </form>
      {message_area}
    </section>
  </main>
</body>
</html>
"""
    return html.encode("utf-8")


def create_app():
    def app(environ, start_response):
        method = environ.get("REQUEST_METHOD", "GET").upper()
        path = environ.get("PATH_INFO", "/")

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

        if method == "GET":
            body = _render_page("")
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        if method == "POST":
            article_input = _read_post_input(environ)
            body = _render_page(article_input, check_article_status(article_input))
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
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


def serve_web_app(host: str = "127.0.0.1", port: int = 8000) -> None:
    with make_server(host, port, application) as server:
        print(f"Serving web app at http://{host}:{port}")
        server.serve_forever()
