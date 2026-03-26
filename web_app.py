from __future__ import annotations

from html import escape
from io import BytesIO
from urllib.parse import parse_qs

from archive_read import has_saved_article
from article_resolver import resolve_article_input


def _html_page(message: str, input_value: str) -> bytes:
    safe_message = escape(message)
    safe_value = escape(input_value)

    html = f"""\
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Nicodic Archiver</title>
  </head>
  <body>
    <main>
      <h1>Nicodic Archiver</h1>
      <form method="post" action="/resolve">
        <label for="article_input">Article name or URL</label><br>
        <input
          id="article_input"
          name="article_input"
          type="text"
          value="{safe_value}"
          style="width: min(720px, 95vw);"
        >
        <button type="submit">Submit</button>
      </form>
      <hr>
      <div id="message">{safe_message}</div>
    </main>
  </body>
</html>
"""
    return html.encode("utf-8")


def _read_form_body(environ) -> dict[str, str]:
    try:
        length = int(environ.get("CONTENT_LENGTH") or "0")
    except ValueError:
        length = 0

    raw = environ.get("wsgi.input", BytesIO(b"")).read(length)
    parsed = parse_qs(raw.decode("utf-8"), keep_blank_values=True)
    out: dict[str, str] = {}
    for k, v in parsed.items():
        out[k] = v[-1] if v else ""
    return out


def app(environ, start_response):
    """
    Minimal WSGI app.

    - GET /: input form
    - POST /resolve: resolve input + saved check, then show message
    """

    method = environ.get("REQUEST_METHOD", "GET").upper()
    path = environ.get("PATH_INFO", "/")

    if method == "GET" and path == "/":
        body = _html_page("Ready.", "")
        start_response(
            "200 OK",
            [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Content-Length", str(len(body))),
            ],
        )
        return [body]

    if method == "POST" and path == "/resolve":
        form = _read_form_body(environ)
        raw_input = form.get("article_input", "")

        try:
            result = resolve_article_input(raw_input)
            if not result["ok"]:
                message = (
                    f"Resolution failed: {result['failure_kind']}. "
                    f"Input={result['normalized_input']}"
                )
                body = _html_page(message, result["normalized_input"])
                start_response(
                    "200 OK",
                    [
                        ("Content-Type", "text/html; charset=utf-8"),
                        ("Content-Length", str(len(body))),
                    ],
                )
                return [body]

            canonical = result["canonical_target"]
            saved = has_saved_article(
                canonical["article_id"],
                canonical["article_type"],
            )
            if saved:
                saved_note = "Saved article detected."
            else:
                saved_note = "No saved archive yet."

            message = (
                f"{saved_note} "
                f"title={result['title']} "
                f"url={canonical['article_url']}"
            )
            body = _html_page(message, result["normalized_input"])
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]
        except Exception as exc:
            body = _html_page(
                f"Internal error: {type(exc).__name__}",
                raw_input.strip(),
            )
            start_response(
                "500 Internal Server Error",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

    start_response(
        "404 Not Found",
        [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Content-Length", "9"),
        ],
    )
    return [b"Not Found"]


__all__ = ["app"]
