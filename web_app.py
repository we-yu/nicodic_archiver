import os
import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse
from uuid import uuid4
from wsgiref.simple_server import make_server

from archive_read import (
    get_saved_article_summary,
    get_saved_article_summary_by_exact_title,
    get_saved_article_export,
)
from article_resolver import resolve_article_input
from target_list import register_target_url


DEFAULT_TARGET_DB_PATH = os.environ.get("TARGET_DB_PATH", "data/nicodic.db")
DEFAULT_WEB_ACTION_LOG_PATH = os.environ.get(
    "WEB_ACTION_LOG_PATH",
    "data/web_action.log",
)
DEFAULT_DOWNLOAD_FORMAT = "txt"
SUPPORTED_DOWNLOAD_FORMATS = ("txt", "md", "csv")

UI_TEXTS = {
    "page_title": "NicoNicoPedia Archive Checker",
    "heading": "NicoNicoPedia Archive Checker",
    "lede": (
        "Enter an article name or article URL. The page will either "
        "download a saved archive or register the article for later "
        "collection."
    ),
    "input_label": "Article name or article URL",
    "input_placeholder": (
        "例: ニコニコ大百科 / "
        "https://dic.nicovideo.jp/a/ニコニコ大百科"
    ),
    "submit_label": "Submit",
    "result_heading": "Result",
    "empty_message": "Submit an article name or article URL to continue.",
    "busy_message": (
        "Checking the archive and preparing the next step. Saved "
        "articles will download as {format_name}."
    ),
    "saved_message": (
        "Saved article found. {format_name} download will start "
        "automatically."
    ),
    "saved_hint": (
        "If the download does not start, your browser may have blocked "
        "it."
    ),
    "registered_message": (
        "Article registered for archive checking. Please try again "
        "later."
    ),
    "error_messages": {
        "not_found": "Article was not found.",
        "invalid_input": (
            "Enter an article name or a valid Nicopedia article URL."
        ),
        "ambiguous": "Could not resolve the article.",
        "temporary_fetch_failure": (
            "Temporary fetch failure. Please try again later."
        ),
        "unexpected_internal_error": "An unexpected internal error occurred.",
    },
    "field_labels": {
        "title": "Article title",
        "article_id": "Article ID",
        "url": "URL",
        "response_count": "Saved response count",
        "reference_id": "Reference ID",
    },
    "format_labels": {
        "txt": "TXT",
        "md": "MD",
        "csv": "CSV",
    },
}


def _normalize_web_input(article_input: str) -> str:
    return article_input.strip()


def _looks_like_url_input(article_input: str) -> bool:
    parsed = urlparse(article_input)
    return bool(parsed.scheme or parsed.netloc)


def _display_format_name(requested_format: str) -> str:
    return UI_TEXTS["format_labels"].get(
        requested_format,
        requested_format.upper(),
    )


def _normalize_download_format(value: str | None) -> str:
    text = (value or "").strip().lower()
    if text in SUPPORTED_DOWNLOAD_FORMATS:
        return text
    return DEFAULT_DOWNLOAD_FORMAT


def _humanize_title(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return "unknown"

    if _looks_like_url_input(text):
        parsed = urlparse(text)
        path_parts = [part for part in parsed.path.split("/") if part]
        if path_parts:
            decoded = unquote(path_parts[-1]).strip()
            if decoded:
                return decoded

    decoded = unquote(text).strip()
    return decoded if decoded else "unknown"


def _sanitize_download_filename_title(value: str) -> str:
    text = _humanize_title(value)
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    return text or "article"


def _ascii_download_fallback(article_id: str, article_type: str) -> str:
    return f"{article_id}{article_type}_article"


def _build_download_filename(
    article_id: str,
    article_type: str,
    title: str | None,
    requested_format: str,
) -> str:
    safe_title = _sanitize_download_filename_title(title or "")
    return f"{article_id}{article_type}_{safe_title}.{requested_format}"


def _build_content_disposition(
    article_id: str,
    article_type: str,
    title: str | None,
    requested_format: str,
) -> str:
    utf8_filename = _build_download_filename(
        article_id,
        article_type,
        title,
        requested_format,
    )
    ascii_filename = (
        f"{_ascii_download_fallback(article_id, article_type)}.{requested_format}"
    )
    encoded = quote(utf8_filename, safe="")
    return (
        f'attachment; filename="{ascii_filename}"; '
        f"filename*=UTF-8''{encoded}"
    )


def _classify_runtime_failure(exc: RuntimeError) -> str | None:
    detail = str(exc)
    if "timeout=" in detail:
        return "temporary_fetch_failure"
    if "Failed to fetch" in detail:
        return "temporary_fetch_failure"
    return None


def check_article_status(article_input: str) -> dict:
    """Resolve input and return a bounded archive status result."""

    normalized_input = _normalize_web_input(article_input)

    try:
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
            return {
                "status": "resolution_failure",
                "input": resolution["normalized_input"],
                "failure_kind": resolution["failure_kind"],
                "message": "Could not resolve the input.",
            }

        canonical_target = resolution["canonical_target"]
        archive_summary = get_saved_article_summary(
            canonical_target["article_id"],
            canonical_target["article_type"],
        )
        archive_summary = _restore_saved_url_input_parity(
            resolution,
            archive_summary,
        )

        if archive_summary["found"]:
            return {
                "status": "saved",
                "input": resolution["normalized_input"],
                "title": archive_summary["title"] or resolution["title"],
                "matched_by": resolution["matched_by"],
                "article_url": archive_summary["url"]
                or canonical_target["article_url"],
                "article_id": archive_summary["article_id"],
                "article_type": archive_summary["article_type"],
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
    except RuntimeError as exc:
        failure_kind = _classify_runtime_failure(exc)
        if failure_kind is not None:
            return {
                "status": "resolution_failure",
                "input": normalized_input,
                "failure_kind": failure_kind,
                "error_detail": str(exc),
                "message": "Temporary failure while resolving the article.",
            }
        return {
            "status": "internal_error",
            "input": normalized_input,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc),
            "message": "Internal error while checking article status.",
        }
    except Exception as exc:
        return {
            "status": "internal_error",
            "input": normalized_input,
            "error_kind": type(exc).__name__,
            "error_detail": str(exc),
            "message": "Internal error while checking article status.",
        }


def _result_error_code(result: dict) -> str:
    if result["status"] == "resolution_failure":
        return result.get("failure_kind", "unknown_resolution_failure")
    if result["status"] == "internal_error":
        return "unexpected_internal_error"
    return "unknown_error"


def _user_error_message(result: dict) -> str:
    error_code = _result_error_code(result)
    return UI_TEXTS["error_messages"].get(
        error_code,
        "不明なエラーが発生しました。",
    )


def _read_post_form(environ: dict) -> dict:
    content_length = environ.get("CONTENT_LENGTH", "0").strip()
    try:
        body_size = int(content_length)
    except ValueError:
        body_size = 0

    body = environ["wsgi.input"].read(body_size).decode("utf-8")
    form_data = parse_qs(body, keep_blank_values=True)
    return {key: values[0] if values else "" for key, values in form_data.items()}


def _read_query_params(environ: dict) -> dict:
    query = environ.get("QUERY_STRING", "")
    parsed = parse_qs(query, keep_blank_values=True)
    return {key: values[0] if values else "" for key, values in parsed.items()}


def _web_log_value(value: object) -> str:
    if value is None:
        return "unknown"
    text = str(value).strip()
    return text if text else "unknown"


def _visitor_hint(environ: dict) -> str:
    remote_addr = environ.get("HTTP_X_FORWARDED_FOR") or environ.get(
        "REMOTE_ADDR",
        "unknown",
    )
    user_agent = " ".join(environ.get("HTTP_USER_AGENT", "unknown").split())
    if len(user_agent) > 60:
        user_agent = f"{user_agent[:57]}..."
    return f"addr={remote_addr} ua={user_agent}"


def _append_web_action_log_lines(log_path: Path, lines: list[str]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as stream:
        for line in lines:
            stream.write(f"{line}\n")


def _log_web_action(
    web_action_log_path: str,
    environ: dict,
    action_kind: str,
    input_value: str,
    requested_format: str | None,
    result_status: str,
    resolved_title: str | None = None,
    resolved_article_id: str | None = None,
    resolved_article_type: str | None = None,
    resolved_canonical_url: str | None = None,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> str:
    action_id = uuid4().hex
    timestamp = datetime.now(timezone.utc).isoformat()
    title = _humanize_title(
        resolved_title or input_value or resolved_canonical_url
    )
    try:
        _append_web_action_log_lines(
            Path(web_action_log_path),
            [
                "",
                "WEB_ACTION_START",
                f"  action_id={action_id}",
                f"  timestamp={timestamp}",
                f"  action_kind={action_kind}",
                f"  visitor_hint={_visitor_hint(environ)}",
                f"  input_value={_web_log_value(input_value)}",
                f"  resolved_title={_web_log_value(title)}",
                (
                    "  resolved_article_id="
                    f"{_web_log_value(resolved_article_id)}"
                ),
                (
                    "  resolved_article_type="
                    f"{_web_log_value(resolved_article_type)}"
                ),
                (
                    "  resolved_canonical_url="
                    f"{_web_log_value(resolved_canonical_url)}"
                ),
                f"  requested_format={_web_log_value(requested_format)}",
                f"  result_status={_web_log_value(result_status)}",
                f"  error_code={_web_log_value(error_code)}",
                f"  error_detail={_web_log_value(error_detail)}",
                "WEB_ACTION_END",
                "",
            ],
        )
    except OSError:
        pass
    return action_id


def _build_saved_ui_result(check_result: dict) -> dict:
    return {
        "status": "saved",
        "message": UI_TEXTS["saved_message"].format(
            format_name=_display_format_name(DEFAULT_DOWNLOAD_FORMAT)
        ),
        "title": check_result["title"],
        "article_id": check_result["article_id"],
        "article_url": check_result["article_url"],
        "article_type": check_result["article_type"],
        "response_count": check_result["response_count"],
        "input": check_result["input"],
    }


def _build_registered_ui_result(check_result: dict) -> dict:
    return {
        "status": "registered",
        "message": UI_TEXTS["registered_message"],
        "title": check_result["title"],
        "article_id": check_result["article_id"],
        "article_url": check_result["article_url"],
        "article_type": check_result["article_type"],
        "input": check_result["input"],
    }


def _build_error_ui_result(result: dict, reference_id: str | None) -> dict:
    error_result = {
        "status": "error",
        "message": _user_error_message(result),
        "error_code": _result_error_code(result),
    }
    if reference_id is not None:
        error_result["reference_id"] = reference_id
    return error_result


def _build_download_query(check_result: dict) -> str:
    return urlencode(
        {
            "article_id": check_result["article_id"],
            "article_type": check_result["article_type"],
            "resolved_title": check_result["title"],
            "requested_format": DEFAULT_DOWNLOAD_FORMAT,
        }
    )


def _download_input_value(query: dict) -> str:
    return (
        query.get("article_input")
        or query.get("resolved_title")
        or ""
    )


def _restore_saved_url_input_parity(
    resolution: dict,
    archive_summary: dict,
) -> dict:
    if archive_summary["found"]:
        return archive_summary
    if resolution.get("matched_by") != "article_url":
        return archive_summary

    title = resolution.get("title")
    if not title:
        return archive_summary

    saved_title_summary = get_saved_article_summary_by_exact_title(title)
    if not saved_title_summary["found"]:
        return archive_summary

    canonical_target = resolution["canonical_target"]
    if saved_title_summary["article_id"] != canonical_target["article_id"]:
        return archive_summary

    return saved_title_summary


def _submit_archive_check(
    article_input: str,
    target_db_path: str,
    web_action_log_path: str,
    environ: dict,
) -> tuple[dict, str | None]:
    check_result = check_article_status(article_input)

    if check_result["status"] == "saved":
        return _build_saved_ui_result(check_result), _build_download_query(
            check_result
        )

    if check_result["status"] == "unsaved":
        try:
            registration_status = register_target_url(
                check_result["article_url"],
                target_db_path,
            )
        except Exception as exc:
            failure_result = {
                "status": "internal_error",
                "input": check_result["input"],
                "error_kind": type(exc).__name__,
                "error_detail": str(exc),
                "message": "Internal error while checking article status.",
            }
            reference_id = _log_web_action(
                web_action_log_path,
                environ,
                action_kind="failed_action",
                input_value=article_input,
                requested_format=None,
                result_status="registration_failed",
                resolved_title=check_result["title"],
                resolved_article_id=check_result["article_id"],
                resolved_article_type=check_result["article_type"],
                resolved_canonical_url=check_result["article_url"],
                error_code="registration_failed",
                error_detail=str(exc),
            )
            return _build_error_ui_result(failure_result, reference_id), None

        if registration_status in {"added", "reactivated", "duplicate"}:
            _log_web_action(
                web_action_log_path,
                environ,
                action_kind="registration",
                input_value=article_input,
                requested_format=None,
                result_status=registration_status,
                resolved_title=check_result["title"],
                resolved_article_id=check_result["article_id"],
                resolved_article_type=check_result["article_type"],
                resolved_canonical_url=check_result["article_url"],
            )
            return _build_registered_ui_result(check_result), None

        failure_result = {
            "status": "internal_error",
            "input": check_result["input"],
            "error_kind": "RegistrationError",
            "error_detail": "Canonical article URL could not be added.",
            "message": "Internal error while checking article status.",
        }
        reference_id = _log_web_action(
            web_action_log_path,
            environ,
            action_kind="failed_action",
            input_value=article_input,
            requested_format=None,
            result_status="registration_failed",
            resolved_title=check_result["title"],
            resolved_article_id=check_result["article_id"],
            resolved_article_type=check_result["article_type"],
            resolved_canonical_url=check_result["article_url"],
            error_code="registration_failed",
            error_detail=failure_result["error_detail"],
        )
        return _build_error_ui_result(failure_result, reference_id), None

    reference_id = _log_web_action(
        web_action_log_path,
        environ,
        action_kind="failed_action",
        input_value=article_input,
        requested_format=None,
        result_status=check_result["status"],
        resolved_title=check_result.get("title") or check_result.get("input"),
        resolved_article_id=check_result.get("article_id"),
        resolved_article_type=check_result.get("article_type"),
        resolved_canonical_url=check_result.get("article_url"),
        error_code=_result_error_code(check_result),
        error_detail=check_result.get("error_detail") or check_result["message"],
    )
    return _build_error_ui_result(check_result, reference_id), None


def _render_result_detail(label: str, value: str) -> str:
    return f"<p><strong>{escape(label)}:</strong> {escape(value)}</p>"


def _render_message_area(
    result: dict | None,
    download_query: str | None = None,
) -> str:
    if result is None:
        return (
            '<section class="message-area empty">'
            f"<h2>{escape(UI_TEXTS['result_heading'])}</h2>"
            f"<p>{escape(UI_TEXTS['empty_message'])}</p>"
            "</section>"
        )

    lines = [
        f'<section class="message-area {escape(result["status"])}">',
        f"<h2>{escape(UI_TEXTS['result_heading'])}</h2>",
        f'<p class="status-line">{escape(result["message"])}</p>',
    ]

    if result["status"] == "saved":
        lines.extend(
            [
                _render_result_detail(
                    UI_TEXTS["field_labels"]["title"],
                    result["title"],
                ),
                _render_result_detail(
                    UI_TEXTS["field_labels"]["article_id"],
                    result["article_id"],
                ),
                _render_result_detail(
                    UI_TEXTS["field_labels"]["url"],
                    result["article_url"],
                ),
                _render_result_detail(
                    UI_TEXTS["field_labels"]["response_count"],
                    str(result["response_count"]),
                ),
                f'<p class="followup-note">{escape(UI_TEXTS["saved_hint"])}</p>',
            ]
        )
        if download_query is not None:
            lines.append(
                '<iframe name="download_frame" class="download-frame"></iframe>'
            )
            lines.append('<div class="download-controls">')
            lines.append(
                (
                    '<form method="get" action="/download" '
                    'target="download_frame" class="download-form">'
                )
            )
            lines.append(
                (
                    '<input type="hidden" name="article_id" '
                    f'value="{escape(result["article_id"])}">'
                )
            )
            lines.append(
                (
                    '<input type="hidden" name="article_type" '
                    f'value="{escape(result["article_type"])}">'
                )
            )
            lines.append(
                (
                    '<input type="hidden" name="resolved_title" '
                    f'value="{escape(result["title"])}">'
                )
            )
            lines.append(
                '<label class="format-label" for="download_format">Format</label>'
            )
            lines.append('<select id="download_format" name="requested_format">')
            for fmt in SUPPORTED_DOWNLOAD_FORMATS:
                label = _display_format_name(fmt)
                selected = " selected" if fmt == DEFAULT_DOWNLOAD_FORMAT else ""
                lines.append(
                    (
                        f'<option value="{escape(fmt)}"{selected}>'
                        f"{escape(label)}</option>"
                    )
                )
            lines.append("</select>")
            lines.append('<button type="submit">Download</button>')
            lines.append("</form>")
            lines.append("</div>")
            lines.append(
                (
                    '<form method="get" action="/download" '
                    'target="download_frame" data-auto-download-form hidden>'
                )
            )
            for key, value in parse_qs(download_query).items():
                if not value:
                    continue
                lines.append(
                    (
                        '<input type="hidden" '
                        f'name="{escape(key)}" '
                        f'value="{escape(value[0])}">'
                    )
                )
            lines.append("</form>")

    if result["status"] == "registered":
        lines.extend(
            [
                _render_result_detail(
                    UI_TEXTS["field_labels"]["title"],
                    result["title"],
                ),
                _render_result_detail(
                    UI_TEXTS["field_labels"]["article_id"],
                    result["article_id"],
                ),
                _render_result_detail(
                    UI_TEXTS["field_labels"]["url"],
                    result["article_url"],
                ),
            ]
        )

    if result["status"] == "error" and "reference_id" in result:
        lines.append(
            _render_result_detail(
                UI_TEXTS["field_labels"]["reference_id"],
                result["reference_id"],
            )
        )

    lines.append("</section>")
    return "".join(lines)


def _render_page(
    article_input: str,
    result: dict | None = None,
    download_query: str | None = None,
) -> bytes:
    safe_input = escape(article_input)
    format_name = _display_format_name(DEFAULT_DOWNLOAD_FORMAT)
    message_area = _render_message_area(result, download_query)
    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{escape(UI_TEXTS['page_title'])}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4efe5;
      --panel: #fffaf2;
      --ink: #1f2430;
      --accent: #0f766e;
      --accent-disabled: #6b8f8b;
      --border: #d9ccb4;
      --muted: #6b7280;
      --saved: #14532d;
      --registered: #92400e;
      --error: #991b1b;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, \"Times New Roman\", serif;
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
    button[disabled] {{
      background: var(--accent-disabled);
      cursor: progress;
    }}
    .busy-message {{
      margin: 0;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .message-area {{
      margin-top: 24px;
      padding: 18px;
      border-radius: 14px;
      border: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.72);
    }}
        .message-area p {{
            overflow-wrap: anywhere;
            word-break: break-word;
        }}
    .message-area h2 {{ margin-top: 0; font-size: 1.2rem; }}
    .message-area.empty {{ color: var(--muted); }}
    .message-area.saved .status-line {{ color: var(--saved); }}
    .message-area.registered .status-line {{ color: var(--registered); }}
    .message-area.error .status-line {{ color: var(--error); }}
    .followup-note {{ color: var(--muted); }}
    .download-frame {{ display: none; width: 0; height: 0; border: 0; }}
    .download-controls {{ margin-top: 14px; }}
    .download-form {{ display: flex; gap: 10px; align-items: center; }}
    .download-form select {{
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--border);
      background: #fff;
      font: inherit;
    }}
    .format-label {{ font-weight: 700; }}
    @media (max-width: 640px) {{
      main {{ padding: 24px 14px 36px; }}
      .panel {{ padding: 18px; }}
      button {{ width: 100%; }}
      .download-form {{ flex-direction: column; align-items: stretch; }}
    }}
  </style>
</head>
<body>
  <main>
    <section class=\"panel\">
      <h1>{escape(UI_TEXTS['heading'])}</h1>
      <p class=\"lede\">{escape(UI_TEXTS['lede'])}</p>
      <form method=\"post\" action=\"/\" data-archive-check-form>
        <label for=\"article_input\">{escape(UI_TEXTS['input_label'])}</label>
        <input
          id=\"article_input\"
          name=\"article_input\"
          type=\"text\"
          value=\"{safe_input}\"
          placeholder=\"{escape(UI_TEXTS['input_placeholder'])}\"
          autocomplete=\"off\"
        >
        <button type=\"submit\" data-submit-button>
          {escape(UI_TEXTS['submit_label'])}
        </button>
        <p class=\"busy-message\" data-busy-message hidden aria-live=\"polite\">
          {escape(UI_TEXTS['busy_message'].format(format_name=format_name))}
        </p>
      </form>
      {message_area}
    </section>
  </main>
  <script>
    const form = document.querySelector("[data-archive-check-form]");
    if (form) {{
      form.addEventListener("submit", (event) => {{
        if (form.dataset.submitting === "true") {{
          event.preventDefault();
          return;
        }}
        form.dataset.submitting = "true";
        const input = form.querySelector("[name='article_input']");
        const button = form.querySelector("[data-submit-button]");
        const busy = form.querySelector("[data-busy-message]");
        if (input) {{
          input.setAttribute("readonly", "readonly");
          input.setAttribute("aria-disabled", "true");
        }}
        if (button) {{
          button.disabled = true;
        }}
        if (busy) {{
          busy.hidden = false;
        }}
      }});
    }}
    const autoDownloadForm = document.querySelector(
      "[data-auto-download-form]"
    );
    if (autoDownloadForm) {{
      autoDownloadForm.submit();
    }}
  </script>
</body>
</html>
"""
    return html.encode("utf-8")


def create_app(
    target_db_path: str = DEFAULT_TARGET_DB_PATH,
    web_action_log_path: str = DEFAULT_WEB_ACTION_LOG_PATH,
):
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

        if method == "POST" and path == "/":
            form = _read_post_form(environ)
            article_input = form.get("article_input", "")
            result, download_query = _submit_archive_check(
                article_input,
                target_db_path,
                web_action_log_path,
                environ,
            )
            body = _render_page(article_input, result, download_query)
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        if method == "GET" and path == "/download":
            query = _read_query_params(environ)
            article_id = query.get("article_id", "")
            article_type = query.get("article_type", "")
            article_input = _download_input_value(query)
            requested_format = _normalize_download_format(
                query.get("requested_format")
            )

            if not article_id or not article_type:
                body = b"Bad Request"
                start_response(
                    "400 Bad Request",
                    [
                        ("Content-Type", "text/plain; charset=utf-8"),
                        ("Content-Length", str(len(body))),
                    ],
                )
                return [body]

            export_result = get_saved_article_export(
                article_id,
                article_type,
                requested_format,
            )
            if not export_result["found"]:
                reference_id = _log_web_action(
                    web_action_log_path,
                    environ,
                    action_kind="failed_action",
                    input_value=article_input,
                    requested_format=requested_format,
                    result_status="download_missing",
                    resolved_title=query.get("resolved_title"),
                    resolved_article_id=article_id,
                    resolved_article_type=article_type,
                    error_code="download_missing",
                    error_detail="Saved article was not found for download.",
                )
                body = _render_page(
                    article_input,
                    {
                        "status": "error",
                        "message": "An unexpected internal error occurred.",
                        "error_code": "download_missing",
                        "reference_id": reference_id,
                    },
                )
                start_response(
                    "200 OK",
                    [
                        ("Content-Type", "text/html; charset=utf-8"),
                        ("Content-Length", str(len(body))),
                    ],
                )
                return [body]

            _log_web_action(
                web_action_log_path,
                environ,
                action_kind="download",
                input_value=article_input,
                requested_format=requested_format,
                result_status="success",
                resolved_title=query.get("resolved_title"),
                resolved_article_id=article_id,
                resolved_article_type=article_type,
            )
            body = export_result["content"].encode("utf-8")
            content_type = "text/plain; charset=utf-8"
            if requested_format == "md":
                content_type = "text/markdown; charset=utf-8"
            if requested_format == "csv":
                content_type = "text/csv; charset=utf-8"
            start_response(
                "200 OK",
                [
                    ("Content-Type", content_type),
                    (
                        "Content-Disposition",
                        _build_content_disposition(
                            article_id,
                            article_type,
                            export_result.get("title")
                            or query.get("resolved_title"),
                            requested_format,
                        ),
                    ),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        if path not in {"/", "/download"}:
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


def run_web(host: str = "127.0.0.1", port: int = 8080) -> None:
    with make_server(host, port, application) as httpd:
        print(f"Serving web UI on http://{host}:{port}")
        httpd.serve_forever()


def serve_web_app(
    host: str = "127.0.0.1",
    port: int = 8000,
    target_db_path: str = DEFAULT_TARGET_DB_PATH,
) -> None:
    app = create_app(target_db_path=target_db_path)
    with make_server(host, port, app) as server:
        print(f"Serving web app at http://{host}:{port}")
        server.serve_forever()
