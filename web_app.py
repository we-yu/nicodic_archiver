import os
import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse
from uuid import uuid4
from wsgiref.simple_server import make_server

from archive_read import (
    ALLOWED_REGISTERED_PER_PAGE,
    DEFAULT_REGISTERED_PER_PAGE,
    DEFAULT_REGISTERED_SORT_BY,
    DEFAULT_REGISTERED_SORT_ORDER,
    REGISTERED_ARTICLE_COLUMNS,
    REGISTERED_SORT_ALLOWLIST,
    _render_registered_list_csv,
    get_saved_article_export,
    get_saved_article_summary,
    get_saved_article_summary_by_exact_title,
    query_registered_articles,
)
from article_resolver import resolve_article_input
from target_list import register_target_url


DEFAULT_TARGET_DB_PATH = os.environ.get("TARGET_DB_PATH", "data/nicodic.db")
DEFAULT_WEB_ACTION_LOG_PATH = os.environ.get(
    "WEB_ACTION_LOG_PATH",
    "data/web_action.log",
)
DEFAULT_DOWNLOAD_FORMAT = "txt"
DOWNLOAD_FORMATS = ("txt", "md", "csv")

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
        "md": "Markdown",
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


def _normalize_download_format(requested_format: str | None) -> str:
    if requested_format in DOWNLOAD_FORMATS:
        return requested_format
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


def _sanitize_article_id_for_filename(article_id: str) -> str:
    """Decode URL-encoded article_id and sanitize for filename prefix."""
    decoded = unquote(article_id)
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", decoded)
    safe = re.sub(r"\s+", " ", safe).strip(" .")
    return safe or "article"


def _ascii_download_fallback(article_id: str, article_type: str) -> str:
    """Return ASCII-safe fallback filename (RFC 5987 non-supporting browsers)."""
    decoded = unquote(article_id)
    try:
        decoded.encode("ascii")
        ascii_id = re.sub(r'[<>:"/\\|?*\x00-\x1f\s]', "_", decoded)
        ascii_id = ascii_id.strip("_") or "article"
    except UnicodeEncodeError:
        ascii_id = "article"
    return f"{ascii_id}{article_type}_article"


def _build_download_filename(
    article_id: str,
    article_type: str,
    title: str | None,
    requested_format: str,
) -> str:
    safe_id = _sanitize_article_id_for_filename(article_id)
    safe_title = _sanitize_download_filename_title(title or "")
    return f"{safe_id}{article_type}_{safe_title}.{requested_format}"


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


def _build_saved_ui_result(check_result: dict, requested_format: str) -> dict:
    return {
        "status": "saved",
        "message": UI_TEXTS["saved_message"].format(
            format_name=_display_format_name(requested_format)
        ),
        "title": check_result["title"],
        "article_id": check_result["article_id"],
        "article_url": check_result["article_url"],
        "article_type": check_result["article_type"],
        "response_count": check_result["response_count"],
        "input": check_result["input"],
        "requested_format": requested_format,
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


def _build_download_query(check_result: dict, requested_format: str) -> str:
    return urlencode(
        {
            "article_id": check_result["article_id"],
            "article_type": check_result["article_type"],
            "resolved_title": check_result["title"],
            "requested_format": requested_format,
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
    requested_format = _normalize_download_format(
        environ.get("copilot.requested_format")
    )

    if check_result["status"] == "saved":
        return _build_saved_ui_result(
            check_result,
            requested_format,
        ), _build_download_query(
            check_result,
            requested_format,
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


def _render_format_selector(selected_format: str) -> str:
    lines = [
        '<fieldset class="format-selector">',
        '<legend>Download format</legend>',
    ]
    for download_format in DOWNLOAD_FORMATS:
        checked = " checked" if download_format == selected_format else ""
        lines.append(
            (
                '<label class="format-option">'
                f'<input type="radio" name="requested_format" '
                f'value="{escape(download_format)}"{checked}>'
                f'{escape(_display_format_name(download_format))}'
                '</label>'
            )
        )
    lines.append("</fieldset>")
    return "".join(lines)


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
    selected_format = DEFAULT_DOWNLOAD_FORMAT
    if result is not None:
        selected_format = result.get(
            "requested_format",
            DEFAULT_DOWNLOAD_FORMAT,
        )
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
        .format-selector {{
            margin: 16px 0;
            padding: 12px 14px;
            border: 1px solid var(--border);
            border-radius: 12px;
            display: flex;
            gap: 14px;
            flex-wrap: wrap;
        }}
        .format-selector legend {{
            padding: 0 6px;
            font-weight: 700;
        }}
        .format-option {{
            display: inline-flex;
            gap: 6px;
            align-items: center;
            font-weight: 400;
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
    .list-link-line {{ margin: 0 0 16px; }}
    .list-btn {{
      display: inline-block;
      padding: 7px 18px;
      border: 1px solid var(--accent);
      border-radius: 8px;
      color: var(--accent);
      text-decoration: none;
      font-size: 0.9rem;
      background: transparent;
    }}
    .list-btn:hover {{ background: rgba(15, 118, 110, 0.08); }}
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
      <h1>{escape(UI_TEXTS['heading'])}</h1>
      <p class=\"lede\">{escape(UI_TEXTS['lede'])}</p>
      <p class=\"list-link-line\">
        <a href=\"/registered\" target=\"_blank\" class=\"list-btn\">
          登録済み記事一覧
        </a>
      </p>
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
                {_render_format_selector(selected_format)}
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
                const selectedFormat = form.querySelector(
                    "input[name='requested_format']:checked"
                );
                const autoDownload = document.querySelector(
                    "[data-auto-download-form]"
                );
                if (selectedFormat && autoDownload) {{
                    const downloadFormatInput = autoDownload.querySelector(
                        "input[name='requested_format']"
                    );
                    if (downloadFormatInput) {{
                        downloadFormatInput.value = selectedFormat.value;
                    }}
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


def _normalize_registered_sort_by(value: str) -> str:
    if value in REGISTERED_SORT_ALLOWLIST:
        return value
    return DEFAULT_REGISTERED_SORT_BY


def _normalize_registered_sort_order(value: str) -> str:
    return "asc" if value == "asc" else DEFAULT_REGISTERED_SORT_ORDER


def _normalize_registered_per_page(value: str) -> int:
    try:
        n = int(value)
    except (ValueError, TypeError):
        return DEFAULT_REGISTERED_PER_PAGE
    if n in ALLOWED_REGISTERED_PER_PAGE:
        return n
    return DEFAULT_REGISTERED_PER_PAGE


def _build_registered_url(
    sort_by: str,
    sort_order: str,
    q: str,
    page: int,
    per_page: int,
) -> str:
    params: dict = {
        "sort_by": sort_by,
        "sort_order": sort_order,
        "page": str(page),
        "per_page": str(per_page),
    }
    if q:
        params["q"] = q
    return "/registered?" + urlencode(params)


def _registered_sort_header_cell(
    col: dict,
    sort_by: str,
    sort_order: str,
    q: str,
    page: int,
    per_page: int,
) -> str:
    key = col["key"]
    label = escape(col["label"])
    if key not in REGISTERED_SORT_ALLOWLIST:
        return f"<th>{label}</th>"
    if sort_by == key:
        next_order = "asc" if sort_order == "desc" else "desc"
        ind = " &#9660;" if sort_order == "desc" else " &#9650;"
    else:
        next_order = "desc"
        ind = ""
    url = escape(_build_registered_url(key, next_order, q, page, per_page))
    return (
        f'<th><a href="{url}" class="sort-link">'
        f"{label}{ind}</a></th>"
    )


def _registered_row_html(row: dict) -> str:
    unscrapped = (row.get("saved_response_count") or 0) == 0
    cls = ' class="not-scraped"' if unscrapped else ""
    cells = []
    for col in REGISTERED_ARTICLE_COLUMNS:
        key = col["key"]
        val = row.get(key)
        if key == "canonical_url" and val:
            safe_url = escape(str(val))
            cell = (
                f'<td><a href="{safe_url}" target="_blank"'
                f' rel="noopener noreferrer"'
                f' class="ext-link">{safe_url}</a></td>'
            )
        elif val is None:
            cell = "<td></td>"
        else:
            cell = f"<td>{escape(str(val))}</td>"
        cells.append(cell)
    return f"<tr{cls}>{''.join(cells)}</tr>"


def _registered_pagination_html(
    page: int,
    total_pages: int,
    sort_by: str,
    sort_order: str,
    q: str,
    per_page: int,
) -> str:
    def _purl(p: int) -> str:
        return escape(
            _build_registered_url(sort_by, sort_order, q, p, per_page)
        )

    parts = []
    if page <= 1:
        parts.append('<span class="page-btn disabled">First</span>')
        parts.append('<span class="page-btn disabled">Prev</span>')
    else:
        parts.append(
            f'<a class="page-btn" href="{_purl(1)}">First</a>'
        )
        parts.append(
            f'<a class="page-btn" href="{_purl(page - 1)}">Prev</a>'
        )
    parts.append(
        f'<span class="page-info">Page {page} / {total_pages}</span>'
    )
    if page >= total_pages:
        parts.append('<span class="page-btn disabled">Next</span>')
        parts.append('<span class="page-btn disabled">Last</span>')
    else:
        parts.append(
            f'<a class="page-btn" href="{_purl(page + 1)}">Next</a>'
        )
        parts.append(
            f'<a class="page-btn" href="{_purl(total_pages)}">Last</a>'
        )
    return (
        '<nav class="pagination">' + "".join(parts) + "</nav>"
    )


def _render_registered_list_page(query_params: dict) -> bytes:
    sort_by = _normalize_registered_sort_by(
        query_params.get("sort_by", "")
    )
    sort_order = _normalize_registered_sort_order(
        query_params.get("sort_order", "")
    )
    search = query_params.get("q", "").strip()
    per_page = _normalize_registered_per_page(
        query_params.get("per_page", "")
    )
    try:
        page = max(1, int(query_params.get("page", "1") or "1"))
    except (ValueError, TypeError):
        page = 1

    result = query_registered_articles(
        sort_by=sort_by,
        sort_order=sort_order,
        search=search,
        page=page,
        per_page=per_page,
    )
    rows = result["rows"]
    total = result["total"]
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)

    # Build header cells
    header_cells = "".join(
        _registered_sort_header_cell(
            col, sort_by, sort_order, search, page, per_page
        )
        for col in REGISTERED_ARTICLE_COLUMNS
    )
    rows_html = "".join(_registered_row_html(r) for r in rows)
    pagination = _registered_pagination_html(
        page, total_pages, sort_by, sort_order, search, per_page
    )

    # Showing meta line
    if total == 0:
        showing = "Count: 0"
    else:
        start = (page - 1) * per_page + 1
        end = min(page * per_page, total)
        showing = (
            f"Count: {total} &mdash; Showing {start}&ndash;{end}"
        )

    search_esc = escape(search)
    sort_by_esc = escape(sort_by)
    sort_ord_esc = escape(sort_order)
    per_page_str = str(per_page)

    csv_params = urlencode({
        "sort_by": sort_by,
        "sort_order": sort_order,
        "q": search,
        "page": str(page),
        "per_page": per_page_str,
    })
    csv_url = escape(f"/registered/csv?{csv_params}")

    clear_link = ""
    if search:
        cl_url = escape(
            _build_registered_url(sort_by, sort_order, "", 1, per_page)
        )
        clear_link = f' <a href="{cl_url}" class="clear-link">Clear</a>'

    per_page_opts = "".join(
        f'<option value="{n}"'
        + (" selected" if n == per_page else "")
        + f">{n}</option>"
        for n in ALLOWED_REGISTERED_PER_PAGE
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Registered Articles</title>
  <style>
    body {{
      font-family: Georgia, serif;
      margin: 0;
      padding: 24px 20px;
      background: #f4efe5;
      color: #1f2430;
    }}
    h1 {{ margin: 0 0 8px; font-size: 1.8rem; }}
    .meta {{ color: #6b7280; margin: 0 0 12px; font-size: 0.9rem; }}
    .controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      margin-bottom: 12px;
    }}
    .search-form {{
      display: flex;
      gap: 6px;
      align-items: center;
    }}
    .search-form input[type="text"] {{
      padding: 5px 10px;
      border: 1px solid #d9ccb4;
      border-radius: 6px;
      font: inherit;
      min-width: 180px;
    }}
    button, .btn {{
      padding: 5px 14px;
      border: 0;
      border-radius: 6px;
      background: #0f766e;
      color: #fff;
      font: inherit;
      cursor: pointer;
      text-decoration: none;
    }}
    button:hover, .btn:hover {{ background: #0d6560; }}
    .clear-link {{
      color: #6b7280;
      font-size: 0.88rem;
      text-decoration: none;
    }}
    .clear-link:hover {{ text-decoration: underline; }}
    select.per-page {{
      padding: 4px 8px;
      border: 1px solid #d9ccb4;
      border-radius: 6px;
      font: inherit;
    }}
    .csv-link {{
      padding: 5px 12px;
      border: 1px solid #0f766e;
      border-radius: 6px;
      color: #0f766e;
      text-decoration: none;
      font-size: 0.9rem;
      background: transparent;
    }}
    .csv-link:hover {{ background: rgba(15, 118, 110, 0.08); }}
    table {{
      border-collapse: collapse;
      width: 100%;
      background: #fffaf2;
      border: 1px solid #d9ccb4;
      border-radius: 8px;
      overflow: hidden;
    }}
    th, td {{
      text-align: left;
      padding: 8px 12px;
      border-bottom: 1px solid #e8dfc8;
      word-break: break-word;
    }}
    th {{ background: #f0e9d8; font-weight: 700; }}
    th a.sort-link {{
      color: #1f2430;
      text-decoration: none;
      white-space: nowrap;
    }}
    th a.sort-link:hover {{ text-decoration: underline; }}
    tr:last-child td {{ border-bottom: none; }}
    tr.not-scraped td {{ background: #fff8e6; }}
    a {{ color: #0f766e; }}
    .ext-link {{ word-break: break-all; }}
    .pagination {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin-top: 16px;
    }}
    .page-btn {{
      padding: 4px 10px;
      border: 1px solid #d9ccb4;
      border-radius: 6px;
      color: #0f766e;
      text-decoration: none;
      font-size: 0.9rem;
    }}
    .page-btn.disabled {{
      color: #aaa;
      border-color: #e0d8c8;
      pointer-events: none;
    }}
    .page-info {{ font-size: 0.9rem; color: #6b7280; }}
  </style>
</head>
<body>
  <h1>Registered Articles</h1>
  <p class="meta">
    {showing} &mdash;
    <a href="/" target="_self">&larr; Top</a>
  </p>
  <div class="controls">
    <form method="get" action="/registered" class="search-form">
      <input type="hidden" name="sort_by" value="{sort_by_esc}">
      <input type="hidden" name="sort_order" value="{sort_ord_esc}">
      <input type="hidden" name="per_page" value="{per_page_str}">
      <input type="text" name="q" value="{search_esc}"
             placeholder="Search title or article ID&hellip;">
      <button type="submit">Search</button>{clear_link}
    </form>
    <form method="get" action="/registered">
      <input type="hidden" name="q" value="{search_esc}">
      <input type="hidden" name="sort_by" value="{sort_by_esc}">
      <input type="hidden" name="sort_order" value="{sort_ord_esc}">
      <input type="hidden" name="page" value="1">
      <select name="per_page" class="per-page"
              onchange="this.form.submit()">{per_page_opts}</select>
    </form>
    <a href="{csv_url}" class="csv-link">&#8595; CSV (this page)</a>
  </div>
  <table>
    <thead>
      <tr>{header_cells}</tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
  {pagination}
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
            environ["copilot.requested_format"] = form.get(
                "requested_format",
                DEFAULT_DOWNLOAD_FORMAT,
            )
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
                query.get(
                    "requested_format",
                    DEFAULT_DOWNLOAD_FORMAT,
                )
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
            content_type = {
                "txt": "text/plain; charset=utf-8",
                "md": "text/markdown; charset=utf-8",
                "csv": "text/csv; charset=utf-8",
            }.get(requested_format, "text/plain; charset=utf-8")
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

        if method == "GET" and path == "/registered":
            query = _read_query_params(environ)
            body = _render_registered_list_page(query)
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/html; charset=utf-8"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]

        if method == "GET" and path == "/registered/csv":
            query = _read_query_params(environ)
            sort_by = _normalize_registered_sort_by(
                query.get("sort_by", "")
            )
            sort_order = _normalize_registered_sort_order(
                query.get("sort_order", "")
            )
            search = query.get("q", "").strip()
            per_page = _normalize_registered_per_page(
                query.get("per_page", "")
            )
            try:
                csv_page = max(
                    1, int(query.get("page", "1") or "1")
                )
            except (ValueError, TypeError):
                csv_page = 1
            csv_result = query_registered_articles(
                sort_by=sort_by,
                sort_order=sort_order,
                search=search,
                page=csv_page,
                per_page=per_page,
            )
            csv_text = _render_registered_list_csv(
                csv_result["rows"]
            )
            csv_body = csv_text.encode("utf-8")
            fname = f"registered_articles_p{csv_page}.csv"
            start_response(
                "200 OK",
                [
                    ("Content-Type", "text/csv; charset=utf-8"),
                    (
                        "Content-Disposition",
                        f'attachment; filename="{fname}"',
                    ),
                    ("Content-Length", str(len(csv_body))),
                ],
            )
            return [csv_body]

        if path not in {
            "/", "/download", "/registered", "/registered/csv"
        }:
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
