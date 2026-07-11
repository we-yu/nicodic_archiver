import os
import re
from datetime import datetime, timedelta, timezone
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
from issue_report import (
    IssueReportRateLimiter,
    format_issue_context_block,
    issue_report_enabled,
    submit_issue_report,
)
from target_list import register_target_url


DEFAULT_TARGET_DB_PATH = os.environ.get("TARGET_DB_PATH", "data/nicodic.db")
DEFAULT_WEB_ACTION_LOG_PATH = os.environ.get(
    "WEB_ACTION_LOG_PATH",
    "data/web_action.log",
)
DEFAULT_DOWNLOAD_FORMAT = "txt"
DOWNLOAD_FORMATS = ("txt", "md", "csv")
JST = timezone(timedelta(hours=9))

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
        "denylisted": "This article is excluded from archive collection.",
        "invalid_input": (
            "Enter an article name or a valid Nicopedia article URL."
        ),
        "ambiguous": "Could not resolve the article.",
        "temporary_fetch_failure": (
            "Temporary fetch failure. Please try again later."
        ),
        "unexpected_internal_error": "An unexpected internal error occurred.",
        "registration_resolution_failure": (
            "Article metadata resolution failed; the target was not registered."
        ),
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
    if result["status"] == "denylisted":
        return "denylisted"
    if result["status"] == "registration_resolution_failure":
        return "registration_resolution_failure"
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


def _build_error_ui_result(
    result: dict,
    reference_id: str | None,
    *,
    article_input: str | None = None,
    requested_format: str | None = None,
    path: str = "/",
    action: str = "archive_check",
) -> dict:
    error_result = {
        "status": "error",
        "message": _user_error_message(result),
        "error_code": _result_error_code(result),
    }
    if reference_id is not None:
        error_result["reference_id"] = reference_id
        input_value = (
            article_input
            if article_input is not None
            else result.get("input", "")
        )
        fmt = requested_format or DEFAULT_DOWNLOAD_FORMAT
        error_result["issue_context_text"] = format_issue_context_block(
            reference_id=reference_id,
            action=action,
            input_value=input_value,
            download_format=fmt,
            result=_result_error_code(result),
            message=error_result["message"],
            path=path,
        )
    return error_result


def _log_issue_report_action(
    web_action_log_path: str,
    environ: dict,
    report_result: dict,
) -> None:
    try:
        _append_web_action_log_lines(
            Path(web_action_log_path),
            [
                "",
                "WEB_ACTION_START",
                f"  action_id={report_result['reference_id']}",
                f"  timestamp={report_result['timestamp']}",
                "  action_kind=issue_report",
                f"  visitor_hint={_visitor_hint(environ)}",
                f"  path={_web_log_value(report_result.get('path', '/'))}",
                f"  outcome={_web_log_value(report_result.get('outcome'))}",
                f"  reason={_web_log_value(report_result.get('reason'))}",
                (
                    "  report_length="
                    f"{report_result.get('report_length', 0)}"
                ),
                "WEB_ACTION_END",
                "",
            ],
        )
    except OSError:
        pass


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
    request_path = environ.get("PATH_INFO", "/") or "/"

    def _archive_error(result: dict, reference_id: str | None) -> dict:
        return _build_error_ui_result(
            result,
            reference_id,
            article_input=article_input,
            requested_format=requested_format,
            path=request_path,
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
                source="web_user",
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
            return _archive_error(failure_result, reference_id), None

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

        if registration_status == "resolution_failure":
            failure_result = {
                "status": "registration_resolution_failure",
                "input": check_result["input"],
                "message": (
                    "Article could not be registered due to a metadata "
                    "resolution failure."
                ),
            }
            reference_id = _log_web_action(
                web_action_log_path,
                environ,
                action_kind="registration",
                input_value=article_input,
                requested_format=None,
                result_status="resolution_failure",
                resolved_title=check_result["title"],
                resolved_article_id=check_result["article_id"],
                resolved_article_type=check_result["article_type"],
                resolved_canonical_url=check_result["article_url"],
                error_code="resolution_failure",
            )
            return _archive_error(failure_result, reference_id), None

        if registration_status == "denylisted":
            reference_id = _log_web_action(
                web_action_log_path,
                environ,
                action_kind="registration",
                input_value=article_input,
                requested_format=None,
                result_status="denylisted",
                resolved_title=check_result["title"],
                resolved_article_id=check_result["article_id"],
                resolved_article_type=check_result["article_type"],
                resolved_canonical_url=check_result["article_url"],
                error_code="denylisted",
            )
            return _archive_error(
                {"status": "denylisted"},
                reference_id,
            ), None

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
        return _archive_error(failure_result, reference_id), None

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
    return _archive_error(check_result, reference_id), None


def _render_result_detail(label: str, value: str) -> str:
    return f"<p><strong>{escape(label)}:</strong> {escape(value)}</p>"


def _render_format_selector(selected_format: str) -> str:
    lines = [
        '<fieldset class="format-selector">',
        '<legend>Download format</legend>',
        '<div class="format-options">',
    ]
    for download_format in DOWNLOAD_FORMATS:
        checked = " checked" if download_format == selected_format else ""
        lines.append(
            (
                '<label class="format-option">'
                f'<input type="radio" name="requested_format" '
                f'value="{escape(download_format)}"{checked}>'
                '<span class="format-option-label">'
                f'{escape(_display_format_name(download_format))}'
                '</span>'
                '</label>'
            )
        )
    lines.append("</div>")
    lines.append("</fieldset>")
    return "".join(lines)


def _shared_console_style() -> str:
    """Design tokens and chrome shared by the top page and Registered page."""
    return """
    :root {
      color-scheme: light;
      --ink: #171b26;
      --ink-soft: #3d4354;
      --muted: #6b7280;
      --paper: #f3efe2;
      --paper-deep: #eae1cb;
      --panel: #fffdf7;
      --panel-alt: #faf4e5;
      --border: #e1d5b6;
      --border-strong: #cdba8a;
      --header-bg: #12172a;
      --header-bg-soft: #1c2247;
      --header-ink: #f4f1e6;
      --accent: #0f766e;
      --accent-strong: #0b5a54;
      --accent-soft: rgba(15, 118, 110, 0.14);
      --gold: #b3803c;
      --saved: #146c43;
      --registered: #92400e;
      --error: #b3261e;
      --focus: #2f6fed;
      --radius-lg: 20px;
      --radius-md: 12px;
      --radius-sm: 8px;
      --shadow-panel: 0 24px 48px rgba(15, 17, 26, 0.10);
      --shadow-header: 0 8px 24px rgba(10, 12, 20, 0.22);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", system-ui, -apple-system,
        "Helvetica Neue", Arial, sans-serif;
      color: var(--ink);
      background: var(--paper);
    }
    a { color: var(--accent-strong); }
    :focus-visible {
      outline: 3px solid var(--focus);
      outline-offset: 2px;
      border-radius: 4px;
    }
    .app-shell { min-height: 100%; }
    .site-header {
      background: linear-gradient(
        135deg, var(--header-bg) 0%, var(--header-bg-soft) 100%
      );
      color: var(--header-ink);
      box-shadow: var(--shadow-header);
      border-bottom: 3px solid var(--gold);
    }
    .site-header-inner {
      max-width: 1120px;
      margin: 0 auto;
      padding: 14px 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
    }
    .brand {
      display: inline-flex;
      align-items: center;
      gap: 12px;
      color: var(--header-ink);
      text-decoration: none;
    }
    .brand-mark {
      display: inline-grid;
      place-items: center;
      width: 34px;
      height: 34px;
      border-radius: 9px;
      background: linear-gradient(155deg, var(--gold), #8a5e26);
      color: #1c1406;
      font-weight: 700;
      font-size: 1.05rem;
    }
    .brand-text { display: flex; flex-direction: column; line-height: 1.15; }
    .brand-name { font-weight: 700; font-size: 1.05rem; letter-spacing: 0.01em; }
    .brand-tagline {
      font-size: 0.7rem;
      color: rgba(244, 241, 230, 0.68);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .site-nav { display: flex; gap: 8px; flex-wrap: wrap; }
    .site-nav-link {
      color: var(--header-ink);
      text-decoration: none;
      font-size: 0.86rem;
      padding: 7px 14px;
      border-radius: 999px;
      border: 1px solid rgba(244, 241, 230, 0.25);
      background: rgba(244, 241, 230, 0.06);
      white-space: nowrap;
    }
    .site-nav-link:hover { background: rgba(244, 241, 230, 0.18); }
    .site-nav-link.is-current {
      background: rgba(244, 241, 230, 0.92);
      color: var(--header-bg);
      border-color: transparent;
      font-weight: 600;
    }
    .site-footer {
      max-width: 1120px;
      margin: 0 auto;
      padding: 26px 20px 40px;
      color: var(--muted);
      font-size: 0.82rem;
    }
    button, .btn, .page-btn, a {
      -webkit-tap-highlight-color: transparent;
    }
    @media (prefers-reduced-motion: no-preference) {
      .panel, .page-btn, .site-nav-link, button, .btn, tr {
        transition: box-shadow 0.15s ease, transform 0.15s ease,
          background-color 0.15s ease, border-color 0.15s ease;
      }
    }
    """


def _site_header_html(current: str) -> str:
    top_current = " is-current" if current == "top" else ""
    reg_current = " is-current" if current == "registered" else ""
    return (
        '<header class="site-header">'
        '<div class="site-header-inner">'
        '<a class="brand" href="/">'
        '<span class="brand-mark" aria-hidden="true">N</span>'
        '<span class="brand-text">'
        '<span class="brand-name">NicoArc</span>'
        '<span class="brand-tagline">Archive Console</span>'
        "</span>"
        "</a>"
        '<nav class="site-nav" aria-label="Primary">'
        f'<a href="/" class="site-nav-link{top_current}">Top</a>'
        f'<a href="/registered" target="_blank" '
        f'class="site-nav-link{reg_current}">登録済み記事一覧</a>'
        "</nav>"
        "</div>"
        "</header>"
    )


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
        issue_context = result.get("issue_context_text")
        if issue_context:
            lines.append(_render_issue_context_block(issue_context))

    lines.append("</section>")
    return "".join(lines)


def _render_issue_context_block(issue_context_text: str) -> str:
    context_id = f"issue-context-{uuid4().hex[:8]}"
    safe_text = escape(issue_context_text)
    return (
        '<div class="issue-context-block">'
        '<p class="issue-context-heading">Issue report context:</p>'
        f'<textarea id="{context_id}" class="issue-context-text" '
        'readonly rows="8">'
        f"{safe_text}"
        "</textarea>"
        f'<button type="button" class="copy-context-btn" '
        f'data-copy-target="{context_id}">Copy</button>'
        "</div>"
    )


def _render_issue_report_section(
    issue_report_result: dict | None = None,
    issue_context_prefill: str | None = None,
) -> str:
    open_attr = " open" if issue_report_result else ""
    status_html = ""
    if issue_report_result is not None:
        status_class = (
            "issue-report-status ok"
            if issue_report_result.get("ok")
            else "issue-report-status error"
        )
        status_html = (
            f'<p class="{status_class}">'
            f"{escape(issue_report_result['message'])}"
            "</p>"
        )
    configured = issue_report_enabled()
    if configured:
        availability_note = (
            '<p class="issue-report-note">'
            "Send a short problem report to the site operator."
            "</p>"
        )
    else:
        availability_note = (
            '<p class="issue-report-note">'
            "Issue reporting is not configured on this server."
            "</p>"
        )
    context_hidden = ""
    if issue_context_prefill:
        context_hidden = (
            '<input type="hidden" name="issue_context" '
            f'value="{escape(issue_context_prefill)}">'
        )
    return (
        f'<details class="issue-report"{open_attr}>'
        "<summary>Report a problem</summary>"
        '<div class="issue-report-body">'
        f"{availability_note}"
        f"{status_html}"
        '<form method="post" action="/issue-report">'
        '<label for="report_body">Describe the problem</label>'
        '<textarea id="report_body" name="report_body" rows="5" '
        'maxlength="1000"></textarea>'
        f"{context_hidden}"
        '<label class="hp-field" aria-hidden="true">'
        'Website<input type="text" name="website" tabindex="-1" '
        'autocomplete="off">'
        "</label>"
        '<button type="submit">Send report</button>'
        "</form>"
        "</div>"
        "</details>"
    )


def _render_page(
    article_input: str,
    result: dict | None = None,
    download_query: str | None = None,
    issue_report_result: dict | None = None,
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
    issue_context_prefill = None
    if result is not None and result.get("status") == "error":
        issue_context_prefill = result.get("issue_context_text")
    issue_report_section = _render_issue_report_section(
        issue_report_result=issue_report_result,
        issue_context_prefill=issue_context_prefill,
    )
    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{escape(UI_TEXTS['page_title'])}</title>
  <style>
    {_shared_console_style()}
    main {{
      max-width: 760px;
      margin: 0 auto;
      padding: 36px 20px 64px;
      display: grid;
      gap: 18px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius-lg);
      padding: 24px;
      box-shadow: var(--shadow-panel);
    }}
    .hero {{
      background: linear-gradient(
        165deg, var(--panel) 0%, var(--panel-alt) 100%
      );
      border-top: 3px solid var(--gold);
    }}
    .hero-top {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .eyebrow {{
      margin: 0 0 6px;
      color: var(--accent-strong);
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.1em;
    }}
    .secondary-action {{
      display: inline-flex;
      align-items: center;
      padding: 8px 16px;
      border: 1px solid var(--accent);
      border-radius: 999px;
      color: var(--accent-strong);
      text-decoration: none;
      font-size: 0.86rem;
      font-weight: 600;
      background: #fff;
      white-space: nowrap;
    }}
    .secondary-action:hover {{ background: var(--accent-soft); }}
    h1 {{
      margin: 0 0 10px;
      font-size: clamp(1.7rem, 3.4vw, 2.4rem);
      line-height: 1.15;
      letter-spacing: -0.01em;
    }}
    p {{ line-height: 1.55; }}
    .lede {{ color: var(--ink-soft); margin: 0; max-width: 56ch; }}
    .panel-title {{
      margin: 0 0 14px;
      font-size: 1.05rem;
      font-weight: 700;
      color: var(--ink);
    }}
    form {{ display: grid; gap: 14px; }}
    .field {{ display: grid; gap: 6px; }}
    label {{ font-weight: 700; font-size: 0.92rem; }}
    input[type=\"text\"] {{
      width: 100%;
      padding: 13px 15px;
      border-radius: var(--radius-md);
      border: 1px solid var(--border-strong);
      background: #fff;
      font: inherit;
      color: var(--ink);
    }}
    input[type=\"text\"]:focus-visible {{ border-color: var(--focus); }}
    .actions-row {{
      display: flex;
      align-items: center;
      gap: 14px;
      flex-wrap: wrap;
      margin-top: 4px;
    }}
    button {{
      width: fit-content;
      padding: 12px 22px;
      border: 0;
      border-radius: 999px;
      background: linear-gradient(155deg, var(--accent), var(--accent-strong));
      color: #fff;
      font: inherit;
      font-weight: 600;
      cursor: pointer;
      box-shadow: 0 10px 20px rgba(15, 118, 110, 0.25);
    }}
    button:hover {{ box-shadow: 0 12px 24px rgba(15, 118, 110, 0.32); }}
    button[disabled] {{
      background: var(--muted);
      box-shadow: none;
      cursor: progress;
    }}
    .busy-message {{
      margin: 0;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .format-selector {{
      margin: 0;
      padding: 0;
      border: 0;
    }}
    .format-selector legend {{
      padding: 0 0 8px;
      font-weight: 700;
      font-size: 0.92rem;
    }}
    .format-options {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .format-option {{
      display: inline-flex;
      gap: 8px;
      align-items: center;
      font-weight: 500;
      padding: 8px 14px;
      border: 1px solid var(--border-strong);
      border-radius: 999px;
      background: #fff;
      cursor: pointer;
    }}
    .format-option input {{ accent-color: var(--accent); }}
    .format-option:has(input:checked) {{
      background: var(--accent-soft);
      border-color: var(--accent);
      color: var(--accent-strong);
    }}
    .result-shell {{ padding: 0; overflow: hidden; }}
    .message-area {{
      padding: 20px 24px;
      border-left: 5px solid var(--border-strong);
    }}
    .message-area p {{
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
    .message-area h2 {{
      margin-top: 0;
      font-size: 0.82rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}
    .message-area.empty {{ color: var(--muted); }}
    .status-line {{ font-size: 1.05rem; font-weight: 600; margin-top: 0; }}
    .message-area.saved {{ border-left-color: var(--saved); }}
    .message-area.saved .status-line {{ color: var(--saved); }}
    .message-area.saved .status-line::before {{ content: \"\\2713  \"; }}
    .message-area.registered {{ border-left-color: var(--registered); }}
    .message-area.registered .status-line {{ color: var(--registered); }}
    .message-area.registered .status-line::before {{ content: \"+  \"; }}
    .message-area.error {{ border-left-color: var(--error); }}
    .message-area.error .status-line {{ color: var(--error); }}
    .message-area.error .status-line::before {{ content: \"!  \"; }}
    .followup-note {{ color: var(--muted); }}
    .download-frame {{ display: none; width: 0; height: 0; border: 0; }}
    .issue-context-block {{
      margin-top: 14px;
      padding-top: 12px;
      border-top: 1px dashed var(--border);
    }}
    .issue-context-heading {{
      margin: 0 0 8px;
      font-weight: 700;
    }}
    .issue-context-text {{
      width: 100%;
      font-family: ui-monospace, \"Cascadia Mono\", Consolas, monospace;
      font-size: 0.85rem;
      padding: 10px 12px;
      border-radius: var(--radius-sm);
      border: 1px solid var(--border);
      background: var(--panel-alt);
      resize: vertical;
      color: var(--ink);
    }}
    .copy-context-btn {{
      margin-top: 8px;
      padding: 8px 16px;
      font-size: 0.85rem;
      background: var(--ink);
      box-shadow: none;
    }}
    .issue-panel {{ padding: 0; }}
    .issue-report {{ padding: 18px 24px; }}
    .issue-report summary {{
      cursor: pointer;
      font-weight: 700;
      color: var(--accent-strong);
      list-style: none;
    }}
    .issue-report summary::before {{ content: \"\\270E  \"; }}
    .issue-report-body {{
      margin-top: 14px;
      display: grid;
      gap: 10px;
    }}
    .issue-report-note {{
      margin: 0;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .issue-report-status.ok {{ color: var(--saved); font-weight: 600; }}
    .issue-report-status.error {{ color: var(--error); font-weight: 600; }}
    .issue-report textarea {{
      width: 100%;
      min-height: 120px;
      padding: 12px 14px;
      border-radius: var(--radius-md);
      border: 1px solid var(--border-strong);
      background: #fff;
      font: inherit;
      resize: vertical;
    }}
    .hp-field {{
      position: absolute;
      left: -10000px;
      width: 1px;
      height: 1px;
      overflow: hidden;
    }}
    @media (max-width: 640px) {{
      main {{ padding: 20px 14px 40px; }}
      .panel {{ padding: 18px; }}
      .message-area {{ padding: 16px; }}
      .issue-report {{ padding: 16px; }}
      button {{ width: 100%; }}
    }}
  </style>
</head>
<body>
  <div class=\"app-shell\">
    {_site_header_html("top")}
    <main>
      <section class=\"panel hero\">
        <div class=\"hero-top\">
          <p class=\"eyebrow\">Web Archive Utility</p>
          <a href=\"/registered\" target=\"_blank\"
             class=\"secondary-action\">登録済み記事一覧</a>
        </div>
        <h1>{escape(UI_TEXTS['heading'])}</h1>
        <p class=\"lede\">{escape(UI_TEXTS['lede'])}</p>
      </section>
      <section class=\"panel\">
        <h2 class=\"panel-title\">Check an article</h2>
        <form method=\"post\" action=\"/\" data-archive-check-form>
          <div class=\"field\">
            <label for=\"article_input\">
              {escape(UI_TEXTS['input_label'])}
            </label>
            <input
              id=\"article_input\"
              name=\"article_input\"
              type=\"text\"
              value=\"{safe_input}\"
              placeholder=\"{escape(UI_TEXTS['input_placeholder'])}\"
              autocomplete=\"off\"
            >
          </div>
          {_render_format_selector(selected_format)}
          <div class=\"actions-row\">
            <button type=\"submit\" data-submit-button>
              {escape(UI_TEXTS['submit_label'])}
            </button>
            <p class=\"busy-message\" data-busy-message hidden
               aria-live=\"polite\">
              {escape(UI_TEXTS['busy_message'].format(format_name=format_name))}
            </p>
          </div>
        </form>
      </section>
      <section class=\"panel result-shell\">
        {message_area}
      </section>
      <section class=\"panel issue-panel\">
        {issue_report_section}
      </section>
    </main>
    <footer class=\"site-footer\">
      <p>NicoArc Archive Console &mdash; local archive utility.</p>
    </footer>
  </div>
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
    document.querySelectorAll("[data-copy-target]").forEach((button) => {{
      button.addEventListener("click", () => {{
        const targetId = button.getAttribute("data-copy-target");
        const target = targetId ? document.getElementById(targetId) : null;
        if (!target) {{
          return;
        }}
        target.focus();
        target.select();
        if (typeof target.setSelectionRange === "function") {{
          target.setSelectionRange(0, target.value.length);
        }}
        try {{
          navigator.clipboard.writeText(target.value);
        }} catch (err) {{
          document.execCommand("copy");
        }}
      }});
    }});
  </script>
</body>
</html>
"""
    return html.encode("utf-8")


def _normalize_registered_sort_by(value: str) -> str:
    # Legacy saved-stat sort aliases now resolve to the observed board max.
    if value in ("latest_scraped_max_res_no", "saved_max_res_no"):
        value = "observed_max_res_no"
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


def _registered_column_class(key: str) -> str:
    classes = {
        "article_id": "col-article-id",
        "article_type": "col-article-type",
        "title": "col-title",
        "canonical_url": "col-canonical-url",
        "created_at": "col-created-at",
        "saved_response_count": "col-saved-count",
        "observed_max_res_no": "col-observed-max-res",
        "last_scraped_at": "col-last-scraped",
    }
    return classes.get(key, "")


def _registered_align_class(key: str) -> str:
    if key in {"title", "canonical_url"}:
        return "align-left"
    if key in {"article_id", "saved_response_count", "observed_max_res_no"}:
        return "align-right"
    return "align-center"


def _format_registered_time(value: str | None) -> str:
    if not value:
        return ""
    normalized = value.strip()
    if not normalized:
        return ""
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return normalized
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(JST).strftime("%Y-%m-%d %H:%M JST")


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
    col_class = " ".join(
        part
        for part in (
            _registered_column_class(key),
            _registered_align_class(key),
        )
        if part
    )
    cls_attr = f' class="{col_class}"' if col_class else ""
    if key not in REGISTERED_SORT_ALLOWLIST:
        return f"<th{cls_attr}>{label}</th>"
    if sort_by == key:
        next_order = "asc" if sort_order == "desc" else "desc"
        ind = " &#9660;" if sort_order == "desc" else " &#9650;"
    else:
        next_order = "desc"
        ind = ""
    url = escape(_build_registered_url(key, next_order, q, page, per_page))
    return (
        f'<th{cls_attr}><a href="{url}" class="sort-link">'
        f"{label}{ind}</a></th>"
    )


def _registered_has_completed_scrape_check(row: dict) -> bool:
    """Recorded responses exist and/or last-scrape timestamp is stored."""
    if (row.get("saved_response_count") or 0) > 0:
        return True
    last_ts = row.get("last_scraped_at") or ""
    return bool(last_ts.strip())


def _registered_row_html(row: dict) -> str:
    unscrapped = not _registered_has_completed_scrape_check(row)
    cls = ' class="not-scraped"' if unscrapped else ""
    cells = []
    for col in REGISTERED_ARTICLE_COLUMNS:
        key = col["key"]
        val = row.get(key)
        col_class = " ".join(
            part
            for part in (
                _registered_column_class(key),
                _registered_align_class(key),
            )
            if part
        )
        cls_attr = f' class="{col_class}"' if col_class else ""
        if key == "canonical_url" and val:
            safe_url = escape(str(val))
            cell = (
                f'<td{cls_attr}><a href="{safe_url}" target="_blank"'
                f' rel="noopener noreferrer"'
                f' class="ext-link truncated-url" title="{safe_url}">'
                f"{safe_url}</a></td>"
            )
        elif key in {"created_at", "last_scraped_at"}:
            display = _format_registered_time(val)
            cell = f"<td{cls_attr}>{escape(display)}</td>"
        elif val is None:
            cell = f"<td{cls_attr}></td>"
        else:
            cell = f"<td{cls_attr}>{escape(str(val))}</td>"
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
    top_pagination = _registered_pagination_html(
        page, total_pages, sort_by, sort_order, search, per_page
    )
    bottom_pagination = _registered_pagination_html(
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
    refresh_url = escape(
        _build_registered_url(sort_by, sort_order, search, page, per_page)
    )
    reset_url = "/registered"

    csv_params = urlencode({
        "sort_by": sort_by,
        "sort_order": sort_order,
        "q": search,
        "page": str(page),
        "per_page": per_page_str,
    })
    csv_url = escape(f"/registered/csv?{csv_params}")

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
    {_shared_console_style()}
    .registered-main {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 28px 20px 48px;
      display: grid;
      gap: 16px;
    }}
    .page-head {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .page-head h1 {{
      margin: 0;
      font-size: 1.5rem;
      letter-spacing: -0.01em;
    }}
    .meta {{ color: var(--muted); margin: 0; font-size: 0.88rem; }}
    .panel.toolbar {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius-md);
      padding: 14px 16px;
      box-shadow: var(--shadow-panel);
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
    }}
    .search-form {{
      display: flex;
      gap: 6px;
      align-items: center;
      flex-wrap: wrap;
    }}
    .search-form input[type="text"] {{
      padding: 8px 12px;
      border: 1px solid var(--border-strong);
      border-radius: var(--radius-sm);
      font: inherit;
      min-width: 280px;
      width: min(42vw, 30rem);
      background: #fff;
    }}
    .toolbar-actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }}
    button, .btn {{
      padding: 8px 16px;
      border: 0;
      border-radius: var(--radius-sm);
      background: var(--accent);
      color: #fff;
      font: inherit;
      font-weight: 600;
      cursor: pointer;
      text-decoration: none;
    }}
    button:hover, .btn:hover {{ background: var(--accent-strong); }}
    select.per-page {{
      padding: 7px 10px;
      border: 1px solid var(--border-strong);
      border-radius: var(--radius-sm);
      font: inherit;
      background: #fff;
    }}
    .csv-link, .aux-link {{
      padding: 7px 14px;
      border: 1px solid var(--accent);
      border-radius: var(--radius-sm);
      color: var(--accent-strong);
      text-decoration: none;
      font-size: 0.88rem;
      background: transparent;
      font-weight: 600;
    }}
    .csv-link:hover, .aux-link:hover {{
      background: var(--accent-soft);
    }}
    .aux-link {{ border-color: var(--border-strong); color: var(--ink); }}
    .panel.table-panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius-md);
      box-shadow: var(--shadow-panel);
      padding: 0;
      overflow: hidden;
    }}
    .table-shell {{ overflow-x: auto; }}
    table {{
      border-collapse: collapse;
      width: 100%;
      table-layout: fixed;
      background: var(--panel);
    }}
    th, td {{
      padding: 9px 12px;
      border-bottom: 1px solid var(--border);
      vertical-align: middle;
    }}
    th {{
      background: var(--panel-alt);
      font-weight: 700;
      font-size: 0.85rem;
      position: sticky;
      top: 0;
      z-index: 1;
      box-shadow: inset 0 -1px 0 var(--border-strong);
    }}
    th.align-left, td.align-left {{ text-align: left; }}
    th.align-center, td.align-center {{ text-align: center; }}
    th.align-right, td.align-right {{ text-align: right; }}
    td.align-right {{ font-variant-numeric: tabular-nums; }}
    th a.sort-link {{
      color: var(--ink);
      text-decoration: none;
      white-space: nowrap;
    }}
    th a.sort-link:hover {{ text-decoration: underline; }}
    tr:last-child td {{ border-bottom: none; }}
    tbody tr:hover td {{ background: var(--panel-alt); }}
    tr.not-scraped td {{ background: #fbeecb; }}
    tr.not-scraped:hover td {{ background: #f7e5b4; }}
    a {{ color: var(--accent-strong); }}
    .col-article-id {{ width: 9ch; }}
    .col-article-type {{ width: 6ch; }}
    .col-title {{ width: 21%; }}
    .col-canonical-url {{ width: 13%; }}
    .col-created-at, .col-last-scraped {{ width: 17ch; }}
    .col-saved-count, .col-observed-max-res {{ width: 11ch; }}
    td.col-title {{
      font-weight: 600;
      white-space: normal;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
    th.col-title {{ white-space: nowrap; }}
    th.col-canonical-url,
    td.col-canonical-url {{
      white-space: nowrap;
      overflow: hidden;
    }}
    .ext-link.truncated-url {{
      display: block;
      width: 100%;
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      word-break: normal;
    }}
    .pagination {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin-top: 16px;
    }}
    .pagination.top {{ margin: 0; }}
    .pagination.bottom {{ margin-top: 4px; }}
    .page-btn {{
      padding: 5px 12px;
      border: 1px solid var(--border-strong);
      border-radius: var(--radius-sm);
      color: var(--accent-strong);
      text-decoration: none;
      font-size: 0.88rem;
      background: var(--panel);
    }}
    .page-btn:hover {{ background: var(--accent-soft); }}
    .page-btn.disabled {{
      color: #aaa;
      border-color: var(--border);
      pointer-events: none;
      background: transparent;
    }}
    .page-info {{ font-size: 0.88rem; color: var(--muted); }}
    @media (max-width: 720px) {{
      .registered-main {{ padding: 18px 12px 36px; }}
      .panel.toolbar {{ flex-direction: column; align-items: stretch; }}
      .search-form input[type="text"] {{ min-width: 0; width: 100%; }}
      .toolbar-actions {{ justify-content: flex-start; }}
    }}
  </style>
</head>
<body>
  <div class="app-shell">
    {_site_header_html("registered")}
    <main class="registered-main">
      <div class="page-head">
        <h1>Registered Articles</h1>
        <p class="meta">{showing}</p>
      </div>
      <section class="panel toolbar">
        <form method="get" action="/registered" class="search-form">
          <input type="hidden" name="sort_by" value="{sort_by_esc}">
          <input type="hidden" name="sort_order" value="{sort_ord_esc}">
          <input type="hidden" name="per_page" value="{per_page_str}">
          <input
              type="text"
              name="q"
              value="{search_esc}"
              class="registered-search-input"
              placeholder="Search title or article ID"
          >
          <button type="submit">Search</button>
        </form>
        <div class="toolbar-actions">
          <form method="get" action="/registered">
            <input type="hidden" name="q" value="{search_esc}">
            <input type="hidden" name="sort_by" value="{sort_by_esc}">
            <input type="hidden" name="sort_order" value="{sort_ord_esc}">
            <input type="hidden" name="page" value="1">
            <select name="per_page" class="per-page"
                    onchange="this.form.submit()">{per_page_opts}</select>
          </form>
          <a href="{csv_url}" class="csv-link">&#8595; CSV (this page)</a>
          <a href="{refresh_url}" class="aux-link">Refresh</a>
          <a href="{reset_url}" class="aux-link">Reset</a>
        </div>
      </section>
      {top_pagination.replace('class="pagination"', 'class="pagination top"')}
      <section class="panel table-panel">
        <div class="table-shell">
          <table>
            <thead>
              <tr>{header_cells}</tr>
            </thead>
            <tbody>
{rows_html}
            </tbody>
          </table>
        </div>
      </section>
      {bottom_pagination.replace('class="pagination"', 'class="pagination bottom"')}
    </main>
    <footer class="site-footer">
      <p>NicoArc Archive Console &mdash; local archive utility.</p>
    </footer>
  </div>
</body>
</html>
"""
    return html.encode("utf-8")


def create_app(
    target_db_path: str = DEFAULT_TARGET_DB_PATH,
    web_action_log_path: str = DEFAULT_WEB_ACTION_LOG_PATH,
    issue_report_rate_limiter: IssueReportRateLimiter | None = None,
):
    rate_limiter = issue_report_rate_limiter or IssueReportRateLimiter()

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

        if method == "POST" and path == "/issue-report":
            form = _read_post_form(environ)
            report_result = submit_issue_report(
                report_body=form.get("report_body", ""),
                issue_context=form.get("issue_context", ""),
                honeypot=form.get("website", ""),
                environ=environ,
                rate_limiter=rate_limiter,
            )
            _log_issue_report_action(
                web_action_log_path,
                environ,
                report_result,
            )
            body = _render_page(
                "",
                issue_report_result=report_result,
            )
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
                error_result = _build_error_ui_result(
                    {
                        "status": "internal_error",
                        "input": article_input,
                        "message": "An unexpected internal error occurred.",
                    },
                    reference_id,
                    article_input=article_input,
                    requested_format=requested_format,
                    path="/download",
                    action="download",
                )
                body = _render_page(
                    article_input,
                    error_result,
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
