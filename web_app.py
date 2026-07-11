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
    return (
        '<div class="result-detail">'
        f'<span class="detail-label">{escape(label)}</span>'
        f'<span class="detail-value">{escape(value)}</span>'
        "</div>"
    )


def _render_format_selector(selected_format: str) -> str:
    lines = [
        '<fieldset class="format-selector">',
        '<legend class="section-label">Download format</legend>',
        '<div class="format-option-row">',
    ]
    for download_format in DOWNLOAD_FORMATS:
        checked = " checked" if download_format == selected_format else ""
        lines.append(
            (
                '<label class="format-option">'
                f'<input type="radio" name="requested_format" '
                f'value="{escape(download_format)}"{checked}>'
                '<span class="format-option-text">'
                f'{escape(_display_format_name(download_format))}'
                "</span>"
                '</label>'
            )
        )
    lines.append("</div>")
    lines.append("</fieldset>")
    return "".join(lines)


def _render_message_area(
    result: dict | None,
    download_query: str | None = None,
) -> str:
    if result is None:
        return (
            '<section class="message-area empty">'
            '<div class="section-heading">'
            f"<h2>{escape(UI_TEXTS['result_heading'])}</h2>"
            "</div>"
            '<div class="message-body">'
            f"<p>{escape(UI_TEXTS['empty_message'])}</p>"
            "</div>"
            "</section>"
        )

    lines = [
        f'<section class="message-area {escape(result["status"])}">',
        '<div class="section-heading">',
        f"<h2>{escape(UI_TEXTS['result_heading'])}</h2>",
        "</div>",
        '<div class="message-body">',
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

    lines.append("</div>")
    lines.append("</section>")
    return "".join(lines)


def _render_issue_context_block(issue_context_text: str) -> str:
    context_id = f"issue-context-{uuid4().hex[:8]}"
    safe_text = escape(issue_context_text)
    return (
        '<div class="issue-context-block">'
        '<div class="issue-context-header">'
        '<p class="issue-context-heading">Issue report context:</p>'
        f'<button type="button" class="copy-context-btn button-secondary" '
        f'data-copy-target="{context_id}">Copy</button>'
        "</div>"
        f'<textarea id="{context_id}" class="issue-context-text" '
        'readonly rows="8">'
        f"{safe_text}"
        "</textarea>"
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
            '<p class="issue-report-note utility-note">'
            "Send a short problem report to the site operator."
            "</p>"
        )
    else:
        availability_note = (
            '<p class="issue-report-note utility-note">'
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
        '<summary class="section-heading-inline">Report a problem</summary>'
        '<div class="issue-report-body">'
        f"{availability_note}"
        f"{status_html}"
        '<form method="post" action="/issue-report" class="issue-report-form">'
        '<label for="report_body" class="section-label">Describe the problem</label>'
        '<textarea id="report_body" name="report_body" rows="5" '
        'maxlength="1000"></textarea>'
        f"{context_hidden}"
        '<label class="hp-field" aria-hidden="true">'
        'Website<input type="text" name="website" tabindex="-1" '
        'autocomplete="off">'
        "</label>"
        '<div class="form-actions">'
        '<button type="submit" class="button-primary">Send report</button>'
        "</div>"
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
    :root {{
      color-scheme: light;
            --bg: #f3ece0;
            --bg-accent: #ebe0d0;
            --panel: #fffaf2;
            --panel-strong: #fffdf8;
            --ink: #1f2430;
            --ink-soft: #3f4a58;
      --accent: #0f766e;
            --accent-deep: #0d5f59;
            --accent-soft: #d8ece7;
      --accent-disabled: #6b8f8b;
            --border: #d7ccb8;
            --border-strong: #c7b69c;
      --muted: #6b7280;
      --saved: #14532d;
      --registered: #92400e;
      --error: #991b1b;
            --shadow: 0 16px 34px rgba(43, 35, 24, 0.08);
            --shadow-soft: 0 8px 18px rgba(43, 35, 24, 0.05);
            --radius-lg: 20px;
            --radius-md: 14px;
            --radius-sm: 10px;
            --focus-ring: 0 0 0 3px rgba(15, 118, 110, 0.18);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
            font-family: Georgia, \"Times New Roman\", serif;
      color: var(--ink);
            background: linear-gradient(180deg, var(--bg-accent) 0%, var(--bg) 100%);
    }}
        a {{ color: var(--accent-deep); }}
        a:hover {{ color: var(--accent); }}
        a:focus-visible,
        button:focus-visible,
        input:focus-visible,
        textarea:focus-visible,
        select:focus-visible,
        summary:focus-visible {{
            outline: none;
            box-shadow: var(--focus-ring);
        }}
        @media (prefers-reduced-motion: no-preference) {{
            a, button, input, textarea, select, summary, tr, .panel {{
                transition:
                    background-color 120ms ease,
                    border-color 120ms ease,
                    box-shadow 120ms ease,
                    color 120ms ease;
            }}
        }}
    main {{
            max-width: 860px;
      margin: 0 auto;
            padding: 32px 20px 64px;
    }}
        .page-shell {{
            display: grid;
            gap: 18px;
        }}
        .page-header {{
            display: flex;
            justify-content: space-between;
            align-items: end;
            gap: 16px;
            padding: 0 4px;
        }}
        .page-header-copy {{
            min-width: 0;
        }}
        .eyebrow {{
            margin: 0 0 6px;
            color: var(--accent-deep);
            font-size: 0.82rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}
        .page-header-actions {{
            display: flex;
            flex-wrap: wrap;
            justify-content: flex-end;
            gap: 10px;
        }}
        .panel {{
            background: linear-gradient(
                180deg,
                var(--panel-strong) 0%,
                var(--panel) 100%
            );
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            padding: 24px;
            box-shadow: var(--shadow);
    }}
    h1 {{
            margin: 0;
            font-size: clamp(2rem, 4vw, 2.8rem);
            line-height: 1.04;
            letter-spacing: -0.02em;
    }}
        h2 {{
            margin: 0;
            font-size: 1.15rem;
            line-height: 1.2;
        }}
        p {{ line-height: 1.58; }}
        .lede {{
            max-width: 62ch;
            color: var(--ink-soft);
            margin: 12px 0 0;
        }}
        form {{ display: grid; gap: 12px; }}
        label {{ font-weight: 700; }}
        .section-label {{
            color: var(--ink);
            font-size: 0.96rem;
        }}
    input[type=\"text\"] {{
      width: 100%;
            min-height: 48px;
            padding: 13px 15px;
            border-radius: var(--radius-sm);
            border: 1px solid var(--border-strong);
      background: #fff;
      font: inherit;
            color: var(--ink);
    }}
        input[type=\"text\"]::placeholder,
        textarea::placeholder {{
            color: #808895;
        }}
        button {{
      width: fit-content;
            min-height: 44px;
            padding: 10px 18px;
            border: 1px solid transparent;
            border-radius: 999px;
      background: var(--accent);
      color: #fff;
            font: inherit;
            font-weight: 700;
      cursor: pointer;
            box-shadow: var(--shadow-soft);
    }}
        button:hover {{ background: var(--accent-deep); }}
    button[disabled] {{
      background: var(--accent-disabled);
            box-shadow: none;
      cursor: progress;
    }}
        .button-primary {{
            background: var(--accent);
            color: #fff;
        }}
        .button-secondary,
        .nav-link-button,
        .list-btn {{
            border-color: var(--border-strong);
            background: transparent;
            color: var(--ink);
            box-shadow: none;
        }}
        .button-secondary:hover,
        .nav-link-button:hover,
        .list-btn:hover {{
            background: rgba(15, 118, 110, 0.08);
            color: var(--ink);
        }}
        .button-quiet {{
            border-color: var(--border);
            background: #fff;
            color: var(--ink-soft);
            box-shadow: none;
        }}
    .busy-message {{
      margin: 0;
      color: var(--muted);
      font-size: 0.95rem;
    }}
        .utility-card {{
            display: grid;
            gap: 18px;
        }}
        .action-form {{
            display: grid;
            gap: 16px;
            padding: 18px;
            border: 1px solid rgba(199, 182, 156, 0.72);
            border-radius: var(--radius-md);
            background: rgba(255, 255, 255, 0.58);
        }}
        .action-row {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            align-items: center;
        }}
        .format-selector {{
            margin: 0;
            padding: 14px 16px 12px;
            border: 1px solid var(--border);
            border-radius: var(--radius-md);
            background: rgba(255, 255, 255, 0.72);
        }}
        .format-selector legend {{
            padding: 0 6px;
        }}
        .format-option-row {{
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }}
        .format-option {{
            display: inline-flex;
            gap: 8px;
            align-items: center;
            padding: 8px 12px;
            border: 1px solid transparent;
            border-radius: 999px;
            background: rgba(15, 118, 110, 0.05);
            font-weight: 400;
            cursor: pointer;
        }}
        .format-option:hover {{
            border-color: rgba(15, 118, 110, 0.22);
            background: rgba(15, 118, 110, 0.09);
        }}
        .format-option input {{ margin: 0; }}
        .format-option-text {{ line-height: 1.2; }}
    .message-area {{
            margin-top: 0;
            padding: 18px;
            border-radius: var(--radius-md);
      border: 1px solid var(--border);
            background: rgba(255, 255, 255, 0.84);
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.6);
    }}
        .message-area p {{
            overflow-wrap: anywhere;
            word-break: break-word;
        }}
        .section-heading {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 12px;
            margin-bottom: 12px;
            padding-bottom: 10px;
            border-bottom: 1px solid rgba(199, 182, 156, 0.52);
        }}
        .section-heading-inline {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }}
        .message-body {{
            display: grid;
            gap: 10px;
        }}
        .message-area h2 {{ font-size: 1.08rem; }}
    .message-area.empty {{ color: var(--muted); }}
        .message-area.saved {{ border-left: 5px solid rgba(20, 83, 45, 0.62); }}
        .message-area.registered {{
            border-left: 5px solid rgba(146, 64, 14, 0.58);
        }}
        .message-area.error {{ border-left: 5px solid rgba(153, 27, 27, 0.58); }}
        .message-area.saved .status-line {{ color: var(--saved); }}
        .message-area.registered .status-line {{ color: var(--registered); }}
        .message-area.error .status-line {{ color: var(--error); }}
        .status-line {{
            margin: 0;
            font-weight: 700;
        }}
        .result-detail {{
            display: grid;
            grid-template-columns: minmax(10rem, 12rem) minmax(0, 1fr);
            gap: 8px 12px;
            align-items: start;
            padding: 9px 0;
            border-top: 1px solid rgba(199, 182, 156, 0.34);
        }}
        .detail-label {{
            color: var(--ink-soft);
            font-weight: 700;
        }}
        .detail-value {{
            min-width: 0;
            color: var(--ink);
        }}
        .followup-note,
        .utility-note {{
            margin: 0;
            color: var(--muted);
            font-size: 0.94rem;
        }}
        .download-frame {{ display: none; width: 0; height: 0; border: 0; }}
    .issue-context-block {{
            margin-top: 8px;
            padding: 14px;
            border: 1px dashed var(--border-strong);
            border-radius: var(--radius-sm);
            background: rgba(255, 255, 255, 0.76);
    }}
        .issue-context-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 10px;
            margin-bottom: 8px;
        }}
    .issue-context-heading {{
            margin: 0;
      font-weight: 700;
    }}
    .issue-context-text {{
      width: 100%;
      font-family: ui-monospace, monospace;
      font-size: 0.85rem;
      padding: 10px 12px;
            border-radius: var(--radius-sm);
            border: 1px solid var(--border-strong);
      background: #fff;
      resize: vertical;
    }}
    .issue-report {{
            border: 1px solid var(--border);
            border-radius: var(--radius-md);
            background: rgba(255, 255, 255, 0.66);
            padding: 14px 16px;
    }}
    .issue-report summary {{
      cursor: pointer;
      font-weight: 700;
            color: var(--ink);
            list-style: none;
    }}
        .issue-report summary::-webkit-details-marker {{ display: none; }}
    .issue-report-body {{
      margin-top: 12px;
      display: grid;
            gap: 12px;
    }}
    .issue-report-status.ok {{ color: var(--saved); }}
    .issue-report-status.error {{ color: var(--error); }}
        .issue-report-form {{ display: grid; gap: 12px; }}
        .issue-report textarea {{
      width: 100%;
      min-height: 120px;
      padding: 12px 14px;
            border-radius: var(--radius-sm);
            border: 1px solid var(--border-strong);
      background: #fff;
      font: inherit;
      resize: vertical;
    }}
        .form-actions {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            align-items: center;
        }}
    .hp-field {{
      position: absolute;
      left: -10000px;
      width: 1px;
      height: 1px;
      overflow: hidden;
    }}
        .nav-link-button,
        .list-btn {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 40px;
            padding: 8px 14px;
            border-radius: 999px;
            text-decoration: none;
            font-size: 0.94rem;
            font-weight: 700;
        }}
        @media (max-width: 720px) {{
            .page-header {{
                align-items: start;
                flex-direction: column;
            }}
            .page-header-actions {{ justify-content: start; }}
            .result-detail {{
                grid-template-columns: 1fr;
                gap: 4px;
            }}
            .issue-context-header {{
                align-items: stretch;
                flex-direction: column;
            }}
        }}
    @media (max-width: 640px) {{
      main {{ padding: 24px 14px 36px; }}
            .panel {{ padding: 18px; }}
            .action-row,
            .form-actions,
            .page-header-actions,
            .format-option-row {{
                align-items: stretch;
                flex-direction: column;
            }}
            button,
            .nav-link-button,
            .list-btn {{ width: 100%; }}
    }}
  </style>
</head>
<body>
  <main>
        <div class=\"page-shell\">
            <header class=\"page-header\">
                <div class=\"page-header-copy\">
                    <p class=\"eyebrow\">Heritage Utility Refresh</p>
                    <h1>{escape(UI_TEXTS['heading'])}</h1>
                    <p class=\"lede\">{escape(UI_TEXTS['lede'])}</p>
                </div>
                <div class=\"page-header-actions\">
                    <a href=\"/registered\" target=\"_blank\" class=\"list-btn\">
                        登録済み記事一覧
                    </a>
                </div>
            </header>
            <section class=\"panel utility-card\">
                <form
                    method=\"post\"
                    action=\"/\"
                    class=\"action-form\"
                    data-archive-check-form
                >
                    <label for=\"article_input\" class=\"section-label\">
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
                    {_render_format_selector(selected_format)}
                    <div class="action-row">
                        <button
                            type="submit"
                            class="button-primary"
                            data-submit-button
                        >
                            {escape(UI_TEXTS['submit_label'])}
                        </button>
                        <p
                            class="busy-message"
                            data-busy-message
                            hidden
                            aria-live="polite"
                        >
                            {escape(UI_TEXTS['busy_message'].format(format_name=format_name))}
                        </p>
                    </div>
                </form>
                {message_area}
                {issue_report_section}
            </section>
        </div>
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
        :root {{
            color-scheme: light;
            --bg: #f3ece0;
            --bg-accent: #ebe0d0;
            --panel: #fffaf2;
            --panel-strong: #fffdf8;
            --ink: #1f2430;
            --ink-soft: #445062;
            --accent: #0f766e;
            --accent-deep: #0d5f59;
            --border: #d7ccb8;
            --border-strong: #c7b69c;
            --muted: #6b7280;
            --shadow: 0 16px 34px rgba(43, 35, 24, 0.08);
            --shadow-soft: 0 8px 18px rgba(43, 35, 24, 0.05);
            --radius-lg: 20px;
            --radius-md: 14px;
            --radius-sm: 10px;
            --focus-ring: 0 0 0 3px rgba(15, 118, 110, 0.18);
        }}
        * {{ box-sizing: border-box; }}
    body {{
      font-family: Georgia, serif;
      margin: 0;
            padding: 32px 20px 48px;
            background: linear-gradient(180deg, var(--bg-accent) 0%, var(--bg) 100%);
            color: var(--ink);
    }}
        a {{ color: var(--accent-deep); }}
        a:hover {{ color: var(--accent); }}
        a:focus-visible,
        button:focus-visible,
        input:focus-visible,
        select:focus-visible {{
            outline: none;
            box-shadow: var(--focus-ring);
        }}
        @media (prefers-reduced-motion: no-preference) {{
            a, button, input, select, tr, .panel {{
                transition:
                    background-color 120ms ease,
                    border-color 120ms ease,
                    box-shadow 120ms ease,
                    color 120ms ease;
            }}
        }}
        .page-shell {{
            max-width: 1360px;
            margin: 0 auto;
            display: grid;
            gap: 18px;
        }}
        .page-header {{
            display: flex;
            justify-content: space-between;
            align-items: end;
            gap: 16px;
            padding: 0 4px;
        }}
        .page-header-copy {{ min-width: 0; }}
        .eyebrow {{
            margin: 0 0 6px;
            color: var(--accent-deep);
            font-size: 0.82rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}
        h1 {{
            margin: 0;
            font-size: clamp(1.9rem, 4vw, 2.6rem);
            line-height: 1.05;
            letter-spacing: -0.02em;
        }}
        .meta {{
            color: var(--muted);
            margin: 12px 0 0;
            font-size: 0.95rem;
        }}
        .page-header-actions {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            justify-content: flex-end;
        }}
        .nav-link-button {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 40px;
            padding: 8px 14px;
            border: 1px solid var(--border-strong);
            border-radius: 999px;
            background: transparent;
            color: var(--ink);
            text-decoration: none;
            font-size: 0.94rem;
            font-weight: 700;
        }}
        .nav-link-button:hover {{
            background: rgba(15, 118, 110, 0.08);
            color: var(--ink);
        }}
        .panel {{
            background: linear-gradient(
                180deg,
                var(--panel-strong) 0%,
                var(--panel) 100%
            );
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            padding: 20px;
            box-shadow: var(--shadow);
        }}
    .controls {{
            display: grid;
            gap: 14px;
    }}
        .toolbar-row {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            align-items: center;
            justify-content: space-between;
        }}
        .toolbar-group {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            align-items: center;
        }}
    .search-form {{
      display: flex;
            flex-wrap: wrap;
            gap: 8px;
      align-items: center;
    }}
    .search-form input[type="text"] {{
            min-height: 42px;
            padding: 8px 12px;
            border: 1px solid var(--border-strong);
            border-radius: var(--radius-sm);
      font: inherit;
            min-width: 280px;
            width: min(42vw, 30rem);
            background: #fff;
            color: var(--ink);
    }}
    button, .btn {{
            min-height: 40px;
            padding: 8px 14px;
            border: 1px solid transparent;
            border-radius: 999px;
            background: var(--accent);
      color: #fff;
      font: inherit;
            font-weight: 700;
      cursor: pointer;
      text-decoration: none;
            box-shadow: var(--shadow-soft);
    }}
        button:hover, .btn:hover {{ background: var(--accent-deep); }}
    select.per-page {{
            min-height: 40px;
            padding: 6px 12px;
            border: 1px solid var(--border-strong);
            border-radius: var(--radius-sm);
            background: #fff;
      font: inherit;
    }}
        .csv-link, .aux-link {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 40px;
            padding: 8px 12px;
            border: 1px solid var(--border-strong);
            border-radius: 999px;
            color: var(--ink);
      text-decoration: none;
            font-size: 0.92rem;
            font-weight: 700;
      background: transparent;
    }}
        .csv-link:hover, .aux-link:hover {{
            background: rgba(15, 118, 110, 0.08);
            color: var(--ink);
        }}
        .table-panel {{
            padding: 0;
            overflow: hidden;
        }}
        .table-scroll {{
            width: 100%;
            overflow-x: auto;
            overflow-y: hidden;
            border-radius: var(--radius-lg);
        }}
        table {{
      border-collapse: collapse;
      width: 100%;
            min-width: 1080px;
            table-layout: fixed;
            background: rgba(255, 255, 255, 0.92);
    }}
    th, td {{
      padding: 8px 12px;
            border-bottom: 1px solid #e8dfc8;
            vertical-align: middle;
    }}
        thead th {{
            position: sticky;
            top: 0;
            z-index: 1;
            background: #f3ead9;
            font-weight: 700;
            color: var(--ink);
            box-shadow: inset 0 -1px 0 rgba(199, 182, 156, 0.8);
        }}
        th.align-left, td.align-left {{ text-align: left; }}
        th.align-center, td.align-center {{ text-align: center; }}
        th.align-right, td.align-right {{ text-align: right; }}
    th a.sort-link {{
            color: var(--ink);
      text-decoration: none;
      white-space: nowrap;
    }}
    th a.sort-link:hover {{ text-decoration: underline; }}
    tr:last-child td {{ border-bottom: none; }}
        tbody tr:hover td {{ background: rgba(15, 118, 110, 0.045); }}
        tr.not-scraped td {{ background: #fff8e6; }}
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
            margin-top: 0;
    }}
        .pagination.top {{ margin: 0; }}
        .pagination.bottom {{ margin: 0; }}
    .page-btn {{
            padding: 7px 12px;
            border: 1px solid var(--border-strong);
            border-radius: 999px;
            color: var(--ink);
            background: #fff;
      text-decoration: none;
      font-size: 0.9rem;
            font-weight: 700;
    }}
    .page-btn.disabled {{
            color: #aaa;
            border-color: #e0d8c8;
            background: #f5f1e8;
      pointer-events: none;
    }}
    .page-info {{ font-size: 0.9rem; color: #6b7280; }}
        @media (max-width: 960px) {{
            .toolbar-row {{
                align-items: stretch;
                flex-direction: column;
            }}
            .toolbar-group {{ width: 100%; }}
        }}
        @media (max-width: 720px) {{
            body {{ padding: 24px 14px 36px; }}
            .page-header {{
                align-items: start;
                flex-direction: column;
            }}
            .page-header-actions {{ justify-content: start; }}
            .search-form,
            .toolbar-group {{
                width: 100%;
            }}
            .search-form input[type="text"] {{
                width: 100%;
                min-width: 0;
            }}
        }}
  </style>
</head>
<body>
    <div class="page-shell">
        <header class="page-header">
            <div class="page-header-copy">
                <p class="eyebrow">Heritage Utility Refresh</p>
                <h1>Registered Articles</h1>
                <p class="meta">
                    {showing} &mdash;
                    <a href="/" target="_self">&larr; Top</a>
                </p>
            </div>
            <div class="page-header-actions">
                <a href="/" target="_self" class="nav-link-button">Top</a>
            </div>
        </header>
        <section class="panel controls">
            <div class="toolbar-row">
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
                <div class="toolbar-group">
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
            </div>
            {top_pagination.replace('class="pagination"', 'class="pagination top"')}
        </section>
        <section class="panel table-panel">
            <div class="table-scroll">
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
        <section class="panel controls">
            {
                bottom_pagination.replace(
                    'class="pagination"',
                    'class="pagination bottom"',
                )
            }
        </section>
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
