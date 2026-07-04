"""Bounded Web issue report helpers: validation, rate limit, Slack webhook."""

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from uuid import uuid4


ISSUE_REPORT_MAX_BODY_CHARS = 1000
DEFAULT_ISSUE_REPORT_TIMEOUT_SECONDS = 10
DEFAULT_ISSUE_REPORT_RATE_LIMIT_SECONDS = 600
ISSUE_REPORT_SLACK_TITLE = "NicoArc web issue report"


def _env_truthy(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def issue_report_webhook_url() -> str:
    return os.environ.get("NICOARC_ISSUE_REPORT_SLACK_WEBHOOK_URL", "").strip()


def issue_report_enabled() -> bool:
    if not _env_truthy("NICOARC_ISSUE_REPORT_ENABLED", default=True):
        return False
    return bool(issue_report_webhook_url())


def issue_report_timeout_seconds() -> int:
    raw = os.environ.get("NICOARC_ISSUE_REPORT_TIMEOUT_SECONDS", "").strip()
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_ISSUE_REPORT_TIMEOUT_SECONDS
    return value if value > 0 else DEFAULT_ISSUE_REPORT_TIMEOUT_SECONDS


def issue_report_rate_limit_seconds() -> int:
    raw = os.environ.get("NICOARC_ISSUE_REPORT_RATE_LIMIT_SECONDS", "").strip()
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_ISSUE_REPORT_RATE_LIMIT_SECONDS
    return value if value > 0 else DEFAULT_ISSUE_REPORT_RATE_LIMIT_SECONDS


def normalize_report_body(raw: str) -> str:
    return (raw or "").strip()


def validate_report_body(body: str) -> str | None:
    """Return None when valid, else a short reason category."""
    text = normalize_report_body(body)
    if not text:
        return "empty_body"
    if len(text) > ISSUE_REPORT_MAX_BODY_CHARS:
        return "body_too_long"
    return None


def bounded_report_text(body: str) -> str:
    text = normalize_report_body(body)
    if len(text) <= ISSUE_REPORT_MAX_BODY_CHARS:
        return text
    return text[:ISSUE_REPORT_MAX_BODY_CHARS]


def bounded_issue_context(context: str | None) -> str:
    if not context:
        return ""
    text = context.strip()
    if len(text) > ISSUE_REPORT_MAX_BODY_CHARS:
        return text[:ISSUE_REPORT_MAX_BODY_CHARS]
    return text


def format_issue_context_block(
    *,
    reference_id: str,
    action: str,
    input_value: str,
    download_format: str,
    result: str,
    message: str,
    path: str,
    time_utc: str | None = None,
) -> str:
    ts = time_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "Issue report context:",
        f"reference_id: {reference_id}",
        f"time_utc: {ts}",
        f"action: {action}",
        f"input: {input_value}",
        f"download_format: {download_format}",
        f"result: {result}",
        f"message: {message}",
        f"path: {path}",
    ]
    return "\n".join(lines)


class IssueReportRateLimiter:
    """In-memory per-client-address submit throttle."""

    def __init__(self) -> None:
        self._last_submit: dict[str, float] = {}

    def allow(self, client_key: str, *, now: float, window_seconds: int) -> bool:
        last = self._last_submit.get(client_key)
        if last is not None and (now - last) < window_seconds:
            return False
        self._last_submit[client_key] = now
        return True


def client_address_key(environ: dict) -> str:
    forwarded = environ.get("HTTP_X_FORWARDED_FOR", "").strip()
    if forwarded:
        return forwarded.split(",", 1)[0].strip() or "unknown"
    return environ.get("REMOTE_ADDR", "unknown") or "unknown"


def build_slack_issue_report_message(
    *,
    reference_id: str,
    report_body: str,
    request_path: str,
    issue_context: str | None,
    visitor_hint: str,
    timestamp: str,
) -> str:
    lines = [
        ISSUE_REPORT_SLACK_TITLE,
        f"reference_id: {reference_id}",
        f"timestamp: {timestamp}",
        f"path: {request_path}",
        f"visitor_hint: {visitor_hint}",
        "",
        "report_body:",
        bounded_report_text(report_body),
    ]
    ctx = bounded_issue_context(issue_context)
    if ctx:
        lines.extend(["", "issue_context:", ctx])
    return "\n".join(lines)


def send_slack_webhook_message(
    webhook_url: str,
    text: str,
    *,
    timeout_seconds: int,
) -> None:
    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        if response.status >= 400:
            raise RuntimeError(f"slack_status={response.status}")


def submit_issue_report(
    *,
    report_body: str,
    issue_context: str | None,
    honeypot: str,
    environ: dict,
    rate_limiter: IssueReportRateLimiter,
    send_fn=None,
    now_provider=None,
) -> dict:
    """Validate and optionally send an issue report.

    Returns a dict with keys: ok, reference_id, outcome, reason, message.
    """
    if send_fn is None:
        send_fn = send_slack_webhook_message
    reference_id = uuid4().hex
    timestamp = datetime.now(timezone.utc).isoformat()
    path = environ.get("PATH_INFO", "/") or "/"

    if honeypot.strip():
        return {
            "ok": True,
            "reference_id": reference_id,
            "outcome": "rejected",
            "reason": "honeypot",
            "message": "Thank you.",
            "report_length": 0,
            "path": path,
            "timestamp": timestamp,
        }

    if not issue_report_enabled():
        return {
            "ok": False,
            "reference_id": reference_id,
            "outcome": "disabled",
            "reason": "disabled",
            "message": (
                "Issue reporting is not available right now. "
                "Please try again later."
            ),
            "report_length": 0,
            "path": path,
            "timestamp": timestamp,
        }

    validation_error = validate_report_body(report_body)
    if validation_error:
        messages = {
            "empty_body": "Please enter a report message.",
            "body_too_long": (
                f"Report is too long (max {ISSUE_REPORT_MAX_BODY_CHARS} "
                "characters)."
            ),
        }
        return {
            "ok": False,
            "reference_id": reference_id,
            "outcome": "rejected",
            "reason": validation_error,
            "message": messages.get(
                validation_error,
                "Report could not be accepted.",
            ),
            "report_length": len(normalize_report_body(report_body)),
            "path": path,
            "timestamp": timestamp,
        }

    import time

    now = time.time() if now_provider is None else now_provider()
    client_key = client_address_key(environ)
    if not rate_limiter.allow(
        client_key,
        now=now,
        window_seconds=issue_report_rate_limit_seconds(),
    ):
        return {
            "ok": False,
            "reference_id": reference_id,
            "outcome": "rejected",
            "reason": "rate_limited",
            "message": (
                "Please wait before submitting another report."
            ),
            "report_length": len(normalize_report_body(report_body)),
            "path": path,
            "timestamp": timestamp,
        }

    body_text = bounded_report_text(report_body)
    slack_text = build_slack_issue_report_message(
        reference_id=reference_id,
        report_body=body_text,
        request_path=path,
        issue_context=issue_context,
        visitor_hint=_bounded_visitor_hint(environ),
        timestamp=timestamp,
    )
    try:
        send_fn(
            issue_report_webhook_url(),
            slack_text,
            timeout_seconds=issue_report_timeout_seconds(),
        )
    except Exception:
        return {
            "ok": False,
            "reference_id": reference_id,
            "outcome": "failed",
            "reason": "slack_send_failed",
            "message": (
                "Could not send the report right now. "
                f"Reference ID: {reference_id}"
            ),
            "report_length": len(body_text),
            "path": path,
            "timestamp": timestamp,
        }

    return {
        "ok": True,
        "reference_id": reference_id,
        "outcome": "sent",
        "reason": "sent",
        "message": (
            "Report received. Thank you. "
            f"Reference ID: {reference_id}"
        ),
        "report_length": len(body_text),
        "path": path,
        "timestamp": timestamp,
    }


def _bounded_visitor_hint(environ: dict) -> str:
    addr = client_address_key(environ)
    user_agent = " ".join(environ.get("HTTP_USER_AGENT", "unknown").split())
    if len(user_agent) > 80:
        user_agent = f"{user_agent[:77]}..."
    return f"addr={addr} ua={user_agent}"
