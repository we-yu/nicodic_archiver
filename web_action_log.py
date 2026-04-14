import os
import time
from urllib.parse import unquote


DEFAULT_WEB_ACTION_LOG_PATH = os.environ.get(
    "WEB_ACTION_LOG_PATH",
    "data/web_action.log",
)


def _now_iso_z() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _clean_human_title(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if "%" in cleaned:
        cleaned = unquote(cleaned)
    return cleaned


def _format_action_block(fields: dict) -> str:
    action_id = fields.get("action_id", "")
    lines = [
        "",
        f"START web_action {action_id}",
        f"timestamp: {fields.get('timestamp', '')}",
        f"action_kind: {fields.get('action_kind', '')}",
        f"visitor_hint: {fields.get('visitor_hint', '')}",
        f"input_value: {fields.get('input_value', '')}",
        f"requested_format: {fields.get('requested_format', '')}",
        f"result_status: {fields.get('result_status', '')}",
        f"resolved_title: {fields.get('resolved_title', '')}",
        f"resolved_article_id: {fields.get('resolved_article_id', '')}",
        f"resolved_article_type: {fields.get('resolved_article_type', '')}",
        f"resolved_canonical_url: {fields.get('resolved_canonical_url', '')}",
    ]

    error_code = fields.get("error_code")
    error_detail = fields.get("error_detail")
    if error_code:
        lines.append(f"error_code: {error_code}")
    if error_detail:
        lines.append(f"error_detail: {error_detail}")

    lines.append(f"END web_action {action_id}")
    lines.append("")
    lines.append("")
    return "\n".join(lines)


def append_web_action_log(
    fields: dict,
    log_path: str = DEFAULT_WEB_ACTION_LOG_PATH,
) -> None:
    fields = dict(fields)
    fields.setdefault("timestamp", _now_iso_z())
    fields["resolved_title"] = _clean_human_title(fields.get("resolved_title")) or ""
    content = _format_action_block(fields)

    parent_dir = os.path.dirname(log_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(content)
