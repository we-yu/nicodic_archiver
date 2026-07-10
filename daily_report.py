"""Once-per-UTC-day Slack runtime report from batch logs + targets."""

from __future__ import annotations

import json
import os
import re
import warnings
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from issue_report import (
    issue_report_timeout_seconds,
    issue_report_webhook_url,
    send_slack_webhook_message,
)
from storage import open_readonly_db
from target_addition_log import read_target_addition_events


DEFAULT_DAILY_REPORT_STATE_PATH = "data/daily_report_state.json"
MAX_DISPLAYED_TARGETS = 10
MAX_TITLE_CHARS = 80

SOURCE_DISPLAY_LABELS = {
    "delete_feeder": "Delete Feeder",
    "hot_word": "HOT Word",
    "web_user": "Web input",
    "operator": "Operator",
    "import": "Import",
    "unknown": "Unknown source",
}

_KV_RE = re.compile(r"^\s*([A-Za-z0-9_]+)=(.*)\s*$")


def daily_report_enabled() -> bool:
    raw = os.environ.get("NICOARC_DAILY_REPORT_ENABLED", "").strip()
    if not raw:
        return False
    return raw.lower() not in {"0", "false", "no", "off"}


def daily_report_state_path(
    state_path: str | Path | None = None,
) -> Path:
    if state_path is not None:
        return Path(state_path)
    raw = os.environ.get("DAILY_REPORT_STATE_PATH", "").strip()
    if raw:
        return Path(raw)
    return Path(DEFAULT_DAILY_REPORT_STATE_PATH)


def report_date_for_attempt(now: datetime | None = None) -> date:
    """UTC calendar day before the attempt day."""
    ts = now or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts.date() - timedelta(days=1)


def report_date_bounds(report_day: date) -> tuple[datetime, datetime]:
    start = datetime(
        report_day.year,
        report_day.month,
        report_day.day,
        tzinfo=timezone.utc,
    )
    end = start + timedelta(days=1)
    return start, end


def _parse_iso_utc(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def _parse_sqlite_utc(value: str) -> datetime | None:
    """Parse target.created_at (SQLite CURRENT_TIMESTAMP or ISO)."""
    text = (value or "").strip()
    if not text:
        return None
    iso = _parse_iso_utc(text)
    if iso is not None:
        return iso
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.replace(tzinfo=timezone.utc)
    return None


def _parse_kv_block(lines: list[str], start_index: int) -> dict[str, str]:
    fields: dict[str, str] = {}
    index = start_index + 1
    while index < len(lines):
        line = lines[index]
        if not line.startswith(" ") and not line.startswith("\t"):
            break
        match = _KV_RE.match(line)
        if match:
            fields[match.group(1)] = match.group(2)
        index += 1
    return fields


def parse_completed_batch_log(text: str) -> dict | None:
    """Return metrics for one completed batch log, or None if incomplete."""
    lines = text.splitlines()
    run_end: dict[str, str] | None = None
    digest: dict[str, str] | None = None
    for index, line in enumerate(lines):
        if line.strip() == "BATCH_RUN_END":
            run_end = _parse_kv_block(lines, index)
        elif line.strip() == "BATCH_DIGEST":
            digest = _parse_kv_block(lines, index)
    if not run_end or not digest:
        return None
    ended_at = _parse_iso_utc(run_end.get("ended_at", ""))
    if ended_at is None:
        return None
    try:
        processed = int(run_end.get("processed_targets", "0"))
    except ValueError:
        processed = 0
    metrics = {"H": 0, "W": 0, "F": 0, "NEW": 0}
    for key in metrics:
        raw = digest.get(key, "0")
        try:
            metrics[key] = int(raw)
        except ValueError:
            metrics[key] = 0
    return {
        "ended_at": ended_at,
        "processed_targets": max(processed, 0),
        "hit": metrics["H"],
        "warn": metrics["W"],
        "fail": metrics["F"],
        "new_responses": metrics["NEW"],
    }


def aggregate_batch_logs_for_day(
    batch_log_dir: str | Path,
    report_day: date,
) -> dict:
    start, end = report_date_bounds(report_day)
    totals = {
        "completed_runs": 0,
        "processed_targets": 0,
        "hit": 0,
        "warn": 0,
        "fail": 0,
        "new_responses": 0,
    }
    directory = Path(batch_log_dir)
    if not directory.is_dir():
        return totals
    for path in sorted(directory.glob("batch_*.log")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        try:
            parsed = parse_completed_batch_log(text)
        except Exception:
            continue
        if parsed is None:
            continue
        ended_at = parsed["ended_at"]
        if ended_at < start or ended_at >= end:
            continue
        totals["completed_runs"] += 1
        totals["processed_targets"] += parsed["processed_targets"]
        totals["hit"] += parsed["hit"]
        totals["warn"] += parsed["warn"]
        totals["fail"] += parsed["fail"]
        totals["new_responses"] += parsed["new_responses"]
    return totals


def _display_title(entry: dict) -> str:
    title = (entry.get("title") or "").strip()
    if title:
        return title
    article_id = (entry.get("article_id") or "").strip()
    if article_id:
        return article_id
    identity = (entry.get("canonical_url") or "").strip()
    if identity:
        # Prefer last path segment; never show a full URL in Slack.
        segment = identity.rstrip("/").rsplit("/", 1)[-1]
        return segment or "unknown"
    return "unknown"


def _truncate_title(title: str) -> str:
    text = " ".join(title.split())
    if len(text) <= MAX_TITLE_CHARS:
        return text
    return text[: MAX_TITLE_CHARS - 3] + "..."


def query_targets_created_on_day(
    target_db_path: str | Path,
    report_day: date,
) -> list[dict]:
    """Read-only query of targets first created in the report UTC day."""
    start, end = report_date_bounds(report_day)
    conn = open_readonly_db(str(target_db_path))
    if conn is None:
        return []
    rows: list[dict] = []
    try:
        cur = conn.execute(
            """
            SELECT article_id, article_type, canonical_url, title, created_at
            FROM target
            ORDER BY created_at ASC, article_id ASC, article_type ASC
            """
        )
        for row in cur.fetchall():
            article_id, article_type, canonical_url, title, created_at = row
            created = _parse_sqlite_utc(created_at or "")
            if created is None:
                continue
            if created < start or created >= end:
                continue
            rows.append(
                {
                    "article_id": str(article_id),
                    "article_type": str(article_type or "a"),
                    "canonical_url": canonical_url or "",
                    "title": title or "",
                    "created_at": created_at,
                }
            )
    except Exception:
        return []
    finally:
        conn.close()
    return rows


def _source_event_maps(
    report_day: date,
    *,
    log_dir: str | Path | None = None,
) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    """Build typed and untyped source maps (first valid event wins).

    Typed events (with article_type) key on (article_id, article_type).
    Untyped events key on article_id only for backward-compatible fallback.
    """
    by_composite: dict[tuple[str, str], str] = {}
    by_article_id_only: dict[str, str] = {}
    for event in read_target_addition_events(report_day, log_dir=log_dir):
        article_id = event["article_id"]
        article_type = event.get("article_type")
        source = event["source"]
        if article_type:
            key = (article_id, str(article_type))
            if key not in by_composite:
                by_composite[key] = source
            continue
        if article_id not in by_article_id_only:
            by_article_id_only[article_id] = source
    return by_composite, by_article_id_only


def _resolve_target_source(
    target: dict,
    by_composite: dict[tuple[str, str], str],
    by_article_id_only: dict[str, str],
) -> str:
    article_id = str(target.get("article_id") or "").strip()
    if not article_id:
        return "unknown"
    article_type = str(target.get("article_type") or "").strip()
    if article_type:
        typed = by_composite.get((article_id, article_type))
        if typed is not None:
            return typed
    untyped = by_article_id_only.get(article_id)
    if untyped is not None:
        return untyped
    return "unknown"


def attach_sources_to_targets(
    targets: list[dict],
    report_day: date,
    *,
    log_dir: str | Path | None = None,
) -> list[dict]:
    by_composite, by_article_id_only = _source_event_maps(
        report_day,
        log_dir=log_dir,
    )
    enriched: list[dict] = []
    for target in targets:
        source = _resolve_target_source(
            target,
            by_composite,
            by_article_id_only,
        )
        enriched.append({**target, "source": source})
    return enriched


def _format_int(value: int) -> str:
    return f"{int(value):,}"


def _source_breakdown(targets: list[dict]) -> str:
    counts = {
        "delete_feeder": 0,
        "hot_word": 0,
        "web_user": 0,
        "other": 0,
    }
    for target in targets:
        source = target.get("source", "unknown")
        if source == "delete_feeder":
            counts["delete_feeder"] += 1
        elif source == "hot_word":
            counts["hot_word"] += 1
        elif source == "web_user":
            counts["web_user"] += 1
        else:
            counts["other"] += 1
    parts: list[str] = []
    if counts["delete_feeder"]:
        parts.append(f"Delete {counts['delete_feeder']}")
    if counts["hot_word"]:
        parts.append(f"HOT {counts['hot_word']}")
    if counts["web_user"]:
        parts.append(f"Web {counts['web_user']}")
    if counts["other"]:
        parts.append(f"Other {counts['other']}")
    return " / ".join(parts)


def format_daily_report_message(
    report_day: date,
    metrics: dict,
    targets: list[dict],
) -> str:
    runs = metrics.get("completed_runs", 0)
    processed = metrics.get("processed_targets", 0)
    new_responses = metrics.get("new_responses", 0)
    hit = metrics.get("hit", 0)
    warn = metrics.get("warn", 0)
    fail = metrics.get("fail", 0)
    lines = [
        f"📊 NicoArc daily report — {report_day.isoformat()} UTC",
        (
            f"Runs {_format_int(runs)} | "
            f"processed {_format_int(processed)} | "
            f"new responses {_format_int(new_responses)} | "
            f"HIT {_format_int(hit)} / WARN {_format_int(warn)} / "
            f"FAIL {_format_int(fail)}"
        ),
    ]
    total_targets = len(targets)
    if total_targets == 0:
        lines.append("New targets 0")
        return "\n".join(lines)

    breakdown = _source_breakdown(targets)
    if breakdown:
        lines.append(
            f"New targets {total_targets} ({breakdown})"
        )
    else:
        lines.append(f"New targets {total_targets}")

    lines.append("")
    shown = targets[:MAX_DISPLAYED_TARGETS]
    for index, target in enumerate(shown, start=1):
        title = _truncate_title(_display_title(target))
        label = SOURCE_DISPLAY_LABELS.get(
            target.get("source", "unknown"),
            "Unknown source",
        )
        lines.append(f"{index}. {title} — {label}")
    overflow = total_targets - len(shown)
    if overflow > 0:
        lines.append(f"... and {overflow} more.")
    return "\n".join(lines)


def load_last_sent_report_date(
    state_path: str | Path | None = None,
) -> date | None:
    path = daily_report_state_path(state_path)
    try:
        if not path.is_file():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        raw = data.get("last_sent_report_date")
        if not raw:
            return None
        return date.fromisoformat(str(raw))
    except Exception:
        return None


def save_last_sent_report_date(
    report_day: date,
    state_path: str | Path | None = None,
) -> None:
    path = daily_report_state_path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"last_sent_report_date": report_day.isoformat()}
    text = json.dumps(payload, ensure_ascii=False) + "\n"
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    os.replace(tmp_path, path)


def build_daily_report_payload(
    *,
    report_day: date,
    batch_log_dir: str | Path,
    target_db_path: str | Path,
    addition_log_dir: str | Path | None = None,
) -> tuple[dict, list[dict], str]:
    metrics = aggregate_batch_logs_for_day(batch_log_dir, report_day)
    targets = query_targets_created_on_day(target_db_path, report_day)
    targets = attach_sources_to_targets(
        targets,
        report_day,
        log_dir=addition_log_dir,
    )
    message = format_daily_report_message(report_day, metrics, targets)
    return metrics, targets, message


def attempt_daily_runtime_report(
    *,
    target_db_path: str | Path,
    batch_log_dir: str | Path | None = None,
    state_path: str | Path | None = None,
    addition_log_dir: str | Path | None = None,
    now: datetime | None = None,
    send_fn=None,
    enabled: bool | None = None,
    webhook_url: str | None = None,
) -> dict:
    """Attempt once-per-report-date Slack delivery. Always non-fatal."""
    result = {
        "attempted": False,
        "sent": False,
        "skipped": True,
        "reason": "not_started",
        "report_date": None,
    }
    try:
        is_enabled = (
            daily_report_enabled() if enabled is None else enabled
        )
        if not is_enabled:
            result["reason"] = "disabled"
            return result

        url = (
            issue_report_webhook_url()
            if webhook_url is None
            else webhook_url.strip()
        )
        if not url:
            result["reason"] = "missing_webhook"
            return result

        report_day = report_date_for_attempt(now)
        result["report_date"] = report_day.isoformat()
        last_sent = load_last_sent_report_date(state_path)
        if last_sent == report_day:
            result["reason"] = "already_sent"
            return result

        log_dir = batch_log_dir
        if log_dir is None:
            log_dir = os.environ.get(
                "BATCH_LOG_DIR",
                "data/batch_runs",
            )
        _metrics, _targets, message = build_daily_report_payload(
            report_day=report_day,
            batch_log_dir=log_dir,
            target_db_path=target_db_path,
            addition_log_dir=addition_log_dir,
        )
        result["attempted"] = True
        result["skipped"] = False
        sender = send_fn or send_slack_webhook_message
        sender(
            url,
            message,
            timeout_seconds=issue_report_timeout_seconds(),
        )
        save_last_sent_report_date(report_day, state_path=state_path)
        result["sent"] = True
        result["reason"] = "sent"
        return result
    except Exception as exc:
        warnings.warn(
            f"daily_report_failed: {type(exc).__name__}",
            RuntimeWarning,
            stacklevel=2,
        )
        result["attempted"] = True
        result["skipped"] = False
        result["sent"] = False
        result["reason"] = "failed"
        return result
