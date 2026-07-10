"""Best-effort JSONL log of first-time target additions."""

from __future__ import annotations

import json
import os
import warnings
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


DEFAULT_TARGET_ADDITION_LOG_DIR = "data/target_additions"
TARGET_ADDITION_RETENTION_DAYS = 31
KNOWN_SOURCES = frozenset(
    {
        "delete_feeder",
        "hot_word",
        "web_user",
        "operator",
        "import",
        "unknown",
    }
)


def target_addition_log_dir(
    log_dir: str | Path | None = None,
) -> Path:
    if log_dir is not None:
        return Path(log_dir)
    raw = os.environ.get("TARGET_ADDITION_LOG_DIR", "").strip()
    if raw:
        return Path(raw)
    return Path(DEFAULT_TARGET_ADDITION_LOG_DIR)


def target_addition_log_path_for_day(
    day: date,
    *,
    log_dir: str | Path | None = None,
) -> Path:
    return (
        target_addition_log_dir(log_dir)
        / f"target_additions_{day.isoformat()}.jsonl"
    )


def _normalize_source(source: str | None) -> str:
    value = (source or "unknown").strip() or "unknown"
    if value not in KNOWN_SOURCES:
        return "unknown"
    return value


def _bounded_title(title: str | None) -> str | None:
    if title is None:
        return None
    text = " ".join(str(title).split())
    if not text:
        return None
    if len(text) > 200:
        return text[:200]
    return text


def append_target_added_event(
    *,
    article_id: str | int,
    title: str | None = None,
    source: str = "unknown",
    article_type: str | None = None,
    log_dir: str | Path | None = None,
    now: datetime | None = None,
) -> None:
    """Append one JSONL event after a successful first-time add.

    Failures are swallowed with a bounded warning; callers must not treat
    logging failure as registration failure.
    """
    try:
        ts = now or datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        day = ts.date()
        path = target_addition_log_path_for_day(day, log_dir=log_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "event": "target_added",
            "source": _normalize_source(source),
            "article_id": str(article_id),
        }
        if article_type:
            payload["article_type"] = str(article_type)
        bounded = _bounded_title(title)
        if bounded is not None:
            payload["title"] = bounded
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        cleanup_old_target_addition_logs(log_dir=log_dir, now=ts)
    except Exception as exc:
        warnings.warn(
            f"target_addition_log_append_failed: {type(exc).__name__}",
            RuntimeWarning,
            stacklevel=2,
        )


def parse_target_addition_line(raw_line: str) -> dict | None:
    text = (raw_line or "").strip()
    if not text:
        return None
    try:
        data = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("event") != "target_added":
        return None
    article_id = data.get("article_id")
    if article_id is None or str(article_id).strip() == "":
        return None
    return {
        "ts": data.get("ts"),
        "event": "target_added",
        "source": _normalize_source(data.get("source")),
        "article_id": str(article_id).strip(),
        "article_type": (
            str(data["article_type"]).strip()
            if data.get("article_type")
            else None
        ),
        "title": _bounded_title(data.get("title")),
    }


def read_target_addition_events(
    day: date,
    *,
    log_dir: str | Path | None = None,
) -> list[dict]:
    path = target_addition_log_path_for_day(day, log_dir=log_dir)
    if not path.is_file():
        return []
    events: list[dict] = []
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            parsed = parse_target_addition_line(raw_line)
            if parsed is not None:
                events.append(parsed)
    except OSError:
        return []
    return events


def cleanup_old_target_addition_logs(
    *,
    log_dir: str | Path | None = None,
    now: datetime | None = None,
    retention_days: int = TARGET_ADDITION_RETENTION_DAYS,
) -> None:
    """Best-effort delete of JSONL files older than retention_days."""
    try:
        if retention_days <= 0:
            return
        ts = now or datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        cutoff = (ts.astimezone(timezone.utc).date()
                  - timedelta(days=retention_days))
        directory = target_addition_log_dir(log_dir)
        if not directory.is_dir():
            return
        for path in directory.glob("target_additions_*.jsonl"):
            stem = path.stem
            # target_additions_YYYY-MM-DD
            if not stem.startswith("target_additions_"):
                continue
            day_text = stem[len("target_additions_"):]
            try:
                file_day = date.fromisoformat(day_text)
            except ValueError:
                continue
            if file_day < cutoff:
                path.unlink(missing_ok=True)
    except Exception:
        pass
