import tarfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import re
import shutil
from typing import Callable, TextIO

from compact_scrape_log import (
    GROUP_PAGE_TOKENS,
    digest_reason_token,
    digest_looks_like_skip,
    format_batch_digest_block,
    board_page_token_key,
    compact_run_id_from_datetime,
    fail_detail_line,
    feeder_summary_compact,
    format_page_err_token,
    format_page_ok_token,
    format_top_err_token,
    http_status_quick,
    join_page_tokens,
    observe_val,
    run_start_compact_fields,
    shell_quote_safe,
    title_for_log,
    utc_ts_z,
    warn_detail_later_page,
    warn_detail_response_cap,
)


ACTIVE_LOG_NAME = "host_cron.log"
DAILY_LOG_RE = re.compile(r"^host_cron\.(\d{8})\.log$")
RUN_START_RE = re.compile(
    r"^\[RUN\] START (\d{4})-(\d{2})-(\d{2}) "
)
RUN_START_COMPACT_TS_RE = re.compile(
    r"^\[RUN START\] ts=(\d{4})-(\d{2})-(\d{2})T"
)
HOST_CRON_OK0_SUM_MODE = "sum"
HOST_CRON_OK0_LINE_MODE = "line"
DEFAULT_HOST_CRON_OK0_SUM_EVERY = 250


def host_cron_ok0_mode_from_env() -> str:
    raw = os.environ.get("HOST_CRON_OK0_MODE", "")
    mode = raw.strip().lower()
    if mode == HOST_CRON_OK0_LINE_MODE:
        return HOST_CRON_OK0_LINE_MODE
    return HOST_CRON_OK0_SUM_MODE


def host_cron_ok0_sum_every_from_env() -> int:
    raw = os.environ.get("HOST_CRON_OK0_SUM_EVERY", "")
    text = raw.strip()
    if not text:
        return DEFAULT_HOST_CRON_OK0_SUM_EVERY
    try:
        value = int(text)
    except ValueError:
        return DEFAULT_HOST_CRON_OK0_SUM_EVERY
    if value <= 0:
        return DEFAULT_HOST_CRON_OK0_SUM_EVERY
    return value


def local_now() -> datetime:
    return datetime.now().astimezone()


def format_run_timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def format_day_token(value: date) -> str:
    return value.strftime("%Y%m%d")


def parse_run_start_day(text: str) -> date | None:
    for raw in text.splitlines():
        line = raw.strip()
        match = RUN_START_RE.match(line)
        if match is not None:
            year, month, day = match.groups()
            return date(int(year), int(month), int(day))
        cm = RUN_START_COMPACT_TS_RE.match(line)
        if cm is not None:
            year, month, day = cm.groups()
            return date(int(year), int(month), int(day))
    return None


def read_active_log_day(log_path: Path) -> date | None:
    if not log_path.exists() or log_path.stat().st_size == 0:
        return None
    return parse_run_start_day(log_path.read_text(encoding="utf-8"))


def daily_log_path(log_dir: Path, log_day: date) -> Path:
    return log_dir / f"host_cron.{format_day_token(log_day)}.log"


def weekly_archive_path(log_dir: Path, start_day: date, end_day: date) -> Path:
    return log_dir / (
        "host_cron."
        f"{format_day_token(start_day)}-{format_day_token(end_day)}.tar.gz"
    )


def batch_weekly_archive_path(log_dir: Path, start_day: date, end_day: date) -> Path:
    return log_dir / (
        "batch_runs."
        f"{format_day_token(start_day)}-{format_day_token(end_day)}.tar.gz"
    )


def week_bounds(log_day: date) -> tuple[date, date]:
    start_day = log_day - timedelta(days=log_day.weekday())
    end_day = start_day + timedelta(days=6)
    return start_day, end_day


@dataclass(frozen=True)
class RotationOutcome:
    rotated_path: Path | None = None
    warning: str | None = None


@dataclass(frozen=True)
class WeeklyArchivePlan:
    archive_path: Path
    member_paths: tuple[Path, ...]
    start_day: date
    end_day: date


def rotate_active_log(log_path: Path, run_day: date) -> RotationOutcome:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if not log_path.exists() or log_path.stat().st_size == 0:
        return RotationOutcome()

    active_day = read_active_log_day(log_path)
    if active_day is None:
        return RotationOutcome(
            warning=(
                "host_cron_rotation_skipped "
                f"reason=unparseable_active_log path={log_path.name}"
            )
        )

    if active_day == run_day:
        return RotationOutcome()

    rotated_path = daily_log_path(log_path.parent, active_day)
    temp_path = rotated_path.with_name(f"{rotated_path.name}.tmp")

    try:
        if rotated_path.exists():
            with temp_path.open("wb") as target:
                with rotated_path.open("rb") as source:
                    shutil.copyfileobj(source, target)
                with log_path.open("rb") as source:
                    shutil.copyfileobj(source, target)
            temp_path.replace(rotated_path)
            log_path.unlink()
        else:
            log_path.replace(rotated_path)
        log_path.touch()
    except OSError as exc:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        return RotationOutcome(
            warning=(
                "host_cron_rotation_failed "
                f"reason={type(exc).__name__}:{exc}"
            )
        )

    return RotationOutcome(rotated_path=rotated_path)


def iter_daily_logs(log_dir: Path) -> list[tuple[date, Path]]:
    if not log_dir.exists():
        return []

    daily_logs: list[tuple[date, Path]] = []
    for entry in sorted(log_dir.iterdir()):
        match = DAILY_LOG_RE.match(entry.name)
        if match is None or not entry.is_file():
            continue
        token = match.group(1)
        log_day = datetime.strptime(token, "%Y%m%d").date()
        daily_logs.append((log_day, entry))
    return daily_logs


def _mtime_day(path: Path) -> date:
    return datetime.fromtimestamp(path.stat().st_mtime).date()


def iter_batch_run_logs(batch_runs_dir: Path) -> list[tuple[date, Path]]:
    if not batch_runs_dir.exists():
        return []

    batch_logs: list[tuple[date, Path]] = []
    for entry in sorted(batch_runs_dir.iterdir()):
        if not entry.is_file():
            continue
        if not entry.name.startswith("batch_"):
            continue
        if not entry.name.endswith(".log"):
            continue
        batch_logs.append((_mtime_day(entry), entry))
    return batch_logs


def _plan_weekly_archives(
    logs: list[tuple[date, Path]],
    *,
    today: date,
    archive_path_fn: Callable[[Path, date, date], Path],
    archive_log_dir: Path,
) -> list[WeeklyArchivePlan]:
    oldest_raw_day = today - timedelta(days=14)
    buckets: dict[tuple[date, date], list[Path]] = {}

    for log_day, path in logs:
        start_day, end_day = week_bounds(log_day)
        if end_day > oldest_raw_day:
            continue
        buckets.setdefault((start_day, end_day), []).append(path)

    plans: list[WeeklyArchivePlan] = []
    for start_day, end_day in sorted(buckets):
        archive_path = archive_path_fn(archive_log_dir, start_day, end_day)
        if archive_path.exists():
            continue
        member_paths = tuple(sorted(buckets[(start_day, end_day)]))
        if not member_paths:
            continue
        plans.append(
            WeeklyArchivePlan(
                archive_path=archive_path,
                member_paths=member_paths,
                start_day=start_day,
                end_day=end_day,
            )
        )

    return plans


def plan_weekly_archives(log_dir: Path, today: date) -> list[WeeklyArchivePlan]:
    return _plan_weekly_archives(
        iter_daily_logs(log_dir),
        today=today,
        archive_path_fn=weekly_archive_path,
        archive_log_dir=log_dir,
    )


def plan_batch_run_archives(
    batch_runs_dir: Path,
    today: date,
) -> list[WeeklyArchivePlan]:
    return _plan_weekly_archives(
        iter_batch_run_logs(batch_runs_dir),
        today=today,
        archive_path_fn=batch_weekly_archive_path,
        archive_log_dir=batch_runs_dir,
    )


def _compress_weekly_archive_plans(
    plans: list[WeeklyArchivePlan],
    *,
    archive_warning_prefix: str,
    cleanup_warning_prefix: str,
) -> list[str]:
    warnings: list[str] = []

    for plan in plans:
        temp_path = plan.archive_path.with_name(f"{plan.archive_path.name}.tmp")

        try:
            with tarfile.open(temp_path, "w:gz") as archive:
                for member_path in plan.member_paths:
                    archive.add(member_path, arcname=member_path.name)
            temp_path.replace(plan.archive_path)
        except (OSError, tarfile.TarError) as exc:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            warnings.append(
                f"{archive_warning_prefix} "
                f"archive={plan.archive_path.name} "
                f"reason={type(exc).__name__}:{exc}"
            )
            continue

        for member_path in plan.member_paths:
            try:
                member_path.unlink()
            except OSError as exc:
                warnings.append(
                    f"{cleanup_warning_prefix} "
                    f"path={member_path.name} "
                    f"reason={type(exc).__name__}:{exc}"
                )

    return warnings


def _write_readme_log(path: Path, lines: list[str]) -> str | None:
    content = "\n".join(lines) + "\n"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    except OSError as exc:
        return (
            f"host_cron_readme_write_failed path={path.name} "
            f"reason={type(exc).__name__}:{exc}"
        )
    return None


def _host_log_readme_lines() -> list[str]:
    return [
        "DIGEST EXP host_cron compact log keys",
        "DIGEST EXP RUN DIGEST -> compact end-of-run summary",
        "DIGEST EXP B=batch/run reference",
        "DIGEST EXP dur=run duration seconds",
        "DIGEST EXP end=final status (success, partial_failure, failure)",
        "DIGEST EXP H=targets with newly saved responses",
        "DIGEST EXP OK0=success targets with zero newly saved responses",
        "DIGEST EXP W=warning or partial targets",
        "DIGEST EXP F=failed targets",
        "DIGEST EXP S=skipped targets",
        "DIGEST EXP NEW=total newly saved responses",
        "DIGEST EXP UOBS=targets with unknown observed max after run",
        "DIGEST EXP P=processed targets",
        "DIGEST EXP T=total loaded targets",
        "DIGEST EXP R=remaining targets",
        "DIGEST EXP historical logs may use long keys such as hit_targets",
        "DIGEST EXP historical logs may use long keys such as ok0_targets",
    ]


def _batch_runs_log_readme_lines() -> list[str]:
    return [
        "DIGEST EXP batch_run digest-first logs",
        "DIGEST EXP BATCH_DIGEST -> compact run counters",
        "DIGEST EXP BATCH_DIGEST_ITEMS -> per-target hit/warn/fail items",
        "DIGEST EXP H=targets with newly saved responses",
        "DIGEST EXP OK0=success targets with zero newly saved responses",
        "DIGEST EXP W=warning or partial targets",
        "DIGEST EXP F=failed targets",
        "DIGEST EXP S=skipped targets",
        "DIGEST EXP NEW=total newly saved responses",
        "DIGEST EXP UOBS=targets with unknown observed max after run",
        "DIGEST EXP BATCH_LOG_VERBOSE=1 restores per-target progress blocks",
        "DIGEST EXP batch archives are named batch_runs.YYYYMMDD-YYYYMMDD.tar.gz",
        "DIGEST EXP batch archive dates come from file mtime, not run ids",
    ]


def ensure_log_readmes(log_dir: Path) -> list[str]:
    warnings: list[str] = []
    host_readme_path = log_dir / "README.log"
    warning = _write_readme_log(host_readme_path, _host_log_readme_lines())
    if warning is not None:
        warnings.append(warning)

    batch_readme_path = log_dir / "batch_runs" / "README.log"
    warning = _write_readme_log(batch_readme_path, _batch_runs_log_readme_lines())
    if warning is not None:
        warnings.append(warning)

    return warnings


def compress_weekly_archives(log_dir: Path, today: date) -> list[str]:
    warnings = _compress_weekly_archive_plans(
        plan_weekly_archives(log_dir, today),
        archive_warning_prefix="host_cron_weekly_archive_failed",
        cleanup_warning_prefix="host_cron_daily_cleanup_failed",
    )
    batch_runs_dir = log_dir / "batch_runs"
    warnings.extend(
        _compress_weekly_archive_plans(
            plan_batch_run_archives(batch_runs_dir, today),
            archive_warning_prefix="batch_runs_weekly_archive_failed",
            cleanup_warning_prefix="batch_runs_log_cleanup_failed",
        )
    )
    warnings.extend(ensure_log_readmes(log_dir))
    return warnings


class HostCronReporter:
    def __init__(
        self,
        stream: TextIO,
        now_provider: Callable[[], datetime] = local_now,
    ) -> None:
        self._stream = stream
        self._now_provider = now_provider
        self._started_at: datetime | None = None
        self._total_targets = 0
        self._ok_targets = 0
        self._hard_fail_targets = 0
        self._partial_targets = 0
        self._error_refs: list[str] = []
        self._current_label: str | None = None
        self._current_page_count = 0
        self._compact_run = False
        self._page_tokens: list[str] = []
        self._step_index = 0
        self._step_total = 0
        self._step_article_id: str | None = None
        self._step_canonical_url: str | None = None
        self._step_saved_before: int | None = None
        self._step_observed_before: str | None = None
        self._compact_step_started = False
        self._compact_step_detail_started = False
        self._pages_ok_step = 0
        self._last_page_key: str | None = None
        self._interrupt_http: str | None = None
        self._response_cap_hit = False
        self._detail_emitted = False
        self._compact_run_stamp = ""
        self._compact_batch_ref = ""
        self._compact_trigger = "host_cron"
        self._compact_started_at_iso = ""
        self._compact_digest_duration_seconds: int | None = None
        self._compact_digest_end: str = ""
        self._ok0_mode = host_cron_ok0_mode_from_env()
        self._ok0_sum_every = host_cron_ok0_sum_every_from_env()
        self._ok0_sum_pending_count = 0
        self._ok0_sum_first_step: int | None = None
        self._ok0_sum_last_step: int | None = None
        self._ok0_sum_last_total: int | None = None
        self._ok0_sum_last_id = "unknown"
        self._ok0_sum_last_page: str | None = None
        self._batch_totals: dict[str, int] | None = None
        self._digest_hit_msgs: list[str] = []
        self._digest_warn_msgs: list[str] = []
        self._digest_fail_msgs: list[str] = []
        self._digest_skip_msgs: list[str] = []
        self._digest_ok0 = 0
        self._total_new_responses = 0
        self._unknown_obs_targets = 0
        self._compact_plain_fail_total = 0
        self._compact_skip_display_total = 0

    def emit(self, tag: str, message: str, indent_level: int = 0) -> None:
        indent = "  " * indent_level
        self._stream.write(f"{indent}[{tag}] {message}\n")
        self._stream.flush()

    def begin_compact_host_run(
        self,
        *,
        started_at_iso: str,
        batch_ref: str,
        archive_db_path: str,
        limit_seconds: int | float | None,
        trigger: str = "host_cron",
    ) -> None:
        dt = datetime.fromisoformat(started_at_iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        utc_dt = dt.astimezone(timezone.utc)
        self._started_at = self._now_provider()
        self._compact_started_at_iso = started_at_iso
        stamp = compact_run_id_from_datetime(utc_dt)
        ts_z = utc_ts_z(utc_dt)
        self._compact_run = True
        self._compact_run_stamp = stamp
        self._compact_batch_ref = batch_ref
        self._compact_trigger = trigger
        self._compact_digest_duration_seconds = None
        self._compact_digest_end = ""
        self._ok0_sum_pending_count = 0
        self._ok0_sum_first_step = None
        self._ok0_sum_last_step = None
        self._ok0_sum_last_total = None
        self._ok0_sum_last_id = "unknown"
        self._ok0_sum_last_page = None
        self._reset_compact_digest_counters()
        self.emit("RUN", f"START {format_run_timestamp(self._started_at)}")
        compact_body = run_start_compact_fields(
            ts_iso_z=ts_z,
            run_stamp=stamp,
            batch_ref=batch_ref,
            trigger=trigger,
            db_path=archive_db_path,
            limit_seconds=limit_seconds,
        )
        self.emit("RUN START", compact_body, indent_level=0)

    def _reset_compact_digest_counters(self) -> None:
        self._digest_hit_msgs.clear()
        self._digest_warn_msgs.clear()
        self._digest_fail_msgs.clear()
        self._digest_skip_msgs.clear()
        self._digest_ok0 = 0
        self._total_new_responses = 0
        self._unknown_obs_targets = 0
        self._compact_plain_fail_total = 0
        self._compact_skip_display_total = 0

    def bind_run_totals(
        self,
        *,
        total_targets: int,
        processed_targets: int,
        remaining_targets: int,
    ) -> None:
        self._batch_totals = {
            "total": total_targets,
            "processed": processed_targets,
            "remaining": remaining_targets,
        }

    def emit_compact_feed_summary(self, summary: dict) -> None:
        if not self._compact_run:
            return
        self.emit("FEEDER SUMMARY", feeder_summary_compact(summary), 1)

    def note_scrape_start_compact(self) -> None:
        if not self._compact_run:
            return
        self.emit("SCRAPE START", f"targets={self._total_targets}", 1)

    def render_batch_digest_block(self) -> list[str]:
        if not self._compact_run:
            return []
        return format_batch_digest_block(
            digest_hit_msgs=self._digest_hit_msgs,
            digest_warn_msgs=self._digest_warn_msgs,
            digest_fail_msgs=self._digest_fail_msgs,
            digest_skip_msgs=self._digest_skip_msgs,
            digest_ok0=self._digest_ok0,
            total_new_responses=self._total_new_responses,
            unknown_obs_targets=self._unknown_obs_targets,
        )

    @staticmethod
    def _looks_like_skip(reason: str | None) -> bool:
        return digest_looks_like_skip(reason)

    def begin_run(self) -> None:
        self._compact_run = False
        self._started_at = self._now_provider()
        self.emit("RUN", f"START {format_run_timestamp(self._started_at)}")

    def note_maintenance_warning(self, message: str) -> None:
        self.emit("WARN", message, indent_level=1)

    def note_targets_loaded(self, count: int, target_db_path: str) -> None:
        self._total_targets = count
        if self._compact_run:
            reg_tail = Path(target_db_path).name
            self.emit(
                "TARGETS",
                f"total={count} source=target_table path={reg_tail}",
                1,
            )
        else:
            self.emit(
                "INFO",
                f"target_db_path={target_db_path} targets={count}",
                1,
            )

    def compact_note_top_fetch_failure(self, status_text: str) -> None:
        if not self._compact_run:
            return
        self._ensure_compact_step_detail_started()
        self._interrupt_http = http_status_quick(status_text)
        self._detail_emitted = True
        tok = format_top_err_token(status_text)
        self._page_tokens.append(tok)
        self._flush_compact_pages()
        st_http = observe_val(self._interrupt_http).replace('"', "?")
        self.emit(
            "FAIL DETAIL",
            fail_detail_line(
                phase="article_top_fetch",
                http_status=st_http,
                reason_snake="article_top_fetch_failed",
            ),
            2,
        )

    def start_target(
        self,
        index: int,
        total: int,
        label: str,
        canonical_url: str,
        *,
        article_id: str | None = None,
        saved_before: int | None = None,
        observed_before: str | None = None,
    ) -> None:
        self._current_label = label
        self._current_page_count = 0
        self._interrupt_http = None
        self._response_cap_hit = False
        self._pages_ok_step = 0
        self._last_page_key = None
        self._page_tokens.clear()
        self._step_index = index
        self._step_total = total
        self._step_article_id = article_id
        self._step_canonical_url = canonical_url
        self._step_saved_before = saved_before
        self._step_observed_before = observed_before
        if self._compact_run:
            self._compact_step_started = True
            self._compact_step_detail_started = False
            self._detail_emitted = False
        else:
            self._compact_step_started = False
            self.emit(
                "STEP",
                f"{index}/{total} title={label} url={canonical_url}",
                1,
            )

    def _flush_compact_partial_rows(self) -> None:
        while len(self._page_tokens) >= GROUP_PAGE_TOKENS:
            chunk = self._page_tokens[:GROUP_PAGE_TOKENS]
            del self._page_tokens[:GROUP_PAGE_TOKENS]
            self.emit("PAGE", join_page_tokens(chunk), 2)

    def _flush_compact_pages(self) -> None:
        self._flush_compact_partial_rows()
        if self._page_tokens:
            self.emit("PAGE", join_page_tokens(self._page_tokens), 2)
            self._page_tokens.clear()

    def _format_compact_step_start(self) -> str:
        sb = self._step_saved_before if self._step_saved_before is not None else 0
        ob = self._step_observed_before if self._step_observed_before else "unknown"
        aid = observe_val(self._step_article_id)
        url = shell_quote_safe(self._step_canonical_url or "unknown")
        label = title_for_log(self._current_label or "?")
        return (
            f"ts={utc_ts_z()} step={self._step_index}/{self._step_total} "
            f"article_id={aid} title=\"{label}\" "
            f"saved_before={sb} observed_before={ob} url={url}"
        )

    def _ensure_compact_step_detail_started(self) -> None:
        if not self._compact_run or not self._compact_step_started:
            return
        if self._compact_step_detail_started:
            return
        self._flush_ok0_sum_pending()
        self.emit("STEP START", self._format_compact_step_start(), 1)
        self._compact_step_detail_started = True

    def _record_ok0_sum_target(self, ref: str) -> None:
        if self._ok0_sum_pending_count == 0:
            self._ok0_sum_first_step = self._step_index
        self._ok0_sum_pending_count += 1
        self._ok0_sum_last_step = self._step_index
        self._ok0_sum_last_total = self._step_total
        self._ok0_sum_last_id = observe_val(self._step_article_id or ref)
        self._ok0_sum_last_page = self._last_page_key

    def _flush_ok0_sum_pending(self) -> None:
        if not self._compact_run:
            return
        if self._ok0_mode != HOST_CRON_OK0_SUM_MODE:
            return
        if self._ok0_sum_pending_count <= 0:
            return

        step_first = self._ok0_sum_first_step or self._step_index
        step_last = self._ok0_sum_last_step or step_first
        step_total = self._ok0_sum_last_total or self._step_total
        elapsed_seconds = 0
        now_dt = self._now_provider()
        if self._started_at is not None:
            elapsed_seconds = int(
                max((now_dt - self._started_at).total_seconds(), 0),
            )
        parts = [
            f"steps={step_first}-{step_last}/{step_total}",
            f"cnt={self._ok0_sum_pending_count}",
            f"total_ok0={self._digest_ok0}",
            f"last_id={self._ok0_sum_last_id}",
            f"elapsed={elapsed_seconds}s",
        ]
        if self._ok0_sum_last_page is not None:
            parts.append(f"last_page={self._ok0_sum_last_page}")
        self.emit("OK0 SUM 🟢", " ".join(parts), 1)

        self._ok0_sum_pending_count = 0
        self._ok0_sum_first_step = None
        self._ok0_sum_last_step = None
        self._ok0_sum_last_total = None
        self._ok0_sum_last_id = "unknown"
        self._ok0_sum_last_page = None

    def _is_compact_ok0_target(
        self,
        *,
        status: str,
        reason: str | None,
        stored_new: int | None,
        saved_after: int | str | None,
        observed_after: int | str | None,
    ) -> bool:
        if status != "success":
            return False
        if (stored_new if stored_new is not None else 0) != 0:
            return False
        if self._reason_token(reason, status) != "already_up_to_date":
            return False
        if self._detail_emitted:
            return False
        if self._pages_ok_step != 1:
            return False
        if observe_val(saved_after) == "unknown":
            return False
        if observe_val(observed_after) == "unknown":
            return False
        if self._interrupt_http is not None:
            return False
        return True

    def _format_step_ok0(
        self,
        *,
        label: str,
        ref: str,
        reason: str | None,
        saved_after: int | str | None,
        observed_after: int | str | None,
        elapsed_s: int | None,
    ) -> str:
        aid = observe_val(self._step_article_id or ref)
        saved = observe_val(saved_after)
        observed = observe_val(observed_after)
        page = observe_val(self._last_page_key)
        elapsed = elapsed_s if elapsed_s is not None else 0
        rsn = self._reason_token(reason, "success")
        return (
            f"ts={utc_ts_z()} step={self._step_index}/{self._step_total} "
            f"article_id={aid} title=\"{title_for_log(label)}\" "
            f"saved={saved} observed={observed} page={page} "
            f"elapsed={elapsed}s reason={rsn}"
        )

    def page_progress(self, page_url: str, collected: int, total: int) -> None:
        self._current_page_count += 1
        if self._compact_run:
            self._pages_ok_step += 1
            self._last_page_key = board_page_token_key(page_url)
            tok = format_page_ok_token(page_url)
            self._page_tokens.append(tok)
            if self._pages_ok_step > 1:
                self._ensure_compact_step_detail_started()
            self._flush_compact_partial_rows()
        else:
            page_ref = page_url.rsplit("/", 1)[-1]
            self.emit("INFO", f"[{page_ref}:OK]", 2)

    def later_page_interrupted(
        self,
        page_url: str,
        status_text: str,
        saved_partial: int,
    ) -> None:
        if self._compact_run:
            self._ensure_compact_step_detail_started()
            self._flush_compact_partial_rows()
            self._page_tokens.append(
                format_page_err_token(page_url, status_text),
            )
            self._flush_compact_pages()
            pk = board_page_token_key(page_url)
            hs = observe_val(http_status_quick(status_text))
            self._interrupt_http = hs
            self._detail_emitted = True
            self.emit(
                "WARN DETAIL",
                warn_detail_later_page(pk, hs, saved_partial),
                2,
            )
            return
        label = self._current_label or "unknown"
        self.emit("WARN", f"{label} later_page_interrupted", 1)
        self.emit(
            "INFO",
            f"page={page_url} status={status_text} "
            f"saved_partial={saved_partial}",
            2,
        )

    def response_cap_reached(self, saved_partial: int) -> None:
        if self._compact_run:
            self._ensure_compact_step_detail_started()
            self._flush_compact_pages()
            self.emit(
                "WARN DETAIL",
                warn_detail_response_cap(saved_partial),
                2,
            )
            self._response_cap_hit = True
            self._detail_emitted = True
            return
        label = self._current_label or "unknown"
        self.emit("WARN", f"{label} response_cap_reached", 1)
        self.emit("INFO", f"saved_partial={saved_partial}", 2)

    def finish_target(
        self,
        status: str,
        label: str,
        total_collected: int,
        ref: str,
        *,
        reason: str | None = None,
        stored_new: int | None = None,
        saved_after: int | str | None = None,
        observed_after: int | str | None = None,
        pages_ok: int | None = None,
        elapsed_s: int | None = None,
    ) -> None:
        had_step = self._compact_step_started
        if self._compact_run:
            self._record_compact_digest(
                had_step=had_step,
                status=status,
                label=label,
                ref=ref,
                reason=reason,
                stored_new=stored_new,
                observed_after=observed_after,
            )
        if self._compact_run and had_step:
            if self._is_compact_ok0_target(
                status=status,
                reason=reason,
                stored_new=stored_new,
                saved_after=saved_after,
                observed_after=observed_after,
            ):
                if self._ok0_mode == HOST_CRON_OK0_LINE_MODE:
                    self.emit(
                        "STEP OK0 🟢",
                        self._format_step_ok0(
                            label=label,
                            ref=ref,
                            reason=reason,
                            saved_after=saved_after,
                            observed_after=observed_after,
                            elapsed_s=elapsed_s,
                        ),
                        1,
                    )
                else:
                    self._record_ok0_sum_target(ref)
                    if self._ok0_sum_pending_count >= self._ok0_sum_every:
                        self._flush_ok0_sum_pending()
                self._page_tokens.clear()
            else:
                self._flush_ok0_sum_pending()
                self._ensure_compact_step_detail_started()
                self._flush_compact_pages()
                self._emit_compact_step_end(
                    status=status,
                    label=label,
                    ref=ref,
                    reason=reason,
                    stored_new=stored_new,
                    saved_after=saved_after,
                    observed_after=observed_after,
                    pages_ok=pages_ok,
                    elapsed_s=elapsed_s,
                )
        if not self._compact_run:
            tag = {"success": "OK", "partial": "WARN", "fail": "ERROR"}[status]
            self.emit(
                tag,
                f"{label} {status} total_collected={total_collected}",
                1,
            )
            if reason:
                self.emit("INFO", reason, 2)

        if status == "success":
            self._ok_targets += 1
        elif status == "partial":
            self._partial_targets += 1
            self._append_error_ref(ref)
        else:
            self._hard_fail_targets += 1
            self._append_error_ref(ref)

        self._current_label = None
        self._current_page_count = 0
        self._compact_step_started = False
        self._compact_step_detail_started = False
        self._page_tokens.clear()
        self._last_page_key = None
        self._interrupt_http = None
        self._response_cap_hit = False
        self._detail_emitted = False

    def _emit_compact_step_end(
        self,
        *,
        status: str,
        label: str,
        ref: str,
        reason: str | None,
        stored_new: int | None,
        saved_after: int | str | None,
        observed_after: int | str | None,
        pages_ok: int | None,
        elapsed_s: int | None,
    ) -> None:
        result_word = {
            "success": "success",
            "partial": "partial",
            "fail": "fail",
        }[status]
        end_tag = {
            "success": "STEP END OK 🟢",
            "partial": "STEP END WARN 🟡",
            "fail": "STEP END FAIL 🔴",
        }[status]
        sto = stored_new if stored_new is not None else 0
        sa = observe_val(saved_after if saved_after is not None else None)
        oa = observe_val(observed_after)
        pg = pages_ok if pages_ok is not None else self._pages_ok_step
        el = elapsed_s if elapsed_s is not None else 0
        rsn = self._reason_token(reason, status)
        aid = observe_val(self._step_article_id or ref)
        body = (
            f"ts={utc_ts_z()} step={self._step_index}/{self._step_total} "
            f"article_id={aid} title=\"{title_for_log(label)}\" "
            f"result={result_word} stored_new={sto} saved_after={sa} "
            f"observed_after={oa} pages_ok={pg} elapsed={el}s reason={rsn}"
        )
        if self._interrupt_http is not None:
            body += f" status={self._interrupt_http}"
        self.emit(end_tag, body, 1)

    def _reason_token(self, reason: str | None, status: str) -> str:
        if self._response_cap_hit:
            return digest_reason_token(
                reason,
                response_cap_hint=True,
                status_fallback=status,
            )
        return digest_reason_token(
            reason,
            response_cap_hint=False,
            status_fallback=status,
        )

    def _record_compact_digest(
        self,
        *,
        had_step: bool,
        status: str,
        label: str,
        ref: str,
        reason: str | None,
        stored_new: int | None,
        observed_after: int | str | None,
    ) -> None:
        prog = (
            f"{self._step_index}/{self._step_total}" if had_step else "?/?"
        )
        ttl = title_for_log(label or "?")
        aid = observe_val(self._step_article_id or ref)
        sn = stored_new if stored_new is not None else 0
        o_lit = observe_val(observed_after)
        skip = (
            status == "fail"
            and HostCronReporter._looks_like_skip(reason)
        )
        if skip:
            self._compact_skip_display_total += 1
            self._digest_skip_msgs.append(
                f'progress={prog} article_id={aid} title="{ttl}" '
                f"detail={self._reason_token(reason, status)}"
            )
            return
        if observe_val(observed_after) == "unknown" and status in (
            "fail",
            "partial",
        ):
            self._unknown_obs_targets += 1
        if status == "fail" and not had_step:
            self._compact_plain_fail_total += 1
            self._digest_fail_msgs.append(
                f'article_id={aid} title="{ttl}" '
                f"detail={self._reason_token(reason, status)} "
                f"observed_after={o_lit}"
            )
            return
        if status == "fail":
            self._compact_plain_fail_total += 1
            self._digest_fail_msgs.append(
                f'progress={prog} article_id={aid} title="{ttl}" '
                f"detail={self._reason_token(reason, status)} "
                f"observed_after={o_lit}"
            )
            return
        if status == "partial":
            self._digest_warn_msgs.append(
                f'progress={prog} article_id={aid} title="{ttl}" '
                f"reason={self._reason_token(reason, status)} "
                f"stored_partial={sn} observed_after={o_lit} "
                f"http={observe_val(self._interrupt_http)}"
            )
            self._total_new_responses += max(sn, 0)
            return
        if sn > 0:
            self._digest_hit_msgs.append(
                f'progress={prog} article_id={aid} title="{ttl}" '
                f"stored_new={sn} observed_after={o_lit}"
            )
            self._total_new_responses += max(sn, 0)
        else:
            self._digest_ok0 += 1

    def _append_error_ref(self, ref: str) -> None:
        if ref not in self._error_refs:
            self._error_refs.append(ref)

    def _emit_compact_run_end(self, final_status: str) -> None:
        ended_local = self._now_provider()
        started_local = self._started_at or ended_local
        duration_seconds = int(
            max((ended_local - started_local).total_seconds(), 0),
        )
        logged = self.derive_run_status(final_status)
        tag = {
            "success": "RUN END OK 🟢",
            "partial_failure": "RUN END WARN 🟡",
            "failure": "RUN END FAIL 🔴",
        }.get(logged, "RUN END WARN 🟡")
        totals = self._batch_totals or {}
        processed = totals.get(
            "processed",
            (
                self._ok_targets + self._partial_targets
                + self._hard_fail_targets
            ),
        )
        remaining = totals.get("remaining", 0)
        total_t = totals.get("total", self._total_targets)
        ok_plain = self._ok_targets
        warn_plain = self._partial_targets
        fails = self._compact_plain_fail_total
        skips = self._compact_skip_display_total
        body = (
            f"ts={utc_ts_z(ended_local.astimezone(timezone.utc))} "
            f"run_id={self._compact_run_stamp} status={logged} "
            f"duration={duration_seconds}s processed={processed} "
            f"total={total_t} ok={ok_plain} warn={warn_plain} fail={fails} "
            f"skip={skips} remaining={remaining}"
        )
        self._compact_digest_duration_seconds = duration_seconds
        self._compact_digest_end = logged
        self.emit(tag, body, 0)

    def _emit_compact_run_digest(self) -> None:
        totals = self._batch_totals or {}
        meta_parts = [
            f"B={self._compact_batch_ref or self._compact_run_stamp}",
            (
                f"dur={self._compact_digest_duration_seconds}s"
                if self._compact_digest_duration_seconds is not None
                else "dur=unknown"
            ),
            f"end={self._compact_digest_end or 'unknown'}",
            f"H={len(self._digest_hit_msgs)}",
            f"OK0={self._digest_ok0}",
            f"W={len(self._digest_warn_msgs)}",
            f"F={len(self._digest_fail_msgs)}",
            f"S={len(self._digest_skip_msgs)}",
            f"NEW={self._total_new_responses}",
            f"UOBS={self._unknown_obs_targets}",
        ]
        if totals:
            meta_parts.append(f"P={totals.get('processed', 0)}")
            meta_parts.append(f"T={totals.get('total', 0)}")
            meta_parts.append(f"R={totals.get('remaining', 0)}")
        meta = " ".join(meta_parts)
        self.emit("RUN DIGEST", meta, 0)
        for m in self._digest_hit_msgs:
            self.emit("HIT", m, 1)
        for m in self._digest_warn_msgs:
            self.emit("WARN", m, 1)
        for m in self._digest_fail_msgs:
            self.emit("FAIL", m, 1)
        for m in self._digest_skip_msgs:
            self.emit("SKIP", m, 1)
        self.emit(
            "OK0",
            f"others={self._digest_ok0}",
            1,
        )

    def finish_run(self, final_status: str) -> None:
        if self._compact_run:
            self._flush_ok0_sum_pending()
            self._emit_compact_run_end(final_status)
            self._emit_compact_run_digest()
            return

        ended_at = self._now_provider()
        started_at_m = self._started_at or ended_at
        duration_seconds = int(
            max((ended_at - started_at_m).total_seconds(), 0),
        )
        logged_status = self.derive_run_status(final_status)
        failed_targets = self._hard_fail_targets + self._partial_targets

        self.emit(
            "RUN",
            (
                f"END {format_run_timestamp(ended_at)} "
                f"status={logged_status}"
            ),
        )
        self.emit(
            "SUMMARY",
            (
                f"targets={self._total_targets} ok={self._ok_targets} "
                f"fail={failed_targets} duration={duration_seconds}s"
            ),
        )
        if self._error_refs:
            refs = ",".join(self._error_refs)
            self.emit(
                "ERROR SUMMARY",
                f"count={len(self._error_refs)} refs={refs}",
            )

    def derive_run_status(self, final_status: str) -> str:
        if final_status == "failure":
            return "failure"
        if final_status == "partial_failure":
            return "partial_failure"
        if self._partial_targets > 0:
            return "partial_failure"
        return "success"
