import tarfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import re
import shutil
from typing import Callable, TextIO


ACTIVE_LOG_NAME = "host_cron.log"
DAILY_LOG_RE = re.compile(r"^host_cron\.(\d{8})\.log$")
RUN_START_RE = re.compile(
    r"^\[RUN\] START (\d{4})-(\d{2})-(\d{2}) "
)


def local_now() -> datetime:
    return datetime.now().astimezone()


def format_run_timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def format_day_token(value: date) -> str:
    return value.strftime("%Y%m%d")


def parse_run_start_day(text: str) -> date | None:
    for line in text.splitlines():
        match = RUN_START_RE.match(line)
        if match is None:
            continue
        year, month, day = match.groups()
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


def plan_weekly_archives(log_dir: Path, today: date) -> list[WeeklyArchivePlan]:
    oldest_raw_day = today - timedelta(days=14)
    buckets: dict[tuple[date, date], list[Path]] = {}

    for log_day, path in iter_daily_logs(log_dir):
        start_day, end_day = week_bounds(log_day)
        if end_day > oldest_raw_day:
            continue
        buckets.setdefault((start_day, end_day), []).append(path)

    plans: list[WeeklyArchivePlan] = []
    for start_day, end_day in sorted(buckets):
        archive_path = weekly_archive_path(log_dir, start_day, end_day)
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


def compress_weekly_archives(log_dir: Path, today: date) -> list[str]:
    warnings: list[str] = []

    for plan in plan_weekly_archives(log_dir, today):
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
                "host_cron_weekly_archive_failed "
                f"archive={plan.archive_path.name} "
                f"reason={type(exc).__name__}:{exc}"
            )
            continue

        for member_path in plan.member_paths:
            try:
                member_path.unlink()
            except OSError as exc:
                warnings.append(
                    "host_cron_daily_cleanup_failed "
                    f"path={member_path.name} "
                    f"reason={type(exc).__name__}:{exc}"
                )

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

    def emit(self, tag: str, message: str, indent_level: int = 0) -> None:
        indent = "  " * indent_level
        self._stream.write(f"{indent}[{tag}] {message}\n")
        self._stream.flush()

    def begin_run(self) -> None:
        self._started_at = self._now_provider()
        self.emit("RUN", f"START {format_run_timestamp(self._started_at)}")

    def note_maintenance_warning(self, message: str) -> None:
        self.emit("WARN", message, indent_level=1)

    def note_targets_loaded(self, count: int, target_db_path: str) -> None:
        self._total_targets = count
        self.emit(
            "INFO",
            f"target_db_path={target_db_path} targets={count}",
            indent_level=1,
        )

    def start_target(
        self,
        index: int,
        total: int,
        label: str,
        canonical_url: str,
    ) -> None:
        self._current_label = label
        self.emit(
            "STEP",
            f"{index}/{total} title={label} url={canonical_url}",
            indent_level=1,
        )

    def page_progress(self, page_url: str, collected: int, total: int) -> None:
        self.emit(
            "INFO",
            f"page={page_url} collected={collected} total={total}",
            indent_level=2,
        )

    def later_page_interrupted(
        self,
        page_url: str,
        status_text: str,
        saved_partial: int,
    ) -> None:
        label = self._current_label or "unknown"
        self.emit("WARN", f"{label} later_page_interrupted", indent_level=1)
        self.emit(
            "INFO",
            (
                f"page={page_url} status={status_text} "
                f"saved_partial={saved_partial}"
            ),
            indent_level=2,
        )

    def response_cap_reached(self, saved_partial: int) -> None:
        label = self._current_label or "unknown"
        self.emit("WARN", f"{label} response_cap_reached", indent_level=1)
        self.emit(
            "INFO",
            f"saved_partial={saved_partial}",
            indent_level=2,
        )

    def finish_target(
        self,
        status: str,
        label: str,
        total_collected: int,
        ref: str,
        reason: str | None = None,
    ) -> None:
        tag = {
            "success": "OK",
            "partial": "WARN",
            "fail": "ERROR",
        }[status]
        self.emit(
            tag,
            f"{label} {status} total_collected={total_collected}",
            indent_level=1,
        )
        if reason:
            self.emit("INFO", reason, indent_level=2)

        if status == "success":
            self._ok_targets += 1
        elif status == "partial":
            self._partial_targets += 1
            self._append_error_ref(ref)
        else:
            self._hard_fail_targets += 1
            self._append_error_ref(ref)

        self._current_label = None

    def _append_error_ref(self, ref: str) -> None:
        if ref not in self._error_refs:
            self._error_refs.append(ref)

    def finish_run(self, final_status: str) -> None:
        ended_at = self._now_provider()
        started_at = self._started_at or ended_at
        duration_seconds = int(max((ended_at - started_at).total_seconds(), 0))
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
