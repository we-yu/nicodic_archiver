from datetime import date, datetime
from io import StringIO
import tarfile

from host_cron import HostCronReporter
from host_cron import compress_weekly_archives, rotate_active_log


class FixedClock:
    def __init__(self, values):
        self._values = list(values)

    def __call__(self):
        return self._values.pop(0)


def test_rotate_active_log_moves_previous_day_log_and_reopens_active(tmp_path):
    log_dir = tmp_path / "logs"
    log_path = log_dir / "host_cron.log"
    log_dir.mkdir()
    log_path.write_text(
        "[RUN] START 2026-04-01 23:50:00\n"
        "[RUN] END 2026-04-02 00:05:00 status=success\n",
        encoding="utf-8",
    )

    outcome = rotate_active_log(log_path, date(2026, 4, 2))

    rotated_path = log_dir / "host_cron.20260401.log"
    assert outcome.rotated_path == rotated_path
    assert outcome.warning is None
    assert rotated_path.read_text(encoding="utf-8").startswith("[RUN] START")
    assert log_path.exists()
    assert log_path.read_text(encoding="utf-8") == ""


def test_rotate_active_log_skips_same_day_active_file(tmp_path):
    log_dir = tmp_path / "logs"
    log_path = log_dir / "host_cron.log"
    log_dir.mkdir()
    log_path.write_text(
        "[RUN] START 2026-04-02 01:00:00\n",
        encoding="utf-8",
    )

    outcome = rotate_active_log(log_path, date(2026, 4, 2))

    assert outcome.rotated_path is None
    assert outcome.warning is None
    assert log_path.read_text(encoding="utf-8") == (
        "[RUN] START 2026-04-02 01:00:00\n"
    )


def test_compress_weekly_archives_keeps_recent_fourteen_days_raw(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    old_days = [date(2026, 3, 16), date(2026, 3, 18), date(2026, 3, 22)]
    recent_day = date(2026, 3, 27)

    for log_day in [*old_days, recent_day]:
        path = log_dir / f"host_cron.{log_day.strftime('%Y%m%d')}.log"
        path.write_text(
            f"[RUN] START {log_day.isoformat()} 01:00:00\n",
            encoding="utf-8",
        )

    warnings = compress_weekly_archives(log_dir, date(2026, 4, 9))

    archive_path = log_dir / "host_cron.20260316-20260322.tar.gz"
    assert warnings == []
    assert archive_path.exists()
    assert not (log_dir / "host_cron.20260316.log").exists()
    assert not (log_dir / "host_cron.20260318.log").exists()
    assert not (log_dir / "host_cron.20260322.log").exists()
    assert (log_dir / "host_cron.20260327.log").exists()

    with tarfile.open(archive_path, "r:gz") as archive:
        names = sorted(archive.getnames())

    assert names == [
        "host_cron.20260316.log",
        "host_cron.20260318.log",
        "host_cron.20260322.log",
    ]


def test_host_cron_reporter_emits_run_block_and_error_summary():
    stream = StringIO()
    reporter = HostCronReporter(
        stream,
        now_provider=FixedClock(
            [
                datetime(2026, 4, 9, 7, 0, 0),
                datetime(2026, 4, 9, 7, 0, 9),
            ]
        ),
    )

    reporter.begin_run()
    reporter.note_targets_loaded(2, "/app/data/nicodic.db")
    reporter.start_target(
        1,
        2,
        "UNIX",
        "https://dic.nicovideo.jp/a/694740",
    )
    reporter.page_progress(
        "https://dic.nicovideo.jp/b/a/694740/1-",
        30,
        30,
    )
    reporter.finish_target("success", "UNIX", 30, "694740")
    reporter.start_target(
        2,
        2,
        "FamilyMart",
        "https://dic.nicovideo.jp/a/218285",
    )
    reporter.later_page_interrupted(
        "https://dic.nicovideo.jp/b/a/218285/31-",
        "404",
        30,
    )
    reporter.finish_target("partial", "FamilyMart", 30, "218285")
    reporter.finish_run("success")

    text = stream.getvalue()
    assert "[RUN] START 2026-04-09 07:00:00" in text
    assert "  [STEP] 1/2 title=UNIX url=https://dic.nicovideo.jp/a/694740" in text
    assert (
        "    [INFO] page=https://dic.nicovideo.jp/b/a/694740/1- "
        "collected=30 total=30" in text
    )
    assert "  [WARN] FamilyMart later_page_interrupted" in text
    assert "[RUN] END 2026-04-09 07:00:09 status=partial_failure" in text
    assert "[SUMMARY] targets=2 ok=1 fail=1 duration=9s" in text
    assert "[ERROR SUMMARY] count=1 refs=218285" in text
