import os
from datetime import date, datetime, timezone
from io import StringIO
import tarfile

from host_cron import HostCronReporter
from host_cron import compress_weekly_archives, parse_run_start_day, rotate_active_log


class FixedClock:
    def __init__(self, values):
        self._values = list(values)

    def __call__(self):
        return self._values.pop(0)


def set_mtime(path, moment):
    timestamp = moment.timestamp()
    os.utime(path, (timestamp, timestamp))


def test_parse_run_start_day_legacy_stamp():
    text = "[RUN] START 2026-04-01 23:50:00\n"
    assert parse_run_start_day(text) == date(2026, 4, 1)


def test_parse_run_start_day_compact_run_start_stamp():
    text = (
        "[RUN START] ts=2026-04-01T23:50:05Z "
        "run_id=20260401T235005Z batch_ref=z\n"
    )
    assert parse_run_start_day(text) == date(2026, 4, 1)


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


def test_compress_weekly_archives_writes_readme_logs_and_ignores_them(
    tmp_path,
):
    log_dir = tmp_path / "logs"
    batch_dir = log_dir / "batch_runs"
    batch_dir.mkdir(parents=True)

    host_old = log_dir / "host_cron.20260316.log"
    host_old.write_text("[RUN] START 2026-03-16 01:00:00\n", encoding="utf-8")

    batch_old = batch_dir / "batch_alpha.log"
    batch_old.write_text("batch\n", encoding="utf-8")
    set_mtime(batch_old, datetime(2026, 3, 16, 12, 0, 0))

    warnings = compress_weekly_archives(log_dir, date(2026, 4, 9))

    assert warnings == []

    host_readme = log_dir / "README.log"
    batch_readme = batch_dir / "README.log"
    assert host_readme.exists()
    assert batch_readme.exists()

    host_text = host_readme.read_text(encoding="utf-8")
    batch_text = batch_readme.read_text(encoding="utf-8")
    for token in [
        "DIGEST EXP",
        "RUN DIGEST",
        "B=",
        "dur=",
        "end=",
        "H=",
        "OK0=",
        "UOBS=",
        "P=",
        "T=",
        "R=",
    ]:
        assert token in host_text
    for token in [
        "DIGEST EXP",
        "BATCH_DIGEST",
        "BATCH_DIGEST_ITEMS",
        "H=",
        "OK0=",
        "UOBS=",
        "BATCH_LOG_VERBOSE=1",
        "batch_runs.YYYYMMDD-YYYYMMDD.tar.gz",
        "mtime",
    ]:
        assert token in batch_text

    host_archive = log_dir / "host_cron.20260316-20260322.tar.gz"
    batch_archive = batch_dir / "batch_runs.20260316-20260322.tar.gz"
    assert host_archive.exists()
    assert batch_archive.exists()

    with tarfile.open(host_archive, "r:gz") as archive:
        assert archive.getnames() == ["host_cron.20260316.log"]
    with tarfile.open(batch_archive, "r:gz") as archive:
        assert archive.getnames() == ["batch_alpha.log"]

    assert host_readme.read_text(encoding="utf-8").startswith("DIGEST EXP")
    assert batch_readme.read_text(encoding="utf-8").startswith("DIGEST EXP")


def test_compress_weekly_archives_keeps_recent_batch_runs_plain(tmp_path):
    log_dir = tmp_path / "logs"
    batch_dir = log_dir / "batch_runs"
    batch_dir.mkdir(parents=True)

    recent_path = batch_dir / "batch_19000101.log"
    recent_path.write_text("recent\n", encoding="utf-8")
    set_mtime(recent_path, datetime(2026, 3, 27, 12, 0, 0))

    warnings = compress_weekly_archives(log_dir, date(2026, 4, 9))

    assert warnings == []
    assert recent_path.exists()
    assert not (batch_dir / "batch_runs.20260323-20260329.tar.gz").exists()


def test_compress_weekly_archives_groups_batch_runs_by_mtime(tmp_path):
    log_dir = tmp_path / "logs"
    batch_dir = log_dir / "batch_runs"
    batch_dir.mkdir(parents=True)

    old_a = batch_dir / "batch_20991231.log"
    old_b = batch_dir / "batch_19000101.log"
    old_c = batch_dir / "batch_77777777.log"
    recent = batch_dir / "batch_11111111.log"
    for path, moment in [
        (old_a, datetime(2026, 3, 16, 12, 0, 0)),
        (old_b, datetime(2026, 3, 18, 12, 0, 0)),
        (old_c, datetime(2026, 3, 22, 12, 0, 0)),
        (recent, datetime(2026, 3, 27, 12, 0, 0)),
    ]:
        path.write_text(f"{path.name}\n", encoding="utf-8")
        set_mtime(path, moment)

    warnings = compress_weekly_archives(log_dir, date(2026, 4, 9))

    archive_path = batch_dir / "batch_runs.20260316-20260322.tar.gz"
    assert warnings == []
    assert archive_path.exists()
    assert not old_a.exists()
    assert not old_b.exists()
    assert not old_c.exists()
    assert recent.exists()

    with tarfile.open(archive_path, "r:gz") as archive:
        names = sorted(archive.getnames())

    assert names == [
        "batch_19000101.log",
        "batch_20991231.log",
        "batch_77777777.log",
    ]


def test_compress_weekly_archives_ignores_batch_run_tar_gz_inputs(tmp_path):
    log_dir = tmp_path / "logs"
    batch_dir = log_dir / "batch_runs"
    batch_dir.mkdir(parents=True)

    tar_path = batch_dir / "batch_runs.20260316-20260322.tar.gz"
    tar_path.write_bytes(b"placeholder")

    warnings = compress_weekly_archives(log_dir, date(2026, 4, 9))

    assert warnings == []
    assert tar_path.exists()


def test_compress_weekly_archives_no_batch_run_candidates_is_safe(tmp_path):
    log_dir = tmp_path / "logs"
    (log_dir / "batch_runs").mkdir(parents=True)

    warnings = compress_weekly_archives(log_dir, date(2026, 4, 9))

    assert warnings == []


def test_compress_weekly_archives_is_idempotent_for_batch_runs(tmp_path):
    log_dir = tmp_path / "logs"
    batch_dir = log_dir / "batch_runs"
    batch_dir.mkdir(parents=True)

    path = batch_dir / "batch_abc.log"
    path.write_text("old\n", encoding="utf-8")
    set_mtime(path, datetime(2026, 3, 16, 12, 0, 0))

    first = compress_weekly_archives(log_dir, date(2026, 4, 9))
    second = compress_weekly_archives(log_dir, date(2026, 4, 9))

    assert first == []
    assert second == []
    assert not path.exists()
    assert (batch_dir / "batch_runs.20260316-20260322.tar.gz").exists()


def test_compress_weekly_archives_keeps_batch_run_logs_when_archive_fails(
    tmp_path,
    monkeypatch,
):
    log_dir = tmp_path / "logs"
    batch_dir = log_dir / "batch_runs"
    batch_dir.mkdir(parents=True)

    path = batch_dir / "batch_fail.log"
    path.write_text("old\n", encoding="utf-8")
    set_mtime(path, datetime(2026, 3, 16, 12, 0, 0))

    def fail_open(*args, **kwargs):
        raise tarfile.TarError("boom")

    monkeypatch.setattr("host_cron.tarfile.open", fail_open)

    warnings = compress_weekly_archives(log_dir, date(2026, 4, 9))

    assert warnings
    assert path.exists()
    assert not (batch_dir / "batch_runs.20260316-20260322.tar.gz").exists()


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
    # Normal-success per-page INFO lines are intentionally compacted out
    # of host_cron output. Warning / error detail (later_page_interrupted)
    # below is still emitted with its INFO detail line.
    assert (
        "    [INFO] page=https://dic.nicovideo.jp/b/a/694740/1- "
        "collected=30 total=30"
    ) not in text
    assert "  [WARN] FamilyMart later_page_interrupted" in text
    assert (
        "    [INFO] page=https://dic.nicovideo.jp/b/a/218285/31- "
        "status=404 saved_partial=30"
    ) in text
    assert "[RUN] END 2026-04-09 07:00:09 status=partial_failure" in text
    assert "[SUMMARY] targets=2 ok=1 fail=1 duration=9s" in text
    assert "[ERROR SUMMARY] count=1 refs=218285" in text


def test_compact_host_run_groups_page_tokens_and_hashes_step_end_shapes():
    started = datetime(2026, 4, 9, 7, 0, 5, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 4, 9, 10, 0, 5),
            datetime(2026, 4, 9, 10, 0, 44),
            datetime(2026, 4, 9, 10, 5, 0),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())
    iso = started.isoformat().replace("+00:00", "Z")

    reporter.begin_compact_host_run(
        started_at_iso=iso,
        batch_ref="ba9cafe12345",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(1, "/app/data/registry.db")

    reporter.start_target(
        1,
        1,
        "Sample",
        "https://dic.nicovideo.jp/a/694740",
        article_id="694740",
        saved_before=0,
        observed_before="unknown",
    )
    root = "https://dic.nicovideo.jp/b/a/694740/"
    for k in range(14):
        n = k * 30 + 1
        reporter.page_progress(f"{root}{n}-", 30, (k + 1) * 30)
    reporter.finish_target(
        "success",
        "Sample",
        400,
        "694740",
        reason=None,
        stored_new=120,
        saved_after=400,
        observed_after=400,
        elapsed_s=40,
        pages_ok=None,
    )
    reporter.bind_run_totals(
        total_targets=1,
        processed_targets=1,
        remaining_targets=0,
    )
    reporter.finish_run("success")

    text = stream.getvalue()
    assert "[RUN START]" in text
    page_lines = [ln for ln in text.splitlines() if "[PAGE]" in ln]
    assert len(page_lines) >= 2
    assert "[STEP START]" in text
    assert " url=" in text.split("[STEP START]", 1)[1].split("\n", 1)[0]
    for ln in text.splitlines():
        if "STEP END" in ln:
            assert " url=" not in ln
            assert ("OK 🟢" in ln or "WARN 🟡" in ln or "FAIL 🔴" in ln)
    assert "[RUN DIGEST]" in text


def test_compact_host_run_folds_zero_response_checked_into_ok0_sum():
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 45),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="zerochk",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(1, "/app/data/registry.db")
    reporter.start_target(
        7,
        100,
        "十段戦(麻雀)",
        "https://dic.nicovideo.jp/a/judan",
        article_id="judan",
        saved_before=0,
        observed_before="unknown",
    )
    reporter.finish_target(
        "success",
        "十段戦(麻雀)",
        0,
        "judan",
        reason="reason=zero_response_checked",
        stored_new=0,
        saved_after=None,
        observed_after=None,
        pages_ok=0,
        elapsed_s=1,
    )
    reporter.bind_run_totals(
        total_targets=1,
        processed_targets=1,
        remaining_targets=0,
    )
    reporter.finish_run("success")

    text = stream.getvalue()
    assert "[OK0 SUM 🟢]" in text
    assert "[STEP END OK 🟢]" not in text
    assert "[STEP START]" not in text
    assert "[OK0] others=1" in text
    assert "reason=zero_response_checked" not in text


def test_compact_host_run_does_not_fold_generic_success_ok_reason():
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 42),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="plainok",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.start_target(
        1,
        1,
        "Mystery",
        "https://dic.nicovideo.jp/a/1",
        article_id="1",
        saved_before=0,
        observed_before="unknown",
    )
    reporter.finish_target(
        "success",
        "Mystery",
        0,
        "1",
        reason=None,
        stored_new=0,
        saved_after=None,
        observed_after=None,
        pages_ok=0,
        elapsed_s=0,
    )

    text = stream.getvalue()
    assert "[STEP END OK 🟢]" in text
    assert "reason=ok" in text
    assert "[OK0 SUM 🟢]" not in text


def test_compact_host_run_summarizes_clean_ok0_target_by_default():
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 45),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="ok0batch",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(1, "/app/data/registry.db")
    reporter.start_target(
        44,
        12192,
        "巨影都市",
        "https://dic.nicovideo.jp/a/5492955",
        article_id="5492955",
        saved_before=514,
        observed_before="514",
    )
    reporter.page_progress(
        "https://dic.nicovideo.jp/b/a/5492955/511-",
        514,
        514,
    )
    reporter.finish_target(
        "success",
        "巨影都市",
        514,
        "5492955",
        reason="already_up_to_date",
        stored_new=0,
        saved_after=514,
        observed_after=514,
        pages_ok=1,
        elapsed_s=0,
    )
    reporter.bind_run_totals(
        total_targets=1,
        processed_targets=1,
        remaining_targets=0,
    )
    reporter.finish_run("success")

    text = stream.getvalue()
    assert "[STEP OK0 🟢]" not in text
    ok0_line = next(
        line for line in text.splitlines() if "[OK0 SUM 🟢]" in line
    )
    assert "steps=44-44/12192" in ok0_line
    assert "cnt=1" in ok0_line
    assert "total_ok0=1" in ok0_line
    assert "last_id=5492955" in ok0_line
    assert "last_page=511" in ok0_line
    assert "elapsed=" in ok0_line
    assert "[STEP START]" not in text
    assert "[PAGE]" not in text
    assert "[STEP END" not in text
    assert "OK0=1" in text
    assert "[OK0] others=1" in text


def test_compact_host_run_ok0_mode_line_keeps_legacy_per_target_line(monkeypatch):
    monkeypatch.setenv("HOST_CRON_OK0_MODE", "line")
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 45),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="ok0line",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(1, "/app/data/registry.db")
    reporter.start_target(
        5,
        10,
        "巨影都市",
        "https://dic.nicovideo.jp/a/5492955",
        article_id="5492955",
        saved_before=514,
        observed_before="514",
    )
    reporter.page_progress(
        "https://dic.nicovideo.jp/b/a/5492955/511-",
        514,
        514,
    )
    reporter.finish_target(
        "success",
        "巨影都市",
        514,
        "5492955",
        reason="already_up_to_date",
        stored_new=0,
        saved_after=514,
        observed_after=514,
        pages_ok=1,
        elapsed_s=0,
    )
    reporter.finish_run("success")

    text = stream.getvalue()
    assert "[STEP OK0 🟢]" in text
    assert "[OK0 SUM 🟢]" not in text


def test_compact_host_run_ok0_sum_interval_groups_targets(monkeypatch):
    monkeypatch.setenv("HOST_CRON_OK0_SUM_EVERY", "2")
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 41),
            datetime(2026, 5, 17, 13, 21, 44),
            datetime(2026, 5, 17, 13, 21, 48),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="ok0sum",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(3, "/app/data/registry.db")
    for idx, aid, page in [
        (1, "101", "1-"),
        (2, "102", "31-"),
        (3, "103", "61-"),
    ]:
        reporter.start_target(
            idx,
            3,
            f"T{aid}",
            f"https://dic.nicovideo.jp/a/{aid}",
            article_id=aid,
            saved_before=10,
            observed_before="10",
        )
        reporter.page_progress(
            f"https://dic.nicovideo.jp/b/a/{aid}/{page}",
            10,
            10,
        )
        reporter.finish_target(
            "success",
            f"T{aid}",
            10,
            aid,
            reason="already_up_to_date",
            stored_new=0,
            saved_after=10,
            observed_after=10,
            pages_ok=1,
            elapsed_s=0,
        )
    reporter.finish_run("success")

    lines = [ln for ln in stream.getvalue().splitlines() if "[OK0 SUM 🟢]" in ln]
    assert len(lines) == 2
    assert "steps=1-2/3" in lines[0]
    assert "cnt=2" in lines[0]
    assert "total_ok0=2" in lines[0]
    assert "steps=3-3/3" in lines[1]
    assert "cnt=1" in lines[1]
    assert "total_ok0=3" in lines[1]


def test_compact_host_run_flushes_pending_ok0_sum_before_hit_detail(monkeypatch):
    monkeypatch.setenv("HOST_CRON_OK0_MODE", "sum")
    monkeypatch.setenv("HOST_CRON_OK0_SUM_EVERY", "250")
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 40),
            datetime(2026, 5, 17, 13, 21, 42),
            datetime(2026, 5, 17, 13, 21, 49),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="ok0flush",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(2, "/app/data/registry.db")
    reporter.start_target(
        1,
        2,
        "OK0",
        "https://dic.nicovideo.jp/a/1",
        article_id="1",
        saved_before=10,
        observed_before="10",
    )
    reporter.page_progress("https://dic.nicovideo.jp/b/a/1/1-", 10, 10)
    reporter.finish_target(
        "success",
        "OK0",
        10,
        "1",
        reason="already_up_to_date",
        stored_new=0,
        saved_after=10,
        observed_after=10,
        pages_ok=1,
        elapsed_s=0,
    )

    reporter.start_target(
        2,
        2,
        "HIT",
        "https://dic.nicovideo.jp/a/2",
        article_id="2",
        saved_before=10,
        observed_before="40",
    )
    reporter.page_progress("https://dic.nicovideo.jp/b/a/2/11-", 30, 40)
    reporter.page_progress("https://dic.nicovideo.jp/b/a/2/41-", 10, 50)
    reporter.finish_target(
        "success",
        "HIT",
        50,
        "2",
        reason=None,
        stored_new=40,
        saved_after=50,
        observed_after=50,
        pages_ok=2,
        elapsed_s=1,
    )
    reporter.finish_run("success")

    text = stream.getvalue()
    ok0_pos = text.index("[OK0 SUM 🟢]")
    step_start_pos = text.index("[STEP START]")
    assert ok0_pos < step_start_pos
    assert "[STEP END OK 🟢]" in text


def test_compact_host_run_invalid_ok0_mode_falls_back_to_sum(monkeypatch):
    monkeypatch.setenv("HOST_CRON_OK0_MODE", "invalid")
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 45),
            datetime(2026, 5, 17, 13, 21, 50),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="ok0invalid",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(1, "/app/data/registry.db")
    reporter.start_target(
        1,
        1,
        "OK0",
        "https://dic.nicovideo.jp/a/1",
        article_id="1",
        saved_before=10,
        observed_before="10",
    )
    reporter.page_progress("https://dic.nicovideo.jp/b/a/1/1-", 10, 10)
    reporter.finish_target(
        "success",
        "OK0",
        10,
        "1",
        reason="already_up_to_date",
        stored_new=0,
        saved_after=10,
        observed_after=10,
        pages_ok=1,
        elapsed_s=0,
    )
    reporter.finish_run("success")

    text = stream.getvalue()
    assert "[OK0 SUM 🟢]" in text
    assert "[STEP OK0 🟢]" not in text


def test_compact_host_run_invalid_ok0_sum_every_falls_back_to_250(monkeypatch):
    monkeypatch.setenv("HOST_CRON_OK0_SUM_EVERY", "bad")
    reporter = HostCronReporter(StringIO(), now_provider=FixedClock([]))
    assert reporter._ok0_sum_every == 250


def test_compact_host_run_hit_target_keeps_detailed_step_lines():
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 41),
            datetime(2026, 5, 17, 13, 21, 50),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="hitbatch",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(1, "/app/data/registry.db")
    reporter.start_target(
        1,
        1,
        "Sample",
        "https://dic.nicovideo.jp/a/694740",
        article_id="694740",
        saved_before=280,
        observed_before="400",
    )
    reporter.page_progress(
        "https://dic.nicovideo.jp/b/a/694740/391-",
        30,
        400,
    )
    reporter.finish_target(
        "success",
        "Sample",
        430,
        "694740",
        reason=None,
        stored_new=30,
        saved_after=430,
        observed_after=430,
        pages_ok=1,
        elapsed_s=2,
    )

    text = stream.getvalue()
    assert "[STEP START]" in text
    assert "[PAGE] [391 OK]" in text
    assert "[STEP END OK 🟢]" in text
    assert "stored_new=30" in text
    assert "[STEP OK0 🟢]" not in text


def test_compact_host_run_warn_target_keeps_detailed_step_lines():
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 42),
            datetime(2026, 5, 17, 13, 21, 51),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="warnbatch",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(1, "/app/data/registry.db")
    reporter.start_target(
        3,
        3,
        "FamilyMart",
        "https://dic.nicovideo.jp/a/218285",
        article_id="218285",
        saved_before=30,
        observed_before="unknown",
    )
    reporter.page_progress(
        "https://dic.nicovideo.jp/b/a/218285/1-",
        30,
        30,
    )
    reporter.later_page_interrupted(
        "https://dic.nicovideo.jp/b/a/218285/31-",
        "404",
        30,
    )
    reporter.finish_target(
        "partial",
        "FamilyMart",
        30,
        "218285",
        reason="later_page_interrupted",
        stored_new=0,
        saved_after=30,
        observed_after="unknown",
        pages_ok=1,
        elapsed_s=3,
    )

    text = stream.getvalue()
    assert "[STEP START]" in text
    assert "[PAGE] [1 OK][31 ERR404]" in text
    assert "[WARN DETAIL]" in text
    assert "[STEP END WARN 🟡]" in text
    assert "status=404" in text
    assert "[STEP OK0 🟢]" not in text


def test_compact_host_run_digest_keeps_ok0_and_other_counters():
    started = datetime(2026, 5, 17, 4, 21, 39, tzinfo=timezone.utc)
    clock = FixedClock(
        [
            datetime(2026, 5, 17, 13, 21, 39),
            datetime(2026, 5, 17, 13, 21, 40),
            datetime(2026, 5, 17, 13, 21, 41),
            datetime(2026, 5, 17, 13, 21, 50),
        ]
    )
    stream = StringIO()
    reporter = HostCronReporter(stream, now_provider=lambda: clock())

    reporter.begin_compact_host_run(
        started_at_iso=started.isoformat().replace("+00:00", "Z"),
        batch_ref="mixbatch",
        archive_db_path="/app/data/nicodic.db",
        limit_seconds=7200,
        trigger="host_cron",
    )
    reporter.note_targets_loaded(2, "/app/data/registry.db")
    reporter.start_target(
        1,
        2,
        "巨影都市",
        "https://dic.nicovideo.jp/a/5492955",
        article_id="5492955",
        saved_before=514,
        observed_before="514",
    )
    reporter.page_progress(
        "https://dic.nicovideo.jp/b/a/5492955/511-",
        514,
        514,
    )
    reporter.finish_target(
        "success",
        "巨影都市",
        514,
        "5492955",
        reason="already_up_to_date",
        stored_new=0,
        saved_after=514,
        observed_after=514,
        pages_ok=1,
        elapsed_s=0,
    )
    reporter.start_target(
        2,
        2,
        "Sample",
        "https://dic.nicovideo.jp/a/694740",
        article_id="694740",
        saved_before=280,
        observed_before="400",
    )
    reporter.page_progress(
        "https://dic.nicovideo.jp/b/a/694740/391-",
        30,
        400,
    )
    reporter.finish_target(
        "success",
        "Sample",
        430,
        "694740",
        reason=None,
        stored_new=30,
        saved_after=430,
        observed_after=430,
        pages_ok=1,
        elapsed_s=1,
    )
    reporter.bind_run_totals(
        total_targets=2,
        processed_targets=2,
        remaining_targets=0,
    )
    reporter.finish_run("success")

    text = stream.getvalue()
    digest_line = next(
        line for line in text.splitlines() if "[RUN DIGEST]" in line
    )
    assert "[RUN DIGEST] B=mixbatch" in text
    assert "dur=" in digest_line
    assert "s end=success" in digest_line
    assert "H=1" in digest_line
    assert "OK0=1" in digest_line
    assert "W=0" in digest_line
    assert "F=0" in digest_line
    assert "S=0" in digest_line
    assert "NEW=30" in digest_line
    assert "P=2 T=2 R=0" in digest_line
    assert "[OK0 SUM 🟢]" in text
    assert "[STEP OK0 🟢]" not in text
    assert "[OK0] others=1" in text
