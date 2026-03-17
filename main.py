import os
import sys
import uuid
import time
from datetime import datetime, timezone
from pathlib import Path

from cli import inspect_article
from orchestrator import run_scrape
from target_list import load_target_urls


# ============================================================
# エントリポイント
# ============================================================


def run_batch_scrape(target_list_path: str) -> tuple[str, int]:
    """Run one full batch pass and return (final_status, failed_targets)."""

    targets = load_target_urls(target_list_path)

    print(f"Loaded {len(targets)} scrape target(s) from {target_list_path}")

    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(timezone.utc).isoformat()
    log_dir = Path(os.environ.get("BATCH_LOG_DIR", "data/batch_runs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"batch_{run_id}.log"

    with log_path.open("a", encoding="utf-8") as f:
        f.write("BATCH_RUN_START\n")
        f.write(f"run_id={run_id}\n")
        f.write(f"started_at={started_at}\n")
        f.write(f"target_list_path={target_list_path}\n")
        f.write(f"total_targets={len(targets)}\n")

    failed_targets = 0
    for idx, target in enumerate(targets, start=1):
        print(f"[{idx}/{len(targets)}] Scraping: {target}")
        try:
            ok = run_scrape(target)
        except Exception as exc:
            failed_targets += 1
            print(f"[FAIL] {target} ({type(exc).__name__}: {exc})")
            with log_path.open("a", encoding="utf-8") as f:
                f.write("FAIL\n")
                f.write(f"target={target}\n")
                f.write(f"short_reason={type(exc).__name__}: {exc}\n")
            continue

        if ok is False:
            failed_targets += 1
            print(f"[FAIL] {target}")
            with log_path.open("a", encoding="utf-8") as f:
                f.write("FAIL\n")
                f.write(f"target={target}\n")
                f.write("short_reason=run_scrape_returned_false\n")
            continue

        print(f"[OK] {target}")

    ended_at = datetime.now(timezone.utc).isoformat()
    total_targets = len(targets)
    if failed_targets == 0:
        final_status = "success"
    elif failed_targets == total_targets:
        final_status = "failure"
    else:
        final_status = "partial_failure"

    with log_path.open("a", encoding="utf-8") as f:
        f.write("BATCH_RUN_END\n")
        f.write(f"run_id={run_id}\n")
        f.write(f"started_at={started_at}\n")
        f.write(f"ended_at={ended_at}\n")
        f.write(f"total_targets={total_targets}\n")
        f.write(f"failed_targets={failed_targets}\n")
        f.write(f"final_status={final_status}\n")

    return final_status, failed_targets


def run_periodic_scrape(
    target_list_path: str,
    interval_seconds: float,
    max_runs: int | None = None,
) -> None:
    """Run full batch passes repeatedly with a fixed sleep interval."""

    completed_runs = 0

    while max_runs is None or completed_runs < max_runs:
        run_number = completed_runs + 1
        print(f"[periodic] Run {run_number} starting")

        try:
            final_status, failed_targets = run_batch_scrape(target_list_path)
        except KeyboardInterrupt:
            print("Periodic execution interrupted. Exiting safely.")
            return

        print(
            f"[periodic] Run {run_number} finished "
            f"with status={final_status} failed_targets={failed_targets}"
        )

        completed_runs += 1
        if max_runs is not None and completed_runs >= max_runs:
            return

        print(f"[periodic] Sleeping {interval_seconds} second(s)")
        try:
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("Periodic execution interrupted. Exiting safely.")
            return


def main():
    """
    CLIエントリポイント。
    - 通常: 記事URL指定でスクレイプ実行
    - inspect: DB内容表示
    """

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py <article_url>")
        print("  python main.py inspect <article_id> <article_type> [--last N]")
        print("  python main.py targets <target_list_path>")
        print("  python main.py batch <target_list_path>")
        print(
            "  python main.py periodic <target_list_path> <interval_seconds> "
            "[--max-runs N]"
        )
        sys.exit(1)

    # inspectモード
    if sys.argv[1] == "inspect":

        if len(sys.argv) < 4:
            print("Usage: inspect <article_id> <article_type> [--last N]")
            sys.exit(1)

        article_id = sys.argv[2]
        article_type = sys.argv[3]

        last_n = None
        if "--last" in sys.argv:
            idx = sys.argv.index("--last")
            last_n = int(sys.argv[idx + 1])

        inspect_article(article_id, article_type, last_n)
        return

    if sys.argv[1] == "targets":

        if len(sys.argv) < 3:
            print("Usage: targets <target_list_path>")
            sys.exit(1)

        target_list_path = sys.argv[2]
        targets = load_target_urls(target_list_path)

        print(f"Loaded {len(targets)} scrape target(s) from {target_list_path}")
        for target in targets:
            print(target)
        return

    if sys.argv[1] == "batch":

        if len(sys.argv) < 3:
            print("Usage: batch <target_list_path>")
            sys.exit(1)

        _, failed_targets = run_batch_scrape(sys.argv[2])

        if failed_targets:
            sys.exit(1)
        return

    if sys.argv[1] == "periodic":

        if len(sys.argv) < 4:
            print(
                "Usage: periodic <target_list_path> <interval_seconds> "
                "[--max-runs N]"
            )
            sys.exit(1)

        target_list_path = sys.argv[2]
        interval_seconds = float(sys.argv[3])

        max_runs = None
        if "--max-runs" in sys.argv:
            idx = sys.argv.index("--max-runs")
            max_runs = int(sys.argv[idx + 1])

        run_periodic_scrape(target_list_path, interval_seconds, max_runs=max_runs)
        return

    # 通常スクレイプモード
    article_url = sys.argv[1]

    run_scrape(article_url)


if __name__ == "__main__":
    main()
