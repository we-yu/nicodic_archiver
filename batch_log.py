from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


LOG_DIR = Path("data") / "batch_runs"


@dataclass(frozen=True)
class BatchRunLog:
    run_id: str
    started_at: str
    log_path: Path


def start_batch_run_log(total_targets: int) -> BatchRunLog:
    now = datetime.now(timezone.utc)
    run_id = f"batch-{now.strftime('%Y%m%dT%H%M%S%fZ')}"
    started_at = now.isoformat()
    log_path = LOG_DIR / f"{run_id}.log"

    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        "START "
        f"run_id={run_id} "
        f"started_at={started_at} "
        f"total_targets={total_targets}\n",
        encoding="utf-8",
    )

    return BatchRunLog(run_id=run_id, started_at=started_at, log_path=log_path)


def append_batch_failure_detail(
    batch_log: BatchRunLog,
    target: str,
    reason: str,
) -> None:
    with batch_log.log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(
            f"FAIL target={target} "
            f"reason={reason}\n"
        )


def finish_batch_run_log(
    batch_log: BatchRunLog,
    total_targets: int,
    failed_targets: int,
) -> str:
    ended_at = datetime.now(timezone.utc).isoformat()
    final_status = determine_final_status(total_targets, failed_targets)

    with batch_log.log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(
            "END "
            f"run_id={batch_log.run_id} "
            f"started_at={batch_log.started_at} "
            f"ended_at={ended_at} "
            f"total_targets={total_targets} "
            f"failed_targets={failed_targets} "
            f"final_status={final_status}\n"
        )

    return final_status


def determine_final_status(total_targets: int, failed_targets: int) -> str:
    if failed_targets == 0:
        return "success"
    if failed_targets == total_targets:
        return "failure"
    return "partial_failure"
