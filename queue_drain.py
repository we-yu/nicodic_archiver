from orchestrator import run_scrape_outcome
from storage import init_db, list_queue_requests, remove_queue_request


QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP = 10_800


def drain_queue_requests(limit: int | None = None) -> dict:
    """Drain queued requests sequentially with queue-drain-only cap policy."""

    conn = init_db()
    try:
        queued_requests = list_queue_requests(conn, limit=limit)
    finally:
        conn.close()

    dequeued = 0
    kept = 0
    results = []

    for request in queued_requests:
        try:
            outcome = run_scrape_outcome(
                request["article_url"],
                response_cap=QUEUE_DRAIN_PER_ARTICLE_RESPONSE_CAP,
            )
        except Exception as exc:
            kept += 1
            results.append(
                {
                    "article_id": request["article_id"],
                    "article_type": request["article_type"],
                    "result": "kept",
                    "detail": f"{type(exc).__name__}: {exc}",
                }
            )
            continue

        if outcome["ok"]:
            conn = init_db()
            try:
                remove_queue_request(
                    conn,
                    request["article_id"],
                    request["article_type"],
                )
            finally:
                conn.close()

            dequeued += 1
            results.append(
                {
                    "article_id": request["article_id"],
                    "article_type": request["article_type"],
                    "result": "dequeued",
                    "cap_reached": outcome["cap_reached"],
                }
            )
            continue

        kept += 1
        results.append(
            {
                "article_id": request["article_id"],
                "article_type": request["article_type"],
                "result": "kept",
                "cap_reached": outcome["cap_reached"],
            }
        )

    return {
        "processed": len(queued_requests),
        "dequeued": dequeued,
        "kept": kept,
        "results": results,
    }
