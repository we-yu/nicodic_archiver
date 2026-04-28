"""Canonical URL identity merge for article_type='a'.

Bounded maintenance seam. It detects duplicate canonical_url groups in the
articles table for article_type='a' (e.g. an old numeric article_id plus a
new slug article_id derived from /a/<title> sharing the same canonical
URL), chooses a safe keep identity (the slug form), preserves old numeric
identity responses by transferring missing res_nos into the keep identity,
and only after that preservation is complete and verified cleans the old
numeric article and response rows. Target rows for the old numeric
identity are normalized non-destructively (deactivated).

Safety properties:
- explicit DB path; no implicit runtime DB default
- dry-run is the safe default
- never reintroduces article_type='id'
- only operates on rows that share a canonical_url with another
  article_type='a' row; non-duplicate rows are never touched
- destructive cleanup of source articles / responses runs only after
  per-source response preservation is verified
- target rows are deactivated, never deleted
"""
import sqlite3
from pathlib import Path
from urllib.parse import urlparse


def _is_numeric_identity(article_id: str) -> bool:
    return bool(article_id) and article_id.isdigit()


def _slug_from_canonical_url(canonical_url: str | None) -> str | None:
    if not canonical_url:
        return None
    parsed = urlparse(canonical_url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) != 2 or parts[0] != "a":
        return None
    slug = parts[1]
    return slug or None


def find_canonical_url_duplicate_groups(conn) -> list[dict]:
    """Return duplicate canonical_url groups for article_type='a'.

    Read-only. Each returned group has 2 or more rows.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT article_id, article_type, canonical_url
        FROM articles
        WHERE article_type='a'
              AND canonical_url IS NOT NULL
              AND canonical_url <> ''
        ORDER BY canonical_url ASC, article_id ASC
        """
    )
    by_url: dict[str, list[dict]] = {}
    for article_id, article_type, canonical_url in cur.fetchall():
        by_url.setdefault(canonical_url, []).append(
            {
                "article_id": article_id,
                "article_type": article_type,
                "canonical_url": canonical_url,
            }
        )

    groups: list[dict] = []
    for canonical_url, rows in by_url.items():
        if len(rows) < 2:
            continue
        groups.append({"canonical_url": canonical_url, "rows": rows})
    groups.sort(key=lambda group: group["canonical_url"])
    return groups


def choose_keep_identity(group: dict) -> dict | None:
    """Pick the row whose article_id matches the canonical_url slug.

    Returns None when no row matches; the caller treats that group as
    unsafe to auto-merge and leaves it for operator review.
    """
    expected_slug = _slug_from_canonical_url(group["canonical_url"])
    if expected_slug is None:
        return None

    for row in group["rows"]:
        if row["article_id"] == expected_slug and row["article_type"] == "a":
            return row
    return None


def _list_response_res_nos(
    cur, article_id: str, article_type: str
) -> set[int]:
    cur.execute(
        """
        SELECT res_no FROM responses
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    return {int(row[0]) for row in cur.fetchall()}


def _count_responses(cur, article_id: str, article_type: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*) FROM responses
        WHERE article_id=? AND article_type=?
        """,
        (article_id, article_type),
    )
    return int(cur.fetchone()[0])


def _build_group_plan(conn, group: dict) -> dict:
    keep = choose_keep_identity(group)

    if keep is None:
        sources = []
        for row in group["rows"]:
            sources.append(
                {
                    "article_id": row["article_id"],
                    "article_type": row["article_type"],
                    "skip_reason": "no_safe_keep_identity",
                    "source_response_count": 0,
                    "responses_to_transfer": 0,
                }
            )
        return {
            "canonical_url": group["canonical_url"],
            "keep_identity": None,
            "sources": sources,
            "skip_reason": "no_safe_keep_identity",
        }

    cur = conn.cursor()
    keep_res_nos = _list_response_res_nos(
        cur, keep["article_id"], keep["article_type"]
    )

    sources = []
    for row in group["rows"]:
        if (
            row["article_id"] == keep["article_id"]
            and row["article_type"] == keep["article_type"]
        ):
            continue

        if row["article_type"] != "a":
            sources.append(
                {
                    "article_id": row["article_id"],
                    "article_type": row["article_type"],
                    "skip_reason": "non_a_article_type",
                    "source_response_count": _count_responses(
                        cur, row["article_id"], row["article_type"]
                    ),
                    "responses_to_transfer": 0,
                }
            )
            continue

        if not _is_numeric_identity(row["article_id"]):
            sources.append(
                {
                    "article_id": row["article_id"],
                    "article_type": row["article_type"],
                    "skip_reason": "non_numeric_source_kept_safe",
                    "source_response_count": _count_responses(
                        cur, row["article_id"], row["article_type"]
                    ),
                    "responses_to_transfer": 0,
                }
            )
            continue

        source_res_nos = _list_response_res_nos(
            cur, row["article_id"], row["article_type"]
        )
        sources.append(
            {
                "article_id": row["article_id"],
                "article_type": row["article_type"],
                "skip_reason": None,
                "source_response_count": len(source_res_nos),
                "responses_to_transfer": len(
                    source_res_nos - keep_res_nos
                ),
            }
        )

    return {
        "canonical_url": group["canonical_url"],
        "keep_identity": {
            "article_id": keep["article_id"],
            "article_type": keep["article_type"],
        },
        "sources": sources,
        "skip_reason": None,
    }


def plan_canonical_url_merge(conn) -> list[dict]:
    """Return the per-group merge plan. Read-only."""

    groups = find_canonical_url_duplicate_groups(conn)
    return [_build_group_plan(conn, group) for group in groups]


def _transfer_missing_responses(
    conn, source: dict, keep: dict
) -> dict:
    """Copy source responses missing at keep into keep identity.

    Uses INSERT OR IGNORE keyed on UNIQUE(article_id, article_type, res_no)
    so an existing keep res_no is never overwritten or duplicated.
    Returns sets used for verification.
    """
    cur = conn.cursor()
    keep_res_nos_before = _list_response_res_nos(
        cur, keep["article_id"], keep["article_type"]
    )
    source_res_nos = _list_response_res_nos(
        cur, source["article_id"], source["article_type"]
    )

    cur.execute(
        """
        INSERT OR IGNORE INTO responses
        (article_id, article_type, res_no, id_hash, poster_name,
         posted_at, content_html, content_text, res_hidden,
         idhash_hidden, good_count, bad_count, scraped_at)
        SELECT ?, ?, res_no, id_hash, poster_name, posted_at,
               content_html, content_text, res_hidden,
               idhash_hidden, good_count, bad_count, scraped_at
        FROM responses
        WHERE article_id=? AND article_type=?
        """,
        (
            keep["article_id"],
            keep["article_type"],
            source["article_id"],
            source["article_type"],
        ),
    )

    keep_res_nos_after = _list_response_res_nos(
        cur, keep["article_id"], keep["article_type"]
    )
    transferred = keep_res_nos_after - keep_res_nos_before

    return {
        "source_response_count": len(source_res_nos),
        "missing_at_keep_before": len(
            source_res_nos - keep_res_nos_before
        ),
        "transferred": len(transferred),
        "source_res_nos": source_res_nos,
        "keep_res_nos_after": keep_res_nos_after,
    }


def _preservation_verified(transfer_stats: dict) -> bool:
    return transfer_stats["source_res_nos"].issubset(
        transfer_stats["keep_res_nos_after"]
    )


def _delete_old_numeric_article_rows(conn, source: dict) -> dict:
    """Delete source articles + responses. Caller verified preservation."""

    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM responses
        WHERE article_id=? AND article_type=?
        """,
        (source["article_id"], source["article_type"]),
    )
    deleted_responses = cur.rowcount
    cur.execute(
        """
        DELETE FROM articles
        WHERE article_id=? AND article_type=?
        """,
        (source["article_id"], source["article_type"]),
    )
    deleted_articles = cur.rowcount
    return {
        "deleted_articles": deleted_articles,
        "deleted_responses": deleted_responses,
    }


def _normalize_target_row(conn, source: dict) -> str:
    """Deactivate the source target row; never deletes target.

    Returns one of: 'deactivated', 'already_inactive', 'absent'.
    """

    cur = conn.cursor()
    cur.execute(
        """
        SELECT is_active FROM target
        WHERE article_id=? AND article_type=?
        """,
        (source["article_id"], source["article_type"]),
    )
    row = cur.fetchone()
    if row is None:
        return "absent"
    if not row[0]:
        return "already_inactive"
    cur.execute(
        """
        UPDATE target SET is_active=0
        WHERE article_id=? AND article_type=?
        """,
        (source["article_id"], source["article_type"]),
    )
    return "deactivated"


def _empty_source_entry(src_plan: dict) -> dict:
    return {
        "article_id": src_plan["article_id"],
        "article_type": src_plan["article_type"],
        "skip_reason": src_plan.get("skip_reason"),
        "source_response_count": src_plan.get("source_response_count", 0),
        "transferred": 0,
        "verification": "skipped",
        "deleted_articles": 0,
        "deleted_responses": 0,
        "target_normalization": "skipped",
    }


def apply_canonical_url_merge(
    conn, *, dry_run: bool = True
) -> dict:
    """Plan or apply canonical URL identity merge.

    Always returns a structured summary. When ``dry_run`` is True
    (default), no DB writes are performed. When False, missing source
    responses are first transferred into the keep identity. Cleanup of
    old numeric source articles and responses runs only when the
    transfer is verified for that source. Target rows for the source
    identity are deactivated, never deleted.
    """

    plan = plan_canonical_url_merge(conn)
    summary: dict = {"dry_run": dry_run, "groups": []}

    for group_plan in plan:
        group_summary = {
            "canonical_url": group_plan["canonical_url"],
            "keep_identity": group_plan["keep_identity"],
            "skip_reason": group_plan["skip_reason"],
            "sources": [],
        }

        if group_plan["skip_reason"] is not None:
            for src_plan in group_plan["sources"]:
                group_summary["sources"].append(_empty_source_entry(src_plan))
            summary["groups"].append(group_summary)
            continue

        keep = group_plan["keep_identity"]
        for src_plan in group_plan["sources"]:
            entry = _empty_source_entry(src_plan)

            if src_plan.get("skip_reason") is not None:
                group_summary["sources"].append(entry)
                continue

            if dry_run:
                entry["transferred"] = src_plan["responses_to_transfer"]
                entry["verification"] = "dry_run"
                group_summary["sources"].append(entry)
                continue

            stats = _transfer_missing_responses(conn, src_plan, keep)
            entry["transferred"] = stats["transferred"]

            if _preservation_verified(stats):
                entry["verification"] = "ok"
                cleanup = _delete_old_numeric_article_rows(conn, src_plan)
                entry["deleted_articles"] = cleanup["deleted_articles"]
                entry["deleted_responses"] = cleanup["deleted_responses"]
                entry["target_normalization"] = _normalize_target_row(
                    conn, src_plan
                )
            else:
                entry["verification"] = "failed_no_cleanup"

            group_summary["sources"].append(entry)

        summary["groups"].append(group_summary)

    if not dry_run:
        conn.commit()

    return summary


def merge_canonical_url_identities(
    db_path: str, *, apply: bool = False
) -> dict:
    """Operator-facing seam.

    Requires an explicit DB path. ``apply=False`` (default) is a safe
    dry-run; the runtime DB is never modified by accident.
    """

    if not db_path:
        raise ValueError(
            "explicit db_path is required; refusing implicit default"
        )
    if not Path(db_path).exists():
        raise FileNotFoundError(f"db_path does not exist: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        return apply_canonical_url_merge(conn, dry_run=not apply)
    finally:
        conn.close()


def format_merge_summary_lines(db_path: str, summary: dict) -> list[str]:
    """Human-readable lines for the operator. Stable, line-oriented."""

    mode = "dry-run" if summary["dry_run"] else "apply"
    lines = [
        "=== CANONICAL URL IDENTITY MERGE ===",
        f"DB: {db_path}",
        f"Mode: {mode}",
        f"Groups: {len(summary['groups'])}",
    ]

    for group in summary["groups"]:
        lines.append(f"- canonical_url: {group['canonical_url']}")
        keep = group["keep_identity"]
        if keep is None:
            lines.append(f"  keep_identity: none ({group['skip_reason']})")
        else:
            lines.append(
                f"  keep_identity: {keep['article_id']} {keep['article_type']}"
            )

        for src in group["sources"]:
            lines.append(
                "  source: "
                f"{src['article_id']} {src['article_type']} | "
                f"source_responses={src['source_response_count']} | "
                f"transferred={src['transferred']} | "
                f"verification={src['verification']} | "
                f"deleted_articles={src['deleted_articles']} | "
                f"deleted_responses={src['deleted_responses']} | "
                f"target={src['target_normalization']}"
                + (
                    f" | skip_reason={src['skip_reason']}"
                    if src.get("skip_reason")
                    else ""
                )
            )

    return lines
