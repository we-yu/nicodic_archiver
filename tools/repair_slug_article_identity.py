"""
Repair legacy slug article_id rows for article_type='a' archives.

Problem:
- Legacy rows may have article_type='a' but article_id is a URL-encoded
  /a/<slug> value (non-digits).

Goal:
- Normalize those groups so saved archive identity uses the numeric
  NicoNicoPedia article ID (digits-only string), while preserving:
  - article_type = 'a'
  - canonical_url = canonical /a/<slug> URL

Safety:
- explicit db_path required; never uses runtime defaults
- dry-run is default
- apply requires explicit flag
- apply runs in a transaction; failures rollback
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from orchestrator import fetch_article_metadata_record  # noqa: E402
from storage import validate_saved_article_identity  # noqa: E402


def _is_digits_only(value: str) -> bool:
    return bool(value) and value.isdigit()


def _canonical_url_key(canonical_url: str | None) -> str | None:
    if not canonical_url:
        return None
    text = canonical_url.strip()
    return text if text else None


def _is_canonical_a_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc != "dic.nicovideo.jp":
        return False
    parts = [part for part in parsed.path.split("/") if part]
    return len(parts) == 2 and parts[0] == "a" and bool(parts[1])


def _resolve_numeric_article_id_from_db_rows(rows: list[dict]) -> str | None:
    for row in rows:
        article_id = row.get("article_id") or ""
        if _is_digits_only(article_id):
            return article_id
    return None


def _resolve_numeric_article_id_from_network(canonical_url: str) -> str | None:
    record = fetch_article_metadata_record(canonical_url)
    article_id = record.get("article_id") or ""
    if not _is_digits_only(article_id):
        return None
    return article_id


def _list_slug_article_groups(conn: sqlite3.Connection) -> list[dict]:
    """
    Return groups keyed by canonical_url for legacy slug identities.

    Group definition:
    - articles.article_type='a'
    - canonical_url is present
    - at least one articles.article_id is non-digits (legacy slug identity)
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT article_id, article_type, title, canonical_url
        FROM articles
        WHERE article_type='a'
              AND canonical_url IS NOT NULL
              AND canonical_url <> ''
        ORDER BY canonical_url ASC, article_id ASC
        """
    )

    by_url: dict[str, list[dict]] = {}
    for article_id, article_type, title, canonical_url in cur.fetchall():
        key = _canonical_url_key(canonical_url)
        if key is None:
            continue
        by_url.setdefault(key, []).append(
            {
                "article_id": article_id,
                "article_type": article_type,
                "title": title,
                "canonical_url": canonical_url,
            }
        )

    groups: list[dict] = []
    for canonical_url, rows in by_url.items():
        if not any(not _is_digits_only(r["article_id"] or "") for r in rows):
            continue
        groups.append({"canonical_url": canonical_url, "rows": rows})
    return groups


def _count_legacy_slug_rows(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM articles
        WHERE article_type='a'
              AND (article_id IS NULL OR article_id='' OR article_id GLOB '*[^0-9]*')
        """
    )
    article_count = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT COUNT(*)
        FROM responses
        WHERE article_type='a'
              AND (article_id IS NULL OR article_id='' OR article_id GLOB '*[^0-9]*')
        """
    )
    response_count = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT COUNT(*)
        FROM target
        WHERE article_type='a'
              AND (article_id IS NULL OR article_id='' OR article_id GLOB '*[^0-9]*')
        """
    )
    target_count = int(cur.fetchone()[0])

    return {
        "articles": article_count,
        "responses": response_count,
        "target": target_count,
    }


def _best_title(rows: list[dict]) -> str:
    for row in rows:
        title = (row.get("title") or "").strip()
        if title:
            return title
    return "unknown"


def _list_group_sources(rows: list[dict]) -> list[str]:
    sources: list[str] = []
    for row in rows:
        article_id = (row.get("article_id") or "").strip()
        if not article_id:
            continue
        if not _is_digits_only(article_id):
            sources.append(article_id)
    return sorted(set(sources))


def _list_existing_numeric_ids(rows: list[dict]) -> list[str]:
    ids: list[str] = []
    for row in rows:
        article_id = (row.get("article_id") or "").strip()
        if _is_digits_only(article_id):
            ids.append(article_id)
    return sorted(set(ids))


def _ensure_numeric_article_row(
    conn: sqlite3.Connection,
    *,
    numeric_id: str,
    title: str,
    canonical_url: str,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO articles
        (article_id, article_type, title, canonical_url)
        VALUES (?, 'a', ?, ?)
        """,
        (numeric_id, title, canonical_url),
    )
    cur.execute(
        """
        UPDATE articles
        SET title = COALESCE(NULLIF(title, ''), ?),
            canonical_url = ?
        WHERE article_id = ? AND article_type = 'a'
        """,
        (title, canonical_url, numeric_id),
    )


def _transfer_responses(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    dest_id: str,
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO responses
        (article_id, article_type, res_no, id_hash, poster_name, posted_at,
         content_html, content_text, res_hidden, idhash_hidden, good_count,
         bad_count, scraped_at)
        SELECT ?, 'a', res_no, id_hash, poster_name, posted_at,
               content_html, content_text, res_hidden, idhash_hidden, good_count,
               bad_count, scraped_at
        FROM responses
        WHERE article_id = ? AND article_type = 'a'
        """,
        (dest_id, source_id),
    )
    return int(cur.rowcount or 0)


def _count_responses_for_id(conn: sqlite3.Connection, article_id: str) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM responses"
        " WHERE article_id=? AND article_type='a'",
        (article_id,),
    )
    return int(cur.fetchone()[0])


def _missing_responses_count(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    dest_id: str,
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM responses AS src
        WHERE src.article_id=? AND src.article_type='a'
              AND NOT EXISTS (
                  SELECT 1
                  FROM responses AS dst
                  WHERE dst.article_id=?
                        AND dst.article_type='a'
                        AND dst.res_no=src.res_no
              )
        """,
        (source_id, dest_id),
    )
    return int(cur.fetchone()[0])


def _cleanup_source_identity(
    conn: sqlite3.Connection,
    *,
    source_id: str,
) -> dict:
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM responses WHERE article_id=? AND article_type='a'",
        (source_id,),
    )
    deleted_responses = int(cur.rowcount or 0)
    cur.execute(
        "DELETE FROM articles WHERE article_id=? AND article_type='a'",
        (source_id,),
    )
    deleted_articles = int(cur.rowcount or 0)
    return {
        "deleted_articles": deleted_articles,
        "deleted_responses": deleted_responses,
    }


def _normalize_target_rows(
    conn: sqlite3.Connection,
    *,
    canonical_url: str,
    source_ids: list[str],
    dest_id: str,
) -> dict:
    """
    Target handling rules:
    - never delete-first
    - deactivate legacy slug targets
    - ensure a dest numeric target row exists, keeping it active if any source
      target was active
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT article_id, is_active
        FROM target
        WHERE article_type='a' AND canonical_url=?
        """,
        (canonical_url,),
    )
    existing = {row[0]: int(row[1]) for row in cur.fetchall()}

    desired_active = int(existing.get(dest_id, 0))
    for sid in source_ids:
        desired_active = max(desired_active, int(existing.get(sid, 0)))

    actions: list[dict] = []
    for sid in source_ids:
        if sid not in existing:
            actions.append({"article_id": sid, "action": "absent"})
            continue
        if existing[sid] == 0:
            actions.append({"article_id": sid, "action": "already_inactive"})
            continue
        cur.execute(
            """
            UPDATE target
            SET is_active=0, canonical_url=?
            WHERE article_id=? AND article_type='a'
            """,
            (canonical_url, sid),
        )
        actions.append({"article_id": sid, "action": "deactivated"})

    cur.execute(
        """
        INSERT OR IGNORE INTO target
        (article_id, article_type, canonical_url, is_active)
        VALUES (?, 'a', ?, ?)
        """,
        (dest_id, canonical_url, desired_active),
    )
    cur.execute(
        """
        UPDATE target
        SET canonical_url=?, is_active=?
        WHERE article_id=? AND article_type='a'
        """,
        (canonical_url, desired_active, dest_id),
    )

    return {
        "dest_is_active": bool(desired_active),
        "sources": actions,
    }


def plan_slug_article_identity_repair(
    conn: sqlite3.Connection,
    *,
    allow_network: bool,
    limit: int | None = None,
) -> dict:
    all_groups = _list_slug_article_groups(conn)
    legacy_counts = _count_legacy_slug_rows(conn)
    total_detected = len(all_groups)
    groups = all_groups if limit is None else all_groups[:limit]
    plan: dict = {
        "dry_run": True,
        "allow_network": allow_network,
        "legacy_counts": legacy_counts,
        "total_detected_groups": total_detected,
        "processed_groups": len(groups),
        "groups": [],
    }

    for group in groups:
        canonical_url = group["canonical_url"]
        rows = group["rows"]
        sources = _list_group_sources(rows)
        existing_numeric = _list_existing_numeric_ids(rows)
        numeric_id = _resolve_numeric_article_id_from_db_rows(rows)
        resolved_by = "db"

        if numeric_id is None:
            resolved_by = "network"
            if not allow_network:
                numeric_id = None
                resolved_by = "skipped_network_disallowed"
            elif not _is_canonical_a_url(canonical_url):
                numeric_id = None
                resolved_by = "skipped_invalid_canonical_url"
            else:
                numeric_id = _resolve_numeric_article_id_from_network(
                    canonical_url
                )
                if numeric_id is None:
                    resolved_by = "network_failed"

        if numeric_id is not None:
            validate_saved_article_identity(numeric_id, "a")

        plan["groups"].append(
            {
                "canonical_url": canonical_url,
                "source_article_ids": sources,
                "existing_numeric_article_ids": existing_numeric,
                "resolved_numeric_article_id": numeric_id,
                "resolved_by": resolved_by,
                "title": _best_title(rows),
            }
        )

    resolved = sum(
        1 for g in plan["groups"]
        if g["resolved_numeric_article_id"] is not None
    )
    plan["resolved_groups"] = resolved
    plan["skipped_groups"] = len(plan["groups"]) - resolved
    return plan


def apply_slug_article_identity_repair(
    conn: sqlite3.Connection,
    *,
    allow_network: bool,
    limit: int | None = None,
) -> dict:
    summary = plan_slug_article_identity_repair(
        conn, allow_network=allow_network, limit=limit
    )
    summary["dry_run"] = False

    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        for group in summary["groups"]:
            numeric_id = group["resolved_numeric_article_id"]
            canonical_url = group["canonical_url"]
            sources = list(group["source_article_ids"])
            title = group["title"]

            group["apply"] = {
                "status": "skipped",
                "reason": None,
                "response_transfers": [],
                "verification": [],
                "cleanup": [],
                "target": None,
            }

            if numeric_id is None:
                group["apply"]["reason"] = group["resolved_by"]
                continue

            validate_saved_article_identity(numeric_id, "a")
            _ensure_numeric_article_row(
                conn,
                numeric_id=numeric_id,
                title=title,
                canonical_url=canonical_url,
            )

            for sid in sources:
                src_count = _count_responses_for_id(conn, sid)
                inserted = _transfer_responses(
                    conn, source_id=sid, dest_id=numeric_id
                )
                missing = _missing_responses_count(
                    conn, source_id=sid, dest_id=numeric_id
                )
                group["apply"]["response_transfers"].append(
                    {
                        "source_article_id": sid,
                        "source_count": src_count,
                        "inserted": inserted,
                        "missing_after_transfer": missing,
                    }
                )
                group["apply"]["verification"].append(
                    {
                        "source_article_id": sid,
                        "ok": missing == 0,
                    }
                )

            if not all(v["ok"] for v in group["apply"]["verification"]):
                group["apply"]["status"] = "failed_verification_no_cleanup"
                raise RuntimeError(
                    "response preservation verification failed; rolling back"
                )

            for sid in sources:
                group["apply"]["cleanup"].append(
                    {
                        "source_article_id": sid,
                        **_cleanup_source_identity(conn, source_id=sid),
                    }
                )

            group["apply"]["target"] = _normalize_target_rows(
                conn,
                canonical_url=canonical_url,
                source_ids=sources,
                dest_id=numeric_id,
            )
            group["apply"]["status"] = "applied"

        conn.commit()

        applied = sum(
            1 for g in summary["groups"]
            if g.get("apply", {}).get("status") == "applied"
        )
        summary["applied_groups"] = applied
        total_inserted = 0
        total_duplicate = 0
        total_missing = 0
        for g in summary["groups"]:
            for t in g.get("apply", {}).get("response_transfers") or []:
                total_inserted += t.get("inserted", 0)
                total_duplicate += (
                    t.get("source_count", 0) - t.get("inserted", 0)
                )
                total_missing += t.get("missing_after_transfer", 0)
        summary["response_inserted"] = total_inserted
        summary["response_duplicate"] = total_duplicate
        summary["response_missing_after"] = total_missing
        return summary
    except Exception:
        conn.rollback()
        raise


def repair_slug_article_identity(
    db_path: str,
    *,
    apply: bool = False,
    allow_network: bool = False,
    limit: int | None = None,
) -> dict:
    if not db_path:
        raise ValueError(
            "explicit db_path is required; refusing implicit default"
        )
    if not Path(db_path).exists():
        raise FileNotFoundError(f"db_path does not exist: {db_path}")
    if limit is not None and limit < 0:
        raise ValueError(f"limit must be non-negative, got {limit}")

    conn = sqlite3.connect(db_path)
    try:
        if apply:
            return apply_slug_article_identity_repair(
                conn, allow_network=allow_network, limit=limit
            )
        return plan_slug_article_identity_repair(
            conn, allow_network=allow_network, limit=limit
        )
    finally:
        conn.close()


def format_repair_summary_lines(
    db_path: str,
    summary: dict,
    *,
    summary_only: bool = False,
) -> list[str]:
    mode = "dry-run" if summary.get("dry_run") else "apply"
    counts = summary.get("legacy_counts") or {}
    groups = summary.get("groups") or []
    total_detected = summary.get("total_detected_groups", len(groups))
    processed = summary.get("processed_groups", len(groups))
    resolved = summary.get("resolved_groups", 0)
    skipped = summary.get("skipped_groups", 0)
    lines = [
        "=== REPAIR SLUG ARTICLE IDENTITY ===",
        f"DB: {db_path}",
        f"Mode: {mode}",
        f"Allow network: {bool(summary.get('allow_network'))}",
        (
            "Legacy counts: "
            f"articles={counts.get('articles', 0)} "
            f"responses={counts.get('responses', 0)} "
            f"target={counts.get('target', 0)}"
        ),
        f"Total detected groups: {total_detected}",
        f"Processed groups: {processed}",
        f"Resolved groups: {resolved}",
        f"Skipped groups: {skipped}",
    ]
    if not summary.get("dry_run"):
        lines.append(f"Applied groups: {summary.get('applied_groups', 0)}")
        if "response_inserted" in summary:
            lines.append(
                "Response totals: "
                f"inserted={summary['response_inserted']} "
                f"duplicate={summary['response_duplicate']} "
                f"missing_after={summary['response_missing_after']}"
            )

    if summary_only:
        return lines

    for group in groups:
        lines.append(f"- canonical_url: {group['canonical_url']}")
        lines.append(
            "  resolved_numeric_article_id: "
            f"{group['resolved_numeric_article_id']}"
        )
        lines.append(f"  resolved_by: {group['resolved_by']}")
        lines.append(
            f"  sources: {', '.join(group['source_article_ids']) or '-'}"
        )
        existing = group.get("existing_numeric_article_ids") or []
        lines.append(
            f"  existing_numeric_ids: {', '.join(existing) or '-'}"
        )
        if not summary.get("dry_run"):
            apply_info = group.get("apply") or {}
            lines.append(f"  apply_status: {apply_info.get('status')}")
            reason = apply_info.get("reason")
            if reason:
                lines.append(f"  apply_reason: {reason}")
            for item in apply_info.get("response_transfers") or []:
                lines.append(
                    "  transfer: "
                    f"source={item['source_article_id']} "
                    f"inserted={item['inserted']} "
                    f"missing_after={item['missing_after_transfer']}"
                )
            target_info = apply_info.get("target")
            if target_info is not None:
                lines.append(
                    "  target_dest_is_active: "
                    f"{target_info['dest_is_active']}"
                )
                for src in target_info.get("sources") or []:
                    lines.append(
                        "  target_source: "
                        f"{src['article_id']} action={src['action']}"
                    )

    return lines


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_slug_article_identity",
        description=(
            "Repair legacy slug article_id rows in a copied archive DB."
        ),
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Explicit SQLite DB path (required; no implicit runtime default).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (writes). Without this flag, dry-run only.",
    )
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help=(
            "Allow metadata fetch from canonical_url when DB lacks numeric ID."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Limit number of canonical_url groups processed (default: all)."
        ),
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Suppress per-group output; print summary statistics only.",
    )
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 0:
        parser.error("--limit must be non-negative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    summary = repair_slug_article_identity(
        args.db,
        apply=bool(args.apply),
        allow_network=bool(args.allow_network),
        limit=args.limit,
    )
    for line in format_repair_summary_lines(
        args.db,
        summary,
        summary_only=bool(args.summary_only),
    ):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
