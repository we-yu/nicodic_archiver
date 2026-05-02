"""Repair legacy slug article_id rows in an explicit SQLite archive DB.

This maintenance tool detects legacy article_type='a' rows whose article_id is
not digits-only text, plans a repair keyed by canonical_url, and optionally
applies that repair inside one transaction when --apply is given.

Safety properties:
- explicit --db PATH is required
- dry-run is the default
- writes require explicit --apply
- no implicit runtime DB default exists
- rollback on apply failure
- never creates article_type='id' rows
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from orchestrator import fetch_article_metadata_record
from storage import validate_saved_article_identity


def _is_numeric_identity(article_id: str | None) -> bool:
    return isinstance(article_id, str) and bool(article_id) and article_id.isdigit()


def _connect_existing_db(db_path: str) -> sqlite3.Connection:
    if not db_path:
        raise ValueError("explicit --db PATH is required")

    resolved_path = Path(db_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"db_path does not exist: {db_path}")

    conn = sqlite3.connect(str(resolved_path))
    conn.row_factory = sqlite3.Row
    _require_tables(conn, {"articles", "responses", "target"})
    return conn


def _require_tables(conn: sqlite3.Connection, expected: set[str]) -> None:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    table_names = {row[0] for row in cur.fetchall()}
    missing = sorted(expected - table_names)
    if missing:
        raise ValueError(
            "db is missing required tables: " + ", ".join(missing)
        )


def _column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cur.fetchall()}


def _load_article_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM articles
        WHERE article_type='a'
        ORDER BY canonical_url ASC, article_id ASC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def _load_target_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM target
        WHERE article_type='a'
        ORDER BY canonical_url ASC, article_id ASC, id ASC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def _load_response_identity_counts(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT article_id, COUNT(*) AS response_count
        FROM responses
        WHERE article_type='a'
        GROUP BY article_id
        ORDER BY article_id ASC
        """
    )
    response_counts: dict[str, dict[str, int]] = {}
    for row in cur.fetchall():
        response_counts[row[0]] = {"response_count": int(row[1])}
    return response_counts


def detect_legacy_slug_identity_groups(conn: sqlite3.Connection) -> dict[str, Any]:
    articles = _load_article_rows(conn)
    targets = _load_target_rows(conn)
    response_counts = _load_response_identity_counts(conn)

    groups_by_url: dict[str, dict[str, Any]] = {}
    known_legacy_ids: set[str] = set()

    for article_row in articles:
        if _is_numeric_identity(article_row["article_id"]):
            continue
        canonical_url = article_row.get("canonical_url")
        if not canonical_url:
            continue
        group = groups_by_url.setdefault(
            canonical_url,
            {
                "canonical_url": canonical_url,
                "legacy_article_rows": [],
                "legacy_target_rows": [],
                "numeric_article_rows": [],
                "numeric_target_rows": [],
                "legacy_response_identities": [],
            },
        )
        group["legacy_article_rows"].append(article_row)
        known_legacy_ids.add(article_row["article_id"])

    for target_row in targets:
        canonical_url = target_row.get("canonical_url")
        if not canonical_url:
            continue
        group = groups_by_url.setdefault(
            canonical_url,
            {
                "canonical_url": canonical_url,
                "legacy_article_rows": [],
                "legacy_target_rows": [],
                "numeric_article_rows": [],
                "numeric_target_rows": [],
                "legacy_response_identities": [],
            },
        )
        if _is_numeric_identity(target_row["article_id"]):
            group["numeric_target_rows"].append(target_row)
        else:
            group["legacy_target_rows"].append(target_row)
            known_legacy_ids.add(target_row["article_id"])

    for article_row in articles:
        if not _is_numeric_identity(article_row["article_id"]):
            continue
        canonical_url = article_row.get("canonical_url")
        if not canonical_url or canonical_url not in groups_by_url:
            continue
        groups_by_url[canonical_url]["numeric_article_rows"].append(article_row)

    for group in groups_by_url.values():
        seen_response_ids = set()
        for row in group["legacy_article_rows"] + group["legacy_target_rows"]:
            article_id = row["article_id"]
            if article_id in seen_response_ids:
                continue
            seen_response_ids.add(article_id)
            group["legacy_response_identities"].append(
                {
                    "article_id": article_id,
                    "response_count": response_counts.get(article_id, {})
                    .get("response_count", 0),
                }
            )

    orphan_response_identities = []
    for article_id, counts in response_counts.items():
        if _is_numeric_identity(article_id):
            continue
        if article_id in known_legacy_ids:
            continue
        orphan_response_identities.append(
            {
                "article_id": article_id,
                "response_count": counts["response_count"],
                "skip_reason": "missing_canonical_url_source",
            }
        )

    groups = [groups_by_url[key] for key in sorted(groups_by_url)]
    return {
        "groups": groups,
        "orphan_response_identities": orphan_response_identities,
    }


def _resolve_numeric_id_from_db(group: dict[str, Any]) -> tuple[str | None, str | None]:
    candidates = {
        row["article_id"] for row in group["numeric_article_rows"]
        if _is_numeric_identity(row["article_id"])
    }
    candidates.update(
        row["article_id"] for row in group["numeric_target_rows"]
        if _is_numeric_identity(row["article_id"])
    )

    if not candidates:
        return None, None
    if len(candidates) > 1:
        return None, "conflicting_numeric_identities"
    return next(iter(candidates)), None


def _resolve_numeric_id_from_metadata(canonical_url: str) -> tuple[str | None, str | None]:
    try:
        metadata = fetch_article_metadata_record(canonical_url)
    except Exception as exc:  # pragma: no cover - covered through mocks
        return None, f"metadata_resolution_failed:{type(exc).__name__}"

    if metadata.get("article_type") != "a":
        return None, "metadata_not_canonical_a"

    metadata_url = metadata.get("article_url")
    if metadata_url != canonical_url:
        return None, "metadata_canonical_url_mismatch"

    article_id = metadata.get("article_id")
    try:
        validate_saved_article_identity(article_id, "a")
    except ValueError:
        return None, "metadata_non_numeric_article_id"

    return article_id, None


def _pick_preferred_article_row(group: dict[str, Any]) -> dict[str, Any] | None:
    candidates = group["numeric_article_rows"] + group["legacy_article_rows"]
    if not candidates:
        return None

    def sort_key(row: dict[str, Any]) -> tuple[int, str, str]:
        return (
            0 if _is_numeric_identity(row["article_id"]) else 1,
            row.get("created_at") or "",
            row.get("article_id") or "",
        )

    return sorted(candidates, key=sort_key)[0]


def plan_slug_article_identity_repair(conn: sqlite3.Connection) -> dict[str, Any]:
    detection = detect_legacy_slug_identity_groups(conn)
    plan_groups = []

    for group in detection["groups"]:
        numeric_article_id, db_resolution_error = _resolve_numeric_id_from_db(group)
        numeric_resolution = "db"
        skip_reason = None

        if db_resolution_error is not None:
            skip_reason = db_resolution_error
            numeric_resolution = None
        elif numeric_article_id is None:
            numeric_article_id, metadata_error = _resolve_numeric_id_from_metadata(
                group["canonical_url"]
            )
            numeric_resolution = "metadata" if metadata_error is None else None
            skip_reason = metadata_error

        article_row = _pick_preferred_article_row(group)
        legacy_response_count = sum(
            entry["response_count"] for entry in group["legacy_response_identities"]
        )

        plan_groups.append(
            {
                "canonical_url": group["canonical_url"],
                "resolved_article_id": numeric_article_id,
                "resolution_source": numeric_resolution,
                "skip_reason": skip_reason,
                "legacy_article_ids": [
                    row["article_id"] for row in group["legacy_article_rows"]
                ],
                "legacy_target_ids": [
                    row["article_id"] for row in group["legacy_target_rows"]
                ],
                "legacy_response_identities": group["legacy_response_identities"],
                "legacy_response_count": legacy_response_count,
                "numeric_article_exists": bool(group["numeric_article_rows"]),
                "numeric_target_exists": bool(group["numeric_target_rows"]),
                "article_template": article_row,
            }
        )

    return {
        "groups": plan_groups,
        "orphan_response_identities": detection["orphan_response_identities"],
    }


def _merge_article_values(
    conn: sqlite3.Connection,
    group: dict[str, Any],
    resolved_article_id: str,
) -> dict[str, Any]:
    article_columns = _column_names(conn, "articles")
    row = group["article_template"] or {}
    merged = {
        "article_id": resolved_article_id,
        "article_type": "a",
        "title": row.get("title") or group["canonical_url"],
        "canonical_url": group["canonical_url"],
    }
    for optional_column in [
        "created_at",
        "published_at",
        "modified_at",
        "latest_scraped_at",
    ]:
        if optional_column in article_columns:
            merged[optional_column] = row.get(optional_column)
    return merged


def _upsert_numeric_article_row(
    conn: sqlite3.Connection,
    group: dict[str, Any],
    resolved_article_id: str,
) -> str:
    merged = _merge_article_values(conn, group, resolved_article_id)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM articles
        WHERE article_id=? AND article_type='a'
        """,
        (resolved_article_id,),
    )
    exists = cur.fetchone() is not None

    if exists:
        assignments = ["title=?", "canonical_url=?"]
        params = [merged["title"], merged["canonical_url"]]
        for optional_column in ["published_at", "modified_at", "latest_scraped_at"]:
            if optional_column in merged:
                assignments.append(
                    f"{optional_column}=COALESCE({optional_column}, ?)"
                )
                params.append(merged[optional_column])
        params.extend([resolved_article_id])
        cur.execute(
            f"""
            UPDATE articles
            SET {', '.join(assignments)}
            WHERE article_id=? AND article_type='a'
            """,
            params,
        )
        return "updated_existing"

    insert_columns = ["article_id", "article_type", "title", "canonical_url"]
    insert_values = [
        merged["article_id"],
        merged["article_type"],
        merged["title"],
        merged["canonical_url"],
    ]
    for optional_column in [
        "created_at",
        "published_at",
        "modified_at",
        "latest_scraped_at",
    ]:
        if optional_column in merged and merged[optional_column] is not None:
            insert_columns.append(optional_column)
            insert_values.append(merged[optional_column])

    placeholders = ", ".join("?" for _ in insert_columns)
    cur.execute(
        f"""
        INSERT INTO articles ({', '.join(insert_columns)})
        VALUES ({placeholders})
        """,
        insert_values,
    )
    return "inserted_new"


def _response_res_nos(conn: sqlite3.Connection, article_id: str) -> set[int]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT res_no FROM responses
        WHERE article_id=? AND article_type='a'
        """,
        (article_id,),
    )
    return {int(row[0]) for row in cur.fetchall()}


def _transfer_response_identity(
    conn: sqlite3.Connection,
    source_article_id: str,
    resolved_article_id: str,
) -> dict[str, Any]:
    cur = conn.cursor()
    dest_before = _response_res_nos(conn, resolved_article_id)
    source_res_nos = _response_res_nos(conn, source_article_id)

    cur.execute(
        """
        INSERT OR IGNORE INTO responses
        (article_id, article_type, res_no, id_hash, poster_name, posted_at,
         content_html, content_text, res_hidden, idhash_hidden, good_count,
         bad_count, scraped_at)
        SELECT ?, 'a', res_no, id_hash, poster_name, posted_at, content_html,
               content_text, res_hidden, idhash_hidden, good_count, bad_count,
               scraped_at
        FROM responses
        WHERE article_id=? AND article_type='a'
        """,
        (resolved_article_id, source_article_id),
    )

    dest_after = _response_res_nos(conn, resolved_article_id)
    transferred_res_nos = dest_after - dest_before
    duplicate_res_nos = source_res_nos & dest_before
    verification_ok = source_res_nos.issubset(dest_after)

    return {
        "source_article_id": source_article_id,
        "source_res_nos": source_res_nos,
        "source_response_count": len(source_res_nos),
        "transferred_count": len(transferred_res_nos),
        "duplicate_res_no_count": len(duplicate_res_nos),
        "verification": "ok" if verification_ok else "failed_no_cleanup",
        "cleanup_performed": False,
        "deleted_article_rows": 0,
        "deleted_response_rows": 0,
    }


def _cleanup_legacy_article_identity(
    conn: sqlite3.Connection,
    source_article_id: str,
) -> tuple[int, int]:
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM responses WHERE article_id=? AND article_type='a'",
        (source_article_id,),
    )
    deleted_responses = cur.rowcount
    cur.execute(
        "DELETE FROM articles WHERE article_id=? AND article_type='a'",
        (source_article_id,),
    )
    deleted_articles = cur.rowcount
    return deleted_articles, deleted_responses


def _repair_target_rows(
    conn: sqlite3.Connection,
    group: dict[str, Any],
    resolved_article_id: str,
) -> list[dict[str, Any]]:
    cur = conn.cursor()
    actions = []
    target_rows = sorted(
        group["legacy_target_rows"],
        key=lambda row: (row.get("id") or 0, row["article_id"]),
    )
    if not target_rows:
        return actions

    cur.execute(
        """
        SELECT id FROM target
        WHERE article_id=? AND article_type='a'
        """,
        (resolved_article_id,),
    )
    existing_numeric_target = cur.fetchone()
    reusable_target_id = None if existing_numeric_target else (
        target_rows[0].get("id") if target_rows else None
    )

    for row in target_rows:
        row_id = row.get("id")
        if row_id == reusable_target_id:
            cur.execute(
                """
                UPDATE target
                SET article_id=?, canonical_url=?
                WHERE id=?
                """,
                (resolved_article_id, group["canonical_url"], row_id),
            )
            actions.append({
                "source_article_id": row["article_id"],
                "action": "updated_to_numeric",
            })
            continue

        cur.execute(
            """
            DELETE FROM target
            WHERE id=?
            """,
            (row_id,),
        )
        actions.append({
            "source_article_id": row["article_id"],
            "action": "deleted_after_verification",
        })

    return actions


def apply_slug_article_identity_repair(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = True,
) -> dict[str, Any]:
    plan = plan_slug_article_identity_repair(conn)
    summary = {
        "dry_run": dry_run,
        "groups": [],
        "orphan_response_identities": plan["orphan_response_identities"],
        "transaction": "not_started" if dry_run else "committed",
    }

    if dry_run:
        for group in plan["groups"]:
            summary["groups"].append(
                {
                    **group,
                    "article_action": "dry_run",
                    "response_actions": [],
                    "target_actions": [],
                }
            )
        return summary

    try:
        conn.execute("BEGIN")
        for group in plan["groups"]:
            group_summary = {
                **group,
                "article_action": "skipped",
                "response_actions": [],
                "target_actions": [],
            }

            if group["skip_reason"] is not None:
                summary["groups"].append(group_summary)
                continue

            resolved_article_id = group["resolved_article_id"]
            validate_saved_article_identity(resolved_article_id, "a")
            group_summary["article_action"] = _upsert_numeric_article_row(
                conn,
                group,
                resolved_article_id,
            )

            response_ok = True
            for entry in group["legacy_response_identities"]:
                response_action = _transfer_response_identity(
                    conn,
                    entry["article_id"],
                    resolved_article_id,
                )
                if response_action["verification"] != "ok":
                    response_ok = False
                group_summary["response_actions"].append(response_action)

            if response_ok:
                for response_action in group_summary["response_actions"]:
                    deleted_articles, deleted_responses = _cleanup_legacy_article_identity(
                        conn,
                        response_action["source_article_id"],
                    )
                    response_action["cleanup_performed"] = True
                    response_action["deleted_article_rows"] = deleted_articles
                    response_action["deleted_response_rows"] = deleted_responses

                group_summary["target_actions"] = _repair_target_rows(
                    conn,
                    group,
                    resolved_article_id,
                )

            summary["groups"].append(group_summary)

        conn.commit()
        summary["transaction"] = "committed"
        return summary
    except Exception:
        conn.rollback()
        summary["transaction"] = "rolled_back"
        raise


def repair_slug_article_identities(
    db_path: str,
    *,
    apply: bool = False,
) -> dict[str, Any]:
    conn = _connect_existing_db(db_path)
    try:
        return apply_slug_article_identity_repair(conn, dry_run=not apply)
    finally:
        conn.close()


def format_repair_summary_lines(db_path: str, summary: dict[str, Any]) -> list[str]:
    mode = "apply" if not summary["dry_run"] else "dry-run"
    lines = [
        "=== SLUG ARTICLE IDENTITY REPAIR ===",
        f"DB: {db_path}",
        f"Mode: {mode}",
        f"Transaction: {summary['transaction']}",
        f"Groups: {len(summary['groups'])}",
        f"Orphan response identities: {len(summary['orphan_response_identities'])}",
    ]

    for group in summary["groups"]:
        lines.append(f"- canonical_url: {group['canonical_url']}")
        lines.append(
            "  resolved_article_id: "
            f"{group['resolved_article_id'] or 'unresolved'}"
        )
        lines.append(
            "  resolution_source: "
            f"{group['resolution_source'] or 'none'}"
        )
        if group.get("skip_reason"):
            lines.append(f"  skip_reason: {group['skip_reason']}")
        lines.append(f"  article_action: {group['article_action']}")
        lines.append(
            f"  legacy_article_ids: {', '.join(group['legacy_article_ids']) or 'none'}"
        )
        lines.append(
            f"  legacy_target_ids: {', '.join(group['legacy_target_ids']) or 'none'}"
        )

        for action in group["response_actions"]:
            lines.append(
                "  response_source: "
                f"{action['source_article_id']} | "
                f"count={action['source_response_count']} | "
                f"transferred={action['transferred_count']} | "
                f"duplicates_skipped={action['duplicate_res_no_count']} | "
                f"verification={action['verification']} | "
                f"deleted_articles={action['deleted_article_rows']} | "
                f"deleted_responses={action['deleted_response_rows']}"
            )

        for target_action in group["target_actions"]:
            lines.append(
                "  target_source: "
                f"{target_action['source_article_id']} | "
                f"action={target_action['action']}"
            )

    for orphan in summary["orphan_response_identities"]:
        lines.append(
            "- orphan_response_identity: "
            f"{orphan['article_id']} | responses={orphan['response_count']} | "
            f"skip_reason={orphan['skip_reason']}"
        )

    return lines


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run or apply repair for legacy article_type='a' rows whose "
            "article_id stores a /a/<slug> value instead of a numeric ID."
        )
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Explicit SQLite DB path to inspect or repair.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply writes. Omit for dry-run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        summary = repair_slug_article_identities(args.db, apply=args.apply)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    for line in format_repair_summary_lines(args.db, summary):
        print(line)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
