from pathlib import Path

from article_resolver import resolve_article_input
from collection_policy import find_denylisted_article_id
from dicopedia_urls import parse_target_identity
from storage import (
    get_target,
    init_db,
    list_targets,
    mark_target_redirected,
    open_readonly_db,
    register_target,
    set_target_active_state,
)


def _parse_target_line(raw_line: str) -> str | None:
    line = raw_line.strip()

    if not line or line.startswith("#"):
        return None

    return line


def list_active_target_urls(target_db_path: str) -> list[str]:
    """Load active target URLs from the SQLite-backed target registry."""

    conn = open_readonly_db(target_db_path)
    if conn is None:
        return []
    try:
        return [entry["canonical_url"] for entry in list_targets(conn)]
    finally:
        conn.close()


def list_registered_targets(
    target_db_path: str,
    *,
    active_only: bool = False,
) -> list[dict]:
    """Load registry entries for operator-facing list views."""

    conn = open_readonly_db(target_db_path)
    if conn is None:
        return []
    try:
        return list_targets(conn, active_only=active_only)
    finally:
        conn.close()


def normalize_target_url(article_url: str) -> str | None:
    """Best-effort parse of a dic article URL shape (syntax only; may be id)."""

    target_identity = parse_target_identity(article_url)
    if target_identity is None:
        return None

    return target_identity["canonical_url"]


def validate_target_url(article_url: str) -> bool:
    return parse_target_identity(article_url) is not None


def register_target_url(article_url: str, target_db_path: str) -> str:
    """Resolve numeric identity via metadata, then persist one target row."""

    candidate = article_url.strip()
    if find_denylisted_article_id(article_url=candidate):
        return "denylisted"

    parsed = parse_target_identity(candidate)
    if parsed is None:
        return "invalid"

    resolution = resolve_article_input(candidate)
    if not resolution["ok"]:
        kind = resolution["failure_kind"]
        if kind == "invalid_input":
            return "invalid"
        return "resolution_failure"

    canonical_target = resolution["canonical_target"]
    numeric_id = canonical_target["article_id"]

    post_denylisted = find_denylisted_article_id(
        article_id=numeric_id,
        article_url=canonical_target["article_url"],
    )
    if post_denylisted is not None:
        return "denylisted"

    title = resolution.get("title") or ""
    observed_max_res_no = resolution.get("observed_max_res_no")
    observed_max_res_no_source = resolution.get(
        "observed_max_res_no_source"
    )

    conn = init_db(target_db_path)
    try:
        result = register_target(
            conn,
            numeric_id,
            canonical_target["article_type"],
            canonical_target["article_url"],
            title=title,
            observed_max_res_no=observed_max_res_no,
            observed_max_res_no_source=observed_max_res_no_source,
        )
    finally:
        conn.close()

    return result["status"]


def inspect_registered_target(
    article_id: str,
    article_type: str,
    target_db_path: str,
) -> dict | None:
    """Load one target registry entry for operator-facing inspection."""

    conn = open_readonly_db(target_db_path)
    if conn is None:
        return None
    try:
        return get_target(conn, article_id, article_type)
    finally:
        conn.close()


def deactivate_target(
    article_id: str,
    article_type: str,
    target_db_path: str,
) -> dict:
    """Deactivate one target without removing it from the registry."""

    conn = init_db(target_db_path)
    try:
        return set_target_active_state(conn, article_id, article_type, False)
    finally:
        conn.close()


def reactivate_target(
    article_id: str,
    article_type: str,
    target_db_path: str,
) -> dict:
    """Reactivate one target already present in the registry."""

    conn = init_db(target_db_path)
    try:
        return set_target_active_state(conn, article_id, article_type, True)
    finally:
        conn.close()


def handoff_redirected_target(
    article_id: str,
    article_type: str,
    redirect_target_url: str,
    target_db_path: str,
) -> dict:
    """Deactivate a redirected source target and register the redirect target."""

    redirect_identity = parse_target_identity(redirect_target_url)
    if redirect_identity is None:
        return {
            "found": False,
            "status": "invalid_redirect_target",
            "entry": None,
            "register_status": "invalid_redirect_target",
            "redirect_target": None,
            "target_identity": {
                "article_id": article_id,
                "article_type": article_type,
            },
        }

    source_identity = {
        "article_id": article_id,
        "article_type": article_type,
    }
    conn = init_db(target_db_path)
    try:
        redirect_result = mark_target_redirected(
            conn,
            article_id,
            article_type,
            redirect_identity["canonical_url"],
        )
        if not redirect_result["found"]:
            return {
                "found": False,
                "status": redirect_result["status"],
                "entry": None,
                "register_status": "skipped",
                "redirect_target": redirect_identity,
                "target_identity": source_identity,
            }

        register_status = "self_redirect"
        register_entry = None
        nested = resolve_article_input(
            redirect_identity["canonical_url"],
        )
        if not nested["ok"]:
            register_status = "resolution_failure"
        elif (
            nested["canonical_target"]["article_id"] == article_id
            and nested["canonical_target"]["article_type"] == article_type
        ):
            pass
        elif find_denylisted_article_id(
            article_id=nested["canonical_target"]["article_id"],
            article_url=nested["canonical_target"]["article_url"],
        ):
            register_status = "denylisted"
        else:
            ct = nested["canonical_target"]
            register_result = register_target(
                conn,
                ct["article_id"],
                ct["article_type"],
                ct["article_url"],
                title=nested.get("title") or "",
            )
            register_status = register_result["status"]
            register_entry = register_result["entry"]
    finally:
        conn.close()

    return {
        "found": True,
        "status": "redirected",
        "entry": redirect_result["entry"],
        "register_status": register_status,
        "register_entry": register_entry,
        "redirect_target": redirect_identity,
        "target_identity": source_identity,
    }


def import_targets_from_text_file(
    source_path: str,
    target_db_path: str,
) -> dict:
    """Import legacy plain-text targets into the SQLite registry once."""

    counts = {
        "source_path": source_path,
        "target_db_path": target_db_path,
        "processed": 0,
        "added": 0,
        "duplicate": 0,
        "denylisted": 0,
        "reactivated": 0,
        "invalid": 0,
        "resolution_failure": 0,
    }

    lines = Path(source_path).read_text(encoding="utf-8").splitlines()

    for raw_line in lines:
        line = _parse_target_line(raw_line)
        if line is None:
            continue

        counts["processed"] += 1
        result = register_target_url(line, target_db_path)
        counts[result] += 1

    return counts
