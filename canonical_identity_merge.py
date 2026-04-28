from pathlib import Path
import sqlite3

from target_list import parse_target_identity


def _is_numeric_article_id(article_id):
    return article_id.isdigit()


def _open_explicit_db(db_path, *, write):
    db_file = Path(db_path)
    if not db_file.is_file():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    if write:
        return sqlite3.connect(str(db_file))

    return sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)


def _list_duplicate_canonical_urls(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT canonical_url
        FROM articles
        WHERE article_type='a'
          AND canonical_url IS NOT NULL
          AND canonical_url != ''
        GROUP BY canonical_url
        HAVING COUNT(*) > 1
        ORDER BY canonical_url ASC
        """
    )
    return [row[0] for row in cur.fetchall()]


def _load_article_group(conn, canonical_url):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, article_id, title, canonical_url, created_at
        FROM articles
        WHERE article_type='a' AND canonical_url=?
        ORDER BY created_at ASC, id ASC
        """,
        (canonical_url,),
    )
    return [
        {
            "row_id": row[0],
            "article_id": row[1],
            "article_type": "a",
            "title": row[2],
            "canonical_url": row[3],
            "created_at": row[4],
        }
        for row in cur.fetchall()
    ]


def _load_target_group(conn, canonical_url):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, article_id, article_type, canonical_url, is_active,
               is_redirected, redirect_target_url, redirect_detected_at,
               created_at
        FROM target
        WHERE article_type='a' AND canonical_url=?
        ORDER BY created_at ASC, id ASC
        """,
        (canonical_url,),
    )
    return [
        {
            "row_id": row[0],
            "article_id": row[1],
            "article_type": row[2],
            "canonical_url": row[3],
            "is_active": bool(row[4]),
            "is_redirected": bool(row[5]),
            "redirect_target_url": row[6],
            "redirect_detected_at": row[7],
            "created_at": row[8],
        }
        for row in cur.fetchall()
    ]


def _select_keep_article(rows, canonical_url):
    parsed = parse_target_identity(canonical_url)
    if parsed is not None and parsed["article_type"] == "a":
        keep_article_id = parsed["article_id"]
        for row in rows:
            if row["article_id"] == keep_article_id:
                return row

    for row in rows:
        if not _is_numeric_article_id(row["article_id"]):
            return row

    return rows[0]


def _count_responses(conn, article_id):
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM responses WHERE article_id=? AND article_type='a'",
        (article_id,),
    )
    return cur.fetchone()[0]


def _count_missing_responses(conn, source_article_id, keep_article_id):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM responses AS source
        WHERE source.article_id=?
          AND source.article_type='a'
          AND NOT EXISTS (
              SELECT 1
              FROM responses AS keep
              WHERE keep.article_id=?
                AND keep.article_type='a'
                AND keep.res_no=source.res_no
          )
        """,
        (source_article_id, keep_article_id),
    )
    return cur.fetchone()[0]


def _copy_missing_responses(conn, source_article_id, keep_article_id):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO responses (
            article_id,
            article_type,
            res_no,
            id_hash,
            poster_name,
            posted_at,
            content_html,
            content_text,
            res_hidden,
            idhash_hidden,
            good_count,
            bad_count,
            scraped_at
        )
        SELECT
            ?,
            'a',
            source.res_no,
            source.id_hash,
            source.poster_name,
            source.posted_at,
            source.content_html,
            source.content_text,
            source.res_hidden,
            source.idhash_hidden,
            source.good_count,
            source.bad_count,
            source.scraped_at
        FROM responses AS source
        WHERE source.article_id=?
          AND source.article_type='a'
          AND NOT EXISTS (
              SELECT 1
              FROM responses AS keep
              WHERE keep.article_id=?
                AND keep.article_type='a'
                AND keep.res_no=source.res_no
          )
        """,
        (keep_article_id, source_article_id, keep_article_id),
    )


def _normalize_targets_for_group(conn, canonical_url, keep_article_id):
    target_rows = _load_target_group(conn, canonical_url)
    if not target_rows:
        return {
            "rekeyed": 0,
            "deleted": 0,
            "target_rows": [],
        }

    keep_target = None
    for row in target_rows:
        if row["article_id"] == keep_article_id:
            keep_target = row
            break

    if keep_target is None:
        keep_target = next(
            (row for row in target_rows if row["is_active"]),
            target_rows[0],
        )
        conn.execute(
            """
            UPDATE target
            SET article_id=?, article_type='a', canonical_url=?
            WHERE id=?
            """,
            (keep_article_id, canonical_url, keep_target["row_id"]),
        )
        keep_target = dict(keep_target)
        keep_target["article_id"] = keep_article_id
        keep_target["canonical_url"] = canonical_url
        rekeyed = 1
    else:
        rekeyed = 0

    deleted = 0
    for row in target_rows:
        if row["row_id"] == keep_target["row_id"]:
            continue
        conn.execute("DELETE FROM target WHERE id=?", (row["row_id"],))
        deleted += 1

    return {
        "rekeyed": rekeyed,
        "deleted": deleted,
        "target_rows": target_rows,
    }


def _build_group_plan(conn, canonical_url):
    article_rows = _load_article_group(conn, canonical_url)
    keep_row = _select_keep_article(article_rows, canonical_url)
    source_rows = [
        row for row in article_rows if row["article_id"] != keep_row["article_id"]
    ]

    merge_pairs = []
    for source_row in source_rows:
        source_response_count = _count_responses(conn, source_row["article_id"])
        missing_response_count = _count_missing_responses(
            conn,
            source_row["article_id"],
            keep_row["article_id"],
        )
        merge_pairs.append(
            {
                "source_identity": {
                    "article_id": source_row["article_id"],
                    "article_type": "a",
                },
                "source_response_count": source_response_count,
                "missing_response_count": missing_response_count,
                "overlapping_response_count": (
                    source_response_count - missing_response_count
                ),
            }
        )

    return {
        "canonical_url": canonical_url,
        "keep_identity": {
            "article_id": keep_row["article_id"],
            "article_type": "a",
        },
        "source_identities": [pair["source_identity"] for pair in merge_pairs],
        "merge_pairs": merge_pairs,
        "target_row_count": len(_load_target_group(conn, canonical_url)),
    }


def merge_canonical_a_identity_groups(db_path, *, apply=False):
    conn = _open_explicit_db(db_path, write=apply)
    try:
        result = {
            "db_path": db_path,
            "apply": bool(apply),
            "dry_run": not apply,
            "group_count": 0,
            "copied_response_count": 0,
            "skipped_existing_response_count": 0,
            "cleaned_article_count": 0,
            "cleaned_response_count": 0,
            "target_rekey_count": 0,
            "target_deleted_count": 0,
            "groups": [],
        }

        canonical_urls = _list_duplicate_canonical_urls(conn)
        result["group_count"] = len(canonical_urls)

        for canonical_url in canonical_urls:
            group_result = _build_group_plan(conn, canonical_url)

            if not apply:
                result["copied_response_count"] += sum(
                    pair["missing_response_count"]
                    for pair in group_result["merge_pairs"]
                )
                result["skipped_existing_response_count"] += sum(
                    pair["overlapping_response_count"]
                    for pair in group_result["merge_pairs"]
                )
                result["groups"].append(group_result)
                continue

            conn.execute("SAVEPOINT canonical_identity_merge")
            try:
                group_result["verified"] = True
                group_result["cleanup_performed"] = False

                for pair in group_result["merge_pairs"]:
                    source_article_id = pair["source_identity"]["article_id"]
                    keep_article_id = group_result["keep_identity"]["article_id"]

                    _copy_missing_responses(conn, source_article_id, keep_article_id)
                    missing_after = _count_missing_responses(
                        conn,
                        source_article_id,
                        keep_article_id,
                    )
                    pair["verified_missing_after"] = missing_after
                    if missing_after != 0:
                        group_result["verified"] = False
                        break

                    result["copied_response_count"] += pair["missing_response_count"]
                    result["skipped_existing_response_count"] += pair[
                        "overlapping_response_count"
                    ]

                if not group_result["verified"]:
                    conn.execute("ROLLBACK TO SAVEPOINT canonical_identity_merge")
                    conn.execute("RELEASE SAVEPOINT canonical_identity_merge")
                    result["groups"].append(group_result)
                    continue

                target_changes = _normalize_targets_for_group(
                    conn,
                    canonical_url,
                    group_result["keep_identity"]["article_id"],
                )
                group_result["target_changes"] = target_changes
                result["target_rekey_count"] += target_changes["rekeyed"]
                result["target_deleted_count"] += target_changes["deleted"]

                for pair in group_result["merge_pairs"]:
                    source_article_id = pair["source_identity"]["article_id"]
                    deleted_response_count = _count_responses(conn, source_article_id)
                    conn.execute(
                        "DELETE FROM responses WHERE article_id=? AND article_type='a'",
                        (source_article_id,),
                    )
                    conn.execute(
                        "DELETE FROM articles WHERE article_id=? AND article_type='a'",
                        (source_article_id,),
                    )
                    result["cleaned_response_count"] += deleted_response_count
                    result["cleaned_article_count"] += 1

                conn.execute("RELEASE SAVEPOINT canonical_identity_merge")
                group_result["cleanup_performed"] = True
                result["groups"].append(group_result)
            except Exception:
                conn.execute("ROLLBACK TO SAVEPOINT canonical_identity_merge")
                conn.execute("RELEASE SAVEPOINT canonical_identity_merge")
                raise

        if apply:
            conn.commit()

        return result
    finally:
        conn.close()
