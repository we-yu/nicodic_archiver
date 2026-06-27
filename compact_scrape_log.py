"""Compact scrape / batch logging format helpers.

Small pure functions used by HostCronReporter and batch file appenders so new
digest fields stay localized.
"""

import re
import shlex
from datetime import datetime, timezone


def utc_ts_z(dt: datetime | None = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def feeder_summary_compact(summary: dict) -> str:
    checked_to = summary.get("checked_to_res_no")
    if checked_to is None:
        r_range = "none"
    else:
        r_range = (
            f"{summary.get('checked_from_res_no')}-{checked_to}"
        )
    pieces = (
        f"range={r_range}",
        f"checked={summary.get('responses_checked', 0)}",
        f"extracted={summary.get('extracted_candidates', 0)}",
        f"processed={summary.get('processed_candidates', 0)}",
        f"registered={summary.get('registered_candidates', 0)}",
        f"handed_off={summary.get('handed_off_candidates', 0)}",
        f"skipped_invalid={summary.get('skipped_invalid_candidates', 0)}",
        (
            "skipped_resolution="
            f"{summary.get('skipped_resolution_failures', 0)}"
        ),
        (
            "skipped_denylist="
            f"{summary.get('skipped_denylisted_candidates', 0)}"
        ),
        (
            "skipped_registration="
            f"{summary.get('skipped_registration_failures', 0)}"
        ),
        (
            "last_processed="
            f"{summary.get('updated_last_processed_res_no', 0)}"
        ),
    )
    return " ".join(pieces)


_PAGE_START_RE = re.compile(r"^(\d+)-?(?:\.[^/]*)?$")


def board_page_token_key(page_url: str) -> str:
    candidate = page_url.rstrip("/").split("/")[-1]
    match = _PAGE_START_RE.search(candidate)
    if match is None:
        return candidate[:24] if candidate else "?"
    return match.group(1)


def http_status_quick(status_text: str) -> str:
    digits = "".join(ch for ch in status_text if ch.isdigit())
    return digits[:3] if digits else status_text.replace(" ", "")[:12]


def format_page_ok_token(page_url: str) -> str:
    key = board_page_token_key(page_url)
    return f"[{key} OK]"


def format_page_err_token(page_url: str, status: str) -> str:
    key = board_page_token_key(page_url)
    st = http_status_quick(status)[:3]
    label = "ERR500" if st == "500" else f"ERR{st}".upper()
    return f"[{key} {label}]"


def format_top_err_token(status: str) -> str:
    st = http_status_quick(status)[:3]
    tag = "ERR500" if st == "500" else f"ERR{st}".upper()
    return f"[top {tag}]"


GROUP_PAGE_TOKENS = 11


def join_page_tokens(tokens: list[str]) -> str:
    return "".join(tokens)


def title_for_log(title: str, max_len: int = 72) -> str:
    t = title.replace('"', '\\"').replace("\n", " ").strip()
    if len(t) > max_len:
        t = t[: max_len - 1] + "…"
    return t


def shell_quote_safe(value: str) -> str:
    return shlex.quote(value)


def compact_run_id_from_datetime(dt_utc: datetime) -> str:
    """Dense UTC stamp for host_cron correlation (collision risk ~1s granularity)."""

    return dt_utc.strftime("%Y%m%dT%H%M%SZ")


def observe_val(value: int | str | None) -> str:
    if value is None:
        return "unknown"
    text = str(value).strip()
    return text if text else "unknown"


def run_start_compact_fields(
    *,
    ts_iso_z: str,
    run_stamp: str,
    batch_ref: str,
    trigger: str,
    db_path: str,
    limit_seconds: int | float | None,
) -> str:
    lim_part = (
        f"limit_seconds={int(limit_seconds)}"
        if limit_seconds is not None
        else "limit_seconds=none"
    )
    return (
        f"ts={ts_iso_z} run_id={run_stamp} batch_ref={batch_ref} "
        f"trigger={trigger} db={db_path} {lim_part}"
    )


def warn_detail_later_page(
    page_token_key: str,
    http_status: str,
    saved_partial: int,
) -> str:
    return (
        f"page_start={page_token_key} status={http_status} "
        f"saved_partial={saved_partial} reason=later_page_interrupted"
    )


def warn_detail_response_cap(saved_partial: int) -> str:
    return (
        f"saved_partial={saved_partial} reason=response_cap_reached"
    )


def fail_detail_line(
    *,
    phase: str,
    http_status: str,
    reason_snake: str,
) -> str:
    return f"phase={phase} status={http_status} reason={reason_snake}"


def flush_page_tokens_by_group(
    tokens: list[str], group_size: int = GROUP_PAGE_TOKENS
) -> tuple[list[list[str]], list[str]]:
    """Split token list into ROWS of ``group_size`` tokens; leftover stays."""
    rows: list[list[str]] = []
    rem = tokens
    while len(rem) >= group_size:
        rows.append(rem[:group_size])
        rem = rem[group_size:]
    return rows, rem


def batch_digest_counters_lines(
    *,
    hit_targets: int,
    ok0_targets: int,
    warn_targets: int,
    fail_targets: int,
    skip_targets: int,
    total_new_responses: int,
    observed_max_unknown_targets: int,
) -> list[str]:
    return [
        "BATCH_DIGEST",
        f"  H={hit_targets}",
        f"  OK0={ok0_targets}",
        f"  W={warn_targets}",
        f"  F={fail_targets}",
        f"  S={skip_targets}",
        f"  NEW={total_new_responses}",
        f"  UOBS={observed_max_unknown_targets}",
    ]


def format_batch_digest_block(
    *,
    digest_hit_msgs: list[str],
    digest_warn_msgs: list[str],
    digest_fail_msgs: list[str],
    digest_skip_msgs: list[str],
    digest_ok0: int,
    total_new_responses: int,
    unknown_obs_targets: int,
) -> list[str]:
    head = batch_digest_counters_lines(
        hit_targets=len(digest_hit_msgs),
        ok0_targets=digest_ok0,
        warn_targets=len(digest_warn_msgs),
        fail_targets=len(digest_fail_msgs),
        skip_targets=len(digest_skip_msgs),
        total_new_responses=total_new_responses,
        observed_max_unknown_targets=unknown_obs_targets,
    )
    items: list[str] = ["BATCH_DIGEST_ITEMS"]
    flat: list[str] = []
    flat += [f"HIT {m}" for m in digest_hit_msgs]
    flat += [f"WARN {m}" for m in digest_warn_msgs]
    flat += [f"FAIL {m}" for m in digest_fail_msgs]
    flat += [f"SKIP {m}" for m in digest_skip_msgs]
    for ln in flat:
        items.append(f"  {ln}")
    tail = (
        [f"  OK0 others={digest_ok0}"]
        if digest_ok0
        else ["  OK0 others=0"]
    )
    return head + items + tail


def digest_reason_token(
    reason: str | None,
    *,
    response_cap_hint: bool = False,
    status_fallback: str = "fail",
) -> str:
    if response_cap_hint:
        return "response_cap_reached"
    if reason:
        for key in (
            "already_up_to_date",
            "later_page_interrupted",
            "response_cap_reached",
            "redirect_detected",
            "article_not_found",
            "invalid_target_url_shape",
            "skip_denylist",
            "redirect_handoff_failed",
        ):
            if key in reason:
                return key
        tail = reason.replace("reason=", "")[:40]
        return tail or "unspecified"
    if status_fallback == "success":
        return "ok"
    if status_fallback == "partial":
        return "partial"
    return "fail"


def digest_looks_like_skip(reason: str | None) -> bool:
    r = reason or ""
    return (
        "skip_denylist" in r or "invalid_target_url_shape" in r
    )


class BatchDigestRecorder:
    """Collects BATCH_DIGEST-compatible rows during run_batch_scrape."""

    __slots__ = (
        "_digest_hit_msgs",
        "_digest_warn_msgs",
        "_digest_fail_msgs",
        "_digest_skip_msgs",
        "_digest_ok0",
        "_total_new_responses",
        "_unknown_obs_targets",
    )

    def __init__(self) -> None:
        self._digest_hit_msgs: list[str] = []
        self._digest_warn_msgs: list[str] = []
        self._digest_fail_msgs: list[str] = []
        self._digest_skip_msgs: list[str] = []
        self._digest_ok0 = 0
        self._total_new_responses = 0
        self._unknown_obs_targets = 0

    def render_block(self) -> list[str]:
        return format_batch_digest_block(
            digest_hit_msgs=self._digest_hit_msgs,
            digest_warn_msgs=self._digest_warn_msgs,
            digest_fail_msgs=self._digest_fail_msgs,
            digest_skip_msgs=self._digest_skip_msgs,
            digest_ok0=self._digest_ok0,
            total_new_responses=self._total_new_responses,
            unknown_obs_targets=self._unknown_obs_targets,
        )

    def add_finish_entry(
        self,
        *,
        had_step: bool,
        prog_idx: int,
        prog_total: int,
        article_id_val: str | None,
        label: str | None,
        ref: str,
        status: str,
        reason: str | None,
        stored_new: int | None,
        observed_after: int | str | None,
        interrupt_http: str | None,
        response_cap_hint: bool = False,
    ) -> None:
        prog = f"{prog_idx}/{prog_total}" if had_step else "?/?"
        ttl = title_for_log(label or "?")
        aid = observe_val(article_id_val if article_id_val is not None else ref)
        sn = stored_new if stored_new is not None else 0
        reason_tok = digest_reason_token(
            reason,
            response_cap_hint=response_cap_hint,
            status_fallback=status,
        )
        skip = status == "fail" and digest_looks_like_skip(reason)
        if skip:
            self._digest_skip_msgs.append(
                f'progress={prog} article_id={aid} title="{ttl}" '
                f"detail={reason_tok}"
            )
            return
        o_lit = observe_val(observed_after)
        if observe_val(observed_after) == "unknown" and status in (
            "fail",
            "partial",
        ):
            self._unknown_obs_targets += 1
        if status == "fail" and not had_step:
            self._digest_fail_msgs.append(
                f'article_id={aid} title="{ttl}" '
                f"detail={reason_tok} observed_after={o_lit}"
            )
            return
        if status == "fail":
            self._digest_fail_msgs.append(
                f'progress={prog} article_id={aid} title="{ttl}" '
                f"detail={reason_tok} observed_after={o_lit}"
            )
            return
        if status == "partial":
            http_out = observe_val(interrupt_http)
            self._digest_warn_msgs.append(
                f'progress={prog} article_id={aid} title="{ttl}" '
                f"reason={reason_tok} "
                f"stored_partial={sn} observed_after={o_lit} http={http_out}"
            )
            self._total_new_responses += max(sn, 0)
            return
        if sn > 0:
            self._digest_hit_msgs.append(
                f'progress={prog} article_id={aid} title="{ttl}" '
                f"stored_new={sn} observed_after={o_lit}"
            )
            self._total_new_responses += max(sn, 0)
            return
        self._digest_ok0 += 1
