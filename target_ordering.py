import os
import random
from dataclasses import dataclass

from dicopedia_urls import parse_target_identity


DEFAULT_TARGET_ORDER_MODE = "default"
REVERSE_TARGET_ORDER_MODE = "reverse"
RANDOM_ROTATION_TARGET_ORDER_MODE = "random_rotation"

VALID_TARGET_ORDER_MODES = {
    DEFAULT_TARGET_ORDER_MODE,
    REVERSE_TARGET_ORDER_MODE,
    RANDOM_ROTATION_TARGET_ORDER_MODE,
}


@dataclass(frozen=True)
class TargetOrderDecision:
    ordered_targets: list[str]
    requested_mode: str
    effective_mode: str
    target_count: int
    requested_mode_source: str | None = None
    requested_start_article_id: str | None = None
    requested_start_article_id_source: str | None = None
    start_index: int | None = None
    start_article_id: str | None = None
    fallback_mode: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class TargetOrderConfig:
    mode: str | None = None
    start_article_id: str | None = None
    mode_source: str | None = None
    start_article_id_source: str | None = None


def _normalized_mode(raw_mode: str | None) -> str:
    text = (raw_mode or "").strip().lower()
    if not text:
        return DEFAULT_TARGET_ORDER_MODE
    return text


def _normalized_optional_text(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    text = raw_value.strip()
    if not text:
        return None
    return text


def _article_id_for_target(target_url: str) -> str | None:
    identity = parse_target_identity(target_url)
    if identity is None:
        return None
    return identity["article_id"]


def _rotate_targets(targets: list[str], start_index: int) -> list[str]:
    if not targets:
        return []
    return targets[start_index:] + targets[:start_index]


def _article_ids_for_targets(
    targets: list[str],
    target_article_ids: list[str | None] | None,
) -> list[str | None]:
    if target_article_ids is None:
        return [_article_id_for_target(target) for target in targets]
    if len(target_article_ids) != len(targets):
        raise ValueError("target_article_ids must match targets length")

    article_ids: list[str | None] = []
    for target, article_id in zip(targets, target_article_ids):
        article_ids.append(article_id or _article_id_for_target(target))
    return article_ids


def resolve_target_order_config(
    *,
    cli_mode: str | None = None,
    cli_start_article_id: str | None = None,
    environ: dict[str, str] | None = None,
) -> TargetOrderConfig:
    env = environ or os.environ

    if cli_mode is not None:
        mode = cli_mode
        mode_source = "cli"
    else:
        mode = env.get("TARGET_ORDER_MODE")
        mode_source = "env" if mode is not None else "default"

    if cli_start_article_id is not None:
        start_article_id = _normalized_optional_text(cli_start_article_id)
        start_article_id_source = (
            "cli" if start_article_id is not None else None
        )
    else:
        start_article_id = _normalized_optional_text(
            env.get("TARGET_ORDER_START_ARTICLE_ID")
        )
        start_article_id_source = "env" if start_article_id is not None else None

    return TargetOrderConfig(
        mode=mode,
        start_article_id=start_article_id,
        mode_source=mode_source,
        start_article_id_source=start_article_id_source,
    )


def order_targets_for_run(
    targets: list[str],
    *,
    config: TargetOrderConfig | None = None,
    mode: str | None = None,
    start_article_id: str | None = None,
    target_article_ids: list[str | None] | None = None,
    randrange_fn=None,
) -> TargetOrderDecision:
    active_config = config or TargetOrderConfig(
        mode=mode,
        start_article_id=start_article_id,
    )
    requested_mode = _normalized_mode(active_config.mode)
    target_count = len(targets)
    ordered_targets = list(targets)
    requested_start = _normalized_optional_text(active_config.start_article_id)
    article_ids = _article_ids_for_targets(targets, target_article_ids)

    if requested_start is not None:
        if not requested_start or not requested_start.isdigit():
            return TargetOrderDecision(
                ordered_targets=ordered_targets,
                requested_mode=requested_mode,
                effective_mode=DEFAULT_TARGET_ORDER_MODE,
                target_count=target_count,
                requested_mode_source=active_config.mode_source,
                requested_start_article_id=requested_start,
                requested_start_article_id_source=(
                    active_config.start_article_id_source
                ),
                fallback_mode=DEFAULT_TARGET_ORDER_MODE,
                reason="invalid_start_article_id",
            )

        for index, target_article_id in enumerate(article_ids):
            if target_article_id == requested_start:
                return TargetOrderDecision(
                    ordered_targets=_rotate_targets(list(targets), index),
                    requested_mode=requested_mode,
                    effective_mode="start_article_id",
                    target_count=target_count,
                    requested_mode_source=active_config.mode_source,
                    requested_start_article_id=requested_start,
                    requested_start_article_id_source=(
                        active_config.start_article_id_source
                    ),
                    start_index=index,
                    start_article_id=requested_start,
                )

        return TargetOrderDecision(
            ordered_targets=ordered_targets,
            requested_mode=requested_mode,
            effective_mode=DEFAULT_TARGET_ORDER_MODE,
            target_count=target_count,
            requested_mode_source=active_config.mode_source,
            requested_start_article_id=requested_start,
            requested_start_article_id_source=active_config.start_article_id_source,
            fallback_mode=DEFAULT_TARGET_ORDER_MODE,
            reason="start_article_id_not_found",
        )

    if requested_mode not in VALID_TARGET_ORDER_MODES:
        return TargetOrderDecision(
            ordered_targets=ordered_targets,
            requested_mode=requested_mode,
            effective_mode=DEFAULT_TARGET_ORDER_MODE,
            target_count=target_count,
            requested_mode_source=active_config.mode_source,
            fallback_mode=DEFAULT_TARGET_ORDER_MODE,
            reason="unknown_mode",
        )

    if requested_mode == REVERSE_TARGET_ORDER_MODE:
        return TargetOrderDecision(
            ordered_targets=list(reversed(targets)),
            requested_mode=requested_mode,
            effective_mode=REVERSE_TARGET_ORDER_MODE,
            target_count=target_count,
            requested_mode_source=active_config.mode_source,
        )

    if requested_mode == RANDOM_ROTATION_TARGET_ORDER_MODE and targets:
        chooser = randrange_fn or random.randrange
        start_index = chooser(len(targets))
        return TargetOrderDecision(
            ordered_targets=_rotate_targets(list(targets), start_index),
            requested_mode=requested_mode,
            effective_mode=RANDOM_ROTATION_TARGET_ORDER_MODE,
            target_count=target_count,
            requested_mode_source=active_config.mode_source,
            start_index=start_index,
            start_article_id=article_ids[start_index],
        )

    return TargetOrderDecision(
        ordered_targets=ordered_targets,
        requested_mode=requested_mode,
        effective_mode=DEFAULT_TARGET_ORDER_MODE,
        target_count=target_count,
        requested_mode_source=active_config.mode_source,
    )


def order_targets_for_run_from_env(
    targets: list[str],
    *,
    environ: dict[str, str] | None = None,
    target_article_ids: list[str | None] | None = None,
    randrange_fn=None,
) -> TargetOrderDecision:
    config = resolve_target_order_config(environ=environ)
    return order_targets_for_run(
        targets,
        config=config,
        target_article_ids=target_article_ids,
        randrange_fn=randrange_fn,
    )


def format_target_order_log_line(decision: TargetOrderDecision) -> str:
    parts = [
        "[TARGET ORDER]",
        f"mode={decision.requested_mode}",
    ]

    if decision.requested_mode_source is not None:
        parts.append(f"mode_source={decision.requested_mode_source}")

    if decision.requested_start_article_id is not None:
        parts.append(
            "requested_start_article_id="
            f"{decision.requested_start_article_id}"
        )
        if decision.requested_start_article_id_source is not None:
            parts.append(
                "requested_start_article_id_source="
                f"{decision.requested_start_article_id_source}"
            )

    parts.append(f"targets={decision.target_count}")
    parts.append(f"effective={decision.effective_mode}")

    if decision.start_index is not None:
        parts.append(f"start_index={decision.start_index}")

    if decision.start_article_id is not None:
        parts.append(f"start_article_id={decision.start_article_id}")

    if decision.fallback_mode is not None:
        parts.append(f"fallback={decision.fallback_mode}")

    if decision.reason is not None:
        parts.append(f"reason={decision.reason}")

    return " ".join(parts)
