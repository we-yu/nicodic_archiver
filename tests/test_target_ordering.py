from target_ordering import format_target_order_log_line
from target_ordering import resolve_target_order_config
from target_ordering import order_targets_for_run


TARGETS = [
    "https://dic.nicovideo.jp/a/1",
    "https://dic.nicovideo.jp/a/2",
    "https://dic.nicovideo.jp/a/3",
    "https://dic.nicovideo.jp/a/4",
    "https://dic.nicovideo.jp/a/5",
]


def test_default_mode_preserves_input_order():
    decision = order_targets_for_run(TARGETS, mode="default")

    assert decision.ordered_targets == TARGETS
    assert decision.effective_mode == "default"
    assert decision.requested_mode == "default"


def test_reverse_mode_reverses_input_order():
    decision = order_targets_for_run(TARGETS, mode="reverse")

    assert decision.ordered_targets == list(reversed(TARGETS))
    assert decision.effective_mode == "reverse"


def test_random_rotation_rotates_without_shuffle_or_duplicates():
    chooser_calls: list[int] = []

    def chooser(limit: int) -> int:
        chooser_calls.append(limit)
        return 2

    decision = order_targets_for_run(
        TARGETS,
        mode="random_rotation",
        randrange_fn=chooser,
    )

    assert chooser_calls == [5]
    assert decision.ordered_targets == [
        "https://dic.nicovideo.jp/a/3",
        "https://dic.nicovideo.jp/a/4",
        "https://dic.nicovideo.jp/a/5",
        "https://dic.nicovideo.jp/a/1",
        "https://dic.nicovideo.jp/a/2",
    ]
    assert sorted(decision.ordered_targets) == sorted(TARGETS)
    assert len(set(decision.ordered_targets)) == len(TARGETS)
    assert decision.effective_mode == "random_rotation"
    assert decision.start_index == 2
    assert decision.start_article_id == "3"


def test_blank_env_start_article_id_allows_reverse_mode():
    config = resolve_target_order_config(
        environ={
            "TARGET_ORDER_MODE": "reverse",
            "TARGET_ORDER_START_ARTICLE_ID": "",
        }
    )

    decision = order_targets_for_run(TARGETS, config=config)

    assert config.start_article_id is None
    assert config.start_article_id_source is None
    assert decision.ordered_targets == list(reversed(TARGETS))
    assert decision.effective_mode == "reverse"
    assert decision.reason is None


def test_blank_env_start_article_id_allows_random_rotation_mode():
    config = resolve_target_order_config(
        environ={
            "TARGET_ORDER_MODE": "random_rotation",
            "TARGET_ORDER_START_ARTICLE_ID": "   ",
        }
    )

    decision = order_targets_for_run(
        TARGETS,
        config=config,
        randrange_fn=lambda _limit: 2,
    )

    assert config.start_article_id is None
    assert config.start_article_id_source is None
    assert decision.ordered_targets == [
        "https://dic.nicovideo.jp/a/3",
        "https://dic.nicovideo.jp/a/4",
        "https://dic.nicovideo.jp/a/5",
        "https://dic.nicovideo.jp/a/1",
        "https://dic.nicovideo.jp/a/2",
    ]
    assert decision.effective_mode == "random_rotation"
    assert decision.reason is None


def test_start_article_override_rotates_to_matching_article_id():
    decision = order_targets_for_run(
        TARGETS,
        mode="default",
        start_article_id="4",
    )

    assert decision.ordered_targets == [
        "https://dic.nicovideo.jp/a/4",
        "https://dic.nicovideo.jp/a/5",
        "https://dic.nicovideo.jp/a/1",
        "https://dic.nicovideo.jp/a/2",
        "https://dic.nicovideo.jp/a/3",
    ]
    assert decision.effective_mode == "start_article_id"
    assert decision.start_index == 3
    assert decision.start_article_id == "4"


def test_start_article_override_takes_precedence_over_reverse_and_random():
    decision = order_targets_for_run(
        TARGETS,
        mode="reverse",
        start_article_id="2",
        randrange_fn=lambda _limit: 4,
    )

    assert decision.ordered_targets == [
        "https://dic.nicovideo.jp/a/2",
        "https://dic.nicovideo.jp/a/3",
        "https://dic.nicovideo.jp/a/4",
        "https://dic.nicovideo.jp/a/5",
        "https://dic.nicovideo.jp/a/1",
    ]
    assert decision.effective_mode == "start_article_id"


def test_invalid_start_article_override_falls_back_to_default_order():
    decision = order_targets_for_run(
        TARGETS,
        mode="random_rotation",
        start_article_id="999999999",
        randrange_fn=lambda _limit: 3,
    )

    assert decision.ordered_targets == TARGETS
    assert decision.effective_mode == "default"
    assert decision.fallback_mode == "default"
    assert decision.reason == "start_article_id_not_found"


def test_unknown_mode_falls_back_to_default_order():
    decision = order_targets_for_run(TARGETS, mode="unknown")

    assert decision.ordered_targets == TARGETS
    assert decision.effective_mode == "default"
    assert decision.fallback_mode == "default"
    assert decision.reason == "unknown_mode"


def test_empty_target_list_is_safe():
    decision = order_targets_for_run([], mode="random_rotation")

    assert decision.ordered_targets == []
    assert decision.target_count == 0
    assert decision.effective_mode == "default"


def test_log_line_includes_effective_target_order_decision_fields():
    config = resolve_target_order_config(cli_mode="random_rotation")
    decision = order_targets_for_run(
        TARGETS,
        config=config,
        randrange_fn=lambda _limit: 1,
    )

    line = format_target_order_log_line(decision)

    assert line.startswith("[TARGET ORDER]")
    assert "mode=random_rotation" in line
    assert "mode_source=cli" in line
    assert "targets=5" in line
    assert "effective=random_rotation" in line
    assert "start_index=1" in line
    assert "start_article_id=2" in line


def test_start_article_override_matches_stored_numeric_article_id():
    targets = [
        "https://dic.nicovideo.jp/a/%E3%83%86%E3%82%B9%E3%83%88",
        "https://dic.nicovideo.jp/a/other",
    ]
    decision = order_targets_for_run(
        targets,
        start_article_id="5400838",
        target_article_ids=["5400838", "200"],
    )

    assert decision.ordered_targets == targets
    assert decision.effective_mode == "start_article_id"
    assert decision.start_article_id == "5400838"


def test_cli_values_override_environment_values_in_resolved_config():
    config = resolve_target_order_config(
        cli_mode="reverse",
        cli_start_article_id="5400838",
        environ={
            "TARGET_ORDER_MODE": "random_rotation",
            "TARGET_ORDER_START_ARTICLE_ID": "11111",
        },
    )

    assert config.mode == "reverse"
    assert config.mode_source == "cli"
    assert config.start_article_id == "5400838"
    assert config.start_article_id_source == "cli"


def test_env_values_are_used_when_cli_values_are_absent():
    config = resolve_target_order_config(
        environ={
            "TARGET_ORDER_MODE": "reverse",
            "TARGET_ORDER_START_ARTICLE_ID": "12345",
        }
    )

    assert config.mode == "reverse"
    assert config.mode_source == "env"
    assert config.start_article_id == "12345"
    assert config.start_article_id_source == "env"


def test_blank_env_start_article_id_is_treated_as_absent():
    config = resolve_target_order_config(
        environ={
            "TARGET_ORDER_MODE": "reverse",
            "TARGET_ORDER_START_ARTICLE_ID": "",
        }
    )

    assert config.mode == "reverse"
    assert config.mode_source == "env"
    assert config.start_article_id is None
    assert config.start_article_id_source is None
