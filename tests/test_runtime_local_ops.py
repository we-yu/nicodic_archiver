import os
from pathlib import Path
import subprocess


def test_runtime_local_example_contains_expected_keys():
    example_path = Path(".env.runtime.local.example")
    text = example_path.read_text(encoding="utf-8")

    assert "WEB_BIND_HOST=" in text
    assert "WEB_PORT=" in text
    assert "LOCAL_UID=" in text
    assert "LOCAL_GID=" in text
    assert "TARGET_ORDER_MODE=default" in text
    assert "# TARGET_ORDER_MODE=random_rotation" in text
    assert "# TARGET_ORDER_MODE=reverse" in text
    assert "# TARGET_ORDER_START_ARTICLE_ID=" in text


def test_gitignore_treats_runtime_local_env_as_local_only():
    text = Path(".gitignore").read_text(encoding="utf-8")

    assert ".env.runtime.local" in text


def test_runtime_up_wrapper_loads_local_env_and_recreates_runtime():
    text = Path("tools/runtime_up.sh").read_text(encoding="utf-8")

    assert "runtime_local_load" in text
    assert "runtime_local_validate" in text
    assert "runtime_local_warn_port_clash" in text
    assert "--force-recreate" in text
    assert "docker-compose.runtime.yml" in text


def test_periodic_wrapper_loads_shared_runtime_env_helper():
    text = Path("runtime/periodic_once.sh").read_text(encoding="utf-8")

    assert 'tools/runtime_env.sh' in text
    assert "runtime_local_load" in text
    assert "runtime_local_validate" in text


def test_runtime_env_helper_preserves_host_inline_target_order_overrides():
    text = Path("tools/runtime_env.sh").read_text(encoding="utf-8")

    assert "TARGET_ORDER_MODE" in text
    assert "TARGET_ORDER_START_ARTICLE_ID" in text
    assert "preserved_assignments" in text


def test_periodic_wrapper_forwards_target_order_env_into_container_exec():
    text = Path("runtime/periodic_once.sh").read_text(encoding="utf-8")

    assert 'TARGET_ORDER_MODE="${TARGET_ORDER_MODE:-}"' in text
    assert (
        'TARGET_ORDER_START_ARTICLE_ID="${TARGET_ORDER_START_ARTICLE_ID:-}"'
        in text
    )
    assert (
        'ONESHOT_LIMIT_DURATION_SECONDS='
        '"${ONESHOT_LIMIT_DURATION_SECONDS:-}"' in text
    )
    assert (
        'SOFT_TERMINATE_FILE='
        '"${SOFT_TERMINATE_FILE:-runtime/control/stop_after_current}"'
        in text
    )


def test_personal_runtime_doc_mentions_local_env_and_wrapper():
    text = Path("docs/PERSONAL_RUNTIME.md").read_text(encoding="utf-8")

    assert ".env.runtime.local" in text
    assert "tools/runtime_up.sh" in text
    assert "tools/runtime_periodic_ops.sh" in text
    assert "LOCAL_UID" in text
    assert "WEB_PORT" in text
    assert "TARGET_ORDER_MODE" in text
    assert "TARGET_ORDER_START_ARTICLE_ID" in text
    assert "--target-order-mode reverse" in text
    assert "--target-order-start-article-id 5400838" in text


def test_runtime_periodic_ops_helper_contains_expected_subcommands():
    text = Path("tools/runtime_periodic_ops.sh").read_text(encoding="utf-8")

    assert "status)" in text
    assert "stop-once)" in text
    assert "stop-count)" in text
    assert "show-stop)" in text
    assert "clear-stop)" in text
    assert "clear-lock)" in text
    assert "MAX_STOP_COUNT=255" in text


def test_runtime_periodic_ops_helper_status_and_stop_commands(tmp_path):
    stop_file = tmp_path / "control" / "stop_after_current"
    lock_file = tmp_path / "logs" / "periodic_once.lock"
    process_file = tmp_path / "processes.txt"
    process_file.write_text("", encoding="utf-8")
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_file.write_text("held", encoding="utf-8")

    env = os.environ.copy()
    env["SOFT_TERMINATE_FILE"] = str(stop_file)
    env["PERIODIC_LOCK_FILE"] = str(lock_file)
    env["RUNTIME_PERIODIC_OPS_PS_FILE"] = str(process_file)
    env["RUNTIME_PERIODIC_OPS_SKIP_DOCKER"] = "1"

    subprocess.run(
        ["bash", "tools/runtime_periodic_ops.sh", "stop-count", "9999"],
        check=True,
        env=env,
    )

    assert stop_file.read_text(encoding="utf-8") == "255\n"

    show_result = subprocess.run(
        ["bash", "tools/runtime_periodic_ops.sh", "show-stop"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert "soft_stop_exists=yes" in show_result.stdout
    assert "soft_stop_countdown=255" in show_result.stdout

    status_result = subprocess.run(
        ["bash", "tools/runtime_periodic_ops.sh", "status"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert "periodic_lock_exists=yes" in status_result.stdout
    assert f"periodic_lock_path={lock_file}" in status_result.stdout
    assert "soft_stop_exists=yes" in status_result.stdout
    assert "scrape_like_processes=none" in status_result.stdout

    subprocess.run(
        ["bash", "tools/runtime_periodic_ops.sh", "clear-stop"],
        check=True,
        env=env,
    )

    assert not stop_file.exists()


def test_runtime_periodic_ops_helper_clear_lock_refuses_when_work_appears_active(
    tmp_path,
):
    stop_file = tmp_path / "control" / "stop_after_current"
    lock_file = tmp_path / "logs" / "periodic_once.lock"
    process_file = tmp_path / "processes.txt"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_file.write_text("held", encoding="utf-8")
    process_file.write_text(
        "123 python main.py periodic data/nicodic.db\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["SOFT_TERMINATE_FILE"] = str(stop_file)
    env["PERIODIC_LOCK_FILE"] = str(lock_file)
    env["RUNTIME_PERIODIC_OPS_PS_FILE"] = str(process_file)
    env["RUNTIME_PERIODIC_OPS_SKIP_DOCKER"] = "1"

    result = subprocess.run(
        ["bash", "tools/runtime_periodic_ops.sh", "clear-lock"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 1
    assert "Refusing to clear lock" in result.stdout
    assert lock_file.exists()
