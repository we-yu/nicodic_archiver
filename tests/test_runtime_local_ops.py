from pathlib import Path


def test_runtime_local_example_contains_expected_keys():
    example_path = Path(".env.runtime.local.example")
    text = example_path.read_text(encoding="utf-8")

    assert "WEB_BIND_HOST=" in text
    assert "WEB_PORT=" in text
    assert "LOCAL_UID=" in text
    assert "LOCAL_GID=" in text


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


def test_personal_runtime_doc_mentions_local_env_and_wrapper():
    text = Path("docs/PERSONAL_RUNTIME.md").read_text(encoding="utf-8")

    assert ".env.runtime.local" in text
    assert "tools/runtime_up.sh" in text
    assert "LOCAL_UID" in text
    assert "WEB_PORT" in text
