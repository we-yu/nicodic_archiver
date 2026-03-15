from pathlib import Path


# Temporary plain-text input source until a more structured target registry exists.
def load_target_urls(file_path: str) -> list[str]:
    """Load a stable URL list from a human-editable plain text file."""

    seen_urls = set()
    targets = []

    for raw_line in Path(file_path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if line in seen_urls:
            continue

        seen_urls.add(line)
        targets.append(line)

    return targets
