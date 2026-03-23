from pathlib import Path
from urllib.parse import urlparse


def _parse_target_line(raw_line: str) -> str | None:
    line = raw_line.strip()

    if not line or line.startswith("#"):
        return None

    return line


# Temporary plain-text input source until a more structured target registry exists.
def load_target_urls(file_path: str) -> list[str]:
    """Load a stable URL list from a human-editable plain text file."""

    seen_urls = set()
    targets = []

    for raw_line in Path(file_path).read_text(encoding="utf-8").splitlines():
        line = _parse_target_line(raw_line)
        if line is None:
            continue

        if line in seen_urls:
            continue

        seen_urls.add(line)
        targets.append(line)

    return targets


def validate_target_url(article_url: str) -> bool:
    candidate = article_url.strip()
    if not candidate:
        return False

    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc != "dic.nicovideo.jp":
        return False

    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) != 2:
        return False

    article_type, article_id = path_parts
    if not article_type or not article_id:
        return False

    return True


def add_target_url(article_url: str, target_list_path: str) -> str:
    candidate = article_url.strip()
    if not validate_target_url(candidate):
        return "invalid"

    target_path = Path(target_list_path)
    existing_targets = []
    if target_path.exists():
        existing_targets = load_target_urls(target_list_path)

    if candidate in existing_targets:
        return "duplicate"

    target_path.parent.mkdir(parents=True, exist_ok=True)

    prefix = ""
    if target_path.exists():
        existing_text = target_path.read_text(encoding="utf-8")
        if existing_text and not existing_text.endswith("\n"):
            prefix = "\n"

    with target_path.open("a", encoding="utf-8") as target_file:
        target_file.write(f"{prefix}{candidate}\n")

    return "added"
