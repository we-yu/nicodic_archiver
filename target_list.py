from pathlib import Path
from urllib.parse import urlparse


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


def validate_article_url(article_url: str) -> tuple[bool, str]:
    """
    Minimal syntax validation for Nico Nico Pedia article URLs.
    This does not perform any online existence check.
    """
    if not article_url or not article_url.strip():
        return False, "empty_url"

    parsed = urlparse(article_url.strip())
    if parsed.scheme not in {"http", "https"}:
        return False, "invalid_scheme"
    if not parsed.netloc:
        return False, "missing_host"

    parts = parsed.path.strip("/").split("/")
    if len(parts) < 2:
        return False, "invalid_path"

    article_type = parts[0]
    article_id = parts[1]
    if not article_type or not article_id:
        return False, "invalid_path"

    # Minimal guard: expected article paths like /a/<id>
    if article_type not in {"a"}:
        return False, "unexpected_article_type"
    if not article_id.isdigit():
        return False, "unexpected_article_id"

    return True, "ok"


def add_target_url(article_url: str, target_list_path: str) -> tuple[bool, str]:
    """
    Add one target URL to a plain-text list file.
    - No duplicates (based on exact line match after strip).
    - Minimal URL syntax validation only.

    Returns:
        (added, reason)
    """
    ok, reason = validate_article_url(article_url)
    if not ok:
        return False, reason

    url = article_url.strip()
    path = Path(target_list_path)

    existing_targets = []
    existing_text = ""
    if path.exists():
        existing_targets = load_target_urls(str(path))
        existing_text = path.read_text(encoding="utf-8")

    if url in set(existing_targets):
        return False, "duplicate"

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        if existing_text and not existing_text.endswith("\n"):
            f.write("\n")
        f.write(url + "\n")

    return True, "added"
