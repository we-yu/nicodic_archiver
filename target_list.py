"""
Plain-text target list loader.

Provisional file-based input source for article URLs; not a long-term registry.
"""


def load_target_list(filepath: str) -> list[str]:
    """
    Read a plain-text target list: one URL per line.
    Blank lines and lines starting with # are skipped. Lines are stripped.
    Returns a list of URL strings in order of appearance.
    """
    urls = []
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)
    return urls
