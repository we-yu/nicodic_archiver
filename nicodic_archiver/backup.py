"""JSON backup export for nicodic_archiver."""

import json
from pathlib import Path

from .db import get_responses


def export_json(db_path: str, article_slug: str, json_dir: str) -> Path:
    """Export all stored responses for *article_slug* to a JSON file.

    The output file is named ``<json_dir>/<article_slug>.json``.

    Args:
        db_path: Path to the SQLite database.
        article_slug: Article identifier.
        json_dir: Directory to write the JSON file into.

    Returns:
        :class:`~pathlib.Path` of the written file.
    """
    responses = get_responses(db_path, article_slug)
    out_dir = Path(json_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{article_slug}.json"
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(responses, fh, ensure_ascii=False, indent=2)
    return out_path
