import sys

from archive_read import build_download_filename
from archive_read import find_saved_article_ref_by_id
from archive_read import find_saved_article_ref_by_title
from archive_read import get_saved_article_export


FORMAT_FLAGS = {
    "--txt": "txt",
    "--md": "md",
    "--csv": "csv",
}


def _parse_args(argv: list[str]) -> dict:
    target_title = None
    target_id = None
    output_format = "txt"
    idx = 0

    while idx < len(argv):
        arg = argv[idx]
        if arg in FORMAT_FLAGS:
            output_format = FORMAT_FLAGS[arg]
            idx += 1
            continue
        if arg == "--id":
            if idx + 1 >= len(argv):
                raise ValueError("Missing value for --id")
            target_id = argv[idx + 1]
            idx += 2
            continue
        if arg == "--title":
            if idx + 1 >= len(argv):
                raise ValueError("Missing value for --title")
            target_title = argv[idx + 1]
            idx += 2
            continue
        if arg.startswith("--"):
            raise ValueError(f"Unknown option: {arg}")
        if target_title is not None:
            raise ValueError("Only one bare title argument is supported")
        target_title = arg
        idx += 1

    if target_id is not None and target_title is not None:
        raise ValueError("Use either --id or a title, not both")
    if target_id is None and target_title is None:
        raise ValueError("Specify an article title or --id ID")

    return {
        "target_id": target_id,
        "target_title": target_title,
        "format": output_format,
    }


def _resolve_article_ref(parsed: dict) -> dict | None:
    if parsed["target_id"] is not None:
        return find_saved_article_ref_by_id(parsed["target_id"])
    return find_saved_article_ref_by_title(parsed["target_title"])


def run_admin_export(
    argv: list[str],
    stdout=None,
    stderr=None,
) -> int:
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr

    try:
        parsed = _parse_args(argv)
    except ValueError as exc:
        stderr.write(f"error: {exc}\n")
        return 2

    article_ref = _resolve_article_ref(parsed)
    if article_ref is None:
        stderr.write("not found: saved article was not found in SQLite\n")
        return 1

    article_id = article_ref["article_id"]
    article_type = article_ref["article_type"]
    requested_format = parsed["format"]
    export_result = get_saved_article_export(
        article_id,
        article_type,
        requested_format,
    )
    if not export_result["found"]:
        stderr.write("not found: saved archive content was not found\n")
        return 1

    filename = build_download_filename(
        article_id,
        article_type,
        export_result.get("title") or article_ref.get("title"),
        requested_format,
    )
    stderr.write(f"exporting {article_id} {article_type} as {filename}\n")
    stdout.write(export_result["content"])
    return 0


def main() -> None:
    raise SystemExit(run_admin_export(sys.argv[1:]))


if __name__ == "__main__":
    main()
