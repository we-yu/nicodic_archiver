import sys

from cli import inspect_article
from orchestrator import run_scrape
from target_list import load_target_urls


# ============================================================
# エントリポイント
# ============================================================


def run_batch_scrape(target_list_path: str) -> int:
    """Run a serial batch by reusing the existing single-article scrape flow."""

    targets = load_target_urls(target_list_path)
    total = len(targets)
    failures = 0

    print(f"Loaded {total} scrape target(s) from {target_list_path}")

    for index, target_url in enumerate(targets, start=1):
        print(f"[{index}/{total}] START {target_url}")

        try:
            run_scrape(target_url)
        except Exception as exc:
            failures += 1
            print(f"[{index}/{total}] FAILED {target_url}: {exc}")
            continue

        print(f"[{index}/{total}] OK {target_url}")

    if failures:
        print(f"Batch finished with failures: {failures}/{total}")
        return 1

    print(f"Batch finished successfully: {total}/{total}")
    return 0


def main():
    """
    CLIエントリポイント。
    - 通常: 記事URL指定でスクレイプ実行
    - inspect: DB内容表示
    """

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py <article_url>")
        print("  python main.py inspect <article_id> <article_type> [--last N]")
        print("  python main.py targets <target_list_path>")
        print("  python main.py batch <target_list_path>")
        sys.exit(1)

    # inspectモード
    if sys.argv[1] == "inspect":

        if len(sys.argv) < 4:
            print("Usage: inspect <article_id> <article_type> [--last N]")
            sys.exit(1)

        article_id = sys.argv[2]
        article_type = sys.argv[3]

        last_n = None
        if "--last" in sys.argv:
            idx = sys.argv.index("--last")
            last_n = int(sys.argv[idx + 1])

        inspect_article(article_id, article_type, last_n)
        return

    if sys.argv[1] == "targets":

        if len(sys.argv) < 3:
            print("Usage: targets <target_list_path>")
            sys.exit(1)

        target_list_path = sys.argv[2]
        targets = load_target_urls(target_list_path)

        print(f"Loaded {len(targets)} scrape target(s) from {target_list_path}")
        for target in targets:
            print(target)
        return

    if sys.argv[1] == "batch":

        if len(sys.argv) < 3:
            print("Usage: batch <target_list_path>")
            sys.exit(1)

        exit_code = run_batch_scrape(sys.argv[2])
        if exit_code != 0:
            sys.exit(exit_code)
        return

    # 通常スクレイプモード
    article_url = sys.argv[1]

    run_scrape(article_url)


if __name__ == "__main__":
    main()
