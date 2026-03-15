import sys

from cli import inspect_article
from orchestrator import run_scrape
from target_list import load_target_list


# ============================================================
# エントリポイント
# ============================================================

def main():
    """
    CLIエントリポイント。
    - 通常: 記事URL指定でスクレイプ実行
    - inspect: DB内容表示
    """

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python main.py <article_url>")
        print("  python main.py --target-list <file>")
        print("  python main.py inspect <article_id> <article_type> [--last N]")
        sys.exit(1)

    # target list: provisional file-based source; run first target only (no batch).
    if sys.argv[1] == "--target-list":
        if len(sys.argv) < 3:
            print("Usage: python main.py --target-list <file>")
            sys.exit(1)
        urls = load_target_list(sys.argv[2])
        if not urls:
            print("Target list is empty or had no valid lines.")
            sys.exit(1)
        run_scrape(urls[0])
        return

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

    # 通常スクレイプモード
    article_url = sys.argv[1]

    run_scrape(article_url)


if __name__ == "__main__":
    main()
