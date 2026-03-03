"""CLI entry-points for nicodic_archiver."""

import click

from .backup import export_json
from .config import DEFAULT_DB_PATH, DEFAULT_JSON_DIR
from .db import get_last_no, get_responses, init_db, list_articles, update_scrape_state, upsert_responses
from .scraper import fetch_all_responses, fetch_new_responses


@click.group()
def cli() -> None:
    """nicodic_archiver — scrape and store NicoNico Dictionary BBS comments."""


@cli.command()
@click.argument("article_slug")
@click.option(
    "--db",
    default=DEFAULT_DB_PATH,
    show_default=True,
    help="Path to the SQLite database.",
)
@click.option(
    "--json-dir",
    default=DEFAULT_JSON_DIR,
    show_default=True,
    help="Directory for JSON backup output.",
)
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help="Force a full re-scrape instead of differential update.",
)
def scrape(article_slug: str, db: str, json_dir: str, full: bool) -> None:
    """Scrape BBS responses for ARTICLE_SLUG and save to DB + JSON backup.

    By default only responses newer than the last stored response are fetched
    (differential scraping).  Use --full to re-fetch everything.
    """
    init_db(db)

    if full:
        click.echo(f"[scrape] Full scrape: {article_slug}")
        responses = fetch_all_responses(article_slug)
    else:
        last_no = get_last_no(db, article_slug)
        if last_no == 0:
            click.echo(f"[scrape] No prior data — fetching all: {article_slug}")
        else:
            click.echo(
                f"[scrape] Differential scrape from response #{last_no + 1}: {article_slug}"
            )
        responses = fetch_new_responses(article_slug, last_no)

    if not responses:
        click.echo("[scrape] No new responses found.")
    else:
        written = upsert_responses(db, article_slug, responses)
        new_last_no = responses[-1]["no"]
        update_scrape_state(db, article_slug, new_last_no)
        out = export_json(db, article_slug, json_dir)
        click.echo(
            f"[scrape] Saved {written} responses (up to #{new_last_no}). "
            f"JSON backup: {out}"
        )


@cli.command()
@click.argument("article_slug", required=False, default=None)
@click.option(
    "--db",
    default=DEFAULT_DB_PATH,
    show_default=True,
    help="Path to the SQLite database.",
)
@click.option(
    "--limit",
    default=20,
    show_default=True,
    help="Maximum number of responses to display.",
)
def inspect(article_slug: str | None, db: str, limit: int) -> None:
    """Inspect stored responses.

    If ARTICLE_SLUG is omitted, list all scraped articles.
    """
    if article_slug is None:
        articles = list_articles(db)
        if not articles:
            click.echo("No articles found in database.")
            return
        for a in articles:
            click.echo(a)
        return

    responses = get_responses(db, article_slug)
    if not responses:
        click.echo(f"No responses stored for: {article_slug}")
        return

    click.echo(f"Article: {article_slug}  total stored: {len(responses)}")
    for r in responses[:limit]:
        click.echo(f"  #{r['no']:>5}  [{r['date']}]  {r['body']}")


if __name__ == "__main__":
    cli()
