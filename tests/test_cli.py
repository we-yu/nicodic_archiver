"""Tests for nicodic_archiver.cli."""

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from nicodic_archiver.cli import cli
from nicodic_archiver.db import init_db, upsert_responses, update_scrape_state
from tests.conftest import make_response


@pytest.fixture()
def runner():
    return CliRunner()


class TestScrapeCommand:
    def _api_response(self, responses, total):
        return {"response": responses, "totalResponseCount": total}

    def test_full_scrape_saves_responses(self, runner, tmp_db, tmp_path):
        responses = [make_response(i) for i in range(1, 4)]
        api_data = self._api_response(responses, 3)

        with patch("nicodic_archiver.scraper.requests") as mock_req:
            mock_req.get.return_value.raise_for_status.return_value = None
            mock_req.get.return_value.json.return_value = api_data
            result = runner.invoke(
                cli,
                ["scrape", "testarticle", "--db", tmp_db, "--json-dir", str(tmp_path), "--full"],
            )

        assert result.exit_code == 0, result.output
        assert "Saved 3 responses" in result.output

    def test_differential_scrape_uses_last_no(self, runner, tmp_db, tmp_path):
        # Pre-seed the state so last_no == 5
        upsert_responses(tmp_db, "art", [make_response(i) for i in range(1, 6)])
        update_scrape_state(tmp_db, "art", 5)

        new_responses = [make_response(6), make_response(7)]
        api_data = self._api_response(new_responses, 7)

        with patch("nicodic_archiver.scraper.requests") as mock_req:
            mock_req.get.return_value.raise_for_status.return_value = None
            mock_req.get.return_value.json.return_value = api_data
            result = runner.invoke(
                cli,
                ["scrape", "art", "--db", tmp_db, "--json-dir", str(tmp_path)],
            )

        assert result.exit_code == 0, result.output
        assert "Differential scrape from response #6" in result.output
        assert "Saved 2 responses" in result.output
        # Verify the API was called with from=6
        call_params = mock_req.get.call_args[1]["params"]
        assert call_params["from"] == 6

    def test_no_new_responses_message(self, runner, tmp_db, tmp_path):
        update_scrape_state(tmp_db, "art", 10)
        api_data = self._api_response([], 10)

        with patch("nicodic_archiver.scraper.requests") as mock_req:
            mock_req.get.return_value.raise_for_status.return_value = None
            mock_req.get.return_value.json.return_value = api_data
            result = runner.invoke(
                cli,
                ["scrape", "art", "--db", tmp_db, "--json-dir", str(tmp_path)],
            )

        assert result.exit_code == 0, result.output
        assert "No new responses" in result.output


class TestInspectCommand:
    def test_inspect_lists_articles_when_no_slug(self, runner, tmp_db):
        upsert_responses(tmp_db, "alpha", [make_response(1)])
        result = runner.invoke(cli, ["inspect", "--db", tmp_db])
        assert result.exit_code == 0, result.output
        assert "alpha" in result.output

    def test_inspect_shows_responses(self, runner, tmp_db):
        upsert_responses(tmp_db, "myart", [make_response(1, body="hello")])
        result = runner.invoke(cli, ["inspect", "myart", "--db", tmp_db])
        assert result.exit_code == 0, result.output
        assert "hello" in result.output
        assert "#    1" in result.output

    def test_inspect_empty_db(self, runner, tmp_db):
        result = runner.invoke(cli, ["inspect", "--db", tmp_db])
        assert result.exit_code == 0
        assert "No articles" in result.output

    def test_inspect_unknown_article(self, runner, tmp_db):
        result = runner.invoke(cli, ["inspect", "unknown", "--db", tmp_db])
        assert result.exit_code == 0
        assert "No responses" in result.output
