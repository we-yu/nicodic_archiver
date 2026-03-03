"""Tests for nicodic_archiver.db."""

import pytest

from nicodic_archiver.db import (
    get_last_no,
    get_responses,
    list_articles,
    update_scrape_state,
    upsert_responses,
)
from tests.conftest import make_response


class TestUpsertResponses:
    def test_insert_and_retrieve(self, tmp_db):
        responses = [make_response(1), make_response(2)]
        count = upsert_responses(tmp_db, "testarticle", responses)

        assert count == 2
        stored = get_responses(tmp_db, "testarticle")
        assert len(stored) == 2
        assert stored[0]["no"] == 1

    def test_upsert_replaces_existing(self, tmp_db):
        upsert_responses(tmp_db, "art", [make_response(1, body="old")])
        upsert_responses(tmp_db, "art", [make_response(1, body="new")])
        stored = get_responses(tmp_db, "art")
        assert len(stored) == 1
        assert stored[0]["body"] == "new"

    def test_empty_list_returns_zero(self, tmp_db):
        assert upsert_responses(tmp_db, "art", []) == 0

    def test_multiple_articles_isolated(self, tmp_db):
        upsert_responses(tmp_db, "art1", [make_response(1)])
        upsert_responses(tmp_db, "art2", [make_response(1, body="other")])
        assert len(get_responses(tmp_db, "art1")) == 1
        assert len(get_responses(tmp_db, "art2")) == 1


class TestScrapeState:
    def test_get_last_no_returns_zero_when_missing(self, tmp_db):
        assert get_last_no(tmp_db, "unknown") == 0

    def test_update_and_get_last_no(self, tmp_db):
        update_scrape_state(tmp_db, "art", 42)
        assert get_last_no(tmp_db, "art") == 42

    def test_update_increments_correctly(self, tmp_db):
        update_scrape_state(tmp_db, "art", 5)
        update_scrape_state(tmp_db, "art", 99)
        assert get_last_no(tmp_db, "art") == 99


class TestListArticles:
    def test_empty_db(self, tmp_db):
        assert list_articles(tmp_db) == []

    def test_lists_after_insert(self, tmp_db):
        upsert_responses(tmp_db, "alpha", [make_response(1)])
        upsert_responses(tmp_db, "beta", [make_response(1)])
        articles = list_articles(tmp_db)
        assert "alpha" in articles
        assert "beta" in articles
