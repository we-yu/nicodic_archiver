"""Shared fixtures for nicodic_archiver tests."""

import pytest

from nicodic_archiver.db import init_db


@pytest.fixture()
def tmp_db(tmp_path):
    """Return a path to a freshly initialised, empty SQLite database."""
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    return db_path


def make_response(no: int, body: str = "test body", user_id: str = "u1") -> dict:
    """Build a minimal API response dict."""
    return {
        "no": no,
        "userId": user_id,
        "body": body,
        "date": f"2024-01-{no:02d}T00:00:00+09:00",
    }
