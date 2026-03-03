"""Tests for nicodic_archiver.backup."""

import json

from nicodic_archiver.backup import export_json
from nicodic_archiver.db import upsert_responses
from tests.conftest import make_response


class TestExportJson:
    def test_creates_json_file(self, tmp_db, tmp_path):
        upsert_responses(tmp_db, "myarticle", [make_response(1), make_response(2)])
        out = export_json(tmp_db, "myarticle", str(tmp_path))

        assert out.exists()
        assert out.name == "myarticle.json"

    def test_json_content(self, tmp_db, tmp_path):
        upsert_responses(tmp_db, "myarticle", [make_response(1, body="hello")])
        out = export_json(tmp_db, "myarticle", str(tmp_path))

        data = json.loads(out.read_text(encoding="utf-8"))
        assert len(data) == 1
        assert data[0]["body"] == "hello"

    def test_creates_directory_if_missing(self, tmp_db, tmp_path):
        new_dir = tmp_path / "subdir"
        assert not new_dir.exists()
        export_json(tmp_db, "art", str(new_dir))
        assert new_dir.exists()

    def test_empty_article_exports_empty_list(self, tmp_db, tmp_path):
        out = export_json(tmp_db, "noresponses", str(tmp_path))
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data == []
