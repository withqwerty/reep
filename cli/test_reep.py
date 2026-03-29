"""
Tests for the reep CLI.

Run: python -m pytest cli/test_reep.py -v
"""

import csv
import json
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

# Import the CLI module
import sys
sys.path.insert(0, str(Path(__file__).parent))
import reep


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

SAMPLE_ENTITY = {
    "qid": "Q99760796",
    "type": "player",
    "name_en": "Cole Palmer",
    "aliases_en": "Cole Jermaine Palmer",
    "date_of_birth": "2002-05-06",
    "nationality": "United Kingdom",
    "position": "attacking midfielder",
    "height_cm": 185,
    "external_ids": {
        "transfermarkt": "568177",
        "fbref": "dc7f8a28",
        "sofascore": "982780",
        "soccerway": "525801",
    },
}

SAMPLE_TEAM = {
    "qid": "Q9617",
    "type": "team",
    "name_en": "Arsenal F.C.",
    "aliases_en": "Arsenal, The Gunners",
    "country": "United Kingdom",
    "stadium": "Emirates Stadium",
    "external_ids": {
        "transfermarkt": "11",
        "fbref": "18bb7c10",
    },
}

SAMPLE_SEARCH_RESPONSE = {"results": [SAMPLE_ENTITY], "count": 1}
SAMPLE_LOOKUP_RESPONSE = {"results": [SAMPLE_ENTITY]}
SAMPLE_STATS_RESPONSE = {
    "total_entities": 435280,
    "by_type": {"player": 386025, "team": 45347, "coach": 3908},
    "by_provider": {"transfermarkt": 209419, "fbref": 106143},
}


# ---------------------------------------------------------------------------
# format_entity tests
# ---------------------------------------------------------------------------

class TestFormatEntity:
    def test_basic_format(self):
        output = reep.format_entity(SAMPLE_ENTITY)
        assert "Cole Palmer" in output
        assert "player" in output
        assert "Q99760796" in output
        assert "transfermarkt" in output
        assert "568177" in output

    def test_verbose_shows_bio(self):
        output = reep.format_entity(SAMPLE_ENTITY, verbose=True)
        assert "2002-05-06" in output
        assert "United Kingdom" in output
        assert "attacking midfielder" in output
        assert "185cm" in output
        assert "Cole Jermaine Palmer" in output

    def test_non_verbose_hides_bio(self):
        output = reep.format_entity(SAMPLE_ENTITY, verbose=False)
        assert "2002-05-06" not in output
        assert "attacking midfielder" not in output

    def test_team_format(self):
        output = reep.format_entity(SAMPLE_TEAM, verbose=True)
        assert "Arsenal F.C." in output
        assert "team" in output
        assert "Emirates Stadium" in output
        assert "United Kingdom" in output

    def test_empty_ids(self):
        entity = {"name_en": "Nobody", "type": "player", "qid": "Q1", "external_ids": {}}
        output = reep.format_entity(entity)
        assert "(no provider IDs)" in output

    def test_missing_fields_dont_crash(self):
        entity = {"name_en": "Minimal", "type": "player", "qid": "Q1"}
        output = reep.format_entity(entity)
        assert "Minimal" in output

    def test_uses_name_fallback(self):
        entity = {"name": "Fallback Name", "type": "coach", "key_wikidata": "Q2"}
        output = reep.format_entity(entity)
        assert "Fallback Name" in output
        assert "Q2" in output


# ---------------------------------------------------------------------------
# API command tests (mocked)
# ---------------------------------------------------------------------------

class TestSearchCommand:
    @patch("reep.api_get")
    def test_search_prints_results(self, mock_api, capsys):
        mock_api.return_value = SAMPLE_SEARCH_RESPONSE
        args = SimpleNamespace(name="Cole Palmer", type=None, limit=10, verbose=False)
        reep.cmd_search(args)
        output = capsys.readouterr().out
        assert "Cole Palmer" in output
        assert "transfermarkt" in output

    @patch("reep.api_get")
    def test_search_no_results(self, mock_api, capsys):
        mock_api.return_value = {"results": [], "count": 0}
        args = SimpleNamespace(name="Nonexistent Player", type=None, limit=10, verbose=False)
        reep.cmd_search(args)
        output = capsys.readouterr().out
        assert "No results found" in output

    @patch("reep.api_get")
    def test_search_with_type_filter(self, mock_api):
        mock_api.return_value = SAMPLE_SEARCH_RESPONSE
        args = SimpleNamespace(name="Arsenal", type="team", limit=10, verbose=False)
        reep.cmd_search(args)
        call_path = mock_api.call_args[0][0]
        assert "type=team" in call_path


class TestResolveCommand:
    @patch("reep.api_get")
    def test_resolve_prints_entity(self, mock_api, capsys):
        mock_api.return_value = SAMPLE_LOOKUP_RESPONSE
        args = SimpleNamespace(provider="transfermarkt", id="568177")
        reep.cmd_resolve(args)
        output = capsys.readouterr().out
        assert "Cole Palmer" in output

    @patch("reep.api_get")
    def test_resolve_no_match(self, mock_api, capsys):
        mock_api.return_value = {"results": []}
        args = SimpleNamespace(provider="transfermarkt", id="999999999")
        reep.cmd_resolve(args)
        output = capsys.readouterr().out
        assert "No entity found" in output


class TestLookupCommand:
    @patch("reep.api_get")
    def test_lookup_by_qid(self, mock_api, capsys):
        mock_api.return_value = SAMPLE_LOOKUP_RESPONSE
        args = SimpleNamespace(qid="Q99760796")
        reep.cmd_lookup(args)
        output = capsys.readouterr().out
        assert "Cole Palmer" in output


class TestTranslateCommand:
    @patch("reep.api_get")
    def test_translate_outputs_id_only(self, mock_api, capsys):
        mock_api.return_value = SAMPLE_LOOKUP_RESPONSE
        args = SimpleNamespace(source="transfermarkt", id="568177", target="fbref")
        reep.cmd_translate(args)
        output = capsys.readouterr().out.strip()
        assert output == "dc7f8a28"

    @patch("reep.api_get")
    def test_translate_missing_target(self, mock_api, capsys):
        mock_api.return_value = SAMPLE_LOOKUP_RESPONSE
        args = SimpleNamespace(source="transfermarkt", id="568177", target="nonexistent")
        with pytest.raises(SystemExit):
            reep.cmd_translate(args)


class TestStatsCommand:
    @patch("reep.api_get")
    def test_stats_output(self, mock_api, capsys):
        mock_api.return_value = SAMPLE_STATS_RESPONSE
        args = SimpleNamespace()
        reep.cmd_stats(args)
        output = capsys.readouterr().out
        assert "435,280" in output
        assert "player" in output
        assert "transfermarkt" in output


# ---------------------------------------------------------------------------
# Local search tests
# ---------------------------------------------------------------------------

class TestLocalSearch:
    @pytest.fixture
    def local_data(self, tmp_path):
        """Create temporary CSV files for local search testing."""
        people = tmp_path / "people.csv"
        teams = tmp_path / "teams.csv"

        with open(people, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "key_wikidata", "type", "name", "full_name", "date_of_birth",
                "nationality", "position", "height_cm",
                "key_transfermarkt", "key_fbref",
            ])
            writer.writeheader()
            writer.writerow({
                "key_wikidata": "Q99760796", "type": "player",
                "name": "Cole Palmer", "full_name": "Cole Jermaine Palmer",
                "date_of_birth": "2002-05-06", "nationality": "United Kingdom",
                "position": "attacking midfielder", "height_cm": "185",
                "key_transfermarkt": "568177", "key_fbref": "dc7f8a28",
            })
            writer.writerow({
                "key_wikidata": "Q11893", "type": "player",
                "name": "Mohamed Salah", "full_name": "",
                "date_of_birth": "1992-06-15", "nationality": "Egypt",
                "position": "forward", "height_cm": "175",
                "key_transfermarkt": "148455", "key_fbref": "e342ad68",
            })

        with open(teams, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "key_wikidata", "name", "country", "key_transfermarkt", "key_fbref",
            ])
            writer.writeheader()
            writer.writerow({
                "key_wikidata": "Q9617", "name": "Arsenal F.C.",
                "country": "United Kingdom",
                "key_transfermarkt": "11", "key_fbref": "18bb7c10",
            })

        return tmp_path

    @patch.object(reep, "DATA_DIR")
    def test_local_search_finds_player(self, mock_dir, local_data, capsys):
        mock_dir.__truediv__ = lambda self, x: local_data / x
        # Patch DATA_DIR directly
        original = reep.DATA_DIR
        reep.DATA_DIR = local_data
        try:
            args = SimpleNamespace(name="Palmer", type=None, limit=10, verbose=False)
            reep.cmd_local_search(args)
            output = capsys.readouterr().out
            assert "Cole Palmer" in output
            assert "transfermarkt" in output
        finally:
            reep.DATA_DIR = original

    @patch.object(reep, "DATA_DIR")
    def test_local_search_type_filter(self, mock_dir, local_data, capsys):
        reep.DATA_DIR = local_data
        try:
            args = SimpleNamespace(name="Arsenal", type="team", limit=10, verbose=False)
            reep.cmd_local_search(args)
            output = capsys.readouterr().out
            assert "Arsenal" in output
        finally:
            reep.DATA_DIR = Path.home() / ".reep"

    @patch.object(reep, "DATA_DIR")
    def test_local_search_no_results(self, mock_dir, local_data, capsys):
        reep.DATA_DIR = local_data
        try:
            args = SimpleNamespace(name="Nonexistent", type=None, limit=10, verbose=False)
            reep.cmd_local_search(args)
            output = capsys.readouterr().out
            assert "No results found" in output
        finally:
            reep.DATA_DIR = Path.home() / ".reep"
