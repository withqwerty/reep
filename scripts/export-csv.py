"""
Export Wikidata entity JSON files to Chadwick-style CSV register files.

Reads from data/json/ (Wikidata) and optionally data/custom_ids.json
(custom provider mappings) to produce:
  data/people.csv     — all players and coaches with bio + provider IDs
  data/teams.csv      — all teams with bio + provider IDs
  data/names.csv      — alternate names / aliases

Usage:
  python scripts/export-csv.py                          # default paths
  python scripts/export-csv.py --source /path/to/json   # custom source dir
"""

import argparse
import csv
import json
from pathlib import Path
from datetime import datetime, timezone

DEFAULT_SOURCE = Path(__file__).parent.parent / "data" / "json"
CUSTOM_IDS_PATH = Path(__file__).parent.parent / "data" / "custom_ids.json"
REEP_ID_MAP_PATH = Path(__file__).parent.parent / "data" / "reep_id_map.json"
OUTPUT_DIR = Path(__file__).parent.parent / "data"

# Column order for people.csv
PEOPLE_COLUMNS = [
    "reep_id",
    "key_wikidata",
    "type",
    "name",
    "full_name",
    "date_of_birth",
    "nationality",
    "position",
    "height_cm",
    # Provider IDs
    "key_transfermarkt",
    "key_transfermarkt_manager",
    "key_fbref",
    "key_soccerway",
    "key_sofascore",
    "key_flashscore",
    "key_opta",
    "key_premier_league",
    "key_11v11",
    "key_espn",
    "key_national_football_teams",
    "key_worldfootball",
    "key_soccerbase",
    "key_kicker",
    # New providers
    "key_uefa",
    "key_lequipe",
    "key_fff_fr",
    "key_serie_a",
    "key_besoccer",
    "key_footballdatabase_eu",
    "key_eu_football_info",
    "key_hugman",
    "key_german_fa",
    "key_statmuse_pl",
    "key_sofifa",
    "key_soccerdonna",
    "key_dongqiudi",
    # Custom verified providers (sourced outside Wikidata)
    "key_understat",
    "key_whoscored",
    "key_fbref_verified",
    "key_sportmonks",
    "key_api_football",
    "key_fotmob",
    "key_fpl_code",
    "key_thesportsdb",
    "key_skillcorner",
    "key_wyscout",
    "key_impect",
    "key_heimspiel",
]

# Column order for teams.csv
TEAM_COLUMNS = [
    "reep_id",
    "key_wikidata",
    "name",
    "country",
    "founded",
    "stadium",
    # Provider IDs
    "key_transfermarkt",
    "key_fbref",
    "key_soccerway",
    "key_opta",
    # New providers
    "key_kicker",
    "key_flashscore",
    "key_sofascore",
    "key_soccerbase",
    "key_uefa",
    "key_footballdatabase_eu",
    "key_worldfootball",
    "key_espn",
    "key_playmakerstats",
    # Custom verified providers (sourced outside Wikidata)
    "key_clubelo",
    "key_sportmonks",
    "key_api_football",
    "key_sofifa",
    "key_fotmob",
]

# Column order for names.csv
NAME_COLUMNS = [
    "key_wikidata",
    "name",
    "alias",
]

COMPETITION_COLUMNS = [
    "reep_id",
    "key_wikidata",
    "name",
    "country",
    # Provider IDs
    "key_transfermarkt",
    "key_fbref",
    "key_opta",
]

SEASON_COLUMNS = [
    "reep_id",
    "key_wikidata",
    "name",
    "competition_reep_id",
]


def load_json(path: Path) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def load_custom_ids(path: Path) -> dict[str, dict[str, str]]:
    """Load custom_ids.json into {reep_id: {provider: external_id}} lookup."""
    if not path.exists():
        return {}
    with open(path) as f:
        rows = json.load(f)
    lookup: dict[str, dict[str, str]] = {}
    for row in rows:
        key = row.get("reep_id")
        if not key:
            # Legacy format: fall back to qid+type (pre-rekey)
            key = f"{row['qid']}:{row.get('type', 'player')}"
        lookup.setdefault(key, {})[row["provider"]] = row["external_id"]
    print(f"Loaded {len(rows)} custom IDs for {len(lookup)} entities")
    return lookup


def load_reep_id_map(path: Path) -> dict[str, str]:
    """Load reep_id_map.json: {'qid:type' -> reep_id}."""
    if not path.exists():
        print(f"  No reep_id map at {path} — reep_id column will be empty")
        return {}
    with open(path) as f:
        data = json.load(f)
    print(f"Loaded {len(data):,} reep_id mappings")
    return data


def export_people(players: list[dict], coaches: list[dict], out_path: Path,
                   custom_ids: dict[str, dict[str, str]] | None = None,
                   reep_id_map: dict[str, str] | None = None):
    """Export players + coaches to people.csv."""
    custom_ids = custom_ids or {}
    reep_id_map = reep_id_map or {}
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PEOPLE_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for entity in sorted(players + coaches, key=lambda e: (e.get("name_en", ""), e.get("type", ""))):
            reep_id = reep_id_map.get(f"{entity['qid']}:{entity['type']}", "")
            row = {
                "reep_id": reep_id,
                "key_wikidata": entity["qid"],
                "type": entity["type"],
                "name": entity.get("name_en", ""),
                "full_name": entity.get("full_name") or "",
                "date_of_birth": entity.get("date_of_birth") or "",
                "nationality": entity.get("nationality") or "",
                "position": entity.get("position") or "",
                "height_cm": entity.get("height_cm") or "",
            }

            ids = entity.get("external_ids", {})
            for provider, ext_id in ids.items():
                col = f"key_{provider}"
                if col in PEOPLE_COLUMNS:
                    row[col] = ext_id

            # Merge custom IDs (don't overwrite Wikidata). Key is reep_id or qid:type fallback.
            custom_key = reep_id or f"{entity['qid']}:{entity['type']}"
            for provider, ext_id in custom_ids.get(custom_key, {}).items():
                col = f"key_{provider}"
                if col in PEOPLE_COLUMNS and col not in row:
                    row[col] = ext_id

            writer.writerow(row)

    return len(players) + len(coaches)


def export_teams(teams: list[dict], out_path: Path,
                 custom_ids: dict[str, dict[str, str]] | None = None,
                 reep_id_map: dict[str, str] | None = None):
    """Export teams to teams.csv."""
    custom_ids = custom_ids or {}
    reep_id_map = reep_id_map or {}
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TEAM_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for entity in sorted(teams, key=lambda e: e.get("name_en", "")):
            reep_id = reep_id_map.get(f"{entity['qid']}:team", "")
            row = {
                "reep_id": reep_id,
                "key_wikidata": entity["qid"],
                "name": entity.get("name_en", ""),
                "country": entity.get("country") or "",
                "founded": entity.get("founded") or "",
                "stadium": entity.get("stadium") or "",
            }

            ids = entity.get("external_ids", {})
            for provider, ext_id in ids.items():
                col = f"key_{provider}"
                if col in TEAM_COLUMNS:
                    row[col] = ext_id

            # Merge custom IDs (don't overwrite Wikidata)
            custom_key = reep_id or f"{entity['qid']}:team"
            for provider, ext_id in custom_ids.get(custom_key, {}).items():
                col = f"key_{provider}"
                if col in TEAM_COLUMNS and col not in row:
                    row[col] = ext_id

            writer.writerow(row)

    return len(teams)


def export_names(all_entities: list[dict], out_path: Path):
    """Export alias mappings to names.csv."""
    rows = []
    seen: set[tuple[str, str]] = set()
    for entity in all_entities:
        aliases_str = entity.get("aliases_en")
        if not aliases_str:
            continue
        name = entity.get("name_en", "")
        for alias in aliases_str.split(", "):
            alias = alias.strip()
            key = (entity["qid"], alias)
            if alias and alias != name and key not in seen:
                seen.add(key)
                rows.append({
                    "key_wikidata": entity["qid"],
                    "name": name,
                    "alias": alias,
                })

    rows.sort(key=lambda r: (r["name"], r["alias"]))

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=NAME_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def export_competitions(competitions: list[dict], out_path: Path,
                        custom_ids: dict[str, dict[str, str]] | None = None,
                        reep_id_map: dict[str, str] | None = None):
    """Export competitions to competitions.csv."""
    custom_ids = custom_ids or {}
    reep_id_map = reep_id_map or {}
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COMPETITION_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for entity in sorted(competitions, key=lambda e: e.get("name_en", "")):
            reep_id = reep_id_map.get(f"{entity['qid']}:competition", "")
            row = {
                "reep_id": reep_id,
                "key_wikidata": entity["qid"],
                "name": entity.get("name_en", ""),
                "country": entity.get("country") or "",
            }

            ids = entity.get("external_ids", {})
            for provider, ext_id in ids.items():
                col = f"key_{provider}"
                if col in COMPETITION_COLUMNS:
                    row[col] = ext_id

            custom_key = reep_id or f"{entity['qid']}:competition"
            for provider, ext_id in custom_ids.get(custom_key, {}).items():
                col = f"key_{provider}"
                if col in COMPETITION_COLUMNS and col not in row:
                    row[col] = ext_id

            writer.writerow(row)

    return len(competitions)


def export_seasons(seasons: list[dict], out_path: Path,
                   custom_ids: dict[str, dict[str, str]] | None = None,
                   reep_id_map: dict[str, str] | None = None):
    """Export seasons to seasons.csv."""
    custom_ids = custom_ids or {}
    reep_id_map = reep_id_map or {}
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SEASON_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for entity in sorted(seasons, key=lambda e: e.get("name_en", "")):
            reep_id = reep_id_map.get(f"{entity['qid']}:season", "")
            row = {
                "reep_id": reep_id,
                "key_wikidata": entity["qid"],
                "name": entity.get("name_en", ""),
                # TODO(#9): competition_reep_id is blank in CSV because the QID->reep_id
                # mapping requires reep_id_map.json (generated from D1). Could resolve via
                # reep_id_map.get(f"{entity.get('competition_qid')}:competition", "")
                # but adds ordering dependency on fetch-custom-ids.py running first.
                "competition_reep_id": "",
            }

            custom_key = reep_id or f"{entity['qid']}:season"
            for provider, ext_id in custom_ids.get(custom_key, {}).items():
                col = f"key_{provider}"
                if col in SEASON_COLUMNS and col not in row:
                    row[col] = ext_id

            writer.writerow(row)

    return len(seasons)


def main():
    parser = argparse.ArgumentParser(description="Export Wikidata entities to CSV register")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="Source JSON directory")
    args = parser.parse_args()

    source = args.source
    if not source.exists():
        print(f"Source directory not found: {source}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Source: {source}")
    print(f"Output: {OUTPUT_DIR}\n")

    # Load Wikidata entities
    players = load_json(source / "players.json")
    teams = load_json(source / "teams.json")
    coaches = load_json(source / "coachs.json")
    print(f"Loaded: {len(players)} players, {len(teams)} teams, {len(coaches)} coaches")

    # Load competition and season entities (if available)
    comp_path = source / "competitions.json"
    season_path = source / "seasons.json"
    competitions = load_json(comp_path) if comp_path.exists() else []
    seasons = load_json(season_path) if season_path.exists() else []
    if competitions:
        print(f"Loaded: {len(competitions)} competitions")
    if seasons:
        print(f"Loaded: {len(seasons)} seasons")

    # Load custom IDs (if available)
    custom_ids = load_custom_ids(CUSTOM_IDS_PATH)

    # Load reep_id map (generated by fetch-custom-ids.py)
    reep_id_map = load_reep_id_map(REEP_ID_MAP_PATH)

    # Export
    n_people = export_people(players, coaches, OUTPUT_DIR / "people.csv", custom_ids, reep_id_map)
    print(f"Exported {n_people} people to data/people.csv")

    n_teams = export_teams(teams, OUTPUT_DIR / "teams.csv", custom_ids, reep_id_map)
    print(f"Exported {n_teams} teams to data/teams.csv")

    if competitions:
        n_comp = export_competitions(competitions, OUTPUT_DIR / "competitions.csv", custom_ids, reep_id_map)
        print(f"Exported {n_comp} competitions to data/competitions.csv")

    if seasons:
        n_seasons = export_seasons(seasons, OUTPUT_DIR / "seasons.csv", custom_ids, reep_id_map)
        print(f"Exported {n_seasons} seasons to data/seasons.csv")

    all_entities = players + teams + coaches + competitions + seasons
    n_names = export_names(all_entities, OUTPUT_DIR / "names.csv")
    print(f"Exported {n_names} aliases to data/names.csv")

    # Write metadata
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Wikidata SPARQL + custom verified mappings",
        "counts": {
            "people": n_people,
            "teams": n_teams,
            "competitions": len(competitions),
            "seasons": len(seasons),
            "aliases": n_names,
            "custom_ids": sum(len(v) for v in custom_ids.values()),
        },
    }
    with open(OUTPUT_DIR / "meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    print(f"\nMeta written to data/meta.json")


if __name__ == "__main__":
    main()
