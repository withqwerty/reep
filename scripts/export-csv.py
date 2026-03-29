"""
Export Wikidata entity JSON files to Chadwick-style CSV register files.

Reads from myteam-website/data/wikidata/ and produces:
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
OUTPUT_DIR = Path(__file__).parent.parent / "data"

# Column order for people.csv
PEOPLE_COLUMNS = [
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
]

# Column order for teams.csv
TEAM_COLUMNS = [
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
]

# Column order for names.csv
NAME_COLUMNS = [
    "key_wikidata",
    "name",
    "alias",
]


def load_json(path: Path) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def export_people(players: list[dict], coaches: list[dict], out_path: Path):
    """Export players + coaches to people.csv."""
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PEOPLE_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for entity in sorted(players + coaches, key=lambda e: e.get("name_en", "")):
            row = {
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

            writer.writerow(row)

    return len(players) + len(coaches)


def export_teams(teams: list[dict], out_path: Path):
    """Export teams to teams.csv."""
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TEAM_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for entity in sorted(teams, key=lambda e: e.get("name_en", "")):
            row = {
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

            writer.writerow(row)

    return len(teams)


def export_names(all_entities: list[dict], out_path: Path):
    """Export alias mappings to names.csv."""
    rows = []
    for entity in all_entities:
        aliases_str = entity.get("aliases_en")
        if not aliases_str:
            continue
        name = entity.get("name_en", "")
        for alias in aliases_str.split(", "):
            alias = alias.strip()
            if alias and alias != name:
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

    # Load
    players = load_json(source / "players.json")
    teams = load_json(source / "teams.json")
    coaches = load_json(source / "coachs.json")
    print(f"Loaded: {len(players)} players, {len(teams)} teams, {len(coaches)} coaches")

    # Export
    n_people = export_people(players, coaches, OUTPUT_DIR / "people.csv")
    print(f"Exported {n_people} people to data/people.csv")

    n_teams = export_teams(teams, OUTPUT_DIR / "teams.csv")
    print(f"Exported {n_teams} teams to data/teams.csv")

    n_names = export_names(players + teams + coaches, OUTPUT_DIR / "names.csv")
    print(f"Exported {n_names} aliases to data/names.csv")

    # Write metadata
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Wikidata SPARQL (query.wikidata.org)",
        "counts": {
            "people": n_people,
            "teams": n_teams,
            "aliases": n_names,
        },
    }
    with open(OUTPUT_DIR / "meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    print(f"\nMeta written to data/meta.json")


if __name__ == "__main__":
    main()
