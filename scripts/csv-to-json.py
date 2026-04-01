"""
Convert CSV register files back to JSON format for D1 seeding.

Reads people.csv and teams.csv and produces players.json, teams.json, coachs.json
in the same format as fetch-wikidata-entities.py output.

Usage:
  python scripts/csv-to-json.py                    # default data/ paths
  python scripts/csv-to-json.py --output /tmp/out  # custom output dir
"""

import argparse
import csv
import json
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "json"

# Provider columns in people.csv → external_ids key
PEOPLE_PROVIDERS = [
    "transfermarkt", "transfermarkt_manager", "fbref", "soccerway",
    "sofascore", "flashscore", "opta", "premier_league", "11v11",
    "espn", "national_football_teams", "worldfootball", "soccerbase",
    "kicker", "uefa", "lequipe", "fff_fr", "serie_a", "besoccer",
    "footballdatabase_eu", "eu_football_info", "hugman", "german_fa",
    "statmuse_pl", "sofifa", "soccerdonna", "dongqiudi",
    "understat", "whoscored", "fbref_verified", "sportmonks",
    "api_football", "fotmob",
]

TEAM_PROVIDERS = [
    "transfermarkt", "fbref", "soccerway", "opta", "kicker",
    "flashscore", "sofascore", "soccerbase", "uefa",
    "footballdatabase_eu", "worldfootball", "espn", "playmakerstats",
    "clubelo", "sportmonks", "api_football", "sofifa", "fotmob",
]


def main():
    parser = argparse.ArgumentParser(description="Convert CSV register to JSON for seeding")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()

    out = args.output
    out.mkdir(parents=True, exist_ok=True)

    # --- People (players + coaches) ---
    players = []
    coaches = []

    with open(DATA_DIR / "people.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            entity = {
                "qid": row["key_wikidata"],
                "type": row["type"],
                "name_en": row["name"],
            }

            if row.get("full_name"):
                entity["full_name"] = row["full_name"]
            if row.get("date_of_birth"):
                entity["date_of_birth"] = row["date_of_birth"]
            if row.get("nationality"):
                entity["nationality"] = row["nationality"]
            if row.get("position"):
                entity["position"] = row["position"]
            if row.get("height_cm"):
                try:
                    entity["height_cm"] = float(row["height_cm"])
                except ValueError:
                    pass

            # Extract provider IDs
            ext_ids = {}
            for provider in PEOPLE_PROVIDERS:
                val = row.get(f"key_{provider}", "").strip()
                if val:
                    ext_ids[provider] = val
            entity["external_ids"] = ext_ids

            if row["type"] == "coach":
                coaches.append(entity)
            else:
                players.append(entity)

    # --- Teams ---
    teams = []

    with open(DATA_DIR / "teams.csv", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            entity = {
                "qid": row["key_wikidata"],
                "type": "team",
                "name_en": row["name"],
            }

            if row.get("country"):
                entity["country"] = row["country"]
            if row.get("founded"):
                entity["founded"] = row["founded"]
            if row.get("stadium"):
                entity["stadium"] = row["stadium"]

            ext_ids = {}
            for provider in TEAM_PROVIDERS:
                val = row.get(f"key_{provider}", "").strip()
                if val:
                    ext_ids[provider] = val
            entity["external_ids"] = ext_ids

            teams.append(entity)

    # Write JSON
    for name, data in [("players", players), ("teams", teams), ("coachs", coaches)]:
        path = out / f"{name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        print(f"Wrote {len(data):,} {name} to {path}")

    print(f"\nTotal: {len(players):,} players, {len(teams):,} teams, {len(coaches):,} coaches")


if __name__ == "__main__":
    main()
