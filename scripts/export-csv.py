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

def _read_api_version() -> str:
    """Read API version from package.json (single source of truth)."""
    pkg_path = Path(__file__).parent.parent / "package.json"
    if pkg_path.exists():
        return json.loads(pkg_path.read_text())["version"]
    return "unknown"

API_VERSION = _read_api_version()

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
    "position_detail",
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
    "key_opta_numeric",
    "key_thesportsdb",
    "key_skillcorner",
    "key_wyscout",
    "key_impect",
    "key_heimspiel",
    "key_capology",
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
    "key_thesportsdb",
    "key_understat",
    "key_opta_numeric",
    "key_capology",
]

# Column order for names.csv
NAME_COLUMNS = [
    "reep_id",
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
    "key_opta",          # UUID format
    "key_opta_numeric",  # Legacy numeric
    "key_optacore",
    "key_fotmob",
    "key_whoscored",
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


# Entity type can differ between Wikidata JSON and D1 (e.g. player retired
# and became coach). Try the exact key first, then fall back to other types.
_FALLBACK_TYPES = ["player", "coach", "team", "competition", "season"]


def _resolve_reep_id(reep_id_map: dict[str, str], qid: str, primary_type: str) -> str:
    """Look up reep_id, falling back to alternative type keys if needed.

    For custom entities (from match scripts) the 'qid' slot holds the reep_id
    itself — see fetch-custom-entities.py. Detect that by prefix and return
    directly rather than looking up in the Wikidata-only map.
    """
    if qid and qid.startswith("reep_"):
        return qid
    rid = reep_id_map.get(f"{qid}:{primary_type}")
    if rid:
        return rid
    for t in _FALLBACK_TYPES:
        if t != primary_type:
            rid = reep_id_map.get(f"{qid}:{t}")
            if rid:
                return rid
    return ""


def export_people(players: list[dict], coaches: list[dict], out_path: Path,
                   custom_ids: dict[str, dict[str, str]] | None = None,
                   reep_id_map: dict[str, str] | None = None,
                   position_detail_map: dict[str, str] | None = None):
    """Export players + coaches to people.csv."""
    custom_ids = custom_ids or {}
    reep_id_map = reep_id_map or {}
    position_detail_map = position_detail_map or {}
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PEOPLE_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for entity in sorted(players + coaches, key=lambda e: (e.get("name_en", ""), e.get("type", ""))):
            reep_id = _resolve_reep_id(reep_id_map, entity['qid'], entity['type'])
            row = {
                "reep_id": reep_id,
                "key_wikidata": entity["qid"],
                "type": entity["type"],
                "name": entity.get("name_en", ""),
                "full_name": entity.get("full_name") or "",
                "date_of_birth": entity.get("date_of_birth") or "",
                "nationality": entity.get("nationality") or "",
                "position": entity.get("position") or "",
                "position_detail": position_detail_map.get(reep_id, entity.get("position_detail") or ""),
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
            reep_id = _resolve_reep_id(reep_id_map, entity['qid'], "team")
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


def export_names(all_entities: list[dict], out_path: Path, reep_id_map: dict[str, str],
                  custom_aliases_path: Path | None = None):
    """Export alias mappings to names.csv.

    Merges two sources:
      1. entities.aliases_en (Wikidata alt labels)
      2. custom_aliases.json (name variants discovered by match scripts)
    """
    rows = []
    seen: set[tuple[str, str]] = set()  # (reep_id, alias)

    # Build QID → reep_id reverse lookup
    qid_to_reep: dict[str, str] = {}
    for key, reep_id in reep_id_map.items():
        qid = key.split(":")[0]
        qid_to_reep.setdefault(qid, reep_id)

    # Source 1: Wikidata aliases from entities
    for entity in all_entities:
        aliases_str = entity.get("aliases_en")
        if not aliases_str:
            continue
        qid = entity["qid"]
        reep_id = qid_to_reep.get(qid, "")
        name = entity.get("name_en", "")
        for alias in aliases_str.split(", "):
            alias = alias.strip()
            key = (reep_id or qid, alias)
            if alias and alias != name and key not in seen:
                seen.add(key)
                rows.append({
                    "reep_id": reep_id,
                    "key_wikidata": qid,
                    "name": name,
                    "alias": alias,
                })

    # Source 2: custom_aliases from match scripts
    if custom_aliases_path and custom_aliases_path.exists():
        with open(custom_aliases_path) as f:
            custom_aliases = json.load(f)

        # Build reep_id → (name, qid) lookup from entities
        reep_to_info: dict[str, tuple[str, str]] = {}
        for entity in all_entities:
            qid = entity["qid"]
            rid = qid_to_reep.get(qid, "")
            if rid:
                reep_to_info[rid] = (entity.get("name_en", ""), qid)

        n_custom = 0
        for row in custom_aliases:
            rid = row["reep_id"]
            alias = row["alias"]
            key = (rid, alias)
            if key in seen:
                continue
            seen.add(key)
            name, qid = reep_to_info.get(rid, ("", ""))
            if alias != name:
                rows.append({
                    "reep_id": rid,
                    "key_wikidata": qid,
                    "name": name,
                    "alias": alias,
                })
                n_custom += 1

        print(f"  (includes {n_custom} custom aliases from match scripts)")

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
            reep_id = _resolve_reep_id(reep_id_map, entity['qid'], "competition")
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
            reep_id = _resolve_reep_id(reep_id_map, entity['qid'], "season")
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

    # Staleness check: warn if reep_id_map.json is older than custom_ids.json
    if REEP_ID_MAP_PATH.exists() and CUSTOM_IDS_PATH.exists():
        map_mtime = REEP_ID_MAP_PATH.stat().st_mtime
        cid_mtime = CUSTOM_IDS_PATH.stat().st_mtime
        if map_mtime < cid_mtime:
            age_mins = (cid_mtime - map_mtime) / 60
            print(f"\n  WARNING: reep_id_map.json is {age_mins:.0f}min older than custom_ids.json.")
            print("  Run fetch-custom-ids.py first to ensure reep_ids are up to date.")
            print("  Continuing, but some reep_ids may be missing.\n")

    # Load position_detail map (generated by fetch-custom-ids.py)
    pos_detail_path = OUTPUT_DIR / "position_detail.json"
    position_detail_map: dict[str, str] = {}
    if pos_detail_path.exists():
        with open(pos_detail_path) as f:
            position_detail_map = json.load(f)
        print(f"Loaded {len(position_detail_map):,} position_detail values")

    # Export
    n_people = export_people(players, coaches, OUTPUT_DIR / "people.csv", custom_ids, reep_id_map, position_detail_map)
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
    custom_aliases_path = OUTPUT_DIR / "custom_aliases.json"
    n_names = export_names(all_entities, OUTPUT_DIR / "names.csv", reep_id_map, custom_aliases_path)
    print(f"Exported {n_names} aliases to data/names.csv")

    # Write metadata
    now = datetime.now(timezone.utc)
    data_version = f"{now.isocalendar().year}.{now.isocalendar().week:02d}"
    meta = {
        "data_version": data_version,
        "api_version": API_VERSION,
        "generated_at": now.isoformat(),
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
