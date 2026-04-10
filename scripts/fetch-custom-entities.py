"""
Fetch entities created by match scripts (source != 'wikidata') from D1 and merge
them into the local data/json/*.json files so export-csv.py picks them up.

Background
----------
fetch-wikidata-entities.py only writes Wikidata-sourced entities to json/*.json.
Match scripts (sync-transfermarkt-datasets.py, import-opta-entities.py, ...) write
directly to D1 when they create new entities — those never make it back into json
without this step, which is why 15,000+ custom entities have been silently absent
from the public CSVs.

This script is idempotent: runs the D1 query, formats each row to match the
shape of the existing json entries, and merges by reep_id so re-running does
nothing unless there are new entities in D1.

Usage:
    python scripts/fetch-custom-entities.py          # merge all types
    python scripts/fetch-custom-entities.py --dry-run
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

DB_NAME = "football-entities"
ROOT = Path(__file__).parent.parent
JSON_DIR = ROOT / "data" / "json"

# D1 type → json filename
TYPE_TO_FILE = {
    "player": "players.json",
    "team": "teams.json",
    "coach": "coachs.json",
    "competition": "competitions.json",
    "season": "seasons.json",
}


def query_d1(sql: str) -> list[dict]:
    """Run a JSON query against remote D1 and return the result rows."""
    result = subprocess.run(
        ["pnpm", "exec", "wrangler", "d1", "execute", DB_NAME,
         "--remote", "--json", f"--command={sql}"],
        capture_output=True, text=True, timeout=300, cwd=str(ROOT),
    )
    if result.returncode != 0:
        print(f"D1 error: {result.stderr[:500]}", file=sys.stderr)
        return []
    try:
        data = json.loads(result.stdout)
        return data[0].get("results", [])
    except (json.JSONDecodeError, ValueError, IndexError, KeyError):
        return []


def load_json_file(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return json.load(f)


def save_json_file(path: Path, rows: list[dict]) -> None:
    with open(path, "w") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def format_d1_row_as_json_entry(row: dict) -> dict:
    """Shape a D1 entities row like the existing json/{type}s.json entries.

    The existing Wikidata-sourced entries use `qid` as the primary key. For
    custom entities we store the reep_id in the `qid` slot as a fallback —
    this keeps every record keyed and lets export-csv.py's reep_id resolution
    still work (it tries the reep_id_map first, then falls back on `qid`).
    """
    return {
        "qid": row.get("reep_id", ""),  # reep_id as primary key for custom entities
        "reep_id": row.get("reep_id", ""),
        "type": row.get("type", ""),
        "name_en": row.get("name_en", ""),
        "aliases_en": row.get("aliases_en"),
        "full_name": row.get("full_name"),
        "date_of_birth": row.get("date_of_birth"),
        "nationality": row.get("nationality"),
        "position": row.get("position"),
        "current_team_qid": None,
        "height_cm": row.get("height_cm"),
        "country": row.get("country"),
        "founded": row.get("founded"),
        "stadium": row.get("stadium"),
        "competition_qid": None,
        "external_ids": {},  # populated from custom_ids.json at export time
        "source": row.get("source", ""),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("Fetching custom entities (source != 'wikidata') from D1...")
    # Pull in a single query, then split by type
    rows = query_d1(
        "SELECT reep_id, type, name_en, aliases_en, full_name, date_of_birth, "
        "nationality, position, height_cm, country, founded, stadium, source "
        "FROM entities WHERE source != 'wikidata' AND source IS NOT NULL"
    )
    print(f"  {len(rows):,} custom entities in D1")

    by_type: dict[str, list[dict]] = {}
    for r in rows:
        by_type.setdefault(r["type"], []).append(r)
    for t, rs in sorted(by_type.items()):
        print(f"    {t}: {len(rs):,}")

    for entity_type, d1_rows in by_type.items():
        filename = TYPE_TO_FILE.get(entity_type)
        if not filename:
            print(f"  [skip] unknown type: {entity_type}")
            continue
        path = JSON_DIR / filename
        existing = load_json_file(path)

        # Build a reep_id index of current file contents
        # (both new custom entries and any that previously had source populated)
        existing_ids = set()
        for e in existing:
            rid = e.get("reep_id") or e.get("qid", "")
            if rid and rid.startswith("reep_"):
                existing_ids.add(rid)

        new_entries = []
        for row in d1_rows:
            rid = row["reep_id"]
            if rid in existing_ids:
                continue
            new_entries.append(format_d1_row_as_json_entry(row))

        print(f"  {filename}: {len(existing):,} existing, +{len(new_entries)} new")

        if new_entries and not args.dry_run:
            merged = existing + new_entries
            save_json_file(path, merged)
            print(f"    wrote {len(merged):,} rows")

    if args.dry_run:
        print("\n(dry run — no files written)")


if __name__ == "__main__":
    main()
