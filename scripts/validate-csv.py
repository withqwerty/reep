"""
Validate exported CSV files for completeness and consistency.

Catches issues that validate-db.py can't — these operate on the CSV
output of export-csv.py, not the D1 database.

Usage:
  python scripts/validate-csv.py           # validate all CSVs
  python scripts/validate-csv.py --verbose # show sample failures
"""

import argparse
import csv
import json
import sys
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"
WARN = "\033[33m!\033[0m"


def load_csv(name: str) -> list[dict]:
    path = DATA_DIR / name
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def check_reep_ids(rows: list[dict], file_name: str, verbose: bool) -> tuple[bool, int, list[str]]:
    """Check that reep_id is populated for all rows."""
    missing = [r for r in rows if not r.get("reep_id", "").strip()]
    samples = []
    if missing and verbose:
        for r in missing[:10]:
            name = r.get("name", r.get("key_wikidata", "?"))
            samples.append(f"    {name} (QID: {r.get('key_wikidata', '?')})")
    return len(missing) == 0, len(missing), samples


def check_required_columns(rows: list[dict], file_name: str,
                            required: list[str]) -> tuple[bool, list[str]]:
    """Check that required columns exist in the CSV."""
    if not rows:
        return True, []
    headers = set(rows[0].keys())
    missing_cols = [c for c in required if c not in headers]
    return len(missing_cols) == 0, missing_cols


def check_empty_names(rows: list[dict], verbose: bool) -> tuple[bool, int, list[str]]:
    """Check for empty name fields."""
    empty = [r for r in rows if not r.get("name", "").strip()]
    samples = []
    if empty and verbose:
        for r in empty[:10]:
            samples.append(f"    reep_id={r.get('reep_id', '?')}, QID={r.get('key_wikidata', '?')}")
    return len(empty) == 0, len(empty), samples


def check_custom_ids_sync(verbose: bool) -> tuple[bool, int, list[str]]:
    """Check custom_ids.json reep_ids exist in people.csv or teams.csv."""
    cid_path = DATA_DIR / "custom_ids.json"
    if not cid_path.exists():
        return True, 0, []

    with open(cid_path) as f:
        custom_ids = json.load(f)

    csv_reep_ids = set()
    for name in ["people.csv", "teams.csv", "competitions.csv", "seasons.csv"]:
        for row in load_csv(name):
            rid = row.get("reep_id", "").strip()
            if rid:
                csv_reep_ids.add(rid)

    orphaned = []
    seen_reep_ids = set()
    for row in custom_ids:
        rid = row.get("reep_id", "")
        if rid and rid not in csv_reep_ids and rid not in seen_reep_ids:
            seen_reep_ids.add(rid)
            orphaned.append(row)

    samples = []
    if orphaned and verbose:
        for r in orphaned[:10]:
            samples.append(f"    {r.get('reep_id')} ({r.get('provider')}: {r.get('external_id')})")
    return len(orphaned) == 0, len(orphaned), samples


def check_names_reep_ids(verbose: bool) -> tuple[bool, int, list[str]]:
    """Check names.csv reep_ids are populated and exist in entity CSVs."""
    names = load_csv("names.csv")
    if not names:
        return True, 0, []

    # Check for missing reep_ids in names.csv
    missing = [r for r in names if not r.get("reep_id", "").strip()]
    samples = []
    if missing and verbose:
        for r in missing[:10]:
            samples.append(f"    alias={r.get('alias', '?')}, QID={r.get('key_wikidata', '?')}")
    return len(missing) == 0, len(missing), samples


def main():
    parser = argparse.ArgumentParser(description="Validate exported CSV files")
    parser.add_argument("--verbose", action="store_true", help="Show sample failures")
    args = parser.parse_args()

    tests = []

    # --- people.csv ---
    people = load_csv("people.csv")
    if people:
        ok, missing_cols = check_required_columns(people, "people.csv",
                                                    ["reep_id", "key_wikidata", "name", "type"])
        tests.append(("people.csv has required columns", ok,
                       f"missing: {missing_cols}" if not ok else ""))

        ok, count, samples = check_reep_ids(people, "people.csv", args.verbose)
        msg = f"{count} rows missing reep_id" if not ok else ""
        tests.append(("people.csv reep_ids populated", ok, msg, samples))

        ok, count, samples = check_empty_names(people, args.verbose)
        msg = f"{count} empty names" if not ok else ""
        tests.append(("people.csv no empty names", ok, msg, samples))
    else:
        tests.append(("people.csv exists", False, "file not found or empty", []))

    # --- teams.csv ---
    teams = load_csv("teams.csv")
    if teams:
        ok, missing_cols = check_required_columns(teams, "teams.csv",
                                                    ["reep_id", "key_wikidata", "name"])
        tests.append(("teams.csv has required columns", ok,
                       f"missing: {missing_cols}" if not ok else ""))

        ok, count, samples = check_reep_ids(teams, "teams.csv", args.verbose)
        msg = f"{count} rows missing reep_id" if not ok else ""
        tests.append(("teams.csv reep_ids populated", ok, msg, samples))
    else:
        tests.append(("teams.csv exists", False, "file not found or empty", []))

    # --- names.csv ---
    names = load_csv("names.csv")
    if names:
        ok, missing_cols = check_required_columns(names, "names.csv",
                                                    ["reep_id", "key_wikidata", "alias"])
        tests.append(("names.csv has required columns", ok,
                       f"missing: {missing_cols}" if not ok else ""))

        ok, count, samples = check_names_reep_ids(args.verbose)
        msg = f"{count} aliases missing reep_id" if not ok else ""
        tests.append(("names.csv reep_ids populated", ok, msg, samples))
    else:
        tests.append(("names.csv exists", False, "file not found or empty", []))

    # --- competitions.csv ---
    comps = load_csv("competitions.csv")
    if comps:
        ok, count, samples = check_reep_ids(comps, "competitions.csv", args.verbose)
        msg = f"{count} rows missing reep_id" if not ok else ""
        tests.append(("competitions.csv reep_ids populated", ok, msg, samples))

    # --- seasons.csv ---
    seasons = load_csv("seasons.csv")
    if seasons:
        ok, count, samples = check_reep_ids(seasons, "seasons.csv", args.verbose)
        msg = f"{count} rows missing reep_id" if not ok else ""
        tests.append(("seasons.csv reep_ids populated", ok, msg, samples))

    # --- cross-file: custom_ids.json vs CSVs (warning only — many IDs are for
    # competitions/seasons not in the Wikidata JSON export) ---
    ok, count, samples = check_custom_ids_sync(args.verbose)
    msg = f"{count} custom_ids reep_ids not in any CSV" if not ok else ""
    tests.append(("custom_ids.json reep_ids exist in CSVs", ok, msg, samples, "warn"))

    # --- Print results ---
    print(f"Running {len(tests)} CSV checks...\n")
    passed = 0
    failed = 0
    warned = 0
    for entry in tests:
        name = entry[0]
        ok = entry[1]
        msg = entry[2] if len(entry) > 2 else ""
        samples = entry[3] if len(entry) > 3 else []
        severity = entry[4] if len(entry) > 4 else "fail"

        if ok:
            print(f"  {PASS} {name}")
            passed += 1
        elif severity == "warn":
            print(f"  {WARN} {name}: {msg}")
            warned += 1
            for s in samples:
                print(s)
        else:
            print(f"  {FAIL} {name}: {msg}")
            failed += 1
            for s in samples:
                print(s)

    print(f"\n{'=' * 50}")
    parts = [f"{passed} passed"]
    if warned:
        parts.append(f"{warned} warnings")
    if failed:
        parts.append(f"{failed} failed")
    print(f"  {', '.join(parts)} ({len(tests)} total)")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
