"""
Fetch custom_ids from D1 and write to data/custom_ids.json.

This is the bridge between reep-custom (private scraping scripts) and
the public CSV export. The JSON file makes custom provider mappings
available without exposing how they were sourced.

Usage:
  python scripts/fetch-custom-ids.py            # fetch from remote D1
  python scripts/fetch-custom-ids.py --local    # fetch from local D1
"""

import argparse
import json
import subprocess
from pathlib import Path

DB_NAME = "football-entities"
OUTPUT = Path(__file__).parent.parent / "data" / "custom_ids.json"
REPO_ROOT = Path(__file__).parent.parent

BATCH_SIZE = 5000


def query_d1(sql: str, local: bool = False) -> list[dict]:
    """Run a SQL query against D1 and return result rows."""
    cmd = ["npx", "wrangler", "d1", "execute", DB_NAME, f"--command={sql}"]
    if not local:
        cmd.append("--remote")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=60, cwd=str(REPO_ROOT)
    )
    try:
        data = json.loads(
            result.stdout[result.stdout.index("[") : result.stdout.rindex("]") + 1]
        )
        return data[0].get("results", [])
    except (json.JSONDecodeError, ValueError, IndexError):
        print(f"Query failed: {result.stderr.strip()}")
        return []


def main():
    parser = argparse.ArgumentParser(description="Fetch custom_ids from D1")
    parser.add_argument("--local", action="store_true", help="Use local D1 instead of remote")
    args = parser.parse_args()

    # Get total count first
    count_rows = query_d1("SELECT COUNT(*) as total FROM custom_ids;", args.local)
    total = count_rows[0]["total"] if count_rows else 0
    print(f"custom_ids in D1: {total}")

    if total == 0:
        print("No custom IDs found.")
        return

    # Fetch all rows (paginated to avoid response size limits)
    all_rows = []
    offset = 0
    while offset < total:
        sql = f"SELECT qid, type, provider, external_id FROM custom_ids ORDER BY qid, type, provider LIMIT {BATCH_SIZE} OFFSET {offset};"
        rows = query_d1(sql, args.local)
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
        print(f"  fetched {len(all_rows)}/{total}")

    # Write output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(all_rows, f, indent=2)

    # Summary by provider
    providers: dict[str, int] = {}
    for row in all_rows:
        providers[row["provider"]] = providers.get(row["provider"], 0) + 1

    print(f"\nWrote {len(all_rows)} custom IDs to {OUTPUT}")
    print("Providers:")
    for p, count in sorted(providers.items(), key=lambda x: -x[1]):
        print(f"  {p}: {count}")


if __name__ == "__main__":
    main()
