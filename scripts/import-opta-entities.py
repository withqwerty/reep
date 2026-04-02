"""
Import Opta-only players as new entities in D1.

Phase 3, Unit 9: Creates entities for Opta players not in Wikidata.
These players have Opta F1 IDs but no Wikidata QID and were previously
unresolvable. Each gets a minted reep_id and source=opta.

The script:
  1. Loads the Opta CSV and existing custom_ids Opta mappings
  2. Identifies unmatched players (Opta ID not in custom_ids)
  3. Mints reep_ids and creates entity rows
  4. Inserts Opta IDs into custom_ids

Usage:
  python scripts/import-opta-entities.py --dry-run    # preview counts
  python scripts/import-opta-entities.py               # import to remote D1
  python scripts/import-opta-entities.py --local       # use local D1
"""

import argparse
import csv
import json
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

DB_NAME = "football-entities"
REPO_ROOT = Path(__file__).parent.parent
OPTA_CSV = Path(__file__).parent.parent.parent / "reep-custom" / "data" / "opta" / "player_database.all.-.Copy.csv"
BATCH_SIZE = 500
ROWS_PER_INSERT = 200


def query_d1(sql: str, remote: bool = True) -> list[dict]:
    cmd = ["npx", "wrangler", "d1", "execute", DB_NAME, f"--command={sql}"]
    if remote:
        cmd.append("--remote")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60,
                            cwd=str(REPO_ROOT))
    try:
        data = json.loads(
            result.stdout[result.stdout.index("["):result.stdout.rindex("]") + 1]
        )
        return data[0].get("results", [])
    except (json.JSONDecodeError, ValueError, IndexError):
        return []


def escape_sql(val: str | None) -> str:
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"


def execute_sql_file(sql_path: str, remote: bool = True) -> bool:
    cmd = ["npx", "wrangler", "d1", "execute", DB_NAME, f"--file={sql_path}"]
    if remote:
        cmd.append("--remote")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                            cwd=str(REPO_ROOT))
    output = result.stdout + result.stderr
    if result.returncode != 0 and "success" not in output.lower():
        print(f"  ERROR: {output[:500]}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Import Opta-only players as new entities")
    parser.add_argument("--dry-run", action="store_true", help="Preview counts only")
    parser.add_argument("--local", action="store_true", help="Use local D1")
    parser.add_argument("--file", type=Path, default=OPTA_CSV, help="Opta CSV path")
    args = parser.parse_args()

    remote = not args.local
    csv_path = args.file

    if not csv_path.exists():
        print(f"ERROR: Opta CSV not found at {csv_path}")
        sys.exit(1)

    # --- Step 1: Load Opta CSV ---
    print(f"Loading Opta players from {csv_path}...")
    opta_players = {}
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            opta_id = row.get("PlayerID", "").strip()
            if opta_id:
                opta_players[opta_id] = row
    print(f"  {len(opta_players):,} players in CSV")

    # --- Step 2: Find already-matched Opta IDs ---
    print("\nChecking existing Opta mappings in custom_ids...")
    existing_opta_ids: set[str] = set()
    offset = 0
    while True:
        rows = query_d1(
            f"SELECT external_id FROM custom_ids WHERE provider = 'opta' "
            f"LIMIT 10000 OFFSET {offset};",
            remote=remote,
        )
        if not rows:
            break
        for r in rows:
            existing_opta_ids.add(r["external_id"])
        offset += 10000
    print(f"  {len(existing_opta_ids):,} Opta IDs already in custom_ids")

    # Also check provider_ids (Wikidata-sourced opta mappings)
    offset = 0
    while True:
        rows = query_d1(
            f"SELECT external_id FROM provider_ids WHERE provider = 'opta' "
            f"LIMIT 10000 OFFSET {offset};",
            remote=remote,
        )
        if not rows:
            break
        for r in rows:
            existing_opta_ids.add(r["external_id"])
        offset += 10000
    print(f"  {len(existing_opta_ids):,} total Opta IDs across both tables")

    # --- Step 3: Identify unmatched ---
    unmatched = {oid: row for oid, row in opta_players.items() if oid not in existing_opta_ids}
    print(f"\n  Unmatched Opta players: {len(unmatched):,}")

    if not unmatched:
        print("All Opta players already matched. Nothing to import.")
        return

    # Stats
    with_dob = sum(1 for r in unmatched.values() if r.get("DateOfBirth", "").strip())
    without_dob = len(unmatched) - with_dob
    print(f"  With DOB: {with_dob:,}")
    print(f"  Without DOB: {without_dob:,}")

    if args.dry_run:
        print(f"\n[dry-run] Would create {len(unmatched):,} new entities.")
        print("\nSample:")
        for oid, row in list(unmatched.items())[:10]:
            print(f"  {row.get('PlayerName', '?'):<30} DOB: {row.get('DateOfBirth', 'N/A'):<12} OID: {oid[:20]}...")
        return

    # --- Step 4: Mint reep_ids and create entities ---
    print(f"\nCreating {len(unmatched):,} new entities...")
    used_ids: set[str] = set()
    entity_stmts = []
    custom_stmts = []

    for opta_id, row in unmatched.items():
        # Mint reep_id
        for _ in range(10):
            reep_id = f"reep_p{uuid.uuid4().hex[:8]}"
            if reep_id not in used_ids:
                break
        used_ids.add(reep_id)

        name = row.get("PlayerName", "").strip() or "Unknown"
        dob = row.get("DateOfBirth", "").strip() or None

        # Entity INSERT
        entity_stmts.append(
            f"INSERT OR IGNORE INTO entities (qid, type, name_en, date_of_birth, reep_id) "
            f"VALUES ({escape_sql(reep_id)}, 'player', {escape_sql(name)}, {escape_sql(dob)}, {escape_sql(reep_id)});"
        )

        # custom_ids INSERT
        custom_stmts.append(
            f"INSERT OR IGNORE INTO custom_ids (reep_id, provider, external_id, source, confidence) "
            f"VALUES ({escape_sql(reep_id)}, 'opta', {escape_sql(opta_id)}, 'opta-f1-import', 1.0);"
        )

    print(f"  {len(entity_stmts):,} entity INSERTs")
    print(f"  {len(custom_stmts):,} custom_ids INSERTs")

    # Execute entity inserts
    print("\nInserting entities...")
    all_stmts = entity_stmts
    failed = 0
    for i in range(0, len(all_stmts), BATCH_SIZE):
        batch = all_stmts[i:i + BATCH_SIZE]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("\n".join(batch))
            sql_path = f.name
        try:
            ok = execute_sql_file(sql_path, remote=remote)
        finally:
            Path(sql_path).unlink(missing_ok=True)
        if not ok:
            failed += 1
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(all_stmts) + BATCH_SIZE - 1) // BATCH_SIZE
        if batch_num % 5 == 0 or batch_num == total_batches:
            print(f"  Batch {batch_num}/{total_batches}...")

    if failed > 0:
        print(f"  WARNING: {failed} batch(es) failed")

    # Execute custom_ids inserts
    print("\nInserting custom_ids...")
    failed = 0
    for i in range(0, len(custom_stmts), BATCH_SIZE):
        batch = custom_stmts[i:i + BATCH_SIZE]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("\n".join(batch))
            sql_path = f.name
        try:
            ok = execute_sql_file(sql_path, remote=remote)
        finally:
            Path(sql_path).unlink(missing_ok=True)
        if not ok:
            failed += 1

    if failed > 0:
        print(f"  WARNING: {failed} batch(es) failed")
        sys.exit(1)

    # --- Step 5: Verify ---
    print("\nVerifying...")
    rows = query_d1("SELECT COUNT(*) as cnt FROM entities WHERE reep_id LIKE 'reep_p%' AND qid = reep_id;", remote=remote)
    opta_entity_count = rows[0]["cnt"] if rows else 0
    print(f"  Opta-sourced entities (qid=reep_id): {opta_entity_count:,}")

    rows = query_d1("SELECT COUNT(*) as cnt FROM custom_ids WHERE source = 'opta-f1-import';", remote=remote)
    opta_custom_count = rows[0]["cnt"] if rows else 0
    print(f"  Opta import custom_ids: {opta_custom_count:,}")

    print(f"  Expected: ~{len(unmatched):,}")

    if opta_entity_count > 0:
        print(f"\nImported {opta_entity_count:,} Opta-only players as new entities.")
    else:
        print("\nWARNING: No entities imported!")

    # Sample
    print("\nSample new entities:")
    rows = query_d1(
        "SELECT e.reep_id, e.name_en, e.date_of_birth, c.external_id "
        "FROM entities e "
        "JOIN custom_ids c ON c.reep_id = e.reep_id AND c.source = 'opta-f1-import' "
        "LIMIT 5;",
        remote=remote,
    )
    for r in rows:
        print(f"  {r['reep_id']} — {r['name_en']} (DOB: {r.get('date_of_birth', 'N/A')}) — opta:{r['external_id'][:20]}...")

    print("\nDone.")


if __name__ == "__main__":
    main()
