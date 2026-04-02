"""
Create the provider_ids table and populate from external_ids + wikidata QIDs.

Phase 2, Unit 3: provider_ids replaces external_ids with reep_id as FK.
Both tables coexist during the dual-write period. provider_ids has the
same lifecycle as external_ids (dropped and recreated on weekly refresh).

Usage:
  python scripts/create-provider-ids.py --dry-run   # preview counts, don't execute
  python scripts/create-provider-ids.py              # create and populate on remote D1
  python scripts/create-provider-ids.py --local      # use local D1
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

DB_NAME = "football-entities"
ROWS_PER_INSERT = 200
STMTS_PER_FILE = 5000


def query_d1(sql: str, remote: bool = True) -> list[dict]:
    """Run a SQL query against D1 and return result rows."""
    cmd = ["npx", "wrangler", "d1", "execute", DB_NAME, f"--command={sql}"]
    if remote:
        cmd.append("--remote")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
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
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    output = result.stdout + result.stderr
    if result.returncode != 0 and "success" not in output.lower():
        print(f"  ERROR: {output[:500]}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description="Create provider_ids table from external_ids")
    parser.add_argument("--dry-run", action="store_true", help="Preview counts only")
    parser.add_argument("--local", action="store_true", help="Use local D1")
    args = parser.parse_args()

    remote = not args.local

    # --- Step 1: Check current state ---
    print("Checking current state...")
    ext_rows = query_d1("SELECT COUNT(*) as cnt FROM external_ids;", remote=remote)
    ext_count = ext_rows[0]["cnt"] if ext_rows else 0

    ent_rows = query_d1("SELECT COUNT(*) as cnt FROM entities WHERE reep_id IS NOT NULL;", remote=remote)
    ent_count = ent_rows[0]["cnt"] if ent_rows else 0

    expected_count = ext_count + ent_count
    print(f"  external_ids rows: {ext_count:,}")
    print(f"  entities with reep_id (wikidata rows): {ent_count:,}")
    print(f"  Expected provider_ids total: {expected_count:,}")

    if ext_count == 0:
        print("ERROR: external_ids is empty. Nothing to populate from.")
        sys.exit(1)

    if ent_count == 0:
        print("ERROR: No entities have reep_ids. Run mint-reep-ids.py first.")
        sys.exit(1)

    # Check if provider_ids already exists
    existing = query_d1("SELECT COUNT(*) as cnt FROM provider_ids;", remote=remote)
    if existing:
        existing_count = existing[0]["cnt"]
        print(f"\n  provider_ids already exists with {existing_count:,} rows.")
        if existing_count > 0:
            print("  Table will be dropped and recreated.")

    if args.dry_run:
        print(f"\n[dry-run] Would create provider_ids with ~{expected_count:,} rows.")
        return

    # --- Step 2: Drop and create provider_ids ---
    print("\nCreating provider_ids table...")
    query_d1("DROP TABLE IF EXISTS provider_ids;", remote=remote)
    query_d1("""
        CREATE TABLE IF NOT EXISTS provider_ids (
            reep_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            external_id TEXT NOT NULL,
            PRIMARY KEY (reep_id, provider, external_id)
        );
    """, remote=remote)

    # --- Step 3: Populate from external_ids ---
    # Fetch external_ids in pages and write INSERT statements.
    # Can't use INSERT...SELECT across tables in a single D1 command because
    # the JOIN + INSERT would be too large for a single statement. Page through instead.
    print("\nPopulating from external_ids (via JOIN on entities for reep_id)...")
    page_size = 10_000
    offset = 0
    all_rows = []

    while True:
        rows = query_d1(
            f"SELECT e.reep_id, ei.provider, ei.external_id "
            f"FROM external_ids ei "
            f"JOIN entities e ON ei.qid = e.qid AND ei.type = e.type "
            f"LIMIT {page_size} OFFSET {offset};",
            remote=remote,
        )
        if not rows:
            break
        all_rows.extend(rows)
        offset += page_size
        print(f"  Fetched {len(all_rows):,} rows...")

    print(f"\nFetched {len(all_rows):,} external_id mappings.")

    # --- Step 4: Add wikidata QID mappings ---
    print("Adding wikidata QID mappings...")
    offset = 0
    wikidata_rows = []
    while True:
        rows = query_d1(
            f"SELECT reep_id, 'wikidata' as provider, qid as external_id "
            f"FROM entities WHERE reep_id IS NOT NULL "
            f"LIMIT {page_size} OFFSET {offset};",
            remote=remote,
        )
        if not rows:
            break
        wikidata_rows.extend(rows)
        offset += page_size
        print(f"  Fetched {len(wikidata_rows):,} wikidata mappings...")

    all_rows.extend(wikidata_rows)
    print(f"\nTotal rows to insert: {len(all_rows):,}")

    # --- Step 5: Generate and execute INSERT statements ---
    print("\nGenerating INSERT statements...")
    stmts = []
    for i in range(0, len(all_rows), ROWS_PER_INSERT):
        batch = all_rows[i:i + ROWS_PER_INSERT]
        values = []
        for r in batch:
            values.append(
                f"({escape_sql(r['reep_id'])}, {escape_sql(r['provider'])}, {escape_sql(r['external_id'])})"
            )
        stmts.append(
            "INSERT OR IGNORE INTO provider_ids (reep_id, provider, external_id) VALUES\n"
            + ",\n".join(values) + ";"
        )

    print(f"  {len(stmts):,} INSERT statements")
    print(f"\nExecuting in batches of {STMTS_PER_FILE} statements per file...")
    failed = 0
    total_files = (len(stmts) + STMTS_PER_FILE - 1) // STMTS_PER_FILE

    for i in range(0, len(stmts), STMTS_PER_FILE):
        batch = stmts[i:i + STMTS_PER_FILE]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("\n".join(batch))
            sql_path = f.name

        try:
            ok = execute_sql_file(sql_path, remote=remote)
        finally:
            Path(sql_path).unlink(missing_ok=True)

        file_num = i // STMTS_PER_FILE + 1
        status = "OK" if ok else "FAILED"
        if not ok:
            failed += 1
        print(f"  File {file_num}/{total_files}: {len(batch)} statements — {status}")

    if failed > 0:
        print(f"\nWARNING: {failed} file(s) failed.")
        sys.exit(1)

    # --- Step 6: Create lookup index ---
    print("\nCreating lookup index...")
    query_d1(
        "CREATE INDEX IF NOT EXISTS idx_provider_ids_lookup ON provider_ids(provider, external_id);",
        remote=remote,
    )

    # --- Step 7: Verify ---
    print("\nVerifying...")
    rows = query_d1("SELECT COUNT(*) as cnt FROM provider_ids;", remote=remote)
    actual_count = rows[0]["cnt"] if rows else -1
    print(f"  provider_ids rows: {actual_count:,}")
    print(f"  Expected (external_ids + wikidata): {expected_count:,}")

    if actual_count == expected_count:
        print("\nCounts match.")
    elif actual_count > 0 and abs(actual_count - expected_count) / expected_count < 0.01:
        print(f"\nCounts within 1% tolerance (diff: {actual_count - expected_count:+,}).")
        print("Minor differences may be due to entities without external_ids or duplicate mappings.")
    else:
        print(f"\nWARNING: Count mismatch! Expected {expected_count:,}, got {actual_count:,}")
        sys.exit(1)

    # Spot checks
    print("\nSpot checks:")
    # Messi
    rows = query_d1(
        "SELECT provider, external_id FROM provider_ids WHERE reep_id = "
        "(SELECT reep_id FROM entities WHERE qid = 'Q615' AND type = 'player') "
        "ORDER BY provider LIMIT 5;",
        remote=remote,
    )
    for r in rows:
        print(f"  Messi: {r['provider']} = {r['external_id']}")

    # Check wikidata mapping exists
    rows = query_d1(
        "SELECT reep_id FROM provider_ids WHERE provider = 'wikidata' AND external_id = 'Q615';",
        remote=remote,
    )
    if rows:
        print(f"  Wikidata Q615 → {rows[0]['reep_id']}")
    else:
        print("  WARNING: Wikidata Q615 mapping not found!")

    print("\nDone.")


if __name__ == "__main__":
    main()
