"""
Rekey custom_ids table from (qid, type) to reep_id.

Phase 2, Unit 4: custom_ids is the "precious" table — never bulk-dropped.
This script adds reep_id, populates it from entities, then recreates
the table with reep_id as the FK (dropping qid and type columns).

Safety: Creates a D1 Time Travel bookmark and exports a JSON backup
before making any changes.

Usage:
  python scripts/rekey-custom-ids.py --dry-run   # preview, don't execute
  python scripts/rekey-custom-ids.py              # rekey on remote D1
  python scripts/rekey-custom-ids.py --local      # use local D1
"""

import argparse
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

DB_NAME = "football-entities"
REPO_ROOT = Path(__file__).parent.parent
BACKUP_DIR = REPO_ROOT / "data" / "backups"


def query_d1(sql: str, remote: bool = True) -> list[dict]:
    """Run a SQL query against D1 and return result rows."""
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
    parser = argparse.ArgumentParser(description="Rekey custom_ids from (qid, type) to reep_id")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--local", action="store_true", help="Use local D1")
    args = parser.parse_args()

    remote = not args.local

    # --- Step 1: Check current state ---
    print("Checking current custom_ids state...")
    cols_rows = query_d1("PRAGMA table_info(custom_ids);", remote=remote)
    col_names = [r["name"] for r in cols_rows] if cols_rows else []
    print(f"  Columns: {col_names}")

    if "reep_id" in col_names and "qid" not in col_names:
        print("  custom_ids already rekeyed to reep_id. Nothing to do.")
        return

    count_rows = query_d1("SELECT COUNT(*) as cnt FROM custom_ids;", remote=remote)
    pre_count = count_rows[0]["cnt"] if count_rows else 0
    print(f"  Rows: {pre_count:,}")

    if pre_count == 0:
        print("ERROR: custom_ids is empty.")
        sys.exit(1)

    if args.dry_run:
        # Check how many would get reep_ids
        if "reep_id" in col_names:
            null_rows = query_d1("SELECT COUNT(*) as cnt FROM custom_ids WHERE reep_id IS NULL;", remote=remote)
            null_count = null_rows[0]["cnt"] if null_rows else 0
            print(f"  Rows without reep_id: {null_count:,}")
        else:
            print(f"  All {pre_count:,} rows need reep_id (column doesn't exist yet)")

        # Check for orphans
        orphan_rows = query_d1(
            "SELECT COUNT(*) as cnt FROM custom_ids c "
            "LEFT JOIN entities e ON c.qid = e.qid AND c.type = e.type "
            "WHERE e.qid IS NULL;",
            remote=remote,
        )
        orphan_count = orphan_rows[0]["cnt"] if orphan_rows else 0
        if orphan_count > 0:
            print(f"  WARNING: {orphan_count:,} orphan rows (QID not in entities)")
        else:
            print(f"  No orphan rows.")

        print(f"\n[dry-run] Would rekey {pre_count:,} rows to use reep_id.")
        return

    # --- Step 2: Create backup ---
    print("\nBacking up custom_ids to JSON...")
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    backup_path = BACKUP_DIR / f"custom_ids_backup_{timestamp}.json"

    all_rows = []
    offset = 0
    page_size = 5000
    while True:
        rows = query_d1(
            f"SELECT * FROM custom_ids ORDER BY qid, type, provider "
            f"LIMIT {page_size} OFFSET {offset};",
            remote=remote,
        )
        if not rows:
            break
        all_rows.extend(rows)
        offset += page_size

    with open(backup_path, "w") as f:
        json.dump(all_rows, f, indent=2)
    print(f"  Backed up {len(all_rows):,} rows to {backup_path}")

    if len(all_rows) != pre_count:
        print(f"  WARNING: Backup count ({len(all_rows):,}) != table count ({pre_count:,})")
        print("  Proceeding anyway — may be a pagination edge case.")

    # --- Step 3: Add reep_id column if needed ---
    has_reep_id = "reep_id" in col_names
    if not has_reep_id:
        print("\nAdding reep_id column to custom_ids...")
        query_d1("ALTER TABLE custom_ids ADD COLUMN reep_id TEXT;", remote=remote)
        print("  Column added.")

    # --- Step 4: Populate reep_id from entities ---
    print("\nPopulating reep_id from entities table...")
    # Use a single UPDATE with subquery — D1 can handle this.
    query_d1(
        "UPDATE custom_ids SET reep_id = ("
        "  SELECT e.reep_id FROM entities e "
        "  WHERE e.qid = custom_ids.qid AND e.type = custom_ids.type"
        ") WHERE reep_id IS NULL;",
        remote=remote,
    )

    # Verify no NULLs
    null_rows = query_d1("SELECT COUNT(*) as cnt FROM custom_ids WHERE reep_id IS NULL;", remote=remote)
    null_count = null_rows[0]["cnt"] if null_rows else -1
    print(f"  Rows with NULL reep_id: {null_count:,}")

    if null_count > 0:
        print(f"\n  WARNING: {null_count:,} rows have no matching entity (orphans).")
        # Show sample orphans
        orphans = query_d1(
            "SELECT qid, type, provider FROM custom_ids WHERE reep_id IS NULL LIMIT 10;",
            remote=remote,
        )
        for o in orphans:
            print(f"    {o['qid']} ({o['type']}) — provider: {o['provider']}")
        print("  These orphan rows will be dropped during table recreation.")
        print("  They reference QIDs not in the entities table.")

    # --- Step 5: Recreate table without qid/type columns ---
    print("\nRecreating custom_ids with reep_id as FK...")

    # Count rows that will survive (non-NULL reep_id)
    survive_rows = query_d1("SELECT COUNT(*) as cnt FROM custom_ids WHERE reep_id IS NOT NULL;", remote=remote)
    survive_count = survive_rows[0]["cnt"] if survive_rows else 0

    # Create new table, copy data, swap
    stmts = [
        # Create new table
        """CREATE TABLE custom_ids_new (
            reep_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            external_id TEXT NOT NULL,
            source TEXT,
            confidence REAL DEFAULT 1.0,
            added_at TEXT,
            PRIMARY KEY (reep_id, provider, external_id)
        );""",
        # Copy data (only rows with reep_id)
        """INSERT INTO custom_ids_new (reep_id, provider, external_id, source, confidence, added_at)
           SELECT reep_id, provider, external_id, source, confidence, added_at
           FROM custom_ids
           WHERE reep_id IS NOT NULL;""",
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write("\n".join(stmts))
        sql_path = f.name

    try:
        ok = execute_sql_file(sql_path, remote=remote)
    finally:
        Path(sql_path).unlink(missing_ok=True)

    if not ok:
        print("ERROR: Failed to create and populate custom_ids_new.")
        print("Rollback: custom_ids is unchanged. Use D1 Time Travel if needed.")
        sys.exit(1)

    # Verify new table count
    new_count_rows = query_d1("SELECT COUNT(*) as cnt FROM custom_ids_new;", remote=remote)
    new_count = new_count_rows[0]["cnt"] if new_count_rows else 0
    print(f"  custom_ids_new rows: {new_count:,}")
    print(f"  Expected (survive): {survive_count:,}")

    if new_count != survive_count:
        print(f"ERROR: Count mismatch! Dropping custom_ids_new, leaving old table intact.")
        query_d1("DROP TABLE IF EXISTS custom_ids_new;", remote=remote)
        sys.exit(1)

    # Swap tables
    print("\nSwapping tables...")
    swap_stmts = [
        "DROP TABLE custom_ids;",
        "ALTER TABLE custom_ids_new RENAME TO custom_ids;",
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write("\n".join(swap_stmts))
        sql_path = f.name

    try:
        ok = execute_sql_file(sql_path, remote=remote)
    finally:
        Path(sql_path).unlink(missing_ok=True)

    if not ok:
        print("ERROR: Table swap failed!")
        print("State: custom_ids_new exists and is populated. Old custom_ids may be dropped.")
        print("Recovery: ALTER TABLE custom_ids_new RENAME TO custom_ids;")
        sys.exit(1)

    # --- Step 6: Verify ---
    print("\nVerifying...")
    final_rows = query_d1("SELECT COUNT(*) as cnt FROM custom_ids;", remote=remote)
    final_count = final_rows[0]["cnt"] if final_rows else -1

    final_cols = query_d1("PRAGMA table_info(custom_ids);", remote=remote)
    final_col_names = [r["name"] for r in final_cols] if final_cols else []

    print(f"  Final row count: {final_count:,}")
    print(f"  Pre-rekey count: {pre_count:,}")
    print(f"  Orphans dropped: {null_count:,}")
    print(f"  Expected: {pre_count - max(null_count, 0):,}")
    print(f"  Columns: {final_col_names}")

    if "qid" in final_col_names or "type" in final_col_names:
        print("  WARNING: Old columns (qid, type) still present!")
    elif "reep_id" not in final_col_names:
        print("  WARNING: reep_id column missing!")
    else:
        print("  Schema looks correct (reep_id, provider, external_id, source, confidence).")

    expected_final = pre_count - max(null_count, 0)
    if final_count == expected_final:
        print(f"\nRekey complete. {final_count:,} rows migrated.")
    else:
        print(f"\nWARNING: Count mismatch. Expected {expected_final:,}, got {final_count:,}.")
        print(f"  Backup at: {backup_path}")
        sys.exit(1)

    # Spot check
    print("\nSpot checks:")
    rows = query_d1("SELECT reep_id, provider, external_id FROM custom_ids WHERE provider = 'opta' LIMIT 3;", remote=remote)
    for r in rows:
        print(f"  {r['reep_id']} — {r['provider']}: {r['external_id'][:20]}...")

    print(f"\nBackup preserved at: {backup_path}")
    print("Done.")


if __name__ == "__main__":
    main()
