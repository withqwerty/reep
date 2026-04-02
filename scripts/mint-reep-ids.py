"""
Mint reep_id values for all entities that don't have one yet.

Adds the reep_id column (if missing), then generates random IDs
in the format reep_<type_prefix><8hex> for each entity where
reep_id IS NULL. Idempotent — re-running only processes unassigned entities.

Usage:
  python scripts/mint-reep-ids.py --dry-run       # preview SQL, don't execute
  python scripts/mint-reep-ids.py                  # mint IDs on remote D1
  python scripts/mint-reep-ids.py --local          # mint IDs on local D1
"""

import argparse
import json
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

DB_NAME = "football-entities"
BATCH_SIZE = 500  # UPDATE statements per SQL file

TYPE_PREFIXES = {
    "player": "p",
    "team": "t",
    "coach": "c",
}


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


def escape_sql(val) -> str:
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


def generate_reep_id(entity_type: str) -> str:
    """Generate a reep_id: reep_<type_prefix><8hex>."""
    prefix = TYPE_PREFIXES.get(entity_type)
    if not prefix:
        raise ValueError(f"Unknown entity type: {entity_type}")
    hex8 = uuid.uuid4().hex[:8]
    return f"reep_{prefix}{hex8}"


def main():
    parser = argparse.ArgumentParser(description="Mint reep_id values for entities")
    parser.add_argument("--dry-run", action="store_true", help="Generate SQL only")
    parser.add_argument("--local", action="store_true", help="Use local D1")
    args = parser.parse_args()

    remote = not args.local

    # --- Step 1: Ensure reep_id column exists ---
    print("Checking for reep_id column...")
    # ALTER TABLE ADD COLUMN is a no-op if column already exists in SQLite
    # but D1 will error, so we check first.
    rows = query_d1("PRAGMA table_info(entities);", remote=remote)
    has_reep_id = any(r["name"] == "reep_id" for r in rows)

    if not has_reep_id:
        print("  Adding reep_id column to entities table...")
        if not args.dry_run:
            query_d1("ALTER TABLE entities ADD COLUMN reep_id TEXT;", remote=remote)
            print("  Column added.")
        else:
            print("  [dry-run] Would add reep_id column.")
    else:
        print("  reep_id column already exists.")

    # --- Step 2: Count entities needing IDs ---
    # In dry-run mode the column may not exist yet, so query all entities instead.
    if has_reep_id:
        rows = query_d1("SELECT COUNT(*) as total FROM entities WHERE reep_id IS NULL;", remote=remote)
        null_count = rows[0]["total"] if rows else 0
    else:
        rows = query_d1("SELECT COUNT(*) as total FROM entities;", remote=remote)
        null_count = rows[0]["total"] if rows else 0

    rows = query_d1("SELECT COUNT(*) as total FROM entities;", remote=remote)
    total_count = rows[0]["total"] if rows else 0
    print(f"\n{null_count:,} of {total_count:,} entities need reep_ids.")

    if null_count == 0:
        print("All entities already have reep_ids. Nothing to do.")
        return

    # --- Step 3: Fetch entities without reep_id and generate IDs ---
    print("\nFetching entities without reep_id...")
    # Fetch in pages to avoid memory issues with 435K+ entities.
    page_size = 10_000
    offset = 0
    all_updates = []
    used_ids = set()

    where_clause = "WHERE reep_id IS NULL" if has_reep_id else ""
    while True:
        rows = query_d1(
            f"SELECT qid, type FROM entities {where_clause} "
            f"LIMIT {page_size} OFFSET {offset};",
            remote=remote,
        )
        if not rows:
            break

        for row in rows:
            # Generate unique ID, retry on collision with already-generated IDs
            for _ in range(10):
                reep_id = generate_reep_id(row["type"])
                if reep_id not in used_ids:
                    break
            else:
                print(f"  ERROR: Failed to generate unique ID after 10 attempts for {row['qid']}")
                sys.exit(1)

            used_ids.add(reep_id)
            all_updates.append((reep_id, row["qid"], row["type"]))

        offset += page_size
        print(f"  Fetched {offset:,} entities...")

    print(f"\nGenerated {len(all_updates):,} reep_ids.")

    # --- Step 4: Generate and execute UPDATE statements ---
    print("\nGenerating UPDATE statements...")
    stmts = []
    for reep_id, qid, etype in all_updates:
        stmts.append(
            f"UPDATE entities SET reep_id = {escape_sql(reep_id)} "
            f"WHERE qid = {escape_sql(qid)} AND type = {escape_sql(etype)} "
            f"AND reep_id IS NULL;"
        )

    if args.dry_run:
        print(f"\n[dry-run] Would execute {len(stmts):,} UPDATE statements.")
        print("\nSample statements:")
        for s in stmts[:10]:
            print(f"  {s}")

        # Show type distribution
        from collections import Counter
        types = Counter(t for _, _, t in all_updates)
        print(f"\nType distribution:")
        for t, c in types.most_common():
            prefix = TYPE_PREFIXES[t]
            print(f"  {t}: {c:,} (reep_{prefix}*)")
        return

    print(f"\nExecuting {len(stmts):,} UPDATE statements in batches of {BATCH_SIZE}...")
    failed_batches = 0
    for i in range(0, len(stmts), BATCH_SIZE):
        batch = stmts[i:i + BATCH_SIZE]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("\n".join(batch))
            sql_path = f.name

        ok = execute_sql_file(sql_path, remote=remote)
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(stmts) + BATCH_SIZE - 1) // BATCH_SIZE
        status = "OK" if ok else "FAILED"
        if not ok:
            failed_batches += 1
        print(f"  Batch {batch_num}/{total_batches}: {len(batch)} statements — {status}")

    if failed_batches > 0:
        print(f"\nWARNING: {failed_batches} batch(es) failed. Re-run to retry (idempotent).")
        sys.exit(1)

    # --- Step 5: Create unique index ---
    print("\nCreating unique index on reep_id...")
    query_d1("CREATE UNIQUE INDEX IF NOT EXISTS idx_entities_reep_id ON entities(reep_id);", remote=remote)

    # --- Step 6: Verify ---
    print("\nVerifying...")
    rows = query_d1("SELECT COUNT(*) as total FROM entities WHERE reep_id IS NULL;", remote=remote)
    remaining = rows[0]["total"] if rows else -1

    rows = query_d1("SELECT COUNT(DISTINCT reep_id) as unique_ids FROM entities;", remote=remote)
    unique_count = rows[0]["unique_ids"] if rows else -1

    rows = query_d1("SELECT COUNT(*) as total FROM entities;", remote=remote)
    total = rows[0]["total"] if rows else -1

    print(f"  Entities without reep_id: {remaining:,}")
    print(f"  Unique reep_ids: {unique_count:,}")
    print(f"  Total entities: {total:,}")

    if remaining == 0 and unique_count == total:
        print("\nAll entities have unique reep_ids.")
    else:
        print("\nWARNING: Verification failed!")
        if remaining > 0:
            print(f"  {remaining:,} entities still without reep_id")
        if unique_count != total:
            print(f"  Unique count ({unique_count:,}) != total ({total:,}) — possible collisions!")
        sys.exit(1)

    # Spot-check: show a few examples
    print("\nSample reep_ids:")
    for etype in ["player", "team", "coach"]:
        rows = query_d1(
            f"SELECT reep_id, qid, name_en FROM entities WHERE type = {escape_sql(etype)} LIMIT 3;",
            remote=remote,
        )
        for r in rows:
            print(f"  {r['reep_id']} — {r['name_en']} ({r['qid']})")

    print("\nDone.")


if __name__ == "__main__":
    main()
