"""
Resolve duplicate entities from the dedup report.

Merges Opta-only entities into their Wikidata counterparts:
  1. Move the Opta ID from the duplicate's custom_ids to the Wikidata entity
  2. Delete the duplicate entity
  3. Verify: no orphan custom_ids, correct entity count

Only processes score >= 0.90 matches. Uses data/dedup-report.json as input.

Usage:
  python scripts/resolve-dupes.py --dry-run    # preview merges
  python scripts/resolve-dupes.py              # execute on remote D1
  python scripts/resolve-dupes.py --local      # use local D1
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

DB_NAME = "football-entities"
REPO_ROOT = Path(__file__).parent.parent
REPORT_PATH = REPO_ROOT / "data" / "dedup-report.json"
BATCH_SIZE = 200
MIN_SCORE = 0.90


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
    parser = argparse.ArgumentParser(description="Resolve duplicate entities")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()
    remote = not args.local

    if not REPORT_PATH.exists():
        print(f"ERROR: Dedup report not found at {REPORT_PATH}")
        print("Run dedup-check.py first.")
        sys.exit(1)

    with open(REPORT_PATH) as f:
        dupes = json.load(f)

    # Filter to high-confidence matches only
    dupes = [d for d in dupes if d["score"] >= MIN_SCORE]
    print(f"Loaded {len(dupes)} duplicates (score >= {MIN_SCORE})")

    if not dupes:
        print("No duplicates to resolve.")
        return

    # --- Preview ---
    print(f"\nMerge plan: for each duplicate pair:")
    print(f"  1. UPDATE custom_ids: repoint Opta ID from opta_reep_id → wd_reep_id")
    print(f"  2. DELETE opta entity from entities table")
    print(f"  3. DELETE any remaining custom_ids for the opta entity")

    # Snapshot counts
    ent_rows = query_d1("SELECT COUNT(*) as cnt FROM entities;", remote=remote)
    cid_rows = query_d1("SELECT COUNT(*) as cnt FROM custom_ids;", remote=remote)
    pre_ent = ent_rows[0]["cnt"] if ent_rows else 0
    pre_cid = cid_rows[0]["cnt"] if cid_rows else 0
    print(f"\nPre-merge counts:")
    print(f"  entities: {pre_ent:,}")
    print(f"  custom_ids: {pre_cid:,}")
    print(f"  Entities to remove: {len(dupes)}")
    print(f"  Expected post-merge entities: {pre_ent - len(dupes):,}")

    if args.dry_run:
        print(f"\n[dry-run] Would resolve {len(dupes)} duplicates.")
        print("\nSample merges:")
        for d in dupes[:10]:
            print(f"  {d['opta_name']:<25} {d['opta_reep_id']} → {d['wd_reep_id']} ({d['wd_qid']})")
        return

    # --- Execute ---
    stmts = []
    for d in dupes:
        opta_rid = d["opta_reep_id"]
        wd_rid = d["wd_reep_id"]

        # Repoint custom_ids from opta entity to wikidata entity
        stmts.append(
            f"UPDATE OR IGNORE custom_ids SET reep_id = {escape_sql(wd_rid)} "
            f"WHERE reep_id = {escape_sql(opta_rid)};"
        )
        # Delete any custom_ids that couldn't be repointed (conflict on PK)
        stmts.append(
            f"DELETE FROM custom_ids WHERE reep_id = {escape_sql(opta_rid)};"
        )
        # Delete the duplicate entity
        stmts.append(
            f"DELETE FROM entities WHERE reep_id = {escape_sql(opta_rid)};"
        )

    print(f"\nExecuting {len(stmts):,} statements...")
    failed = 0
    for i in range(0, len(stmts), BATCH_SIZE):
        batch = stmts[i:i + BATCH_SIZE]
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
        total = (len(stmts) + BATCH_SIZE - 1) // BATCH_SIZE
        if batch_num % 3 == 0 or batch_num == total:
            print(f"  Batch {batch_num}/{total}...")

    if failed > 0:
        print(f"\n  WARNING: {failed} batch(es) failed")

    # --- Verify ---
    print("\nVerifying...")
    post_ent = query_d1("SELECT COUNT(*) as cnt FROM entities;", remote=remote)
    post_cid = query_d1("SELECT COUNT(*) as cnt FROM custom_ids;", remote=remote)
    post_ent_count = post_ent[0]["cnt"] if post_ent else -1
    post_cid_count = post_cid[0]["cnt"] if post_cid else -1

    print(f"  entities: {pre_ent:,} → {post_ent_count:,} (removed {pre_ent - post_ent_count:,})")
    print(f"  custom_ids: {pre_cid:,} → {post_cid_count:,}")

    expected_ent = pre_ent - len(dupes)
    if post_ent_count == expected_ent:
        print(f"\n  Entity count matches expected ({expected_ent:,})")
    else:
        print(f"\n  WARNING: Expected {expected_ent:,} entities, got {post_ent_count:,}")

    # Verify no orphan custom_ids pointing to deleted entities
    orphans = query_d1(
        "SELECT COUNT(*) as cnt FROM custom_ids c "
        "LEFT JOIN entities e ON c.reep_id = e.reep_id "
        "WHERE e.reep_id IS NULL;",
        remote=remote,
    )
    orphan_count = orphans[0]["cnt"] if orphans else -1
    if orphan_count == 0:
        print(f"  No orphan custom_ids — clean")
    else:
        print(f"  WARNING: {orphan_count:,} orphan custom_ids rows")

    # Spot check: Gareth Bale should now have opta ID on his Wikidata entity
    bale = query_d1(
        "SELECT c.provider, c.external_id FROM custom_ids c "
        "JOIN entities e ON c.reep_id = e.reep_id "
        "WHERE e.qid = 'Q184586' AND c.provider = 'opta';",
        remote=remote,
    )
    if bale:
        print(f"\n  Spot check: Gareth Bale (Q184586) — opta: {bale[0]['external_id'][:20]}...")
    else:
        print(f"\n  Spot check: Gareth Bale opta ID not found (may not have been in dupes)")

    print("\nDone.")


if __name__ == "__main__":
    main()
