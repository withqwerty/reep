"""
Phase 4 cutover: make reep_id the primary key of entities.

Executes as a numbered script with checkpoint verification between steps.
On failure at any step, logs which step failed and exits — rollback via
D1 Time Travel.

Steps:
  1. Pre-flight checks (counts, FTS triggers, reep_id coverage)
  2. Drop FTS triggers
  3. Drop entities_fts table
  4. Create entities_new with reep_id PK, populate from old table
  5. Verify entities_new count
  6. Drop old entities, rename entities_new
  7. Drop external_ids (replaced by provider_ids)
  8. Rebuild FTS index
  9. Recreate FTS sync triggers
  10. Post-flight checks

Usage:
  python scripts/cutover-reep-id.py --dry-run              # preview plan
  python scripts/cutover-reep-id.py --database <DB_ID>     # run on specific D1
  python scripts/cutover-reep-id.py                        # run on football-entities (remote)
  python scripts/cutover-reep-id.py --local                # run on local D1
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_DB = "football-entities"
REPO_ROOT = Path(__file__).parent.parent


def query_d1(sql: str, db_name: str, remote: bool = True) -> list[dict]:
    cmd = ["npx", "wrangler", "d1", "execute", db_name, f"--command={sql}"]
    if remote:
        cmd.append("--remote")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                            cwd=str(REPO_ROOT))
    try:
        data = json.loads(
            result.stdout[result.stdout.index("["):result.stdout.rindex("]") + 1]
        )
        return data[0].get("results", [])
    except (json.JSONDecodeError, ValueError, IndexError):
        return []


def execute_sql_file(sql_path: str, db_name: str, remote: bool = True) -> bool:
    cmd = ["npx", "wrangler", "d1", "execute", db_name, f"--file={sql_path}"]
    if remote:
        cmd.append("--remote")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                            cwd=str(REPO_ROOT))
    output = result.stdout + result.stderr
    if result.returncode != 0 and "success" not in output.lower():
        print(f"    ERROR: {output[:500]}")
        return False
    return True


def run_sql(sql: str, db_name: str, remote: bool, step: str) -> bool:
    """Execute SQL via temp file with step label for logging."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql)
        sql_path = f.name
    try:
        ok = execute_sql_file(sql_path, db_name, remote)
    finally:
        Path(sql_path).unlink(missing_ok=True)
    if not ok:
        print(f"\n  FAILED at step: {step}")
        print(f"  Rollback: use D1 Time Travel to restore to pre-cutover state.")
        sys.exit(1)
    return True


def count(sql: str, db_name: str, remote: bool) -> int:
    rows = query_d1(sql, db_name, remote)
    return rows[0]["cnt"] if rows else -1


def main():
    parser = argparse.ArgumentParser(description="Phase 4: cutover to reep_id as PK")
    parser.add_argument("--database", default=DEFAULT_DB, help="D1 database name or ID")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    db = args.database
    remote = not args.local

    print(f"=== Phase 4 Cutover: reep_id as PK ===")
    print(f"Database: {db}")
    print(f"Remote: {remote}")
    print()

    # =========================================================================
    # Step 1: Pre-flight checks
    # =========================================================================
    print("Step 1: Pre-flight checks")

    ent_count = count("SELECT COUNT(*) as cnt FROM entities;", db, remote)
    print(f"  entities: {ent_count:,}")

    prov_count = count("SELECT COUNT(*) as cnt FROM provider_ids;", db, remote)
    print(f"  provider_ids: {prov_count:,}")

    cid_count = count("SELECT COUNT(*) as cnt FROM custom_ids;", db, remote)
    print(f"  custom_ids: {cid_count:,}")

    null_reep = count("SELECT COUNT(*) as cnt FROM entities WHERE reep_id IS NULL;", db, remote)
    print(f"  entities without reep_id: {null_reep:,}")
    if null_reep > 0:
        print(f"  ABORT: {null_reep:,} entities lack reep_ids. Run mint-reep-ids.py first.")
        sys.exit(1)

    team_qid_count = count(
        "SELECT COUNT(*) as cnt FROM entities WHERE current_team_qid IS NOT NULL;", db, remote)
    print(f"  entities with current_team_qid: {team_qid_count:,}")

    # Check FTS triggers exist
    triggers = query_d1(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'entities_fts%';",
        db, remote)
    trigger_names = [t["name"] for t in triggers]
    print(f"  FTS triggers: {trigger_names}")

    # Check provider_ids has data
    if prov_count <= 0:
        print(f"  ABORT: provider_ids is empty. Run create-provider-ids.py first.")
        sys.exit(1)

    print(f"\n  All pre-flight checks passed.")

    if args.dry_run:
        print(f"\n[dry-run] Would execute cutover:")
        print(f"  - Drop FTS triggers + table")
        print(f"  - Create entities_new (reep_id PK, no qid column)")
        print(f"  - Populate from entities ORDER BY reep_id")
        print(f"  - current_team_qid → current_team_reep_id via LEFT JOIN")
        print(f"  - Drop old entities, rename entities_new")
        print(f"  - Drop external_ids")
        print(f"  - Rebuild FTS + triggers")
        print(f"  - Expected: {ent_count:,} entities with reep_id PK")
        return

    # =========================================================================
    # Step 2: Drop FTS triggers
    # =========================================================================
    print("\nStep 2: Drop FTS triggers")
    run_sql("""
DROP TRIGGER IF EXISTS entities_fts_ai;
DROP TRIGGER IF EXISTS entities_fts_ad;
DROP TRIGGER IF EXISTS entities_fts_au;
""", db, remote, "drop FTS triggers")
    print("  OK")

    # =========================================================================
    # Step 3: Drop FTS table
    # =========================================================================
    print("\nStep 3: Drop entities_fts")
    run_sql("DROP TABLE IF EXISTS entities_fts;", db, remote, "drop FTS table")
    print("  OK")

    # =========================================================================
    # Step 4: Create entities_new and populate
    # =========================================================================
    print("\nStep 4: Create entities_new with reep_id PK")
    run_sql("""
CREATE TABLE entities_new (
  reep_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  name_en TEXT NOT NULL,
  aliases_en TEXT,
  name_native TEXT,
  full_name TEXT,
  date_of_birth TEXT,
  nationality TEXT,
  position TEXT,
  current_team_reep_id TEXT,
  height_cm REAL,
  country TEXT,
  founded TEXT,
  stadium TEXT,
  source TEXT NOT NULL DEFAULT 'wikidata',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);
""", db, remote, "create entities_new")
    print("  Table created")

    print("  Populating from entities...")
    run_sql("""
INSERT INTO entities_new (
  reep_id, type, name_en, aliases_en, name_native, full_name,
  date_of_birth, nationality, position, current_team_reep_id,
  height_cm, country, founded, stadium, source
)
SELECT
  e.reep_id,
  e.type,
  e.name_en,
  e.aliases_en,
  e.name_native,
  e.full_name,
  e.date_of_birth,
  e.nationality,
  e.position,
  team.reep_id,
  e.height_cm,
  e.country,
  e.founded,
  e.stadium,
  CASE WHEN e.qid LIKE 'Q%' THEN 'wikidata' ELSE 'opta' END
FROM entities e
LEFT JOIN entities team ON team.qid = e.current_team_qid AND team.type = 'team'
ORDER BY e.reep_id;
""", db, remote, "populate entities_new")
    print("  Populated")

    # =========================================================================
    # Step 5: Verify entities_new count
    # =========================================================================
    print("\nStep 5: Verify entities_new count")
    new_count = count("SELECT COUNT(*) as cnt FROM entities_new;", db, remote)
    print(f"  entities_new: {new_count:,}")
    print(f"  entities (old): {ent_count:,}")

    if new_count != ent_count:
        print(f"  ABORT: Count mismatch! {new_count:,} != {ent_count:,}")
        print(f"  Dropping entities_new, leaving old table intact.")
        query_d1("DROP TABLE IF EXISTS entities_new;", db, remote)
        sys.exit(1)
    print("  Counts match — OK")

    # =========================================================================
    # Step 6: Drop external_ids first (has FK to entities — must go before entities)
    # =========================================================================
    print("\nStep 6: Drop external_ids (has FK to entities, must drop first)")
    run_sql("DROP TABLE IF EXISTS external_ids;", db, remote, "drop external_ids")
    print("  OK")

    # =========================================================================
    # Step 7: Drop old entities, rename
    # =========================================================================
    print("\nStep 7: Drop old entities, rename entities_new")
    # Execute as separate commands — D1 can't batch DROP + RENAME reliably
    run_sql("DROP TABLE entities;", db, remote, "drop old entities")
    run_sql("ALTER TABLE entities_new RENAME TO entities;", db, remote, "rename entities_new")
    print("  OK")

    # =========================================================================
    # Step 8: Create indexes
    # =========================================================================
    print("\nStep 8: Create indexes")
    run_sql("""
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name_en);
CREATE INDEX IF NOT EXISTS idx_entities_current_team ON entities(current_team_reep_id);
""", db, remote, "create indexes")
    print("  OK")

    # =========================================================================
    # Step 9: Rebuild FTS
    # =========================================================================
    print("\nStep 9: Rebuild FTS index")
    run_sql("""
CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(
  name_en,
  aliases_en,
  content='entities',
  content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2'
);
""", db, remote, "create FTS table")

    run_sql(
        "INSERT INTO entities_fts(entities_fts) VALUES('rebuild');",
        db, remote, "rebuild FTS index"
    )
    print("  FTS rebuilt")

    # =========================================================================
    # Step 10: Recreate FTS triggers
    # =========================================================================
    print("\nStep 10: Recreate FTS sync triggers")
    run_sql("""
CREATE TRIGGER IF NOT EXISTS entities_fts_ai AFTER INSERT ON entities BEGIN
  INSERT INTO entities_fts(rowid, name_en, aliases_en)
  VALUES (new.rowid, new.name_en, new.aliases_en);
END;

CREATE TRIGGER IF NOT EXISTS entities_fts_ad AFTER DELETE ON entities BEGIN
  INSERT INTO entities_fts(entities_fts, rowid, name_en, aliases_en)
  VALUES ('delete', old.rowid, old.name_en, old.aliases_en);
END;

CREATE TRIGGER IF NOT EXISTS entities_fts_au AFTER UPDATE ON entities BEGIN
  INSERT INTO entities_fts(entities_fts, rowid, name_en, aliases_en)
  VALUES ('delete', old.rowid, old.name_en, old.aliases_en);
  INSERT INTO entities_fts(rowid, name_en, aliases_en)
  VALUES (new.rowid, new.name_en, new.aliases_en);
END;
""", db, remote, "create FTS triggers")
    print("  OK")

    # =========================================================================
    # Step 11: Post-flight checks
    # =========================================================================
    print("\n" + "=" * 60)
    print("Post-flight checks")
    print("=" * 60)

    post_ent = count("SELECT COUNT(*) as cnt FROM entities;", db, remote)
    print(f"  entities: {post_ent:,} (expected {ent_count:,})")

    post_prov = count("SELECT COUNT(*) as cnt FROM provider_ids;", db, remote)
    print(f"  provider_ids: {post_prov:,} (expected {prov_count:,})")

    post_cid = count("SELECT COUNT(*) as cnt FROM custom_ids;", db, remote)
    print(f"  custom_ids: {post_cid:,} (expected {cid_count:,})")

    # Check PK is reep_id
    schema = query_d1(
        "SELECT sql FROM sqlite_master WHERE name = 'entities' AND type = 'table';",
        db, remote)
    if schema:
        schema_sql = schema[0]["sql"]
        if "reep_id TEXT PRIMARY KEY" in schema_sql or "reep_id PRIMARY KEY" in schema_sql:
            print(f"  PK: reep_id — OK")
        else:
            print(f"  WARNING: PK may not be reep_id! Schema: {schema_sql[:100]}")
    else:
        print(f"  WARNING: Could not read schema")

    # Check no qid column
    cols = query_d1("PRAGMA table_info(entities);", db, remote)
    col_names = [c["name"] for c in cols] if cols else []
    if "qid" in col_names:
        print(f"  WARNING: qid column still present!")
    else:
        print(f"  qid column removed — OK")
    if "current_team_reep_id" in col_names:
        print(f"  current_team_reep_id present — OK")
    if "source" in col_names:
        print(f"  source column present — OK")

    # Check current_team_reep_id population
    post_team = count(
        "SELECT COUNT(*) as cnt FROM entities WHERE current_team_reep_id IS NOT NULL;",
        db, remote)
    print(f"  current_team_reep_id populated: {post_team:,} (was current_team_qid: {team_qid_count:,})")
    if post_team < team_qid_count:
        diff = team_qid_count - post_team
        print(f"  NOTE: {diff:,} fewer team refs — teams deleted from Wikidata or missing reep_id")

    # FTS spot check
    print(f"\n  FTS spot checks:")
    for name in ["Messi", "Cole Palmer", "Arsenal"]:
        fts_rows = query_d1(
            f"SELECT e.reep_id, e.name_en FROM entities_fts "
            f"JOIN entities e ON e.rowid = entities_fts.rowid "
            f"WHERE entities_fts MATCH '\"{name}\"*' LIMIT 1;",
            db, remote)
        if fts_rows:
            print(f"    '{name}' → {fts_rows[0]['reep_id']} ({fts_rows[0]['name_en']})")
        else:
            print(f"    '{name}' → NOT FOUND (FTS may need rebuild)")

    # Check external_ids is gone
    ext_check = query_d1("SELECT COUNT(*) as cnt FROM external_ids;", db, remote)
    if not ext_check:
        print(f"  external_ids dropped — OK")
    else:
        print(f"  WARNING: external_ids still exists with {ext_check[0]['cnt']:,} rows")

    # Verdict
    checks_passed = (
        post_ent == ent_count
        and post_prov == prov_count
        and post_cid == cid_count
        and "qid" not in col_names
    )

    print(f"\n{'=' * 60}")
    if checks_passed:
        print(f"CUTOVER COMPLETE — all checks passed")
    else:
        print(f"CUTOVER COMPLETE — some checks had warnings (review above)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
