"""
Clone production D1 data to a staging database for cutover rehearsal.

Exports entities, provider_ids, and custom_ids from production via
paginated SELECT queries, then imports into the staging D1.

Usage:
  python scripts/clone-to-staging.py --staging-db <DB_NAME_OR_ID>
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

PROD_DB = "football-entities"
REPO_ROOT = Path(__file__).parent.parent
ROWS_PER_INSERT = 200
STMTS_PER_FILE = 2000


def query_d1(sql: str, db: str, remote: bool = True) -> list[dict]:
    cmd = ["npx", "wrangler", "d1", "execute", db, f"--command={sql}"]
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


def escape_sql(val) -> str:
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"


def execute_sql_file(sql_path: str, db: str, remote: bool = True) -> bool:
    cmd = ["npx", "wrangler", "d1", "execute", db, f"--file={sql_path}"]
    if remote:
        cmd.append("--remote")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                            cwd=str(REPO_ROOT))
    output = result.stdout + result.stderr
    if result.returncode != 0 and "success" not in output.lower():
        print(f"  ERROR: {output[:300]}")
        return False
    return True


def clone_table(table: str, columns: list[str], prod_db: str, staging_db: str,
                create_sql: str, page_size: int = 5000):
    """Clone a table from production to staging."""
    print(f"\n  Cloning {table}...")

    # Create table in staging
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(create_sql)
        sql_path = f.name
    try:
        execute_sql_file(sql_path, staging_db)
    finally:
        Path(sql_path).unlink(missing_ok=True)

    # Fetch all rows from production
    col_str = ", ".join(columns)
    all_rows = []
    offset = 0
    while True:
        rows = query_d1(
            f"SELECT {col_str} FROM {table} LIMIT {page_size} OFFSET {offset};",
            prod_db, remote=True)
        if not rows:
            break
        all_rows.extend(rows)
        offset += page_size
        if offset % 50000 == 0:
            print(f"    Fetched {len(all_rows):,}...")

    print(f"    Fetched {len(all_rows):,} total rows")

    # Generate INSERT statements
    stmts = []
    for i in range(0, len(all_rows), ROWS_PER_INSERT):
        batch = all_rows[i:i + ROWS_PER_INSERT]
        values = []
        for r in batch:
            vals = ", ".join(escape_sql(r.get(c)) for c in columns)
            values.append(f"({vals})")
        stmts.append(
            f"INSERT OR IGNORE INTO {table} ({col_str}) VALUES\n"
            + ",\n".join(values) + ";"
        )

    # Execute in file batches
    failed = 0
    total_files = (len(stmts) + STMTS_PER_FILE - 1) // STMTS_PER_FILE
    for i in range(0, len(stmts), STMTS_PER_FILE):
        batch = stmts[i:i + STMTS_PER_FILE]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("\n".join(batch))
            sql_path = f.name
        try:
            ok = execute_sql_file(sql_path, staging_db)
        finally:
            Path(sql_path).unlink(missing_ok=True)
        if not ok:
            failed += 1
        file_num = i // STMTS_PER_FILE + 1
        if file_num % 5 == 0 or file_num == total_files:
            print(f"    File {file_num}/{total_files}...")

    if failed > 0:
        print(f"    WARNING: {failed} file(s) failed")
    else:
        print(f"    Inserted {len(all_rows):,} rows — OK")

    return len(all_rows)


def main():
    parser = argparse.ArgumentParser(description="Clone production D1 to staging")
    parser.add_argument("--staging-db", required=True, help="Staging D1 database name or ID")
    args = parser.parse_args()

    staging = args.staging_db
    print(f"Cloning {PROD_DB} → {staging}")

    # Clone entities (with all current columns including reep_id)
    ent_count = clone_table(
        "entities",
        ["qid", "type", "name_en", "aliases_en", "name_native", "full_name",
         "date_of_birth", "nationality", "position", "current_team_qid",
         "height_cm", "country", "founded", "stadium", "reep_id"],
        PROD_DB, staging,
        """CREATE TABLE IF NOT EXISTS entities (
            qid TEXT NOT NULL, type TEXT NOT NULL, name_en TEXT NOT NULL,
            aliases_en TEXT, name_native TEXT, full_name TEXT,
            date_of_birth TEXT, nationality TEXT, position TEXT,
            current_team_qid TEXT, height_cm REAL, country TEXT,
            founded TEXT, stadium TEXT, reep_id TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (qid, type)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_entities_reep_id ON entities(reep_id);"""
    )

    # Clone provider_ids
    prov_count = clone_table(
        "provider_ids",
        ["reep_id", "provider", "external_id"],
        PROD_DB, staging,
        """CREATE TABLE IF NOT EXISTS provider_ids (
            reep_id TEXT NOT NULL, provider TEXT NOT NULL, external_id TEXT NOT NULL,
            PRIMARY KEY (reep_id, provider, external_id)
        );
        CREATE INDEX IF NOT EXISTS idx_provider_ids_lookup ON provider_ids(provider, external_id);"""
    )

    # Clone custom_ids
    cid_count = clone_table(
        "custom_ids",
        ["reep_id", "provider", "external_id", "source", "confidence"],
        PROD_DB, staging,
        """CREATE TABLE IF NOT EXISTS custom_ids (
            reep_id TEXT NOT NULL, provider TEXT NOT NULL, external_id TEXT NOT NULL,
            source TEXT, confidence REAL DEFAULT 1.0,
            PRIMARY KEY (reep_id, provider, external_id)
        );"""
    )

    # Clone external_ids (needed for cutover to drop it)
    ext_count = clone_table(
        "external_ids",
        ["qid", "type", "provider", "external_id"],
        PROD_DB, staging,
        """CREATE TABLE IF NOT EXISTS external_ids (
            qid TEXT NOT NULL, type TEXT NOT NULL, provider TEXT NOT NULL,
            external_id TEXT NOT NULL,
            PRIMARY KEY (qid, type, provider)
        );"""
    )

    # Create FTS (same as production)
    print("\n  Creating FTS index on staging...")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write("""
CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(
  name_en, aliases_en, content='entities', content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2'
);
INSERT INTO entities_fts(entities_fts) VALUES('rebuild');

CREATE TRIGGER IF NOT EXISTS entities_fts_ai AFTER INSERT ON entities BEGIN
  INSERT INTO entities_fts(rowid, name_en, aliases_en) VALUES (new.rowid, new.name_en, new.aliases_en);
END;
CREATE TRIGGER IF NOT EXISTS entities_fts_ad AFTER DELETE ON entities BEGIN
  INSERT INTO entities_fts(entities_fts, rowid, name_en, aliases_en) VALUES ('delete', old.rowid, old.name_en, old.aliases_en);
END;
CREATE TRIGGER IF NOT EXISTS entities_fts_au AFTER UPDATE ON entities BEGIN
  INSERT INTO entities_fts(entities_fts, rowid, name_en, aliases_en) VALUES ('delete', old.rowid, old.name_en, old.aliases_en);
  INSERT INTO entities_fts(rowid, name_en, aliases_en) VALUES (new.rowid, new.name_en, new.aliases_en);
END;
""")
        sql_path = f.name
    try:
        execute_sql_file(sql_path, staging)
    finally:
        Path(sql_path).unlink(missing_ok=True)
    print("    FTS built — OK")

    print(f"\n{'='*60}")
    print(f"Clone complete:")
    print(f"  entities: {ent_count:,}")
    print(f"  provider_ids: {prov_count:,}")
    print(f"  custom_ids: {cid_count:,}")
    print(f"  external_ids: {ext_count:,}")
    print(f"\nStaging DB: {staging}")
    print(f"Run cutover: python scripts/cutover-reep-id.py --database {staging}")


if __name__ == "__main__":
    main()
