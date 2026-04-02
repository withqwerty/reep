"""
Seed Wikidata entity JSON files into Cloudflare D1 via wrangler CLI.

Uses bulk INSERT ... VALUES syntax and large SQL files for efficient import.
Wrangler handles the upload-to-R2 + import flow internally.

Usage:
  python scripts/seed-wikidata-d1.py                # seed all types
  python scripts/seed-wikidata-d1.py --type player   # single type
  python scripts/seed-wikidata-d1.py --dry-run       # generate SQL only
  python scripts/seed-wikidata-d1.py --local          # seed local D1
"""

import argparse
import json
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "json"
DB_NAME = "football-entities"
ROWS_PER_INSERT = 200  # rows per INSERT statement (avoids "statement too long")
STMTS_PER_FILE = 5000  # statements per SQL file


SAFETY_THRESHOLD = 0.9  # refuse to seed if new count < 90% of current


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


def generate_entity_inserts(entities: list[dict]) -> list[str]:
    """Generate bulk INSERT statements for entities table."""
    stmts = []
    cols = ("qid", "type", "name_en", "aliases_en", "full_name",
            "date_of_birth", "nationality", "position", "current_team_qid",
            "height_cm", "country", "founded", "stadium")
    col_str = ", ".join(cols)

    for i in range(0, len(entities), ROWS_PER_INSERT):
        batch = entities[i : i + ROWS_PER_INSERT]
        rows = []
        for e in batch:
            vals = (
                escape_sql(e["qid"]),
                escape_sql(e["type"]),
                escape_sql(e["name_en"]),
                escape_sql(e.get("aliases_en")),
                escape_sql(e.get("full_name")),
                escape_sql(e.get("date_of_birth")),
                escape_sql(e.get("nationality")),
                escape_sql(e.get("position")),
                escape_sql(e.get("current_team_qid")),
                str(e.get("height_cm")) if e.get("height_cm") is not None else "NULL",
                escape_sql(e.get("country")),
                escape_sql(e.get("founded")),
                escape_sql(e.get("stadium")),
            )
            rows.append(f"({', '.join(vals)})")
        stmts.append(f"INSERT OR REPLACE INTO entities ({col_str}) VALUES\n" + ",\n".join(rows) + ";")

    return stmts


def generate_id_inserts(entities: list[dict]) -> list[str]:
    """Generate bulk INSERT statements for external_ids table."""
    stmts = []
    all_id_rows = []

    for e in entities:
        for provider, ext_id in e.get("external_ids", {}).items():
            all_id_rows.append((e["qid"], e["type"], provider, ext_id))

    for i in range(0, len(all_id_rows), ROWS_PER_INSERT):
        batch = all_id_rows[i : i + ROWS_PER_INSERT]
        rows = []
        for qid, etype, provider, ext_id in batch:
            rows.append(f"({escape_sql(qid)}, {escape_sql(etype)}, {escape_sql(provider)}, {escape_sql(ext_id)})")
        stmts.append(
            "INSERT OR REPLACE INTO external_ids (qid, type, provider, external_id) VALUES\n"
            + ",\n".join(rows) + ";"
        )

    return stmts


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
    parser = argparse.ArgumentParser(description="Seed Wikidata entities into D1")
    parser.add_argument("--type", choices=["player", "team", "coach"], help="Single entity type")
    parser.add_argument("--dry-run", action="store_true", help="Generate SQL only")
    parser.add_argument("--local", action="store_true", help="Use local D1")
    parser.add_argument("--force", action="store_true", help="Skip safety count check")
    args = parser.parse_args()

    types = ["player", "team", "coach"]
    if args.type:
        types = [args.type]

    # --- Pre-seed safety gate ---
    # Count new entities from JSON, compare to current D1 count.
    # Refuse to proceed if the new count is significantly lower (truncated fetch).
    new_count = 0
    for entity_type in types:
        filename = f"{entity_type}s.json" if entity_type != "coach" else "coachs.json"
        json_path = DATA_DIR / filename
        if json_path.exists():
            with open(json_path) as f:
                new_count += len(json.load(f))

    if not args.dry_run and not args.local and not args.force:
        rows = query_d1("SELECT COUNT(*) as total FROM entities;")
        current_count = rows[0]["total"] if rows else 0

        if current_count > 0 and new_count > 0:
            ratio = new_count / current_count
            print(f"Safety check: {new_count:,} new entities vs {current_count:,} in D1 ({ratio:.0%})")
            if ratio < SAFETY_THRESHOLD:
                print(f"ABORTED — new count is {ratio:.0%} of current ({SAFETY_THRESHOLD:.0%} minimum).")
                print("This likely means the Wikidata fetch was incomplete.")
                print("Use --force to override, or fix the fetch and retry.")
                sys.exit(1)
        elif current_count > 0 and new_count == 0:
            print("ABORTED — no new entities found in JSON files.")
            sys.exit(1)

    # Drop and recreate tables with current schema.
    # Safe because the seed is a full refresh. FTS table must also be recreated
    # since the entities table structure (PK) changes.
    if not args.dry_run:
        print("Recreating tables with current schema...", end=" ", flush=True)
        schema_sql = """
DROP TRIGGER IF EXISTS entities_fts_ai;
DROP TRIGGER IF EXISTS entities_fts_ad;
DROP TRIGGER IF EXISTS entities_fts_au;
DROP TABLE IF EXISTS entities_fts;
DROP TABLE IF EXISTS external_ids;
DROP TABLE IF EXISTS entities;

CREATE TABLE IF NOT EXISTS entities (
  qid TEXT NOT NULL,
  type TEXT NOT NULL,
  name_en TEXT NOT NULL,
  aliases_en TEXT,
  name_native TEXT,
  full_name TEXT,
  date_of_birth TEXT,
  nationality TEXT,
  position TEXT,
  current_team_qid TEXT,
  height_cm REAL,
  country TEXT,
  founded TEXT,
  stadium TEXT,
  reep_id TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (qid, type)
);

CREATE TABLE IF NOT EXISTS external_ids (
  qid TEXT NOT NULL,
  type TEXT NOT NULL,
  provider TEXT NOT NULL,
  external_id TEXT NOT NULL,
  PRIMARY KEY (qid, type, provider),
  FOREIGN KEY (qid, type) REFERENCES entities(qid, type)
);

CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name_en);
CREATE INDEX IF NOT EXISTS idx_entities_current_team ON entities(current_team_qid);
CREATE UNIQUE INDEX IF NOT EXISTS idx_entities_reep_id ON entities(reep_id);
CREATE INDEX IF NOT EXISTS idx_external_ids_provider ON external_ids(provider, external_id);

CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(
  name_en,
  aliases_en,
  content='entities',
  content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2'
);
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(schema_sql)
            tmp_path = f.name
        try:
            if execute_sql_file(tmp_path, remote=not args.local):
                print("OK")
            else:
                print("FAILED — cannot proceed without correct schema")
                sys.exit(1)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    for entity_type in types:
        filename = f"{entity_type}s.json" if entity_type != "coach" else "coachs.json"
        json_path = DATA_DIR / filename
        if not json_path.exists():
            print(f"Skipping {entity_type}: {json_path} not found")
            continue

        with open(json_path) as f:
            entities = json.load(f)

        print(f"\n{'='*60}")
        print(f"Seeding {len(entities)} {entity_type}s into D1...")
        print(f"{'='*60}")

        entity_stmts = generate_entity_inserts(entities)
        id_stmts = generate_id_inserts(entities)
        all_stmts = entity_stmts + id_stmts
        print(f"  {len(entity_stmts)} entity inserts + {len(id_stmts)} ID inserts = {len(all_stmts)} total")

        if args.dry_run:
            out_path = DATA_DIR / f"{entity_type}s-seed.sql"
            with open(out_path, "w") as f:
                f.write("\n".join(all_stmts))
            print(f"  Dry run: saved to {out_path}")
            continue

        # Execute in file-sized batches
        total_files = (len(all_stmts) + STMTS_PER_FILE - 1) // STMTS_PER_FILE
        for i in range(0, len(all_stmts), STMTS_PER_FILE):
            batch = all_stmts[i : i + STMTS_PER_FILE]
            file_num = i // STMTS_PER_FILE + 1

            with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
                f.write("\n".join(batch))
                tmp_path = f.name

            print(f"  File {file_num}/{total_files} ({len(batch)} stmts)...", end=" ", flush=True)
            if execute_sql_file(tmp_path, remote=not args.local):
                print("OK")
            else:
                print("FAILED")
                Path(tmp_path).unlink()
                return

            Path(tmp_path).unlink()

    # --- Mint reep_ids for all entities ---
    # After a full seed, entities have no reep_ids. Mint them before building provider_ids.
    if not args.dry_run:
        print("\nMinting reep_ids for all entities...")
        type_prefixes = {"player": "p", "team": "t", "coach": "c"}
        page_size = 10_000
        offset = 0
        mint_stmts = []
        used_ids: set[str] = set()

        while True:
            rows = query_d1(
                f"SELECT qid, type FROM entities WHERE reep_id IS NULL "
                f"LIMIT {page_size} OFFSET {offset};",
                remote=not args.local,
            )
            if not rows:
                break
            for row in rows:
                prefix = type_prefixes[row["type"]]
                for _ in range(10):
                    reep_id = f"reep_{prefix}{uuid.uuid4().hex[:8]}"
                    if reep_id not in used_ids:
                        break
                used_ids.add(reep_id)
                mint_stmts.append(
                    f"UPDATE entities SET reep_id = {escape_sql(reep_id)} "
                    f"WHERE qid = {escape_sql(row['qid'])} AND type = {escape_sql(row['type'])} "
                    f"AND reep_id IS NULL;"
                )
            offset += page_size
        print(f"  Generated {len(mint_stmts):,} reep_ids")

        batch_size = 500
        failed = 0
        for i in range(0, len(mint_stmts), batch_size):
            batch = mint_stmts[i:i + batch_size]
            with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
                f.write("\n".join(batch))
                tmp_path = f.name
            try:
                if not execute_sql_file(tmp_path, remote=not args.local):
                    failed += 1
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        if failed > 0:
            print(f"  WARNING: {failed} batch(es) failed during minting")
        else:
            print(f"  Minted {len(mint_stmts):,} reep_ids — OK")

    # --- Rebuild provider_ids from external_ids + wikidata QIDs ---
    if not args.dry_run:
        print("\nRebuilding provider_ids table...")
        provider_schema = """
DROP TABLE IF EXISTS provider_ids;
CREATE TABLE IF NOT EXISTS provider_ids (
    reep_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    PRIMARY KEY (reep_id, provider, external_id)
);
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(provider_schema)
            tmp_path = f.name
        try:
            execute_sql_file(tmp_path, remote=not args.local)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        # Populate from external_ids via JOIN on entities for reep_id
        offset = 0
        provider_stmts = []
        while True:
            rows = query_d1(
                f"SELECT e.reep_id, ei.provider, ei.external_id "
                f"FROM external_ids ei "
                f"JOIN entities e ON ei.qid = e.qid AND ei.type = e.type "
                f"LIMIT {page_size} OFFSET {offset};",
                remote=not args.local,
            )
            if not rows:
                break
            for r in rows:
                provider_stmts.append(
                    f"({escape_sql(r['reep_id'])}, {escape_sql(r['provider'])}, {escape_sql(r['external_id'])})"
                )
            offset += page_size

        # Add wikidata QID mappings
        offset = 0
        while True:
            rows = query_d1(
                f"SELECT reep_id, qid FROM entities WHERE reep_id IS NOT NULL "
                f"LIMIT {page_size} OFFSET {offset};",
                remote=not args.local,
            )
            if not rows:
                break
            for r in rows:
                provider_stmts.append(
                    f"({escape_sql(r['reep_id'])}, 'wikidata', {escape_sql(r['qid'])})"
                )
            offset += page_size

        # Write INSERT statements
        insert_stmts = []
        for i in range(0, len(provider_stmts), ROWS_PER_INSERT):
            batch = provider_stmts[i:i + ROWS_PER_INSERT]
            insert_stmts.append(
                "INSERT OR IGNORE INTO provider_ids (reep_id, provider, external_id) VALUES\n"
                + ",\n".join(batch) + ";"
            )

        print(f"  {len(provider_stmts):,} provider_ids rows to insert")
        failed = 0
        total_files = (len(insert_stmts) + STMTS_PER_FILE - 1) // STMTS_PER_FILE
        for i in range(0, len(insert_stmts), STMTS_PER_FILE):
            batch = insert_stmts[i:i + STMTS_PER_FILE]
            with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
                f.write("\n".join(batch))
                tmp_path = f.name
            try:
                if not execute_sql_file(tmp_path, remote=not args.local):
                    failed += 1
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        if failed > 0:
            print(f"  WARNING: {failed} file(s) failed during provider_ids population")
        else:
            print(f"  provider_ids rebuilt — OK")

        # Create lookup index
        query_d1(
            "CREATE INDEX IF NOT EXISTS idx_provider_ids_lookup ON provider_ids(provider, external_id);",
            remote=not args.local,
        )

        # Verify counts
        ext_rows = query_d1("SELECT COUNT(*) as cnt FROM external_ids;", remote=not args.local)
        prov_rows = query_d1("SELECT COUNT(*) as cnt FROM provider_ids;", remote=not args.local)
        ent_rows = query_d1("SELECT COUNT(*) as cnt FROM entities WHERE reep_id IS NOT NULL;", remote=not args.local)
        ext_count = ext_rows[0]["cnt"] if ext_rows else 0
        prov_count = prov_rows[0]["cnt"] if prov_rows else 0
        ent_count = ent_rows[0]["cnt"] if ent_rows else 0
        expected = ext_count + ent_count
        print(f"  provider_ids: {prov_count:,} (expected {expected:,})")
        if prov_count > 0 and abs(prov_count - expected) / expected < 0.01:
            print(f"  Counts within tolerance — OK")
        elif prov_count == expected:
            print(f"  Counts match exactly — OK")
        else:
            print(f"  WARNING: Count mismatch!")

    # Rebuild FTS index after all entities are seeded
    print("\nRebuilding FTS index...", end=" ", flush=True)
    if args.dry_run:
        print("skipped (dry run)")
    else:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("INSERT INTO entities_fts(entities_fts) VALUES('rebuild');")
            tmp_path = f.name
        try:
            if execute_sql_file(tmp_path, remote=not args.local):
                print("OK")
            else:
                print("FAILED — search will be broken until rebuild succeeds")
                sys.exit(1)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        # Recreate FTS sync triggers after rebuild
        print("Recreating FTS triggers...", end=" ", flush=True)
        trigger_sql = """
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
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(trigger_sql)
            tmp_path = f.name
        try:
            if execute_sql_file(tmp_path, remote=not args.local):
                print("OK")
            else:
                print("FAILED — incremental FTS sync will not work until triggers are recreated")
                sys.exit(1)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    print("\nDone!")


if __name__ == "__main__":
    main()
