"""
Incremental Wikidata update — fetch and seed only changed entities.

Uses schema:dateModified to find entities changed since the last run,
then fetches their data via scoped SPARQL (reusing existing query patterns)
and updates D1 with INSERT OR REPLACE (no DROP, no data loss).

See docs/incremental-fetch-design.md for full design.

Usage:
  python scripts/incremental-update.py                    # incremental from last run
  python scripts/incremental-update.py --since 2026-03-24 # explicit date
  python scripts/incremental-update.py --dry-run          # show changed QID counts, no writes
  python scripts/incremental-update.py --local            # use local D1
"""

import argparse
import json
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Import shared constants and functions from the full fetch script.
# The filename has hyphens so we use importlib.
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "fetch_wikidata_entities",
    Path(__file__).parent / "fetch-wikidata-entities.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

PLAYER_IDS = _mod.PLAYER_IDS
TEAM_IDS = _mod.TEAM_IDS
COACH_IDS = _mod.COACH_IDS
COMPETITION_IDS = _mod.COMPETITION_IDS
SEASON_IDS = _mod.SEASON_IDS
BIO_BATCH_SIZE = _mod.BIO_BATCH_SIZE
sparql_query = _mod.sparql_query
parse_ids_phase = _mod.parse_ids_phase
merge_bio = _mod.merge_bio
build_player_bio_query = _mod.build_player_bio_query
build_team_bio_query = _mod.build_team_bio_query
build_coach_bio_query = _mod.build_coach_bio_query
build_competition_bio_query = _mod.build_competition_bio_query
build_season_bio_query = _mod.build_season_bio_query

DB_NAME = "football-entities"
CIRCUIT_BREAKER_THRESHOLD = 20_000
SENTINEL_FILE = Path(__file__).parent.parent / ".circuit-breaker-tripped"


# ---------------------------------------------------------------------------
# D1 helpers (shared pattern with seed-wikidata-d1.py)
# ---------------------------------------------------------------------------

def query_d1(sql: str, remote: bool = True) -> list[dict]:
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


# ---------------------------------------------------------------------------
# Step 1: Find changed QIDs via schema:dateModified
# ---------------------------------------------------------------------------

def fetch_changed_qids(since: str) -> dict[str, list[str]]:
    """Fetch QIDs modified since the given date, per entity type."""
    type_queries = {
        "player": f"""
            SELECT DISTINCT ?e WHERE {{
              ?e wdt:P106 wd:Q937857 .
              ?e schema:dateModified ?mod .
              FILTER(?mod > "{since}T00:00:00Z"^^xsd:dateTime)
              FILTER NOT EXISTS {{ ?e wdt:P31 wd:Q95074 }}
              FILTER NOT EXISTS {{ ?e wdt:P31 wd:Q15632617 }}
            }}""",
        "team": f"""
            SELECT DISTINCT ?e WHERE {{
              ?e wdt:P31 ?type .
              ?type (wdt:P279)* wd:Q476028 .
              ?e schema:dateModified ?mod .
              FILTER(?mod > "{since}T00:00:00Z"^^xsd:dateTime)
            }}""",
        "coach": f"""
            SELECT DISTINCT ?e WHERE {{
              ?e wdt:P106 wd:Q628099 .
              ?e schema:dateModified ?mod .
              FILTER(?mod > "{since}T00:00:00Z"^^xsd:dateTime)
              FILTER NOT EXISTS {{ ?e wdt:P31 wd:Q95074 }}
              FILTER NOT EXISTS {{ ?e wdt:P31 wd:Q15632617 }}
            }}""",
        "competition": f"""
            SELECT DISTINCT ?e WHERE {{
              {{ ?e wdt:P31/wdt:P279* wd:Q15991290 . }}
              UNION
              {{ ?e wdt:P12758 [] . }}
              UNION
              {{ ?e wdt:P13664 [] . }}
              UNION
              {{ ?e wdt:P8735 [] . }}
              ?e schema:dateModified ?mod .
              FILTER(?mod > "{since}T00:00:00Z"^^xsd:dateTime)
            }}""",
        "season": f"""
            SELECT DISTINCT ?e WHERE {{
              ?e wdt:P3450 ?comp .
              ?comp wdt:P31/wdt:P279* wd:Q15991290 .
              ?e schema:dateModified ?mod .
              FILTER(?mod > "{since}T00:00:00Z"^^xsd:dateTime)
            }}""",
    }

    changed: dict[str, list[str]] = {}
    for entity_type, query in type_queries.items():
        print(f"  Fetching changed {entity_type} QIDs...", end=" ", flush=True)
        t0 = time.time()
        rows = sparql_query(query)
        qids = [r["e"].split("/")[-1] for r in rows]
        elapsed = time.time() - t0
        print(f"{len(qids):,} ({elapsed:.1f}s)")
        changed[entity_type] = qids
        time.sleep(2)

    return changed


# ---------------------------------------------------------------------------
# Step 2: Circuit breaker
# ---------------------------------------------------------------------------

def check_circuit_breaker(changed: dict[str, list[str]]) -> bool:
    """Check if total changed QIDs exceeds threshold. Returns True if OK."""
    total = sum(len(qids) for qids in changed.values())
    if total > CIRCUIT_BREAKER_THRESHOLD:
        print(f"\n  Circuit breaker: {total:,} > {CIRCUIT_BREAKER_THRESHOLD:,} threshold")
        print(f"  Writing sentinel file and exiting for full fetch fallback.")
        SENTINEL_FILE.write_text(
            f"tripped={datetime.now(timezone.utc).isoformat()}\n"
            f"total={total}\n"
        )
        return False
    print(f"\n  Circuit breaker: OK ({total:,} under {CIRCUIT_BREAKER_THRESHOLD:,} threshold)")
    return True


# ---------------------------------------------------------------------------
# Step 3: FTS trigger guard
# ---------------------------------------------------------------------------

def check_fts_triggers(remote: bool) -> bool:
    """Verify FTS sync triggers exist. Recreate if missing."""
    rows = query_d1(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name = 'entities_fts_ai';",
        remote=remote,
    )
    if rows:
        print("  FTS triggers: present")
        return True

    print("  FTS triggers: MISSING — recreating...", end=" ", flush=True)
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
        if execute_sql_file(tmp_path, remote=remote):
            print("OK")
            return True
        else:
            print("FAILED — FTS will drift")
            return False
    finally:
        Path(tmp_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Step 4: Scoped SPARQL fetch (IDs + bio)
# ---------------------------------------------------------------------------

TYPE_CONFIGS = {
    "player": (PLAYER_IDS, build_player_bio_query),
    "team": (TEAM_IDS, build_team_bio_query),
    "coach": (COACH_IDS, build_coach_bio_query),
    "competition": (COMPETITION_IDS, build_competition_bio_query),
    "season": (SEASON_IDS, build_season_bio_query),
}


def build_scoped_ids_query(qids: list[str], entity_type: str) -> str:
    """Build an IDs query scoped to specific QIDs via VALUES clause."""
    id_props = TYPE_CONFIGS[entity_type][0]
    values = " ".join(f"wd:{q}" for q in qids)
    id_optionals = "\n".join(
        f"  OPTIONAL {{ ?e wdt:{prop} ?id_{name} . }}"
        for name, prop in id_props.items()
    )
    id_selects = " ".join(f"?id_{name}" for name in id_props)

    return f"""
SELECT ?e ?eLabel {id_selects}
WHERE {{
  VALUES ?e {{ {values} }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def fetch_scoped(qids: list[str], entity_type: str) -> dict[str, dict]:
    """Fetch IDs + bio for specific QIDs using scoped SPARQL queries."""
    id_props, bio_query_fn = TYPE_CONFIGS[entity_type]

    # Phase 1: IDs in batches of BIO_BATCH_SIZE
    all_id_rows = []
    total_batches = (len(qids) + BIO_BATCH_SIZE - 1) // BIO_BATCH_SIZE
    for i in range(0, len(qids), BIO_BATCH_SIZE):
        batch = qids[i : i + BIO_BATCH_SIZE]
        batch_num = i // BIO_BATCH_SIZE + 1
        print(f"    IDs batch {batch_num}/{total_batches} ({len(batch)} QIDs)...", end=" ", flush=True)
        query = build_scoped_ids_query(batch, entity_type)
        rows = sparql_query(query)
        all_id_rows.extend(rows)
        print(f"{len(rows)} rows")
        if i + BIO_BATCH_SIZE < len(qids):
            time.sleep(2)

    entities = parse_ids_phase(all_id_rows, entity_type, id_props)

    # Phase 2: Bio in batches (reuses existing bio query builders + merge_bio)
    print(f"  Fetching bio details...")
    qid_list = list(entities.keys())
    for i in range(0, len(qid_list), BIO_BATCH_SIZE):
        batch = qid_list[i : i + BIO_BATCH_SIZE]
        batch_num = i // BIO_BATCH_SIZE + 1
        print(f"    Bio batch {batch_num}/{total_batches} ({len(batch)} QIDs)...", end=" ", flush=True)
        query = bio_query_fn(batch)
        rows = sparql_query(query)
        merge_bio(entities, rows, entity_type)
        print(f"{len(rows)} rows")
        if i + BIO_BATCH_SIZE < len(qid_list):
            time.sleep(2)

    return entities


# ---------------------------------------------------------------------------
# Step 5: Update D1 (DELETE stale provider_ids + INSERT OR REPLACE entities)
# ---------------------------------------------------------------------------

ROWS_PER_INSERT = 200
STMTS_PER_FILE = 5000

ENTITY_COLS = ("reep_id", "type", "name_en", "aliases_en", "full_name",
               "date_of_birth", "nationality", "position", "current_team_reep_id",
               "height_cm", "country", "founded", "stadium", "source",
               "competition_reep_id", "updated_at")

TYPE_PREFIXES = {"player": "p", "team": "t", "coach": "c", "competition": "l", "season": "s"}

# Processing order matters: competitions must be seeded before seasons (FK dependency).
TYPE_ORDER = ["player", "team", "coach", "competition", "season"]


def generate_update_sql(entities: dict[str, dict], entity_type: str,
                        existing_reep_ids: dict[str, str] | None = None,
                        team_qid_to_reep: dict[str, str] | None = None,
                        comp_qid_to_reep: dict[str, str] | None = None) -> list[str]:
    """Generate SQL: DELETE stale provider_ids + INSERT OR REPLACE entities + provider_ids.

    existing_reep_ids: map of qid -> reep_id for entities already in D1.
    team_qid_to_reep: map of team QID -> reep_id for current_team resolution.
    comp_qid_to_reep: map of competition QID -> reep_id for season FK resolution.
    New entities get a freshly minted reep_id.
    """
    stmts = []
    entity_list = list(entities.values())

    # Assign reep_ids: look up existing or mint new
    for e in entity_list:
        qid = e["qid"]
        if existing_reep_ids and qid in existing_reep_ids:
            e["reep_id"] = existing_reep_ids[qid]
        elif not e.get("reep_id"):
            prefix = TYPE_PREFIXES.get(e["type"], "p")
            e["reep_id"] = f"reep_{prefix}{uuid.uuid4().hex[:8]}"

    # DELETE provider_ids for changed entities (will be re-inserted)
    # Only delete Wikidata-sourced IDs, not custom_ids
    for i in range(0, len(entity_list), ROWS_PER_INSERT):
        batch = entity_list[i : i + ROWS_PER_INSERT]
        reep_ids = ", ".join(escape_sql(e["reep_id"]) for e in batch)
        stmts.append(f"DELETE FROM provider_ids WHERE reep_id IN ({reep_ids});")

    # INSERT OR REPLACE entities (reep_id PK schema)
    col_str = ", ".join(ENTITY_COLS)
    for i in range(0, len(entity_list), ROWS_PER_INSERT):
        batch = entity_list[i : i + ROWS_PER_INSERT]
        rows = []
        for e in batch:
            # Resolve current_team QID to reep_id
            team_qid = e.get("current_team_qid")
            team_reep = None
            if team_qid and team_qid_to_reep:
                team_reep = team_qid_to_reep.get(team_qid)

            # Resolve competition QID to reep_id for seasons
            comp_reep = None
            if entity_type == "season":
                comp_qid = e.get("competition_qid")
                if comp_qid and comp_qid_to_reep:
                    comp_reep = comp_qid_to_reep.get(comp_qid)

            vals = (
                escape_sql(e["reep_id"]),
                escape_sql(e["type"]),
                escape_sql(e["name_en"]),
                escape_sql(e.get("aliases_en")),
                escape_sql(e.get("full_name")),
                escape_sql(e.get("date_of_birth")),
                escape_sql(e.get("nationality")),
                escape_sql(e.get("position")),
                escape_sql(team_reep),
                str(e.get("height_cm")) if e.get("height_cm") is not None else "NULL",
                escape_sql(e.get("country")),
                escape_sql(e.get("founded")),
                escape_sql(e.get("stadium")),
                "'wikidata'",
                escape_sql(comp_reep),
                "datetime('now')",
            )
            rows.append(f"({', '.join(vals)})")
        stmts.append(
            f"INSERT OR REPLACE INTO entities ({col_str}) VALUES\n"
            + ",\n".join(rows) + ";"
        )

    # INSERT provider_ids (wikidata QID + all provider IDs)
    all_provider_rows = []
    for e in entity_list:
        reep_id = e.get("reep_id")
        if not reep_id:
            continue
        # Wikidata QID mapping
        all_provider_rows.append((reep_id, "wikidata", e["qid"]))
        # Provider ID mappings
        for provider, ext_id in e.get("external_ids", {}).items():
            all_provider_rows.append((reep_id, provider, ext_id))

    for i in range(0, len(all_provider_rows), ROWS_PER_INSERT):
        batch = all_provider_rows[i : i + ROWS_PER_INSERT]
        rows = []
        for reep_id, provider, ext_id in batch:
            rows.append(
                f"({escape_sql(reep_id)}, {escape_sql(provider)}, {escape_sql(ext_id)})"
            )
        stmts.append(
            "INSERT OR IGNORE INTO provider_ids (reep_id, provider, external_id) VALUES\n"
            + ",\n".join(rows) + ";"
        )

    return stmts


def execute_stmts(stmts: list[str], remote: bool, label: str) -> bool:
    """Execute SQL statements in file-sized batches."""
    total_files = (len(stmts) + STMTS_PER_FILE - 1) // STMTS_PER_FILE
    for i in range(0, len(stmts), STMTS_PER_FILE):
        batch = stmts[i : i + STMTS_PER_FILE]
        file_num = i // STMTS_PER_FILE + 1

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("\n".join(batch))
            tmp_path = f.name

        print(f"  {label} file {file_num}/{total_files} ({len(batch)} stmts)...", end=" ", flush=True)
        if execute_sql_file(tmp_path, remote=remote):
            print("OK")
        else:
            print("FAILED")
            Path(tmp_path).unlink(missing_ok=True)
            return False

        Path(tmp_path).unlink(missing_ok=True)

    return True


# ---------------------------------------------------------------------------
# Step 6: Update last_run timestamp
# ---------------------------------------------------------------------------

def get_last_run(remote: bool) -> str | None:
    """Read last_run timestamp from D1 meta table."""
    rows = query_d1(
        "SELECT value FROM meta WHERE key = 'last_incremental';",
        remote=remote,
    )
    if rows:
        return rows[0].get("value")
    return None


def update_last_run(timestamp: str, remote: bool):
    """Write last_run timestamp to D1 meta table."""
    query_d1(
        f"CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);",
        remote=remote,
    )
    query_d1(
        f"INSERT OR REPLACE INTO meta (key, value) VALUES ('last_incremental', '{timestamp}');",
        remote=remote,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Incremental Wikidata update")
    parser.add_argument("--since", help="Override: fetch changes since this date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Show changed QID counts, no writes")
    parser.add_argument("--local", action="store_true", help="Use local D1")
    args = parser.parse_args()

    remote = not args.local
    run_start = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Determine since date
    if args.since:
        since = args.since
    else:
        since = get_last_run(remote)
        if not since:
            print("No last_run timestamp found in D1 meta table.")
            print("Use --since YYYY-MM-DD for the first run, or run a full fetch first.")
            sys.exit(1)
        # Take just the date part if it's a full timestamp
        since = since.split("T")[0]

    print(f"Incremental update: changes since {since}")
    print(f"{'='*60}")

    # Step 1: Find changed QIDs
    print("\nStep 1: Finding changed entities...")
    changed = fetch_changed_qids(since)
    total = sum(len(qids) for qids in changed.values())

    if total == 0:
        print("\nNo changed entities found. Nothing to do.")
        update_last_run(run_start, remote)
        return

    # Step 2: Circuit breaker
    if not check_circuit_breaker(changed):
        sys.exit(0)

    if args.dry_run:
        print(f"\nDry run complete. {total:,} entities would be updated.")
        print(f"Sample QIDs: {', '.join(list(changed['player'])[:5])}")
        return

    # Step 3: FTS trigger guard
    print("\nStep 3: Checking FTS triggers...")
    check_fts_triggers(remote)

    # Step 4-5: Fetch + update per type
    unmapped = set(changed.keys()) - set(TYPE_ORDER)
    assert not unmapped, f"Unhandled entity types in changed but not in TYPE_ORDER: {unmapped}"
    for entity_type in TYPE_ORDER:
        qids = changed.get(entity_type, [])
        if not qids:
            print(f"\n  {entity_type}: no changes, skipping")
            continue

        print(f"\n{'='*60}")
        print(f"Processing {len(qids):,} changed {entity_type}s")
        print(f"{'='*60}")

        # Step 4: Fetch via scoped SPARQL
        print(f"  Fetching IDs...")
        entities = fetch_scoped(qids, entity_type)
        print(f"  Parsed {len(entities):,} entities")

        # Look up existing reep_ids for changed entities via provider_ids
        existing_reep_ids: dict[str, str] = {}  # qid -> reep_id
        qid_list_for_lookup = list(entities.keys())
        for i in range(0, len(qid_list_for_lookup), 200):
            batch_qids = qid_list_for_lookup[i:i + 200]
            qid_sql = ", ".join(escape_sql(q) for q in batch_qids)
            rows = query_d1(
                f"SELECT external_id AS qid, reep_id FROM provider_ids "
                f"WHERE provider = 'wikidata' AND external_id IN ({qid_sql});",
                remote=remote,
            )
            for r in rows:
                existing_reep_ids[r["qid"]] = r["reep_id"]
        print(f"  Found {len(existing_reep_ids):,} existing reep_ids")

        # Look up team reep_ids for current_team resolution (players/coaches only)
        team_qid_to_reep: dict[str, str] = {}
        if entity_type in ("player", "coach"):
            team_qids = {e.get("current_team_qid") for e in entities.values() if e.get("current_team_qid")}
            team_qids = list(team_qids - {None})
            for i in range(0, len(team_qids), 200):
                batch = team_qids[i:i + 200]
                qid_sql = ", ".join(escape_sql(q) for q in batch)
                rows = query_d1(
                    f"SELECT external_id AS qid, reep_id FROM provider_ids "
                    f"WHERE provider = 'wikidata' AND external_id IN ({qid_sql});",
                    remote=remote,
                )
                for r in rows:
                    team_qid_to_reep[r["qid"]] = r["reep_id"]
            if team_qids:
                print(f"  Resolved {len(team_qid_to_reep):,}/{len(team_qids):,} team QIDs to reep_ids")

        # Look up competition reep_ids for season FK resolution
        comp_qid_to_reep: dict[str, str] = {}
        if entity_type == "season":
            comp_qids = {e.get("competition_qid") for e in entities.values() if e.get("competition_qid")}
            comp_qids_list = list(comp_qids - {None})
            for i in range(0, len(comp_qids_list), 200):
                batch = comp_qids_list[i:i + 200]
                qid_sql = ", ".join(escape_sql(q) for q in batch)
                rows = query_d1(
                    f"SELECT external_id AS qid, reep_id FROM provider_ids "
                    f"WHERE provider = 'wikidata' AND external_id IN ({qid_sql});",
                    remote=remote,
                )
                for r in rows:
                    comp_qid_to_reep[r["qid"]] = r["reep_id"]
            if comp_qids_list:
                print(f"  Resolved {len(comp_qid_to_reep):,}/{len(comp_qids_list):,} competition QIDs to reep_ids")

        # Step 5: Update D1
        print(f"  Generating SQL...")
        stmts = generate_update_sql(entities, entity_type, existing_reep_ids, team_qid_to_reep, comp_qid_to_reep)
        print(f"  {len(stmts)} statements")

        if not execute_stmts(stmts, remote, entity_type):
            print(f"\n  FAILED to update {entity_type}s — aborting")
            sys.exit(1)

    # Step 6: Update last_run
    print(f"\nUpdating last_run timestamp to {run_start}...")
    update_last_run(run_start, remote)

    print(f"\nDone! Updated {total:,} entities.")


if __name__ == "__main__":
    main()
