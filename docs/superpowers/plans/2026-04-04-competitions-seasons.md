# Competitions & Seasons Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add competition and season entity types to Reep, enabling cross-provider ID resolution for football competitions and their seasonal editions.

**Architecture:** Competitions (`reep_l`) and seasons (`reep_s`) are added to the existing `entities` table with a `competition_reep_id` FK on seasons. Wikidata SPARQL queries are explored first to assess coverage, then the pipeline, worker API, and CSV exports are extended. The incremental update path is the production pipeline — no changes to the stale full-seed script.

**Tech Stack:** Python (SPARQL queries, D1 seeding, CSV export), TypeScript (Cloudflare Worker), SQL (D1/SQLite), YAML (GitHub Actions)

**Spec:** `docs/superpowers/specs/2026-04-04-competitions-seasons-design.md`

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `scripts/research-competitions.py` | Create | SPARQL exploration script for coverage assessment |
| `scripts/fetch-wikidata-entities.py` | Modify | Add COMPETITION_IDS, SEASON_IDS dicts + query builders |
| `scripts/incremental-update.py` | Modify | Add competition/season to TYPE_PREFIXES, TYPE_CONFIGS, ENTITY_COLS, fetch_changed_qids |
| `scripts/export-csv.py` | Modify | Add competitions.csv and seasons.csv export functions |
| `src/worker.ts` | Modify | Add competition_reep_id to ENTITY_COLS, FTS season exclusion, response shaping |
| `cli/reep.py` | Modify | Add competition/season to type choices |
| `openapi.yaml` | Modify | Add competition/season types and competition_reep_id field |
| `CLAUDE.md` | Modify | Update identity model table, commands, scripts |
| `README.md` | Modify | Update coverage table |
| `.github/workflows/update-register.yml` | Modify | Extend validation and sample generation for new types |

---

## Task 1: Research — Explore Wikidata Competition Coverage

**Files:**
- Create: `scripts/research-competitions.py`

This is the research phase. Run SPARQL queries to measure what Wikidata has before committing to a sourcing strategy.

- [ ] **Step 1: Create the research script**

```python
"""
Explore Wikidata coverage of football competitions and seasons.
Outputs counts and samples — does not write to D1.

Usage:
  python scripts/research-competitions.py
"""

import json
import time
import urllib.request
import urllib.parse
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "reep-football-register/1.0 (https://github.com/withqwerty/reep)"


def sparql_query(query: str, retries: int = 3) -> list[dict]:
    body = urllib.parse.urlencode({"query": query}).encode("utf-8")
    req = urllib.request.Request(
        ENDPOINT,
        data=body,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode(), strict=False)
            return [
                {k: v["value"] for k, v in binding.items()}
                for binding in data["results"]["bindings"]
            ]
        except Exception as e:
            if attempt < retries:
                print(f"  Error: {e}. Retrying in 15s...")
                time.sleep(15)
                continue
            raise
    return []


def main():
    print("=" * 60)
    print("1. Football competitions (Q15991290 + subclasses)")
    print("=" * 60)

    # Count all football competitions
    rows = sparql_query("""
        SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {
          ?e wdt:P31/wdt:P279* wd:Q15991290 .
        }
    """)
    print(f"  Total competitions: {rows[0]['count'] if rows else '?'}")
    time.sleep(3)

    # Count competitions with specific provider IDs
    for provider, prop in [("transfermarkt", "P12758"), ("fbref", "P13664"), ("opta", "P8735")]:
        rows = sparql_query(f"""
            SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {{
              ?e wdt:P31/wdt:P279* wd:Q15991290 .
              ?e wdt:{prop} ?id .
            }}
        """)
        print(f"  With {provider} ID ({prop}): {rows[0]['count'] if rows else '?'}")
        time.sleep(3)

    # Sample competitions with names + provider IDs
    print("\n  Sample competitions with provider IDs:")
    rows = sparql_query("""
        SELECT ?e ?eLabel ?tm ?fbref ?opta WHERE {
          ?e wdt:P31/wdt:P279* wd:Q15991290 .
          OPTIONAL { ?e wdt:P12758 ?tm . }
          OPTIONAL { ?e wdt:P13664 ?fbref . }
          OPTIONAL { ?e wdt:P8735 ?opta . }
          FILTER(BOUND(?tm) || BOUND(?fbref) || BOUND(?opta))
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        LIMIT 20
    """)
    for r in rows:
        qid = r["e"].split("/")[-1]
        name = r.get("eLabel", "?")
        ids = []
        if r.get("tm"): ids.append(f"tm={r['tm']}")
        if r.get("fbref"): ids.append(f"fbref={r['fbref']}")
        if r.get("opta"): ids.append(f"opta={r['opta']}")
        print(f"    {qid}: {name} [{', '.join(ids)}]")
    time.sleep(5)

    print(f"\n{'=' * 60}")
    print("2. Football seasons (P3450 = sports season of league)")
    print("=" * 60)

    # Count all seasons
    rows = sparql_query("""
        SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {
          ?e wdt:P3450 ?comp .
        }
    """)
    print(f"  Total seasons (items with P3450): {rows[0]['count'] if rows else '?'}")
    time.sleep(3)

    # Count seasons that are specifically football
    rows = sparql_query("""
        SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {
          ?e wdt:P3450 ?comp .
          ?comp wdt:P31/wdt:P279* wd:Q15991290 .
        }
    """)
    print(f"  Football-specific seasons: {rows[0]['count'] if rows else '?'}")
    time.sleep(3)

    # Check if seasons have ANY provider ID properties
    print("\n  Checking season provider ID properties...")
    for provider, prop in [("transfermarkt", "P12758"), ("fbref", "P13664"), ("opta", "P8735")]:
        rows = sparql_query(f"""
            SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {{
              ?e wdt:P3450 ?comp .
              ?comp wdt:P31/wdt:P279* wd:Q15991290 .
              ?e wdt:{prop} ?id .
            }}
        """)
        print(f"    Seasons with {provider} ID ({prop}): {rows[0]['count'] if rows else '?'}")
        time.sleep(3)

    # Sample seasons
    print("\n  Sample seasons:")
    rows = sparql_query("""
        SELECT ?e ?eLabel ?comp ?compLabel WHERE {
          ?e wdt:P3450 ?comp .
          ?comp wdt:P31/wdt:P279* wd:Q15991290 .
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        LIMIT 20
    """)
    for r in rows:
        qid = r["e"].split("/")[-1]
        name = r.get("eLabel", "?")
        comp_qid = r["comp"].split("/")[-1]
        comp_name = r.get("compLabel", "?")
        print(f"    {qid}: {name} -> {comp_qid} ({comp_name})")
    time.sleep(3)

    # Historical depth — earliest seasons
    print("\n  Historical depth (earliest seasons):")
    rows = sparql_query("""
        SELECT ?e ?eLabel ?start WHERE {
          ?e wdt:P3450 ?comp .
          ?comp wdt:P31/wdt:P279* wd:Q15991290 .
          ?e wdt:P580 ?start .
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        ORDER BY ?start
        LIMIT 10
    """)
    for r in rows:
        name = r.get("eLabel", "?")
        start = r.get("start", "?").split("T")[0]
        print(f"    {start}: {name}")

    print(f"\n{'=' * 60}")
    print("3. Summary")
    print("=" * 60)
    print("  Review the counts above to decide sourcing strategy.")
    print("  If competition coverage with provider IDs is reasonable,")
    print("  proceed with Wikidata-first pipeline.")
    print("  If sparse, consider FBref/Transfermarkt as primary seed.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the research script**

Run: `python scripts/research-competitions.py`

Expected: Counts and samples for competitions and seasons. Note the numbers — they inform the sourcing decision in Task 2.

- [ ] **Step 3: Commit**

```bash
git add scripts/research-competitions.py
git commit -m "research: add SPARQL exploration script for competitions and seasons"
```

---

## Task 2: Decision Gate — Evaluate Research Results

**Files:** None (decision only)

- [ ] **Step 1: Evaluate competition coverage**

Based on the research script output, answer:
- How many football competitions does Wikidata have?
- How many have at least one provider ID (Transfermarkt, FBref, or Opta)?
- Are top-5 European leagues present? Major cups? Continental competitions?

- [ ] **Step 2: Evaluate season coverage**

Based on the research script output, answer:
- How many football-specific seasons exist?
- Do ANY seasons have provider ID properties? (Expected: near zero)
- How far back do seasons go historically?

- [ ] **Step 3: Decide sourcing strategy**

Present findings to user. Two outcomes:
- **Wikidata-first:** Reasonable competition coverage → proceed with Tasks 3-10 as written
- **Alternative source:** Sparse coverage → design a FBref/Transfermarkt import script (new task, not in this plan)

**Do not proceed past this task until the sourcing decision is made.**

---

## Task 3: Schema — Add `competition_reep_id` Column to D1

**Files:**
- None (DDL executed directly against D1)

- [ ] **Step 1: Add the column to production D1**

Run:
```bash
pnpm exec wrangler d1 execute football-entities --remote \
  --command="ALTER TABLE entities ADD COLUMN competition_reep_id TEXT;"
```

Expected: Success. Column added, NULL for all existing rows.

- [ ] **Step 2: Verify the column exists**

Run:
```bash
pnpm exec wrangler d1 execute football-entities --remote \
  --command="PRAGMA table_info(entities);"
```

Expected: `competition_reep_id` appears in the column list.

- [ ] **Step 3: Add index on competition_reep_id**

```bash
pnpm exec wrangler d1 execute football-entities --remote \
  --command="CREATE INDEX IF NOT EXISTS idx_entities_competition ON entities(competition_reep_id);"
```

Expected: Index created. This supports the "find all seasons of a competition" query.

---

## Task 4: Pipeline — Add Competition and Season Queries to `fetch-wikidata-entities.py`

**Files:**
- Modify: `scripts/fetch-wikidata-entities.py:28-96` (provider dicts and excluded list)
- Modify: `scripts/fetch-wikidata-entities.py:233-312` (query builders)
- Modify: `scripts/fetch-wikidata-entities.py:315-343` (parse_ids_phase)
- Modify: `scripts/fetch-wikidata-entities.py:346-455` (bio queries and merge)
- Modify: `scripts/fetch-wikidata-entities.py:471-532` (main)

- [ ] **Step 1: Add COMPETITION_IDS and SEASON_IDS provider dicts**

After `COACH_IDS` (line 96), add:

```python
COMPETITION_IDS = {
    "transfermarkt": "P12758",
    "fbref": "P13664",
    "opta": "P8735",
}

# Season provider IDs — may be empty if Wikidata has no dedicated season properties.
# Season cross-referencing will rely on custom_ids (derived from competition ID + year).
SEASON_IDS: dict[str, str] = {}
```

Update the excluded properties comment (lines 30-43) to remove P12758, P13664, P8735 from the excluded list since they're now supported.

- [ ] **Step 2: Add competition and season query builders**

After `build_coach_ids_query` (line 312), add:

```python
def build_competition_ids_query(limit: int = 0, offset: int = 0) -> str:
    id_optionals = "\n".join(
        f"  OPTIONAL {{ ?e wdt:{prop} ?id_{name} . }}"
        for name, prop in COMPETITION_IDS.items()
    )
    id_selects = " ".join(f"?id_{name}" for name in COMPETITION_IDS)
    limit_clause = f"LIMIT {limit}" if limit else ""
    offset_clause = f"OFFSET {offset}" if offset else ""

    return f"""
SELECT ?e ?eLabel {id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e WHERE {{
      ?e wdt:P31/wdt:P279* wd:Q15991290 .
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_season_ids_query(limit: int = 0, offset: int = 0) -> str:
    # Seasons are items with P3450 (sports season of league) pointing to a football competition.
    # SEASON_IDS may be empty — seasons often lack dedicated provider properties.
    id_optionals = "\n".join(
        f"  OPTIONAL {{ ?e wdt:{prop} ?id_{name} . }}"
        for name, prop in SEASON_IDS.items()
    )
    id_selects = " ".join(f"?id_{name}" for name in SEASON_IDS)
    limit_clause = f"LIMIT {limit}" if limit else ""
    offset_clause = f"OFFSET {offset}" if offset else ""

    return f"""
SELECT ?e ?eLabel ?competitionQid {id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e ?competitionQid WHERE {{
      ?e wdt:P3450 ?comp .
      ?comp wdt:P31/wdt:P279* wd:Q15991290 .
      BIND(?comp AS ?competitionQid)
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""
```

- [ ] **Step 3: Add competition bio query**

After `build_coach_bio_query` (line 403), add:

```python
def build_competition_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?countryLabel
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P17 ?country . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_season_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?startDate ?endDate ?competitionQid
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P580 ?startDate . }}
  OPTIONAL {{ ?e wdt:P582 ?endDate . }}
  OPTIONAL {{ ?e wdt:P3450 ?competitionQid . }}
}}
"""
```

- [ ] **Step 4: Extend `parse_ids_phase` to handle season's competition QID**

The existing `parse_ids_phase` (line 315) initialises entity dicts with player/team-specific fields. For seasons, we need to capture the `competitionQid` from the SPARQL results. Modify `parse_ids_phase` to store it:

In `parse_ids_phase`, after the entity dict initialisation (line 323-338), add handling for `competitionQid`:

```python
        # Capture competition QID for seasons (from SPARQL result)
        comp_qid_uri = row.get("competitionQid")
        if comp_qid_uri and entity_type == "season":
            comp_qid = extract_qid(comp_qid_uri)
            if comp_qid.startswith("Q"):
                entities[qid]["competition_qid"] = comp_qid
```

Also ensure the entity template includes `"competition_qid": None` for all types.

- [ ] **Step 5: Extend `merge_bio` for competition and season types**

In `merge_bio` (line 406), add handling after the team block (line 449):

```python
        if entity_type == "competition":
            if not e.get("country") and row.get("countryLabel"):
                e["country"] = row["countryLabel"]

        if entity_type == "season":
            # Competition QID from bio query (backup — primary source is IDs phase)
            if not e.get("competition_qid") and row.get("competitionQid"):
                comp_uri = row["competitionQid"]
                comp_qid = comp_uri.split("/")[-1] if "/" in comp_uri else comp_uri
                if comp_qid.startswith("Q"):
                    e["competition_qid"] = comp_qid
```

- [ ] **Step 6: Update `main()` type_configs and args**

In `main()` (line 471), update `type_configs` and the `--type` argument:

Update the argparse `--type` choices (line 474):
```python
    parser.add_argument("--type", choices=["player", "team", "coach", "competition", "season"], help="Single entity type")
```

Update `type_configs` (line 480-484):
```python
    type_configs = {
        "player": (build_player_ids_query, PLAYER_IDS, build_player_bio_query),
        "team": (build_team_ids_query, TEAM_IDS, build_team_bio_query),
        "coach": (build_coach_ids_query, COACH_IDS, build_coach_bio_query),
        "competition": (build_competition_ids_query, COMPETITION_IDS, build_competition_bio_query),
        "season": (build_season_ids_query, SEASON_IDS, build_season_bio_query),
    }
```

Also export the new constants so `incremental-update.py` can import them:

After `PLAYER_IDS` assignment rename pattern — add at the module level (these are already importable by name, but the incremental script imports specific names):

```python
# Aliases for incremental-update.py imports
PLAYER_PROVIDERS = PLAYER_IDS
TEAM_PROVIDERS = TEAM_IDS
COACH_PROVIDERS = COACH_IDS
COMPETITION_PROVIDERS = COMPETITION_IDS
SEASON_PROVIDERS = SEASON_IDS
```

Actually — check what `incremental-update.py` imports first (line 38-40): it imports `PLAYER_IDS`, `TEAM_IDS`, `COACH_IDS`. So we just need `COMPETITION_IDS` and `SEASON_IDS` to be importable, which they already are as module-level constants.

- [ ] **Step 7: Test with `--test` flag**

Run: `python scripts/fetch-wikidata-entities.py --type competition --test 5`

Expected: Fetches 5 competition entities with names and provider IDs. Creates `data/json/competitions.json`.

Run: `python scripts/fetch-wikidata-entities.py --type season --test 5`

Expected: Fetches 5 season entities with names and competition QID links. Creates `data/json/seasons.json`.

- [ ] **Step 8: Commit**

```bash
git add scripts/fetch-wikidata-entities.py
git commit -m "feat: add competition and season SPARQL queries to Wikidata fetch"
```

---

## Task 5: Pipeline — Extend `incremental-update.py` for Competitions and Seasons

**Files:**
- Modify: `scripts/incremental-update.py:38-48` (imports)
- Modify: `scripts/incremental-update.py:94-133` (fetch_changed_qids)
- Modify: `scripts/incremental-update.py:206-209` (TYPE_CONFIGS)
- Modify: `scripts/incremental-update.py:277-282` (ENTITY_COLS, TYPE_PREFIXES)
- Modify: `scripts/incremental-update.py:285-372` (generate_update_sql)
- Modify: `scripts/incremental-update.py:479-534` (main loop)

- [ ] **Step 1: Update imports**

At lines 38-47, add the new imports:

```python
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
```

- [ ] **Step 2: Add changed-QID queries for competitions and seasons**

In `fetch_changed_qids` (line 94), add to `type_queries`:

```python
        "competition": f"""
            SELECT DISTINCT ?e WHERE {{
              ?e wdt:P31/wdt:P279* wd:Q15991290 .
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
```

- [ ] **Step 3: Update TYPE_CONFIGS, TYPE_PREFIXES, and ENTITY_COLS**

```python
TYPE_CONFIGS = {
    "player": (PLAYER_IDS, build_player_bio_query),
    "team": (TEAM_IDS, build_team_bio_query),
    "coach": (COACH_IDS, build_coach_bio_query),
    "competition": (COMPETITION_IDS, build_competition_bio_query),
    "season": (SEASON_IDS, build_season_bio_query),
}
```

```python
TYPE_PREFIXES = {"player": "p", "team": "t", "coach": "c", "competition": "l", "season": "s"}
```

```python
ENTITY_COLS = ("reep_id", "type", "name_en", "aliases_en", "full_name",
               "date_of_birth", "nationality", "position", "current_team_reep_id",
               "height_cm", "country", "founded", "stadium", "source",
               "competition_reep_id", "updated_at")
```

- [ ] **Step 4: Update `generate_update_sql` for `competition_reep_id`**

In `generate_update_sql` (line 285), the `vals` tuple (line 325-341) must include `competition_reep_id`. Add after the `stadium` value and before `'wikidata'`:

```python
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
```

Add `comp_qid_to_reep: dict[str, str] | None = None` parameter to `generate_update_sql`. The default `None` ensures existing calls for player/team/coach types don't need changes — they'll pass `None` (or omit the argument) and `comp_reep` will stay `None`.

- [ ] **Step 5: Update main loop to process competitions before seasons and resolve competition reep_ids**

In `main()`, change the per-type loop (line 479) to ensure ordering and add competition QID resolution for seasons:

```python
    # Process competitions before seasons so competition reep_ids exist for FK resolution
    type_order = ["player", "team", "coach", "competition", "season"]
    for entity_type in type_order:
        qids = changed.get(entity_type, [])
        if not qids:
            print(f"\n  {entity_type}: no changes, skipping")
            continue

        # ... (existing fetch + reep_id lookup code) ...

        # Look up competition reep_ids for seasons
        comp_qid_to_reep: dict[str, str] = {}
        if entity_type == "season":
            comp_qids = {e.get("competition_qid") for e in entities.values() if e.get("competition_qid")}
            comp_qids = list(comp_qids - {None})
            for i in range(0, len(comp_qids), 200):
                batch = comp_qids[i:i + 200]
                qid_sql = ", ".join(escape_sql(q) for q in batch)
                rows = query_d1(
                    f"SELECT external_id AS qid, reep_id FROM provider_ids "
                    f"WHERE provider = 'wikidata' AND external_id IN ({qid_sql});",
                    remote=remote,
                )
                for r in rows:
                    comp_qid_to_reep[r["qid"]] = r["reep_id"]
            if comp_qids:
                print(f"  Resolved {len(comp_qid_to_reep):,}/{len(comp_qids):,} competition QIDs to reep_ids")

        # Step 5: Update D1
        print(f"  Generating SQL...")
        stmts = generate_update_sql(entities, entity_type, existing_reep_ids, team_qid_to_reep, comp_qid_to_reep)
```

- [ ] **Step 6: Test with dry run**

Run: `python scripts/incremental-update.py --dry-run`

Expected: Shows changed QID counts for all 5 entity types including competition and season.

- [ ] **Step 7: Commit**

```bash
git add scripts/incremental-update.py
git commit -m "feat: extend incremental update pipeline for competitions and seasons"
```

---

## Task 6: Worker API — Add `competition_reep_id` to Responses and FTS Exclusion

**Files:**
- Modify: `src/worker.ts:178-193` (handleSearch FTS query)
- Modify: `src/worker.ts:289` (ENTITY_COLS)

- [ ] **Step 1: Add `competition_reep_id` to ENTITY_COLS**

Change line 289:

```typescript
const ENTITY_COLS = "reep_id, type, name_en, aliases_en, full_name, date_of_birth, nationality, position, current_team_reep_id, height_cm, country, founded, stadium, source, competition_reep_id";
```

This automatically propagates to `/lookup`, `/batch/lookup`, and `/resolve` responses via `lookupByReepId`.

- [ ] **Step 2: Exclude seasons from default FTS search**

In `handleSearch` (line 178), add a default exclusion for seasons when no explicit `?type=` filter is provided. After the existing type filter block (lines 187-190):

```typescript
  if (type) {
    query += " AND e.type = ?";
    binds.push(type);
  } else {
    // Exclude seasons from default search — they pollute results with
    // "2024-25 Premier League", "2023-24 Premier League" etc.
    // Use ?type=season explicitly to search seasons.
    query += " AND e.type != 'season'";
  }
```

- [ ] **Step 3: Test locally**

Run: `pnpm exec wrangler dev --remote`

Test in another terminal:
```bash
# Competition lookup
curl -s "http://localhost:8787/search?name=Premier+League" -H "X-Reep-Key: $REEP_BYPASS_KEY" | python -m json.tool

# Verify seasons excluded from default search
curl -s "http://localhost:8787/search?name=Premier+League&type=season" -H "X-Reep-Key: $REEP_BYPASS_KEY" | python -m json.tool

# Stats should show new types
curl -s "http://localhost:8787/stats" -H "X-Reep-Key: $REEP_BYPASS_KEY" | python -m json.tool
```

- [ ] **Step 4: Commit**

```bash
git add src/worker.ts
git commit -m "feat: add competition_reep_id to API responses, exclude seasons from default FTS"
```

---

## Task 7: CSV Export — Add `competitions.csv` and `seasons.csv`

**Files:**
- Modify: `scripts/export-csv.py:27-117` (column definitions)
- Modify: `scripts/export-csv.py:153-261` (export functions)
- Modify: `scripts/export-csv.py:264-318` (main)

- [ ] **Step 1: Add column definitions**

After `NAME_COLUMNS` (line 117), add:

```python
COMPETITION_COLUMNS = [
    "reep_id",
    "key_wikidata",
    "name",
    "country",
    # Provider IDs
    "key_transfermarkt",
    "key_fbref",
    "key_opta",
]

SEASON_COLUMNS = [
    "reep_id",
    "key_wikidata",
    "name",
    "competition_reep_id",
]
```

- [ ] **Step 2: Add export functions**

After `export_names` (line 234), add:

```python
def export_competitions(competitions: list[dict], out_path: Path,
                        custom_ids: dict[str, dict[str, str]] | None = None,
                        reep_id_map: dict[str, str] | None = None):
    """Export competitions to competitions.csv."""
    custom_ids = custom_ids or {}
    reep_id_map = reep_id_map or {}
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COMPETITION_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for entity in sorted(competitions, key=lambda e: e.get("name_en", "")):
            reep_id = reep_id_map.get(f"{entity['qid']}:competition", "")
            row = {
                "reep_id": reep_id,
                "key_wikidata": entity["qid"],
                "name": entity.get("name_en", ""),
                "country": entity.get("country") or "",
            }

            ids = entity.get("external_ids", {})
            for provider, ext_id in ids.items():
                col = f"key_{provider}"
                if col in COMPETITION_COLUMNS:
                    row[col] = ext_id

            custom_key = reep_id or f"{entity['qid']}:competition"
            for provider, ext_id in custom_ids.get(custom_key, {}).items():
                col = f"key_{provider}"
                if col in COMPETITION_COLUMNS and col not in row:
                    row[col] = ext_id

            writer.writerow(row)

    return len(competitions)


def export_seasons(seasons: list[dict], out_path: Path,
                   custom_ids: dict[str, dict[str, str]] | None = None,
                   reep_id_map: dict[str, str] | None = None):
    """Export seasons to seasons.csv."""
    custom_ids = custom_ids or {}
    reep_id_map = reep_id_map or {}
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SEASON_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        for entity in sorted(seasons, key=lambda e: e.get("name_en", "")):
            reep_id = reep_id_map.get(f"{entity['qid']}:season", "")
            row = {
                "reep_id": reep_id,
                "key_wikidata": entity["qid"],
                "name": entity.get("name_en", ""),
                "competition_reep_id": "",  # Populated from D1, not JSON
            }

            custom_key = reep_id or f"{entity['qid']}:season"
            for provider, ext_id in custom_ids.get(custom_key, {}).items():
                col = f"key_{provider}"
                if col in SEASON_COLUMNS and col not in row:
                    row[col] = ext_id

            writer.writerow(row)

    return len(seasons)
```

- [ ] **Step 3: Update `main()` to load and export new types**

In `main()` (line 264), add loading and exporting:

After loading coaches (line 282):
```python
    # Load competition and season entities (if available)
    comp_path = source / "competitions.json"
    season_path = source / "seasons.json"
    competitions = load_json(comp_path) if comp_path.exists() else []
    seasons = load_json(season_path) if season_path.exists() else []
    if competitions:
        print(f"Loaded: {len(competitions)} competitions")
    if seasons:
        print(f"Loaded: {len(seasons)} seasons")
```

After the teams export (line 296):
```python
    if competitions:
        n_comp = export_competitions(competitions, OUTPUT_DIR / "competitions.csv", custom_ids, reep_id_map)
        print(f"Exported {n_comp} competitions to data/competitions.csv")

    if seasons:
        n_seasons = export_seasons(seasons, OUTPUT_DIR / "seasons.csv", custom_ids, reep_id_map)
        print(f"Exported {n_seasons} seasons to data/seasons.csv")
```

Update the names export (line 298) to include competitions and seasons:
```python
    n_names = export_names(players + teams + coaches + competitions + seasons, OUTPUT_DIR / "names.csv")
```

Update the metadata counts (line 303-309):
```python
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Wikidata SPARQL + custom verified mappings",
        "counts": {
            "people": n_people,
            "teams": n_teams,
            "competitions": len(competitions),
            "seasons": len(seasons),
            "aliases": n_names,
            "custom_ids": sum(len(v) for v in custom_ids.values()),
        },
    }
```

- [ ] **Step 4: Test export**

Run: `python scripts/export-csv.py`

Expected: If `data/json/competitions.json` and `data/json/seasons.json` exist (from Task 4 test run), they produce `data/competitions.csv` and `data/seasons.csv`. If not, the script skips them gracefully.

- [ ] **Step 5: Commit**

```bash
git add scripts/export-csv.py
git commit -m "feat: add competitions.csv and seasons.csv exports"
```

---

## Task 8: Documentation Updates

**Files:**
- Modify: `CLAUDE.md`
- Modify: `cli/reep.py:5, 29-35, 261, 275, 292`
- Modify: `openapi.yaml`

- [ ] **Step 1: Update CLAUDE.md identity model table**

Change the reserved rows to active:

```markdown
| `reep_l` | competition | `reep_l3a8f01bc` |
| `reep_s` | season | `reep_s7d2e49a0` |
| `reep_m` | match | reserved |
```

Update the D1 Tables section to mention that `entities` now includes competitions and seasons, and that `competition_reep_id` is the season→competition FK.

Add to the Scripts table:
```markdown
| `scripts/research-competitions.py` | SPARQL exploration for competition/season coverage |
```

- [ ] **Step 2: Update CLI type choices**

In `cli/reep.py`, update line 5:
```python
Look up football player, team, coach, competition, and season IDs across providers.
```

Update the `--type` choices at lines 261, 275, 292:
```python
    p_search.add_argument("--type", choices=["player", "team", "coach", "competition", "season"])
```
```python
    p_lookup.add_argument("--type", choices=["player", "team", "coach", "competition", "season"])
```
```python
    p_local.add_argument("--type", choices=["player", "team", "coach", "competition", "season"])
```

- [ ] **Step 3: Update openapi.yaml**

Add `competition` and `season` to the type enum. Add `competition_reep_id` as a nullable string field in the entity response schema.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md cli/reep.py openapi.yaml
git commit -m "docs: update identity model, CLI, and OpenAPI for competitions and seasons"
```

---

## Task 9: Workflow — Extend GitHub Action

**Files:**
- Modify: `.github/workflows/update-register.yml`

- [ ] **Step 1: Update validation step for new types**

In the "Validate fetch output" step (line 77-98), add competitions and seasons to the validation (for full mode):

```python
          for name in ['players', 'teams', 'coachs', 'competitions', 'seasons']:
              path = data_dir / f'{name}.json'
              if not path.exists():
                  # Competitions and seasons are optional for now
                  if name in ('competitions', 'seasons'):
                      print(f'  {name}: not found (optional)')
                      continue
                  print(f'MISSING: {path}')
                  exit(1)
              count = len(json.load(open(path)))
              print(f'  {name}: {count:,}')
              total += count
```

- [ ] **Step 2: Update sample CSV regeneration**

In the "Regenerate sample CSVs" step (line 142-147), add new CSVs:

```bash
          for f in data/people.csv data/teams.csv data/names.csv data/competitions.csv data/seasons.csv; do
            base=$(basename "$f")
            if [ -f "$f" ]; then
              (head -1 "$f"; tail -n +2 "$f" | awk 'BEGIN{srand(42)} {print rand() "\t" $0}' | sort -n | head -50 | cut -f2-) > "data/samples/$base"
            fi
          done
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/update-register.yml
git commit -m "ci: extend weekly workflow for competition and season entity types"
```

---

## Task 10: Deploy and Verify

**Files:** None (deployment + verification)

- [ ] **Step 1: Deploy the worker**

Run: `pnpm exec wrangler deploy`

Expected: Successful deployment with the new `ENTITY_COLS` and FTS exclusion.

- [ ] **Step 2: Verify API with test data**

If competitions/seasons have been seeded (from the incremental update or a test run):

```bash
# Check stats for new type counts
curl -s "https://reep-api.rahulkeerthi2-95d.workers.dev/stats" -H "X-Reep-Key: $REEP_BYPASS_KEY" | python -m json.tool

# Search for a known competition
curl -s "https://reep-api.rahulkeerthi2-95d.workers.dev/search?name=Premier+League" -H "X-Reep-Key: $REEP_BYPASS_KEY" | python -m json.tool

# Verify seasons excluded from default search
curl -s "https://reep-api.rahulkeerthi2-95d.workers.dev/search?name=Premier+League&type=season" -H "X-Reep-Key: $REEP_BYPASS_KEY" | python -m json.tool
```

- [ ] **Step 3: Run a real incremental update**

Run: `python scripts/incremental-update.py`

Expected: Processes all 5 entity types. Competition and season counts appear in the output.

- [ ] **Step 4: Export and verify CSVs**

Run:
```bash
python scripts/fetch-custom-ids.py
python scripts/export-csv.py
```

Expected: `data/competitions.csv` and `data/seasons.csv` are generated alongside existing CSVs.
