# Competitions & Seasons in Reep

**Date:** 2026-04-04
**Status:** Draft
**Scope:** Add competition and season entity types to the Reep register

## 1. Scope

### In scope

- `competition` (`reep_l`) — recurring football competitions (leagues, cups, tournaments, super cups)
- `season` (`reep_s`) — one edition/instance of a competition
- Cross-provider ID resolution for both types
- Season → competition relationship
- Wikidata coverage research phase before committing to sourcing strategy
- FTS search support for competitions (not seasons — see §7)

### Out of scope

- Matches (`reep_m`) — deferred to future work that includes match events, metadata, and potentially separate storage
- Type-specific detail tables (country, tier, format, dates) — schema designed to allow these later without migration
- Non-football competitions

### Definitions

- **Competition**: a recurring football event — league, cup, tournament, super cup, friendly series. Examples: Premier League, FA Cup, UEFA Champions League, Community Shield.
- **Season**: one edition of a competition. Examples: 2024-25 Premier League, 2024-25 FA Cup. The term "season" is canonical in the schema and API (`type=season`, prefix `reep_s`). "Edition" is an informal alias used in human-facing contexts where "season" reads awkwardly (cups, one-off events).

## 2. Entity Model

Both types live in the existing `entities` table. Competitions and seasons share the same universal columns as players/teams/coaches:

- `reep_id` (PK) — `reep_l<8hex>` for competitions, `reep_s<8hex>` for seasons
- `name_en` — "Premier League", "2024-25 Premier League"
- `type` — `"competition"` or `"season"`
- `source`, `created_at`, `updated_at`

Person/team-specific columns (`date_of_birth`, `nationality`, `position`, `height_cm`, `country`, `founded`, `stadium`, `current_team_reep_id`) will be NULL for these types — same as they already are for irrelevant entity types.

Provider ID mappings use the existing `provider_ids` and `custom_ids` tables unchanged — they are entity-type-agnostic.

### Season → Competition relationship

A new column on `entities`:

```sql
ALTER TABLE entities ADD COLUMN competition_reep_id TEXT;
```

NULL for all types except season. References a competition entity's `reep_id`.

**Trade-off acknowledged:** This adds a type-specific nullable FK column to a universal table. An alternative is an `entity_relationships` table (`parent_reep_id, child_reep_id, relationship_type`). The column approach is chosen because:

1. Only one relationship exists today (season → competition). A relationship table is premature.
2. The column is directly queryable: `SELECT * FROM entities WHERE type = 'season' AND competition_reep_id = ?`
3. If matches later introduce multiple FKs (home_team, away_team, season, competition), that's a different entity type with different storage needs — matches are already scoped out and may not use the `entities` table at all.
4. Migration path if we change our mind: create the relationship table, backfill from the column, drop the column. Low reversal cost.

## 3. Identity

Same minting scheme as existing entities:

| Prefix | Type | Example |
|--------|------|---------|
| `reep_l` | competition | `reep_l3a8f01bc` |
| `reep_s` | season | `reep_s7d2e49a0` |

Wikidata QIDs are a provider mapping (`provider=wikidata` in `provider_ids`), not the identity backbone — consistent with the existing model.

## 4. Research Phase

Before building any pipeline, run exploratory SPARQL queries to assess Wikidata coverage. This is a **data quality exercise**, not a go/no-go gate — the schema, API, and pipeline architecture are the same regardless of outcome.

### Competitions

- Query instances of `Q15991290` (association football competition) and subclasses
- Count total items
- Count items with provider IDs: P12758 (Transfermarkt), P13664 (FBref), P8735 (Opta)
- Assess: top 5 leagues? All UEFA members? Cups? Lower tiers? Women's competitions?

### Seasons

- Query `Q3919108` (association football league season) and items with `P3450` (sports season of league/competition)
- Count total, assess historical depth
- **Critical question:** do season items have *any* dedicated provider ID properties, or are season IDs purely derived (competition ID + year in URL schemes)?

### Comparison

- Pull FBref's competition/season index as a baseline
- Measure: what percentage of FBref competitions have Wikidata items?

### What the research decides

- **If Wikidata has reasonable competition coverage with provider IDs:** Wikidata seeds competitions through the existing pipeline. Custom_ids fill gaps.
- **If Wikidata competition coverage is sparse:** A non-Wikidata source (FBref, Transfermarkt) becomes the primary seed, with Wikidata as just another provider mapping — same pattern as `import-opta-entities.py`.
- **For seasons specifically:** Research is expected to confirm that season provider IDs are mostly derived, not stored as Wikidata properties. This determines whether seasons need a custom ID derivation strategy before they can ship as useful (see §5).

## 5. Season Provider IDs — The Hard Part

Competition cross-referencing is straightforward: Wikidata has P12758 (Transfermarkt), P13664 (FBref), P8735 (Opta) on competition items. Sparse but real.

Season cross-referencing is harder. Most providers derive season IDs from competition + year:

- FBref: `/en/comps/9/2024-2025/` (competition ID + season slug)
- Transfermarkt: `/premier-league/startseite/wettbewerb/GB1/plus/?saison_id=2024`
- Opta: competition ID + season year as separate parameters
- WhoScored: similar derivation from competition + year

If Wikidata has no dedicated season provider properties (likely), then:

1. `provider_ids` for seasons will be nearly empty from the SPARQL pipeline
2. The cross-referencing value of seasons depends entirely on `custom_ids`
3. Populating `custom_ids` for seasons requires derivation logic: given a competition's provider IDs and a season's year range, construct the provider-specific season ID

### Sequencing implication

**Competitions can ship as a standalone useful feature.** They have Wikidata provider IDs and the `/resolve` endpoint works immediately.

**Seasons ship useful only once a custom ID strategy exists.** Options:

- **A)** Ship seasons as entities (minted reep_ids, FKs to competitions) but accept that `/resolve` returns nothing for them until custom_ids are populated. Seasons are discoverable via the competition FK but not cross-referenceable yet.
- **B)** Build a season ID derivation script as part of the initial work — given a competition's provider IDs and a season year, generate the expected season IDs for each provider. This is bespoke per provider but formulaic.
- **C)** Defer seasons entirely until the derivation strategy is built.

**Recommendation: A, with B as fast follow.** Ship the schema and seed seasons from Wikidata so the identity backbone exists. Then build derivation scripts in reep-custom to populate cross-references. This matches how players/teams evolved — Wikidata first, custom_ids layered on.

## 6. Pipeline Changes

### `fetch-wikidata-entities.py`

- Add `COMPETITION_PROVIDERS` dict (P12758, P13664, P8735, plus any discovered in research)
- Add `SEASON_PROVIDERS` dict (TBD from research, may be empty)
- Add `build_competition_ids_query()` — instances of Q15991290 and subclasses
- Add `build_season_ids_query()` — items with P3450 (sports season of league)
- Both follow the existing two-phase pattern (IDs then bio)

### `incremental-update.py`

This is the production weekly path. Changes needed:

- `TYPE_PREFIXES` — add `"competition": "l"`, `"season": "s"`
- `TYPE_CONFIGS` — add entries with competition/season provider dicts and bio query builders
- `ENTITY_COLS` — add `competition_reep_id`
- `fetch_changed_qids()` — add SPARQL queries for changed competitions/seasons
- Season processing must resolve competition QID → `reep_id` (same pattern as existing team QID → `reep_id` resolution for `current_team_reep_id`)
- **Ordering:** Process competitions before seasons in the per-type loop so competition reep_ids exist when seasons need them

### `seed-wikidata-d1.py`

The full seed script has a stale DDL (still uses `PRIMARY KEY (qid, type)` instead of `reep_id` PK). This is a pre-existing issue. For this work:

- Add competition/season to `types` list and JSON file handling
- Update hardcoded DDL to include `competition_reep_id` column
- **Note:** The full seed's stale DDL should be fixed separately — it would regress the schema if run today regardless of this feature.

### `enrich-wikidata-bio.py`

- Extend to handle competition/season types
- For seasons: extract P3450 (competition link) to populate `competition_reep_id` during enrichment

### `export-csv.py`

- New CSV exports: `competitions.csv`, `seasons.csv`
- Seasons CSV includes `competition_reep_id` column
- Merged from both `provider_ids` and `custom_ids` (same as existing CSVs)

### `fetch-custom-ids.py`

No changes — already type-agnostic.

## 7. FTS Search

**Problem:** Including seasons in FTS pollutes search results. Searching "Premier League" would return the competition entity plus dozens of season entities ("2024-25 Premier League", "2023-24 Premier League", etc.), degrading the primary use case (finding players/teams).

**Solution:** Include competitions in FTS but **exclude seasons**. Seasons are discoverable via the competition FK (`WHERE type = 'season' AND competition_reep_id = ?`), not via free-text search.

Implementation: The FTS triggers fire on all `entities` inserts. Either:
- Filter in the trigger: `WHEN new.type != 'season'`
- Or filter at query time in the worker: add `AND type != 'season'` to FTS queries when no explicit `?type=` filter is passed

Worker-side filtering is simpler and doesn't require trigger changes.

## 8. Worker API Changes

### Automatic (no code changes)

- `/lookup` — handles any `reep_` prefix. `reep_l...` and `reep_s...` work automatically.
- `/resolve` — provider + external_id resolution is type-agnostic.
- `/stats` — count queries pick up new types automatically.

### Required changes

- `/search` — default query (no `?type=` filter) should exclude seasons from FTS results to avoid pollution. `?type=season` explicitly still works.
- `/lookup` response — include `competition_reep_id` field for season entities (new nullable field in response body). NULL for non-season entities.
- `ENTITY_COLS` constant — add `competition_reep_id` to the SELECT column list.

### Nice-to-have (not required for launch)

- `/search?type=season&competition=reep_l...` — filter seasons by competition. Syntactic sugar over a direct D1 query.

## 9. Documentation Updates

- `CLAUDE.md` identity model table — remove "reserved" from `reep_l` and `reep_s` rows
- `CLAUDE.md` commands — add any new scripts
- `openapi.yaml` — add `competition` and `season` to type enum, add `competition_reep_id` field to response schema
- `README.md` — update coverage table with competition/season counts
- `cli/reep.py` — if applicable, add new types to type filter options

## 10. Weekly Workflow

Extend `update-register.yml`:

1. Fetch competition and season entities from Wikidata (new query phases)
2. Enrich bio data for new types
3. Incremental update to D1 (**competitions before seasons** — ordering matters for FK resolution)
4. Existing steps continue: fetch custom_ids, export CSVs, verify counts

Verify counts step should include per-type counts for competitions and seasons.

Workflow timeout (currently 60 minutes) may need increasing — monitor after first run.

## 11. Migration Path

Additive, no breaking changes:

1. `ALTER TABLE entities ADD COLUMN competition_reep_id TEXT` — single DDL
2. No existing data affected (column NULL for all current entities)
3. New type values appear alongside existing ones
4. API response gains a new nullable field — backwards compatible
5. Detail tables can be added later as standalone tables joining on `reep_id`

### Hardcoded schema checklist

These locations must be updated atomically:

- [ ] `incremental-update.py` `ENTITY_COLS` tuple
- [ ] `incremental-update.py` `TYPE_PREFIXES` dict
- [ ] `incremental-update.py` `TYPE_CONFIGS` dict
- [ ] `incremental-update.py` `fetch_changed_qids()` type queries
- [ ] `seed-wikidata-d1.py` schema DDL (if updating the stale seed script)
- [ ] `seed-wikidata-d1.py` `generate_entity_inserts()` column list
- [ ] `src/worker.ts` `ENTITY_COLS` or equivalent SELECT
- [ ] `src/worker.ts` FTS query default exclusion
- [ ] `scripts/export-csv.py` column lists and new CSV outputs

## 12. Sequencing

1. **Research** — SPARQL exploration, coverage assessment, FBref comparison
2. **Schema** — `ALTER TABLE`, update type constraints
3. **Pipeline (competitions)** — SPARQL queries, seed logic, CSV export
4. **Pipeline (seasons)** — SPARQL queries, FK resolution, CSV export
5. **API** — `competition_reep_id` in responses, FTS exclusion for seasons
6. **Documentation** — README, CLAUDE.md, OpenAPI, CLI
7. **Weekly workflow** — extend GitHub Action, verify counts
8. **Custom ID derivation (fast follow)** — season ID derivation scripts in reep-custom

## 13. Open Questions

- What Wikidata classes best capture football competitions? `Q15991290` may be too broad or too narrow — research phase will clarify.
- Are there Wikidata properties for season IDs we haven't found? The excluded-properties list in `fetch-wikidata-entities.py` only covers competitions and matches, not seasons explicitly.
- Should women's competitions be included from the start, or phased in?
- How should we name competitions that exist at multiple tiers (e.g. "Premier League" vs "English Premier League" vs "Barclays Premier League")?
