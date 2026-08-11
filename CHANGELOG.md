# Changelog

All notable changes to the Reep Register API and data are documented here. Versioned releases (v2.x.y) contain API changes; calver releases (YYYY.WW) are data-only.

## Unreleased

### Docs
- Marked this repository as the frozen v0/RapidAPI/D1 surface for the migration
  bridge and pointed new integrations to `reep.football/api`,
  `reep.football/downloads`, and the RapidAPI migration guide.
- Clarified that v0 `reep_...` IDs are not interchangeable with Reep v1 IDs.
- Disclosed that the **data files** are frozen too, not only the API: the last CSV
  release was `2026.25` (21 June 2026) and D1 has taken no writes since
  25 April 2026. Reported faults are fixed in the v1 register, not back-ported.
- Corrected stale record counts in the README data table — `people.csv` (~488K →
  444,707), `competitions.csv` (~336 → 212) and `seasons.csv` (~3.8K → 1,200) were
  all overstated. Counts are now exact rather than approximate.
- Removed remaining claims of ongoing updates: `GET /stats` "live counts", Wikidata
  IDs updating "with each refresh", contributed IDs being served via the v0 API, and
  Wikidata edits being picked up by "the next refresh".

## v2.7.0 - 2026-04-24

### API
- **Stable `reep_id`s with redirect-on-retire.** A Reep ID promoted to prod is guaranteed to keep resolving forever. When an entity is deduplicated or re-canonicalised, its old `reep_id` now redirects to the canonical entity instead of returning not-found. The response body carries the canonical entity's fields with three meta markers attached: `_deprecated: true`, `_canonical_id` (the new canonical `reep_id`), and `_deprecated_at` (ISO timestamp). Provider-ID lookups (e.g. `?id=Q42`) redirect silently — no meta fields, since the client didn't name a specific `reep_id`.
- **410 Gone for retired `reep_id`s with no successor.** Entities removed without a canonical replacement (e.g. upstream confirmed non-football) return a 410 with `_deprecated: true`, `_canonical_id: null`, and `_deprecated_reason: "retired"`.

### Data quality
- **Search results exclude retired entities.** `/search` and any FTS-backed path no longer surface soft-deleted rows.

## v2.6.0 - 2026-04-23

### API
- **Add `match` as a first-class entity type.** Reep IDs with the `reep_m...` prefix can now be looked up and resolved alongside players, teams, coaches, competitions, and seasons.
- **Make `/resolve` and `/batch/resolve` type-aware.** Provider IDs are no longer assumed to be globally unique across entity classes. Both endpoints now accept an optional `type`, and ambiguous cross-type IDs return an explicit ambiguity response instead of silently choosing one entity.
- **Return match metadata on entity responses.** Match lookups now include the fixture date, home/away team Reep IDs, score, round, venue, referee, attendance, and season label when available.

### Data
- **Add partitioned match export support.** Canonical match identity can now be published as narrow, compressed per-competition/per-season CSV partitions plus a separate match-ID mapping export, without bundling any raw event data.

## v2.5.0 - 2026-04-20

### Data
- **API-Football player IDs: +14,054.** Cross-referenced from public TheSportsDB records, accepted only when the date of birth matches the Reep entity. Coverage jumps from Premier League–only to global.
- **ESPN player IDs: +9,948.** Same cross-reference pass, same DOB safeguard.
- **Capology IDs: +31,960.** Auto-confirmed the subset where name uniqueness on both sides plus nationality agreement make the match unambiguous. The residual ambiguous pairs are under community validation.

### Data quality
- All new IDs in this release are gated on date-of-birth exact match before writing. Records without a verifiable DOB or with conflicting DOBs are never auto-confirmed.

## v2.4.1 - 2026-04-13

### Docs
- Rewrote CHANGELOG to be public-facing — removed internal script references, private repo paths, and pipeline implementation details. Entries now focus on API changes, data additions, and data quality fixes.
- README: replaced internal `wrangler d1 execute` commands in Coverage section with a pointer to `GET /stats`. Renamed "Custom" source label to "Verified" for clarity. Removed struck-through Opta property rows (P8736/P8737) from the Wikidata properties table.

## 2026.19 - 2026-04-10

### Data pipeline
- Incremental weekly refresh now uses the same multi-language label fallback and team category classification as the full refresh. Previously, incrementally updated entities could miss these improvements until the next full run.

## 2026.18 - 2026-04-10

### Data
- **DOB precision** now available per entity: `"day"`, `"month"`, or `"year"`. Year-precision dates (`YYYY-01-01`) are no longer ambiguous with real January 1 births.
- **Multi-language label fallback.** Entities without an English name now fall back through a 30-language chain (Romance → Germanic → Slavic → Turkic → Asian → Arabic) instead of storing a raw Wikidata QID as the name.
- **Multi-position capture.** Players with multiple positions (e.g. forward + winger) now have all positions listed, not just the first.
- **Native name** (`name_native`) added for players and coaches — the name in the person's native language/script.

## 2026.17 - 2026-04-10

Internal reliability improvements to the Wikidata refresh pipeline. No user-facing changes.

## 2026.16 - 2026-04-10

### Data
- **Broader team coverage.** The Wikidata ingest now finds clubs classified as generic "sports clubs" with football as their sport, not just those explicitly tagged as football clubs. Adds many well-known clubs that were previously missing, including FC Barcelona and Boca Juniors.
- **Team category** field on team entities: `senior_men`, `women`, `beach_soccer`, `futsal`, `youth`, or `reserve`. Helps distinguish auxiliary squads from the main men's team.
- **Disambiguated entity names.** Entities sharing the same English name within a type now include a description suffix (e.g. "Estudiantes (Resistencia)") so consumers can tell them apart.
- Seasonal entities no longer leak into the competition dataset.
- 239 orphaned coach-specific Transfermarkt mappings removed (dual-career players whose coach entity duplicated the player mapping).

### Data quality
- **CA Boca Juniors** stub re-classified from `(women)` to `(beach soccer)` based on Wikidata type.

## 2026.15 - 2026-04-10

### Data
- **+14,944 entities** added to public CSVs. These entities existed in the API but were missing from the downloadable CSV files. `people.csv`: 429,785 → 444,707. `competitions.csv`: 191 → 213.
- **7 new Transfermarkt club IDs**: FC Augsburg, Argentinos Juniors, Club Atlético Lanús, Fredrikstad FK, Unión Magdalena, FC Universitatea Cluj, Kayseri Erciyesspor.
- **154 new Transfermarkt player IDs.**
- **149 `position_detail` backfills** and **8,853 new aliases.**
- **15 new competitions**: Belgian Super Cup, Danish Cup, Greek Football Cup, KNVB Cup, Johan Cruijff Shield, Taça da Liga, Supertaça Cândido de Oliveira, Russian Cup, Russian Super Cup, Scottish Cup, Ukrainian Cup, Ukrainian Super Cup, UEFA Champions/Europa/Conference League qualifying rounds.
- **342 dates of birth refined** from year-only stubs to exact dates.

### Data quality
- **Three famous clubs corrected.** Reep was previously serving beach soccer / women's / reserve entities under the names "FC Barcelona", "CA Boca Juniors", and "Club Atlético Estudiantes". The real men's clubs are now present with proper metadata (founded dates, stadiums, Transfermarkt IDs). The auxiliary entities have been renamed with disambiguating suffixes.
- **Africa Cup of Nations** restored from a stale Wikidata label (`Afrique corrompu fédération`).
- Duplicate Transfermarkt code removed from a Senegal Ligue 1 seasonal entity.
- 2 corrupt Transfermarkt IDs repaired (upstream Wikidata data-entry errors).

## v2.4.0 - 2026-04-07

### API (breaking)
- **Remove `fpl_code` provider.** Replaced by `opta_numeric` in v2.2.0. Lookups with `provider=fpl_code` now return `unknown_provider`. Migrate to `provider=opta_numeric` — same IDs, same entities.
- **Add `optacore` provider** — Opta's core numeric competition IDs (47 competitions). Separate numbering system from `opta_numeric`.

### Data
- **941 new `opta_numeric` player IDs** from archived Opta feeds (2016–2025). Total opta_numeric players: 3,239 → 4,180.
- **63,436 anonymous opta_numeric IDs extracted** from 1,219 archived feeds across 217 competitions, stored for future matching.

## v2.3.0 - 2026-04-07

### API
- **Add `capology` provider** — salary/wage data. 1,044 team IDs. Player IDs pending crowdsourced validation.
- **Add `position_detail` field** to all entity responses (`/search`, `/lookup`, `/resolve`, `/batch/*`). Contains Transfermarkt's granular sub-position (e.g. `Centre-Back`, `Attacking Midfield`, `Right Winger`). Null for entities without TM data. The existing coarse `position` field is unchanged.

### Data
- **1,044 Capology team IDs** added.
- **42,377 `position_detail` values** backfilled from [transfermarkt-datasets](https://github.com/dcaribou/transfermarkt-datasets) across 13 sub-positions.
- **2,023 new player entities** — active 2024–25 squad players not yet in Wikidata.
- **1,756 Transfermarkt player IDs**, **30 competition codes** (GB1, ES1, L1, etc.), and **5 club IDs** added.

## 2026.14 - 2026-04-05

### Data quality
- **24,149 broken entity names fixed.** Entities whose names were stored as raw Wikidata QIDs (e.g. `Q82595` instead of `Bundesliga`) now have proper names. Covers ~11K German/Dutch/Spanish/Arabic clubs and thousands of Ukrainian/Russian/Korean players. 8 entities with no recoverable name and no provider coverage were removed. Full methodology in [#10](https://github.com/withqwerty/reep/issues/10).

## v2.2.0 - 2026-04-05

### API
- **Add `opta_numeric` provider.** Splits Opta's two distinct ID systems:
  - **`opta`** — 25-char alphanumeric UUIDs (Stats Perform / The Analyst). Players (50K), teams, competitions, seasons.
  - **`opta_numeric`** — legacy numeric codes (e.g. Premier League = 8, La Liga = 23). Not interchangeable with `opta` UUIDs.
- 52 numeric competition codes migrated from `opta` → `opta_numeric`. 34 The Analyst UUIDs + 4 calendar UUIDs migrated from `opta_analytics` → `opta`.

### Data
- 2,225 TheSportsDB team IDs.
- 147 Understat IDs (6 leagues, 100 teams, 41 seasons).
- 98 FotMob competition IDs.
- 41 Opta UUID mappings (31 historical PL teams + 10 PL seasons).
- 34 Opta competition UUIDs.
- 19 Transfermarkt competition IDs.
- 466 FBref season IDs.
- `teams.csv`: added `key_thesportsdb`, `key_understat` columns; removed `key_opta_analytics`.

## v2.1.0 - 2026-04-05

### API
- **Add `GET /health` endpoint** (public, no auth). Returns `{status, version, db, timestamp}` with 200 when healthy, 503 if the database is unreachable.

## v2.0.0 - 2026-04-05

The initial public release of the Reep Register.

### API
- `reep_id` as canonical primary key
- Endpoints: `/search`, `/lookup`, `/resolve`, `/stats`, `/batch/lookup`, `/batch/resolve`
- 43 providers supported
- Full-text search with BM25 ranking, prefix matching, diacritics-insensitive
- Entity types: player, team, coach, competition, season

### Data
- 490,922 entities (399K players, 45K teams, 44K coaches, 269 competitions, 2,502 seasons)
- 353,266 custom verified IDs across 14 providers
- 1.7M Wikidata-sourced provider ID mappings
- Weekly incremental refresh + monthly full reconciliation
