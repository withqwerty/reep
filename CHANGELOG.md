# Changelog

## 2026.19 - 2026-04-10

### Scripts
- `incremental-update.py`: `build_scoped_ids_query` was silently out of sync with the Phase 1 query builders in `fetch-wikidata-entities.py`, so the scheduled Monday incremental refresh was bypassing this session's Phase 1 improvements (multi-language label fallback, description capture, team category classification). The scoped query now matches the shared shape: it captures `?eLabel` + `?eDescription` + `?typeQid` (for teams) and uses the same `LABEL_LANGS` 30-language chain. `LABEL_LANGS` is now imported from the shared module alongside the other shared constants. Note: the scoped query still doesn't replicate the expanded team SPARQL UNION (`#1` — `?type (wdt:P279)* wd:Q103229495` and the `P641=Q2736 AND sports club` path) because the VALUES clause already restricts the result set to specific QIDs — a full refresh is still required to import entities that are newly reachable via the expanded filter (e.g. the real Q7156 Futbol Club Barcelona and Q170703 Boca Juniors).

## 2026.18 - 2026-04-10

### Scripts
- `fetch-wikidata-entities.py`: four Phase 2 enrichment additions. All take effect on the next weekly refresh and add new fields to `data/json/{type}s.json`; existing consumers see the additions as new optional keys.
  - **DOB precision captured.** Phase 2 player + coach queries now read `p:P569/psv:P569/wikibase:timePrecision` alongside the date value. Each entity with a DOB now carries a `date_of_birth_precision` field: `"day"` (precision 11), `"month"` (10), or `"year"` (9). Year-precision stubs (`YYYY-01-01`) are no longer indistinguishable from real January 1 births — match scripts can now use this to pick the right fallback path.
  - **Multi-language label fallback.** The SPARQL label service was configured for English only, so any entity without an English label had its QID stored as the name (`Q82595` → the kind of broken name `backfill-broken-names.py` used to chase). The label service now accepts a 30-language chain (`en,es,pt,ca,de,fr,it,nl,da,sv,no,fi,pl,cs,sk,hr,sr,ro,hu,uk,ru,tr,ja,ko,zh,ar,he,fa,id,vi,th`) and returns the first available, falling back from English through Romance/Germanic then Slavic/Turkic then Asian/Arabic. The existing QID-as-name drop in `parse_ids_phase` stays as the final safety net.
  - **Multi-position capture.** Player P413 (position) was being stored as the first value only. `merge_bio` now accumulates all positions across the Phase 2 result rows and joins them into a comma-separated `position` field. Players who play both forward and winger (or midfielder and striker) no longer lose that information.
  - **Native name (P1559) captured.** Phase 2 player + coach queries now fetch `wdt:P1559` (name in native language) and store it as `name_native`. This is the Wikidata equivalent of the salimt dataset's `name_in_home_country` field that the TM sync script uses for transliteration matches, so reep now has native names for Eastern European, Asian, and Arabic players without needing the salimt bridge.

## 2026.17 - 2026-04-10

### Scripts
- `fetch-wikidata-entities.py`: operational hygiene pass.
  - **Narrower exception handling in `sparql_query`.** The retry loop was catching a blanket `Exception`, which swallowed logic bugs, attribute errors, and type errors along with the transient network/JSON failures it was meant to recover from. Now catches only `OSError` (network family: URLError, ConnectionError, TimeoutError, etc.) and `json.JSONDecodeError` / `KeyError` (malformed responses). Anything else propagates immediately so real bugs surface loudly.
  - **Run-delta metrics.** Each per-type save now reads the previous `data/json/{type}s.json`, compares it to the new entity list, and prints a one-line summary: `+added / -removed (bio changes: N, ID changes: M)`. A final per-type summary table at the end of the run gives a single glance at what changed. Covers name_en / DOB / nationality / position / height / country / founded / stadium / aliases / full_name / description_en / team_category / external_ids. Would have caught the 41K `reep_id_map` deficit from a partial fetch during the run itself instead of surfacing downstream.
  - **Removed dead `parse_tsv_results` function** (47 lines). The script only uses JSON format these days — the TSV parser hadn't been called by anything for a while.

## 2026.16 - 2026-04-10

### Scripts
- `fetch-wikidata-entities.py`: four correctness and coverage fixes. All take effect on the next weekly refresh.
  - **Team SPARQL filter expanded** to cover three paths: `subclass-of Q476028` (association football club), `subclass-of Q103229495` (men's association football team), and `P641 = Q2736 AND subclass-of Q847017` (sports club with football as sport). Previously only the first path was covered, which missed many well-known clubs (including Q7156 Futbol Club Barcelona and Q170703 Boca Juniors) and smaller regional clubs that Wikidata classifies generically.
  - **Description-based label disambiguation.** Phase 1 now captures `?eDescription` alongside `?eLabel`. Entities that share the same English label within a type get `(description)` appended to their `name_en` so consumers can tell them apart. SPARQL label-service "Q{number}" fallbacks (when no English label exists) are dropped at ingest rather than stored as broken names.
  - **Team category field** on team entities: `senior_men` / `women` / `beach_soccer` / `futsal` / `youth` / `reserve` — derived from the P31 chain, defaulting to `senior_men` when no specific category matches. Stored in `teams.json`; not yet surfaced in the D1 schema.
  - **Seasons excluded from competition fetch.** Entities with `P3450` (sports season of league) no longer leak into the competition result set — they're handled by the separate season fetch. Prevents seasonal entities inheriting their parent competition's Transfermarkt code.
- `fetch-wikidata-entities.py`: removed `transfermarkt_player` from `COACH_IDS`. P2446 on coach entities (dual-career players turned managers) was being stored under a coach-only provider name that nothing else queries. 239 orphan rows deleted from D1.

### Data quality fixes
- **CA Boca Juniors (women) → CA Boca Juniors (beach soccer)** — the Boca stub renamed in 2026.15 was re-classified after the new team category work: P31 = Q116953048 is *beach soccer club*, not a women's team.

## 2026.15 - 2026-04-10 (data-only)

### Data
- **+14,944 entities** now appear in the public CSVs that had been created directly in D1 by match scripts since v2.3.0 but were never carried back into the intermediate JSON files consumed by `export-csv.py`: 2,023 Transfermarkt-sourced players, 12,899 Opta-sourced players, 8 competitions, 14 new competitions from this release. A new `fetch-custom-entities.py` step now pulls them across every run. `people.csv` grew 429,785 → 444,707; `competitions.csv` grew 191 → 213.
- **7 new Transfermarkt club IDs**: FC Augsburg (167), Argentinos Juniors (1030), Club Atlético Lanús (333), Fredrikstad FK (3837), Unión Magdalena (14680), FC Universitatea Cluj (6429), Kayseri Erciyesspor (6894). All are top-flight clubs whose formal-name variants on Transfermarkt (e.g. `Fußball-Club Augsburg 1907`, `Asociación Atlética Argentinos Juniors`) previously escaped matching.
- **154 new Transfermarkt player IDs** added.
- **149 `position_detail` backfills** and **8,853 new player and club aliases**.
- **15 new competition entities** for domestic cups and UEFA qualifying rounds that were missing from reep entirely — Belgian Super Cup, Danish Cup, Greek Football Cup, KNVB Cup, Johan Cruijff Shield, Taça da Liga, Supertaça Cândido de Oliveira, Russian Cup, Russian Super Cup, Scottish Cup, Ukrainian Cup, Ukrainian Super Cup, and the UEFA Champions/Europa/Conference League qualifying rounds. All three qualifying rounds now have their Transfermarkt codes (CLQ/ELQ/ECLQ).
- **342 `date_of_birth` values** refined from the `YYYY-01-01` Wikidata year-precision stub to the specific day, where an external source confirmed the year.

### Data quality fixes
- **Three famous clubs split from Wikidata-fallback stubs.** reep had been serving `FC Barcelona` (Q5424838, actually FC Barcelona Beach Soccer), `CA Boca Juniors` (Q5008937, a smaller Boca entity), and `Club Atlético Estudiantes` (Q118951641, Estudiantes de Resistencia) under names that look like the main men's clubs — because the SPARQL label service returned the bare parent-club string for these auxiliary entities. The real men's clubs (`Q7156` Futbol Club Barcelona, `Q170703` Boca Juniors, `Q8206935` Estudiantes de Río Cuarto) were missing from reep entirely. All three real clubs are now present with proper founded dates (1899 / 1905 / 1912), stadium (Camp Nou, La Bombonera), country, aliases, and Transfermarkt IDs (131 / 189 / 14602). The three stub entities were renamed with disambiguating suffixes (`(beach soccer)`, `(women)`, `(Resistencia)`).
- **AFCN entity renamed** from `Afrique corrompu fédération` (a stale Wikidata label that has since been corrected upstream) to `Africa Cup of Nations`.
- **Duplicate SEN1 Transfermarkt code removed** from a seasonal entity (2025–26 Senegal Ligue 1) that had inherited its parent competition's code. The canonical mapping stays on the parent Senegal Premier League entity.
- **2 corrupt Transfermarkt external_ids repaired.** One player entity had its player name stored in the ID field; one club entity had a full Transfermarkt URL stored instead of the numeric ID. Both came from upstream Wikidata P2446 data-entry errors. The club case was repaired to the correct numeric ID parsed from the URL path; the player case was deleted because no valid TM ID was recoverable.

### Scripts
- `fetch-custom-entities.py` (new): pulls entities that were created directly in D1 by non-Wikidata sources and merges them into `data/json/*.json` so `export-csv.py` picks them up. Runs after `fetch-custom-ids.py` in the standard pipeline. Idempotent. Fills the long-standing gap where match-script-created entities lived in D1 but never reached the public CSVs.
- `fetch-custom-ids.py`: hardened against silent partial-data failures. A transient API error mid-pagination used to return an empty result set that the caller couldn't distinguish from legitimate end-of-data, so the script would write an incomplete JSON file and exit with code 0. `query_d1` now raises a `QueryError` on subprocess non-zero exit, unparseable JSON, or unexpected response shape, with automatic retry + exponential backoff for transient failures. A new `paginated_fetch` helper wraps every paginated loop, takes an expected total, and refuses to write if the actual row count doesn't match. Main wraps everything in `try / except QueryError → sys.exit(1)`. File writes are atomic (temp + rename).
- `export-csv.py`: `_resolve_reep_id` and the Source 1 alias export loop now both short-circuit when the `qid` slot already holds a `reep_*` identifier — the fallback path for custom entities that have no Wikidata QID.

## v2.4.0 - 2026-04-07

### API (breaking)
- **Remove `fpl_code` provider.** Replaced by `opta_numeric` in v2.2.0. Lookups with `provider=fpl_code` now return `unknown_provider`. Migrate to `provider=opta_numeric` — same IDs, same entities.
- Add `optacore` to `VALID_PROVIDERS` — Opta's core numeric competition IDs (47 competitions). Separate numbering system from `opta_numeric`.

### Data
- **941 new `opta_numeric` player IDs** from Opta Web Archive packed feeds (2016–2025). Matched via DOB+position → Opta CSV → TM/FBref/UUID bridge. Total opta_numeric: 4,180 (was 3,239).
- **63,436 anonymous opta_numeric IDs extracted** from 1,219 archived feeds (F1 match results, F40 squad rosters, F3 standings) across 217 competitions. Stored in `data/opta-numeric/` for future matching as more bridges become available.

### Scripts
- `reep-custom/scripts/decode_opta_packed.py`: TEA decryption + F1/F3/F40 binary parsers for Opta packed feeds
- `reep-custom/scripts/batch-decode-f1.py`: batch decode 878 F1 packed feeds from CDX
- `reep-custom/scripts/batch-decode-opta.py`: batch decode F3 (366) and F40 (141) packed feeds
- `reep-custom/scripts/batch-decode-f1-json.py`: batch decode 52 non-packed F1 JSON feeds
- `reep-custom/scripts/match-opta-archive.py`: match extracted IDs to Reep via CSV bridge

## v2.3.0 - 2026-04-07

### API
- Add `capology` to `VALID_PROVIDERS` — salary/wage data provider. 1,044 team IDs (country-verified name matching). Player IDs pending crowdsourced validation.
- Add `position_detail` field to all entity responses (`/search`, `/lookup`, `/resolve`, `/batch/*`). Contains Transfermarkt's granular sub-position (e.g. `Centre-Back`, `Attacking Midfield`, `Right Winger`). Null for entities without TM position data. The existing `position` field remains unchanged as the coarse category.

### Data
- **1,044 Capology team IDs** added (country-verified name matching)
- **42,377 `position_detail` values** backfilled from [dcaribou/transfermarkt-datasets](https://github.com/dcaribou/transfermarkt-datasets) across 13 TM sub-positions
- **2,023 new player entities** from transfermarkt-datasets (active 2024-25 squad players not in Wikidata)
- **1,756 Transfermarkt player IDs** added as custom_ids (DOB+name matching)
- **30 Transfermarkt competition codes** matched to Reep competitions (alpha codes like GB1, ES1, L1)
- **5 Transfermarkt club IDs** matched

### Scripts
- `reep-custom/scripts/sync-transfermarkt-datasets.py`: weekly sync from TM DuckDB. Downloads from R2, matches players (DOB+name), competitions (alias+country), clubs (name). Creates new entities for unmatched active players. Backfills `position_detail` for all entities with TM IDs.
- `reep-custom/scripts/match-capology.py`: matches Capology player/team slugs to Reep entities. Teams write directly; players export CSV for validator.

## 2026.14 - 2026-04-05 (data-only)

### Data quality
- Backfilled `name_en` for **24,149 entities** that had broken `Q12345`-format names (99.96% of 24,157). The Wikidata `wikibase:label` service returns the QID as a fallback when no English label exists, and those fallbacks had been stored as entity names. Fix used a 3-pass approach: local Wikidata dump scan (23,498 entities), live API pass for post-dump additions (648), extended fallback chain (P1448/P1705/P1559 claims + sitelinks) for stubborn cases (1), and deletion of the 8 remaining entities which had no labels in any Wikidata language AND no provider coverage beyond Wikidata itself. Full methodology in #10.
- Top restored names: Bundesliga (Q82595), Serie A (Q15804), ~11K German/Dutch/Spanish/Arabic football clubs, and thousands of Ukrainian/Russian/Korean players. Unlocks name-based matching on FotMob/TheSportsDB/Opta/Understat for previously invisible entities.

### Scripts
- `reep-custom/scripts/backfill-broken-names.py`: re-runnable Wikidata dump scanner for future Q-label cleanup

## v2.2.0 - 2026-04-05

### API
- Add `opta_numeric` to `VALID_PROVIDERS`. Splits Opta's two distinct ID systems into separate providers:
  - **`opta`** — alphanumeric UUID format (Stats Perform F1 / The Analyst). Used across players (50K), teams (34 PL historical), competitions (33), and seasons (14). This is the canonical Opta provider.
  - **`opta_numeric`** — legacy numeric codes from Wikidata P8735 (52 competitions, e.g. PL=8, La Liga=23). Separate because the two ID systems are not interchangeable and both remain in use.
- Migrated previously misfiled data: 52 numeric competition codes moved from `opta` → `opta_numeric`; 34 The Analyst competition UUIDs + 4 calendar UUIDs moved from provisional `opta_analytics` → `opta`.

### Data
- 2,225 TheSportsDB team IDs added (strict country+name matching, reserves/women/youth filtered)
- 147 Understat competition/team/season IDs added from Kaggle dataset (6 leagues, 100 teams, 41 seasons)
- 98 FotMob competition IDs (FBref-bridged country-aware matching + tier-3 manual review)
- 41 Opta UUID mappings added (31 historical PL teams + 10 historical PL seasons from `/Volumes/WQ/projects/www/src/data/opta-*`)
- 34 Opta competition UUIDs (from The Analyst power rankings)
- 19 Transfermarkt competition IDs (curated from dcaribou/transfermarkt-datasets)
- 466 FBref season IDs (derived from worldfootballR season URLs)
- teams.csv: add `key_thesportsdb`, `key_understat` columns (removed provisional `key_opta_analytics`)

## v2.1.0 - 2026-04-05

### API
- Add `GET /health` endpoint (public, no auth) for uptime monitoring. Returns 200 with `{status, version, db, timestamp}` when healthy, 503 if D1 is unreachable. Responses are not cached (`Cache-Control: no-store`).

## v2.0.0 - 2026-04-05

The initial public release of the Reep Register as a versioned product.

### API
- `reep_id` is now the canonical primary key (replaces Wikidata QID)
- Endpoints: `/search`, `/lookup`, `/resolve`, `/stats`, `/batch/lookup`, `/batch/resolve`
- 43 providers supported across Wikidata-sourced and custom-verified mappings
- Provider validation: `/resolve` and `/batch/resolve` reject unknown provider names with 400
- Full-text search with BM25 ranking, prefix matching, diacritics-insensitive
- Constant-time auth comparison, fail-closed when unconfigured
- Entity types: player, team, coach, competition, season

### Data (2026.14)
- 490,922 entities (399K players, 45K teams, 44K coaches, 269 competitions, 2,502 seasons)
- 353,266 custom verified IDs across 14 providers
- 1.7M Wikidata-sourced provider ID mappings
- Non-football competition filter active
- Weekly incremental Wikidata refresh (Monday 04:00 UTC)
- Monthly dump reconciliation workflow operational

### Infrastructure
- Reep ID migration complete (from QID composite key to self-minted `reep_<type><8hex>`)
- Incremental Wikidata update pipeline (replaces full seed for weekly runs)
- D1 time-travel recovery (30-day rollback window)
- Dump reconciliation: entity adds, ID corrections, remove candidates, occupation-lost tracking
