# Changelog

## 2026.15 - 2026-04-10 (data-only)

### Data
- **7 new Transfermarkt club IDs** added: FC Augsburg (167), Argentinos Juniors (1030), Club Atlético Lanús (333), Fredrikstad FK (3837), Unión Magdalena (14680), FC Universitatea Cluj (6429), Kayseri Erciyesspor (6894). All are top-flight clubs whose formal TM names (e.g. `Fußball-Club Augsburg 1907`, `Asociación Atlética Argentinos Juniors`) previously failed the sync script's exact-name matcher.
- **154 new Transfermarkt player IDs** added across two matching paths: 22 recovered from a namespace-collision bug fix (see Scripts), 43 via salimt's `name_in_home_country` native-name bridge (Brazilian/Portuguese nicknames like "Marcão" / "Kaká" / "Rafinha" → full Wikidata legal names), and 89 via a new year-precision placeholder fallback for reep entities whose DOBs are `YYYY-01-01` stubs.
- **149 `position_detail` backfills** + **47 new player aliases** harvested from salimt name variants.

### Scripts
- `reep-custom/scripts/sync-transfermarkt-datasets.py`: major overhaul of club matching and a critical bug fix.
  - **Fixed namespace-collision bug in `load_existing_tm_ids`.** The old loader conflated TM's separate player/team/competition ID namespaces into a single set. Because TM reuses numeric IDs across types (e.g. player 131 = Lars Ricken, club 131 = FC Barcelona), clubs whose ID collided with any reep player/competition were silently skipped. New `load_existing_tm_ids_by_type()` buckets by entity type. This bug alone had been blocking ~50 active first-tier clubs from being matched.
  - **New country-scoped club matcher** with distinctive-token scoring. Stopwords now include club legal suffixes, football-word variants in many languages (fussball/fotboll/fudbalski/futebol/...), articles/conjunctions (la/el/y/e/van/von/...), and common founding years. Multilingual women's/youth/reserve exclusion (dames/frauen/femenino/kvinde/...). Single-token-subset matches are deliberately demoted to review to avoid false positives like "FK Beograd" ⊆ "IMT Novi Beograd".
  - **Wikidata-fallback stub detector.** Reep entities with no founded/stadium/aliases are flagged as probable SPARQL label-service fallbacks (e.g. `Q5424838` labeled "FC Barcelona" is actually FC Barcelona Femení; `Q5008937` "CA Boca Juniors" is Boca Juniors Femenino). These are routed to `data/tm-club-review.csv` rather than auto-applied, so the real men's clubs remain detectable by the monthly dump reconciliation.
  - **Borderline matches written to `reep-custom/data/tm-club-review.csv`** with score and reason — 24 candidates from this run, covering broken-entity stubs, legitimate missing entities (Legia Warsaw, Grazer AK, Olympiacos variant, Aris Thessaloniki, several Greek/Romanian clubs), and parent/child disambiguation ties.
  - **salimt-transfermarkt-datasets integration.** A second TM source (92K player profiles vs dcaribou's 47K) is now loaded from `/Volumes/WQ/ref_code/salimt-transfermarkt-datasets/...` when present. Its `name_in_home_country` field feeds into `match_players` as an additional name variant, recovering matches where dcaribou's Latin-script `name` diverges from reep's Wikidata-sourced full name.
  - **Year-placeholder DOB fallback.** `load_reep_people` now builds a secondary index of reep entities whose DOB is a `YYYY-01-01` Wikidata year-precision stub. `match_players` falls back to this index when the primary exact-DOB lookup misses, with a stricter ≥0.95 name-similarity bar and an unambiguous-hit guard (rejects matches where multiple reep entities share the same name+year).

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
