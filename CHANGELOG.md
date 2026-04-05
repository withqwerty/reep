# Changelog

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
