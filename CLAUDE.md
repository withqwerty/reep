# CLAUDE.md - Reep

The football entity register. Maps player, team, and coach IDs across 30+ data providers.

This repo holds **data + API only**. All data-wrangling scripts, the Astro site, and the Python CLI now live in sibling repos. See the "Sibling repos" section.

- Public repo: github.com/withqwerty/reep
- API: Cloudflare Worker `reep-api` at reep-api.rahulkeerthi2-95d.workers.dev (deployed from this repo)
- RapidAPI listing: rapidapi.com/withqwerty-withqwerty-default/api/the-reep-register
- D1 database: `football-entities` (52cf53a2-7453-4ae5-a149-f43c360514ad, WEUR)

## Sibling repos

- `../reep-custom` — **private** workhorse. All data-wrangling scripts (Wikidata fetch, dump reconciliation, match scripts, exports), the D1 schema, intermediate JSON, docs/plans. Populates `custom_ids` + `provider_ids` + `entities` in D1 and regenerates this repo's CSVs. See `../reep-custom/CLAUDE.md`.
- `../reep-site` — **private** Astro site deployed to `reep.football` via Cloudflare Pages.
- `../reep-cli` — **public** Python CLI (`pip install reep` or similar).
- `../football-docs` — MCP server serving provider documentation (StatsBomb, Opta, Wyscout, kloppy, SportMonks, etc.). When you need to understand a provider's data model, event types, qualifier IDs, or API surface, use `mcp__plugin_nutmeg_football-docs__search_docs` instead of guessing. See `../football-docs/CLAUDE.md` (gitignored, local only).

## What lives in this repo

```
reep/
├── src/              Cloudflare Worker source (TypeScript)
├── data/             Published CSVs + intermediate JSON (custom_ids.json, reep_id_map.json)
│   └── samples/      First 100 rows of each CSV, kept up to date so users can preview on GitHub
├── openapi.yaml      OpenAPI spec for the RapidAPI listing
├── wrangler.toml     Cloudflare Worker config
├── package.json      Worker dependencies
├── CHANGELOG.md      API semver + data calver release notes
└── README.md         Public-facing docs
```

**Not here any more** (moved during the 2026-04 restructure):

- `scripts/` → `../reep-custom/scripts/` (all 20 Python scripts that generate `data/`)
- `schemas/` → `../reep-custom/schemas/`
- `data/json/` → `../reep-custom/data/json/` (gitignored there — regenerated weekly)
- `data/backups/`, `data/dedup-report.json`, `data/match_dictionary.csv` → `../reep-custom/data/` (also gitignored)
- `docs/` → `../reep-custom/docs/` (plan/spec docs — this repo has no `docs/` dir)
- `cli/` → `../reep-cli` (separate public repo)
- `site/` → `../reep-site` (separate private repo)
- `.github/workflows/update-register.yml` → deleted (replaced by `../reep-custom/docs/dump-workflow.md`)
- `.github/workflows/ci.yml` → deleted (only ran CLI tests, which moved to reep-cli)

## Architecture

```
Wikidata dump ─┐
               ├──► reep-custom scripts ──► D1 (entities + provider_ids + custom_ids)
Match scripts ─┘                             │
                                             ▼
                          reep-custom/scripts/fetch-custom-ids.py
                          reep-custom/scripts/export-csv.py
                                             │
                                             ▼
                                reep/data/*.csv (public)
                                             │
                                             ▼
                                reep-api Worker ──► RapidAPI
```

- `reep-custom` owns the write path to D1 and regenerates this repo's CSVs
- `reep` serves the data via the Cloudflare Worker and hosts the CSVs for direct download
- All data is public. API serves all providers to all plans

## Identity Model

Every entity has a self-minted `reep_id` as its canonical primary key: `reep_<type_prefix><8hex>`.

| Prefix | Type | Example |
|--------|------|---------|
| `reep_p` | player | `reep_p2804f5db` |
| `reep_t` | team | `reep_t0871097b` |
| `reep_c` | coach | `reep_c9103de59` |
| `reep_l` | competition | `reep_l3a8f01bc` |
| `reep_s` | season | `reep_s7d2e49a0` |
| `reep_m` | match | reserved |

Wikidata QIDs are a provider mapping (`provider=wikidata` in `provider_ids`), not the identity backbone. Entities can exist without a Wikidata QID (e.g. lower-league players sourced from Opta).

Design document: `../reep-custom/docs/plan-reep-id.md`

## D1 Tables

- `entities` - 488K+ players/teams/coaches/competitions/seasons with bio data, PK `reep_id`. Seasons have `competition_reep_id` FK to their competition.
- `provider_ids` - 1.7M Wikidata-sourced provider ID mappings (including `provider=wikidata` for QIDs), PK `(reep_id, provider, external_id)`. Dropped and recreated on weekly refresh.
- `custom_ids` - ~353K verified mappings, PK `(reep_id, provider, external_id)` (Opta, FotMob, Understat, WhoScored, Club Elo, SportMonks, API-Football, FBref verified, Impect, Wyscout, SkillCorner, heim:spiel, TheSportsDB, ESPN). Never bulk-dropped.
- `custom_aliases` - name variants collected from provider data sources, PK `(reep_id, alias)`. Merged into `data/names.csv` by `export-csv.py`.
- `entities_fts` - FTS5 virtual table for full-text search on entity names (synced from entities via triggers, rebuilt after each seed)

Schema source of truth: `../reep-custom/schemas/football-entities.sql`.

## Adding a new provider

Order matters — the worker validates provider names against `VALID_PROVIDERS` in `src/worker.ts`, so IDs written to D1 for an unknown provider are not queryable until the worker is redeployed.

**reep (this repo) — do first:**
1. Add provider to `VALID_PROVIDERS` Set in `src/worker.ts`
2. Add to: openapi.yaml resolve enum, ../reep-cli/reep.py PROVIDERS list, README.md coverage table
3. Bump `API_VERSION` const in `src/worker.ts` (minor) + `package.json` version to match
4. Add CHANGELOG.md entry
5. Commit: `release: v2.x.y — add <provider> provider`
6. `git tag v2.x.y` + GitHub Release
7. `pnpm exec wrangler deploy`

**reep-custom — do second:**
8. Write and run the match script (IDs go live in API immediately)
9. Update reep-custom `CLAUDE.md` provider table + scripts table

**reep (this repo) — finalize:**
10. `cd ../reep-custom && python scripts/fetch-custom-ids.py` — pull new custom_ids from D1 (writes to `../reep/data/custom_ids.json`)
11. `python scripts/export-csv.py` — regenerate CSVs (writes to `../reep/data/`)
12. `python scripts/validate-csv.py` — verify the export
13. Commit data changes in **this** repo

## Worker (src/worker.ts)

- All requests require RapidAPI proxy secret OR bypass key (`X-Reep-Key` header)
- All data (Wikidata + custom) is served to all plans
- /search uses FTS5 full-text search with BM25 ranking (prefix matching, diacritics-insensitive)
- /lookup accepts `?id=` with auto-detection: Reep IDs (`reep_p...`) or QIDs (`Q...`). Legacy `?qid=` still works.
- /resolve searches provider_ids first, then custom_ids
- All responses include `reep_id` as the canonical identifier and `qid` as a convenience field (null if not in Wikidata)
- Endpoints: GET /search, /resolve, /lookup, /stats + POST /batch/lookup, /batch/resolve
- Version: `API_VERSION` const at top of file (currently 2.0.0)
- Provider validation: `VALID_PROVIDERS` Set at top of file — `/resolve` and `/batch/resolve` reject unknown providers with 400

## Secrets (Cloudflare Worker)

- `RAPIDAPI_PROXY_SECRET` - validates RapidAPI proxy requests
- `BYPASS_KEY` - internal access key (also in myteam-website/.env as REEP_BYPASS_KEY)

## GitHub Secrets

- `CLOUDFLARE_API_TOKEN` - for wrangler D1 access
- `CLOUDFLARE_ACCOUNT_ID` - Cloudflare account

## Release Management

Two version tracks: API (semver) and Data (calver). Full plan: `../reep-custom/docs/plans/release-management.md`.

### API versioning (semver)

- **Source of truth**: `API_VERSION` const in `src/worker.ts` + `package.json` version (kept in sync, same commit)
- **Major**: breaking response shape, removed endpoint, auth change
- **Minor**: new endpoint, new query param, new provider
- **Patch**: bug fix, no response change
- **Process**: bump both versions → CHANGELOG.md entry → commit `release: v2.x.y` → git tag → deploy → GitHub Release

### Data versioning (calver)

- **Format**: `YYYY.WW` (ISO year.week), auto-stamped in `data/meta.json` by `export-csv.py` (which now runs from reep-custom)
- **Notable data releases** (new provider, bulk reconciliation, >1% entity count change) get a `data-YYYY.WW` GitHub Release tag
- **Routine weekly refreshes** are just commits, no tag

### CHANGELOG.md

In repo root. API bumps get `## v2.x.y` headings, data-only weeks get `## YYYY.WW` headings. Newest first.

**Before adding a data heading, run `date +%G.%V` and use THAT as the ISO week.** If a heading for that week already exists, add to it — **do not create a new heading**. The plan's collision rule (`../reep-custom/docs/plans/release-management.md`) says: *"One data tag per ISO week maximum. The changelog entry lists all events under the same `YYYY.WW` heading."* Multiple headings in the same week means you're creating multiple logical releases for the same calendar week, which breaks the data-tag model.

## Deployment

The Worker reads from D1 at runtime. For existing providers, new data is available instantly without redeploying. New providers require a redeploy (worker validates against `VALID_PROVIDERS`). Only redeploy when `src/worker.ts` changes:

```bash
cd /Users/rahulkeerthi/Work/reep && pnpm exec wrangler deploy
```

## FTS5 Notes

- `entities_fts` is an external-content FTS5 table linked to `entities` via implicit rowid
- Sync triggers keep the index updated for incremental changes; bulk seeds drop triggers and rebuild
- `wrangler d1 export` does not work with virtual tables. Drop FTS first, export, then recreate and rebuild
- Rollback: `DROP TRIGGER IF EXISTS entities_fts_ai; DROP TRIGGER IF EXISTS entities_fts_ad; DROP TRIGGER IF EXISTS entities_fts_au; DROP TABLE IF EXISTS entities_fts;`

## D1 Time Travel

Point-in-time recovery for the last 30 days. Use to rollback bad data:

```bash
npx wrangler d1 time-travel info football-entities --timestamp "2026-04-01T22:00:00Z" --json
npx wrangler d1 time-travel restore football-entities --bookmark <bookmark_id>
```

No `--remote` flag. Time Travel commands always act on the remote database.

## Opta / Stats Perform ID systems

Stats Perform (Opta's parent) operates **multiple distinct ID systems** that are not interchangeable. Reep splits them into separate providers. Full breakdown: `../reep-custom/docs/providers/opta-stats-perform.md`.

| Provider | Format | Source | Coverage |
|----------|--------|--------|----------|
| `opta` | Alphanumeric UUID (e.g. `2kwbbcootiqqgmrzs6o5inle5`) | Stats Perform F1 data products, The Analyst | Players (50K), PL teams, competitions, PL seasons |
| `opta_numeric` | Integer (e.g. `8` for PL) | Wikidata P8735, FPL bootstrap, SD API | Players, teams, competitions, seasons |
| `premier_league` | Integer | premierleague.com player page URLs (Wikidata P12539) | Players (4.9K PL-registered) |
| `optacore` | Integer | `../reep-custom/data/opta/sdapi-mapping-league-seasons.json` | Competitions + seasons |

**`opta` is always UUID format.** Never mix numeric codes under `opta`.

**Wikidata P8736 (player numeric) and P8737 (team numeric) are excluded from ingest** — both superseded by the Stats Perform F1 UUIDs and would duplicate/conflict with the canonical `opta` player IDs. See excluded list in `../reep-custom/scripts/fetch-wikidata-entities.py`.

## Wikidata property mapping (source of truth)

`../reep-custom/scripts/fetch-wikidata-entities.py` contains the canonical mapping of Reep provider names to Wikidata property IDs in `PLAYER_PROVIDERS`, `TEAM_PROVIDERS`, and `COACH_PROVIDERS` dicts. This is the ONLY trusted source for property IDs. Never guess P-numbers. If a provider is not in these dicts, it has no Wikidata property.

Providers with Wikidata properties include: transfermarkt, fbref, soccerway, sofascore, flashscore, espn, kicker, 11v11, besoccer, soccerbase, worldfootball, national_football_teams, eu_football_info, footballdatabase_eu, lequipe, uefa, opta, and others.

Providers WITHOUT Wikidata properties (in custom_ids only): understat, whoscored, fotmob, wyscout, skillcorner, impect, sportmonks, api_football, thesportsdb, sofifa, clubelo.

## Commands

This repo deliberately has no `scripts/` directory. All data-wrangling commands live in `../reep-custom`:

```bash
# From this repo (reep):
pnpm exec wrangler deploy                    # deploy Worker (only when code changes)
pnpm exec wrangler secret put SECRET_NAME    # set Worker secret

# From the sibling repo (reep-custom):
cd ../reep-custom
python scripts/fetch-custom-ids.py           # fetch custom IDs from D1 → reep/data/custom_ids.json
python scripts/export-csv.py                 # regenerate reep/data/*.csv
python scripts/validate-csv.py               # validate exported CSVs
python scripts/check-sync.py                 # check what's out of sync (providers, CSVs, docs)
python scripts/seed-wikidata-d1.py           # seed D1 from reep-custom/data/json/
python scripts/apply-dump-snapshot.py --seed # one-shot: apply dump + reseed
```

## Security

**Auth model:** Worker checks `X-RapidAPI-Proxy-Secret` (set by RapidAPI proxy) or `X-Reep-Key` (bypass for internal use). Auth fails closed — if `RAPIDAPI_PROXY_SECRET` env var is missing, all requests are rejected. Both comparisons use constant-time HMAC to prevent timing side-channels. Secrets are stored in Cloudflare's encrypted secret store (`wrangler secret`).

**Accepted risks:**
- **CORS wildcard `*`**: Intentional. This is a public read-only API. No cookies or sessions. Any origin can make requests.
- **D1 database ID in `wrangler.toml`**: Required by Wrangler. The ID alone does not grant access — a valid Cloudflare API token + account ID is needed.
- **Cache-Control `public` on all responses**: Acceptable for a public read-only API. Error responses (401, 404) are also cached — this is fine since they contain no user-specific data.
