# CLAUDE.md - Reep

The football entity register. Maps player, team, and coach IDs across 30+ data providers.

- Public repo: github.com/withqwerty/reep
- Private repo (custom IDs): github.com/withqwerty/reep-custom
- API: Cloudflare Worker `reep-api` at reep-api.rahulkeerthi2-95d.workers.dev (deployed from this repo)
- RapidAPI listing: rapidapi.com/withqwerty-withqwerty-default/api/the-reep-register
- D1 database: `football-entities` (52cf53a2-7453-4ae5-a149-f43c360514ad, WEUR)

## Architecture

```
Wikidata SPARQL -> data/json/*.json -> D1 (entities + provider_ids)
                                       ↑
reep-custom scripts -> D1 (custom_ids) ─┘
                                       │
                     fetch-custom-ids.py -> data/custom_ids.json + data/reep_id_map.json
                                       │
                     export-csv.py merges both -> data/*.csv (keyed on reep_id)
                                       │
                     reep-api Worker -> reads entities + provider_ids + custom_ids + FTS -> API responses
```

- Weekly GitHub Action refreshes Wikidata data + fetches custom_ids + exports CSVs
- custom_ids table is maintained by reep-custom (private scripts, public data)
- All data is public. API serves all providers to all plans
- Entities from Wikidata and non-Wikidata sources (e.g. Opta) coexist with stable Reep IDs

## Identity Model

Every entity has a self-minted `reep_id` as its canonical primary key: `reep_<type_prefix><8hex>`.

| Prefix | Type | Example |
|--------|------|---------|
| `reep_p` | player | `reep_p2804f5db` |
| `reep_t` | team | `reep_t0871097b` |
| `reep_c` | coach | `reep_c9103de59` |
| `reep_m` | match | reserved |
| `reep_l` | league | reserved |
| `reep_s` | season | reserved |

Wikidata QIDs are a provider mapping (`provider=wikidata` in `provider_ids`), not the identity backbone. Entities can exist without a Wikidata QID (e.g. lower-league players sourced from Opta).

Design document: `docs/plan-reep-id.md`

## D1 Tables

- `entities` - 488K players/teams/coaches with bio data, PK `reep_id`
- `provider_ids` - 1.7M Wikidata-sourced provider ID mappings (including `provider=wikidata` for QIDs), PK `(reep_id, provider, external_id)`. Dropped and recreated on weekly refresh.
- `custom_ids` - ~353K verified mappings, PK `(reep_id, provider, external_id)` (Opta, FotMob, Understat, WhoScored, Club Elo, SportMonks, API-Football, FBref verified, Impect, Wyscout, SkillCorner, heim:spiel, TheSportsDB, ESPN). Never bulk-dropped.
- `entities_fts` - FTS5 virtual table for full-text search on entity names (synced from entities via triggers, rebuilt after each seed)

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/fetch-wikidata-entities.py` | SPARQL extraction (paginated, two-phase) |
| `scripts/enrich-wikidata-bio.py` | Bio enrichment (DOB, nationality, position, aliases) |
| `scripts/seed-wikidata-d1.py` | Bulk INSERT into D1 + mint reep_ids + rebuild provider_ids + FTS rebuild |
| `scripts/incremental-update.py` | Incremental Wikidata update (dual-writes to provider_ids) |
| `scripts/fetch-custom-ids.py` | Export custom_ids + reep_id_map from D1 to JSON |
| `scripts/export-csv.py` | JSON -> CSV for public download (merges Wikidata + custom, keyed on reep_id) |
| `scripts/mint-reep-ids.py` | Mint reep_ids for entities that don't have one |
| `scripts/create-provider-ids.py` | Create provider_ids table from external_ids (one-time migration) |
| `scripts/rekey-custom-ids.py` | Rekey custom_ids from (qid, type) to reep_id (one-time migration) |
| `scripts/import-opta-entities.py` | Import Opta-only players as new entities |
| `scripts/dedup-check.py` | Check for duplicate entities (DOB + name similarity) |
| `scripts/resolve-dupes.py` | Merge duplicate entities from dedup report |
| `scripts/cutover-reep-id.py` | Phase 4 cutover: make reep_id the PK (one-time migration) |
| `scripts/clone-to-staging.py` | Clone production D1 to staging for rehearsal |

## Adding a new provider

After running a new match script that writes a new provider to custom_ids:
1. `python scripts/fetch-custom-ids.py` - update custom_ids.json + reep_id_map.json
2. `python scripts/export-csv.py` - regenerate CSVs
3. Add provider to: worker.ts /resolve provider list, openapi.yaml resolve enum, cli/reep.py PROVIDERS list, README.md coverage table
4. Redeploy worker if code changed

## Worker (src/worker.ts)

- All requests require RapidAPI proxy secret OR bypass key (`X-Reep-Key` header)
- All data (Wikidata + custom) is served to all plans
- /search uses FTS5 full-text search with BM25 ranking (prefix matching, diacritics-insensitive)
- /lookup accepts `?id=` with auto-detection: Reep IDs (`reep_p...`) or QIDs (`Q...`). Legacy `?qid=` still works.
- /resolve searches provider_ids first, then custom_ids
- All responses include `reep_id` as the canonical identifier and `qid` as a convenience field (null if not in Wikidata)
- Endpoints: GET /search, /resolve, /lookup, /stats + POST /batch/lookup, /batch/resolve
- Version: 2.0.0

## Secrets (Cloudflare Worker)

- `RAPIDAPI_PROXY_SECRET` - validates RapidAPI proxy requests
- `BYPASS_KEY` - internal access key (also in myteam-website/.env as REEP_BYPASS_KEY)

## GitHub Secrets

- `CLOUDFLARE_API_TOKEN` - for wrangler D1 access
- `CLOUDFLARE_ACCOUNT_ID` - Cloudflare account

## Deployment

The Worker reads from D1 at runtime. New data (weekly refresh, custom_ids) is available instantly without redeploying. Only redeploy when `src/worker.ts` changes:

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

## Wikidata property mapping (source of truth)

`scripts/fetch-wikidata-entities.py` contains the canonical mapping of Reep provider names to Wikidata property IDs in `PLAYER_PROVIDERS`, `TEAM_PROVIDERS`, and `COACH_PROVIDERS` dicts. This is the ONLY trusted source for property IDs. Never guess P-numbers. If a provider is not in these dicts, it has no Wikidata property.

Providers with Wikidata properties include: transfermarkt, fbref, soccerway, sofascore, flashscore, espn, kicker, 11v11, besoccer, soccerbase, worldfootball, national_football_teams, eu_football_info, footballdatabase_eu, lequipe, uefa, opta, and others.

Providers WITHOUT Wikidata properties (in custom_ids only): understat, whoscored, fotmob, wyscout, skillcorner, impect, sportmonks, api_football, thesportsdb, sofifa, clubelo.

## Commands

```bash
pnpm exec wrangler deploy                    # deploy Worker (only when code changes)
pnpm exec wrangler secret put SECRET_NAME    # set Worker secret
python scripts/incremental-update.py         # incremental Wikidata update (weekly)
python scripts/incremental-update.py --dry-run  # show what would change
python scripts/fetch-wikidata-entities.py --ids-only  # full fetch (manual dispatch only, not scheduled)
python scripts/fetch-custom-ids.py     # fetch custom IDs + reep_id map from D1
python scripts/export-csv.py           # regenerate CSVs (Wikidata + custom, keyed on reep_id)
python scripts/check-sync.py           # check what's out of sync (providers, CSVs, docs)
python scripts/mint-reep-ids.py        # mint reep_ids for entities without one
python scripts/mint-reep-ids.py --dry-run  # preview what would be minted
```

## Provider coverage by entity type

Not all providers cover all entity types. To see live coverage:

```bash
pnpm exec wrangler d1 execute football-entities --remote --command "
  SELECT provider, type, COUNT(*) as cnt
  FROM (
    SELECT pi.provider, e.type FROM provider_ids pi JOIN entities e ON pi.reep_id = e.reep_id
    UNION ALL
    SELECT ci.provider, e.type FROM custom_ids ci JOIN entities e ON ci.reep_id = e.reep_id
  )
  GROUP BY provider, type ORDER BY provider, type"
```

Key multi-type providers: fbref (player/team/coach), soccerway (player/team/coach), transfermarkt (player/team via main, coach via transfermarkt_manager), fotmob (player/team/coach), soccerbase (player/team/coach). Most other providers are player-only. playmakerstats is team-only. clubelo is player+team.
