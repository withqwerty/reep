# CLAUDE.md - Reep

The Reep Register: a cross-provider lookup for football players, teams, coaches, competitions, and seasons. This repo is the **public** half — it holds the published CSVs and the Cloudflare Worker that serves them as an API.

- GitHub: github.com/withqwerty/reep
- API: `reep-api.rahulkeerthi2-95d.workers.dev` (deployed from `src/`)
- RapidAPI: rapidapi.com/withqwerty-withqwerty-default/api/the-reep-register
- Website: reep.football
- D1 database: `football-entities`

## What's in this repo

```
src/              Cloudflare Worker (TypeScript)
data/             Published CSVs + supporting JSON
  samples/        First-100-row previews of each CSV (rendered on GitHub)
openapi.yaml      API spec used for the RapidAPI listing
wrangler.toml     Worker config
CHANGELOG.md      API semver + data calver release notes
```

Data is refreshed upstream and the CSVs here are regenerated as part of that workflow. This repo does **not** contain the code that produces the data — it only serves it.

## Identity model

Every entity has a self-minted `reep_id` as its canonical primary key: `reep_<type_prefix><8hex>`.

| Prefix | Type | Example |
|--------|------|---------|
| `reep_p` | player | `reep_p2804f5db` |
| `reep_t` | team | `reep_t0871097b` |
| `reep_c` | coach | `reep_c9103de59` |
| `reep_l` | competition | `reep_l3a8f01bc` |
| `reep_s` | season | `reep_s7d2e49a0` |
| `reep_m` | match | reserved |

Wikidata QIDs are one provider mapping among many (`provider=wikidata`), not the identity backbone. Entities can exist without a Wikidata QID.

## D1 tables

- `entities` — 490K+ players, teams, coaches, competitions, seasons. PK `reep_id`.
- `provider_ids` — Wikidata-sourced mappings from `reep_id` to external provider IDs.
- `custom_ids` — verified mappings for providers with no Wikidata property (Opta, FotMob, Understat, SportMonks, etc.).
- `custom_aliases` — name variants harvested from provider data, merged into `data/names.csv`.
- `entities_fts` — FTS5 virtual table for full-text search, synced from `entities`.

## Worker

All endpoints live in `src/worker.ts`. Overview:

- Auth: every request must carry `X-RapidAPI-Proxy-Secret` (set by the RapidAPI proxy) or `X-Reep-Key` (internal bypass). Fails closed. Constant-time HMAC comparison.
- `GET /search` — FTS5 full-text search with BM25 ranking, prefix matching, diacritics-insensitive.
- `GET /lookup?id=…` — Reep ID or QID, auto-detected.
- `GET /resolve?provider=…&id=…` — searches `provider_ids` then `custom_ids`. Rejects unknown providers with 400.
- `GET /stats` — aggregate counts.
- `POST /batch/lookup`, `POST /batch/resolve`.
- CORS `*` is intentional (public read-only API).
- `API_VERSION` const at the top of the file is the source of truth for the API version and is bumped in lockstep with `package.json` version.
- `VALID_PROVIDERS` Set at the top of the file is the authoritative list of provider names the `/resolve` family will accept.

Every response includes `reep_id` as the canonical identifier and `qid` as a convenience field (null if the entity isn't in Wikidata).

## Deploy

```bash
cd /Users/rahulkeerthi/Work/reep
pnpm exec wrangler deploy
```

The Worker reads from D1 at runtime. For existing providers, data updates are live instantly without redeploying. New providers require a redeploy because the worker validates provider names against `VALID_PROVIDERS`.

Worker secrets (`RAPIDAPI_PROXY_SECRET`, `BYPASS_KEY`) are managed via `pnpm exec wrangler secret put`.

## Provider IDs: Opta / Stats Perform

Stats Perform operates several distinct ID systems that look similar but are not interchangeable. Reep splits them into separate providers:

| Provider | Format | Coverage |
|----------|--------|----------|
| `opta` | 25-char alphanumeric UUID | Modern Stats Perform F1 products and The Analyst |
| `opta_numeric` | Integer | Legacy Opta numeric IDs (also = FPL player/team `code`) |
| `optacore` | Integer | Separate numeric system used by the SD API |
| `premier_league` | Integer | premierleague.com player page IDs |

`opta` is always UUID format — never mix numeric codes into it.

## Release management

- **API** uses semver. `API_VERSION` (in `src/worker.ts`) and `package.json` version are always bumped together in the same commit, with a `CHANGELOG.md` entry.
- **Data** uses ISO-week calver (`YYYY.WW`), auto-stamped in `data/meta.json` by the upstream export step.
- Notable data releases (new providers, bulk reconciliations, or any change that shifts entity counts by >1%) get a `data-YYYY.WW` GitHub Release tag. Routine weekly refreshes are just commits.

**Before adding a `YYYY.WW` data heading, run `date +%G.%V` and use that ISO week.** One heading per week — if one already exists, append to it rather than creating a new heading. Multiple headings in the same week means you're minting multiple logical releases for one calendar week, which breaks the data-tag model.

## Changes to this repo

- Worker code lives here (`src/worker.ts`). Edit it, bump `API_VERSION` + `package.json` version, add a `CHANGELOG.md` entry, commit, tag, deploy.
- CSV/JSON data files are regenerated upstream and committed here as part of the release flow — don't hand-edit them.
- The Python CLI lives in its own repo: github.com/withqwerty/reep-cli.
- The marketing site lives in a separate private repo and is deployed to reep.football via Cloudflare Pages.
