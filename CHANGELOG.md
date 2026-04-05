# Changelog

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
