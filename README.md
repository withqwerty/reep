# Reep

![Charles Reep's match notations from 1953](https://ichef.bbci.co.uk/ace/standard/624/cpsprodpb/FC93/production/_124995646_bbc1953notations.jpg)

The football entity register. Maps player, team, coach, competition, and season identities across Transfermarkt, FBref, UEFA, Sofascore, and 30+ data providers.

Named after [Charles Reep](https://en.wikipedia.org/wiki/Charles_Reep) (1904--2002), an RAF wing commander who hand-recorded every action in over 2,200 football matches starting in the 1950s. He's considered the grandfather of football analytics -- decades before expected goals or tracking data, Reep was tallying passes, shots, and sequences with pen and paper, pioneering the idea that football could be understood through data.

## Start here: the current register

The living register is **Reep v1**, published at [reep.football](https://reep.football).
It is open infrastructure: complete, dated snapshots of the provider-ID crosswalk —
Reep IDs, provider namespaces and identifiers, redirects and confidence labels —
free to download and use.

| I want to... | Go to |
|---|---|
| Download the register (CSV + DuckDB) | [reep.football/downloads](https://reep.football/downloads) |
| Look up or resolve a single entity | [reep.football/api](https://reep.football/api) |
| See what's covered | [reep.football/coverage](https://reep.football/coverage) |
| Understand how identities are decided | [reep.football/entity-resolution](https://reep.football/entity-resolution) |
| Report a wrong ID, a bad merge or a missing entity | [open a correction issue](https://github.com/withqwerty/reep/issues/new?template=report-correction.yml) |
| Contribute ID mappings | [open a data issue](https://github.com/withqwerty/reep/issues/new?template=contribute-data.yml) |
| Support the upkeep | [reep.football/sponsorship](https://reep.football/sponsorship) |

**Why isn't the v1 engine in this repository?** The register is open; the engine that
builds and repairs it is not. Producing a trustworthy crosswalk repeatably is the work
we do commercially, and it funds the public register. What is open is the output and the
method: the snapshots, the evidence rules, the identity policy, and the measured
precision and recall of each release. No part of the public register is held back to
make sponsorship or consultancy more attractive — see the
[independence policy](https://reep.football/sponsorship-independence).

> **This repository is the frozen v0 register and RapidAPI/D1 API surface.**
> It remains available for existing users during the migration bridge, and everything
> below documents it. New integrations should start from the v1 surfaces above; see the
> [RapidAPI migration guide](https://reep.football/api/migration). v0 `reep_...` IDs are
> **not** interchangeable with Reep v1 IDs.
>
> **The data files are frozen too, not just the API.** The last CSV release was
> `2026.25` (21 June 2026), and the API's database has taken no writes since
> 25 April 2026. Neither receives further updates or corrections.

---

## v0 reference (frozen)

Everything from here down describes the frozen v0 data files and API.

## What is this?

A canonical identity file for football. Every person, club, competition, and season gets a stable Reep ID (`reep_<type_prefix><8hex>`), linked to their IDs on other platforms. If you have a Transfermarkt ID and need the FBref ID for the same player — or want to resolve an Opta competition ID to its FBref equivalent — this register gives you the answer.

The unique key is `reep_id`. Wikidata QIDs are available as a provider mapping where the entity exists in Wikidata, but entities can exist independently (e.g. lower-league players sourced from Opta).

People who are both players and coaches (e.g. Pep Guardiola) have separate records with distinct Reep IDs — `reep_p*` for the player record, `reep_c*` for the coach record.

Think of it as the football equivalent of the [Chadwick Baseball Bureau Register](https://github.com/chadwickbureau/register).

## Data

These CSVs are the v0 public files. The v1 release files use a different
bridge-register contract with canonical CSVs, a DuckDB convenience bundle,
release metadata, namespace-scoped bridges, bridge-only provider roles and
overlay-only Wikidata aliases. Use
[reep.football/downloads](https://reep.football/downloads) for the current v1
download surface.

| File | Records | Description |
|------|---------|-------------|
| [`data/people.csv`](data/people.csv) | 444,707 | Players and coaches with provider IDs and bio |
| [`data/teams.csv`](data/teams.csv) | 45,337 | Clubs with provider IDs and metadata |
| [`data/competitions.csv`](data/competitions.csv) | 212 | Leagues, cups, and tournaments with provider IDs |
| [`data/seasons.csv`](data/seasons.csv) | 1,200 | Season editions of competitions |
| [`data/names.csv`](data/names.csv) | 27,591 | Alternate names and aliases |
| [`data/meta.json`](data/meta.json) | — | Generation timestamp and counts |

### People schema

| Column | Description | Example |
|--------|-------------|---------|
| `reep_id` | Reep ID (canonical key) | `reep_p2804f5db` |
| `key_wikidata` | Wikidata QID (empty if not in Wikidata) | `Q99760796` |
| `type` | `player` or `coach` | `player` |
| `name` | Primary English name | `Cole Palmer` |
| `full_name` | Birth/legal name | `Cole Jermaine Palmer` |
| `date_of_birth` | ISO date | `2002-05-06` |
| `nationality` | Country | `United Kingdom` |
| `position` | Playing position | `attacking midfielder` |
| `height_cm` | Height in centimetres | `185` |
| `key_transfermarkt` | [Transfermarkt](https://www.transfermarkt.com/) player ID | `568177` |
| `key_transfermarkt_manager` | Transfermarkt manager ID (coaches only) | `50100` |
| `key_fbref` | [FBref](https://fbref.com/) player ID | `dc7f8a28` |
| `key_soccerway` | [Soccerway](https://www.scorebar.com/) person ID | `525801` |
| `key_sofascore` | [Sofascore](https://www.sofascore.com/) player ID | `982780` |
| `key_flashscore` | [Flashscore](https://www.flashscore.com/) player ID | `palmer-cole/h8agbDt7` |
| `key_opta` | [Opta](https://www.statsperform.com/) player ID | `7cwgrmorsb42qaj5vrhp8fhzp` |
| `key_premier_league` | [Premier League](https://www.premierleague.com/) player ID | `49293` |
| `key_11v11` | [11v11](https://www.11v11.com/) player ID | `265554` |
| `key_espn` | [ESPN FC](https://www.espn.com/football/) player ID | — |
| `key_national_football_teams` | [National Football Teams](https://www.national-football-teams.com/) ID | `92970` |
| `key_worldfootball` | [WorldFootball.net](https://www.worldfootball.net/) ID | `cole-palmer` |
| `key_soccerbase` | [Soccerbase](https://www.soccerbase.com/) player ID | `125454` |
| `key_kicker` | [Kicker](https://www.kicker.de/) player ID | `cole-palmer` |
| `key_uefa` | [UEFA](https://www.uefa.com/) player ID | — |
| `key_lequipe` | [L'Equipe](https://www.lequipe.fr/) player ID | — |
| `key_fff_fr` | [FFF.fr](https://www.fff.fr/) player ID | — |
| `key_serie_a` | [Lega Serie A](https://www.legaseriea.it/) player ID | — |
| `key_besoccer` | [BeSoccer](https://www.besoccer.com/) player ID | — |
| `key_footballdatabase_eu` | [FootballDatabase.eu](https://www.footballdatabase.eu/) person ID | — |
| `key_eu_football_info` | [EU-Football.info](https://eu-football.info/) player ID | — |
| `key_hugman` | [Barry Hugman's Footballers](https://www.barryhugmansfootballers.com/) ID | — |
| `key_german_fa` | [DFB](https://www.dfb.de/) person ID | — |
| `key_statmuse_pl` | [StatMuse](https://www.statmuse.com/) PL player ID | — |
| `key_sofifa` | [SoFIFA](https://sofifa.com/) / EA FC player ID | — |
| `key_soccerdonna` | [Soccerdonna](https://www.soccerdonna.de/) player ID (women's football) | — |
| `key_dongqiudi` | [Dongqiudi](https://www.dongqiudi.com/) player ID | — |
| `key_understat` | [Understat](https://understat.com/) player ID | `1234` |
| `key_whoscored` | [WhoScored](https://www.whoscored.com/) player ID | `456789` |
| `key_fbref_verified` | FBref ID (cross-verified via worldfootballR) | `dc7f8a28` |
| `key_sportmonks` | [SportMonks](https://www.sportmonks.com/) player ID | `12345` |
| `key_api_football` | [API-Football](https://www.api-football.com/) player ID | `1100` |
| `key_fotmob` | [FotMob](https://www.fotmob.com/) player ID | `292462` |
| `key_opta_numeric` | Opta legacy numeric ID (same as FPL `code`, The Analyst `sc-` IDs) | `244851` |
| `key_thesportsdb` | [TheSportsDB](https://www.thesportsdb.com/) player ID | `34146086` |
| `key_skillcorner` | [SkillCorner](https://www.skillcorner.com/) player ID | `23959` |
| `key_wyscout` | [Wyscout](https://wyscout.com/) player ID | `234966` |
| `key_impect` | [Impect](https://www.impect.com/) player ID | `52615` |
| `key_heimspiel` | [heim:spiel](https://heimspiel.de/) player ID | `361032` |
| `key_capology` | [Capology](https://www.capology.com/) player slug | `cole-palmer-36271` |
| `position_detail` | Granular position from Transfermarkt | `Attacking Midfield` |

### Teams schema

| Column | Description | Example |
|--------|-------------|---------|
| `reep_id` | Reep ID (canonical key) | `reep_t0871097b` |
| `key_wikidata` | Wikidata QID | `Q9616` |
| `name` | Primary English name | `Arsenal F.C.` |
| `country` | Country | `United Kingdom` |
| `founded` | Founding date | `1886-10-01` |
| `stadium` | Home ground | `Emirates Stadium` |
| `key_transfermarkt` | Transfermarkt team ID | `11` |
| `key_fbref` | FBref squad ID | `18bb7c10` |
| `key_soccerway` | Soccerway team ID | `660` |
| `key_opta` | Opta team ID | `b3sy95iqnw2bv69a0gxunhiot` |
| `key_kicker` | Kicker team ID | — |
| `key_flashscore` | Flashscore team ID | — |
| `key_sofascore` | Sofascore team ID | — |
| `key_soccerbase` | Soccerbase team ID | — |
| `key_uefa` | UEFA team ID | — |
| `key_footballdatabase_eu` | FootballDatabase.eu team ID | — |
| `key_worldfootball` | WorldFootball.net team ID | — |
| `key_espn` | ESPN team ID | — |
| `key_playmakerstats` | [PlaymakerStats](https://www.playmakerstats.com/) team ID | — |
| `key_clubelo` | [Club Elo](http://clubelo.com/) team ID | `Arsenal` |
| `key_sportmonks` | SportMonks team ID | `123` |
| `key_api_football` | API-Football team ID | `42` |
| `key_sofifa` | SoFIFA / EA FC team ID | `1` |
| `key_fotmob` | FotMob team ID | `9825` |
| `key_opta_numeric` | Opta legacy numeric team ID | `3` |
| `key_capology` | Capology team slug | `arsenal` |

### Competitions schema

| Column | Description | Example |
|--------|-------------|---------|
| `reep_id` | Reep ID (canonical key) | `reep_lb3d230cb` |
| `key_wikidata` | Wikidata QID | `Q9448` |
| `name` | Competition name | `Premier League` |
| `country` | Country | `United Kingdom` |
| `key_transfermarkt` | Transfermarkt competition ID | `GB1` |
| `key_fbref` | FBref competition ID | `9` |
| `key_opta` | Opta competition ID (UUID) | `2kwbbcootiqqgmrzs6o5inle5` |
| `key_opta_numeric` | Opta legacy numeric competition ID | `8` |
| `key_optacore` | Opta core numeric competition ID | `1` |

### Seasons schema

| Column | Description | Example |
|--------|-------------|---------|
| `reep_id` | Reep ID (canonical key) | `reep_sa7f63ba6` |
| `key_wikidata` | Wikidata QID | `Q124371422` |
| `name` | Season name | `2024–25 Premier League` |
| `competition_reep_id` | Reep ID of parent competition | `reep_lb3d230cb` |

### Names schema

| Column | Description | Example |
|--------|-------------|---------|
| `key_wikidata` | Wikidata QID | `Q11893` |
| `name` | Primary name | `Cristiano Ronaldo` |
| `alias` | Alternate name | `Cristiano Ronaldo dos Santos Aveiro` |

## Coverage

Not every entity has every ID. Coverage depends on what the Wikidata community has mapped plus independently verified mappings. `GET /stats` returns the frozen v0 counts; they no longer change.

| Provider | Source | Notes |
|----------|--------|-------|
| Transfermarkt | Wikidata | Highest coverage across all entities |
| FBref | Wikidata | Strong for recent players |
| Soccerway | Wikidata | Broad international coverage |
| Sofascore | Wikidata | Modern players well covered |
| Premier League | Wikidata | PL players only |
| Opta | Verified | Alphanumeric IDs from Stats Perform's Opta F1 database (~50K players) |
| Opta numeric | Verified | Legacy Opta numeric IDs (same as FPL `code`, The Analyst `sc-` IDs) |
| Impect | Verified | DOB + name matching via Impect export |
| Wyscout | Verified | Via Impect ID mappings |
| SkillCorner | Verified | Via Impect ID mappings |
| heim:spiel | Verified | Via Impect ID mappings |
| TheSportsDB | Verified | Direct Wikidata link + DOB/name matching |
| API-Football | Verified | Via TheSportsDB + direct matching |
| ESPN | Verified | Via TheSportsDB mappings |
| FotMob | Verified | DOB + name matching |
| FBref verified | Verified | Cross-verified via worldfootballR |
| Understat | Verified | Cross-reference matching |
| WhoScored | Verified | Cross-reference matching |
| SportMonks | Verified | Cross-reference matching |
| Club Elo | Verified | Manual team mapping |

**Wikidata** IDs are community-maintained; they updated automatically with each refresh while v0 was live, and are now fixed at the `2026.25` snapshot. **Verified** IDs were matched independently using DOB, name, and cross-provider bridges, then validated before inclusion.

## Usage

### Python

```python
import csv

# Load people into a dict keyed by Reep ID
people = {}
with open("data/people.csv") as f:
    for row in csv.DictReader(f):
        people[row["reep_id"]] = row

# Look up by Transfermarkt ID
tm_index = {row["key_transfermarkt"]: row for row in people.values() if row["key_transfermarkt"]}
palmer = tm_index["568177"]
print(palmer["reep_id"])    # "reep_p2804f5db"
print(palmer["key_fbref"])  # "dc7f8a28"
```

### R

```r
library(readr)
people <- read_csv("data/people.csv")

# All Premier League-registered players
pl_players <- people |> filter(key_premier_league != "")

# Cross-reference: Transfermarkt -> FBref
people |>
  filter(key_transfermarkt == "568177") |>
  select(reep_id, name, key_fbref, key_sofascore)
```

### SQL (load into SQLite)

```bash
sqlite3 reep.db <<EOF
.mode csv
.import data/people.csv people
.import data/teams.csv teams
.import data/competitions.csv competitions
.import data/seasons.csv seasons
.import data/names.csv names
EOF
```

```sql
-- Find all IDs for a player
SELECT * FROM people WHERE name LIKE '%Salah%';

-- Reverse lookup: FBref ID -> everything
SELECT * FROM people WHERE key_fbref = 'e342ad68';

-- Lookup by Reep ID
SELECT * FROM people WHERE reep_id = 'reep_p2804f5db';
```

## API

The API in this repository is the v0 REST interface served through RapidAPI and
the legacy Cloudflare Worker. It remains available for existing users during the
migration bridge. All providers (Wikidata + custom verified) are available to all
plans.

**Get your API key on [RapidAPI](https://rapidapi.com/withqwerty-withqwerty-default/api/the-reep-register).**

For new integrations, use the v1 release API at
[reep.football/api](https://reep.football/api). The v1 API is release-backed,
uses direct Reep API keys, and requires namespace-aware provider ID resolution:

```bash
curl -H "Authorization: Bearer $REEP_API_KEY" \
  "https://reep.football/api/v1/resolve/transfermarkt/568177?namespace=spieler&type=player"
```

See the [RapidAPI migration guide](https://reep.football/api/migration) before
moving a v0 consumer. v1 is not a drop-in continuity layer for v0 `reep_...` IDs.

| Endpoint | Description | Example |
|----------|-------------|---------|
| `GET /search` | Search by name (prefix matching) | `/search?name=Cole Palmer&type=player` |
| `GET /resolve` | Translate provider ID | `/resolve?provider=transfermarkt&id=568177` |
| `GET /lookup` | Look up by Reep ID or Wikidata QID | `/lookup?id=reep_p2804f5db` |
| `GET /stats` | Database statistics | `/stats` |

The `/lookup` endpoint auto-detects the ID type: Reep IDs start with `reep_`, Wikidata QIDs start with `Q`. The legacy `?qid=` parameter is still supported.

All endpoints that return entities accept an optional `type` parameter (`player`, `team`, `coach`, `competition`, `season`). For dual-role people, `/lookup` without `type` returns all records. Default search excludes seasons to avoid noise — use `type=season` to search seasons explicitly.

## Reep IDs

Every entity in the register has a self-minted Reep ID as its canonical identifier. The format is `reep_<type_prefix><8hex>`:

| Prefix | Entity type | Example |
|--------|-------------|---------|
| `reep_p` | Player | `reep_p2804f5db` (Cole Palmer) |
| `reep_t` | Team | `reep_t0871097b` (Arsenal F.C.) |
| `reep_c` | Coach | `reep_c9103de59` (A. H. Albut) |
| `reep_l` | Competition | `reep_lb3d230cb` (Premier League) |
| `reep_s` | Season | `reep_sa7f63ba6` (2024–25 Premier League) |

Reep IDs are stable — they never change, even if a player's Wikidata QID is merged or deleted. Wikidata QIDs are available as a provider mapping (`key_wikidata` in CSVs, `qid` in API responses) but are not the identity backbone.

This design follows the [Chadwick Baseball Bureau Register](https://github.com/chadwickbureau/register) model: self-minted UUIDs as primary keys, with all provider IDs (including Wikidata) as cross-references.

## CLI

A Python CLI for the register lives in its own repo: [**withqwerty/reep-cli**](https://github.com/withqwerty/reep-cli).

```bash
pip install git+https://github.com/withqwerty/reep-cli.git

# Search by name
reep search "Cole Palmer"

# Resolve: Transfermarkt -> all IDs
reep resolve transfermarkt 568177

# Translate: just output the target ID (pipe-friendly)
reep translate transfermarkt 568177 fbref
# dc7f8a28

# Download CSVs for offline use
reep download

# Search offline
reep local "Salah"
```

## Source

Most data is extracted from [Wikidata](https://www.wikidata.org/) via SPARQL. Wikidata is a free, collaborative knowledge base maintained by thousands of volunteers. The cross-provider ID mappings exist because the Wikidata community has systematically added external identifier properties for football data sources.

Entities not in Wikidata (e.g. lower-league players) are sourced from authoritative provider databases like Opta's F1 player database.

### Wikidata properties used

| Property | Provider |
|----------|----------|
| [P2446](https://www.wikidata.org/wiki/Property:P2446) | Transfermarkt player ID |
| [P2447](https://www.wikidata.org/wiki/Property:P2447) | Transfermarkt manager ID |
| [P7223](https://www.wikidata.org/wiki/Property:P7223) | Transfermarkt team ID |
| [P5750](https://www.wikidata.org/wiki/Property:P5750) | FBref player ID |
| [P8642](https://www.wikidata.org/wiki/Property:P8642) | FBref squad ID |
| [P2369](https://www.wikidata.org/wiki/Property:P2369) | Soccerway person ID |
| [P6131](https://www.wikidata.org/wiki/Property:P6131) | Soccerway team ID |
| [P12302](https://www.wikidata.org/wiki/Property:P12302) | Sofascore player ID |
| [P8259](https://www.wikidata.org/wiki/Property:P8259) | Flashscore player ID |
| [P12539](https://www.wikidata.org/wiki/Property:P12539) | Premier League player ID |
| [P12551](https://www.wikidata.org/wiki/Property:P12551) | 11v11 player ID |
| [P3681](https://www.wikidata.org/wiki/Property:P3681) | ESPN FC player ID |
| [P2574](https://www.wikidata.org/wiki/Property:P2574) | National Football Teams ID |
| [P2020](https://www.wikidata.org/wiki/Property:P2020) | WorldFootball.net ID |
| [P2193](https://www.wikidata.org/wiki/Property:P2193) | Soccerbase player ID |
| [P2276](https://www.wikidata.org/wiki/Property:P2276) | UEFA player ID |
| [P7361](https://www.wikidata.org/wiki/Property:P7361) | UEFA team ID |
| [P3665](https://www.wikidata.org/wiki/Property:P3665) | L'Equipe player ID |
| [P9264](https://www.wikidata.org/wiki/Property:P9264) | FFF.fr player ID |
| [P13064](https://www.wikidata.org/wiki/Property:P13064) | Lega Serie A player ID |
| [P12577](https://www.wikidata.org/wiki/Property:P12577) | BeSoccer player ID |
| [P3537](https://www.wikidata.org/wiki/Property:P3537) | FootballDatabase.eu person ID |
| [P7351](https://www.wikidata.org/wiki/Property:P7351) | FootballDatabase.eu team ID |
| [P3726](https://www.wikidata.org/wiki/Property:P3726) | EU-Football.info player ID |
| [P12606](https://www.wikidata.org/wiki/Property:P12606) | Barry Hugman's Footballers ID |
| [P4023](https://www.wikidata.org/wiki/Property:P4023) | German FA person ID |
| [P12567](https://www.wikidata.org/wiki/Property:P12567) | StatMuse PL player ID |
| [P12312](https://www.wikidata.org/wiki/Property:P12312) | Kicker team ID |
| [P7876](https://www.wikidata.org/wiki/Property:P7876) | Flashscore team ID |
| [P13897](https://www.wikidata.org/wiki/Property:P13897) | Sofascore team ID |
| [P7454](https://www.wikidata.org/wiki/Property:P7454) | Soccerbase team ID |
| [P7287](https://www.wikidata.org/wiki/Property:P7287) | WorldFootball.net team ID |
| [P1469](https://www.wikidata.org/wiki/Property:P1469) | SoFIFA / EA FC player ID |
| [P4381](https://www.wikidata.org/wiki/Property:P4381) | Soccerdonna player ID (women's football) |
| [P8134](https://www.wikidata.org/wiki/Property:P8134) | Soccerdonna coach ID |
| [P11379](https://www.wikidata.org/wiki/Property:P11379) | Dongqiudi player ID |
| [P7280](https://www.wikidata.org/wiki/Property:P7280) | PlaymakerStats team ID |
| [P12758](https://www.wikidata.org/wiki/Property:P12758) | Transfermarkt competition ID |
| [P13664](https://www.wikidata.org/wiki/Property:P13664) | FBref competition ID |
| [P8735](https://www.wikidata.org/wiki/Property:P8735) | Opta competition ID |

### Provider notes

**Opta / Stats Perform** — Three distinct ID systems exist within Stats Perform's ecosystem:
- **`opta`** — 25-char alphanumeric UUIDs (e.g. `7cwgrmorsb42qaj5vrhp8fhzp`) from the current SD API / Stats Perform F1 database. Used for players (50K), teams, competitions, and seasons. This is the canonical Opta provider.
- **`opta_numeric`** — Legacy numeric codes (e.g. `244851` for Cole Palmer). Same as FPL `code` field, The Analyst `sc-` URL IDs, and Wikidata P8735/P8736/P8737. Players (3.8K), teams (255), competitions (73), coaches (28). Sources: FPL data, Wikidata dump, Opta Web Archive feeds.
- **`optacore`** — A separate numeric system with different numbers (e.g. FA Cup = 93 vs opta_numeric 1). Competitions only (47). From the SD API mapping file.

**WorldFootball.net / heim:spiel** — WorldFootball.net (owned by heim:spiel) migrated from slug-based URLs (e.g. `cole-palmer`) to numeric IDs in November 2025. The old slugs still work via redirect. Wikidata P2020 contains the old slug format. The heim:spiel numeric IDs in Reep are the same as the new WorldFootball.net IDs — the URL prefix indicates entity type:

| Type | WorldFootball.net URL | heim:spiel ID |
|------|----------------------|---------------|
| Player | `/pe426937` | `426937` |
| Team | `/te1672` | `1672` |
| Competition | `/co91` | `91` |
| Match | `/ma10988177` | `10988177` |

**Soccerway / Flashscore** — Both owned by [Livesport](https://www.livesport.eu/) (Czech data company), but use separate ID systems. Soccerway changed their URL/ID scheme in September 2025. Reep has 139K Soccerway IDs in the old numeric format from Wikidata P2369 (e.g. `45569`). The old URLs still redirect:

| Format | URL | ID |
|--------|-----|----|
| Old (numeric) | `int.soccerway.com/players/-/45569/` | `45569` |
| New (slug) | `soccerway.com/player/zver-mateja/p0DFdwlo/` | `p0DFdwlo` |

A new Wikidata property for the new format has been proposed but not yet approved.

## Updates

**The v0 register is no longer refreshed.** The last data release was `2026.25`, generated on 21 June 2026, and the D1 database behind the API has taken no writes since 25 April 2026. No further refreshes are planned, and reported faults are **not** back-ported here — they are fixed in [the v1 register](https://reep.football/downloads) instead. The `data/meta.json` file records when the current CSVs were generated.

Historically, v0 was refreshed periodically from Wikidata, plus monthly reconciliation against the full Wikidata dump to catch drift (deleted entities, lost occupations, missed IDs). Each refresh picked up new entities, updated IDs, and corrections made by the Wikidata community, and custom provider mappings persisted across refreshes.

## Contributing

### Share ID mappings

Have a dataset that maps football player or team IDs across providers? We'd love to include it. Send us a CSV with these columns:

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `provider` | Yes | Provider name | `wyscout` |
| `external_id` | Yes | The player/team ID on that provider | `12345` |
| `name` | Yes | Player or team name (for validation) | `Cole Palmer` |
| `date_of_birth` | Recommended | ISO date (helps us match accurately) | `2002-05-06` |
| `transfermarkt_id` | Recommended | Transfermarkt ID (best for accurate matching) | `568177` |
| `type` | Recommended | `player`, `team`, or `coach` | `player` |
| `nationality` | Optional | Country (helps disambiguate) | `England` |

The more columns you include, the more accurately we can match to existing entities. A Transfermarkt ID or date of birth alone is usually enough.

**How to submit:**
- [Open an issue](https://github.com/withqwerty/reep/issues/new) with your CSV attached or linked
- Email getintouch@withqwerty.com if you prefer to contribute anonymously

We validate and match all submissions before adding them. Your IDs go into our verified custom mappings. Note that submissions now land in [the v1 register](https://reep.football/downloads) — the frozen v0 files and API here are not updated.

### Edit Wikidata directly

If a player is missing a Transfermarkt ID or FBref ID, the ideal fix is to add it to their [Wikidata](https://www.wikidata.org/) page — the next v1 refresh picks it up automatically. The frozen v0 files here will not pick it up.

- [How to edit Wikidata](https://www.wikidata.org/wiki/Wikidata:Introduction)
- [Add an external identifier](https://www.wikidata.org/wiki/Help:Statements#Adding_statements)

Wikidata requires ~50 manual edits and a 4-day waiting period before bulk edits are possible. If you have a large dataset, send it to us (see above) and we'll handle the Wikidata submission on your behalf.

### Code contributions

PRs to the Worker (`src/`) and documentation are welcome in this repo. CLI PRs belong in [withqwerty/reep-cli](https://github.com/withqwerty/reep-cli). Note that the data CSVs are regenerated upstream from Wikidata + verified mappings — don't PR data changes directly.

### What this repo doesn't contain

This repo publishes IDs, the API, and the published CSVs — not scraping logic or raw data dumps from providers. Matching and ingestion scripts are maintained in a separate private repo.

## Support the register

Keeping a football identity register true is continuous work: cutting and publishing
releases, repairing wrong merges, adding competitions and sources, and measuring
accuracy on every release. That upkeep is funded by sponsorship, and the record of it
is public — releases published, corrections applied and measured precision and recall
are all on [reep.football/sponsorship](https://reep.football/sponsorship).

Sponsorship buys acknowledgement and nothing else. It never buys favourable identity
decisions, suppressed corrections, exclusive fields or earlier access, and a data
provider that sponsors the register is named publicly as a provider sponsor. The full
terms are in the [independence policy](https://reep.football/sponsorship-independence).

Reporting a correction is worth as much as money. If you spot a wrong ID or a bad
merge, [open an issue](https://github.com/withqwerty/reep/issues/new?template=report-correction.yml).

## License

The data is derived from [Wikidata](https://www.wikidata.org/) and is available under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).
