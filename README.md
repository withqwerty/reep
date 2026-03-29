# Reep

[![CI](https://github.com/withqwerty/reep/actions/workflows/ci.yml/badge.svg)](https://github.com/withqwerty/reep/actions/workflows/ci.yml)

![Charles Reep's match notations from 1953](https://ichef.bbci.co.uk/ace/standard/624/cpsprodpb/FC93/production/_124995646_bbc1953notations.jpg)

The football entity register. Maps player, team, and coach identities across Transfermarkt, FBref, UEFA, Sofascore, and 30+ data providers.

Named after [Charles Reep](https://en.wikipedia.org/wiki/Charles_Reep) (1904--2002), an RAF wing commander who hand-recorded every action in over 2,200 football matches starting in the 1950s. He's considered the grandfather of football analytics -- decades before expected goals or tracking data, Reep was tallying passes, shots, and sequences with pen and paper, pioneering the idea that football could be understood through data.

## What is this?

A canonical identity file for football. Every person and club gets a stable [Wikidata](https://www.wikidata.org/) QID, linked to their IDs on other platforms. If you have a Transfermarkt ID and need the FBref ID for the same player, this register gives you the answer.

Think of it as the football equivalent of the [Chadwick Baseball Bureau Register](https://github.com/chadwickbureau/register).

## Data

| File | Records | Description |
|------|---------|-------------|
| [`data/people.csv`](data/people.csv) | ~430K | Players and coaches with provider IDs and bio |
| [`data/teams.csv`](data/teams.csv) | ~45K | Clubs with provider IDs and metadata |
| [`data/names.csv`](data/names.csv) | varies | Alternate names and aliases |
| [`data/meta.json`](data/meta.json) | — | Generation timestamp and counts |

### People schema

| Column | Description | Example |
|--------|-------------|---------|
| `key_wikidata` | Wikidata QID (canonical key) | `Q99760796` |
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
| `key_opta` | Opta player ID | — |
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

### Teams schema

| Column | Description | Example |
|--------|-------------|---------|
| `key_wikidata` | Wikidata QID | `Q9616` |
| `name` | Primary English name | `Arsenal F.C.` |
| `country` | Country | `United Kingdom` |
| `founded` | Founding date | `1886-10-01` |
| `stadium` | Home ground | `Emirates Stadium` |
| `key_transfermarkt` | Transfermarkt team ID | `11` |
| `key_fbref` | FBref squad ID | `18bb7c10` |
| `key_soccerway` | Soccerway team ID | `660` |
| `key_opta` | Opta team ID | — |
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

### Names schema

| Column | Description | Example |
|--------|-------------|---------|
| `key_wikidata` | Wikidata QID | `Q11893` |
| `name` | Primary name | `Cristiano Ronaldo` |
| `alias` | Alternate name | `Cristiano Ronaldo dos Santos Aveiro` |

## Coverage

Not every entity has every ID. Coverage depends on what the Wikidata community has mapped plus custom verified mappings:

| Provider | Player coverage | Source | Notes |
|----------|----------------|--------|-------|
| Transfermarkt | Best | Wikidata | Highest coverage across all entities |
| FBref | Good | Wikidata | Strong for recent players |
| Soccerway | Good | Wikidata | Broad international coverage |
| Sofascore | Good | Wikidata | Modern players well covered |
| Opta | Sparse | Wikidata | Few entries have Opta IDs in Wikidata |
| Premier League | Decent | Wikidata | PL players only |
| Understat | ~2.3K | Custom | Matched via Transfermarkt bridge |
| WhoScored | ~2.3K | Custom | Matched via Transfermarkt bridge |
| SportMonks | ~600 | Custom | Players + teams via TM bridge |
| API-Football | Growing | Custom | Name + DOB matching |
| Club Elo | ~176 teams | Custom | Manual team mapping |

IDs sourced from Wikidata are community-maintained. Custom IDs are verified independently — see the [Reep API](#api) for methodology details.

## Usage

### Python

```python
import csv

# Load people into a dict keyed by Transfermarkt ID
people = {}
with open("data/people.csv") as f:
    for row in csv.DictReader(f):
        tm_id = row["key_transfermarkt"]
        if tm_id:
            people[tm_id] = row

# Look up Cole Palmer's FBref ID from his Transfermarkt ID
palmer = people["568177"]
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
  select(name, key_fbref, key_sofascore)
```

### SQL (load into SQLite)

```bash
sqlite3 reep.db <<EOF
.mode csv
.import data/people.csv people
.import data/teams.csv teams
.import data/names.csv names
EOF
```

```sql
-- Find all IDs for a player
SELECT * FROM people WHERE name LIKE '%Salah%';

-- Reverse lookup: FBref ID -> everything
SELECT * FROM people WHERE key_fbref = 'e342ad68';
```

## API

The Reep API provides the same data as the CSVs via a convenient REST interface. All providers (Wikidata + custom verified) are available to all plans.

**Base URL:** `https://reep-api.rahulkeerthi2-95d.workers.dev`

| Endpoint | Description | Example |
|----------|-------------|---------|
| `GET /search` | Search by name | `/search?name=Cole Palmer&type=player` |
| `GET /resolve` | Translate provider ID | `/resolve?provider=transfermarkt&id=568177` |
| `GET /lookup` | Look up by Wikidata QID | `/lookup?qid=Q99760796` |
| `GET /stats` | Database statistics | `/stats` |

Available on [RapidAPI](https://rapidapi.com/withqwerty-withqwerty-default/api/the-reep-register).

## CLI

```bash
# Search by name
python cli/reep.py search "Cole Palmer"

# Resolve: Transfermarkt -> all IDs
python cli/reep.py resolve transfermarkt 568177

# Translate: just output the target ID (pipe-friendly)
python cli/reep.py translate transfermarkt 568177 fbref
# dc7f8a28

# Download CSVs for offline use
python cli/reep.py download

# Search offline
python cli/reep.py local "Salah"
```

## Source

All data is extracted from [Wikidata](https://www.wikidata.org/) via SPARQL. Wikidata is a free, collaborative knowledge base maintained by thousands of volunteers. The cross-provider ID mappings exist because the Wikidata community has systematically added external identifier properties for football data sources.

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
| [P8736](https://www.wikidata.org/wiki/Property:P8736) | Opta player ID |
| [P8737](https://www.wikidata.org/wiki/Property:P8737) | Opta team ID |
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

## Updates

The register is rebuilt weekly from Wikidata. Each release picks up new entities, updated IDs, and corrections made by the Wikidata community.

## Contributing

The best way to improve this register is to **edit Wikidata directly**. If a player is missing a Transfermarkt ID or FBref ID, add it to their Wikidata page. The next weekly build will pick it up automatically.

- [How to edit Wikidata](https://www.wikidata.org/wiki/Wikidata:Introduction)
- [Add an external identifier](https://www.wikidata.org/wiki/Help:Statements#Adding_statements)

## License

The data is derived from [Wikidata](https://www.wikidata.org/) and is available under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).
