"""
Extract all football entities from Wikidata with cross-provider external IDs.
Outputs JSON files ready for D1 seeding.

Two-phase approach to avoid SPARQL timeouts:
  Phase 1: Names + external IDs (light query)
  Phase 2: Bio details fetched in batches by QID (heavier but targeted)

Usage:
  python scripts/fetch-wikidata-entities.py                  # full extraction
  python scripts/fetch-wikidata-entities.py --test 10        # test with 10 entities per type
  python scripts/fetch-wikidata-entities.py --type player    # single entity type
  python scripts/fetch-wikidata-entities.py --ids-only       # skip bio phase
"""

import argparse
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "reep-football-register/1.0 (https://github.com/withqwerty/reep)"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "json"

# External ID properties per entity type
#
# Excluded Wikidata properties (reviewed and skipped):
#   P12924  365scores player ID          — only 200 entities, too small
#   P12939  365scores team ID            — only 6 entities
#   P13901  Foot Mercato player ID       — only 296 entities, too small
#   P13665  FBref match ID              — only 4 entities, match entity type not supported
#   P7455   Transfermarkt match ID      — 26K but match entity type not supported yet
#   P7460   Flashscore match ID         — only 22 entities
#   P8736   Opta player ID              — numeric IDs, replaced by alphanumeric Opta IDs via custom matching
#   P8737   Opta team ID               — numeric IDs, replaced by alphanumeric Opta IDs via custom matching
#   P5628   Football.it female player   — redundant with Soccerdonna (P4381)
#   P7878   Soccerdonna team ID         — not checked, women's team coverage TBD
PLAYER_IDS = {
    "transfermarkt": "P2446",
    "fbref": "P5750",
    "soccerway": "P2369",
    "sofascore": "P12302",
    "flashscore": "P8259",
    "premier_league": "P12539",
    "11v11": "P12551",
    "espn": "P3681",
    "national_football_teams": "P2574",
    "worldfootball": "P2020",
    "soccerbase": "P2193",
    "kicker": "P8912",
    # New providers
    "uefa": "P2276",
    "lequipe": "P3665",
    "fff_fr": "P9264",
    "serie_a": "P13064",
    "besoccer": "P12577",
    "footballdatabase_eu": "P3537",
    "eu_football_info": "P3726",
    "hugman": "P12606",
    "german_fa": "P4023",
    "statmuse_pl": "P12567",
    "sofifa": "P1469",
    "soccerdonna": "P4381",
    "dongqiudi": "P11379",
}

TEAM_IDS = {
    "transfermarkt": "P7223",
    "fbref": "P8642",
    "soccerway": "P6131",
    "espn": "P13590",
    # New providers
    "kicker": "P12312",
    "flashscore": "P7876",
    "sofascore": "P13897",
    "soccerbase": "P7454",
    "uefa": "P7361",
    "footballdatabase_eu": "P7351",
    "worldfootball": "P7287",
    "playmakerstats": "P7280",
}

COACH_IDS = {
    "transfermarkt_manager": "P2447",
    "transfermarkt_player": "P2446",
    "fbref": "P5750",
    "soccerway": "P2369",
    "soccerbase": "P2195",
    "soccerdonna": "P8134",
}

COMPETITION_IDS = {
    "transfermarkt": "P12758",
    "fbref": "P13664",
    "opta": "P8735",
}

# Season provider IDs — may be empty if Wikidata has no dedicated season properties.
# Season cross-referencing will rely on custom_ids (derived from competition ID + year).
SEASON_IDS: dict[str, str] = {}

BIO_BATCH_SIZE = 200  # QIDs per bio-detail batch
PAGE_SIZE = 50000  # SPARQL pagination size


def sparql_query(query: str, retries: int = 5, expected_min: int = 0) -> list[dict]:
    """Execute a SPARQL query against Wikidata via POST.

    Uses JSON format with strict=False to handle control characters.
    Retries on malformed JSON (corrupt responses from Wikidata).
    """
    body = urllib.parse.urlencode({"query": query}).encode("utf-8")
    req = urllib.request.Request(
        ENDPOINT,
        data=body,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/sparql-results+json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode(), strict=False)
            rows = []
            for binding in data["results"]["bindings"]:
                row = {}
                for key, val in binding.items():
                    row[key] = val["value"]
                rows.append(row)
            return rows
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code}: {e.reason}")
            if e.code == 429:
                wait = 60 * (attempt + 1)
                print(f"  Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            if e.code in (500, 502, 503) and attempt < retries:
                print(f"  Server error. Retrying in 15s... (attempt {attempt + 1}/{retries})")
                time.sleep(15)
                continue
            if attempt < retries:
                print(f"  Retrying in 10s... (attempt {attempt + 1}/{retries})")
                time.sleep(10)
                continue
            raise
        except (urllib.error.URLError, ConnectionError, Exception) as e:
            if attempt < retries:
                print(f"  Connection error: {e}. Retrying in 10s...")
                time.sleep(10)
                continue
            raise
    return []


def parse_tsv_results(text: str) -> list[dict]:
    """Parse SPARQL TSV results into list of dicts.

    TSV format: first line is ?var1\\t?var2\\t... headers,
    subsequent lines are values. URIs are wrapped in <>, strings in quotes.
    """
    lines = text.strip().split("\n")
    if not lines:
        return []

    # Headers: strip leading ? from variable names
    headers = [h.lstrip("?") for h in lines[0].split("\t")]

    rows = []
    for line in lines[1:]:
        if not line.strip():
            continue
        values = line.split("\t")
        row = {}
        for i, val in enumerate(values):
            if i >= len(headers):
                break
            val = val.strip()
            if not val:
                continue
            # Strip URI brackets: <http://www.wikidata.org/entity/Q123> → http://...
            if val.startswith("<") and val.endswith(">"):
                val = val[1:-1]
            # Strip string quotes: "value" → value
            elif val.startswith('"'):
                # Handle "value"@en or "value"^^<type>
                if '"@' in val:
                    val = val[1:val.rindex('"@')]
                elif '"^^' in val:
                    val = val[1:val.rindex('"^^')]
                elif val.endswith('"'):
                    val = val[1:-1]
            row[headers[i]] = val
        if row:
            rows.append(row)

    # Drop last row if it has fewer columns than the header (truncated response)
    if rows and len(rows[-1]) < len(headers) // 2:
        rows.pop()

    return rows


def sparql_query_paginated(query_fn, limit: int = 0) -> list[dict]:
    """Fetch all results using OFFSET/LIMIT pagination for large datasets."""
    if limit and limit <= PAGE_SIZE:
        return sparql_query(query_fn(limit=limit, offset=0))

    all_rows = []
    offset = 0
    while True:
        page_limit = min(PAGE_SIZE, limit - len(all_rows)) if limit else PAGE_SIZE
        print(f"    Page at offset {offset}...", end=" ", flush=True)
        query = query_fn(limit=page_limit, offset=offset)
        rows = sparql_query(query)
        print(f"{len(rows)} rows")
        all_rows.extend(rows)
        if len(rows) < page_limit:
            break  # Last page
        if limit and len(all_rows) >= limit:
            break
        offset += PAGE_SIZE
        time.sleep(3)  # Be polite between pages
    return all_rows


def extract_qid(uri: str) -> str:
    return uri.split("/")[-1]


# ---------------------------------------------------------------------------
# Phase 1: Names + External IDs (light queries, no cross-product explosion)
# ---------------------------------------------------------------------------

def build_player_ids_query(limit: int = 0, offset: int = 0) -> str:
    id_optionals = "\n".join(
        f"  OPTIONAL {{ ?e wdt:{prop} ?id_{name} . }}"
        for name, prop in PLAYER_IDS.items()
    )
    id_selects = " ".join(f"?id_{name}" for name in PLAYER_IDS)
    limit_clause = f"LIMIT {limit}" if limit else ""
    offset_clause = f"OFFSET {offset}" if offset else ""

    # Subquery fetches QIDs first, then OPTIONALs + labels applied outside
    return f"""
SELECT ?e ?eLabel {id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e WHERE {{
      ?e wdt:P106 wd:Q937857 .
      FILTER NOT EXISTS {{ ?e wdt:P31 wd:Q95074 }}
      FILTER NOT EXISTS {{ ?e wdt:P31 wd:Q15632617 }}
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_team_ids_query(limit: int = 0, offset: int = 0) -> str:
    id_optionals = "\n".join(
        f"  OPTIONAL {{ ?e wdt:{prop} ?id_{name} . }}"
        for name, prop in TEAM_IDS.items()
    )
    id_selects = " ".join(f"?id_{name}" for name in TEAM_IDS)
    limit_clause = f"LIMIT {limit}" if limit else ""
    offset_clause = f"OFFSET {offset}" if offset else ""

    # Use reverse traversal for P279* (optimization: ^wdt:P279* is cheaper)
    return f"""
SELECT ?e ?eLabel {id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e WHERE {{
      ?e wdt:P31 ?type .
      ?type (wdt:P279)* wd:Q476028 .
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_coach_ids_query(limit: int = 0, offset: int = 0) -> str:
    id_optionals = "\n".join(
        f"  OPTIONAL {{ ?e wdt:{prop} ?id_{name} . }}"
        for name, prop in COACH_IDS.items()
    )
    id_selects = " ".join(f"?id_{name}" for name in COACH_IDS)
    limit_clause = f"LIMIT {limit}" if limit else ""
    offset_clause = f"OFFSET {offset}" if offset else ""

    return f"""
SELECT ?e ?eLabel {id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e WHERE {{
      ?e wdt:P106 wd:Q628099 .
      FILTER NOT EXISTS {{ ?e wdt:P31 wd:Q95074 }}
      FILTER NOT EXISTS {{ ?e wdt:P31 wd:Q15632617 }}
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_competition_ids_query(limit: int = 0, offset: int = 0) -> str:
    id_optionals = "\n".join(
        f"  OPTIONAL {{ ?e wdt:{prop} ?id_{name} . }}"
        for name, prop in COMPETITION_IDS.items()
    )
    id_selects = " ".join(f"?id_{name}" for name in COMPETITION_IDS)
    limit_clause = f"LIMIT {limit}" if limit else ""
    offset_clause = f"OFFSET {offset}" if offset else ""

    # Union: class-based (Q15991290 subclasses) + property-based (items with competition IDs).
    # Many competitions have FBref/Opta IDs but aren't typed as Q15991290 subclasses.
    # Filter: only items whose sport (P641) is association football (Q2736) or unspecified.
    # This is a positive filter (allow football) rather than a negative one (block non-football),
    # because Wikidata's non-football sport taxonomy is too fragmented to enumerate reliably.
    prop_unions = "\n      UNION\n".join(
        f"      {{ ?e wdt:{prop} [] . }}" for prop in COMPETITION_IDS.values()
    )
    return f"""
SELECT ?e ?eLabel {id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e WHERE {{
      {{ ?e wdt:P31/wdt:P279* wd:Q15991290 . }}
      UNION
{prop_unions}
      # Only allow association football (Q2736) or items with no sport specified
      FILTER NOT EXISTS {{
        ?e wdt:P641 ?sport .
        FILTER(?sport != wd:Q2736)
      }}
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_season_ids_query(limit: int = 0, offset: int = 0) -> str:
    # Seasons are items with P3450 (sports season of league) pointing to a football competition.
    # SEASON_IDS may be empty — seasons often lack dedicated provider properties.
    id_optionals = "\n".join(
        f"  OPTIONAL {{ ?e wdt:{prop} ?id_{name} . }}"
        for name, prop in SEASON_IDS.items()
    )
    id_selects = (" " + " ".join(f"?id_{name}" for name in SEASON_IDS)) if SEASON_IDS else ""
    limit_clause = f"LIMIT {limit}" if limit else ""
    offset_clause = f"OFFSET {offset}" if offset else ""

    # Only allow seasons of football competitions (P641 = Q2736 or no sport specified)
    return f"""
SELECT ?e ?eLabel ?competitionQid{id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e ?competitionQid WHERE {{
      ?e wdt:P3450 ?comp .
      ?comp wdt:P31/wdt:P279* wd:Q15991290 .
      BIND(?comp AS ?competitionQid)
      FILTER NOT EXISTS {{
        ?comp wdt:P641 ?sport .
        FILTER(?sport != wd:Q2736)
      }}
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def parse_ids_phase(rows: list[dict], entity_type: str, id_props: dict) -> dict[str, dict]:
    """Parse Phase 1 results into entity dict keyed by QID."""
    entities: dict[str, dict] = {}
    for row in rows:
        qid = extract_qid(row.get("e", ""))
        if not qid or not qid.startswith("Q"):
            continue
        if qid not in entities:
            entities[qid] = {
                "qid": qid,
                "type": entity_type,
                "name_en": row.get("eLabel", ""),
                "aliases_en": None,
                "full_name": None,
                "date_of_birth": None,
                "nationality": None,
                "position": None,
                "current_team_qid": None,
                "height_cm": None,
                "country": None,
                "founded": None,
                "stadium": None,
                "competition_qid": None,
                "external_ids": {},
            }
        # Capture competition QID for seasons (from SPARQL result)
        comp_qid_uri = row.get("competitionQid")
        if comp_qid_uri and entity_type == "season":
            comp_qid = extract_qid(comp_qid_uri)
            if comp_qid.startswith("Q"):
                entities[qid]["competition_qid"] = comp_qid
        for name in id_props:
            val = row.get(f"id_{name}")
            if val and name not in entities[qid]["external_ids"]:
                entities[qid]["external_ids"][name] = val
    return entities


# ---------------------------------------------------------------------------
# Phase 2: Bio details in batches
# ---------------------------------------------------------------------------

def build_player_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?birthName ?dob ?nationalityLabel ?positionLabel ?heightAmount
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P1477 ?birthName . FILTER(LANG(?birthName) = "en") }}
  OPTIONAL {{ ?e wdt:P569 ?dob . }}
  OPTIONAL {{ ?e wdt:P1532 ?sportNat . }}
  OPTIONAL {{ ?e wdt:P27 ?citizenship . }}
  BIND(COALESCE(?sportNat, ?citizenship) AS ?nationality)
  OPTIONAL {{ ?e wdt:P413 ?position . }}
  OPTIONAL {{
    ?e p:P2048 ?hStmt .
    ?hStmt psv:P2048 ?hVal .
    ?hVal wikibase:quantityAmount ?heightAmount .
    ?hVal wikibase:quantityUnit wd:Q174728 .
  }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_team_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?countryLabel ?founded ?stadiumLabel
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P17 ?country . }}
  OPTIONAL {{ ?e wdt:P571 ?founded . }}
  OPTIONAL {{ ?e wdt:P115 ?stadium . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_coach_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?birthName ?dob ?nationalityLabel
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P1477 ?birthName . FILTER(LANG(?birthName) = "en") }}
  OPTIONAL {{ ?e wdt:P569 ?dob . }}
  OPTIONAL {{ ?e wdt:P1532 ?sportNat . }}
  OPTIONAL {{ ?e wdt:P27 ?citizenship . }}
  BIND(COALESCE(?sportNat, ?citizenship) AS ?nationality)
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_competition_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?countryLabel
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P17 ?country . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def build_season_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?competitionQid
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P3450 ?competitionQid . }}
}}
"""


def merge_bio(entities: dict[str, dict], bio_rows: list[dict], entity_type: str):
    """Merge Phase 2 bio results into entity dicts."""
    # Collect aliases per QID
    aliases: dict[str, set] = {}

    for row in bio_rows:
        qid = extract_qid(row.get("e", ""))
        if qid not in entities:
            continue
        e = entities[qid]

        # Aliases
        alt = row.get("altLabels")
        if alt:
            aliases.setdefault(qid, set()).add(alt)

        # Only set if not already set (first row wins for scalar fields)
        if not e["full_name"] and row.get("birthName"):
            e["full_name"] = row["birthName"]

        if not e["date_of_birth"] and row.get("dob"):
            dob = row["dob"]
            e["date_of_birth"] = dob.split("T")[0] if "T" in dob else dob

        if not e["nationality"] and row.get("nationalityLabel"):
            e["nationality"] = row["nationalityLabel"]

        if entity_type == "player":
            if not e["position"] and row.get("positionLabel"):
                e["position"] = row["positionLabel"]
            if not e["height_cm"] and row.get("heightAmount"):
                try:
                    e["height_cm"] = float(row["heightAmount"])
                except ValueError:
                    pass

        if entity_type == "team":
            if not e["country"] and row.get("countryLabel"):
                e["country"] = row["countryLabel"]
            if not e["founded"] and row.get("founded"):
                f = row["founded"]
                e["founded"] = f.split("T")[0] if "T" in f else f
            if not e["stadium"] and row.get("stadiumLabel"):
                e["stadium"] = row["stadiumLabel"]

        if entity_type == "competition":
            if not e.get("country") and row.get("countryLabel"):
                e["country"] = row["countryLabel"]

        if entity_type == "season":
            # Competition QID from bio query (backup — primary source is IDs phase)
            if not e.get("competition_qid") and row.get("competitionQid"):
                comp_uri = row["competitionQid"]
                comp_qid = comp_uri.split("/")[-1] if "/" in comp_uri else comp_uri
                if comp_qid.startswith("Q"):
                    e["competition_qid"] = comp_qid

    # Apply aliases
    for qid, alias_set in aliases.items():
        if qid in entities:
            entities[qid]["aliases_en"] = ", ".join(sorted(alias_set))


def fetch_bio_batched(entities: dict[str, dict], entity_type: str, bio_query_fn):
    """Fetch bio details in batches of BATCH_SIZE."""
    qids = list(entities.keys())
    total = len(qids)
    for i in range(0, total, BIO_BATCH_SIZE):
        batch = qids[i : i + BIO_BATCH_SIZE]
        print(f"  Bio batch {i // BIO_BATCH_SIZE + 1}/{(total + BIO_BATCH_SIZE - 1) // BIO_BATCH_SIZE} ({len(batch)} entities)...")
        query = bio_query_fn(batch)
        rows = sparql_query(query)
        merge_bio(entities, rows, entity_type)
        if i + BIO_BATCH_SIZE < total:
            time.sleep(2)


def main():
    parser = argparse.ArgumentParser(description="Extract football entities from Wikidata")
    parser.add_argument("--test", type=int, default=0, help="Limit per entity type (0 = all)")
    parser.add_argument("--type", choices=["player", "team", "coach", "competition", "season"], help="Single entity type")
    parser.add_argument("--ids-only", action="store_true", help="Skip bio details phase")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    type_configs = {
        "player": (build_player_ids_query, PLAYER_IDS, build_player_bio_query),
        "team": (build_team_ids_query, TEAM_IDS, build_team_bio_query),
        "coach": (build_coach_ids_query, COACH_IDS, build_coach_bio_query),
        "competition": (build_competition_ids_query, COMPETITION_IDS, build_competition_bio_query),
        "season": (build_season_ids_query, SEASON_IDS, build_season_bio_query),
    }

    if args.type:
        type_configs = {args.type: type_configs[args.type]}

    for entity_type, (ids_query_fn, id_props, bio_query_fn) in type_configs.items():
        print(f"\n{'='*60}")
        print(f"Phase 1: Fetching {entity_type} names + IDs (limit={args.test or 'all'})...")
        print(f"{'='*60}")

        rows = sparql_query_paginated(ids_query_fn, limit=args.test)
        print(f"  Raw rows: {len(rows)}")

        entities = parse_ids_phase(rows, entity_type, id_props)
        print(f"  Unique entities: {len(entities)}")

        if entities:
            sample = list(entities.values())[0]
            print(f"  Sample: {sample['name_en']} ({sample['qid']})")
            ids_preview = {k: v for k, v in sample["external_ids"].items()}
            print(f"    IDs: {json.dumps(ids_preview)}")

        if not args.ids_only and entities:
            print(f"\nPhase 2: Fetching bio details...")
            fetch_bio_batched(entities, entity_type, bio_query_fn)

            # Show enriched sample
            sample = list(entities.values())[0]
            print(f"  Enriched sample: {sample['name_en']}")
            print(f"    DOB: {sample['date_of_birth']}, Nationality: {sample['nationality']}")
            if entity_type == "player":
                print(f"    Position: {sample['position']}, Height: {sample['height_cm']}")
            print(f"    Aliases: {sample['aliases_en']}")

        # Save
        out_path = OUTPUT_DIR / f"{entity_type}s.json"
        with open(out_path, "w") as f:
            json.dump(list(entities.values()), f, indent=2, ensure_ascii=False)
        print(f"  Saved {len(entities)} entities to {out_path}")

        if len(type_configs) > 1:
            print("  Sleeping 5s between types...")
            time.sleep(5)

    print(f"\nDone! Files in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
