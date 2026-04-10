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
    # NB: P2446 (transfermarkt player ID) used to be stored here under the
    # `transfermarkt_player` provider name for coaches with a prior playing
    # career. That created a coach-only provider namespace that nothing else
    # queries and triggered same-type-duplicate validation noise. Removed
    # 2026-04-10 along with a one-time D1 cleanup of 239 orphan rows.
    "fbref": "P5750",
    "soccerway": "P2369",
    "soccerbase": "P2195",
    "soccerdonna": "P8134",
}

COMPETITION_IDS = {
    "transfermarkt": "P12758",
    "fbref": "P13664",
    # P8735 is Opta's legacy numeric competition codes (e.g. 8 = PL). Ingested under
    # 'opta_numeric' to preserve the distinction from canonical 'opta' UUIDs (Stats
    # Perform F1 / The Analyst). Two separate provider names for two distinct ID systems.
    "opta_numeric": "P8735",
}

# Season provider IDs — may be empty if Wikidata has no dedicated season properties.
# Season cross-referencing will rely on custom_ids (derived from competition ID + year).
SEASON_IDS: dict[str, str] = {}

BIO_BATCH_SIZE = 200  # QIDs per bio-detail batch
PAGE_SIZE = 50000  # SPARQL pagination size

# Multi-language fallback chain for SPARQL label service. When an entity
# has no English label, Wikidata's label service falls back to the QID
# itself ("Q82595"), which then gets stored as a broken name until
# backfill-broken-names.py catches it. Listing fallback languages in
# priority order lets the label service return an actual human-readable
# name for most entities at ingest time.
#
# Priority: English → Romance + Germanic (latin script, easy to read) →
# Slavic + Turkic → Asian + Arabic + Hebrew. Covers ~99% of football
# entities worldwide.
LABEL_LANGS = (
    "en,es,pt,ca,de,fr,it,nl,da,sv,no,fi,"
    "pl,cs,sk,hr,sr,ro,hu,uk,ru,tr,"
    "ja,ko,zh,ar,he,fa,id,vi,th"
)


def sparql_query(query: str, retries: int = 5, expected_min: int = 0) -> list[dict]:
    """Execute a SPARQL query against Wikidata via POST.

    Uses JSON format with strict=False to handle control characters that
    occasionally appear in label fields. Retries on:
      - HTTP 429 (rate limit) with a long backoff
      - HTTP 500 / 502 / 503 (transient server errors)
      - network errors (URLError, OSError, ConnectionError, TimeoutError)
      - malformed JSON (JSONDecodeError) — Wikidata's response stream
        occasionally truncates under load
      - missing expected fields (KeyError on results.bindings) — same
        cause as malformed JSON
    Anything else (AttributeError, TypeError, generic Exception) is a
    bug in our code or an unknown failure and propagates immediately.
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
        except OSError as e:
            # OSError covers the full transient-network family on Python 3.x:
            # URLError (inherits OSError), ConnectionError, TimeoutError,
            # BrokenPipeError, ConnectionResetError, ConnectionRefusedError,
            # and raw socket errors. HTTPError is a URLError subclass but is
            # caught by the preceding handler.
            if attempt < retries:
                print(f"  Network error ({type(e).__name__}): {e}. Retrying in 10s...")
                time.sleep(10)
                continue
            raise
        except (json.JSONDecodeError, KeyError) as e:
            if attempt < retries:
                print(f"  Malformed response ({type(e).__name__}): {e}. Retrying in 10s...")
                time.sleep(10)
                continue
            raise
    return []


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


# Fields compared by compute_delta() when counting "bio changes" on the
# common set of entities between the old and new json snapshots. name_en
# is included so description-disambiguation renames show up too.
_DELTA_BIO_FIELDS = (
    "name_en", "name_native", "date_of_birth", "date_of_birth_precision",
    "nationality", "position", "position_detail", "height_cm",
    "country", "founded", "stadium", "aliases_en",
    "full_name", "description_en", "team_category",
)


def compute_delta(old: list[dict], new: list[dict]) -> dict:
    """Compare two entity lists and return a summary of what changed.

    Returns a dict with:
      added         — count of new QIDs
      removed       — count of QIDs present in old but not new
      bio_changed   — count of entities where at least one scalar bio
                      field changed (see _DELTA_BIO_FIELDS)
      ids_changed   — count of entities whose external_ids dict differs
      added_samples — up to 3 sample added QIDs
      removed_samples — up to 3 sample removed QIDs
    """
    old_by_qid = {e["qid"]: e for e in old if e.get("qid")}
    new_by_qid = {e["qid"]: e for e in new if e.get("qid")}
    added = set(new_by_qid) - set(old_by_qid)
    removed = set(old_by_qid) - set(new_by_qid)
    common = set(old_by_qid) & set(new_by_qid)

    bio_changed = 0
    ids_changed = 0
    for qid in common:
        oe = old_by_qid[qid]
        ne = new_by_qid[qid]
        if any(oe.get(f) != ne.get(f) for f in _DELTA_BIO_FIELDS):
            bio_changed += 1
        if oe.get("external_ids") != ne.get("external_ids"):
            ids_changed += 1

    return {
        "old_count": len(old),
        "new_count": len(new),
        "added": len(added),
        "removed": len(removed),
        "bio_changed": bio_changed,
        "ids_changed": ids_changed,
        "added_samples": sorted(added)[:3],
        "removed_samples": sorted(removed)[:3],
    }


def print_delta(entity_type: str, delta: dict) -> None:
    """Print a one-line summary of the delta against the previous run."""
    if delta["old_count"] == 0:
        print(f"  Delta: first run (no previous {entity_type}s.json)")
        return
    print(
        f"  Delta vs previous: +{delta['added']} / -{delta['removed']} "
        f"(bio changes: {delta['bio_changed']}, ID changes: {delta['ids_changed']})"
    )
    if delta["added"]:
        print(f"    First added: {delta['added_samples']}")
    if delta["removed"]:
        print(f"    First removed: {delta['removed_samples']}")


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
SELECT ?e ?eLabel ?eDescription {id_selects}
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
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{LABEL_LANGS}" . }}
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

    # Three-path union covers three ways Wikidata classifies football clubs:
    # 1. Subclass of Q476028 (association football club) — the canonical org type
    # 2. Subclass of Q103229495 (men's association football team) — some major clubs
    #    (e.g. Q7156 Futbol Club Barcelona, Q170703 Boca Juniors) only have this P31
    #    and were silently missed by the class-only filter before this fix.
    # 3. Sports club (Q847017 subclass) with P641 = association football — catches
    #    smaller/regional clubs (e.g. Q8206935 Estudiantes de Río Cuarto) classified
    #    generically but with an explicit football sport claim.
    #
    # Also captures the raw P31 QID(s) so parse_ids_phase can derive a
    # team_category (senior_men / women / beach_soccer / youth / reserve).
    return f"""
SELECT ?e ?eLabel ?eDescription ?typeQid {id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e WHERE {{
      {{
        ?e wdt:P31 ?type .
        ?type (wdt:P279)* wd:Q476028 .
      }}
      UNION
      {{
        ?e wdt:P31 ?type .
        ?type (wdt:P279)* wd:Q103229495 .
      }}
      UNION
      {{
        ?e wdt:P641 wd:Q2736 .
        ?e wdt:P31 ?type .
        ?type (wdt:P279)* wd:Q847017 .
      }}
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
  OPTIONAL {{ ?e wdt:P31 ?typeQid . }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{LABEL_LANGS}" . }}
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
SELECT ?e ?eLabel ?eDescription {id_selects}
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
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{LABEL_LANGS}" . }}
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

    # Two-path union: class-based (Q15991290 subclasses) + property-based (has competition IDs).
    # Class path: STRICT — require P641 = association football (Q2736). This blocks non-football
    # competitions (NHL, PGA, rugby, etc.) that leaked through when P641 was missing or wrong.
    # Property path: PERMISSIVE — having FBref/Opta IDs is strong enough proof of football.
    # Items without P641 are still allowed through the property path if they have provider IDs.
    prop_unions = "\n        UNION\n".join(
        f"        {{ ?e wdt:{prop} [] . }}" for prop in COMPETITION_IDS.values()
    )
    return f"""
SELECT ?e ?eLabel ?eDescription {id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e WHERE {{
      {{
        # Class path: must explicitly declare sport = association football
        ?e wdt:P31/wdt:P279* wd:Q15991290 .
        ?e wdt:P641 wd:Q2736 .
      }}
      UNION
      {{
        # Property path: has football-specific provider IDs (FBref, Opta, etc.)
{prop_unions}
        # Still block items that explicitly declare a non-football sport
        FILTER NOT EXISTS {{
          ?e wdt:P641 ?sport .
          FILTER(?sport != wd:Q2736)
        }}
      }}
      # Exclude season entities — P3450 = "sports season of league or
      # competition". Those are handled separately by build_season_ids_query
      # and should not end up as competition-type rows. A season like
      # "2025-26 Senegal Ligue 1" leaked in previously and collided with its
      # parent competition (SEN1 Transfermarkt code on both).
      FILTER NOT EXISTS {{ ?e wdt:P3450 ?parentComp . }}
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{LABEL_LANGS}" . }}
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

    # Only allow seasons of football competitions.
    # Two-path union mirrors the competition query:
    # 1. Competition has P641 = Q2736 (strict class path)
    # 2. Competition has football-specific provider IDs (FBref, Opta, etc.)
    comp_prop_unions = "\n        UNION\n".join(
        f"        {{ ?comp wdt:{prop} [] . }}" for prop in COMPETITION_IDS.values()
    )
    return f"""
SELECT ?e ?eLabel ?eDescription ?competitionQid{id_selects}
WHERE {{
  {{
    SELECT DISTINCT ?e ?competitionQid WHERE {{
      ?e wdt:P3450 ?comp .
      BIND(?comp AS ?competitionQid)
      {{
        # Class path: competition must declare sport = association football
        ?comp wdt:P31/wdt:P279* wd:Q15991290 .
        ?comp wdt:P641 wd:Q2736 .
      }}
      UNION
      {{
        # Property path: competition has football-specific provider IDs
{comp_prop_unions}
        FILTER NOT EXISTS {{
          ?comp wdt:P641 ?sport .
          FILTER(?sport != wd:Q2736)
        }}
      }}
    }}
    ORDER BY ?e
    {limit_clause} {offset_clause}
  }}
{id_optionals}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{LABEL_LANGS}" . }}
}}
"""


# Mapping from Wikidata P31 QIDs to reep team_category values. The fetch script
# collects every P31 value for each team (via OPTIONAL { ?e wdt:P31 ?typeQid })
# and picks the highest-priority match here. "senior_men" wins over "unknown"
# when a team has both, because it's the more informative classification.
TEAM_CATEGORY_QIDS = {
    "women": {
        "Q20639857",   # women's association football team
        "Q26783908",   # women's football club
    },
    "beach_soccer": {
        "Q116953048",  # beach soccer club (mislabelled "women's club" in some Q)
        "Q16977640",   # beach soccer team
    },
    "futsal": {
        "Q1077434",    # futsal club
        "Q15994025",   # futsal team
    },
    "youth": {
        "Q22820",      # youth team
        "Q26723090",   # youth football club
        "Q2992826",    # under-21 team
        "Q2466834",    # under-19 team
    },
    "reserve": {
        "Q3563237",    # reserve team
    },
    "senior_men": {
        "Q103229495",  # men's association football team
        "Q476028",     # association football club (default men's senior when ambiguous)
    },
}

# Priority order — when a team has multiple P31 values, the more specific wins.
TEAM_CATEGORY_PRIORITY = [
    "women", "beach_soccer", "futsal", "youth", "reserve", "senior_men",
]


def classify_team(type_qids: set[str]) -> str:
    """Pick the highest-priority team category from a set of P31 QIDs.

    Defaults to 'senior_men' when no specific category matches. The team
    SPARQL filter has already restricted the result set to football-relevant
    entities, so a generic sports club (Q847017 only) without women/youth/
    beach markers is almost always a regular men's senior team (Wikidata
    leaves many small clubs classified that way — e.g. Q170703 Boca Juniors
    itself is only P31 = Q847017 live). Returns 'unknown' only when the
    input set is empty, which indicates a data error upstream.
    """
    if not type_qids:
        return "unknown"
    for category in TEAM_CATEGORY_PRIORITY:
        if type_qids & TEAM_CATEGORY_QIDS[category]:
            return category
    return "senior_men"


def parse_ids_phase(rows: list[dict], entity_type: str, id_props: dict) -> dict[str, dict]:
    """Parse Phase 1 results into entity dict keyed by QID.

    Post-pass: detect entities sharing the same English label within this
    type and auto-disambiguate their `name_en` by appending the Wikidata
    description in parentheses. Addresses the "FC Barcelona" class of bug
    where multiple QIDs share a bare label and consumers can't tell them
    apart by name alone.
    """
    entities: dict[str, dict] = {}
    # Collected P31 QIDs per entity (team classification) — flattened after the loop
    type_qids_by_entity: dict[str, set[str]] = {}

    for row in rows:
        qid = extract_qid(row.get("e", ""))
        if not qid or not qid.startswith("Q"):
            continue
        if qid not in entities:
            # Wikidata's SPARQL label service returns the QID as a fallback
            # when no English label is set. Drop the fallback so
            # backfill-broken-names.py can fix it on a second pass — storing
            # `Q12345` as name_en is worse than an empty string because the
            # empty-name validator catches it immediately.
            label = row.get("eLabel", "")
            if label == qid:
                label = ""
            entities[qid] = {
                "qid": qid,
                "type": entity_type,
                "name_en": label,
                "name_native": None,
                "description_en": row.get("eDescription", "") or None,
                "aliases_en": None,
                "full_name": None,
                "date_of_birth": None,
                "date_of_birth_precision": None,  # 'day' / 'month' / 'year'
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
            if entity_type == "team":
                entities[qid]["team_category"] = None
        # Capture competition QID for seasons (from SPARQL result)
        comp_qid_uri = row.get("competitionQid")
        if comp_qid_uri and entity_type == "season":
            comp_qid = extract_qid(comp_qid_uri)
            if comp_qid.startswith("Q"):
                entities[qid]["competition_qid"] = comp_qid
        # Collect all P31 values for team classification
        if entity_type == "team":
            type_uri = row.get("typeQid", "")
            if type_uri:
                type_q = extract_qid(type_uri)
                if type_q.startswith("Q"):
                    type_qids_by_entity.setdefault(qid, set()).add(type_q)
        for name in id_props:
            val = row.get(f"id_{name}")
            if val and name not in entities[qid]["external_ids"]:
                entities[qid]["external_ids"][name] = val

    # Derive team_category from the collected P31 chain
    if entity_type == "team":
        for qid, type_qids in type_qids_by_entity.items():
            entities[qid]["team_category"] = classify_team(type_qids)
        # Any entity with no P31 at all — shouldn't happen since the SPARQL
        # filter requires a P31 — but defend against it defaulting to
        # senior_men to match classify_team's bias.
        for entity in entities.values():
            if entity.get("team_category") is None:
                entity["team_category"] = "senior_men"

    # --- Disambiguation post-pass ---
    # Group entities by their normalized English label; any label held by
    # more than one entity gets description-suffixed so downstream users
    # can distinguish them. We keep the raw description_en field on each
    # entity regardless so consumers can still access it.
    by_label: dict[str, list[str]] = {}
    for qid, entity in entities.items():
        name = (entity.get("name_en") or "").strip()
        if not name:
            continue
        key = name.casefold()
        by_label.setdefault(key, []).append(qid)

    ambiguous_count = 0
    for label_key, qids in by_label.items():
        if len(qids) < 2:
            continue
        # Count how many of the colliding entities have distinct descriptions
        # available — if they don't, we can't disambiguate and leave them alone.
        descs = {q: (entities[q].get("description_en") or "").strip() for q in qids}
        distinct = {d for d in descs.values() if d}
        if len(distinct) < 2:
            # All share the same description or none have one. Skip — manual
            # review needed.
            continue
        for q in qids:
            desc = descs[q]
            if not desc:
                continue
            # Cap the appended description at 40 chars so names stay readable.
            short = desc if len(desc) <= 40 else desc[:37].rstrip() + "..."
            original = entities[q]["name_en"]
            entities[q]["name_en"] = f"{original} ({short})"
            ambiguous_count += 1

    if ambiguous_count:
        print(f"  Disambiguated {ambiguous_count} entities sharing labels with others")

    return entities


# ---------------------------------------------------------------------------
# Phase 2: Bio details in batches
# ---------------------------------------------------------------------------

def build_player_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?birthName ?nativeName ?dob ?dobPrecision ?nationalityLabel ?positionLabel ?heightAmount
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P1477 ?birthName . FILTER(LANG(?birthName) = "en") }}
  OPTIONAL {{ ?e wdt:P1559 ?nativeName . }}
  OPTIONAL {{
    ?e p:P569 ?dobStmt .
    ?dobStmt psv:P569 ?dobValue .
    ?dobValue wikibase:timeValue ?dob .
    ?dobValue wikibase:timePrecision ?dobPrecision .
  }}
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
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{LABEL_LANGS}" . }}
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
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{LABEL_LANGS}" . }}
}}
"""


def build_coach_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?birthName ?nativeName ?dob ?dobPrecision ?nationalityLabel
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P1477 ?birthName . FILTER(LANG(?birthName) = "en") }}
  OPTIONAL {{ ?e wdt:P1559 ?nativeName . }}
  OPTIONAL {{
    ?e p:P569 ?dobStmt .
    ?dobStmt psv:P569 ?dobValue .
    ?dobValue wikibase:timeValue ?dob .
    ?dobValue wikibase:timePrecision ?dobPrecision .
  }}
  OPTIONAL {{ ?e wdt:P1532 ?sportNat . }}
  OPTIONAL {{ ?e wdt:P27 ?citizenship . }}
  BIND(COALESCE(?sportNat, ?citizenship) AS ?nationality)
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{LABEL_LANGS}" . }}
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
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{LABEL_LANGS}" . }}
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
    # Accumulators — Wikidata returns one row per OPTIONAL join, so multi-value
    # properties (aliases, positions) get expanded across rows and need to be
    # collected into sets before being attached to the entity.
    aliases: dict[str, set] = {}
    positions: dict[str, set] = {}

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

        # Native name (P1559) — first row wins, fall back to birthName only
        # if no native name claim exists
        if not e.get("name_native") and row.get("nativeName"):
            e["name_native"] = row["nativeName"]

        if not e["date_of_birth"] and row.get("dob"):
            dob = row["dob"]
            dob = dob.split("T")[0] if "T" in dob else dob
            # Skip blank-node genid URLs and implausible years
            if not dob.startswith("http") and dob[:1] in ("1", "2"):
                e["date_of_birth"] = dob
                # Capture precision alongside the date. Wikidata precision
                # 11=day, 10=month, 9=year. We only label precision on the
                # same row as the DOB so it stays in sync with the value.
                prec_raw = row.get("dobPrecision")
                if prec_raw:
                    try:
                        prec_int = int(prec_raw)
                    except ValueError:
                        prec_int = None
                    if prec_int == 11:
                        e["date_of_birth_precision"] = "day"
                    elif prec_int == 10:
                        e["date_of_birth_precision"] = "month"
                    elif prec_int == 9:
                        e["date_of_birth_precision"] = "year"

        if not e["nationality"] and row.get("nationalityLabel"):
            e["nationality"] = row["nationalityLabel"]

        if entity_type == "player":
            # Collect all positions; join into a comma-separated string after
            # the loop. Players with multiple P413 values (e.g. forward +
            # winger) used to have only the first position stored.
            pos_label = row.get("positionLabel")
            if pos_label:
                positions.setdefault(qid, set()).add(pos_label)
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

    # Apply accumulated positions (comma-separated, alphabetically stable)
    for qid, position_set in positions.items():
        if qid in entities:
            entities[qid]["position"] = ", ".join(sorted(position_set))


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

    run_summary: dict[str, dict] = {}

    for entity_type, (ids_query_fn, id_props, bio_query_fn) in type_configs.items():
        print(f"\n{'='*60}")
        print(f"Phase 1: Fetching {entity_type} names + IDs (limit={args.test or 'all'})...")
        print(f"{'='*60}")

        out_path = OUTPUT_DIR / f"{entity_type}s.json"
        # Load the previous snapshot BEFORE we overwrite it so compute_delta
        # has something to diff against. Missing file is fine (first run).
        old_entities: list[dict] = []
        if out_path.exists():
            try:
                with open(out_path) as f:
                    old_entities = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                print(f"  warning: could not load previous {out_path.name} ({e}); "
                      f"delta will show first-run counts")
                old_entities = []

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
        new_entities_list = list(entities.values())
        with open(out_path, "w") as f:
            json.dump(new_entities_list, f, indent=2, ensure_ascii=False)
        print(f"  Saved {len(entities)} entities to {out_path}")

        # Compute + print delta vs previous snapshot
        delta = compute_delta(old_entities, new_entities_list)
        print_delta(entity_type, delta)
        run_summary[entity_type] = delta

        if len(type_configs) > 1:
            print("  Sleeping 5s between types...")
            time.sleep(5)

    # Final summary — one-glance view of the full run
    print(f"\n{'='*60}")
    print(f"Run summary")
    print(f"{'='*60}")
    for etype, d in run_summary.items():
        if d["old_count"] == 0:
            print(f"  {etype:<12} {d['new_count']:>7,} (first run)")
        else:
            print(
                f"  {etype:<12} {d['new_count']:>7,}  "
                f"(+{d['added']:,} / -{d['removed']:,}, "
                f"bio Δ {d['bio_changed']:,}, ids Δ {d['ids_changed']:,})"
            )
    print(f"\nDone! Files in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
