"""
Stream a Wikidata JSON dump and extract football entities without storing the full dump.

The Wikidata dump is ~100GB compressed. This script streams it through a decompressor
and filters line-by-line, keeping only entities with football-related properties.
Output is identical to fetch-wikidata-entities.py (JSON files ready for D1 seeding).

Disk usage: only the filtered output (~50MB for data/json/).

Usage:
  # Stream from Wikidata (downloads ~100GB, but never stores it)
  python scripts/stream-wikidata-dump.py

  # From a local dump file (if you downloaded it elsewhere)
  python scripts/stream-wikidata-dump.py --file /path/to/latest-all.json.bz2

  # Test with a small dump
  python scripts/stream-wikidata-dump.py --limit 1000000

  # Dry run: report counts without writing files
  python scripts/stream-wikidata-dump.py --dry-run
"""

import argparse
import bz2
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "json"

DUMP_URL = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2"

# ---------------------------------------------------------------------------
# Wikidata property IDs that identify football entities.
# An entity is included if it has ANY of these properties as a claim.
# ---------------------------------------------------------------------------

PLAYER_IDS = {
    "transfermarkt": "P2446",
    "fbref": "P5750",
    "soccerway": "P2369",
    "sofascore": "P12302",
    "flashscore": "P8259",
    "opta": "P8736",
    "premier_league": "P12539",
    "11v11": "P12551",
    "espn": "P3681",
    "national_football_teams": "P2574",
    "worldfootball": "P2020",
    "soccerbase": "P2193",
    "kicker": "P8912",
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
    "opta": "P8737",
    "espn": "P13590",
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

# All property IDs we care about, mapped to (entity_type, provider_name)
PROPERTY_MAP: dict[str, list[tuple[str, str]]] = {}
for provider, prop in PLAYER_IDS.items():
    PROPERTY_MAP.setdefault(prop, []).append(("player", provider))
for provider, prop in TEAM_IDS.items():
    PROPERTY_MAP.setdefault(prop, []).append(("team", provider))
for provider, prop in COACH_IDS.items():
    PROPERTY_MAP.setdefault(prop, []).append(("coach", provider))

ALL_FOOTBALL_PROPERTIES = set(PROPERTY_MAP.keys())

# Bio properties
P_DOB = "P569"           # date of birth
P_NATIONALITY = "P27"    # country of citizenship
P_POSITION = "P413"      # position played on team
P_HEIGHT = "P2048"       # height
P_CURRENT_TEAM = "P54"   # member of sports team
P_BIRTH_NAME = "P1477"   # birth name
P_COUNTRY = "P17"        # country (for teams)
P_INCEPTION = "P571"     # inception/founding date (for teams)
P_VENUE = "P115"         # home venue (for teams)


def get_claim_value(claim: dict) -> str | None:
    """Extract the main value from a Wikidata claim."""
    mainsnak = claim.get("mainsnak", {})
    datavalue = mainsnak.get("datavalue", {})
    vtype = datavalue.get("type")
    value = datavalue.get("value")

    if vtype == "string":
        return value
    if vtype == "wikibase-entityid":
        return value.get("id")
    if vtype == "time":
        # Return ISO date, strip leading +
        time_str = value.get("time", "")
        if time_str.startswith("+"):
            time_str = time_str[1:]
        return time_str[:10] if len(time_str) >= 10 else time_str
    if vtype == "quantity":
        amount = value.get("amount", "")
        if amount.startswith("+"):
            amount = amount[1:]
        return amount
    return None


def get_label(entity: dict, lang: str = "en") -> str:
    """Get label in the specified language."""
    labels = entity.get("labels", {})
    if lang in labels:
        return labels[lang]["value"]
    return ""


def get_aliases(entity: dict, lang: str = "en") -> str:
    """Get comma-separated aliases in the specified language."""
    aliases = entity.get("aliases", {}).get(lang, [])
    return ", ".join(a["value"] for a in aliases)


def extract_entity(entity: dict) -> list[dict]:
    """Extract football entities from a Wikidata dump entity.

    Returns a list of records — one per type the entity qualifies for.
    E.g. Guardiola returns both a player record and a coach record with
    type-appropriate external IDs.
    """
    claims = entity.get("claims", {})

    # Check if this entity has any football-related properties
    matched_props = ALL_FOOTBALL_PROPERTIES & claims.keys()
    if not matched_props:
        return []

    qid = entity.get("id", "")
    if not qid.startswith("Q"):
        return []

    # Collect external IDs grouped by entity type
    ids_by_type: dict[str, dict[str, str]] = {}

    for prop in matched_props:
        prop_claims = claims[prop]
        if not prop_claims:
            continue
        val = get_claim_value(prop_claims[0])
        if not val:
            continue

        for etype, provider in PROPERTY_MAP[prop]:
            ids_by_type.setdefault(etype, {})[provider] = val

    # Coaches need a coach-specific property (P2447/P2195), not just shared player props
    has_coach_specific = any(
        prop in claims for prop in ("P2447", "P2195")
    )
    if "coach" in ids_by_type and not has_coach_specific:
        # Move coach IDs into player bucket instead
        for provider, val in ids_by_type.pop("coach").items():
            ids_by_type.setdefault("player", {})[provider] = val
    elif "coach" in ids_by_type and has_coach_specific:
        # Remove shared player IDs that are redundant with coach-specific ones
        # e.g. transfermarkt_player is misleading when transfermarkt_manager exists
        coach_ids = ids_by_type["coach"]
        if "transfermarkt_manager" in coach_ids:
            coach_ids.pop("transfermarkt_player", None)
            coach_ids.pop("transfermarkt", None)

    if not ids_by_type:
        return []

    name_en = get_label(entity, "en")
    if not name_en:
        return []

    aliases_en = get_aliases(entity, "en")

    def first_claim_value(prop: str) -> str | None:
        if prop in claims and claims[prop]:
            return get_claim_value(claims[prop][0])
        return None

    # Shared bio fields for people
    person_bio: dict = {}
    birth_name = first_claim_value(P_BIRTH_NAME)
    if birth_name:
        person_bio["full_name"] = birth_name
    dob = first_claim_value(P_DOB)
    if dob:
        person_bio["date_of_birth"] = dob
    nationality_qid = first_claim_value(P_NATIONALITY)
    if nationality_qid:
        person_bio["nationality_qid"] = nationality_qid
    position_qid = first_claim_value(P_POSITION)
    if position_qid:
        person_bio["position_qid"] = position_qid
    height = first_claim_value(P_HEIGHT)
    if height:
        try:
            h = float(height)
            person_bio["height_cm"] = round(h * 100, 1) if h < 3 else round(h, 1)
        except ValueError:
            pass
    team_qid = first_claim_value(P_CURRENT_TEAM)
    if team_qid:
        person_bio["current_team_qid"] = team_qid

    # Team-specific bio
    team_bio: dict = {}
    country_qid = first_claim_value(P_COUNTRY)
    if country_qid:
        team_bio["country_qid"] = country_qid
    founded = first_claim_value(P_INCEPTION)
    if founded:
        team_bio["founded"] = founded
    venue_qid = first_claim_value(P_VENUE)
    if venue_qid:
        team_bio["venue_qid"] = venue_qid

    # Build one record per type
    results: list[dict] = []
    for etype, ext_ids in ids_by_type.items():
        record: dict = {
            "qid": qid,
            "type": etype,
            "name_en": name_en,
            "external_ids": ext_ids,
        }
        if aliases_en:
            record["aliases_en"] = aliases_en

        if etype in ("player", "coach"):
            record.update(person_bio)
        elif etype == "team":
            record.update(team_bio)

        results.append(record)

    return results


# ---------------------------------------------------------------------------
# QID resolution: collect labels for referenced QIDs (countries, positions, etc.)
# during the same dump pass, then resolve after scanning.
# ---------------------------------------------------------------------------

# Known position QID -> label mappings (small fixed set)
POSITION_LABELS = {
    "Q193592": "goalkeeper",
    "Q336286": "defender",
    "Q6483970": "defender",
    "Q17351545": "centre-back",
    "Q18070612": "right-back",
    "Q18070614": "left-back",
    "Q23896851": "wing-back",
    "Q201117": "midfielder",
    "Q6779871": "midfielder",
    "Q15381590": "defensive midfielder",
    "Q6781038": "central midfielder",
    "Q6781042": "attacking midfielder",
    "Q6781044": "right midfielder",
    "Q6781046": "left midfielder",
    "Q280658": "forward",
    "Q6781050": "forward",
    "Q6781054": "centre-forward",
    "Q6423722": "striker",
    "Q6781056": "right winger",
    "Q6781060": "left winger",
    "Q6781062": "winger",
    "Q1251148": "sweeper",
}


SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
SPARQL_USER_AGENT = "reep-football-register/1.0 (https://github.com/withqwerty/reep)"
SPARQL_BATCH_SIZE = 200  # QIDs per SPARQL query


def fetch_qid_labels(qids: set[str]) -> dict[str, str]:
    """Fetch English labels for a set of QIDs via SPARQL. Returns {qid: label}."""
    labels: dict[str, str] = {}
    qid_list = sorted(qids)

    for i in range(0, len(qid_list), SPARQL_BATCH_SIZE):
        batch = qid_list[i:i + SPARQL_BATCH_SIZE]
        values = " ".join(f"wd:{q}" for q in batch)
        query = f"""
        SELECT ?item ?label WHERE {{
          VALUES ?item {{ {values} }}
          ?item rdfs:label ?label .
          FILTER(LANG(?label) = "en")
        }}
        """
        body = urllib.parse.urlencode({"query": query}).encode("utf-8")
        req = urllib.request.Request(
            SPARQL_ENDPOINT,
            data=body,
            headers={
                "User-Agent": SPARQL_USER_AGENT,
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.loads(resp.read().decode(), strict=False)
                for binding in data["results"]["bindings"]:
                    qid = binding["item"]["value"].rsplit("/", 1)[-1]
                    labels[qid] = binding["label"]["value"]
                break
            except Exception as e:
                if attempt < 2:
                    print(f"    Retry {attempt + 1}: {e}")
                    time.sleep(5 * (attempt + 1))
                else:
                    print(f"    Failed batch at offset {i}: {e}")

        # Rate limit: be gentle with Wikidata
        if i + SPARQL_BATCH_SIZE < len(qid_list):
            time.sleep(1)

    return labels


def resolve_qids(entities: list[dict], qid_labels: dict[str, str]) -> None:
    """Resolve _qid fields to human-readable labels in-place."""
    for entity in entities:
        # Nationality
        nat_qid = entity.pop("nationality_qid", None)
        if nat_qid and nat_qid in qid_labels:
            entity["nationality"] = qid_labels[nat_qid]

        # Position (use static map first, fall back to dump labels)
        pos_qid = entity.pop("position_qid", None)
        if pos_qid:
            if pos_qid in POSITION_LABELS:
                entity["position"] = POSITION_LABELS[pos_qid]
            elif pos_qid in qid_labels:
                entity["position"] = qid_labels[pos_qid]

        # Country (teams)
        country_qid = entity.pop("country_qid", None)
        if country_qid and country_qid in qid_labels:
            entity["country"] = qid_labels[country_qid]

        # Venue (teams) — resolve to label as stadium name
        venue_qid = entity.pop("venue_qid", None)
        if venue_qid and venue_qid in qid_labels:
            entity["stadium"] = qid_labels[venue_qid]


def stream_dump(source: str | None, limit: int | None) -> "iter":
    """Yield lines from the Wikidata dump, streaming from URL or local file."""
    if source:
        # Local file
        path = Path(source)
        if path.suffix == ".bz2":
            # Use lbzip2/pbzip2 for parallel decompression (much faster than Python's bz2)
            decompressor = None
            for cmd in ["lbzip2", "pbzip2", "bzip2"]:
                if subprocess.run(["which", cmd], capture_output=True).returncode == 0:
                    decompressor = cmd
                    break

            if decompressor:
                print(f"Decompressing {path.name} with {decompressor}")
                proc = subprocess.Popen(
                    [decompressor, "-d", "-c", str(path)],
                    stdout=subprocess.PIPE,
                )
                try:
                    for i, line in enumerate(proc.stdout):
                        if limit and i >= limit:
                            break
                        yield line.decode("utf-8", errors="replace")
                finally:
                    proc.stdout.close()
                    proc.wait()
            else:
                # Fallback to Python bz2 (single-threaded, slow)
                f = bz2.open(path, "rt", encoding="utf-8")
                try:
                    for i, line in enumerate(f):
                        if limit and i >= limit:
                            break
                        yield line
                finally:
                    f.close()
        else:
            f = open(path, "r", encoding="utf-8")
            try:
                for i, line in enumerate(f):
                    if limit and i >= limit:
                        break
                    yield line
            finally:
                f.close()
    else:
        # Stream from URL using curl + bzip2 subprocess for better performance
        # (Python's bz2 module is single-threaded; lbzip2/pbzip2 can be faster)
        print(f"Streaming from {DUMP_URL}")
        print("This downloads ~100GB but never stores it on disk.\n")

        # Try lbzip2 (parallel) > pbzip2 (parallel) > bzip2 (single-threaded)
        decompressor = None
        for cmd in ["lbzip2", "pbzip2", "bzip2"]:
            if subprocess.run(["which", cmd], capture_output=True).returncode == 0:
                decompressor = cmd
                break

        if not decompressor:
            print("ERROR: No bzip2 decompressor found. Install bzip2, lbzip2, or pbzip2.")
            sys.exit(1)

        print(f"Using decompressor: {decompressor}")

        curl = subprocess.Popen(
            ["curl", "-sL", DUMP_URL],
            stdout=subprocess.PIPE,
        )
        decomp = subprocess.Popen(
            [decompressor, "-d"],
            stdin=curl.stdout,
            stdout=subprocess.PIPE,
        )
        curl.stdout.close()  # allow curl to receive SIGPIPE if decomp exits

        try:
            for i, line in enumerate(decomp.stdout):
                if limit and i >= limit:
                    break
                yield line.decode("utf-8", errors="replace")
        finally:
            decomp.stdout.close()
            decomp.wait()
            curl.wait()


def main():
    parser = argparse.ArgumentParser(
        description="Stream Wikidata dump and extract football entities"
    )
    parser.add_argument("--file", type=str, help="Local dump file path (default: stream from web)")
    parser.add_argument("--limit", type=int, help="Max lines to process (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="Count entities without writing files")
    parser.add_argument("--output", type=str, help="Output directory (default: data/json/)")
    args = parser.parse_args()

    players: list[dict] = []
    teams: list[dict] = []
    coaches: list[dict] = []

    # Collect QIDs we need to resolve (countries, positions, venues)
    qids_to_resolve: set[str] = set()
    # Labels we discover during the scan (every entity's label gets cached)
    qid_labels: dict[str, str] = {}

    start_time = time.time()
    lines_processed = 0
    last_report = start_time

    print("Scanning Wikidata dump for football entities...\n")

    for line in stream_dump(args.file, args.limit):
        # Each line in the dump is a JSON entity, possibly with trailing comma
        line = line.strip().rstrip(",")
        if not line or line in ("[", "]"):
            continue

        lines_processed += 1

        # Quick pre-filter: skip lines that don't contain any of our property IDs
        # This avoids JSON parsing ~95% of entities
        if not any(prop in line for prop in ALL_FOOTBALL_PROPERTIES):
            # But check if this entity is one we need to resolve (country/venue/etc.)
            # We can only do this after the first pass collects qids_to_resolve,
            # so we optimistically cache labels for entities that look like
            # countries (P_INSTANCE_OF = Q6256/Q3624078) -- too expensive.
            # Instead, we'll resolve via a small SPARQL query after the scan.

            # Progress report every 30s
            now = time.time()
            if now - last_report >= 30:
                elapsed = now - start_time
                rate = lines_processed / elapsed
                print(
                    f"  {lines_processed:,} entities scanned "
                    f"({rate:,.0f}/s) — "
                    f"{len(players):,} players, {len(teams):,} teams, {len(coaches):,} coaches "
                    f"[{elapsed:.0f}s]"
                )
                last_report = now
            continue

        try:
            entity = json.loads(line, strict=False)
        except json.JSONDecodeError:
            continue

        records = extract_entity(entity)
        if not records:
            continue

        for result in records:
            # Track QIDs that need label resolution
            for qid_field in ("nationality_qid", "position_qid", "country_qid", "venue_qid"):
                qid_val = result.get(qid_field)
                if qid_val:
                    qids_to_resolve.add(qid_val)

            if result["type"] == "player":
                players.append(result)
            elif result["type"] == "team":
                teams.append(result)
            elif result["type"] == "coach":
                coaches.append(result)

        # Progress report every 30s
        now = time.time()
        if now - last_report >= 30:
            elapsed = now - start_time
            rate = lines_processed / elapsed
            print(
                f"  {lines_processed:,} entities scanned "
                f"({rate:,.0f}/s) — "
                f"{len(players):,} players, {len(teams):,} teams, {len(coaches):,} coaches "
                f"[{elapsed:.0f}s]"
            )
            last_report = now

    scan_elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"Scan complete in {scan_elapsed:.0f}s ({lines_processed:,} entities scanned)")
    print(f"  Players: {len(players):,}")
    print(f"  Teams:   {len(teams):,}")
    print(f"  Coaches: {len(coaches):,}")
    print(f"  Total:   {len(players) + len(teams) + len(coaches):,}")
    print(f"{'='*60}")

    # Resolve QID labels via lightweight SPARQL queries
    # Remove QIDs we already know (static position labels)
    qids_to_resolve -= set(POSITION_LABELS.keys())

    if qids_to_resolve:
        print(f"\nResolving {len(qids_to_resolve):,} QID labels (countries, venues, positions)...")
        qid_labels = fetch_qid_labels(qids_to_resolve)
        print(f"  Resolved {len(qid_labels):,} labels")

    # Resolve _qid fields to human-readable labels
    print("Resolving entity fields...")
    all_entities = players + teams + coaches
    resolve_qids(all_entities, qid_labels)

    resolved_stats = {
        "nationality": sum(1 for e in all_entities if "nationality" in e),
        "position": sum(1 for e in players + coaches if "position" in e),
        "country": sum(1 for e in teams if "country" in e),
        "stadium": sum(1 for e in teams if "stadium" in e),
    }
    print(f"  Resolved: {resolved_stats}")

    elapsed = time.time() - start_time

    if args.dry_run:
        print("\nDry run — no files written.")
        return

    out_dir = Path(args.output) if args.output else OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    for name, data in [("players", players), ("teams", teams), ("coachs", coaches)]:
        path = out_dir / f"{name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        print(f"Wrote {len(data):,} entities to {path}")

    print(f"\nTotal time: {elapsed:.0f}s")


if __name__ == "__main__":
    main()
