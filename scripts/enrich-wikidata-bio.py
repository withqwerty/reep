"""
Enrich existing Wikidata entity JSON files with bio details (Phase 2 only).

Reads QIDs from data/wikidata/*.json, fetches bio info in batches from
Wikidata SPARQL, and merges back. Skips entities that already have bio data.

Usage:
  python scripts/enrich-wikidata-bio.py                  # enrich all types
  python scripts/enrich-wikidata-bio.py --type player    # single type
  python scripts/enrich-wikidata-bio.py --force          # re-enrich even if bio exists
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
DATA_DIR = Path(__file__).parent.parent / "data" / "json"
BATCH_SIZE = 200


def sparql_query(query: str, retries: int = 3) -> list[dict]:
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
                data = json.loads(resp.read().decode())
            return [
                {k: v["value"] for k, v in binding.items()}
                for binding in data["results"]["bindings"]
            ]
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 60 * (attempt + 1)
                print(f"    Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            if attempt < retries:
                print(f"    HTTP {e.code}. Retrying in 15s...")
                time.sleep(15)
                continue
            raise
        except Exception as e:
            if attempt < retries:
                print(f"    Error: {e}. Retrying in 10s...")
                time.sleep(10)
                continue
            raise
    return []


def build_player_bio_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return f"""
SELECT ?e ?altLabels ?birthName ?dob ?nationalityLabel ?positionLabel ?heightAmount
WHERE {{
  VALUES ?e {{ {values} }}
  OPTIONAL {{ ?e skos:altLabel ?altLabels . FILTER(LANG(?altLabels) = "en") }}
  OPTIONAL {{ ?e wdt:P1477 ?birthName . FILTER(LANG(?birthName) = "en") }}
  OPTIONAL {{ ?e wdt:P569 ?dob . }}
  OPTIONAL {{ ?e wdt:P27 ?nationality . }}
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
  OPTIONAL {{ ?e wdt:P27 ?nationality . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


def extract_qid(uri: str) -> str:
    return uri.split("/")[-1]


def has_bio(entity: dict, entity_type: str) -> bool:
    """Check if entity already has bio data."""
    if entity.get("aliases_en") or entity.get("full_name"):
        return True
    if entity.get("date_of_birth") or entity.get("nationality"):
        return True
    if entity_type == "player" and (entity.get("position") or entity.get("height_cm")):
        return True
    if entity_type == "team" and (entity.get("country") or entity.get("stadium")):
        return True
    return False


def merge_bio_rows(entities_by_qid: dict[str, dict], rows: list[dict], entity_type: str):
    """Merge SPARQL bio rows into entity dicts."""
    aliases: dict[str, set] = {}

    for row in rows:
        qid = extract_qid(row.get("e", ""))
        if qid not in entities_by_qid:
            continue
        e = entities_by_qid[qid]

        alt = row.get("altLabels")
        if alt:
            aliases.setdefault(qid, set()).add(alt)

        if not e.get("full_name") and row.get("birthName"):
            e["full_name"] = row["birthName"]

        if not e.get("date_of_birth") and row.get("dob"):
            dob = row["dob"]
            e["date_of_birth"] = dob.split("T")[0] if "T" in dob else dob

        if not e.get("nationality") and row.get("nationalityLabel"):
            e["nationality"] = row["nationalityLabel"]

        if entity_type == "player":
            if not e.get("position") and row.get("positionLabel"):
                e["position"] = row["positionLabel"]
            if not e.get("height_cm") and row.get("heightAmount"):
                try:
                    e["height_cm"] = float(row["heightAmount"])
                except ValueError:
                    pass

        if entity_type == "team":
            if not e.get("country") and row.get("countryLabel"):
                e["country"] = row["countryLabel"]
            if not e.get("founded") and row.get("founded"):
                f = row["founded"]
                e["founded"] = f.split("T")[0] if "T" in f else f
            if not e.get("stadium") and row.get("stadiumLabel"):
                e["stadium"] = row["stadiumLabel"]

    for qid, alias_set in aliases.items():
        if qid in entities_by_qid:
            existing = entities_by_qid[qid].get("aliases_en")
            if existing:
                alias_set.update(existing.split(", "))
            entities_by_qid[qid]["aliases_en"] = ", ".join(sorted(alias_set))


def main():
    parser = argparse.ArgumentParser(description="Enrich Wikidata entities with bio details")
    parser.add_argument("--type", choices=["player", "team", "coach"], help="Single entity type")
    parser.add_argument("--force", action="store_true", help="Re-enrich even if bio exists")
    args = parser.parse_args()

    bio_query_fns = {
        "player": build_player_bio_query,
        "team": build_team_bio_query,
        "coach": build_coach_bio_query,
    }

    types = ["team", "coach", "player"]  # smallest first
    if args.type:
        types = [args.type]

    for entity_type in types:
        filename = f"{entity_type}s.json" if entity_type != "coach" else "coachs.json"
        json_path = DATA_DIR / filename
        if not json_path.exists():
            print(f"Skipping {entity_type}: {json_path} not found")
            continue

        with open(json_path) as f:
            entities = json.load(f)

        # Index by QID
        by_qid = {e["qid"]: e for e in entities}

        # Filter to those needing enrichment
        if args.force:
            need_enrichment = list(by_qid.keys())
        else:
            need_enrichment = [
                qid for qid, e in by_qid.items() if not has_bio(e, entity_type)
            ]

        total = len(need_enrichment)
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"\n{'='*60}")
        print(f"Enriching {total}/{len(entities)} {entity_type}s ({total_batches} batches)...")
        print(f"{'='*60}")

        if total == 0:
            print("  All entities already have bio data. Skipping.")
            continue

        query_fn = bio_query_fns[entity_type]
        enriched = 0
        for i in range(0, total, BATCH_SIZE):
            batch_qids = need_enrichment[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1

            if batch_num % 50 == 1 or batch_num == total_batches:
                print(f"  Batch {batch_num}/{total_batches} ({enriched} enriched so far)...")

            query = query_fn(batch_qids)
            rows = sparql_query(query)
            merge_bio_rows(by_qid, rows, entity_type)
            enriched += len(batch_qids)

            if i + BATCH_SIZE < total:
                time.sleep(2)

        # Save back
        updated = list(by_qid.values())
        with open(json_path, "w") as f:
            json.dump(updated, f, indent=2, ensure_ascii=False)

        # Show sample of enriched data
        sample = next((e for e in updated if e.get("date_of_birth")), updated[0])
        print(f"  Sample: {sample['name_en']}")
        print(f"    DOB: {sample.get('date_of_birth')}, Nationality: {sample.get('nationality')}")
        print(f"    Aliases: {(sample.get('aliases_en') or 'None')[:80]}")
        print(f"  Saved {len(updated)} entities to {json_path}")

        if len(types) > 1:
            print("  Sleeping 5s between types...")
            time.sleep(5)

    print("\nBio enrichment complete! Re-run seed-wikidata-d1.py to push to D1.")


if __name__ == "__main__":
    main()
