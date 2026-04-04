"""
Explore Wikidata coverage of football competitions and seasons.
Outputs counts and samples — does not write to D1.

Usage:
  python scripts/research-competitions.py
"""

import json
import time
import urllib.request
import urllib.parse
from pathlib import Path

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "reep-football-register/1.0 (https://github.com/withqwerty/reep)"


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
                data = json.loads(resp.read().decode(), strict=False)
            return [
                {k: v["value"] for k, v in binding.items()}
                for binding in data["results"]["bindings"]
            ]
        except Exception as e:
            if attempt < retries:
                print(f"  Error: {e}. Retrying in 15s...")
                time.sleep(15)
                continue
            raise
    return []


def main():
    print("=" * 60)
    print("1. Football competitions (Q15991290 + subclasses)")
    print("=" * 60)

    # Count all football competitions
    rows = sparql_query("""
        SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {
          ?e wdt:P31/wdt:P279* wd:Q15991290 .
        }
    """)
    print(f"  Total competitions: {rows[0]['count'] if rows else '?'}")
    time.sleep(3)

    # Count competitions with specific provider IDs
    for provider, prop in [("transfermarkt", "P12758"), ("fbref", "P13664"), ("opta", "P8735")]:
        rows = sparql_query(f"""
            SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {{
              ?e wdt:P31/wdt:P279* wd:Q15991290 .
              ?e wdt:{prop} ?id .
            }}
        """)
        print(f"  With {provider} ID ({prop}): {rows[0]['count'] if rows else '?'}")
        time.sleep(3)

    # Sample competitions with names + provider IDs
    print("\n  Sample competitions with provider IDs:")
    rows = sparql_query("""
        SELECT ?e ?eLabel ?tm ?fbref ?opta WHERE {
          ?e wdt:P31/wdt:P279* wd:Q15991290 .
          OPTIONAL { ?e wdt:P12758 ?tm . }
          OPTIONAL { ?e wdt:P13664 ?fbref . }
          OPTIONAL { ?e wdt:P8735 ?opta . }
          FILTER(BOUND(?tm) || BOUND(?fbref) || BOUND(?opta))
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        LIMIT 20
    """)
    for r in rows:
        qid = r["e"].split("/")[-1]
        name = r.get("eLabel", "?")
        ids = []
        if r.get("tm"): ids.append(f"tm={r['tm']}")
        if r.get("fbref"): ids.append(f"fbref={r['fbref']}")
        if r.get("opta"): ids.append(f"opta={r['opta']}")
        print(f"    {qid}: {name} [{', '.join(ids)}]")
    time.sleep(5)

    print(f"\n{'=' * 60}")
    print("2. Football seasons (P3450 = sports season of league)")
    print("=" * 60)

    # Count all seasons
    rows = sparql_query("""
        SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {
          ?e wdt:P3450 ?comp .
        }
    """)
    print(f"  Total seasons (items with P3450): {rows[0]['count'] if rows else '?'}")
    time.sleep(3)

    # Count seasons that are specifically football
    rows = sparql_query("""
        SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {
          ?e wdt:P3450 ?comp .
          ?comp wdt:P31/wdt:P279* wd:Q15991290 .
        }
    """)
    print(f"  Football-specific seasons: {rows[0]['count'] if rows else '?'}")
    time.sleep(3)

    # Check if seasons have ANY provider ID properties
    print("\n  Checking season provider ID properties...")
    for provider, prop in [("transfermarkt", "P12758"), ("fbref", "P13664"), ("opta", "P8735")]:
        rows = sparql_query(f"""
            SELECT (COUNT(DISTINCT ?e) AS ?count) WHERE {{
              ?e wdt:P3450 ?comp .
              ?comp wdt:P31/wdt:P279* wd:Q15991290 .
              ?e wdt:{prop} ?id .
            }}
        """)
        print(f"    Seasons with {provider} ID ({prop}): {rows[0]['count'] if rows else '?'}")
        time.sleep(3)

    # Sample seasons
    print("\n  Sample seasons:")
    rows = sparql_query("""
        SELECT ?e ?eLabel ?comp ?compLabel WHERE {
          ?e wdt:P3450 ?comp .
          ?comp wdt:P31/wdt:P279* wd:Q15991290 .
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        LIMIT 20
    """)
    for r in rows:
        qid = r["e"].split("/")[-1]
        name = r.get("eLabel", "?")
        comp_qid = r["comp"].split("/")[-1]
        comp_name = r.get("compLabel", "?")
        print(f"    {qid}: {name} -> {comp_qid} ({comp_name})")
    time.sleep(3)

    # Historical depth — earliest seasons
    print("\n  Historical depth (earliest seasons):")
    rows = sparql_query("""
        SELECT ?e ?eLabel ?start WHERE {
          ?e wdt:P3450 ?comp .
          ?comp wdt:P31/wdt:P279* wd:Q15991290 .
          ?e wdt:P580 ?start .
          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        ORDER BY ?start
        LIMIT 10
    """)
    for r in rows:
        name = r.get("eLabel", "?")
        start = r.get("start", "?").split("T")[0]
        print(f"    {start}: {name}")

    print(f"\n{'=' * 60}")
    print("3. Summary")
    print("=" * 60)
    print("  Review the counts above to decide sourcing strategy.")
    print("  If competition coverage with provider IDs is reasonable,")
    print("  proceed with Wikidata-first pipeline.")
    print("  If sparse, consider FBref/Transfermarkt as primary seed.")


if __name__ == "__main__":
    main()
