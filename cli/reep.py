#!/usr/bin/env python3
"""
reep — The Football Entity Register CLI

Look up football player, team, and coach IDs across providers.

Usage:
  reep search "Cole Palmer"
  reep search "Arsenal" --type team
  reep resolve transfermarkt 568177
  reep lookup Q99760796
  reep download                         # fetch latest CSVs from GitHub
  reep stats
"""

import argparse
import csv
import json
import os
import sys
import urllib.request
import urllib.error
from pathlib import Path

API_URL = "https://reep-api.rahulkeerthi2-95d.workers.dev"
GITHUB_RAW = "https://raw.githubusercontent.com/withqwerty/reep/main/data"
DATA_DIR = Path.home() / ".reep"

PROVIDERS = [
    "transfermarkt", "transfermarkt_manager", "fbref", "fbref_verified",
    "soccerway", "sofascore", "flashscore", "opta", "premier_league", "11v11",
    "espn", "national_football_teams", "worldfootball", "soccerbase", "kicker",
    "uefa", "lequipe", "fff_fr", "serie_a", "besoccer",
    "footballdatabase_eu", "eu_football_info", "hugman", "german_fa",
    "statmuse_pl", "sofifa", "soccerdonna", "dongqiudi", "playmakerstats",
    "understat", "whoscored", "clubelo", "sportmonks",
    "api_football", "fotmob", "fpl_code",
    "thesportsdb", "impect", "wyscout", "skillcorner", "heimspiel",
]


def api_get(path: str) -> dict:
    """GET request to the Reep API."""
    url = f"{API_URL}{path}"
    req = urllib.request.Request(url, headers={"User-Agent": "reep-cli/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"API error: {e.code} {e.reason}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Connection error: {e.reason}", file=sys.stderr)
        sys.exit(1)


def format_entity(e: dict, verbose: bool = False) -> str:
    """Format an entity for terminal output."""
    name = e.get("name_en", e.get("name", "?"))
    etype = e.get("type", "?")
    qid = e.get("qid", e.get("key_wikidata", "?"))
    ids = e.get("external_ids", {})

    lines = [f"\033[1m{name}\033[0m  ({etype})  {qid}"]

    if verbose:
        aliases = e.get("aliases_en")
        if aliases:
            lines.append(f"  Aliases: {aliases}")

        bio_parts = []
        if e.get("date_of_birth"):
            bio_parts.append(f"DOB: {e['date_of_birth']}")
        if e.get("nationality"):
            bio_parts.append(f"Nationality: {e['nationality']}")
        if e.get("position"):
            bio_parts.append(f"Position: {e['position']}")
        if e.get("height_cm"):
            bio_parts.append(f"Height: {e['height_cm']}cm")
        if e.get("country"):
            bio_parts.append(f"Country: {e['country']}")
        if e.get("stadium"):
            bio_parts.append(f"Stadium: {e['stadium']}")
        if bio_parts:
            lines.append(f"  {' | '.join(bio_parts)}")

    if ids:
        max_key = max(len(k) for k in ids)
        for k, v in sorted(ids.items()):
            lines.append(f"  {k:<{max_key}}  {v}")
    else:
        lines.append("  (no provider IDs)")

    return "\n".join(lines)


def cmd_search(args):
    """Search by name."""
    params = f"/search?name={urllib.parse.quote(args.name)}&limit={args.limit}"
    if args.type:
        params += f"&type={args.type}"
    data = api_get(params)

    if not data.get("results"):
        print("No results found.")
        return

    for e in data["results"]:
        print(format_entity(e, verbose=args.verbose))
        print()


def cmd_resolve(args):
    """Resolve provider ID."""
    data = api_get(f"/resolve?provider={urllib.parse.quote(args.provider)}&id={urllib.parse.quote(args.id)}")

    if not data.get("results"):
        print(f"No entity found for {args.provider}={args.id}")
        return

    for e in data["results"]:
        print(format_entity(e, verbose=True))


def cmd_lookup(args):
    """Lookup by Wikidata QID."""
    url = f"/lookup?qid={urllib.parse.quote(args.qid)}"
    if args.type:
        url += f"&type={args.type}"
    data = api_get(url)

    if not data.get("results"):
        print(f"No entity found for {args.qid}")
        return

    for e in data["results"]:
        print(format_entity(e, verbose=True))


def cmd_translate(args):
    """Translate ID from one provider to another."""
    data = api_get(f"/resolve?provider={urllib.parse.quote(args.source)}&id={urllib.parse.quote(args.id)}")

    if not data.get("results"):
        print(f"No entity found for {args.source}={args.id}")
        return

    e = data["results"][0]
    ids = e.get("external_ids", {})
    target_id = ids.get(args.target)

    if target_id:
        print(target_id)
    else:
        print(f"No {args.target} ID found for {e.get('name_en', '?')} ({args.source}={args.id})", file=sys.stderr)
        sys.exit(1)


def cmd_download(args):
    """Download latest CSVs from GitHub."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    files = ["people.csv", "teams.csv", "names.csv", "meta.json"]
    for filename in files:
        url = f"{GITHUB_RAW}/{filename}"
        out_path = DATA_DIR / filename
        print(f"Downloading {filename}...", end=" ", flush=True)
        try:
            urllib.request.urlretrieve(url, out_path)
            size = out_path.stat().st_size
            print(f"OK ({size / 1024 / 1024:.1f}MB)" if size > 1024 * 1024 else f"OK ({size / 1024:.0f}KB)")
        except Exception as e:
            print(f"FAILED: {e}")

    print(f"\nData saved to {DATA_DIR}")

    # Show meta
    meta_path = DATA_DIR / "meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        print(f"Generated: {meta.get('generated_at', '?')}")
        counts = meta.get("counts", {})
        print(f"People: {counts.get('people', '?')}, Teams: {counts.get('teams', '?')}, Aliases: {counts.get('aliases', '?')}")


def cmd_local_search(args):
    """Search local CSV files (offline mode)."""
    people_path = DATA_DIR / "people.csv"
    teams_path = DATA_DIR / "teams.csv"

    if not people_path.exists():
        print("No local data. Run 'reep download' first.", file=sys.stderr)
        sys.exit(1)

    query = args.name.lower()
    results = []

    # Search people
    if not args.type or args.type in ("player", "coach"):
        with open(people_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if query in row.get("name", "").lower() or query in row.get("full_name", "").lower():
                    results.append(row)
                    if len(results) >= args.limit:
                        break

    # Search teams
    if not args.type or args.type == "team":
        with open(teams_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if query in row.get("name", "").lower():
                    results.append({**row, "type": "team"})
                    if len(results) >= args.limit:
                        break

    if not results:
        print("No results found.")
        return

    for row in results[:args.limit]:
        ids = {k.replace("key_", ""): v for k, v in row.items() if k.startswith("key_") and v and k != "key_wikidata"}
        e = {
            "name_en": row.get("name", "?"),
            "type": row.get("type", "?"),
            "qid": row.get("key_wikidata", "?"),
            "external_ids": ids,
            **{k: v for k, v in row.items() if k in ("date_of_birth", "nationality", "position", "height_cm", "country", "stadium", "aliases_en", "full_name")},
        }
        print(format_entity(e, verbose=args.verbose))
        print()


def cmd_stats(args):
    """Show database statistics."""
    data = api_get("/stats")
    print(f"Total entities: {data.get('total_entities', '?'):,}")
    print()

    print("By type:")
    for t, c in sorted(data.get("by_type", {}).items()):
        print(f"  {t:<10} {c:>10,}")

    print()
    print("By provider:")
    for p, c in sorted(data.get("by_provider", {}).items(), key=lambda x: -x[1]):
        print(f"  {p:<25} {c:>10,}")


def main():
    import urllib.parse

    parser = argparse.ArgumentParser(
        prog="reep",
        description="The Football Entity Register — cross-provider ID lookup",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # search
    p_search = sub.add_parser("search", help="Search entities by name (online)")
    p_search.add_argument("name", help="Name to search for")
    p_search.add_argument("--type", choices=["player", "team", "coach"])
    p_search.add_argument("--limit", type=int, default=10)
    p_search.add_argument("-v", "--verbose", action="store_true")
    p_search.set_defaults(func=cmd_search)

    # resolve
    p_resolve = sub.add_parser("resolve", help="Resolve a provider ID to all IDs")
    p_resolve.add_argument("provider", choices=PROVIDERS)
    p_resolve.add_argument("id", help="ID from the provider")
    p_resolve.set_defaults(func=cmd_resolve)

    # lookup
    p_lookup = sub.add_parser("lookup", help="Look up by Wikidata QID")
    p_lookup.add_argument("qid", help="Wikidata QID (e.g. Q99760796)")
    p_lookup.add_argument("--type", choices=["player", "team", "coach"])
    p_lookup.set_defaults(func=cmd_lookup)

    # translate
    p_translate = sub.add_parser("translate", help="Translate ID between providers (outputs just the ID)")
    p_translate.add_argument("source", choices=PROVIDERS, help="Source provider")
    p_translate.add_argument("id", help="Source ID")
    p_translate.add_argument("target", choices=PROVIDERS, help="Target provider")
    p_translate.set_defaults(func=cmd_translate)

    # download
    p_download = sub.add_parser("download", help="Download latest CSVs for offline use")
    p_download.set_defaults(func=cmd_download)

    # local
    p_local = sub.add_parser("local", help="Search local CSV files (offline)")
    p_local.add_argument("name", help="Name to search for")
    p_local.add_argument("--type", choices=["player", "team", "coach"])
    p_local.add_argument("--limit", type=int, default=10)
    p_local.add_argument("-v", "--verbose", action="store_true")
    p_local.set_defaults(func=cmd_local_search)

    # stats
    p_stats = sub.add_parser("stats", help="Show database statistics")
    p_stats.set_defaults(func=cmd_stats)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
