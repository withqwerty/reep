"""
Check what's out of sync after data or code changes.

Run after: new provider added, data refreshed, CSVs regenerated, etc.
Reports what needs updating — doesn't change anything.

Usage:
  python scripts/check-sync.py
"""

import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).parent.parent
PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"
WARN = "\033[33m!\033[0m"

issues = []


def check(label: str, ok: bool, detail: str = ""):
    if ok:
        print(f"  {PASS} {label}")
    else:
        msg = f"{label}: {detail}" if detail else label
        print(f"  {FAIL} {msg}")
        issues.append(msg)


def warn(label: str, detail: str):
    print(f"  {WARN} {label}: {detail}")


# ---------------------------------------------------------------------------
# 1. Collect all providers from D1 data (custom_ids.json + Wikidata JSONs)
# ---------------------------------------------------------------------------

print("Providers in data:")

custom_providers = set()
custom_path = ROOT / "data" / "custom_ids.json"
if custom_path.exists():
    with open(custom_path) as f:
        for entry in json.load(f):
            custom_providers.add(entry["provider"])

wikidata_providers = set()
for fname in ["players.json", "teams.json", "coachs.json"]:
    path = ROOT / "data" / "json" / fname
    if path.exists():
        with open(path) as f:
            for entity in json.load(f):
                wikidata_providers.update(entity.get("external_ids", {}).keys())

all_data_providers = custom_providers | wikidata_providers
print(f"  Wikidata: {len(wikidata_providers)} providers")
print(f"  Custom: {len(custom_providers)} providers")
print(f"  Total: {len(all_data_providers)} unique providers")

# ---------------------------------------------------------------------------
# 2. Check worker.ts provider list
# ---------------------------------------------------------------------------

print("\nworker.ts /resolve provider list:")
worker_path = ROOT / "src" / "worker.ts"
worker_providers = set()
if worker_path.exists():
    text = worker_path.read_text()
    # Find the provider array in the resolve handler
    for match in re.finditer(r'"(\w+)"', text):
        candidate = match.group(1)
        if candidate in all_data_providers:
            worker_providers.add(candidate)

missing_worker = all_data_providers - worker_providers
extra_worker = worker_providers - all_data_providers
check("All data providers in worker", not missing_worker,
      f"missing: {sorted(missing_worker)}")
if extra_worker:
    warn("Worker has providers not in data", str(sorted(extra_worker)))

# ---------------------------------------------------------------------------
# 3. Check openapi.yaml enum
# ---------------------------------------------------------------------------

print("\nopenapi.yaml resolve enum:")
openapi_path = ROOT / "openapi.yaml"
openapi_providers = set()
if openapi_path.exists():
    text = openapi_path.read_text()
    in_enum = False
    for line in text.splitlines():
        stripped = line.strip()
        if "enum:" in stripped:
            in_enum = True
            continue
        if in_enum:
            if stripped.startswith("- "):
                openapi_providers.add(stripped[2:].strip().strip('"').strip("'"))
            else:
                in_enum = False

missing_openapi = all_data_providers - openapi_providers
check("All data providers in openapi.yaml", not missing_openapi,
      f"missing: {sorted(missing_openapi)}")

# ---------------------------------------------------------------------------
# 4. Check CLI provider list
# ---------------------------------------------------------------------------

print("\ncli/reep.py PROVIDERS list:")
cli_path = ROOT / "cli" / "reep.py"
cli_providers = set()
if cli_path.exists():
    text = cli_path.read_text()
    for match in re.finditer(r'"(\w+)"', text):
        candidate = match.group(1)
        if candidate in all_data_providers:
            cli_providers.add(candidate)

missing_cli = all_data_providers - cli_providers
check("All data providers in CLI", not missing_cli,
      f"missing: {sorted(missing_cli)}")

# ---------------------------------------------------------------------------
# 5. Check CSV columns include providers that have data for that type
# ---------------------------------------------------------------------------

print("\nCSV columns:")

# Build provider-to-types map from actual data
provider_types: dict[str, set[str]] = {}
for fname, etype in [("players.json", "player"), ("teams.json", "team"), ("coachs.json", "coach")]:
    path = ROOT / "data" / "json" / fname
    if path.exists():
        with open(path) as f:
            for entity in json.load(f):
                for p in entity.get("external_ids", {}).keys():
                    provider_types.setdefault(p, set()).add(etype)

if custom_path.exists():
    with open(custom_path) as f:
        for entry in json.load(f):
            provider_types.setdefault(entry["provider"], set()).add(entry["type"])

csv_type_map = {"people.csv": {"player", "coach"}, "teams.csv": {"team"}}

for csv_name, types in csv_type_map.items():
    csv_path = ROOT / "data" / csv_name
    if csv_path.exists():
        with open(csv_path) as f:
            headers = next(csv.reader(f))
        csv_providers = {h.replace("key_", "") for h in headers if h.startswith("key_") and h != "key_wikidata"}
        # Only flag providers that actually have data for this CSV's types
        relevant = {p for p, t in provider_types.items() if t & types}
        missing_csv = relevant - csv_providers
        check(f"{csv_name} columns", not missing_csv,
              f"missing: {sorted(missing_csv)}")

# ---------------------------------------------------------------------------
# 6. Check sample CSVs match current column structure
# ---------------------------------------------------------------------------

print("\nSample CSVs:")
for csv_name in ["people.csv", "teams.csv"]:
    full_path = ROOT / "data" / csv_name
    sample_path = ROOT / "data" / "samples" / csv_name
    if full_path.exists() and sample_path.exists():
        with open(full_path) as f:
            full_headers = next(csv.reader(f))
        with open(sample_path) as f:
            sample_headers = next(csv.reader(f))
        check(f"samples/{csv_name} headers match",
              full_headers == sample_headers,
              f"columns differ — regenerate samples")

# ---------------------------------------------------------------------------
# 7. Check README coverage table
# ---------------------------------------------------------------------------

print("\nREADME.md coverage table:")
readme_path = ROOT / "README.md"
if readme_path.exists():
    text = readme_path.read_text()
    readme_providers = set()
    # Match key_<provider> in table rows
    for match in re.finditer(r'`key_(\w+)`', text):
        readme_providers.add(match.group(1))
    missing_readme = all_data_providers - readme_providers
    if missing_readme:
        warn("Providers not in README", str(sorted(missing_readme)))
    else:
        check("All providers in README", True)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print(f"\n{'='*50}")
if issues:
    print(f"{len(issues)} issue(s) found:")
    for issue in issues:
        print(f"  • {issue}")
else:
    print("Everything in sync!")
