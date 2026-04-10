"""
Fetch custom_ids from D1 and write to data/custom_ids.json.

This is the bridge between reep-custom (private scraping scripts) and
the public CSV export. The JSON file makes custom provider mappings
available without exposing how they were sourced.

Usage:
  python scripts/fetch-custom-ids.py            # fetch from remote D1
  python scripts/fetch-custom-ids.py --local    # fetch from local D1
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

DB_NAME = "football-entities"
OUTPUT = Path(__file__).parent.parent / "data" / "custom_ids.json"
REEP_ID_MAP_OUTPUT = Path(__file__).parent.parent / "data" / "reep_id_map.json"
REPO_ROOT = Path(__file__).parent.parent

BATCH_SIZE = 5000
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = (5, 15, 30)


class QueryError(RuntimeError):
    """Raised when a D1 query fails — auth error, wrangler crash, timeout,
    or unparseable response. The caller MUST let this propagate rather than
    silently treating it as an empty result set, because a pagination loop
    that breaks on `[]` will write an incomplete file with exit code 0."""


def query_d1(sql: str, local: bool = False, attempts: int = MAX_RETRIES) -> list[dict]:
    """Run a SQL query against D1 and return result rows.

    Raises QueryError on unrecoverable failure. Automatically retries up
    to `attempts` times on transient errors (non-zero return code, empty
    stdout, JSON parse failures) with exponential backoff. Empty result
    sets (valid JSON with `"results": []`) are NOT an error and return
    a legitimate empty list.
    """
    cmd = ["npx", "wrangler", "d1", "execute", DB_NAME, f"--command={sql}"]
    if not local:
        cmd.append("--remote")

    last_error = ""
    for attempt in range(1, attempts + 1):
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60, cwd=str(REPO_ROOT)
        )

        # Check 1: subprocess exit code
        if result.returncode != 0:
            last_error = (
                f"wrangler exit {result.returncode}: "
                f"{(result.stderr or result.stdout).strip()[:400]}"
            )
        else:
            # Check 2: parseable JSON
            try:
                data = json.loads(
                    result.stdout[result.stdout.index("[") : result.stdout.rindex("]") + 1]
                )
            except (json.JSONDecodeError, ValueError, IndexError) as e:
                last_error = (
                    f"parse error: {e}; "
                    f"stdout head={result.stdout[:200]!r} "
                    f"stderr head={result.stderr[:200]!r}"
                )
            else:
                # Success path — extract results (may be legitimately empty)
                if data and isinstance(data, list) and isinstance(data[0], dict):
                    return data[0].get("results", [])
                last_error = f"unexpected response shape: {data!r}"

        # Transient failure — retry with backoff
        if attempt < attempts:
            wait = RETRY_BACKOFF_SECONDS[min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)]
            print(
                f"  [retry {attempt}/{attempts - 1}] query failed ({last_error[:200]}); "
                f"waiting {wait}s...",
                file=sys.stderr,
            )
            time.sleep(wait)

    raise QueryError(f"query failed after {attempts} attempts: {last_error}")


def paginated_fetch(
    query_fn,
    expected_total: int,
    local: bool,
    label: str,
) -> list[dict]:
    """Fetch a paginated result set and verify the total matches expectation.

    query_fn(offset) must return the rows for `offset` to `offset + BATCH_SIZE`.
    This helper:
      - keeps pulling until the underlying query returns an empty batch
      - raises if fewer rows arrived than `expected_total` (partial data)
      - raises if more rows arrived than `expected_total` (schema drift)
    Both cases would previously have written a corrupt json file.
    """
    all_rows: list[dict] = []
    offset = 0
    while True:
        rows = query_fn(offset)
        if not rows:
            break
        all_rows.extend(rows)
        offset += len(rows)
        print(f"  {label}: fetched {len(all_rows)}/{expected_total}")
        if len(rows) < BATCH_SIZE:
            break  # short page — legitimate end of data

    if expected_total and len(all_rows) != expected_total:
        raise QueryError(
            f"{label}: expected {expected_total} rows but got {len(all_rows)} — "
            f"refusing to write partial data. Re-run the script."
        )
    return all_rows


def main():
    parser = argparse.ArgumentParser(description="Fetch custom_ids from D1")
    parser.add_argument("--local", action="store_true", help="Use local D1 instead of remote")
    args = parser.parse_args()

    try:
        _fetch_all(args.local)
    except QueryError as e:
        print(f"\nFATAL: {e}", file=sys.stderr)
        print(
            "Aborted without writing any output files. Previous files on disk "
            "are untouched and still reflect the last successful run.",
            file=sys.stderr,
        )
        sys.exit(1)


def _fetch_all(local: bool) -> None:
    # ---- custom_ids ----
    count_rows = query_d1("SELECT COUNT(*) as total FROM custom_ids;", local)
    total = count_rows[0]["total"] if count_rows else 0
    print(f"custom_ids in D1: {total}")

    if total == 0:
        print("No custom IDs found.")
        return

    def _fetch_custom_ids(offset: int) -> list[dict]:
        return query_d1(
            f"SELECT reep_id, provider, external_id, source, confidence "
            f"FROM custom_ids ORDER BY reep_id, provider "
            f"LIMIT {BATCH_SIZE} OFFSET {offset};",
            local,
        )

    all_rows = paginated_fetch(_fetch_custom_ids, total, local, "custom_ids")

    # Summary by provider
    providers: dict[str, int] = {}
    for row in all_rows:
        providers[row["provider"]] = providers.get(row["provider"], 0) + 1

    # ---- reep_id map (wikidata provider only) ----
    # Production entities table has no qid column — we reconstruct the
    # map from provider_ids rows. Grab the expected count first so the
    # paginated fetch can verify completeness.
    print("\nFetching reep_id map...")
    map_count_rows = query_d1(
        "SELECT COUNT(*) as total FROM provider_ids WHERE provider = 'wikidata';",
        local,
    )
    map_total = map_count_rows[0]["total"] if map_count_rows else 0
    print(f"  wikidata rows in D1: {map_total}")

    def _fetch_reep_map(offset: int) -> list[dict]:
        return query_d1(
            f"SELECT p.external_id AS qid, e.type, e.reep_id "
            f"FROM provider_ids p "
            f"JOIN entities e ON e.reep_id = p.reep_id "
            f"WHERE p.provider = 'wikidata' "
            f"ORDER BY p.reep_id "
            f"LIMIT {BATCH_SIZE} OFFSET {offset};",
            local,
        )

    reep_map_rows = paginated_fetch(_fetch_reep_map, map_total, local, "reep_id_map")
    reep_map: dict[str, str] = {}
    for r in reep_map_rows:
        reep_map[f"{r['qid']}:{r['type']}"] = r["reep_id"]

    # ---- custom_aliases ----
    ALIASES_OUTPUT = Path(__file__).parent.parent / "data" / "custom_aliases.json"
    print("\nFetching custom_aliases...")
    alias_count_rows = query_d1("SELECT COUNT(*) as total FROM custom_aliases;", local)
    alias_total = alias_count_rows[0]["total"] if alias_count_rows else 0
    print(f"  custom_aliases in D1: {alias_total}")

    def _fetch_aliases(offset: int) -> list[dict]:
        return query_d1(
            f"SELECT reep_id, alias, provider, language FROM custom_aliases "
            f"ORDER BY reep_id LIMIT {BATCH_SIZE} OFFSET {offset};",
            local,
        )

    alias_rows = paginated_fetch(_fetch_aliases, alias_total, local, "custom_aliases")

    # ---- position_detail ----
    POSITION_DETAIL_OUTPUT = Path(__file__).parent.parent / "data" / "position_detail.json"
    print("\nFetching position_detail...")
    pos_count_rows = query_d1(
        "SELECT COUNT(*) as total FROM entities WHERE position_detail IS NOT NULL;",
        local,
    )
    pos_total = pos_count_rows[0]["total"] if pos_count_rows else 0
    print(f"  position_detail rows in D1: {pos_total}")

    def _fetch_positions(offset: int) -> list[dict]:
        return query_d1(
            f"SELECT reep_id, position_detail FROM entities "
            f"WHERE position_detail IS NOT NULL "
            f"ORDER BY reep_id "
            f"LIMIT {BATCH_SIZE} OFFSET {offset};",
            local,
        )

    pos_rows = paginated_fetch(_fetch_positions, pos_total, local, "position_detail")
    pos_map: dict[str, str] = {r["reep_id"]: r["position_detail"] for r in pos_rows}

    # ---- All fetches complete and verified. Write outputs atomically. ----
    # Only reach here if every paginated fetch hit its expected count, so
    # writing partial data is no longer a failure mode.
    _write_json(OUTPUT, all_rows)
    print(f"\nWrote {len(all_rows)} custom IDs to {OUTPUT}")
    print("Providers:")
    for p, count in sorted(providers.items(), key=lambda x: -x[1]):
        print(f"  {p}: {count}")

    _write_json(REEP_ID_MAP_OUTPUT, reep_map)
    print(f"Wrote {len(reep_map):,} reep_id mappings to {REEP_ID_MAP_OUTPUT}")

    _write_json(ALIASES_OUTPUT, alias_rows)
    print(f"Wrote {len(alias_rows)} custom aliases to {ALIASES_OUTPUT}")

    _write_json(POSITION_DETAIL_OUTPUT, pos_map)
    print(f"Wrote {len(pos_map):,} position_detail values to {POSITION_DETAIL_OUTPUT}")


def _write_json(path: Path, data) -> None:
    """Write JSON atomically via a temp file + rename, so a crash mid-write
    doesn't leave a half-truncated file on disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2 if isinstance(data, list) else None)
    tmp.replace(path)


if __name__ == "__main__":
    main()
