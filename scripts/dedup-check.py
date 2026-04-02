"""
Check for potential duplicate entities between Opta-imported and Wikidata-sourced.

Phase 3, Unit 10: Compares new Opta entities (source=opta, qid starts with reep_)
against existing Wikidata entities using DOB + name similarity.

Usage:
  python scripts/dedup-check.py            # check remote D1
  python scripts/dedup-check.py --local    # check local D1
"""

import argparse
import json
import subprocess
import sys
import unicodedata
from pathlib import Path

DB_NAME = "football-entities"
REPO_ROOT = Path(__file__).parent.parent
NAME_SIMILARITY_THRESHOLD = 0.67


def query_d1(sql: str, remote: bool = True) -> list[dict]:
    cmd = ["npx", "wrangler", "d1", "execute", DB_NAME, f"--command={sql}"]
    if remote:
        cmd.append("--remote")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60,
                            cwd=str(REPO_ROOT))
    try:
        data = json.loads(
            result.stdout[result.stdout.index("["):result.stdout.rindex("]") + 1]
        )
        return data[0].get("results", [])
    except (json.JSONDecodeError, ValueError, IndexError):
        return []


def normalize(name: str) -> str:
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def name_similarity(a: str, b: str) -> float:
    tokens_a = set(normalize(a).split())
    tokens_b = set(normalize(b).split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    if not intersection:
        return 0.0
    if tokens_a == tokens_b:
        return 1.0
    if len(intersection) == 1 and len(tokens_a) > 1 and len(tokens_b) > 1:
        return 0.0
    shorter, longer = (tokens_a, tokens_b) if len(tokens_a) <= len(tokens_b) else (tokens_b, tokens_a)
    if shorter <= longer:
        return 0.9
    return len(intersection) / max(len(tokens_a), len(tokens_b))


def main():
    parser = argparse.ArgumentParser(description="Check for duplicate entities")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()
    remote = not args.local

    # Load Opta-imported entities (qid starts with reep_, meaning no Wikidata QID)
    print("Loading Opta-imported entities...")
    opta_entities = []
    offset = 0
    while True:
        rows = query_d1(
            f"SELECT reep_id, name_en, date_of_birth FROM entities "
            f"WHERE qid LIKE 'reep_%' AND type = 'player' "
            f"LIMIT 10000 OFFSET {offset};",
            remote=remote,
        )
        if not rows:
            break
        opta_entities.extend(rows)
        offset += 10000
    print(f"  {len(opta_entities):,} Opta-imported entities")

    if not opta_entities:
        print("No Opta-imported entities found. Run import-opta-entities.py first.")
        return

    # Build DOB index for Opta entities
    opta_by_dob: dict[str, list[dict]] = {}
    no_dob = 0
    for e in opta_entities:
        dob = e.get("date_of_birth")
        if dob:
            opta_by_dob.setdefault(dob, []).append(e)
        else:
            no_dob += 1
    print(f"  {len(opta_by_dob):,} unique DOBs, {no_dob:,} without DOB")

    # Load Wikidata-sourced entities with matching DOBs
    dob_list = list(opta_by_dob.keys())
    print(f"\nChecking Wikidata entities for {len(dob_list):,} DOBs...")
    potential_dupes = []

    for i in range(0, len(dob_list), 100):
        batch_dobs = dob_list[i:i + 100]
        dob_sql = ", ".join(f"'{d}'" for d in batch_dobs)
        rows = query_d1(
            f"SELECT reep_id, qid, name_en, date_of_birth FROM entities "
            f"WHERE date_of_birth IN ({dob_sql}) "
            f"AND qid LIKE 'Q%' AND type = 'player';",
            remote=remote,
        )

        # Compare each Wikidata entity against Opta entities with same DOB
        for wd in rows:
            dob = wd["date_of_birth"]
            for opta in opta_by_dob.get(dob, []):
                score = name_similarity(opta["name_en"], wd["name_en"])
                if score >= NAME_SIMILARITY_THRESHOLD:
                    potential_dupes.append({
                        "opta_reep_id": opta["reep_id"],
                        "opta_name": opta["name_en"],
                        "wd_reep_id": wd["reep_id"],
                        "wd_qid": wd["qid"],
                        "wd_name": wd["name_en"],
                        "dob": dob,
                        "score": round(score, 2),
                    })

        if (i // 100) % 10 == 0 and i > 0:
            print(f"  Checked {i:,}/{len(dob_list):,} DOBs, {len(potential_dupes):,} potential dupes so far...")

    # Results
    print(f"\n{'='*60}")
    print(f"Dedup check complete")
    print(f"{'='*60}")
    print(f"  Opta entities checked: {len(opta_entities):,}")
    print(f"  Potential duplicates found: {len(potential_dupes):,}")

    if not potential_dupes:
        print("\nNo duplicates found. Safe to proceed.")
        return

    # Sort by score descending
    potential_dupes.sort(key=lambda x: -x["score"])

    print(f"\nPotential duplicates (score >= {NAME_SIMILARITY_THRESHOLD}):")
    print(f"{'Opta Name':<30} {'WD Name':<30} {'DOB':<12} {'Score':<6} {'Opta ID':<16} {'WD QID'}")
    print("-" * 120)
    for d in potential_dupes[:50]:
        print(
            f"{d['opta_name'][:29]:<30} {d['wd_name'][:29]:<30} {d['dob']:<12} "
            f"{d['score']:<6} {d['opta_reep_id']:<16} {d['wd_qid']}"
        )

    if len(potential_dupes) > 50:
        print(f"\n... and {len(potential_dupes) - 50} more")

    # Write full results to file
    output_path = REPO_ROOT / "data" / "dedup-report.json"
    with open(output_path, "w") as f:
        json.dump(potential_dupes, f, indent=2)
    print(f"\nFull report written to {output_path}")

    # Summary by score bracket
    high = sum(1 for d in potential_dupes if d["score"] >= 0.9)
    med = sum(1 for d in potential_dupes if 0.8 <= d["score"] < 0.9)
    low = sum(1 for d in potential_dupes if d["score"] < 0.8)
    print(f"\nScore distribution:")
    print(f"  >= 0.90 (likely duplicate): {high}")
    print(f"  0.80-0.89 (probable): {med}")
    print(f"  < 0.80 (possible): {low}")


if __name__ == "__main__":
    main()
