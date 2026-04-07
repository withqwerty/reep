import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";

const API_BASE = "https://reep-api.rahulkeerthi2-95d.workers.dev";

const REEP_KEY = process.env.REEP_KEY;
if (!REEP_KEY) {
  console.warn(
    "[build-data] REEP_KEY not set — skipping data fetch. Cached files (if any) will be used."
  );
  process.exit(0);
}

const headers = { "X-Reep-Key": REEP_KEY };

const OUT_DIR = join(import.meta.dirname, "../src/lib");

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function apiFetch(path: string): Promise<unknown> {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, { headers });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} from ${url}`);
  }
  return res.json();
}

interface Entity {
  reep_id: string;
  qid: string | null;
  type: string;
  name_en: string;
  aliases_en: string[] | null;
  full_name: string | null;
  date_of_birth: string | null;
  nationality: string | null;
  position: string | null;
  height_cm: number | null;
  country: string | null;
  competition_reep_id: string | null;
  external_ids: Record<string, string>;
}

interface LookupResponse {
  results: Entity[];
  count: number;
}

interface SearchResponse {
  results: Entity[];
  count: number;
}

interface StatsResponse {
  total_entities: number;
  by_type: Record<string, number>;
  by_provider: Record<string, number>;
  custom_ids_count: number;
}

interface SlimEntity {
  reep_id: string;
  qid: string | null;
  name_en: string;
  aliases_en: string[] | null;
  type: string;
  date_of_birth: string | null;
  nationality: string | null;
  country: string | null;
  position: string | null;
  provider_count: number;
  external_ids: Record<string, string>;
}

function toSlim(entity: Entity): SlimEntity {
  const providerCount = Object.keys(entity.external_ids ?? {}).length;
  return {
    reep_id: entity.reep_id,
    qid: entity.qid ?? null,
    name_en: entity.name_en,
    aliases_en: entity.aliases_en ?? null,
    type: entity.type,
    date_of_birth: entity.date_of_birth ?? null,
    nationality: entity.nationality ?? null,
    country: entity.country ?? null,
    position: entity.position ?? null,
    provider_count: providerCount,
    external_ids: entity.external_ids ?? {},
  };
}

async function main() {
  mkdirSync(OUT_DIR, { recursive: true });

  // --- 1. Fetch stats ---
  console.log("[build-data] Fetching /stats...");
  const stats = (await apiFetch("/stats")) as StatsResponse;
  console.log(
    `[build-data] Stats: ${stats.total_entities.toLocaleString()} entities total`
  );
  await sleep(50);

  // --- 2. Showcase lookups ---
  const SHOWCASE_IDS = [
    { id: "reep_p2804f5db", label: "Cole Palmer (player)" },
    { id: "reep_t8596499a", label: "Arsenal (team)" },
    { id: "reep_lb3d230cb", label: "Premier League (competition)" },
    { id: "reep_sa7f63ba6", label: "2024-25 Premier League (season)" },
  ];

  const examples: Entity[] = [];

  for (const { id, label } of SHOWCASE_IDS) {
    try {
      console.log(`[build-data] Looking up ${id} (${label})...`);
      const data = (await apiFetch(`/lookup?id=${id}`)) as LookupResponse;
      if (data.results && data.results.length > 0) {
        const entity = data.results[0];
        console.log(`[build-data]   -> ${entity.name_en} (${entity.type})`);

        // Sanity-check the Arsenal ID
        if (id === "reep_t8596499a") {
          if (!entity.name_en?.toLowerCase().includes("arsenal")) {
            console.warn(
              `[build-data] WARNING: Expected Arsenal but got "${entity.name_en}" for ${id}`
            );
          }
        }

        examples.push(entity);
      } else {
        console.warn(`[build-data] WARNING: No results for ${id} (${label})`);
      }
    } catch (err) {
      console.warn(
        `[build-data] WARNING: Lookup failed for ${id} (${label}): ${err}`
      );
    }
    await sleep(50);
  }

  console.log(`[build-data] Fetched ${examples.length}/4 showcase entities`);

  // --- 3. Build search index ---
  const allEntities = new Map<string, Entity>();

  // Seed the index with showcase entities so they're always searchable
  for (const entity of examples) {
    allEntities.set(entity.reep_id, entity);
  }

  // a-z single-letter queries + famous players/teams that users will try
  const letters = "abcdefghijklmnopqrstuvwxyz".split("");
  const footballTerms = [
    "Real",
    "Real Madrid",
    "FC",
    "United",
    "City",
    "Athletic",
    "Sporting",
    "Premier",
    "Serie",
    "Liga",
    "Ligue",
    "Bundesliga",
    "Cup",
  ];
  const playerNames = [
    "Messi", "Ronaldo", "Haaland", "Mbappe", "Salah", "Mohamed Salah", "Palmer",
    "Bellingham", "Vinicius", "Saka", "Foden", "De Bruyne", "Modric",
    "Son", "Kane", "Lewandowski", "Neymar", "Pedri", "Gavi",
    "Osimhen", "Wirtz", "Yamal", "Odegaard", "Rice",
  ];
  const queries = [...letters, ...footballTerms, ...playerNames];

  console.log(
    `[build-data] Building search index with ${queries.length} queries...`
  );

  let queryCount = 0;
  for (const q of queries) {
    try {
      const encoded = encodeURIComponent(q);
      const data = (await apiFetch(
        `/search?name=${encoded}&limit=100`
      )) as SearchResponse;
      const results = data.results ?? [];
      for (const entity of results) {
        if (!allEntities.has(entity.reep_id)) {
          allEntities.set(entity.reep_id, entity);
        }
      }
      queryCount++;
      if (queryCount % 10 === 0) {
        process.stdout.write(
          `[build-data] Progress: ${queryCount}/${queries.length} queries, ${allEntities.size.toLocaleString()} unique entities so far\n`
        );
      }
    } catch (err) {
      console.warn(`[build-data] WARNING: Search failed for "${q}": ${err}`);
    }
    await sleep(50);
  }

  console.log(
    `[build-data] Raw search pool: ${allEntities.size.toLocaleString()} unique entities`
  );

  // Filter: only entities with 2+ provider IDs (entities with just a Wikidata QID
  // aren't useful for a crosswalk demo). Sort by provider count descending, take top 10,000.
  const sortedEntities = Array.from(allEntities.values())
    .filter((e) => Object.keys(e.external_ids ?? {}).length >= 2)
    .sort((a, b) => {
      const aCount = Object.keys(a.external_ids ?? {}).length;
      const bCount = Object.keys(b.external_ids ?? {}).length;
      return bCount - aCount;
    })
    .slice(0, 10_000);

  const searchIndex: SlimEntity[] = sortedEntities.map(toSlim);

  console.log(
    `[build-data] Search index: ${searchIndex.length.toLocaleString()} entries (top by provider count)`
  );

  // --- 4. Write output files ---
  const examplesPayload = {
    stats,
    examples,
    generatedAt: new Date().toISOString(),
  };

  const examplesPath = join(OUT_DIR, "examples.json");
  writeFileSync(examplesPath, JSON.stringify(examplesPayload, null, 2));
  console.log(`[build-data] Wrote ${examplesPath}`);

  const searchIndexPath = join(OUT_DIR, "search-index.json");
  writeFileSync(searchIndexPath, JSON.stringify(searchIndex, null, 2));
  console.log(`[build-data] Wrote ${searchIndexPath}`);

  // Also write to public/ so it's served as a static asset for client-side search
  const publicDir = join(import.meta.dirname, "../public");
  mkdirSync(publicDir, { recursive: true });
  const publicSearchPath = join(publicDir, "search-index.json");
  writeFileSync(publicSearchPath, JSON.stringify(searchIndex));
  console.log(`[build-data] Wrote ${publicSearchPath}`);

  console.log("[build-data] Done.");
}

main().catch((err) => {
  console.error("[build-data] Fatal error:", err);
  process.exit(1);
});
