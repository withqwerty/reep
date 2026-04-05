export interface Env {
  DB: D1Database;
  RAPIDAPI_PROXY_SECRET?: string;
  BYPASS_KEY?: string;
}

const API_VERSION = "2.0.0";

const VALID_PROVIDERS = new Set([
  "wikidata",
  "transfermarkt",
  "transfermarkt_manager",
  "fbref",
  "fbref_verified",
  "soccerway",
  "sofascore",
  "flashscore",
  "opta",
  "premier_league",
  "11v11",
  "espn",
  "national_football_teams",
  "worldfootball",
  "soccerbase",
  "kicker",
  "uefa",
  "lequipe",
  "fff_fr",
  "serie_a",
  "besoccer",
  "footballdatabase_eu",
  "eu_football_info",
  "hugman",
  "german_fa",
  "statmuse_pl",
  "sofifa",
  "soccerdonna",
  "dongqiudi",
  "playmakerstats",
  "understat",
  "whoscored",
  "clubelo",
  "sportmonks",
  "api_football",
  "fotmob",
  "fpl_code",
  "thesportsdb",
  "impect",
  "wyscout",
  "skillcorner",
  "heimspiel",
]);

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const JSON_HEADERS = { "Content-Type": "application/json", ...CORS };

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const start = Date.now();

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: CORS });
    }

    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;
    const params = Object.fromEntries(url.searchParams);

    if (method !== "GET" && method !== "POST") {
      console.log(JSON.stringify({ method, path, params, status: 405, ms: Date.now() - start }));
      return json({ error: "Method not allowed" }, 405);
    }

    // Auth: RapidAPI proxy secret or bypass key for internal use
    // Fail closed: if RAPIDAPI_PROXY_SECRET is not configured, reject all requests
    if (!env.RAPIDAPI_PROXY_SECRET) {
      return json({ error: "Server misconfigured" }, 500);
    }

    const proxySecret = request.headers.get("X-RapidAPI-Proxy-Secret");
    const bypassKey = request.headers.get("X-Reep-Key");
    const isRapidApi = await safeCompare(proxySecret || "", env.RAPIDAPI_PROXY_SECRET);
    const isBypass = env.BYPASS_KEY ? await safeCompare(bypassKey || "", env.BYPASS_KEY) : false;

    if (!isRapidApi && !isBypass) {
      console.log(JSON.stringify({ method, path, params, status: 401, ms: Date.now() - start }));
      return json({ error: "Unauthorized. Subscribe at https://rapidapi.com/withqwerty-withqwerty-default/api/the-reep-register" }, 401);
    }

    let response: Response;

    if (path === "/" || path === "") {
      response = json({
        name: "Reep — The Football Entity Register",
        version: API_VERSION,
        docs: "https://github.com/withqwerty/reep",
        endpoints: {
          "GET /lookup": "Look up an entity by Reep ID or Wikidata QID (?id=reep_p... or ?id=Q...)",
          "GET /search": "Search entities by name (prefix matching, e.g. 'Cole Palm')",
          "GET /resolve": "Resolve a provider ID to all other provider IDs",
          "GET /stats": "Database statistics",
          "POST /batch/lookup": "Look up multiple IDs in one request (max 100)",
          "POST /batch/resolve": "Resolve multiple provider IDs in one request (max 100)",
        },
      });
    } else if (method === "GET" && path === "/lookup") {
      response = await handleLookup(url.searchParams, env.DB);
    } else if (method === "GET" && path === "/search") {
      response = await handleSearch(url.searchParams, env.DB);
    } else if (method === "GET" && path === "/resolve") {
      response = await handleResolve(url.searchParams, env.DB);
    } else if (method === "GET" && path === "/stats") {
      response = await handleStats(env.DB);
    } else if (method === "POST" && path === "/batch/lookup") {
      response = await handleBatchLookup(request, env.DB);
    } else if (method === "POST" && path === "/batch/resolve") {
      response = await handleBatchResolve(request, env.DB);
    } else {
      response = json({ error: "Not found" }, 404);
    }

    console.log(JSON.stringify({ method, path, params, status: response.status, ms: Date.now() - start }));
    return response;
  },
} satisfies ExportedHandler<Env>;

/** Constant-time string comparison to prevent timing side-channels on auth secrets. */
async function safeCompare(a: string, b: string): Promise<boolean> {
  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw", enc.encode("reep-auth"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
  );
  const [sigA, sigB] = await Promise.all([
    crypto.subtle.sign("HMAC", key, enc.encode(a)),
    crypto.subtle.sign("HMAC", key, enc.encode(b)),
  ]);
  const bufA = new Uint8Array(sigA);
  const bufB = new Uint8Array(sigB);
  if (bufA.length !== bufB.length) return false;
  let diff = 0;
  for (let i = 0; i < bufA.length; i++) diff |= bufA[i] ^ bufB[i];
  return diff === 0;
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...JSON_HEADERS, "Cache-Control": "public, max-age=3600" },
  });
}

// Fetch all provider IDs for an entity by reep_id.
async function fetchAllIds(db: D1Database, reepId: string): Promise<Record<string, string>> {
  const [providerResult, customResult] = await Promise.all([
    db.prepare("SELECT provider, external_id FROM provider_ids WHERE reep_id = ?").bind(reepId).all(),
    db.prepare("SELECT provider, external_id FROM custom_ids WHERE reep_id = ?").bind(reepId).all(),
  ]);

  const ids: Record<string, string> = {};
  for (const r of providerResult.results) ids[r.provider as string] = r.external_id as string;
  // Custom IDs fill gaps, don't overwrite provider_ids (Wikidata-sourced)
  for (const r of customResult.results) {
    if (!(r.provider as string in ids)) ids[r.provider as string] = r.external_id as string;
  }

  return ids;
}

// GET /lookup?id=reep_p2804f5db  OR  ?id=Q99760796  OR  ?qid=Q99760796 (legacy)
async function handleLookup(
  params: URLSearchParams,
  db: D1Database,
): Promise<Response> {
  const id = params.get("id") || params.get("qid");
  if (!id) {
    return json(
      { error: "Required: ?id=reep_p2804f5db (Reep ID) or ?id=Q99760796 (Wikidata QID)" },
      400,
    );
  }

  const type = params.get("type");

  // Detect ID type by prefix
  if (id.startsWith("reep_")) {
    // Direct Reep ID lookup
    const entity = await lookupByReepId(db, id);
    if (!entity) return json({ results: [], count: 0 });
    return json({ results: [entity], count: 1 });
  }

  // QID lookup — resolve via provider_ids (provider=wikidata)
  const reepIds = await db
    .prepare("SELECT reep_id FROM provider_ids WHERE provider = 'wikidata' AND external_id = ?")
    .bind(id)
    .all();

  if (reepIds.results.length === 0) {
    return json({ results: [], count: 0 });
  }

  // May return multiple (player + coach with same QID)
  let entities = await Promise.all(
    reepIds.results.map((r) => lookupByReepId(db, r.reep_id as string)),
  );

  let results = entities.filter(Boolean) as Record<string, unknown>[];

  if (type) {
    results = results.filter((e) => e.type === type);
  }

  return json({ results, count: results.length });
}

// GET /search?name=Cole+Palmer&type=player&limit=20
async function handleSearch(
  params: URLSearchParams,
  db: D1Database,
): Promise<Response> {
  const name = params.get("name");
  if (!name) {
    return json(
      { error: "Required: ?name=Cole Palmer" },
      400,
    );
  }

  const type = params.get("type");
  const limit = Math.min(Number(params.get("limit")) || 25, 100);

  // Sanitize FTS query: strip non-word chars, quote each token, prefix-match last token
  const sanitized = name.replace(/[^\p{L}\p{N}\s'-]/gu, " ").trim();
  if (!sanitized) {
    return json({ results: [], count: 0 });
  }
  const tokens = sanitized.split(/\s+/).filter(Boolean);
  const ftsQuery = tokens
    .map((t, i) => '"' + t.replace(/"/g, '""') + '"' + (i === tokens.length - 1 ? "*" : ""))
    .join(" ");

  let query = `
    SELECT e.reep_id, e.type, e.name_en, e.aliases_en,
           e.date_of_birth, e.nationality, e.position,
           bm25(entities_fts, 10.0, 1.0) AS score
    FROM entities_fts
    JOIN entities e ON e.rowid = entities_fts.rowid
    WHERE entities_fts MATCH ?`;
  const binds: (string | number)[] = [ftsQuery];

  if (type) {
    query += " AND e.type = ?";
    binds.push(type);
  } else {
    // Exclude seasons from default search — they pollute results with
    // "2024-25 Premier League", "2023-24 Premier League" etc.
    // Use ?type=season explicitly to search seasons.
    query += " AND e.type != 'season'";
  }

  query += " ORDER BY score LIMIT ?";
  binds.push(limit);

  let entities: D1Result<Record<string, unknown>>;
  try {
    entities = await db.prepare(query).bind(...binds).all();
  } catch {
    return json({ error: "Invalid search query" }, 400);
  }

  const results = await Promise.all(
    entities.results.map(async (e) => {
      const ids = await fetchAllIds(db, e.reep_id as string);
      return {
        reep_id: e.reep_id,
        qid: ids.wikidata ?? null,
        type: e.type,
        name_en: e.name_en,
        aliases_en: e.aliases_en,
        date_of_birth: e.date_of_birth,
        nationality: e.nationality,
        position: e.position,
        external_ids: ids,
      };
    }),
  );

  return json({ results, count: results.length });
}

// GET /resolve?provider=transfermarkt&id=568177
async function handleResolve(
  params: URLSearchParams,
  db: D1Database,
): Promise<Response> {
  const provider = params.get("provider");
  const id = params.get("id");

  if (!provider || !id) {
    return json(
      {
        error: "Required: ?provider=transfermarkt&id=568177",
        providers: [...VALID_PROVIDERS],
      },
      400,
    );
  }

  if (!VALID_PROVIDERS.has(provider)) {
    return json(
      {
        error: `Unknown provider: ${provider}`,
        providers: [...VALID_PROVIDERS],
      },
      400,
    );
  }

  const entity = await resolveEntity(db, provider, id);
  if (!entity) return json({ results: [], count: 0 });

  return json({ results: [entity], count: 1 });
}

const ENTITY_COLS = "reep_id, type, name_en, aliases_en, full_name, date_of_birth, nationality, position, current_team_reep_id, height_cm, country, founded, stadium, source, competition_reep_id";

// Helper: look up entity by reep_id, attach provider IDs and qid convenience field
async function lookupByReepId(db: D1Database, reepId: string): Promise<Record<string, unknown> | null> {
  const entity = await db
    .prepare(`SELECT ${ENTITY_COLS} FROM entities WHERE reep_id = ?`)
    .bind(reepId)
    .first();

  if (!entity) return null;

  const ids = await fetchAllIds(db, reepId);
  return { ...entity, qid: ids.wikidata ?? null, external_ids: ids };
}

// Helper: resolve a provider+id to an entity
async function resolveEntity(db: D1Database, provider: string, id: string): Promise<Record<string, unknown> | null> {
  // Search provider_ids first (Wikidata-sourced), then custom_ids
  let match = await db
    .prepare("SELECT reep_id FROM provider_ids WHERE provider = ? AND external_id = ?")
    .bind(provider, id)
    .first();

  if (!match) {
    match = await db
      .prepare("SELECT reep_id FROM custom_ids WHERE provider = ? AND external_id = ?")
      .bind(provider, id)
      .first();
  }

  if (!match) return null;
  return lookupByReepId(db, match.reep_id as string);
}

const BATCH_MAX = 100;

// POST /batch/lookup — body: { ids: ["reep_p...", "Q99760796", ...] }
// Also accepts legacy { qids: [...] } format
async function handleBatchLookup(request: Request, db: D1Database): Promise<Response> {
  let body: { ids?: string[]; qids?: string[] };
  try {
    body = await request.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const ids = body.ids || body.qids;
  if (!Array.isArray(ids) || ids.length === 0) {
    return json({ error: 'Required: { ids: ["reep_p...", "Q99760796", ...] }' }, 400);
  }

  if (ids.length > BATCH_MAX) {
    return json({ error: `Maximum ${BATCH_MAX} IDs per request` }, 400);
  }

  const nested = await Promise.all(
    ids.map(async (id) => {
      if (id.startsWith("reep_")) {
        const entity = await lookupByReepId(db, id);
        return entity ? [entity] : [{ id, error: "not_found" }];
      }
      // QID lookup
      const reepIds = await db
        .prepare("SELECT reep_id FROM provider_ids WHERE provider = 'wikidata' AND external_id = ?")
        .bind(id)
        .all();
      if (reepIds.results.length === 0) return [{ qid: id, error: "not_found" }];
      const entities = await Promise.all(
        reepIds.results.map((r) => lookupByReepId(db, r.reep_id as string)),
      );
      const valid = entities.filter(Boolean) as Record<string, unknown>[];
      return valid.length > 0 ? valid : [{ qid: id, error: "not_found" }];
    }),
  );
  const results = nested.flat();

  return json({ results, count: results.length });
}

// POST /batch/resolve — body: { items: [{ provider: "transfermarkt", id: "568177" }, ...] }
async function handleBatchResolve(request: Request, db: D1Database): Promise<Response> {
  let body: { items?: { provider: string; id: string }[] };
  try {
    body = await request.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const items = body.items;
  if (!Array.isArray(items) || items.length === 0) {
    return json({ error: 'Required: { items: [{ provider: "transfermarkt", id: "568177" }, ...] }' }, 400);
  }

  if (items.length > BATCH_MAX) {
    return json({ error: `Maximum ${BATCH_MAX} items per request` }, 400);
  }

  const results = await Promise.all(
    items.map(async ({ provider, id }) => {
      if (!provider || !id) return { provider, id, error: "missing_fields" };
      if (!VALID_PROVIDERS.has(provider)) return { provider, id, error: "unknown_provider" };
      const entity = await resolveEntity(db, provider, id);
      return entity ?? { provider, id, error: "not_found" };
    }),
  );

  return json({ results, count: results.length });
}

// GET /stats
async function handleStats(db: D1Database): Promise<Response> {
  const [counts, providerCounts, customCounts, total, customTotal] = await Promise.all([
    db.prepare("SELECT type, COUNT(*) as count FROM entities GROUP BY type").all(),
    db.prepare("SELECT provider, COUNT(*) as count FROM provider_ids GROUP BY provider ORDER BY count DESC").all(),
    db.prepare("SELECT provider, COUNT(*) as count FROM custom_ids GROUP BY provider ORDER BY count DESC").all(),
    db.prepare("SELECT COUNT(*) as total FROM entities").first(),
    db.prepare("SELECT COUNT(*) as total FROM custom_ids").first(),
  ]);

  const byProvider: Record<string, number> = {};
  for (const r of providerCounts.results) byProvider[r.provider as string] = r.count as number;
  for (const r of customCounts.results) {
    const p = r.provider as string;
    byProvider[p] = (byProvider[p] ?? 0) + (r.count as number);
  }

  return json({
    total_entities: total?.total,
    by_type: Object.fromEntries(counts.results.map((r) => [r.type, r.count])),
    by_provider: Object.fromEntries(
      Object.entries(byProvider).sort(([, a], [, b]) => b - a),
    ),
    custom_ids_count: customTotal?.total ?? 0,
  });
}
