export interface Env {
  DB: D1Database;
  RAPIDAPI_PROXY_SECRET?: string;
  BYPASS_KEY?: string;
}

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const JSON_HEADERS = { "Content-Type": "application/json", ...CORS };

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: CORS });
    }

    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    if (method !== "GET" && method !== "POST") {
      return json({ error: "Method not allowed" }, 405);
    }

    // Auth: RapidAPI proxy secret or bypass key for internal use
    const proxySecret = request.headers.get("X-RapidAPI-Proxy-Secret");
    const bypassKey = request.headers.get("X-Reep-Key");
    const isRapidApi = env.RAPIDAPI_PROXY_SECRET && proxySecret === env.RAPIDAPI_PROXY_SECRET;
    const isBypass = env.BYPASS_KEY && bypassKey === env.BYPASS_KEY;

    if (env.RAPIDAPI_PROXY_SECRET && !isRapidApi && !isBypass) {
      return json({ error: "Unauthorized. Subscribe at https://rapidapi.com/withqwerty-withqwerty-default/api/the-reep-register" }, 401);
    }

    if (path === "/" || path === "") {
      return json({
        name: "Reep — The Football Entity Register",
        version: "1.0.0",
        docs: "https://github.com/withqwerty/reep",
        endpoints: {
          "GET /lookup": "Look up an entity by Wikidata QID",
          "GET /search": "Search entities by name",
          "GET /resolve": "Resolve a provider ID to all other provider IDs",
          "GET /stats": "Database statistics",
          "POST /batch/lookup": "Look up multiple QIDs in one request (max 100)",
          "POST /batch/resolve": "Resolve multiple provider IDs in one request (max 100)",
        },
      });
    }

    if (method === "GET" && path === "/lookup") {
      return handleLookup(url.searchParams, env.DB);
    }

    if (method === "GET" && path === "/search") {
      return handleSearch(url.searchParams, env.DB);
    }

    if (method === "GET" && path === "/resolve") {
      return handleResolve(url.searchParams, env.DB);
    }

    if (method === "GET" && path === "/stats") {
      return handleStats(env.DB);
    }

    if (method === "POST" && path === "/batch/lookup") {
      return handleBatchLookup(request, env.DB);
    }

    if (method === "POST" && path === "/batch/resolve") {
      return handleBatchResolve(request, env.DB);
    }

    return json({ error: "Not found" }, 404);
  },
} satisfies ExportedHandler<Env>;

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...JSON_HEADERS, "Cache-Control": "public, max-age=3600" },
  });
}

// Fetch all IDs for a QID from both Wikidata and custom sources.
async function fetchAllIds(db: D1Database, qid: string): Promise<Record<string, string>> {
  const [wikidata, custom] = await Promise.all([
    db.prepare("SELECT provider, external_id FROM external_ids WHERE qid = ?").bind(qid).all(),
    db.prepare("SELECT provider, external_id FROM custom_ids WHERE qid = ?").bind(qid).all(),
  ]);

  const ids: Record<string, string> = {};
  for (const r of wikidata.results) ids[r.provider as string] = r.external_id as string;
  // Custom IDs fill gaps, don't overwrite Wikidata
  for (const r of custom.results) {
    if (!(r.provider as string in ids)) ids[r.provider as string] = r.external_id as string;
  }

  return ids;
}

// GET /lookup?qid=Q99760796
async function handleLookup(
  params: URLSearchParams,
  db: D1Database,
): Promise<Response> {
  const qid = params.get("qid");
  if (!qid) {
    return json(
      { error: "Required: ?qid=Q99760796" },
      400,
    );
  }

  const entity = await lookupEntity(db, qid);
  if (!entity) return json({ results: [] });

  return json({ results: [entity] });
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
  const pattern = `%${name}%`;

  let query =
    "SELECT qid, type, name_en, aliases_en, date_of_birth, nationality, position FROM entities WHERE (name_en LIKE ? OR aliases_en LIKE ?)";
  const binds: unknown[] = [pattern, pattern];

  if (type) {
    query += " AND type = ?";
    binds.push(type);
  }

  query += " LIMIT ?";
  binds.push(limit);

  const entities = await db
    .prepare(query)
    .bind(...binds)
    .all();

  const results = await Promise.all(
    entities.results.map(async (e) => ({
      ...e,
      external_ids: await fetchAllIds(db, e.qid as string),
    })),
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
        providers: [
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
          "understat",
          "whoscored",
          "sportmonks",
          "api_football",
          "clubelo",
          "sofifa",
        ],
      },
      400,
    );
  }

  const entity = await resolveEntity(db, provider, id);
  if (!entity) return json({ results: [] });

  return json({ results: [entity] });
}

// Helper: look up a single entity by QID (shared by lookup and batch)
async function lookupEntity(db: D1Database, qid: string): Promise<Record<string, unknown> | null> {
  const entity = await db
    .prepare(
      "SELECT qid, type, name_en, aliases_en, full_name, date_of_birth, nationality, position, current_team_qid, height_cm, country, founded, stadium FROM entities WHERE qid = ?",
    )
    .bind(qid)
    .first();

  if (!entity) return null;

  const external_ids = await fetchAllIds(db, qid);
  return { ...entity, external_ids };
}

// Helper: resolve a provider+id to an entity (shared by resolve and batch)
async function resolveEntity(db: D1Database, provider: string, id: string): Promise<Record<string, unknown> | null> {
  let match = await db
    .prepare("SELECT qid FROM external_ids WHERE provider = ? AND external_id = ?")
    .bind(provider, id)
    .first();

  if (!match) {
    match = await db
      .prepare("SELECT qid FROM custom_ids WHERE provider = ? AND external_id = ?")
      .bind(provider, id)
      .first();
  }

  if (!match) return null;
  return lookupEntity(db, match.qid as string);
}

const BATCH_MAX = 100;

// POST /batch/lookup — body: { qids: ["Q99760796", "Q1354960", ...] }
async function handleBatchLookup(request: Request, db: D1Database): Promise<Response> {
  let body: { qids?: string[] };
  try {
    body = await request.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const qids = body.qids;
  if (!Array.isArray(qids) || qids.length === 0) {
    return json({ error: "Required: { qids: [\"Q99760796\", ...] }" }, 400);
  }

  if (qids.length > BATCH_MAX) {
    return json({ error: `Maximum ${BATCH_MAX} QIDs per request` }, 400);
  }

  const results = await Promise.all(
    qids.map(async (qid) => {
      const entity = await lookupEntity(db, qid);
      return entity ?? { qid, error: "not_found" };
    }),
  );

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
      const entity = await resolveEntity(db, provider, id);
      return entity ?? { provider, id, error: "not_found" };
    }),
  );

  return json({ results, count: results.length });
}

// GET /stats
async function handleStats(db: D1Database): Promise<Response> {
  const [counts, idCounts, customCounts, total, customTotal] = await Promise.all([
    db.prepare("SELECT type, COUNT(*) as count FROM entities GROUP BY type").all(),
    db.prepare("SELECT provider, COUNT(*) as count FROM external_ids GROUP BY provider ORDER BY count DESC").all(),
    db.prepare("SELECT provider, COUNT(*) as count FROM custom_ids GROUP BY provider ORDER BY count DESC").all(),
    db.prepare("SELECT COUNT(*) as total FROM entities").first(),
    db.prepare("SELECT COUNT(*) as total FROM custom_ids").first(),
  ]);

  const byProvider: Record<string, number> = {};
  for (const r of idCounts.results) byProvider[r.provider as string] = r.count as number;
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
