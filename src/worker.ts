export interface Env {
  DB: D1Database;
  RAPIDAPI_PROXY_SECRET?: string;
}

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

const JSON_HEADERS = { "Content-Type": "application/json", ...CORS };

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: CORS });
    }

    if (request.method !== "GET") {
      return json({ error: "Method not allowed" }, 405);
    }

    const url = new URL(request.url);
    const path = url.pathname;

    // RapidAPI proxy secret validation (if configured)
    if (env.RAPIDAPI_PROXY_SECRET) {
      const proxySecret = request.headers.get("X-RapidAPI-Proxy-Secret");
      if (proxySecret !== env.RAPIDAPI_PROXY_SECRET) {
        // Allow direct access too (free tier) -- only block if header is present but wrong
        if (proxySecret) {
          return json({ error: "Unauthorized" }, 401);
        }
      }
    }

    if (path === "/" || path === "") {
      return json({
        name: "Reep — The Football Entity Register",
        version: "1.0.0",
        docs: "https://github.com/withqwerty/reep",
        endpoints: {
          "/lookup": "Look up an entity by Wikidata QID",
          "/search": "Search entities by name",
          "/resolve": "Resolve a provider ID to all other provider IDs",
          "/stats": "Database statistics",
        },
      });
    }

    if (path === "/lookup") {
      return handleLookup(url.searchParams, env.DB);
    }

    if (path === "/search") {
      return handleSearch(url.searchParams, env.DB);
    }

    if (path === "/resolve") {
      return handleResolve(url.searchParams, env.DB);
    }

    if (path === "/stats") {
      return handleStats(env.DB);
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

// Fetch all IDs for a QID (Wikidata + custom, merged)
async function fetchAllIds(db: D1Database, qid: string): Promise<Record<string, string>> {
  const [wikidata, custom] = await Promise.all([
    db.prepare("SELECT provider, external_id FROM external_ids WHERE qid = ?").bind(qid).all(),
    db.prepare("SELECT provider, external_id FROM custom_ids WHERE qid = ?").bind(qid).all(),
  ]);
  // Wikidata IDs first, custom IDs fill gaps (don't overwrite)
  const ids: Record<string, string> = {};
  for (const r of wikidata.results) ids[r.provider as string] = r.external_id as string;
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

  const entity = await db
    .prepare(
      "SELECT qid, type, name_en, aliases_en, full_name, date_of_birth, nationality, position, current_team_qid, height_cm, country, founded, stadium FROM entities WHERE qid = ?",
    )
    .bind(qid)
    .first();

  if (!entity) return json({ results: [] });

  const external_ids = await fetchAllIds(db, qid);

  return json({
    results: [{ ...entity, external_ids }],
  });
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
        ],
      },
      400,
    );
  }

  // Check both tables for the provider+id
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

  if (!match) return json({ results: [] });

  // Delegate to lookup
  const lookupParams = new URLSearchParams({ qid: match.qid as string });
  return handleLookup(lookupParams, db);
}

// GET /stats
async function handleStats(db: D1Database): Promise<Response> {
  const [counts, idCounts, customCounts, total] = await Promise.all([
    db.prepare("SELECT type, COUNT(*) as count FROM entities GROUP BY type").all(),
    db.prepare("SELECT provider, COUNT(*) as count FROM external_ids GROUP BY provider ORDER BY count DESC").all(),
    db.prepare("SELECT provider, COUNT(*) as count FROM custom_ids GROUP BY provider ORDER BY count DESC").all(),
    db.prepare("SELECT COUNT(*) as total FROM entities").first(),
  ]);

  // Merge provider counts from both tables
  const byProvider: Record<string, number> = {};
  for (const r of idCounts.results) byProvider[r.provider as string] = r.count as number;
  for (const r of customCounts.results) {
    const p = r.provider as string;
    byProvider[p] = (byProvider[p] ?? 0) + (r.count as number);
  }

  const customTotal = await db.prepare("SELECT COUNT(*) as total FROM custom_ids").first();

  return json({
    total_entities: total?.total,
    by_type: Object.fromEntries(counts.results.map((r) => [r.type, r.count])),
    by_provider: Object.fromEntries(
      Object.entries(byProvider).sort(([, a], [, b]) => b - a),
    ),
    custom_ids_count: customTotal?.total ?? 0,
  });
}
