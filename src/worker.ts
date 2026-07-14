export interface Env {
  DB: D1Database;
  CONTRIBUTIONS?: R2Bucket;
  RAPIDAPI_PROXY_SECRET?: string;
  BYPASS_KEY?: string;
  // Unkey — direct API subscribers. Keys live in the `reep` namespace (api_IcuIgs3L).
  // UNKEY_ROOT_KEY is used to call POST /v2/keys.verifyKey on the hot path.
  // Absent in dev if you're only testing RapidAPI/bypass auth — the worker will
  // silently skip the Unkey auth path rather than failing closed on the whole request.
  UNKEY_ROOT_KEY?: string;
  UNKEY_API_ID?: string;
  RESEND_API_KEY?: string;
  NOTIFICATION_EMAIL?: string;
}

interface R2EventNotification {
  account: string;
  bucket: string;
  action: string;
  object?: { key: string; size: number; eTag: string };
}

const API_VERSION = "2.7.0";

// The v0 register is FROZEN (migration bridge). D1 has had no data written since
// 2026-04-25; the living register moved to Reep v1 at reep.football. Every
// response says so — in a `data` block on / and /health, and on X-Reep-Data-*
// headers everywhere — so a caller can never mistake an April snapshot for
// current data. Update DATA_AS_OF only if the v0 D1 is genuinely reloaded.
const DATA_AS_OF = "2026-04-25";
const DATA_STATUS = "frozen";
const MIGRATION_URL = "https://reep.football/api/migration";
const FREEZE_NOTICE =
  `This is the frozen v0 register: its data has not been updated since ${DATA_AS_OF} ` +
  `and it will not be refreshed. Transfers, new players and corrections since that date ` +
  `are absent. The maintained register is Reep v1 — see ${MIGRATION_URL}.`;

const DATA_FRESHNESS = {
  status: DATA_STATUS,
  as_of: DATA_AS_OF,
  notice: FREEZE_NOTICE,
  migration: MIGRATION_URL,
} as const;

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
  "opta_numeric",
  "optacore",
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
  "thesportsdb",
  "impect",
  "wyscout",
  "skillcorner",
  "heimspiel",
  "capology",
]);

const VALID_ENTITY_TYPES = new Set([
  "player",
  "team",
  "coach",
  "competition",
  "season",
  "match",
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

    // Public health check (no auth). Returns 200 if the worker is reachable
    // and can query D1; 503 if the DB ping fails.
    if (method === "GET" && path === "/health") {
      let dbOk = true;
      try {
        await env.DB.prepare("SELECT 1").first();
      } catch {
        dbOk = false;
      }
      const status = dbOk ? 200 : 503;
      const body = {
        status: dbOk ? "ok" : "degraded",
        version: API_VERSION,
        db: dbOk ? "ok" : "error",
        // `status: ok` means the worker and D1 are reachable — NOT that the data
        // is current. It is not: see `data`.
        data: DATA_FRESHNESS,
        timestamp: new Date().toISOString(),
      };
      console.log(JSON.stringify({ method, path, params, status, ms: Date.now() - start }));
      return new Response(JSON.stringify(body), {
        status,
        headers: { ...JSON_HEADERS, "Cache-Control": "no-store" },
      });
    }

    // POST /contribute — anonymous file upload to R2 (no auth required)
    if (method === "POST" && path === "/contribute") {
      if (!env.CONTRIBUTIONS) {
        return json({ error: "Contributions not configured" }, 503);
      }
      const contentLength = parseInt(request.headers.get("content-length") || "0", 10);
      if (contentLength > 10 * 1024 * 1024) {
        return json({ error: "File too large (max 10MB)" }, 413);
      }

      try {
        const formData = await request.formData();
        const file = formData.get("file") as unknown;
        if (!isUploadedFile(file)) {
          return json({ error: "No file provided" }, 400);
        }

        const ts = new Date().toISOString().replace(/[:.]/g, "-");
        const safeName = file.name.replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 100);
        const key = `${ts}_${safeName}`;

        await env.CONTRIBUTIONS.put(key, file.stream(), {
          httpMetadata: { contentType: file.type || "application/octet-stream" },
          customMetadata: {
            originalName: file.name,
            size: String(file.size),
            uploadedAt: new Date().toISOString(),
            ip: request.headers.get("cf-connecting-ip") || "unknown",
          },
        });

        console.log(JSON.stringify({ method, path, file: safeName, size: file.size, status: 200, ms: Date.now() - start }));
        return json({ ok: true, message: "Thanks! We'll review your data." });
      } catch (e) {
        console.log(JSON.stringify({ method, path, status: 500, error: String(e), ms: Date.now() - start }));
        return json({ error: "Upload failed" }, 500);
      }
    }

    // ---------- Auth: three paths, tried in order ----------
    //   1. X-RapidAPI-Proxy-Secret  — RapidAPI-originated traffic (legacy marketplace listing)
    //   2. X-Reep-Key               — shared BYPASS_KEY for internal tools (e.g. reep-site build script)
    //   3. Authorization: Bearer …  — direct API subscribers, verified via Unkey with per-call credit deduction
    //
    // Fail closed: if RAPIDAPI_PROXY_SECRET is not configured, reject all requests.
    // (BYPASS_KEY and UNKEY_* are optional — absence just disables their respective paths.)
    if (!env.RAPIDAPI_PROXY_SECRET) {
      return json({ error: "Server misconfigured" }, 500);
    }

    const proxySecret = request.headers.get("X-RapidAPI-Proxy-Secret");
    const bypassKey = request.headers.get("X-Reep-Key");
    const bearerKey = extractBearer(request.headers.get("Authorization"));

    const isRapidApi = await safeCompare(proxySecret || "", env.RAPIDAPI_PROXY_SECRET);
    const isBypass = env.BYPASS_KEY ? await safeCompare(bypassKey || "", env.BYPASS_KEY) : false;

    // Unkey path: only consulted if the caller presented a Bearer token and the env is configured.
    // `unkeyResult` carries the rate-limit / credits state back to the outer handler so we can
    // surface X-Reep-* headers on the success response.
    let unkeyResult: UnkeyVerifyOk | null = null;
    let unkeyError: UnkeyVerifyFail | null = null;
    if (!isRapidApi && !isBypass && bearerKey && env.UNKEY_ROOT_KEY) {
      const cost = await computeCost(request, path, method);
      const result = await verifyUnkey(bearerKey, cost, env.UNKEY_ROOT_KEY);
      if (result.ok) {
        unkeyResult = result;
      } else {
        unkeyError = result;
      }
    }

    const authed = isRapidApi || isBypass || unkeyResult !== null;
    if (!authed) {
      console.log(JSON.stringify({ method, path, params, status: 401, ms: Date.now() - start }));
      // If the caller presented a Bearer token that failed verification, return Unkey's mapped error.
      // Otherwise return a generic 401 pointing at the signup page.
      if (unkeyError) {
        return json(unkeyError.body, unkeyError.status);
      }
      return json(
        {
          error: "unauthorized",
          message: "Get a free API key at https://reep.football/signup or subscribe at https://reep.football/pricing",
        },
        401,
      );
    }

    let response: Response;

    if (path === "/" || path === "") {
      response = json({
        name: "Reep — The Football Entity Register (v0, frozen)",
        version: API_VERSION,
        data: DATA_FRESHNESS,
        docs: "https://github.com/withqwerty/reep",
        endpoints: {
          "GET /health": "Public health check (no auth). Returns 200 if worker + D1 are reachable",
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

    // Surface Unkey credit + rate-limit state on responses when the caller authed via Bearer token.
    // X-Reep-Credits-Remaining : post-deduction credit balance on the key
    // X-Reep-Plan              : plan tier from key meta (free / plus / pro) — set by createKey in the webhook
    if (unkeyResult && response.ok) {
      const headers = new Headers(response.headers);
      // credits comes back as a bare integer on verifyKey (not nested)
      const creditsRaw = unkeyResult.data.credits;
      const remaining = typeof creditsRaw === "number" ? creditsRaw : creditsRaw?.remaining;
      if (typeof remaining === "number") {
        headers.set("X-Reep-Credits-Remaining", String(remaining));
      }
      const plan = unkeyResult.data.meta && typeof unkeyResult.data.meta.plan === "string"
        ? unkeyResult.data.meta.plan
        : undefined;
      if (plan) headers.set("X-Reep-Plan", plan);
      // Forward the first rate-limit bucket's state (most keys will only have one configured)
      const firstRl = unkeyResult.data.ratelimits?.[0];
      if (firstRl) {
        if (typeof firstRl.remaining === "number") {
          headers.set("X-Reep-Ratelimit-Remaining", String(firstRl.remaining));
        }
        if (typeof firstRl.reset === "number") {
          headers.set("X-Reep-Ratelimit-Reset", String(firstRl.reset));
        }
      }
      response = new Response(response.body, { status: response.status, headers });
    }

    // Freeze disclosure on EVERY response. A caller reading only headers, or only
    // the JSON body, still learns the data is an April snapshot rather than a live
    // register. `Warning: 299` is the standard stale-response advisory; the `data`
    // key is additive, so existing clients reading `results` are unaffected.
    {
      const headers = new Headers(response.headers);
      headers.set("X-Reep-Data-Status", DATA_STATUS);
      headers.set("X-Reep-Data-As-Of", DATA_AS_OF);
      headers.set("X-Reep-Migration", MIGRATION_URL);
      headers.set("Warning", `299 - "${FREEZE_NOTICE}"`);

      // Read the body ONCE (never clone-and-reuse the original stream: passing a
      // tee'd stream to a new Response throws, which 1101'd `/` — the one route
      // whose body already carries `data`).
      let body: BodyInit | null = response.body;
      if (response.headers.get("Content-Type")?.includes("application/json")) {
        const text = await response.text();
        body = text;
        try {
          const parsed = JSON.parse(text);
          if (parsed && typeof parsed === "object" && !Array.isArray(parsed) && !("data" in parsed)) {
            body = JSON.stringify({ ...parsed, data: DATA_FRESHNESS });
          }
        } catch {
          // Unparseable body: pass it through verbatim; the headers still disclose.
        }
      }
      response = new Response(body, { status: response.status, headers });
    }

    console.log(JSON.stringify({ method, path, params, status: response.status, ms: Date.now() - start }));
    return response;
  },
  async queue(batch: MessageBatch<R2EventNotification>, env: Env): Promise<void> {
    for (const msg of batch.messages) {
      if (!env.RESEND_API_KEY || !env.NOTIFICATION_EMAIL) {
        msg.ack();
        continue;
      }

      try {
        const event = msg.body;
        const key = event.object?.key ?? "unknown";
        const size = event.object?.size ?? 0;
        const action = event.action ?? "unknown";

        const subject = `New contribution: ${key.split("_").slice(1).join("_") || key}`;
        const html = formatEmailBody(key, size, action);

        await fetch("https://api.resend.com/emails", {
          method: "POST",
          headers: {
            Authorization: `Bearer ${env.RESEND_API_KEY}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            from: "Reep <notifications@withqwerty.com>",
            to: [env.NOTIFICATION_EMAIL],
            subject,
            html,
          }),
        });
      } catch (e) {
        console.log(JSON.stringify({ handler: "queue", error: String(e) }));
      }

      msg.ack();
    }
  },
} satisfies ExportedHandler<Env, R2EventNotification>;

// ---------- Unkey auth (direct API subscribers) ----------

// Endpoints that do not deduct credits. Discovery (/search), diagnostics (/stats),
// and public routes (/health, /contribute, /) are free to call as long as the key is valid.
const FREE_PATHS = new Set(["/", "/search", "/stats", "/health", "/contribute"]);

// Pricing (all tiers configure rate limits + refill at key creation time, so the
// worker just needs to know how much to deduct per call).
const UPGRADE_URL = "https://reep.football/pricing";

interface UnkeyVerifyData {
  valid: boolean;
  code?: string;
  keyId?: string;
  name?: string;
  enabled?: boolean;
  meta?: Record<string, unknown>;
  // Unkey v2's verifyKey endpoint returns credits as a bare integer (the
  // remaining count), NOT a nested {remaining} object. The listKeys endpoint
  // returns the nested shape — they're inconsistent. Accept both so this type
  // is safe to reuse if we ever parse listKeys responses in the worker.
  credits?: number | { remaining?: number };
  permissions?: string[];
  identity?: { id?: string; externalId?: string };
  ratelimits?: Array<{ name: string; remaining?: number; reset?: number }>;
}

interface UnkeyVerifyOk {
  ok: true;
  data: UnkeyVerifyData;
}

interface UnkeyVerifyFail {
  ok: false;
  status: number;
  body: Record<string, unknown>;
}

type UnkeyVerifyResult = UnkeyVerifyOk | UnkeyVerifyFail;

/**
 * Call Unkey POST /v2/keys.verifyKey with optional credit deduction.
 * Atomic: verification and deduction happen in a single call, with Unkey's
 * key-level rate limits (configured at createKey time) enforced server-side.
 *
 * Returns `{ok: true, data}` on VALID, or `{ok: false, status, body}` with a
 * ready-to-return JSON error for anything else. Network / 5xx failures map to
 * 503 (fail closed — no free lookups on Unkey outage).
 */
async function verifyUnkey(
  key: string,
  cost: number,
  rootKey: string,
): Promise<UnkeyVerifyResult> {
  // Always pass credits.cost explicitly — including cost: 0 for free paths.
  // Unkey's verifyKey deducts 1 credit by default when `credits` is omitted
  // (despite docs suggesting otherwise), which would silently burn a credit
  // on every /search call. Explicit cost: 0 skips the deduction cleanly.
  const body: Record<string, unknown> = { key, credits: { cost } };

  let response: Response;
  try {
    response = await fetch("https://api.unkey.com/v2/keys.verifyKey", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${rootKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
  } catch (err) {
    return {
      ok: false,
      status: 503,
      body: { error: "auth_service_unavailable", detail: String(err) },
    };
  }

  if (!response.ok) {
    return {
      ok: false,
      status: 503,
      body: {
        error: "auth_service_error",
        upstream_status: response.status,
      },
    };
  }

  let payload: { data?: UnkeyVerifyData };
  try {
    payload = (await response.json()) as { data?: UnkeyVerifyData };
  } catch {
    return { ok: false, status: 503, body: { error: "auth_service_bad_response" } };
  }

  const data = payload.data;
  if (!data) {
    return { ok: false, status: 503, body: { error: "auth_service_bad_response" } };
  }

  if (data.valid) return { ok: true, data };

  // Map Unkey's invalid-key codes to HTTP statuses.
  switch (data.code) {
    case "USAGE_EXCEEDED":
    case "INSUFFICIENT_CREDITS":
      return {
        ok: false,
        status: 402,
        body: {
          error: "quota_exceeded",
          message: `You've used your monthly Reep API quota. Upgrade at ${UPGRADE_URL} for a bigger plan.`,
          upgrade_url: UPGRADE_URL,
        },
      };
    case "RATE_LIMITED":
      return {
        ok: false,
        status: 429,
        body: {
          error: "rate_limited",
          message: "You're hitting your plan's rate limit. Slow down, upgrade, or batch requests.",
          upgrade_url: UPGRADE_URL,
        },
      };
    case "INSUFFICIENT_PERMISSIONS":
      return {
        ok: false,
        status: 403,
        body: { error: "forbidden", detail: "Your key does not have permission for this endpoint." },
      };
    case "NOT_FOUND":
    case "DISABLED":
    case "EXPIRED":
    default:
      return {
        ok: false,
        status: 401,
        body: {
          error: "unauthorized",
          detail: `Invalid or revoked API key${data.code ? ` (${data.code.toLowerCase()})` : ""}.`,
        },
      };
  }
}

/**
 * Compute the credit cost of a request without consuming its body.
 * Uses request.clone() so the original stream stays intact for the handler.
 * Malformed batch bodies default to cost 1 — the handler will reject them
 * with a 400, but the caller still pays a minimal penalty for the attempt.
 */
async function computeCost(
  request: Request,
  path: string,
  method: string,
): Promise<number> {
  if (FREE_PATHS.has(path)) return 0;
  if (method === "GET" && (path === "/lookup" || path === "/resolve")) return 1;
  if (method === "POST" && (path === "/batch/lookup" || path === "/batch/resolve")) {
    try {
      const body = (await request.clone().json()) as {
        ids?: unknown[];
        items?: unknown[];
      };
      const arr = path === "/batch/lookup" ? body.ids : body.items;
      if (Array.isArray(arr) && arr.length > 0) {
        return Math.min(arr.length, BATCH_MAX);
      }
    } catch {
      /* fall through */
    }
    return 1;
  }
  return 1;
}

/**
 * Extract the Bearer token from an Authorization header.
 * Returns null for missing / malformed headers.
 */
function extractBearer(header: string | null): string | null {
  if (!header) return null;
  const match = header.match(/^Bearer\s+(\S+)$/i);
  return match ? match[1] : null;
}

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

function isUploadedFile(value: unknown): value is File {
  if (!value || typeof value !== "object") return false;
  return (
    "name" in value &&
    "size" in value &&
    "type" in value &&
    "stream" in value &&
    typeof value.stream === "function"
  );
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

// GET /lookup?id=reep_p2804f5db  OR  ?id=reep_m1234abcd  OR  ?id=Q99760796  OR  ?qid=Q99760796 (legacy)
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
    // Direct Reep ID lookup — tombstones surface with deprecation meta
    // or as a 410 Gone for retirements (WIT-41).
    const entity = await lookupByReepId(db, id);
    if (!entity) return json({ results: [], count: 0 });
    if (isRetirementSentinel(entity)) {
      return json(entity, 410);
    }
    return json({ results: [entity], count: 1 });
  }

  // QID lookup — resolve via provider_ids (provider=wikidata).
  // Tombstones on this path redirect SILENTLY to the canonical (WIT-41
  // decision: the caller asked for Q42, not a specific reep_id; give
  // them the canonical data without a deprecation marker). Retired
  // entities on this path surface as not_found (the QID mapping is
  // effectively gone).
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

  let results = (entities.filter(Boolean) as Record<string, unknown>[])
    .filter((e) => !isRetirementSentinel(e))
    .map(stripDeprecationMeta);

  if (type) {
    results = results.filter((e) => e.type === type);
  }

  return json({ results, count: results.length });
}

// GET /search?name=Cole+Palmer&type=player&limit=20&targets=wyscout,fbref
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

  // Optional ?targets=wyscout,fbref — filter external_ids to just these providers.
  // When absent, the full cross-provider ID map is returned.
  const targets = parseTargets(params.get("targets"));

  // Sanitize FTS query: strip non-word chars, quote each token, prefix-match last token
  const sanitized = name.replace(/[^\p{L}\p{N}\s'-]/gu, " ").trim();
  if (!sanitized) {
    return json({ results: [], count: 0 });
  }
  const tokens = sanitized.split(/\s+/).filter(Boolean);
  const ftsQuery = tokens
    .map((t, i) => '"' + t.replace(/"/g, '""') + '"' + (i === tokens.length - 1 ? "*" : ""))
    .join(" ");

  // Filter tombstones from search results (WIT-41). WIT-49 FTS triggers
  // already remove soft-deleted rows from the index on every UPDATE, so
  // the JOIN filter is defence-in-depth for rows that slipped in before
  // the trigger split.
  let query = `
    SELECT e.reep_id, e.type, e.name_en, e.aliases_en,
           e.date_of_birth, e.nationality, e.position, e.position_detail,
           m.match_date, m.kickoff_utc, m.home_team_reep_id, m.away_team_reep_id,
           m.home_score, m.away_score, m.venue, m.referee, m.attendance,
           m.round_label, m.season_reep_id, m.season_label,
           bm25(entities_fts, 10.0, 1.0) AS score
    FROM entities_fts
    JOIN entities e ON e.rowid = entities_fts.rowid
    LEFT JOIN matches m ON m.reep_id = e.reep_id
    WHERE entities_fts MATCH ?
      AND e.deleted_at IS NULL`;
  const binds: (string | number)[] = [ftsQuery];

  if (type) {
    query += " AND e.type = ?";
    binds.push(type);
  } else {
    // Exclude seasons and matches from default search — they pollute results
    // with seasonal editions and fixture rows when the common case is still
    // player/team/coach/competition lookup.
    query += " AND e.type NOT IN ('season', 'match')";
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
      const externalIds = targets ? filterIds(ids, targets) : ids;
      // qid is a convenience alias for external_ids.wikidata. If the caller
      // narrowed targets and didn't ask for wikidata, honour that and drop qid.
      const qid = !targets || targets.includes("wikidata") ? ids.wikidata ?? null : null;
      return {
        reep_id: e.reep_id,
        qid,
        type: e.type,
        name_en: e.name_en,
        aliases_en: e.aliases_en,
        date_of_birth: e.date_of_birth,
        nationality: e.nationality,
        position: e.position,
        position_detail: e.position_detail,
        match_date: e.match_date,
        kickoff_utc: e.kickoff_utc,
        home_team_reep_id: e.home_team_reep_id,
        away_team_reep_id: e.away_team_reep_id,
        home_score: e.home_score,
        away_score: e.away_score,
        venue: e.venue,
        referee: e.referee,
        attendance: e.attendance,
        round_label: e.round_label,
        season_reep_id: e.season_reep_id,
        season_label: e.season_label,
        external_ids: externalIds,
      };
    }),
  );

  return json({ results, count: results.length });
}

// GET /resolve?provider=transfermarkt&id=568177&type=player
async function handleResolve(
  params: URLSearchParams,
  db: D1Database,
): Promise<Response> {
  const provider = params.get("provider");
  const id = params.get("id");
  const type = params.get("type");

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

  if (type && !VALID_ENTITY_TYPES.has(type)) {
    return json(
      {
        error: `Unknown type: ${type}`,
        types: [...VALID_ENTITY_TYPES],
      },
      400,
    );
  }

  const resolved = await resolveEntity(db, provider, id, type || undefined);
  if (resolved.kind === "not_found") return json({ results: [], count: 0 });
  if (resolved.kind === "ambiguous") {
    return json(
      {
        error: "ambiguous_id",
        provider,
        id,
        types: resolved.types,
      },
      409,
    );
  }

  return json({ results: [resolved.entity], count: 1 });
}

const ENTITY_COLS = "e.reep_id, e.type, e.name_en, e.aliases_en, e.full_name, e.date_of_birth, e.nationality, e.position, e.position_detail, e.current_team_reep_id, e.height_cm, e.country, e.founded, e.stadium, e.source, e.competition_reep_id";
const MATCH_COLS = "m.match_date, m.kickoff_utc, m.home_team_reep_id, m.away_team_reep_id, m.home_score, m.away_score, m.venue, m.referee, m.attendance, m.round_label, m.season_reep_id, m.season_label";

// Helper: look up entity by reep_id, attach provider IDs and qid convenience
// field. Soft-delete-aware (WIT-40/41):
//
//   - live entity:          { ...fields, qid, external_ids }
//   - tombstone w/ canonical: follows canonical_reep_id (1 hop, per WIT-51)
//                             and returns the canonical entity spliced with
//                             { _deprecated: true, _canonical_id, _deprecated_at }
//   - tombstone w/o canonical (retired): returns a sentinel
//                             { _deprecated: true, _canonical_id: null,
//                               _deprecated_at, _deprecated_reason: "retired" }
//                             — callers surface 410 Gone.
//   - canonical itself soft-deleted (chain rot, shouldn't happen post-WIT-51):
//                             returns sentinel with _deprecated_reason: "chain_depth_exceeded"
//
// Pass `{ _followCanonical: false }` to short-circuit the recursion; used
// internally to enforce the 1-hop cap.
async function lookupByReepId(
  db: D1Database,
  reepId: string,
  opts: { _followCanonical?: boolean } = {},
): Promise<Record<string, unknown> | null> {
  const followCanonical = opts._followCanonical !== false;

  const row = await db
    .prepare(
      `SELECT ${ENTITY_COLS}, e.deleted_at, e.canonical_reep_id, ${MATCH_COLS}
       FROM entities e
       LEFT JOIN matches m ON m.reep_id = e.reep_id
       WHERE e.reep_id = ?`
    )
    .bind(reepId)
    .first();

  if (!row) return null;

  const deletedAt = row.deleted_at as string | null;
  const canonicalReepId = row.canonical_reep_id as string | null;
  // Strip the meta columns from the entity shape we'll surface.
  const { deleted_at: _d, canonical_reep_id: _c, ...entity } = row as Record<string, unknown> & {
    deleted_at?: unknown;
    canonical_reep_id?: unknown;
  };

  // Live entity — existing path.
  if (!deletedAt) {
    const ids = await fetchAllIds(db, reepId);
    return { ...entity, qid: ids.wikidata ?? null, external_ids: ids };
  }

  // Tombstone without a successor — return a sentinel; handlers convert to 410.
  if (!canonicalReepId) {
    return {
      _deprecated: true,
      _canonical_id: null,
      _deprecated_at: deletedAt,
      _deprecated_reason: "retired",
    };
  }

  // Second-hop refusal (1-hop cap). WIT-51 collapses chains on every merge,
  // so this should only trip if chain collapse has regressed.
  if (!followCanonical) {
    return {
      _deprecated: true,
      _canonical_id: canonicalReepId,
      _deprecated_at: deletedAt,
      _deprecated_reason: "chain_depth_exceeded",
    };
  }

  const canonical = await lookupByReepId(db, canonicalReepId, { _followCanonical: false });
  // Canonical row missing altogether (validate-db chain-rot would catch this).
  if (!canonical) {
    return {
      _deprecated: true,
      _canonical_id: canonicalReepId,
      _deprecated_at: deletedAt,
      _deprecated_reason: "canonical_missing",
    };
  }
  // Canonical is itself a tombstone (sentinel came back). Propagate as-is.
  if (canonical._deprecated) {
    return canonical;
  }
  // Happy path: splice deprecation meta onto the canonical entity.
  return {
    ...canonical,
    _deprecated: true,
    _canonical_id: canonicalReepId,
    _deprecated_at: deletedAt,
  };
}

// Helper: strip deprecation meta from a lookupByReepId result. Used on
// provider-id → reep_id → entity paths where the caller didn't ask by reep_id,
// so the redirect is silent (WIT-41 design decision: "silent redirect for
// /wikidata/Q42 case" — client didn't name a specific reep_id, just give
// them the canonical data).
function stripDeprecationMeta(entity: Record<string, unknown>): Record<string, unknown> {
  const { _deprecated, _canonical_id, _deprecated_at, _deprecated_reason, ...rest } = entity;
  // Referenced to keep TS happy on unused vars — the destructure is the point.
  void _deprecated;
  void _canonical_id;
  void _deprecated_at;
  void _deprecated_reason;
  return rest;
}

// Helper: test if a lookup result is a retirement sentinel (no canonical,
// no entity fields — callers should return 410 Gone).
function isRetirementSentinel(entity: Record<string, unknown>): boolean {
  return entity._deprecated === true && entity._canonical_id === null;
}

type ResolveResult =
  | { kind: "found"; entity: Record<string, unknown> }
  | { kind: "not_found" }
  | { kind: "ambiguous"; types: string[] };

// Helper: resolve a provider+id to an entity, with optional type disambiguation.
async function resolveEntity(
  db: D1Database,
  provider: string,
  id: string,
  type?: string,
): Promise<ResolveResult> {
  const candidateRows = await db
    .prepare(`
      SELECT reep_id, type FROM (
        SELECT e.reep_id AS reep_id, e.type AS type
        FROM provider_ids p
        JOIN entities e ON e.reep_id = p.reep_id
        WHERE p.provider = ? AND p.external_id = ?
        UNION ALL
        SELECT e.reep_id AS reep_id, e.type AS type
        FROM custom_ids c
        JOIN entities e ON e.reep_id = c.reep_id
        WHERE c.provider = ? AND c.external_id = ?
      ) AS candidates
    `)
    .bind(provider, id, provider, id)
    .all();

  const seen = new Set<string>();
  let candidates = candidateRows.results
    .map((row) => ({ reep_id: row.reep_id as string, type: row.type as string }))
    .filter((row) => {
      if (seen.has(row.reep_id)) return false;
      seen.add(row.reep_id);
      return true;
    });

  if (type) {
    candidates = candidates.filter((row) => row.type === type);
  }

  if (candidates.length === 0) return { kind: "not_found" };
  if (candidates.length > 1) {
    return {
      kind: "ambiguous",
      types: [...new Set(candidates.map((row) => row.type))].sort(),
    };
  }

  const entity = await lookupByReepId(db, candidates[0].reep_id);
  if (!entity) return { kind: "not_found" };
  // Provider-id paths silently redirect to the canonical (WIT-41). Retired
  // entities surface as not_found (the provider bridge has no successor).
  if (isRetirementSentinel(entity)) return { kind: "not_found" };
  return { kind: "found", entity: stripDeprecationMeta(entity) };
}

const BATCH_MAX = 100;

// Parse a comma-separated list of provider names from a query string or body field.
// Returns null when the input is empty/missing so callers can distinguish
// "no filter" (return everything) from "filter to empty set" (return nothing).
function parseTargets(raw: string | null | undefined): string[] | null {
  if (!raw) return null;
  const list = raw
    .split(",")
    .map((t) => t.trim())
    .filter((t) => t.length > 0 && VALID_PROVIDERS.has(t));
  return list.length > 0 ? list : null;
}

// Return a new external_ids map containing only keys that appear in `targets`.
function filterIds(ids: Record<string, string>, targets: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const t of targets) {
    if (ids[t] !== undefined) out[t] = ids[t];
  }
  return out;
}

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
        // Direct reep_id: redirect with deprecation meta, or surface
        // retirement as a gone-marker item in the results array (batch
        // endpoint returns a mixed list of entities + error stubs).
        const entity = await lookupByReepId(db, id);
        if (!entity) return [{ id, error: "not_found" }];
        if (isRetirementSentinel(entity)) {
          return [{ id, error: "gone", ...entity }];
        }
        return [entity];
      }
      // QID lookup — silent redirect (WIT-41)
      const reepIds = await db
        .prepare("SELECT reep_id FROM provider_ids WHERE provider = 'wikidata' AND external_id = ?")
        .bind(id)
        .all();
      if (reepIds.results.length === 0) return [{ qid: id, error: "not_found" }];
      const entities = await Promise.all(
        reepIds.results.map((r) => lookupByReepId(db, r.reep_id as string)),
      );
      const valid = (entities.filter(Boolean) as Record<string, unknown>[])
        .filter((e) => !isRetirementSentinel(e))
        .map(stripDeprecationMeta);
      return valid.length > 0 ? valid : [{ qid: id, error: "not_found" }];
    }),
  );
  const results = nested.flat();

  return json({ results, count: results.length });
}

// POST /batch/resolve — body: {
//   items: [{ provider: "transfermarkt", id: "568177", type: "player" }, ...],
//   targets?: ["wyscout", "fbref"]   // optional: filter external_ids in response
// }
// Each result echoes the input in `from` so callers can correlate without relying on array index.
async function handleBatchResolve(request: Request, db: D1Database): Promise<Response> {
  let body: { items?: { provider: string; id: string; type?: string }[]; targets?: unknown };
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

  // Optional targets: narrow the external_ids map in each result to just these providers.
  const targets = Array.isArray(body.targets)
    ? parseTargets((body.targets as unknown[]).filter((t): t is string => typeof t === "string").join(","))
    : null;

  const results = await Promise.all(
    items.map(async ({ provider, id, type }) => {
      const from = type ? { provider, id, type } : { provider, id };
      if (!provider || !id) return { from, error: "missing_fields" };
      if (!VALID_PROVIDERS.has(provider)) return { from, error: "unknown_provider" };
      if (type && !VALID_ENTITY_TYPES.has(type)) return { from, error: "unknown_type" };
      const resolved = await resolveEntity(db, provider, id, type);
      if (resolved.kind === "not_found") return { from, error: "not_found" };
      if (resolved.kind === "ambiguous") return { from, error: "ambiguous", types: resolved.types };
      if (!targets) return { from, ...resolved.entity };
      // Narrow external_ids and qid convenience alias
      const fullIds = (resolved.entity.external_ids as Record<string, string>) || {};
      const filteredIds = filterIds(fullIds, targets);
      const qid = targets.includes("wikidata") ? (resolved.entity.qid ?? null) : null;
      return { from, ...resolved.entity, qid, external_ids: filteredIds };
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

// Format the notification email body for a new R2 contribution upload.
// Parameters:
//   key   — the R2 object key (e.g. "2026-04-07T12-45-31-952Z_Sassuolo_Scouting_2025_26.xlsx")
//   size  — file size in bytes
//   action — the R2 action (e.g. "PutObject")
// Return an HTML string for the email body.
function formatEmailBody(key: string, size: number, _action: string): string {
  const parts = key.split("_");
  const timestamp = parts[0].replace(/T/, " ").replace(/-/g, (m, i) => (i > 9 ? ":" : m));
  const filename = parts.slice(1).join("_") || key;
  const sizeStr = size < 1024 ? `${size} B`
    : size < 1024 * 1024 ? `${(size / 1024).toFixed(1)} KB`
    : `${(size / (1024 * 1024)).toFixed(1)} MB`;

  return `<div style="font-family:system-ui,sans-serif;max-width:480px;padding:20px">
    <h2 style="margin:0 0 16px">New file uploaded to Reep</h2>
    <table style="border-collapse:collapse;width:100%">
      <tr><td style="padding:6px 12px;color:#666">File</td><td style="padding:6px 12px"><strong>${filename}</strong></td></tr>
      <tr><td style="padding:6px 12px;color:#666">Size</td><td style="padding:6px 12px">${sizeStr}</td></tr>
      <tr><td style="padding:6px 12px;color:#666">Time</td><td style="padding:6px 12px">${timestamp}</td></tr>
    </table>
  </div>`;
}
