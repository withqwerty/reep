/**
 * no-op polyfills for non-node environments. tries to load the actual
 * diagnostics_channel module on platforms (bun, deno) that support it, but
 * fails gracefully if not found. This means that the first tick of metrics
 * and tracing will be missed, but that probably doesn't matter much.
 */
// conditionally import from diagnostic_channel, fall back to dummyfill
// all we actually have to mock is the hasSubscribers, since we alwasy check
/* v8 ignore next */
const dummy = { hasSubscribers: false };
export const [metrics, tracing] = await import('node:diagnostics_channel')
    .then(dc => [
    dc.channel('lru-cache:metrics'),
    dc.tracingChannel('lru-cache'),
])
    .catch(() => [dummy, dummy]);
//# sourceMappingURL=diagnostics-channel-esm.mjs.map