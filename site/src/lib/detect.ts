export type InputType =
  | { kind: "name"; query: string }
  | { kind: "reep_id"; id: string }
  | { kind: "qid"; id: string }
  | { kind: "numeric"; id: string };

export function detectInput(raw: string): InputType {
  const trimmed = raw.trim();
  if (!trimmed) return { kind: "name", query: "" };

  // Reep ID: reep_p..., reep_t..., reep_l..., reep_s..., reep_c...
  if (/^reep_[ptlscm][0-9a-f]{7,8}$/i.test(trimmed)) {
    return { kind: "reep_id", id: trimmed.toLowerCase() };
  }

  // Wikidata QID: Q followed by digits
  if (/^Q\d+$/i.test(trimmed)) {
    return { kind: "qid", id: trimmed.toUpperCase() };
  }

  // Pure numeric: could be any provider's ID
  if (/^\d+$/.test(trimmed)) {
    return { kind: "numeric", id: trimmed };
  }

  // Default: treat as a name search
  return { kind: "name", query: trimmed };
}
