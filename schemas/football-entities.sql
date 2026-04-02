-- Football entity register with cross-provider ID mappings
-- Primary key is reep_id: a self-minted universal ID (reep_<type_prefix><8hex>)
-- Type prefix: p=player, t=team, c=coach (m=match, l=league, s=season reserved)

CREATE TABLE IF NOT EXISTS entities (
  reep_id TEXT PRIMARY KEY,        -- universal ID (e.g. reep_p2804f5db)
  type TEXT NOT NULL,              -- 'player', 'team', 'coach'
  name_en TEXT NOT NULL,           -- rdfs:label@en
  aliases_en TEXT,                 -- skos:altLabel@en (comma-separated)
  name_native TEXT,                -- rdfs:label in native language
  full_name TEXT,                  -- P1477 birth name
  date_of_birth TEXT,              -- ISO date
  nationality TEXT,                -- country name
  position TEXT,                   -- GK/DF/MF/FW (players only)
  current_team_reep_id TEXT,       -- reep_id of current team
  height_cm REAL,                  -- height in cm
  country TEXT,                    -- for teams: country
  founded TEXT,                    -- for teams: founding date
  stadium TEXT,                    -- for teams: home ground
  source TEXT NOT NULL DEFAULT 'wikidata', -- provenance: 'wikidata', 'opta', etc.
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

-- Wikidata-sourced provider ID mappings (dropped + recreated on weekly refresh)
CREATE TABLE IF NOT EXISTS provider_ids (
  reep_id TEXT NOT NULL,           -- FK to entities
  provider TEXT NOT NULL,          -- e.g. 'wikidata', 'transfermarkt', 'fbref'
  external_id TEXT NOT NULL,
  PRIMARY KEY (reep_id, provider, external_id)
);

-- Custom verified provider ID mappings (curated, never bulk-dropped)
CREATE TABLE IF NOT EXISTS custom_ids (
  reep_id TEXT NOT NULL,           -- FK to entities
  provider TEXT NOT NULL,          -- e.g. 'opta', 'fotmob', 'understat'
  external_id TEXT NOT NULL,
  source TEXT,                     -- how we know this mapping
  confidence REAL DEFAULT 1.0,     -- match confidence (0.0-1.0)
  added_at TEXT,
  PRIMARY KEY (reep_id, provider, external_id)
);

-- Indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name_en);
CREATE INDEX IF NOT EXISTS idx_entities_current_team ON entities(current_team_reep_id);
CREATE INDEX IF NOT EXISTS idx_provider_ids_lookup ON provider_ids(provider, external_id);

-- Full-text search on entity names (must use lowercase 'fts5' for D1)
CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(
  name_en,
  aliases_en,
  content='entities',
  content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2'
);

-- Keep FTS index in sync with entities table
CREATE TRIGGER IF NOT EXISTS entities_fts_ai AFTER INSERT ON entities BEGIN
  INSERT INTO entities_fts(rowid, name_en, aliases_en)
  VALUES (new.rowid, new.name_en, new.aliases_en);
END;

CREATE TRIGGER IF NOT EXISTS entities_fts_ad AFTER DELETE ON entities BEGIN
  INSERT INTO entities_fts(entities_fts, rowid, name_en, aliases_en)
  VALUES ('delete', old.rowid, old.name_en, old.aliases_en);
END;

CREATE TRIGGER IF NOT EXISTS entities_fts_au AFTER UPDATE ON entities BEGIN
  INSERT INTO entities_fts(entities_fts, rowid, name_en, aliases_en)
  VALUES ('delete', old.rowid, old.name_en, old.aliases_en);
  INSERT INTO entities_fts(rowid, name_en, aliases_en)
  VALUES (new.rowid, new.name_en, new.aliases_en);
END;
