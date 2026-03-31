-- Football entities from Wikidata with cross-provider external IDs
-- Primary key is (qid, type) to support dual-role people (e.g. Guardiola as both player and coach)

CREATE TABLE IF NOT EXISTS entities (
  qid TEXT NOT NULL,              -- Wikidata QID (e.g. Q99760796)
  type TEXT NOT NULL,             -- 'player', 'team', 'coach'
  name_en TEXT NOT NULL,          -- rdfs:label@en
  aliases_en TEXT,                -- skos:altLabel@en (comma-separated)
  name_native TEXT,               -- rdfs:label in native language
  full_name TEXT,                 -- P1477 birth name
  date_of_birth TEXT,             -- ISO date
  nationality TEXT,               -- country name
  position TEXT,                  -- GK/DF/MF/FW (players only)
  current_team_qid TEXT,          -- QID of current team
  height_cm REAL,                 -- height in cm
  country TEXT,                   -- for teams: country
  founded TEXT,                   -- for teams: founding date
  stadium TEXT,                   -- for teams: home ground
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (qid, type)
);

CREATE TABLE IF NOT EXISTS external_ids (
  qid TEXT NOT NULL,              -- FK to entities
  type TEXT NOT NULL,             -- entity type (player, team, coach)
  provider TEXT NOT NULL,         -- e.g. 'transfermarkt', 'fbref', 'sofascore'
  external_id TEXT NOT NULL,
  PRIMARY KEY (qid, type, provider),
  FOREIGN KEY (qid, type) REFERENCES entities(qid, type)
);

-- Indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name_en);
CREATE INDEX IF NOT EXISTS idx_entities_current_team ON entities(current_team_qid);
CREATE INDEX IF NOT EXISTS idx_external_ids_provider ON external_ids(provider, external_id);

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
