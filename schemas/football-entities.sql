-- Football entities from Wikidata with cross-provider external IDs

CREATE TABLE IF NOT EXISTS entities (
  qid TEXT PRIMARY KEY,          -- Wikidata QID (e.g. Q99760796)
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
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS external_ids (
  qid TEXT NOT NULL,              -- FK to entities
  provider TEXT NOT NULL,         -- e.g. 'transfermarkt', 'fbref', 'sofascore'
  external_id TEXT NOT NULL,
  PRIMARY KEY (qid, provider),
  FOREIGN KEY (qid) REFERENCES entities(qid)
);

-- Indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name_en);
CREATE INDEX IF NOT EXISTS idx_entities_current_team ON entities(current_team_qid);
CREATE INDEX IF NOT EXISTS idx_external_ids_provider ON external_ids(provider, external_id);
