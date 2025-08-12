-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Title & form partial matches
CREATE INDEX IF NOT EXISTS idx_snapshots_title_trgm ON snapshots USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_snapshots_form_trgm  ON snapshots USING gin (form_id gin_trgm_ops);

-- FTS over normalized_text
CREATE MATERIALIZED VIEW IF NOT EXISTS documents_fts AS
SELECT d.id,
       to_tsvector('simple', unaccent(coalesce(d.normalized_text,''))) AS tsv
FROM documents d;

CREATE INDEX IF NOT EXISTS idx_documents_fts ON documents_fts USING gin(tsv);

-- Saved searches
CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email text UNIQUE NOT NULL,
  api_key text UNIQUE
);

CREATE TABLE IF NOT EXISTS saved_searches (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid REFERENCES users(id),
  name text NOT NULL,
  params jsonb NOT NULL,
  created_at timestamp DEFAULT now()
);
