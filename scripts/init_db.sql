CREATE TABLE IF NOT EXISTS sources(
  id SERIAL PRIMARY KEY,
  state TEXT NOT NULL,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  allow_re TEXT NOT NULL,
  type TEXT NOT NULL,
  feed_url TEXT,
  sitemap_url TEXT,
  active BOOL DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS documents(
  id BIGSERIAL PRIMARY KEY,
  source_id INT REFERENCES sources(id),
  url TEXT NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw_uri TEXT NOT NULL,
  normalized_text TEXT,
  content_hash TEXT NOT NULL,
  pdf_revision TEXT,
  mime TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots(
  id BIGSERIAL PRIMARY KEY,
  document_id BIGINT REFERENCES documents(id),
  captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  title TEXT,
  topic TEXT,
  score NUMERIC,
  effective_date DATE,
  form_id TEXT
);
CREATE TABLE IF NOT EXISTS diffs(
  id BIGSERIAL PRIMARY KEY,
  snapshot_id BIGINT REFERENCES snapshots(id),
  prev_snapshot_id BIGINT,
  diff_text TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS seen_urls(
  id BIGSERIAL PRIMARY KEY,
  source_id INT REFERENCES sources(id),
  url TEXT UNIQUE,
  first_seen TIMESTAMPTZ DEFAULT now(),
  last_fetched TIMESTAMPTZ,
  last_hash TEXT
);
CREATE TABLE IF NOT EXISTS subscriptions(
  id BIGSERIAL PRIMARY KEY,
  org_name TEXT NOT NULL,
  states TEXT[] DEFAULT '{}',
  topics TEXT[] DEFAULT '{Rates,Forms,Exemptions,Freight,Marketplace,Deadlines}',
  digest_day INT DEFAULT 5,
  alert_topics TEXT[] DEFAULT '{Rates,Forms}',
  min_instant_score INT DEFAULT 6,
  min_weekly_score INT DEFAULT 3,
  email_to TEXT NOT NULL
);
