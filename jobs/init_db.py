"""
Initialize the PostgreSQL database with all required tables and indexes.
Run with: python -m jobs.init_db
"""

from api.db import conn  # <-- import from api.db, not api.server

def create_tables():
    with conn() as c:
        cur = c.cursor()

        # --------- Core tables ----------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            state TEXT,
            name  TEXT,
            url   TEXT,
            UNIQUE(state, name)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id SERIAL PRIMARY KEY,
            source_id      INTEGER REFERENCES sources(id) ON DELETE SET NULL,
            url            TEXT,
            raw_uri        TEXT,
            normalized_text TEXT,
            content_hash   TEXT,
            pdf_revision   TEXT,
            mime           TEXT,
            fetched_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id SERIAL PRIMARY KEY,
            document_id    INTEGER REFERENCES documents(id) ON DELETE CASCADE,
            topic          TEXT,
            title          TEXT,
            score          NUMERIC,
            effective_date DATE,
            form_id        TEXT,
            captured_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS diffs (
            snapshot_id       INTEGER PRIMARY KEY REFERENCES snapshots(id) ON DELETE CASCADE,
            prev_snapshot_id  INTEGER REFERENCES snapshots(id) ON DELETE SET NULL,
            diff_text         TEXT
        );
        """)

        # Tracks last seen hash per URL for the crawler
        cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_urls (
            url          TEXT PRIMARY KEY,
            last_hash    TEXT,
            last_fetched TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # --------- Indexes ----------
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sources_state ON sources(state);")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_source_id ON documents(source_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_documents_url       ON documents(url);")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_document_id    ON snapshots(document_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_topic          ON snapshots(topic);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_score          ON snapshots(score);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_effective_date ON snapshots(effective_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_form_id        ON snapshots(form_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_captured_at    ON snapshots(captured_at DESC);")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_diffs_prev_snapshot_id   ON diffs(prev_snapshot_id);")

        c.commit()

if __name__ == "__main__":
    print("Initializing database schema and indexes...")
    create_tables()
    print("Done. Tables and indexes created if they did not exist.")
