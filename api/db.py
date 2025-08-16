# api/db.py
import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2 import pool
from dotenv import load_dotenv

# Load .env once at import
load_dotenv()

# -----------------------------
# TaxJar (optional / later)
# -----------------------------
TAXJAR_API_KEY = os.getenv("TAXJAR_API_KEY", "").strip()

def get_taxjar_client():
    """
    Returns a TaxJar client if TAXJAR_API_KEY is set and the library is installed.
    Otherwise returns None. You can use this later where needed.

        pip install taxjar
    """
    if not TAXJAR_API_KEY:
        return None
    try:
        import taxjar  # type: ignore
    except Exception:
        # Library not installed yet; that's fine for now.
        return None
    return taxjar.Client(api_key=TAXJAR_API_KEY)

# -----------------------------
# Postgres connection handling
# -----------------------------

def _ensure_ssl_param(url: str, sslmode: str) -> str:
    # Append or override sslmode in a DSN URL.
    # Render requires SSL for external connections.
    if "sslmode=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}sslmode={sslmode}"

def _build_dsn() -> str | None:
    """
    Build a DSN from env. Prefers DATABASE_URL / EXTERNAL_DATABASE_URL.
    Falls back to discrete POSTGRES_* / PG* variables.
    Returns None if not enough info to connect.
    """
    # 1) Single URL (preferred, e.g., Render External Database URL)
    dsn = (os.getenv("DATABASE_URL") or os.getenv("EXTERNAL_DATABASE_URL") or "").strip()
    if dsn:
        # If it's not localhost, require SSL by default
        sslmode = os.getenv("PGSSLMODE", "require")
        return _ensure_ssl_param(dsn, sslmode)

    # 2) Discrete variables
    host = os.getenv("POSTGRES_HOST") or os.getenv("PGHOST") or "localhost"
    port = os.getenv("POSTGRES_PORT") or os.getenv("PGPORT") or "5432"
    user = os.getenv("POSTGRES_USER") or os.getenv("PGUSER") or os.getenv("DB_USER")
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD") or os.getenv("DB_PASSWORD") or ""
    dbname = os.getenv("POSTGRES_DB") or os.getenv("PGDATABASE") or os.getenv("DB_NAME")

    if not (user and dbname):
        return None

    # Default SSL: disable for localhost, require otherwise (good for Render)
    default_ssl = "disable" if host in {"localhost", "127.0.0.1"} else "require"
    sslmode = os.getenv("PGSSLMODE", default_ssl)

    auth = f"{user}:{password}@" if password else f"{user}@"
    base = f"postgresql://{auth}{host}:{port}/{dbname}"
    return _ensure_ssl_param(base, sslmode)

# Global connection pool (created lazily at import so the app can boot even if DB is not ready)
pg_pool: pool.SimpleConnectionPool | None = None

if os.getenv("DISABLE_DB") == "1":
    print("[DB] Skipping DB init (DISABLE_DB=1)")
else:
    try:
        dsn = _build_dsn()
        if dsn:
            pg_pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=int(os.getenv("DB_MAX_CONNECTIONS", "10")),
                dsn=dsn,
            )
            print("[DB] Connection pool ready")
        else:
            print("[DB] No DB env found. Set DATABASE_URL or POSTGRES_* variables.")
    except Exception as e:
        print(f"[DB] Failed to create connection pool: {e}")
        pg_pool = None  # Keep app alive; queries will raise a clear error later.

@contextmanager
def conn():
    """Pooled DB connection context manager."""
    if not pg_pool:
        raise RuntimeError(
            "Database not configured. Set DATABASE_URL (preferred) or POSTGRES_* / PG* env vars."
        )
    connection = pg_pool.getconn()
    try:
        yield connection
    finally:
        pg_pool.putconn(connection)

# ---- Existing query helpers (unchanged API) ----

def get_last_hash(url: str):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT last_hash FROM seen_urls WHERE url=%s", (url,))
        r = cur.fetchone()
        return r[0] if r else None

def get_prev_doc_text(url: str):
    with conn() as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT d.id, d.normalized_text FROM documents d
            WHERE d.url=%s
            ORDER BY d.fetched_at DESC
            LIMIT 1
            """,
            (url,),
        )
        return cur.fetchone()

def touch_seen(url: str, h: str):
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
            INSERT INTO seen_urls(url, last_hash, last_fetched)
            VALUES (%s,%s,now())
            ON CONFLICT (url) DO UPDATE 
            SET last_hash=EXCLUDED.last_hash, last_fetched=now()
            """,
            (url, h),
        )

def insert_document(source_id, url, raw_uri, norm_text, content_hash, pdf_rev, mime):
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
            INSERT INTO documents(source_id,url,raw_uri,normalized_text,content_hash,pdf_revision,mime)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (source_id, url, raw_uri, norm_text, content_hash, pdf_rev, mime),
        )
        return cur.fetchone()[0]

def insert_snapshot(document_id, title, topic, score, effective_date, form_id):
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
            INSERT INTO snapshots(document_id,title,topic,score,effective_date,form_id)
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (document_id, title, topic, score, effective_date, form_id),
        )
        return cur.fetchone()[0]

def insert_diff(snapshot_id, prev_snapshot_id, diff_text):
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
            INSERT INTO diffs(snapshot_id, prev_snapshot_id, diff_text)
            VALUES (%s,%s,%s)
            """,
            (snapshot_id, prev_snapshot_id, diff_text),
        )
