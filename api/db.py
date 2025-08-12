import os
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from contextlib import contextmanager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    user=os.getenv("POSTGRES_USER", "bulletin"),
    password=os.getenv("POSTGRES_PASSWORD", "bulletin"),
    dbname=os.getenv("POSTGRES_DB", "bulletin"),
)

# Create a global connection pool
try:
    pg_pool = pool.SimpleConnectionPool(
        minconn=1,
        maxconn=int(os.getenv("DB_MAX_CONNECTIONS", "10")),
        **PG
    )
    print(f"[DB] Connection pool created for {PG['user']}@{PG['host']}:{PG['port']}/{PG['dbname']}")
except Exception as e:
    print("[DB] Failed to create connection pool:", e)
    raise

@contextmanager
def conn():
    """Pooled DB connection context manager."""
    connection = None
    try:
        connection = pg_pool.getconn()
        yield connection
    finally:
        if connection:
            pg_pool.putconn(connection)

# ---- Existing functions (unchanged logic) ----

def get_last_hash(url: str):
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT last_hash FROM seen_urls WHERE url=%s", (url,))
        r = cur.fetchone()
        return r[0] if r else None

def get_prev_doc_text(url: str):
    with conn() as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT d.id, d.normalized_text FROM documents d
            WHERE d.url=%s
            ORDER BY d.fetched_at DESC
            LIMIT 1
        """, (url,))
        return cur.fetchone()

def touch_seen(url: str, h: str):
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO seen_urls(url, last_hash, last_fetched)
            VALUES (%s,%s,now())
            ON CONFLICT (url) DO UPDATE 
            SET last_hash=EXCLUDED.last_hash, last_fetched=now()
        """, (url, h))

def insert_document(source_id, url, raw_uri, norm_text, content_hash, pdf_rev, mime):
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO documents(source_id,url,raw_uri,normalized_text,content_hash,pdf_revision,mime)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (source_id, url, raw_uri, norm_text, content_hash, pdf_rev, mime))
        return cur.fetchone()[0]

def insert_snapshot(document_id, title, topic, score, effective_date, form_id):
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO snapshots(document_id,title,topic,score,effective_date,form_id)
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (document_id, title, topic, score, effective_date, form_id))
        return cur.fetchone()[0]

def insert_diff(snapshot_id, prev_snapshot_id, diff_text):
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO diffs(snapshot_id, prev_snapshot_id, diff_text)
            VALUES (%s,%s,%s)
        """, (snapshot_id, prev_snapshot_id, diff_text))

