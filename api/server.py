from datetime import datetime, timedelta
from typing import Optional, List
import os
import logging

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2.extras

from api.db import conn

# ------------------------------------------------------------------------------
# App
# ------------------------------------------------------------------------------
app = FastAPI(title="Bulletin API", version="0.2")

# ------------------------------------------------------------------------------
# CORS (env-driven)
#   - Set CORS_ALLOW_ORIGINS as comma-separated list for prod, e.g.:
#       CORS_ALLOW_ORIGINS=https://indirecttax.ai,https://www.indirecttax.ai
#   - If "*", credentials are automatically disabled to comply with spec.
# ------------------------------------------------------------------------------
_default_origins = "https://indirecttax.ai,https://www.indirecttax.ai,http://localhost:5173,http://localhost:3000"
_origins_env = os.getenv("CORS_ALLOW_ORIGINS", _default_origins)
ALLOWED_ORIGINS: List[str] = [o.strip() for o in _origins_env.split(",") if o.strip()]
_is_wildcard = len(ALLOWED_ORIGINS) == 1 and ALLOWED_ORIGINS[0] == "*"

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if not _is_wildcard else ["*"],
    allow_credentials=not _is_wildcard,  # browsers disallow creds with "*"
    allow_methods=["GET"],               # read-only API
    allow_headers=["*"],
)

# ------------------------------------------------------------------------------
# Security headers (lightweight)
# ------------------------------------------------------------------------------
@app.middleware("http")
async def _security_headers(request, call_next):
    resp = await call_next(request)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "no-referrer-when-downgrade"
    return resp

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _dict_cur(c):
    return c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def _parse_date(d: str) -> datetime:
    """Expect YYYY-MM-DD; raise 422 if bad format."""
    try:
        return datetime.strptime(d, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid date format: {d}. Use YYYY-MM-DD.")

def _csv_upper(s: Optional[str]):
    if not s:
        return None
    vals = [v.strip().upper() for v in s.split(",") if v.strip()]
    return vals or None

def _csv_clean(s: Optional[str]):
    if not s:
        return None
    vals = [v.strip() for v in s.split(",") if v.strip()]
    return vals or None

# ------------------------------------------------------------------------------
# Health endpoints
# ------------------------------------------------------------------------------
@app.get("/livez")
def livez():
    """Simple liveness probe — does the app start/respond?"""
    return {"ok": True}

@app.get("/healthz")
def healthz():
    """Readiness probe — DB connectivity without leaking internals."""
    try:
        with conn() as c, c.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return {"ok": True}
    except Exception:
        # Avoid leaking exception text; log server-side if needed
        logging.exception("DB health check failed")
        raise HTTPException(status_code=500, detail="DB check failed")

# ------------------------------------------------------------------------------
# API
# ------------------------------------------------------------------------------
@app.get("/changes")
def list_changes(
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD (inclusive)"),
    to_date: Optional[str] = Query(None, description="YYYY-MM-DD (inclusive)"),
    states: Optional[str] = Query(None, description="CSV of state codes, e.g. TX,CA"),
    topics: Optional[str] = Query(None, description="CSV, e.g. Rates,Forms"),
    q: Optional[str] = Query(None, description="simple text search in title/text/form_id"),
    min_score: Optional[float] = Query(None, ge=0, description="minimum score"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
):
    where = []
    params = []

    if from_date:
        fd = _parse_date(from_date)  # inclusive lower bound
        where.append("s.captured_at >= %s")
        params.append(fd)

    if to_date:
        # Exclusive upper bound: next day at 00:00 avoids time concat/suffix
        td = _parse_date(to_date) + timedelta(days=1)
        where.append("s.captured_at < %s")
        params.append(td)

    ss = _csv_upper(states)
    if ss:
        # Note: rows with NULL state won't match any filter (by design)
        where.append("src.state = ANY(%s)")
        params.append(ss)

    ts = _csv_clean(topics)
    if ts:
        where.append("s.topic = ANY(%s)")
        params.append(ts)

    if q:
        where.append("(s.title ILIKE %s OR d.normalized_text ILIKE %s OR s.form_id ILIKE %s)")
        like = f"%{q}%"
        params.extend([like, like, like])

    if min_score is not None:
        where.append("s.score >= %s")
        params.append(min_score)

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    offset = (page - 1) * page_size

    with conn() as c, _dict_cur(c) as cur:
        # data page
        cur.execute(f"""
            SELECT
                s.id,
                src.state,
                s.topic,
                s.title,
                d.url AS source_url,
                s.captured_at,
                s.score,
                s.effective_date,
                s.form_id
            FROM snapshots s
            JOIN documents d ON d.id = s.document_id
            LEFT JOIN sources src ON src.id = d.source_id
            {clause}
            ORDER BY s.captured_at DESC
            LIMIT %s OFFSET %s
        """, (*params, page_size, offset))
        items = cur.fetchall()

        # total count
        cur.execute(f"""
            SELECT COUNT(*) AS n
            FROM snapshots s
            JOIN documents d ON d.id = s.document_id
            LEFT JOIN sources src ON src.id = d.source_id
            {clause}
        """, tuple(params))
        total = cur.fetchone()["n"]

    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size else 1,
    }

@app.get("/changes/{snapshot_id}")
def get_change(snapshot_id: int):
    with conn() as c, _dict_cur(c) as cur:
        cur.execute("""
            SELECT
                s.id, s.title, s.topic, s.score, s.effective_date, s.form_id,
                s.captured_at, d.url AS source_url, d.raw_uri, d.mime,
                d.normalized_text
            FROM snapshots s
            JOIN documents d ON d.id = s.document_id
            WHERE s.id = %s
        """, (snapshot_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Not found")

        cur.execute("""
            SELECT diff_text, prev_snapshot_id
            FROM diffs
            WHERE snapshot_id = %s
        """, (snapshot_id,))
        diff = cur.fetchone()

    return {
        "id": row["id"],
        "title": row["title"],
        "topic": row["topic"],
        "score": float(row["score"]) if row["score"] is not None else None,
        "effective_date": row["effective_date"],
        "form_id": row["form_id"],
        "captured_at": row["captured_at"],
        "source_url": row["source_url"],
        "raw_uri": row["raw_uri"],
        "mime": row["mime"],
        "diff_excerpt": (diff["diff_text"] if diff else None),
        "prev_snapshot_id": (diff["prev_snapshot_id"] if diff else None),
        "normalized_text": row["normalized_text"],
    }

# Register UI routes (kept as-is)
import api.viewer  # noqa: F401
