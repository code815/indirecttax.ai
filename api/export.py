from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse
from typing import Optional, List
import io, csv

from api.db import conn
import psycopg2.extras

router = APIRouter()

def _dict_cur(c):
    return c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

@router.get("/export.csv")
def export_changes(
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    states: Optional[str] = Query(None, description="CSV of state codes, e.g. TX,CA"),
    topics: Optional[str] = Query(None, description="CSV of topics"),
    q: Optional[str] = Query(None, description="Search in title/body/form_id"),
    min_score: Optional[float] = Query(None),
    limit: int = Query(50000, ge=1, le=100000)
):
    where = []
    params = []
    if from_date:
        where.append("s.captured_at >= %s"); params.append(from_date + " 00:00:00")
    if to_date:
        where.append("s.captured_at <= %s"); params.append(to_date + " 23:59:59")
    if states:
        ss = [s.strip().upper() for s in states.split(",") if s.strip()]
        where.append("COALESCE(NULLIF(src.state,''), NULL) = ANY(%s)"); params.append(ss)
    if topics:
        ts = [t.strip() for t in topics.split(",") if t.strip()]
        where.append("s.topic = ANY(%s)"); params.append(ts)
    if q:
        like = f"%{q}%"
        where.append("(s.title ILIKE %s OR d.normalized_text ILIKE %s OR s.form_id ILIKE %s)")
        params.extend([like, like, like])
    if min_score is not None:
        where.append("s.score >= %s"); params.append(min_score)

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT s.id, COALESCE(NULLIF(src.state,''), NULL) AS state,
               s.topic, s.title, s.form_id, s.effective_date, s.score,
               s.captured_at, d.url AS source_url
        FROM snapshots s
        JOIN documents d ON d.id=s.document_id
        LEFT JOIN sources src ON src.id=d.source_id
        {clause}
        ORDER BY s.captured_at DESC
        LIMIT %s
    """

    with conn() as c, _dict_cur(c) as cur:
        cur.execute(sql, (*params, limit))
        rows = cur.fetchall()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["id","state","topic","title","form_id","effective_date","score","captured_at","source_url"])
    for r in rows:
        w.writerow([r["id"], r["state"], r["topic"], r["title"], r["form_id"],
                    r["effective_date"], r["score"], r["captured_at"], r["source_url"]])
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=changes_export.csv"})
