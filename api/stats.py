from fastapi import APIRouter, Query
from typing import Optional
from api.db import conn
import psycopg2.extras

router = APIRouter()

def _dict_cur(c):
    import psycopg2.extras
    return c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

@router.get("/stats")
def stats(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
):
    where = []
    params = []
    if from_date:
        where.append("s.captured_at >= %s"); params.append(from_date + " 00:00:00")
    if to_date:
        where.append("s.captured_at <= %s"); params.append(to_date + " 23:59:59")
    clause = ("WHERE " + " AND ".join(where)) if where else ""

    with conn() as c, _dict_cur(c) as cur:
        cur.execute(f"""
          SELECT COALESCE(NULLIF(src.state,''), NULL) AS state, COUNT(*) AS n
          FROM snapshots s
          JOIN documents d ON d.id=s.document_id
          LEFT JOIN sources src ON src.id=d.source_id
          {clause}
          GROUP BY 1 ORDER BY n DESC NULLS LAST
        """, tuple(params))
        by_state = cur.fetchall()

        cur.execute(f"""
          SELECT s.topic, COUNT(*) AS n
          FROM snapshots s
          {clause}
          GROUP BY 1 ORDER BY n DESC NULLS LAST
        """, tuple(params))
        by_topic = cur.fetchall()

    return {"by_state": by_state, "by_topic": by_topic}
