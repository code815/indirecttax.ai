# jobs/send_digests.py
# Skeleton: load saved_searches, run changes API query, email/Slack results.
import os, json, logging
from typing import Dict, Any, List
from api.db import conn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("send_digests")

MAIL_WEBHOOK = os.getenv("MAIL_WEBHOOK")  # or SMTP creds
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")

def run_saved_search(params: Dict[str, Any]) -> List[dict]:
    where = []; args = []
    if v := params.get("from_date"): where.append("s.captured_at >= %s"); args.append(v + " 00:00:00")
    if v := params.get("to_date"):   where.append("s.captured_at <= %s"); args.append(v + " 23:59:59")
    if v := params.get("states"):
        where.append("COALESCE(NULLIF(src.state,''), NULL) = ANY(%s)"); args.append(v)
    if v := params.get("topics"):
        where.append("s.topic = ANY(%s)"); args.append(v)
    if v := params.get("q"):
        like = f"%{v}%"; where.append("(s.title ILIKE %s OR d.normalized_text ILIKE %s OR s.form_id ILIKE %s)")
        args.extend([like, like, like])
    if (ms := params.get("min_score")) is not None:
        where.append("s.score >= %s"); args.append(ms)

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT s.id, COALESCE(NULLIF(src.state,''), NULL) AS state,
               s.topic, s.title, s.form_id, s.effective_date,
               s.score, s.captured_at, d.url AS source_url
        FROM snapshots s
        JOIN documents d ON d.id=s.document_id
        LEFT JOIN sources src ON src.id=d.source_id
        {clause}
        ORDER BY s.captured_at DESC
        LIMIT 500
    """
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, tuple(args))
        rows = cur.fetchall()
    out = []
    for r in rows:
        out.append(dict(id=r[0], state=r[1], topic=r[2], title=r[3], form_id=r[4],
                        effective_date=r[5], score=r[6], captured_at=r[7], source_url=r[8]))
    return out

def main():
    with conn() as c, c.cursor() as cur:
        cur.execute("SELECT id, name, params FROM saved_searches")
        searches = cur.fetchall()

    for id_, name, params in searches:
        results = run_saved_search(params)
        log.info("Saved search '%s' -> %d result(s)", name, len(results))
        # TODO: send via email/Slack using MAIL_WEBHOOK/SLACK_WEBHOOK
        # For now just print JSON so you can schedule with Task Scheduler / cron
        print(json.dumps({"name": name, "results": results[:10]}, default=str))

if __name__ == "__main__":
    main()
