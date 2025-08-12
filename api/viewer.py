from html import escape
from datetime import datetime
from typing import Optional, List, Tuple, Dict
from fastapi import Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from urllib.parse import urlparse, urlencode
import time

from api.server import app, conn, _dict_cur  # reuse same app & DB

TABLE_CSS = """
  :root { --ink:#0f172a; --muted:#64748b; --row:#f8fafc; --accent:#0ea5e9; }
  * { box-sizing:border-box }
  body{font-family:system-ui,Segoe UI,Arial,sans-serif;max-width:1150px;margin:32px auto;padding:0 12px;color:var(--ink)}
  h2{margin:0 0 16px}
  table{width:100%;border-collapse:collapse;margin-top:12px}
  th,td{padding:10px 12px;border-bottom:1px solid #eee;font-size:14px;vertical-align:top}
  th{position:sticky;top:0;background:#fafafa;text-align:left;z-index:1}
  tr:hover{background:var(--row)}
  .chip{display:inline-block;padding:2px 8px;border-radius:999px;background:#eef2ff;font-weight:600;font-size:12px}
  .score{font-variant-numeric:tabular-nums}
  .controls{display:flex;gap:12px;row-gap:8px;flex-wrap:wrap;margin-bottom:8px;align-items:end}
  input,select{padding:8px 10px;border:1px solid #e2e8f0;border-radius:10px;min-width:180px}
  label{font-size:12px;color:#475569;display:block;margin-bottom:4px}
  .btn{padding:10px 14px;border:1px solid var(--accent);background:var(--accent);color:#fff;border-radius:10px;cursor:pointer}
  .link{color:var(--accent);text-decoration:underline}
  .meta{color:var(--muted);font-size:12px}
  .chips { display:flex; gap:8px; flex-wrap:wrap; min-width:260px }
  .chips label { font-size:13px; background:#f1f5f9; padding:6px 10px; border-radius:999px; cursor:pointer; border:1px solid #e2e8f0 }
  .chips input { margin-right:6px }
"""

# ---------- helpers ----------

def _with_security_headers(resp: HTMLResponse | PlainTextResponse):
    resp.headers["Content-Security-Policy"] = "default-src 'self'; frame-ancestors 'none'; base-uri 'none'"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp

def infer_state_from_url(u: str) -> Optional[str]:
    try:
        host = urlparse(u).netloc.lower()
    except Exception:
        return None
    mapping = {
        "comptroller.texas.gov": "TX",
        "cdtfa.ca.gov": "CA",
        # extend as needed
    }
    for k, v in mapping.items():
        if host.endswith(k):
            return v
    return None

def _parse_date(d: Optional[str]) -> Optional[str]:
    if not d:
        return None
    try:
        datetime.strptime(d, "%Y-%m-%d")
        return d
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid date format: {d}. Use YYYY-MM-DD.")

# simple in-process cache for options (60s)
_OPTIONS_CACHE: Tuple[float, List[str], List[str]] = (0.0, [], [])

def _get_options():
    global _OPTIONS_CACHE  # must be first line in the function
    now = time.time()
    ts, cached_states, cached_topics = _OPTIONS_CACHE
    if now - ts < 60 and cached_states and cached_topics:
        return cached_states, cached_topics

    with conn() as c, _dict_cur(c) as cur:
        # states from sources plus inference from document URLs
        cur.execute("""
            SELECT DISTINCT state FROM sources WHERE state IS NOT NULL AND state<>''
            UNION
            SELECT DISTINCT inferred_state AS state FROM (
                SELECT url,
                       CASE
                           WHEN url LIKE '%.texas.gov%' THEN 'TX'
                           WHEN url LIKE '%.ca.gov%' THEN 'CA'
                       END AS inferred_state
                FROM documents
                WHERE url IS NOT NULL
            ) AS d
            WHERE inferred_state IS NOT NULL
            ORDER BY 1
        """)
        states = [r["state"] for r in cur.fetchall()]

        cur.execute("SELECT DISTINCT topic FROM snapshots WHERE topic IS NOT NULL ORDER BY 1")
        topics = [r["topic"] for r in cur.fetchall()]

    _OPTIONS_CACHE = (now, states, topics)
    return states, topics

def _getlist(qp, key: str) -> List[str]:
    return [v for v in qp.getlist(key) if v]

# sorting whitelist
_SORT_MAP = {
    "captured_at": "s.captured_at",
    "score": "s.score",
    "effective_date": "s.effective_date",
    "title": "s.title",
}
_SORT_DIRECTIONS = {"asc", "desc"}

def _build_sort(sort: Optional[str], direction: Optional[str]) -> str:
    col = _SORT_MAP.get((sort or "").lower(), "s.captured_at")
    dir_sql = (direction or "").lower()
    if dir_sql not in _SORT_DIRECTIONS:
        dir_sql = "desc"
    return f"{col} {dir_sql}, s.id {dir_sql}"

def _query_changes(
    from_date: Optional[str],
    to_date: Optional[str],
    states: List[str],
    topics: List[str],
    q: Optional[str],
    min_score: Optional[float],
    page: int,
    page_size: int,
    order_by: str
) -> Tuple[List[Dict], int, int, int]:
    where, params = [], []

    if from_date:
        where.append("s.captured_at >= %s")
        params.append(from_date)
    if to_date:
        where.append("s.captured_at < (%s::date + INTERVAL '1 day')")
        params.append(to_date)

    if states:
        where.append("COALESCE(NULLIF(src.state,''), NULL) = ANY(%s)")
        params.append([s.upper() for s in states])

    if topics:
        where.append("s.topic = ANY(%s)")
        params.append(topics)

    if q:
        like = f"%{q}%"
        where.append("(s.title ILIKE %s OR d.normalized_text ILIKE %s OR s.form_id ILIKE %s)")
        params.extend([like, like, like])

    if min_score is not None:
        where.append("s.score >= %s")
        params.append(min_score)

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    offset = (page - 1) * page_size

    with conn() as c, _dict_cur(c) as cur:
        main_query = f"""
            SELECT s.id, COALESCE(NULLIF(src.state,''), NULL) AS state, s.topic, s.title,
                   s.score, s.effective_date, s.form_id,
                   d.url AS source_url, s.captured_at
            FROM snapshots s
            JOIN documents d ON d.id=s.document_id
            LEFT JOIN sources src ON src.id=d.source_id
            {clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        cur.execute(main_query, (*params, page_size, offset))
        items = cur.fetchall()

        count_query = f"""
            SELECT COUNT(*) AS n
            FROM snapshots s
            JOIN documents d ON d.id=s.document_id
            LEFT JOIN sources src ON src.id=d.source_id
            {clause}
        """
        cur.execute(count_query, tuple(params))
        total = cur.fetchone()["n"]

    for it in items:
        if not it["state"]:
            it["state"] = infer_state_from_url(it["source_url"]) or "—"
    return items, total, page, page_size

def _qs_with(qp, **overrides) -> str:
    args = {k: v for k, v in qp.multi_items()}
    args.update({k: v for k, v in overrides.items() if v is not None})
    # remove empties
    args = {k: v for k, v in args.items() if v not in (None, "", [])}
    return urlencode(args, doseq=True)

def _parse_params(qp: Dict) -> Tuple:
    """Parses and validates all query parameters."""
    from_date = _parse_date(qp.get("from"))
    to_date = _parse_date(qp.get("to"))

    states = [s.strip().upper() for s in _getlist(qp, "states")]
    topics = [t.strip() for t in _getlist(qp, "topics")]
    q = qp.get("q")

    min_score_raw = qp.get("min_score")
    try:
        min_score = float(min_score_raw) if min_score_raw not in (None, "") else None
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid min_score: {min_score_raw}")

    try:
        page = max(1, int(qp.get("page", "1")))
        page_size = max(1, min(100, int(qp.get("page_size", "25"))))
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid page or page_size. Must be integers.")

    sort = qp.get("sort", "captured_at")
    direction = qp.get("dir", "desc")
    order_by = _build_sort(sort, direction)

    return from_date, to_date, states, topics, q, min_score, page, page_size, order_by

# ---------- routes ----------

@app.get("/ui", response_class=HTMLResponse)
def ui(request: Request):
    qp = request.query_params
    from_date, to_date, states, topics, q, min_score, page, page_size, order_by = _parse_params(qp)

    items, total, page, page_size = _query_changes(
        from_date, to_date, states, topics, q, min_score, page, page_size, order_by
    )
    all_states, all_topics = _get_options()

    # pagination meta
    start_i = (page - 1) * page_size + 1 if total else 0
    end_i = min(page * page_size, total)
    total_pages = (total + page_size - 1) // page_size if page_size else 1

    def pill_group(name: str, values: List[str], selected: List[str]):
        s = set(selected)
        return "".join(
            f'<label><input type="checkbox" name="{name}" value="{escape(v)}" {"checked" if v in s else ""}> {escape(v)}</label>'
            for v in values
        )

    rows = []
    for it in items:
        rows.append(f"""
        <tr>
          <td>{escape(str(it["id"]))}</td>
          <td><span class="chip">{escape(it["state"] or "—")}</span></td>
          <td>{escape(it["topic"] or "General")}</td>
          <td><a class="link" href="/_diff/{it['id']}" target="_blank" rel="noopener">{escape(it["title"] or "(untitled)")}</a></td>
          <td class="score">{escape(str(it["score"] or ""))}</td>
          <td>{escape(it["effective_date"].isoformat() if it["effective_date"] else "")}</td>
          <td>{escape(it["form_id"] or "")}</td>
          <td><a class="link" href="{escape(it['source_url'])}" target="_blank" rel="noopener">source</a></td>
          <td class="meta">{escape(it["captured_at"].isoformat())}</td>
        </tr>""")

    # sort controls (reflect current selection)
    sort = qp.get("sort", "captured_at")
    direction = qp.get("dir", "desc")
    sort_select = f"""
      <div>
        <label>Sort</label>
        <select name="sort">
          {"".join(f'<option value="{escape(k)}" {"selected" if sort==k else ""}>{escape(k)}</option>' for k in _SORT_MAP.keys())}
        </select>
      </div>
      <div>
        <label>Direction</label>
        <select name="dir">
          <option value="desc" {"selected" if direction.lower()=="desc" else ""}>desc</option>
          <option value="asc" {"selected" if direction.lower()=="asc" else ""}>asc</option>
        </select>
      </div>
    """

    # Safely escape the query string for links
    safe_qp = escape(qp.__str__())

    form_html = f"""
      <form method="get" class="controls">
        <div style="min-width:260px">
          <label>States</label>
          <div class="chips">{pill_group("states", all_states, states)}</div>
        </div>
        <div style="min-width:260px">
          <label>Topics</label>
          <div class="chips">{pill_group("topics", all_topics, topics)}</div>
        </div>
        <div><label>From</label><input type="date" name="from" value="{escape(from_date or "")}"></div>
        <div><label>To</label><input type="date" name="to" value="{escape(to_date or "")}"></div>
        <div><label>Min score</label><input type="number" step="0.1" name="min_score" value="{escape("" if min_score is None else str(min_score))}"></div>
        <div><label>Search</label><input type="text" name="q" value="{escape(q or "")}" placeholder="form id, keyword..."></div>
        {sort_select}
        <div><button class="btn" type="submit">Apply</button></div>
        <div><a class="link" href="/ui">Reset</a></div>
        <div><a class="link" href="/changes?{safe_qp}">JSON API</a></div>
        <div><a class="link" href="/ui/export.csv?{safe_qp}" target="_blank" rel="noopener">Export CSV</a></div>
      </form>
      <div class="meta">Showing {start_i}-{end_i} of {total} result(s). Page {page} of {total_pages}.</div>
    """

    # pagination links
    prev_qs = _qs_with(qp, page=str(max(1, page-1)))
    next_qs = _qs_with(qp, page=str(min(total_pages or 1, page+1)))
    pager_html = f"""
      <div class="controls" style="justify-content:flex-end">
        <a class="link" href="/ui?{escape(prev_qs)}">« Prev</a>
        <span class="meta">Page {page}/{total_pages or 1}</span>
        <a class="link" href="/ui?{escape(next_qs)}">Next »</a>
      </div>
    """

    html = f"""
    <html><head><title>Changes</title><style>{TABLE_CSS}</style></head>
    <body>
      <h2>Recent Changes</h2>
      {form_html}
      <table>
        <thead>
          <tr><th>ID</th><th>State</th><th>Topic</th><th>Title</th>
              <th>Score</th><th>Effective</th><th>Form</th><th>Source</th><th>Captured</th></tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
      {pager_html}
    </body></html>
    """
    return _with_security_headers(HTMLResponse(html))

# CSV export (same filters + sorting; no pagination)
@app.get("/ui/export.csv")
def export_csv(request: Request):
    qp = request.query_params
    from_date, to_date, states, topics, q, min_score, _, _, order_by = _parse_params(qp)

    items, total, _, _ = _query_changes(
        from_date, to_date, states, topics, q, min_score,
        page=1, page_size=100000, order_by=order_by
    )

    cols = ["id","state","topic","title","score","effective_date","form_id","source_url","captured_at"]
    lines = [",".join(cols)]

    def csv_escape(v):
        if v is None:
            return ""
        s = str(v)
        if any(ch in s for ch in [",","\"","\n","\r"]):
            s = "\"" + s.replace("\"","\"\"") + "\""
        return s

    for it in items:
        row = [
            it["id"],
            it.get("state"),
            it.get("topic"),
            it.get("title"),
            it.get("score"),
            it.get("effective_date").isoformat() if it.get("effective_date") else "",
            it.get("form_id"),
            it.get("source_url"),
            it.get("captured_at").isoformat() if it.get("captured_at") else "",
        ]
        lines.append(",".join(csv_escape(v) for v in row))

    csv_body = "\n".join(lines)
    return _with_security_headers(PlainTextResponse(
        content=csv_body,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=changes.csv"}
    ))

# Diff page
DIFF_CSS = """
  body{font-family:ui-monospace,Consolas,monospace;max-width:1100px;margin:32px auto;padding:0 12px}
  .hdr{font-family:system-ui,Segoe UI,Arial,sans-serif;margin-bottom:12px}
  .line{white-space:pre-wrap;border-left:4px solid transparent;padding-left:8px;margin:2px 0}
  .add{color:#166534;background:#ecfdf5;border-color:#16a34a}
  .del{color:#7f1d1d;background:#fef2f2;border-color:#dc2626}
  .ctx{color:#334155;background:#f8fafc}
"""
@app.get("/_diff/{snapshot_id}", response_class=HTMLResponse)
def view_diff(snapshot_id: int):
    with conn() as c, _dict_cur(c) as cur:
        cur.execute("""
          SELECT s.id, s.title, s.topic, d.url AS source_url
          FROM snapshots s JOIN documents d ON d.id=s.document_id
          WHERE s.id=%s
        """, (snapshot_id,))
        meta = cur.fetchone()
        cur.execute("SELECT diff_text, prev_snapshot_id FROM diffs WHERE snapshot_id=%s", (snapshot_id,))
        diff = cur.fetchone()
    if not meta:
        return _with_security_headers(HTMLResponse("<h3 style='font-family:system-ui'>Not found</h3>", status_code=404))
    lines = (diff["diff_text"].splitlines() if diff and diff["diff_text"] else [])
    if not lines:
        lines = ["(no diff available yet — first capture or no textual change)"]
    rendered = []
    for ln in lines:
        cls = "add" if ln.startswith("+") else "del" if ln.startswith("-") else "ctx"
        rendered.append(f'<div class="line {cls}">{escape(ln)}</div>')
    html = f"""
    <html><head><title>Diff {snapshot_id}</title><style>{DIFF_CSS}</style></head>
    <body>
      <div class="hdr">
        <h2 style="margin:0;">{escape(meta["title"] or "Change")} <small>#{snapshot_id}</small></h2>
        <div>Topic: <b>{escape(meta["topic"] or "General")}</b> ·
          <a href="{escape(meta["source_url"])}" target="_blank" rel="noopener">Open source</a> ·
          <a href="/changes/{snapshot_id}" target="_blank" rel="noopener">JSON</a>
        </div>
      </div>
      {''.join(rendered)}
    </body></html>
    """
    return _with_security_headers(HTMLResponse(html))
