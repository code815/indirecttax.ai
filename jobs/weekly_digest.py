@'
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Optional, Dict, List

import requests
import boto3
import psycopg2.extras

from dotenv import load_dotenv
from api.db import conn
from api.s3util import presign

load_dotenv()  # read EMAIL_* / POSTMARK_* / SES_* from .env

EMAIL_PROVIDER = (os.getenv("EMAIL_PROVIDER") or "ses").lower()
EMAIL_FROM = os.getenv("EMAIL_FROM", "alerts@example.com")

# ---------------------------- data access --------------------------------

def fetch_subscriptions() -> List[dict]:
    """Load all subscriptions. If none exist and DEV_EMAIL is set, return a default preview sub."""
    with conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, org_name, states, topics, email_to, min_weekly_score FROM subscriptions")
        subs = cur.fetchall()
    if not subs and os.getenv("DEV_EMAIL"):
        subs = [{
            "id": 0,
            "org_name": "Default",
            "states": [],
            "topics": [],
            "email_to": os.getenv("DEV_EMAIL"),
            "min_weekly_score": 0
        }]
    return subs

def fetch_last_7_days() -> List[dict]:
    since = datetime.now(timezone.utc) - timedelta(days=7)
    with conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT s.id, s.title, s.topic, s.score, s.effective_date, s.form_id, s.captured_at,
                   d.url AS source_url, d.raw_uri, d.mime,
                   COALESCE(src.state,'') AS state
            FROM snapshots s
            JOIN documents d ON d.id = s.document_id
            LEFT JOIN sources src ON src.id = d.source_id
            WHERE s.captured_at >= %s
            ORDER BY s.captured_at DESC
        """, (since,))
        return cur.fetchall()

def parse_raw_key(raw_uri: Optional[str]) -> Optional[str]:
    """Return the S3 key from an s3://bucket/key URI."""
    if not raw_uri or "://" not in raw_uri:
        return None
    try:
        _, rest = raw_uri.split("://", 1)
        _, key = rest.split("/", 1)
        return key
    except Exception:
        return None

# ---------------------------- grouping & html -----------------------------

def group_for_subscriber(rows: List[dict], sub: dict) -> Dict[str, Dict[str, List[dict]]]:
    """Group rows by State -> Topic with subscriber filters and min score applied."""
    S = set((sub.get("states") or []))
    T = set((sub.get("topics") or []))
    min_score = sub.get("min_weekly_score") or 0

    grouped: Dict[str, Dict[str, List[dict]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        # score filter
        if r["score"] is not None and float(r["score"]) < float(min_score):
            continue
        # state/topic filters
        if S and (r["state"] or "").upper() not in S:
            continue
        if T and (r["topic"] or "General") not in T:
            continue
        grouped[(r["state"] or "—").upper()][r["topic"] or "General"].append(r)
    return grouped

def render_html(sub: dict, grouped: Dict[str, Dict[str, List[dict]]]) -> str:
    org = sub["org_name"]
    today_iso = datetime.now(timezone.utc).date().isoformat()
    total = sum(len(v) for topics in grouped.values() for v in topics.values())

    def row(r: dict) -> str:
        key = parse_raw_key(r["raw_uri"])
        archived = presign(key) if key else r["source_url"]
        eff = r["effective_date"].isoformat() if r["effective_date"] else ""
        form = r["form_id"] or ""
        return f"""
          <tr>
            <td style="padding:6px 8px;border-bottom:1px solid #eee">{(r['state'] or '—')}</td>
            <td style="padding:6px 8px;border-bottom:1px solid #eee">{r['topic'] or 'General'}</td>
            <td style="padding:6px 8px;border-bottom:1px solid #eee"><a href="{r['source_url']}" target="_blank" rel="noopener">source</a></td>
            <td style="padding:6px 8px;border-bottom:1px solid #eee"><a href="{archived}" target="_blank" rel="noopener">{(r['title'] or '(untitled)')}</a></td>
            <td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right">{r['score'] or ''}</td>
            <td style="padding:6px 8px;border-bottom:1px solid #eee">{eff}</td>
            <td style="padding:6px 8px;border-bottom:1px solid #eee">{form}</td>
            <td style="padding:6px 8px;border-bottom:1px solid #eee">{r['captured_at'].isoformat()}</td>
          </tr>
        """

    sections = []
    for state in sorted(grouped.keys()):
        topics = grouped[state]
        sections.append(f"<h3 style='margin:18px 0 6px'>{state}</h3>")
        for topic in sorted(topics.keys()):
            sections.append(f"<div style='margin:6px 0 4px;font-weight:600;color:#334155'>{topic}</div>")
            sections.append("""
              <table style="width:100%;border-collapse:collapse;font:14px system-ui,Segoe UI,Arial">
                <thead>
                  <tr style="background:#f8fafc">
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee">State</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee">Topic</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee">Source</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee">Title (archived)</th>
                    <th style="text-align:right;padding:6px 8px;border-bottom:1px solid #eee">Score</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee">Effective</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee">Form</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid #eee">Captured</th>
                  </tr>
                </thead>
                <tbody>
            """)
            for r in topics[topic]:
                sections.append(row(r))
            sections.append("</tbody></table>")

    html = f"""
    <div style="font:14px system-ui,Segoe UI,Arial; color:#0f172a">
      <h2 style="margin:0 0 4px">Weekly Tax Bulletin Digest</h2>
      <div style="color:#64748b;margin:0 0 14px">{today_iso} · {org} · {total} item(s)</div>
      {''.join(sections) if sections else "<p>No items this week for your filters.</p>"}
      <hr style="margin:20px 0;border:0;border-top:1px solid #e2e8f0" />
      <div style="color:#94a3b8;font-size:12px">You are receiving this because you subscribed in the bulletin app.</div>
    </div>
    """
    return html

# ---------------------------- email senders -------------------------------

def send_postmark(to_email: str, subject: str, html: str) -> None:
    token = os.getenv("POSTMARK_TOKEN")
    if not token:
        raise RuntimeError("POSTMARK_TOKEN not set")
    r = requests.post(
        "https://api.postmarkapp.com/email",
        headers={"X-Postmark-Server-Token": token},
        json={"From": EMAIL_FROM, "To": to_email, "Subject": subject, "HtmlBody": html},
        timeout=30,
    )
    r.raise_for_status()

def send_ses(to_email: str, subject: str, html: str) -> None:
    region = os.getenv("SES_REGION", "us-east-1")
    key = os.getenv("SES_ACCESS_KEY")
    secret = os.getenv("SES_SECRET_KEY")
    ses = boto3.client(
        "ses",
        region_name=region,
        aws_access_key_id=key, aws_secret_access_key=secret
    )
    ses.send_email(
        Source=EMAIL_FROM,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Html": {"Data": html, "Charset": "UTF-8"}}
        },
    )

def deliver(to_email: str, subject: str, html: str) -> None:
    prov = EMAIL_PROVIDER
    if prov == "postmark":
        send_postmark(to_email, subject, html)
    else:
        send_ses(to_email, subject, html)

# ---------------------------- entry point ---------------------------------

def main():
    rows = fetch_last_7_days()
    subs = fetch_subscriptions()

    if not subs:
        print("No subscriptions found. Insert one into `subscriptions` or set DEV_EMAIL in .env")
        html = render_html({"org_name": "Preview"}, {"—": {"All": rows}})
        open("weekly_preview.html", "w", encoding="utf-8").write(html)
        print("Wrote weekly_preview.html")
        return

    today_iso = datetime.now(timezone.utc).date().isoformat()
    for sub in subs:
        grouped = group_for_subscriber(rows, sub)
        html = render_html(sub, grouped)
        subj = f"[Bulletins] Weekly Digest – {today_iso} – {sub['org_name']}"
        to = sub.get("email_to")
        if not to:
            print(f"[SKIP] {sub['org_name']}: no email_to set")
            continue
        try:
            deliver(to, subj, html)
            print(f"[SENT] {sub['org_name']} -> {to}")
        except Exception as e:
            fn = f"weekly_{sub['org_name'].replace(' ','_')}.html"
            open(fn, "w", encoding="utf-8").write(html)
            print(f"[ERROR] send failed for {sub['org_name']}: {e}. Saved {fn}")

if __name__ == "__main__":
    main()
'@ | Set-Content .\jobs\weekly_digest.py -Encoding UTF8
