#!/usr/bin/env python3
"""
Fetch a URL, normalize its text, and store:
- sources (upsert)
- documents (new row per fetch)
- snapshots (linked to the new document)
- diffs (against the previous snapshot for the same URL, if any)
- seen_urls (hash of raw bytes to skip duplicates)

Run:
  python -m jobs.fetch_url --url "https://comptroller.texas.gov/taxes/sales/" \
    --state TX --source "Texas Comptroller" --topic Rates \
    --title "TX Sales Tax Page" --form-id "TX-TEST" --effective 2025-01-01
"""
import argparse
import hashlib
import os
from datetime import datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import difflib
import psycopg2

from api.db import (
    conn,
    get_last_hash,
    get_prev_doc_text,
    touch_seen,
    insert_document,
    insert_snapshot,
    insert_diff,
)

UA = os.getenv("CRAWLER_USER_AGENT", "bulletin-fetch/0.1")
TIMEOUT = 40


def norm_text_from_html(html: bytes) -> str:
    soup = BeautifulSoup(html, "lxml")
    # remove script/style
    for bad in soup(["script", "style", "noscript"]):
        bad.decompose()
    txt = soup.get_text(separator="\n")
    # squeeze whitespace
    lines = [ln.strip() for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines)


def ensure_source(state: str | None, name: str | None, homepage_from_url: str | None) -> int | None:
    """
    Return a source_id (or None if not provided). Upserts by (state,name).
    """
    if not (state and name):
        return None
    home = homepage_from_url or ""
    with conn() as c, c.cursor() as cur:
        # try find
        cur.execute(
            "SELECT id FROM sources WHERE state=%s AND name=%s LIMIT 1",
            (state, name),
        )
        r = cur.fetchone()
        if r:
            return r[0]
        # insert
        cur.execute(
            "INSERT INTO sources(state,name,url) VALUES (%s,%s,%s) RETURNING id",
            (state, name, home),
        )
        sid = cur.fetchone()[0]
        c.commit()
        return sid


def latest_snapshot_id_for_url(url: str) -> int | None:
    """
    Find the latest snapshot id for the given document URL (if any).
    """
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT s.id
            FROM snapshots s
            JOIN documents d ON d.id = s.document_id
            WHERE d.url = %s
            ORDER BY s.captured_at DESC, s.id DESC
            LIMIT 1
            """,
            (url,),
        )
        r = cur.fetchone()
        return r[0] if r else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--state", help="e.g. TX")
    ap.add_argument("--source", help="e.g. Texas Comptroller")
    ap.add_argument("--topic", default="General")
    ap.add_argument("--title", default="")
    ap.add_argument("--form-id", dest="form_id")
    ap.add_argument("--effective", help="YYYY-MM-DD")
    args = ap.parse_args()

    url = args.url
    eff_date = None
    if args.effective:
        eff_date = datetime.strptime(args.effective, "%Y-%m-%d").date()

    # fetch
    resp = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT, verify=os.getenv("CRAWLER_VERIFY_TLS", "true").lower() == "true")
    resp.raise_for_status()
    raw_bytes = resp.content
    mime = (resp.headers.get("Content-Type") or "text/html").split(";")[0].strip()

    # dedupe by content hash
    h = hashlib.sha256(raw_bytes).hexdigest()
    last = get_last_hash(url)
    if last and last == h:
        print("[fetch_url] No change (hash match). Skipping insert.")
        return

    # normalize
    norm_text = norm_text_from_html(raw_bytes)
    if not args.title:
        # try to guess from <title>
        try:
            title_guess = BeautifulSoup(raw_bytes, "lxml").title
            args.title = (title_guess.text.strip() if title_guess else "") or url
        except Exception:
            args.title = url

    # upsert source
    host = urlparse(url).scheme + "://" + urlparse(url).netloc
    source_id = ensure_source(args.state, args.source, host)

    # insert document
    doc_id = insert_document(
        source_id=source_id,
        url=url,
        raw_uri=url,             # keep simple; you can swap to s3://... later
        norm_text=norm_text,
        content_hash=h,
        pdf_rev=None,
        mime=mime,
    )

    # insert snapshot
    snap_id = insert_snapshot(
        document_id=doc_id,
        title=args.title,
        topic=args.topic,
        score=None,
        effective_date=eff_date,
        form_id=args.form_id,
    )

    # diff with previous snapshot for this URL (if any)
    prev_snap_id = latest_snapshot_id_for_url(url)
    if prev_snap_id:
        prev_doc = get_prev_doc_text(url)  # (doc_id, normalized_text) from most recent document for this URL
        prev_txt = prev_doc["normalized_text"] if isinstance(prev_doc, dict) else prev_doc[1]
        diff_lines = difflib.unified_diff(
            (prev_txt or "").splitlines(),
            (norm_text or "").splitlines(),
            lineterm="",
        )
        diff_text = "\n".join(diff_lines)
        if diff_text.strip():
            insert_diff(snap_id, prev_snap_id, diff_text)

    # remember this hash for the URL
    touch_seen(url, h)

    print(f"[fetch_url] OK → source_id={source_id} doc_id={doc_id} snapshot_id={snap_id}")


if __name__ == "__main__":
    main()
