# jobs/make_snapshots.py
# Create snapshots for documents that don't have one yet.
# Uses your existing psycopg2 helpers in api/db.py.

import re
import logging
from typing import Optional

from api.db import conn  # uses POSTGRES_* from .env
from parser.normalize import normalize_text, strip_boilerplate

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("make_snapshots")

# --- tiny helpers (reuse/parallel your earlier daily_crawl heuristics) ---

TITLE_RE = re.compile(r"^(?:notice|bulletin|update)[:\s-]+(.{10,120})$", re.I)

def derive_title(text: str) -> str:
    """
    Try to get a decent human title from normalized text.
    - first non-empty line up to ~140 chars
    - strip generic prefixes (Notice: …)
    """
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        m = TITLE_RE.match(line)
        if m:
            return m.group(1)[:140]
        return line[:140]
    return "Update"

def find_effective_date(text: str) -> Optional[str]:
    """
    Grab 'Effective mm/dd/yyyy' or 'Effective Month D, YYYY'.
    Returns YYYY-MM-DD (ISO) or None.
    """
    import datetime
    m = re.search(
        r"(effective|begins|starting)\s*[:\-]?\s*((\d{1,2}/\d{1,2}/\d{2,4})|([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}))",
        text, re.I)
    if not m:
        return None
    raw = m.group(2)
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.datetime.strptime(raw, fmt).date().isoformat()
        except Exception:
            pass
    return None

def find_form_id(text: str) -> Optional[str]:
    """
    Common form ids: 01-339 (TX), DR-xxx (CO/FL etc.), ST-xxx (NY/NJ), generic A-1234.
    """
    m = re.search(r"\b(01-339|DR-\d{2,4}|ST-\d{2,4}|[A-Z]{1,3}-?\d{2,4})\b", text)
    return m.group(0) if m else None

TOPIC_RULES = [
    ("Rates",       [r"rate(?:s)?", r"increase|decrease|change|adjust", r"%|percent|percentage"]),
    ("Forms",       [r"\bform\b|certificate|application|rev\.?|revision|version|expires|supersedes|DR-\d+|ST-\d+|01-339"]),
    ("Exemptions",  [r"exempt|exemption|nontaxable|exclude", r"food|machinery|manufacturing|ppe|grocery|beverage|soda|ssb"]),
    ("Freight",     [r"freight|delivery|shipping|transportation|carrier|fob", r"taxable|nontaxable|separately stated|title|possession"]),
    ("Marketplace", [r"marketplace", r"facilitator|seller|collection|remit"]),
    ("Deadlines",   [r"deadline|due|filing|extension", r"return|remittance|quarter|annual"]),
]

NEGATIVE_RULES = [r"job fair", r"award", r"grant", r"press release", r"hiring|career|internship"]

def classify_topic_score(text: str) -> tuple[str, int]:
    """
    Very simple keyword scoring. Returns (topic, score).
    """
    t = text
    for neg in NEGATIVE_RULES:
        if re.search(neg, t, re.I):
            return ("General", 0)

    best = ("General", 1)
    for topic, rules in TOPIC_RULES:
        matched = 0
        for r in rules:
            if re.search(r, t, re.I):
                matched += 1
        if matched >= max(1, len(rules) - 1):  # loose match
            base = 2 + matched
            # magnitude bonus for longer docs
            magnitude = 1 if len(t) > 2000 else 0
            score = base + magnitude
            if score > best[1]:
                best = (topic, score)
    return best

# --- main ---

def main():
    with conn() as c, c.cursor() as cur:
        # Find documents with no snapshot yet
        cur.execute("""
            SELECT d.id, d.url, COALESCE(d.normalized_text, ''), d.source_id
            FROM documents d
            LEFT JOIN snapshots s ON s.document_id = d.id
            WHERE s.id IS NULL
            ORDER BY d.fetched_at ASC
            LIMIT 500
        """)
        rows = cur.fetchall()

    if not rows:
        log.info("No documents need snapshots. Nothing to do.")
        return

    created = 0
    with conn() as c, c.cursor() as cur:
        for doc_id, url, norm_text, source_id in rows:
            if not norm_text:
                # fetch raw if needed? for now, skip empty
                log.info(f"[SKIP] empty text for {url}")
                continue

            norm = strip_boilerplate(normalize_text(norm_text))
            title = derive_title(norm)
            topic, score = classify_topic_score(norm)
            eff = find_effective_date(norm)
            form_id = find_form_id(norm)

            # Insert snapshot
            cur.execute("""
                INSERT INTO snapshots(document_id, title, topic, score, effective_date, form_id)
                VALUES (%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (doc_id, title, topic, score, eff, form_id))
            snap_id = cur.fetchone()[0]
            created += 1

        c.commit()

    log.info(f"Created {created} snapshot(s).")

if __name__ == "__main__":
    main()
