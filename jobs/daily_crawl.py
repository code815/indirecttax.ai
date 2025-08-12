# jobs/daily_crawl.py
# Bridge orchestrator: uses contractor's fetchers/parsers but writes to your DB/S3,
# preserves idempotency, and keeps your topic scoring + diffing.

import os
import mimetypes
import logging
from urllib.parse import urlparse

from crawler.fetch import fetch_url_with_retries
from parser.html_text import extract_content_from_html
from parser.pdf_extract import extract_text_from_pdf

from api.db import (
    get_last_hash,
    get_prev_doc_text,
    touch_seen,
    insert_document,
    insert_snapshot,
    insert_diff,
)
from api.s3util import ensure_bucket, put_bytes
from parser.normalize import normalize_text, strip_boilerplate
from parser.diffing import sha256, compute_diff
from parser.classify import Classifier, derive_title

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("daily_crawl")

clf = Classifier()
ensure_bucket()

USER_AGENT = os.getenv("CRAWLER_USER_AGENT", "bulletin-monitor/0.2")


def clean_url(u: str) -> str:
    """Trim BOM/comments/fragments and whitespace."""
    if not u:
        return ""
    u = u.strip().lstrip("\ufeff")
    # strip inline comments and left arrows etc.
    for sep in ["  ", "\t", " ←", " #", "  #", " # ", "←", "#"]:
        if sep in u:
            u = u.split(sep, 1)[0].strip()
    return u.split("#", 1)[0].strip()


def s3_key_for(url: str) -> str:
    p = urlparse(url)
    path = p.path.replace("/", "_").strip("_") or "index.html"
    return f"{p.netloc}/{path}"


def detect_mime(url: str, headers: dict) -> str:
    ct = (headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
    if ct:
        return ct
    guess = mimetypes.guess_type(url)[0]
    return (guess or "application/octet-stream").lower()


# (optional) light extractors—feel free to replace with your upgraded versions later
def find_effective_date(text: str):
    import re
    from datetime import datetime
    m = re.search(
        r"(effective|begins)\s*[:\-]?\s*((\d{1,2}/\d{1,2}/\d{2,4})|([a-z]{3,9}\s+\d{1,2},\s+\d{4}))",
        text,
        re.I,
    )
    if not m:
        return None
    raw = m.group(2)
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except Exception:
            pass
    return None


def find_form_id(text: str):
    import re
    # Common patterns (extend later): TX 01-xxx, DR-xxx, ST-xxx, CDTFA-xxx, REG-xx
    pat = re.compile(
        r"\b(01-\d{3}|DR-\d{2,4}|ST-\d{2,4}|CDTFA-\d{2,4}|REG-\d{1,3}|[A-Z]{1,3}-\d{2,4})\b",
        re.I,
    )
    m = pat.search(text)
    return m.group(0) if m else None


def process_url(url: str, source_id=None):
    url = clean_url(url)
    if not url:
        return

    try:
        resp = fetch_url_with_retries(url, user_agent=USER_AGENT)
    except Exception as e:
        log.warning(f"[ERR] fetch failed :: {url} :: {e}")
        return

    mime = detect_mime(url, resp.headers)
    text = None
    raw_bytes = b""

    if "application/pdf" in mime or url.lower().endswith(".pdf"):
        text = extract_text_from_pdf(resp.content)
        raw_bytes = resp.content
        mime = "application/pdf"
    elif "text/html" in mime or mime.startswith("text/") or not mime:
        html = resp.text
        text = extract_content_from_html(html) or html
        raw_bytes = html.encode("utf-8", errors="ignore")
        mime = "text/html"
    else:
        log.info(f"[SKIP] unsupported mime {mime} :: {url}")
        return

    if not text or not text.strip():
        log.info(f"[SKIP] no meaningful text :: {url}")
        return

    # normalize + hash
    norm = strip_boilerplate(normalize_text(text))
    h = sha256(norm)

    # idempotency check
    prev_h = get_last_hash(url)
    if prev_h == h:
        touch_seen(url, h)
        log.info(f"[SKIP] no change :: {url}")
        return

    # archive raw bytes
    key = s3_key_for(url)
    raw_uri = put_bytes(key, raw_bytes)

    # write document
    doc_id = insert_document(source_id, url, raw_uri, norm, h, None, mime)
    prev = get_prev_doc_text(url)

    # classify + score
    topic, base_score = clf.topic_and_score(norm)
    magnitude = 1 if len(norm) > 2000 else 0
    score = base_score + magnitude

    # light metadata (upgrade later via extraction pack)
    eff_date = find_effective_date(norm)
    form_id = find_form_id(norm)

    # snapshot
    snap_id = insert_snapshot(
        doc_id,
        derive_title(norm),
        topic,
        score,
        eff_date,
        form_id,
    )

    # diff vs previous normalized_text (if available)
    if prev and prev.get("normalized_text"):
        diff = compute_diff(prev["normalized_text"], norm)
        insert_diff(snap_id, prev["id"], diff)

    touch_seen(url, h)
    log.info(f"[OK] {topic} score={score} :: {url} -> {raw_uri}")


def main():
    # read urls.txt (BOM tolerant)
    path = os.path.join("jobs", "urls.txt")
    if not os.path.exists(path):
        log.error(f"Seed file not found: {path}")
        return
    raw = open(path, "rb").read().decode("utf-8-sig")
    urls = []
    for line in raw.splitlines():
        cu = clean_url(line)
        if cu and not cu.startswith("#"):
            urls.append(cu)

    if not urls:
        log.warning("No URLs to process.")
        return

    for u in urls:
        try:
            process_url(u)
        except Exception as e:
            log.error(f"[ERR] {u} :: {e}")


if __name__ == "__main__":
    main()


