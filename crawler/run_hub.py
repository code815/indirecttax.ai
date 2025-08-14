# crawler/run_hub.py
# Hub/link discovery with RSS + sitemap support, allowlist filtering,
# junk-link filtering, and robots.txt checks.

import re
import logging
from typing import List, Iterable
from urllib.parse import urljoin, urlparse

import feedparser
from lxml import etree, html

from crawler.fetch import fetch_url_with_retries, _get_robot_parser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -------- junk link filters (expand as needed) --------
JUNK_PATTERNS = [
    re.compile(r"^mailto:", re.I),
    re.compile(r"^tel:", re.I),
    re.compile(r"#.+$"),                      # same-page fragments
    re.compile(r"/careers?(/|$)", re.I),
    re.compile(r"/jobs?(/|$)", re.I),
    re.compile(r"/about(-us)?(/|$)", re.I),
    re.compile(r"/privacy(-policy)?(/|$)", re.I),
    re.compile(r"/terms(-of-service)?(/|$)", re.I),
    re.compile(r"/social|/facebook|/twitter|/linkedin", re.I),
]

def _is_junk(u: str) -> bool:
    return any(p.search(u) for p in JUNK_PATTERNS)

def _unique(seq: Iterable[str]) -> List[str]:
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

# -------- RSS / Atom discovery --------

def discover_links_from_rss(feed_url: str) -> List[str]:
    try:
        logger.info(f"Parsing RSS/Atom feed: {feed_url}")
        feed = feedparser.parse(feed_url)
        links = []
        for entry in feed.entries:
            link = getattr(entry, "link", "") or ""
            if link:
                links.append(link)
        return links
    except Exception as e:
        logger.warning(f"RSS parse failed for {feed_url}: {e}")
        return []

# -------- sitemap discovery (index + urlset) --------

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

def _extract_sitemap_locs(xml_bytes: bytes) -> List[str]:
    """Return <loc> values from either <sitemapindex> or <urlset> documents."""
    try:
        root = etree.fromstring(xml_bytes)
    except Exception:
        return []
    locs = []
    # sitemapindex -> <sitemap><loc>
    locs.extend([el.text for el in root.xpath("//sm:sitemap/sm:loc", namespaces=SITEMAP_NS) if el is not None and el.text])
    # urlset -> <url><loc>
    locs.extend([el.text for el in root.xpath("//sm:url/sm:loc", namespaces=SITEMAP_NS) if el is not None and el.text])
    return [x.strip() for x in locs if x and x.strip()]

def discover_links_from_sitemap(sitemap_url: str, depth: int = 1, max_depth: int = 2) -> List[str]:
    """Recursively gather URLs from sitemap indexes and urlsets (bounded depth)."""
    try:
        resp = fetch_url_with_retries(sitemap_url)
        locs = _extract_sitemap_locs(resp.content)
        out = []
        for loc in locs:
            # Heuristic: if it looks like another sitemap, dive (bounded)
            if depth < max_depth and loc.lower().endswith(("sitemap.xml", ".xml")):
                out.extend(discover_links_from_sitemap(loc, depth + 1, max_depth))
            else:
                out.append(loc)
        return _unique(out)
    except Exception as e:
        logger.warning(f"Sitemap parse failed for {sitemap_url}: {e}")
        return []

# -------- hub page parsing --------

def parse_hub_page(hub_url: str, rules: dict) -> List[str]:
    """
    Discover candidate links from a hub page:
      1) RSS/Atom links in <head>
      2) Sitemaps listed in robots.txt (and recurse)
      3) Fallback: <a href> links filtered by allowlist + junk filters
      4) Robots.txt check for each candidate URL
    """
    discovered: List[str] = []
    allow_re = rules.get("allowlist_regex") or rules.get("allow_re") or ".*"
    allow = re.compile(allow_re)

    try:
        resp = fetch_url_with_retries(hub_url)
        doc = html.fromstring(resp.content)
    except Exception as e:
        logger.error(f"Fetch/parse failed for hub {hub_url}: {e}")
        return []

    # 1) RSS/Atom feeds
    try:
        feed_hrefs = doc.xpath('//head/link[@rel="alternate" and (@type="application/rss+xml" or @type="application/atom+xml")]/@href')
        for href in feed_hrefs:
            feed_url = urljoin(hub_url, href)
            discovered.extend(discover_links_from_rss(feed_url))
    except Exception as e:
        logger.debug(f"No/failed RSS discovery on {hub_url}: {e}")

    # 2) Sitemaps via robots.txt
    try:
        parser = _get_robot_parser(hub_url)
        sitemaps = parser.get_sitemaps() or []
        for sm in sitemaps:
            discovered.extend(discover_links_from_sitemap(sm))
    except Exception as e:
        logger.debug(f"No/failed sitemap discovery on {hub_url}: {e}")

    # 3) Fallback: links on the page
    try:
        hrefs = [urljoin(hub_url, h) for h in doc.xpath("//a/@href")]
        discovered.extend(hrefs)
    except Exception:
        pass

    # Normalize & filter
    base = f"{urlparse(hub_url).scheme}://{urlparse(hub_url).netloc}"
    def _norm(u: str) -> str:
        # ensure absolute
        if not u:
            return ""
        if u.startswith("//"):
            return f"{urlparse(hub_url).scheme}:{u}"
        if u.startswith("/"):
            return urljoin(base, u)
        return u

    candidates = []
    for u in discovered:
        u = _norm(u.strip())
        if not u:
            continue
        if _is_junk(u):
            continue
        if not allow.search(u):
            continue
        candidates.append(u)

    candidates = _unique(candidates)

    # 4) robots allow per-URL
    allowed = []
    for u in candidates:
        try:
            p = _get_robot_parser(u)
            if p.is_allowed("bulletin-monitor/0.2", u):
                allowed.append(u)
        except Exception:
            # if parser fails, be conservative and include (same behavior as fetch path)
            allowed.append(u)

    logger.info(f"[HUB] {hub_url} -> {len(allowed)} kept (from {len(candidates)} candidates)")
    return allowed

# -------- manual test --------

if __name__ == "__main__":
    # Example (adjust rules as needed)
    rules = {"allowlist_regex": r".*\.(pdf|html?)$|.*/tax(es|)/.*"}
    hub = "https://example.com/"
    links = parse_hub_page(hub, rules)
    for i, u in enumerate(links[:50], 1):
        print(f"{i:02d}. {u}")
    if len(links) > 50:
        print(f"...and {len(links)-50} more")


