# jobs/discover_hubs.py
# Visit configured hubs, discover candidate links (RSS + sitemap + page),
# filter with allowlist + junk filters + robots, and append unique URLs to jobs/urls.txt.

import os
import logging
from typing import Set, List

from rules.sources import iter_hubs, get_rules_for_domain
from crawler.run_hub import parse_hub_page, discover_links_from_rss

URLS_FILE = os.path.join("jobs", "urls.txt")
MAX_NEW = int(os.getenv("DISCOVER_MAX_NEW", "0"))  # 0 = no cap

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("discover_hubs")


def _load_existing() -> Set[str]:
    if not os.path.exists(URLS_FILE):
        return set()
    raw = open(URLS_FILE, "rb").read().decode("utf-8-sig")
    urls: Set[str] = set()
    for line in raw.splitlines():
        s = (line or "").strip()
        if not s or s.startswith("#"):
            continue
        urls.add(s.split("#", 1)[0].strip())
    return urls


def _write_append(new_urls: List[str]) -> int:
    if not new_urls:
        return 0
    with open(URLS_FILE, "a", encoding="utf-8") as f:
        for u in new_urls:
            f.write(u + "\n")
    return len(new_urls)


def main():
    existing = _load_existing()
    added: Set[str] = set()

    log.info(f"Loaded {len(existing)} existing seed URLs from {URLS_FILE}")

    for hub in iter_hubs():
        hub_url = hub.get("url")
        if not hub_url:
            continue
        rules = get_rules_for_domain(hub_url) or {"allowlist_regex": ".*"}

        try:
            # Primary: parse hub page (sitemap + RSS discovery inside, plus fallback to <a> links)
            links = parse_hub_page(hub_url, rules)
            # If robots blocks the hub page or nothing found, try RSS directly if provided
            if (not links) and hub.get("feed_url"):
                links = discover_links_from_rss(hub["feed_url"])

            keep = [u for u in links if u not in existing]
            added.update(keep)
            log.info(f"[HUB] {hub.get('_state','?')} – {hub.get('name','(unnamed)')} :: +{len(keep)} new")

        except Exception as e:
            # If hub fetch/parsing fails, try the RSS as a fallback when available
            if hub.get("feed_url"):
                try:
                    feed_links = discover_links_from_rss(hub["feed_url"])
                    keep = [u for u in feed_links if u not in existing]
                    added.update(keep)
                    log.info(f"[HUB-FEED-FALLBACK] {hub.get('_state','?')} – {hub.get('name','(unnamed)')} :: +{len(keep)} from RSS")
                except Exception as e2:
                    log.warning(f"[HUB ERR] {hub_url} (and RSS fallback failed) :: {e2}")
            else:
                log.warning(f"[HUB ERR] {hub_url} :: {e}")

    # Optional cap on how many we append per run
    to_write = sorted(added)
    if MAX_NEW > 0:
        to_write = to_write[:MAX_NEW]

    new_count = _write_append(to_write)
    log.info(f"Appended {new_count} new URLs to {URLS_FILE} (total now ~{len(existing)+new_count}).")


if __name__ == "__main__":
    main()
