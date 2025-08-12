# rules/sources.py
# Loader + helpers for sources.yaml so jobs/discover_hubs.py can use them.

import os
import re
import yaml
from urllib.parse import urlparse
from functools import lru_cache
from typing import Dict, Any, List, Optional

# Allow override via env; default to rules/sources.yaml
_YAML_PATH = os.getenv("SOURCES_YAML", os.path.join("rules", "sources.yaml"))

@lru_cache(maxsize=1)
def load_sources() -> Dict[str, Any]:
    with open(_YAML_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    # Expected shape:
    # {
    #   "states": [
    #     {"code":"TX","hubs":[
    #         {"name": "...", "url": "...", "allow_re": "...", "type": "hub",
    #          "feed_url": "...", "sitemap_url": "..."}
    #     ]},
    #     {"code":"CA", "hubs":[ ... ]}
    #   ]
    # }
    return data

def _domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def iter_hubs() -> List[Dict[str, Any]]:
    """
    Yield hub dicts enriched with '_state'.
    """
    data = load_sources()
    out: List[Dict[str, Any]] = []
    for st in (data.get("states") or []):
        code = (st.get("code") or "").upper()
        for hub in (st.get("hubs") or []):
            if not isinstance(hub, dict):
                continue
            h = dict(hub)
            h["_state"] = code
            out.append(h)
    return out

def get_rules_for_domain(url: str) -> Optional[Dict[str, Any]]:
    """
    Find a hub whose domain or allow_re matches this URL.
    Return a normalized rules dict with:
      - allowlist_regex
      - state
      - hub_name
      - feed_url
      - sitemap_url
    """
    target_dom = _domain_of(url)
    for hub in iter_hubs():
        allow = hub.get("allow_re") or ".*"
        hub_dom = _domain_of(hub.get("url", ""))
        try:
            dom_match = (hub_dom == target_dom) if hub_dom else False
            re_match = re.search(allow, url) is not None
        except re.error:
            re_match = False

        if dom_match or re_match:
            return {
                "allowlist_regex": allow,
                "state": hub.get("_state"),
                "hub_name": hub.get("name"),
                "feed_url": hub.get("feed_url"),
                "sitemap_url": hub.get("sitemap_url"),
            }
    return None
