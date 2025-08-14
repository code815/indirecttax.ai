# crawler/fetch.py
# Robust fetcher with retries, robots.txt, per-domain throttling,
# configurable TLS verify, and Playwright fallback for JS-heavy pages.

import os
import time
import logging
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import urlparse

import requests
from tenacity import retry, wait_exponential, stop_after_attempt, after_log
from robotexclusionrulesparser import RobotExclusionRulesParser

# -------------------- logging --------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------- config knobs --------------------

USER_AGENT = os.getenv("CRAWLER_USER_AGENT", "bulletin-monitor/0.2")
# Default: verify TLS certificates. Set CRAWLER_VERIFY_TLS=false to disable (dev only).
VERIFY_TLS = os.getenv("CRAWLER_VERIFY_TLS", "true").lower() != "false"
# Playwright fallback mode: "auto" (fallback when HTML looks empty), "always", or "off"
PW_MODE = os.getenv("CRAWLER_PLAYWRIGHT", "auto").lower()
# Minimum non-whitespace characters in HTML before we decide it's "empty"
MIN_HTML_CHARS = int(os.getenv("CRAWLER_MIN_HTML_CHARS", "200"))
# Min delay between requests to the same domain if robots has no crawl-delay
DEFAULT_MIN_DELAY = float(os.getenv("CRAWLER_MIN_DELAY", "0.5"))

# -------------------- simple response wrapper --------------------

@dataclass
class ResponseLike:
    url: str
    status_code: int
    headers: Dict[str, str]
    content: bytes

    @property
    def text(self) -> str:
        # Decode best-effort as UTF-8
        try:
            return self.content.decode("utf-8", errors="ignore")
        except Exception:
            return ""

# -------------------- throttling & robots --------------------

_LAST_REQUEST_TIME: Dict[str, float] = {}
_ROBOTS_PARSERS: Dict[str, RobotExclusionRulesParser] = {}

def _get_robot_parser(url: str) -> RobotExclusionRulesParser:
    parsed = urlparse(url)
    domain = parsed.netloc
    if domain in _ROBOTS_PARSERS:
        return _ROBOTS_PARSERS[domain]

    robots_url = f"{parsed.scheme}://{domain}/robots.txt"
    parser = RobotExclusionRulesParser()
    try:
        r = requests.get(robots_url, headers={"User-Agent": USER_AGENT}, timeout=8, verify=VERIFY_TLS)
        if r.ok and r.text:
            parser.parse(r.text)
            logger.info(f"robots.txt loaded for {domain}")
        else:
            logger.info(f"robots.txt not available for {domain} (status {r.status_code})")
    except Exception as e:
        logger.warning(f"robots.txt fetch failed for {domain}: {e}")
    _ROBOTS_PARSERS[domain] = parser
    return parser

def _get_crawl_delay(url: str) -> float:
    parser = _get_robot_parser(url)
    delay = parser.get_crawl_delay(USER_AGENT)
    try:
        return float(delay) if delay is not None else DEFAULT_MIN_DELAY
    except Exception:
        return DEFAULT_MIN_DELAY

def _per_domain_throttle(url: str):
    parsed = urlparse(url)
    domain = parsed.netloc
    min_delay = _get_crawl_delay(url)
    now = time.time()
    last = _LAST_REQUEST_TIME.get(domain, 0.0)
    wait_for = last + min_delay - now
    if wait_for > 0:
        logger.info(f"Throttling {domain} for {wait_for:.2f}s (min_delay={min_delay:.2f})")
        time.sleep(wait_for)
    _LAST_REQUEST_TIME[domain] = time.time()

# -------------------- Playwright fallback --------------------

def _fetch_with_playwright(url: str) -> str:
    """
    Return rendered HTML via Playwright (Chromium), or "" on failure.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except Exception as e:
        logger.error(f"Playwright not installed/available: {e}")
        return ""

    logger.info(f"Using Playwright fallback for {url}")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=USER_AGENT)
            page = ctx.new_page()
            page.goto(url, wait_until="networkidle", timeout=60_000)
            html = page.content()
            browser.close()
            return html or ""
    except PWTimeout:
        logger.error(f"Playwright timeout for {url}")
        return ""
    except Exception as e:
        logger.error(f"Playwright failed for {url}: {e}")
        return ""

# -------------------- main fetch with retries --------------------

@retry(
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(5),
    after=after_log(logger, logging.WARNING),
    reraise=True,
)
def fetch_url_with_retries(url: str, user_agent: Optional[str] = None) -> ResponseLike:
    """
    Fetch a URL with robots.txt, per-domain throttle, retries, and optional Playwright fallback.
    Returns a ResponseLike (has .text, .content, .headers).
    """
    ua = user_agent or USER_AGENT

    # robots allow?
    parser = _get_robot_parser(url)
    try:
        allowed = parser.is_allowed(ua, url)
    except Exception:
        # Some parsers may raise if robots was empty — treat as allowed
        allowed = True
    if not allowed:
        raise ValueError(f"Disallowed by robots.txt: {url}")

    # throttle
    _per_domain_throttle(url)

    # primary: requests
    try:
        r = requests.get(url, headers={"User-Agent": ua}, timeout=30, verify=VERIFY_TLS)
        r.raise_for_status()
        headers = {k: v for k, v in r.headers.items()}
        content = r.content
        resp = ResponseLike(url=r.url, status_code=r.status_code, headers=headers, content=content)
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        if code == 404:
            raise ValueError(f"HTTP 404 Not Found: {url}")
        raise
    except requests.RequestException as e:
        # network/SSL/etc — let tenacity retry
        raise

    # maybe fallback to Playwright for HTML
    ct = (resp.headers.get("Content-Type", "").split(";", 1)[0] or "").lower()
    looks_html = ("text/html" in ct) or (ct == "" and url.lower().endswith((".htm", ".html", "/")))
    need_pw = False
    if PW_MODE == "always" and looks_html:
        need_pw = True
    elif PW_MODE == "auto" and looks_html:
        # Consider it "empty" if too few non-whitespace chars
        if len(resp.text.strip()) < MIN_HTML_CHARS:
            need_pw = True

    if need_pw:
        html = _fetch_with_playwright(url)
        if html and len(html.strip()) >= MIN_HTML_CHARS:
            # Override body but preserve headers
            return ResponseLike(
                url=resp.url,
                status_code=resp.status_code,
                headers={**resp.headers, "Content-Type": "text/html; charset=utf-8"},
                content=html.encode("utf-8", errors="ignore"),
            )

    return resp

# -------------------- manual test --------------------

if __name__ == "__main__":
    tests = [
        "https://httpbin.org/html",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/status/503",  # will retry
        "https://example.com/",
    ]
    for u in tests:
        try:
            r = fetch_url_with_retries(u)
            logger.info(f"{u} -> {r.status_code}, {len(r.content)} bytes, ct={r.headers.get('Content-Type')}")
        except Exception as e:
            logger.error(f"FAIL {u}: {e}")



