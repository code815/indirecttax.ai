# parser/html_text.py
# Readability-first HTML → clean text, with a conservative fallback extractor.

import os
import logging
from typing import Optional, Iterable

from readability import Document
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Tunables (env overrides)
MIN_TEXT_LEN = int(os.getenv("HTML_MIN_TEXT_LEN", "400"))   # if below → try fallback
MAX_BLOCK_LEN = int(os.getenv("HTML_MAX_BLOCK_LEN", "4000"))  # trim any single block to avoid runaway text


def _join_blocks(blocks: Iterable[str]) -> str:
    out = []
    for b in blocks:
        b = (b or "").strip()
        if not b:
            continue
        if len(b) > MAX_BLOCK_LEN:
            b = b[:MAX_BLOCK_LEN].rstrip() + "…"
        out.append(b)
    # double newlines between blocks for readability/diff stability
    return ("\n\n".join(out)).strip()


def _readability_extract(html: str) -> str:
    """
    Use readability-lxml to isolate the article/main content,
    then harvest headings, paragraphs, and list items.
    """
    doc = Document(html)
    main_html = doc.summary(html_partial=True)  # just the main content
    soup = BeautifulSoup(main_html, "lxml")

    blocks = []
    for el in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li"]):
        text = el.get_text(" ", strip=True)
        if text:
            blocks.append(text)
    return _join_blocks(blocks)


def _fallback_extract(html: str) -> str:
    """
    Conservative fallback: strip scripts/styles/nav/aside/footer,
    then collect headings, paragraphs, and list items from the whole page.
    """
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    # Remove common boilerplate containers
    for sel in ["nav", "header", "footer", "aside"]:
        for t in soup.select(sel):
            t.decompose()

    blocks = []
    for el in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "p", "li"]):
        text = el.get_text(" ", strip=True)
        if text:
            blocks.append(text)
    return _join_blocks(blocks)


def extract_content_from_html(html_content: str) -> Optional[str]:
    """
    Extract main content text. Returns None if nothing meaningful found.
    Strategy:
      1) readability-lxml to isolate main article.
      2) If too short, fallback to conservative page-wide extractor.
    """
    if not html_content:
        return None

    try:
        text = _readability_extract(html_content)
        if len(text) >= MIN_TEXT_LEN:
            return text

        logger.info(f"Readability result too short ({len(text)} chars). Using fallback extractor.")
        fb = _fallback_extract(html_content)

        # prefer the longer of the two (but return None if still trivial)
        best = fb if len(fb) > len(text) else text
        return best if len(best.strip()) >= max(80, MIN_TEXT_LEN // 4) else None

    except Exception as e:
        logger.error(f"HTML extraction failed: {e}")
        try:
            fb = _fallback_extract(html_content)
            return fb if len(fb.strip()) >= 80 else None
        except Exception as e2:
            logger.error(f"Fallback extraction also failed: {e2}")
            return None


if __name__ == "__main__":
    sample_html = """
    <html>
      <head><title>Sample</title></head>
      <body>
        <header><nav>menu</nav></header>
        <main>
          <h1>Important Update</h1>
          <p>Beginning January 1, 2026, the rate will change.</p>
          <ul><li>Form 01-339 revised</li><li>New filing portal</li></ul>
        </main>
        <footer>© 2025</footer>
      </body>
    </html>
    """
    txt = extract_content_from_html(sample_html)
    print(txt or "(no text)")


