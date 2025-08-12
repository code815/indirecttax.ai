# parser/pdf_extract.py
# Robust PDF → text with OCR fallback. Safe cleanup on all paths.

import os
import logging
from typing import Optional, List

import fitz  # PyMuPDF
import pytesseract
from PIL import Image

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------- Tunables (env) ----------
PDF_MIN_TEXT = int(os.getenv("PDF_MIN_TEXT", "120"))            # below → try OCR
PDF_OCR_ENABLED = os.getenv("PDF_OCR_ENABLED", "true").lower() != "false"
PDF_OCR_DPI = int(os.getenv("PDF_OCR_DPI", "300"))
PDF_OCR_LANG = os.getenv("PDF_OCR_LANG", "eng")
PDF_MAX_PAGES = int(os.getenv("PDF_MAX_PAGES", "0"))            # 0 = no limit
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "")                  # e.g., "C:\\Program Files\\Tesseract-OCR\\tesseract.exe"
if TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD


def _munge_pix_to_image(pix: fitz.Pixmap) -> Image.Image:
    """Convert PyMuPDF Pixmap → PIL.Image safely."""
    mode = "RGB" if pix.n < 4 else "RGBA"
    img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
    if mode == "RGBA":
        img = img.convert("RGB")
    return img


def _extract_text_pymupdf(doc: fitz.Document, page_limit: int) -> str:
    """Fast text extraction using PyMuPDF."""
    text_parts: List[str] = []
    page_count = doc.page_count
    max_pages = min(page_count, page_limit) if page_limit > 0 else page_count
    for i in range(max_pages):
        try:
            pg = doc.load_page(i)
            text_parts.append(pg.get_text())
        except Exception as e:
            logger.warning(f"PyMuPDF read error on page {i+1}: {e}")
    return "\n".join(tp for tp in text_parts if tp).strip()


def _extract_text_ocr(doc: fitz.Document, dpi: int, page_limit: int, lang: str) -> str:
    """OCR fallback via Tesseract."""
    text_parts: List[str] = []
    page_count = doc.page_count
    max_pages = min(page_count, page_limit) if page_limit > 0 else page_count
    for i in range(max_pages):
        try:
            pg = doc.load_page(i)
            # Render at desired DPI (matrix scales 72dpi base)
            zoom = dpi / 72.0
            pix = pg.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
            img = _munge_pix_to_image(pix)
            txt = pytesseract.image_to_string(img, lang=lang)
            if txt:
                text_parts.append(txt)
        except Exception as e:
            logger.warning(f"OCR error on page {i+1}: {e}")
    return "\n".join(tp for tp in text_parts if tp).strip()


def extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """
    Extract text from a PDF. Strategy:
      1) PyMuPDF text extraction
      2) If too short and OCR enabled → Tesseract OCR on rendered pages
    Returns None if nothing meaningful found.
    """
    if not pdf_bytes:
        return None

    doc: Optional[fitz.Document] = None
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        # 1) Try native text first
        native_text = _extract_text_pymupdf(doc, PDF_MAX_PAGES)
        if len(native_text) >= PDF_MIN_TEXT:
            return native_text

        logger.info(f"PDF native text too short ({len(native_text)} chars). "
                    f"OCR enabled={PDF_OCR_ENABLED}.")
        if not PDF_OCR_ENABLED:
            return native_text if native_text else None

        # 2) OCR fallback
        ocr_text = _extract_text_ocr(doc, PDF_OCR_DPI, PDF_MAX_PAGES, PDF_OCR_LANG)
        if len(ocr_text) >= max(80, PDF_MIN_TEXT // 2):
            # prefer longer of the two if both present
            return ocr_text if len(ocr_text) > len(native_text) else native_text or ocr_text

        # Neither yielded enough text
        return native_text if native_text else None

    except Exception as e:
        logger.error(f"PDF extraction failed: {e}")
        return None
    finally:
        try:
            if doc is not None:
                doc.close()
        except Exception:
            pass


if __name__ == "__main__":
    # quick manual test (drop a sample at sample_data/pdf/sample.pdf)
    path = os.path.join("sample_data", "pdf", "sample.pdf")
    try:
        with open(path, "rb") as f:
            data = f.read()
        txt = extract_text_from_pdf(data)
        if txt:
            print(f"OK, got {len(txt)} chars")
            print(txt[:600], "…")
        else:
            print("No meaningful text extracted.")
    except FileNotFoundError:
        print(f"Put a test PDF at {path} to try this locally.")
