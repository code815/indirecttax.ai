import re
def normalize_text(txt: str) -> str:
    txt = re.sub(r"\r", "\n", txt)
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip().lower()

BOILERPLATE_PATTERNS = [r"© \d{4} state of .*", r"page \d+ of \d+", r"last updated: .*"]
def strip_boilerplate(txt: str) -> str:
    for pat in BOILERPLATE_PATTERNS:
        txt = re.sub(pat, "", txt, flags=re.I)
    return txt
