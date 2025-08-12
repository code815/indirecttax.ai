import hashlib
from diff_match_patch import diff_match_patch

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def compute_diff(old: str, new: str, context_chars=400) -> str:
    dmp = diff_match_patch()
    diffs = dmp.diff_main(old, new)
    dmp.diff_cleanupSemantic(diffs)
    out = []
    for op, data in diffs:
        if op == 1: out.append(f"+ {data[:context_chars]}")
        elif op == -1: out.append(f"- {data[:context_chars]}")
    return "\n".join(out[:2000])
