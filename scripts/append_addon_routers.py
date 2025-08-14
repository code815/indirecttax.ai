# Utility: Append router includes into api/server.py after app creation.
import io, sys, re, os
from pathlib import Path

server_path = Path("api/server.py")
src = server_path.read_text(encoding="utf-8")
needle = "app = FastAPI"
pos = src.find(needle)
if pos == -1:
    print("Could not find FastAPI app initialization; aborting.")
    sys.exit(1)

# Find the first blank line after 'app = FastAPI(...)'
lines = src.splitlines()
idx = 0
for i, ln in enumerate(lines):
    if ln.strip().startswith("app = FastAPI"):
        idx = i
        break
# insert after CORS middleware block if present, else after app line
insert_at = None
for j in range(idx, min(idx+80, len(lines))):
    if "add_middleware" in lines[j]:
        insert_at = j
# find end of that block (closing parenthesis) - naive scan
if insert_at is not None:
    k = insert_at
    parens = 0
    for j in range(insert_at, len(lines)):
        parens += lines[j].count("(")
        parens -= lines[j].count(")")
        if parens <= 0 and j > insert_at:
            insert_at = j + 1
            break
else:
    insert_at = idx + 1

glue = "try:\n    from api.export import router as export_router\n    app.include_router(export_router)\nexcept Exception as _e:\n    # export is optional; log or ignore\n    pass\n\ntry:\n    from api.chat import router as chat_router\n    app.include_router(chat_router)\nexcept Exception:\n    pass\n\ntry:\n    from api.stats import router as stats_router\n    app.include_router(stats_router)\nexcept Exception:\n    pass\n"
lines.insert(insert_at, glue)
server_path.write_text("\n".join(lines), encoding="utf-8")
print("Patched api/server.py with addon routers.")
