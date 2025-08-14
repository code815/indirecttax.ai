# Utility: Inject chat widget snippet into api/viewer.py HTML (safe, idempotent-ish)
import re
from pathlib import Path

vp = Path("api/viewer.py")
src = vp.read_text(encoding="utf-8")
snippet = Path("api/_viewer_chat_snippet.html").read_text(encoding="utf-8")

if "Chat launcher" in src:
    print("Chat snippet already present.")
else:
    # naive: insert before closing </body>
    new_src = re.sub(r"</body>\s*</html>\s*$", snippet + "\n</body></html>", src, flags=re.S|re.I)
    if new_src == src:
        print("Could not inject chat snippet automatically. Insert manually from api/_viewer_chat_snippet.html")
    else:
        vp.write_text(new_src, encoding="utf-8")
        print("Injected chat snippet into api/viewer.py")
