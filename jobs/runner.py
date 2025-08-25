import os, importlib, logging, sys

def main():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    entry = (os.getenv("CRAWL_ENTRY") or "").strip()
    if not entry:
        logging.info("No CRAWL_ENTRY provided; nothing to run. Exiting 0.")
        return 0
    try:
        if ":" not in entry:
            raise ValueError("CRAWL_ENTRY must be 'package.module:function'")
        mod_name, func_name = entry.split(":", 1)
        mod = importlib.import_module(mod_name)
        func = getattr(mod, func_name)
        logging.info("Starting crawl entry: %s.%s", mod_name, func_name)
    except Exception:
        logging.exception("Failed to import CRAWL_ENTRY=%r", entry)
        return 2
    try:
        result = func()
        logging.info("Crawl finished OK: %r", result)
        return 0
    except Exception:
        logging.exception("Crawl error")
        return 1

if __name__ == "__main__":
    sys.exit(main())
