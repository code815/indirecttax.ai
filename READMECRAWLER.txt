Web Crawler Readme

This document outlines how to run the web crawler locally, configure its behavior, and details its known limitations.

How to Run Locally

    Dependencies: Ensure you have the necessary Python libraries installed. The crawler now uses requests, tenacity, robotexclusionrulesparser, readability-lxml, beautifulsoup4, PyMuPDF, pytesseract, feedparser, and Playwright.

        First, install the Python dependencies:
        Bash

pip install requests tenacity robotexclusionrulesparser readability-lxml beautifulsoup4 PyMuPDF pytesseract feedparser playwright

Next, install the Playwright browsers:
Bash

    playwright install

    For pytesseract, you will also need to install the Tesseract OCR engine on your system.

Environment Variables: Create a .env file based on .env.example and fill in the necessary details. The crawler expects the following variables:

POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_HOST=your_host
POSTGRES_DB=your_db
S3_ENDPOINT=http://localhost:9000
S3_BUCKET=bulletin-raw
TLS_VERIFY=true

Note that the S3 endpoint and bucket are configured for local development. The TLS_VERIFY variable is a new addition to control TLS verification.

Seed URLs: Update the jobs/urls.txt file with the initial URLs you want to crawl. These can be hub pages (like a blog's main page) or direct links to documents (HTML or PDF).

Execution: Run the main daily crawl script from the project root:
Bash

    python jobs/daily_crawl.py

Rate-Limit Knobs & Configurations

    Rate Limiting: The crawler respects the Crawl-delay directive in a website's robots.txt file. If no Crawl-delay is specified, it defaults to a minimum of 0.5 seconds between requests to the same domain.

    TLS Verification: The TLS_VERIFY environment variable controls whether the crawler will verify SSL/TLS certificates. The default is true. To disable verification, set TLS_VERIFY=false in your .env file. This is useful for dealing with broken certificates but should be used with caution.

Known Limitations

    Non-Standard Content: While the HTML and PDF parsers are robust, they may struggle with highly non-standard document formats or extremely malformed HTML.

    Scalability: The per-domain throttling mechanism in crawler/fetch.py is a simple in-memory dictionary. This is not suitable for a distributed crawling system, which would require a shared, persistent state for throttling.

Verification Instructions

To ensure the new features are working as expected, you can use the following URLs in your jobs/urls.txt file:

    Playwright Fallback Test: Use a JavaScript-heavy site that loads content dynamically, such as a single-page application.

    Sitemap Test: Use a website with a robots.txt that points to both a <sitemapindex> and a <urlset> file.

    OCR Test: Use a sample PDF that contains image-only text to verify the OCR fallback.

    Idempotency Test: Rerun the crawler with the same seed URLs to confirm that no duplicate entries are created in your mock api.db.