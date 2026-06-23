# MTA-Crawler
An async, scope-limited web crawler that maps a target website into a **GraphML** graph. It randomises its User-Agent on every request, persists state to SQLite so it can survive interruptions, and extracts URLs from both HTML and linked JavaScript files.
