# MTA-Crawler

```
  __  __ _____  _                ____                    _
 |  \/  |_   _|/ \              / ___|_ __ __ ___      _| | ___ _ __
 | |\/| | | | / _ \   ______  | |   | '__/ _` \ \ /\ / / |/ _ \ '__|
 | |  | | | |/ ___ \ |______| | |___| | | (_| |\ V  V /| |  __/ |
 |_|  |_| |_/_/   \_\          \____|_|  \__,_| \_/\_/ |_|\___|_|

  Author: MTAr00t  |  github.com/MTAr00t
```

An async, scope-limited web crawler that maps a target website into a **GraphML** graph.
It randomises its User-Agent on every request, persists state to SQLite so it can survive
interruptions, and extracts URLs from both HTML and linked JavaScript files.

---

## Features

| Feature | Detail |
|---|---|
| **Async crawling** | Built on `aiohttp` + `asyncio`; configurable worker pool |
| **Scope control** | Stays within the domains (and subdomains) you seed |
| **User-Agent rotation** | 60+ real device profiles (Android, iPhone, Tablet, Console…) |
| **SQLite persistence** | Every page and edge is checkpointed; resume after a crash with `--resume` |
| **JS URL extraction** | Downloads linked `.js` files and regex-scans them for additional URLs |
| **Redirect tracking** | Optional `--follow-redirects` records 3xx chains in the graph |
| **Custom headers** | Pass any HTTP header with `-H` |
| **TLS bypass** | `--no-verify` skips certificate validation |
| **GraphML output** | Directed graph importable by Gephi, Cytoscape, NetworkX, yEd, etc. |
| **Colour terminal** | Clear colour-coded log output via `colorama` |

---

## Repository structure

```
MTA-Crawler/
├── crawler.py        # Main crawler — entry point
├── uas.py            # User-Agent pool (60+ device profiles)
├── requirements.txt  # Python dependencies
└── urls.txt          # Example seed file
```

---

## Installation

**Requirements:** Python 3.9 or newer.

```bash
# 1. Clone the repository
git clone https://github.com/MTAr00t/MTA-Crawler.git
cd MTA-Crawler

# 2. (Recommended) create a virtual environment
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Quick start

1. Create a plain-text seed file with one URL per line:

```
# urls.txt
https://example.com
https://blog.example.com
```

2. Run the crawler:

```bash
python crawler.py urls.txt
```

That's it. When done, you'll find `crawl.graphml` and `crawl.db` in the current directory.

---

## Usage

```
python crawler.py <infile> [options]
```

### Positional argument

| Argument | Description |
|---|---|
| `infile` | Path to a text file with one seed URL per line. Lines starting with `#` are ignored. |

### Options

| Flag | Default | Description |
|---|---|---|
| `-d`, `--depth` | `2` | Maximum crawl depth from each seed URL |
| `-c`, `--concurrency` | `10` | Number of parallel worker coroutines |
| `--delay` | `0.0` | Seconds to wait between each request (politeness delay) |
| `-H 'Key: Value'` | — | Add a custom HTTP request header (repeatable) |
| `--no-verify` | off | Disable TLS/SSL certificate verification |
| `--follow-redirects` | off | Follow and record 3xx redirects in the graph |
| `-o`, `--output` | `crawl.graphml` | Output file path for the GraphML graph |
| `--checkpoint-file` | `crawl.db` | SQLite file used for persistence |
| `--resume` | off | Resume from an existing checkpoint file instead of starting fresh |

---

## Examples

**Basic crawl with depth 3:**
```bash
python crawler.py urls.txt -d 3
```

**20 parallel workers, 0.5 s politeness delay:**
```bash
python crawler.py urls.txt -c 20 --delay 0.5
```

**Custom headers + redirect tracking:**
```bash
python crawler.py urls.txt \
  -H 'Authorization: Bearer TOKEN' \
  -H 'X-Custom-Header: value' \
  --follow-redirects
```

**Skip TLS errors (e.g. self-signed cert labs):**
```bash
python crawler.py urls.txt --no-verify
```

**Save to a specific output file:**
```bash
python crawler.py urls.txt -o target_map.graphml
```

**Interrupt and resume later:**
```bash
# start a long crawl
python crawler.py urls.txt -d 5 --checkpoint-file my_crawl.db

# Ctrl-C to stop, then resume where it left off
python crawler.py urls.txt -d 5 --checkpoint-file my_crawl.db --resume
```

---

## Output

### GraphML file (`crawl.graphml`)

A directed graph where:

- **Nodes** represent URLs. Each node carries a `status` attribute:
  - HTTP status code (e.g. `200`, `404`) for crawled pages
  - `external` for out-of-scope URLs
  - `file` for in-scope static assets

- **Edges** represent discovered relationships. Each edge carries a `type` attribute:

| Edge type | Meaning |
|---|---|
| `link` | Standard in-scope HTML hyperlink |
| `file` | In-scope static resource (image, script, style-sheet, etc.) |
| `external` | Link or resource pointing outside the allowed domains |
| `redirect` | A 3xx HTTP redirect (only with `--follow-redirects`) |

Import into **Gephi**, **Cytoscape**, **yEd**, or visualise in Python:

```python
import networkx as nx
G = nx.read_graphml("crawl.graphml")
print(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")
```

### SQLite database (`crawl.db`)

Two tables:

```sql
-- Every URL encountered
SELECT * FROM pages;   -- url, depth, status, processed

-- Every relationship
SELECT * FROM edges;   -- src, dst, type, status
```

You can query or export it with any SQLite client.

---

## How it works

### `crawler.py` — step by step

1. **Startup** — The CLI banner and config summary are printed. Seed URLs are read from the input file and normalised (https forced, `www.` stripped, trailing slashes removed, fragments dropped).

2. **Database init** — `aiosqlite` opens (or creates) `crawl.db` with two tables: `pages` (URL frontier) and `edges` (link graph). An index on `processed` keeps resume fast.

3. **Resume detection** — If `--resume` is passed, unprocessed rows are re-loaded into the async queue and the in-memory NetworkX graph is rebuilt from the DB, so no work is repeated.

4. **Worker pool** — `N` async worker coroutines (controlled by `--concurrency`) are launched. They all pull from a shared `asyncio.Queue`. A `Semaphore` limits actual concurrent HTTP connections.

5. **Fetching** — Each worker calls `fetch()`:
   - Picks a random User-Agent from the pool.
   - GETs the URL with `aiohttp`.
   - Writes the HTTP status back to the DB and graph node.
   - If a 3xx is received and `--follow-redirects` is set, records the redirect edge and enqueues the destination.

6. **Parsing** — For pages within the depth limit, BeautifulSoup extracts every URL from `<a>`, `<img>`, `<script>`, `<link>`, `<iframe>`, `<video>`, `<audio>`, `<embed>`, and `<source>` tags.
   - URLs with file extensions → classified as `file` (in-scope) or `external`.
   - URLs without extensions → classified as `link` (in-scope) or `external`.
   - In-scope, unvisited URLs are enqueued for crawling.

7. **JS extraction** — For every `<script src="…">` pointing to a `.js` file, the JS source is downloaded and all `http(s)://…` patterns are regex-extracted and processed the same way.

8. **Logging** — All log messages are pushed to a dedicated `asyncio.Queue` and printed by a single logger coroutine, preserving order without interleaving.

9. **Shutdown** — `SIGINT` / `SIGTERM` flush the DB and exit cleanly, so `--resume` can pick up exactly where the crawl stopped.

10. **Output** — `networkx.write_graphml()` serialises the in-memory directed graph to a deduplicated GraphML file.

---

### `uas.py` — User-Agent pool

Contains `USER_AGENTS_BY_DEVICE`, a dictionary mapping device categories to lists of real browser User-Agent strings:

| Category | Devices |
|---|---|
| `Android_Generic` | Samsung Galaxy A/S series |
| `Google_Pixel` | Pixel 6, 6a, 6 Pro, 7, 7 Pro |
| `Motorola` | Moto G series |
| `Android_Popular` | Redmi, Huawei, Xiaomi |
| `iPhone` | iPhone 9 through 14 (Safari, Chrome, Firefox) |
| `Windows_Phone` | Lumia / Edge Mobile |
| `Tablet` | Samsung, Lenovo, Nvidia, Fire HD, LG |
| `SetTopBox` | Fire TV, Chromecast, Roku, Nexus Player, Apple TV |
| `Game_Console` | PS4, PS5, PS Vita, Xbox One, Xbox Series X, Nintendo Switch, Wii U |

`USER_AGENTS` is the flat list used by `random.choice()` in `fetch()`.

---

## Legal & ethical use

> **MTA-Crawler is intended for use on systems you own or have explicit written permission to test.**
> Crawling websites without permission may violate their Terms of Service and local laws.
> Always respect `robots.txt`, rate limits, and the target's infrastructure.
> The author is not responsible for misuse.

---

## License

MIT — see [LICENSE](LICENSE) for details.

---

*Made with ❤️ by [MTAr00t](https://github.com/MTAr00t)*
