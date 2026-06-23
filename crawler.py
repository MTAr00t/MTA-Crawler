#!/usr/bin/env python3
"""
MTA-Crawler — crawler.py

Async web crawler with SQLite persistence:
- Reads start URLs from a file
- Respects max depth, concurrency, and delay
- Randomizes User-Agent across 60+ real device profiles
- Custom headers; optional --no-verify & --follow-redirects
- Scope limited to start domains & subdomains
- Persists pages & edges in SQLite so you can resume after interruption
- Extracts URLs from inline <script> tags and linked JS files
- Outputs a deduplicated GraphML map (forced https, no www)

Author : MTAr00t
GitHub : https://github.com/MTAr00t
"""

import argparse
import asyncio
import random
import re
import os
import signal
from urllib.parse import urlparse, urljoin, urldefrag

import aiosqlite
import aiohttp
from bs4 import BeautifulSoup
import networkx as nx
import tldextract
from colorama import Fore, Style, init

from uas import USER_AGENTS

init(autoreset=True)

BANNER = f"""
{Fore.CYAN}
  __  __ _____  _                ____                    _
 |  \/  |_   _|/ \              / ___|_ __ __ ___      _| | ___ _ __
 | |\/| | | | / _ \   ______  | |   | '__/ _` \ \ /\ / / |/ _ \ '__|
 | |  | | | |/ ___ \ |______| | |___| | | (_| |\ V  V /| |  __/ |
 |_|  |_| |_/_/   \_\          \____|_|  \__,_| \_/\_/ |_|\___|_|

{Style.RESET_ALL}{Fore.WHITE}  MTA-Crawler  |  Author: MTAr00t  |  github.com/MTAr00t{Style.RESET_ALL}
{Fore.CYAN}{'─' * 60}{Style.RESET_ALL}
"""


def normalize_url(url, base=None):
    """
    Resolve relative URLs, strip fragments, lowercase scheme/netloc,
    drop 'www.', force 'https', and strip trailing slashes.
    """
    if base:
        url = urljoin(base, url)
    url, _ = urldefrag(url)

    parsed = urlparse(url)
    scheme = "https"
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = (parsed.path or "").rstrip("/")

    return f"{scheme}://{netloc}{path}"


def extract_domain(url):
    ext = tldextract.extract(url)
    return f"{ext.domain}.{ext.suffix}"


def parse_headers(header_list):
    hdrs = {}
    for h in header_list or []:
        if ":" not in h:
            raise argparse.ArgumentTypeError("Header must be 'Key: Value'")
        key, val = h.split(":", 1)
        hdrs[key.strip()] = val.strip()
    return hdrs


class Crawler:
    def __init__(self, start_urls, max_depth, delay, concurrency,
                 headers, no_verify, follow_redirects, out_file):
        self.start_urls = start_urls
        self.max_depth = max_depth
        self.delay = delay
        self.sema = asyncio.Semaphore(concurrency)
        self.headers = headers
        self.ssl = False if no_verify else None
        self.follow_redirects = follow_redirects
        self.out_file = out_file

        self.graph = nx.DiGraph()
        self.visited = set()
        self.queue = asyncio.Queue()
        self.allowed_domains = {extract_domain(u) for u in start_urls}

        # queue for ordered logging
        self.log_queue = asyncio.Queue()

        # SQLite persistence attrs (set in main)
        self.checkpoint_file = None
        self.resume = False

        # runtime
        self.db = None  # aiosqlite.Connection

    async def init_db(self):
        """Open (or create) the SQLite checkpoint database."""
        self.db = await aiosqlite.connect(self.checkpoint_file)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                depth INTEGER,
                status TEXT,
                processed INTEGER
            )
        """)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS edges (
                src TEXT,
                dst TEXT,
                type TEXT,
                status TEXT
            )
        """)
        # index for quick frontier load
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_pages_processed ON pages(processed)")
        await self.db.commit()

    async def logger(self):
        """Central async logger — prints messages in enqueue order."""
        while True:
            msg = await self.log_queue.get()
            if msg is None:
                break
            print(msg)
            self.log_queue.task_done()

    async def crawl(self):
        # 1) Initialize DB
        await self.init_db()

        # 2) Resume or fresh start
        if self.resume:
            # load unprocessed frontier
            async with self.db.execute("SELECT url, depth FROM pages WHERE processed = 0") as cur:
                rows = await cur.fetchall()
                for url, depth in rows:
                    self.queue.put_nowait((url, depth))
                    self.visited.add(url)
            # rebuild in-memory graph
            async with self.db.execute("SELECT url, status FROM pages") as cur:
                for url, status in await cur.fetchall():
                    self.graph.add_node(url, status=status or "")
            async with self.db.execute("SELECT src, dst, type, status FROM edges") as cur:
                for src, dst, typ, st in await cur.fetchall():
                    self.graph.add_edge(src, dst, type=typ, status=st or "")
            print(f"{Fore.BLUE}[RESUME] Loaded {len(self.visited)} pages and graph edges{Style.RESET_ALL}")
        else:
            # seed queue & pages table
            for url in self.start_urls:
                u = normalize_url(url)
                self.queue.put_nowait((u, 0))
                self.visited.add(u)
                await self.db.execute(
                    "INSERT OR IGNORE INTO pages (url, depth, status, processed) VALUES (?, ?, ?, ?)",
                    (u, 0, "", 0)
                )
            await self.db.commit()

        # 3) Handle graceful shutdown on SIGINT / SIGTERM
        def _on_shutdown(signum, frame):
            asyncio.get_event_loop().create_task(self.db.close())
            exit(0)

        signal.signal(signal.SIGINT, _on_shutdown)
        signal.signal(signal.SIGTERM, _on_shutdown)

        # 4) Start crawling with a real aiohttp session
        connector = aiohttp.TCPConnector(ssl=self.ssl)
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            logger_task = asyncio.create_task(self.logger())
            workers = [
                asyncio.create_task(self.worker(session))
                for _ in range(self.sema._value)
            ]

            await self.queue.join()
            for w in workers:
                w.cancel()

        # signal logger to exit
        await self.log_queue.put(None)
        await logger_task

        # 5) Write final GraphML
        nx.write_graphml(self.graph, self.out_file)
        print(f"{Fore.BLUE}Graph written to {self.out_file} with {self.graph.number_of_nodes()} nodes{Style.RESET_ALL}")

        # 6) Close DB
        await self.db.close()

    async def worker(self, session):
        """Worker coroutine — pulls URLs from the queue and calls fetch()."""
        while True:
            url, depth = await self.queue.get()
            try:
                async with self.sema:
                    await self.fetch(session, url, depth)
            except Exception as e:
                await self.log_queue.put(f"{Fore.RED}[!] Error fetching {url}: {e}")
            finally:
                self.queue.task_done()

    async def fetch(self, session, url, depth):
        """Fetch a single URL, parse links/resources, and enqueue new targets."""
        await asyncio.sleep(self.delay)
        hdrs = {**self.headers, "User-Agent": random.choice(USER_AGENTS)}
        allow_redirects = not self.follow_redirects

        resp = await session.get(url, headers=hdrs, allow_redirects=allow_redirects)
        status = str(resp.status)

        # mark page processed in DB
        await self.db.execute(
            "UPDATE pages SET status = ?, processed = 1 WHERE url = ?",
            (status, url)
        )
        await self.db.commit()

        # add node to in-memory graph
        self.graph.add_node(url, status=status)
        await self.log_queue.put(f"{Fore.GREEN}[+] Fetched {url} ({status})")

        # ── Redirect handling ──────────────────────────────────────────────
        if 300 <= resp.status < 400 and self.follow_redirects:
            loc = resp.headers.get("Location")
            if loc:
                nxt = normalize_url(loc, base=url)
                await self.db.execute(
                    "INSERT OR IGNORE INTO pages (url, depth, status, processed) VALUES (?, ?, ?, ?)",
                    (nxt, depth, status, 0)
                )
                await self.db.execute(
                    "INSERT INTO edges (src, dst, type, status) VALUES (?, ?, ?, ?)",
                    (url, nxt, "redirect", status)
                )
                await self.db.commit()

                self.graph.add_node(nxt, status=status)
                self.graph.add_edge(url, nxt, type="redirect", status=status)
                await self.log_queue.put(f"{Fore.CYAN}[>] Redirect: {url} -> {nxt} ({status})")

                if nxt not in self.visited:
                    self.visited.add(nxt)
                    self.queue.put_nowait((nxt, depth))
            return

        # ── HTML parsing & resource discovery ─────────────────────────────
        if depth < self.max_depth:
            text = await resp.text(errors="ignore")
            soup = BeautifulSoup(text, "html.parser")

            # HTML tags and the attribute that carries their URL
            tags = {
                'a': 'href', 'img': 'src', 'iframe': 'src', 'video': 'src',
                'source': 'src', 'link': 'href', 'script': 'src',
                'embed': 'src', 'audio': 'src'
            }

            for tag, attr in tags.items():
                for el in soup.find_all(tag):
                    url_attr = el.get(attr)
                    if not url_attr:
                        continue

                    path = urlparse(url_attr).path
                    ext = os.path.splitext(path)[1].lower()
                    nxt = normalize_url(url_attr, base=url)
                    dom = extract_domain(nxt)
                    netloc = urlparse(nxt).netloc

                    # persist URL in frontier
                    await self.db.execute(
                        "INSERT OR IGNORE INTO pages (url, depth, status, processed) VALUES (?, ?, ?, ?)",
                        (nxt, depth + 1, "", 0)
                    )
                    await self.db.commit()

                    # ── Static files (any URL with a file extension) ───────
                    if ext:
                        edge_type = "file" if dom in self.allowed_domains and (
                            netloc == dom or netloc.endswith(f".{dom}")
                        ) else "external"
                        self.graph.add_node(nxt, status=edge_type)
                        self.graph.add_edge(url, nxt, type=edge_type)
                        await self.db.execute(
                            "INSERT INTO edges (src, dst, type, status) VALUES (?, ?, ?, ?)",
                            (url, nxt, edge_type, "")
                        )
                        await self.db.commit()
                        color = Fore.YELLOW if edge_type == "file" else Fore.MAGENTA
                        msg = "[~] Found internal file:" if edge_type == "file" else "[!] Found external file:"
                        await self.log_queue.put(f"{color}{msg} {nxt}")
                        continue

                    # ── Internal links ────────────────────────────────────
                    if dom in self.allowed_domains and (
                        netloc == dom or netloc.endswith(f".{dom}")
                    ):
                        edge_type = "link"
                        if nxt not in self.visited:
                            self.visited.add(nxt)
                            self.queue.put_nowait((nxt, depth + 1))
                            await self.log_queue.put(f"{Fore.YELLOW}[~] Found internal link: {nxt}")
                        self.graph.add_edge(url, nxt, type="link")

                    # ── External links ────────────────────────────────────
                    else:
                        edge_type = "external"
                        self.graph.add_node(nxt, status="external")
                        self.graph.add_edge(url, nxt, type="external")
                        await self.log_queue.put(f"{Fore.MAGENTA}[!] Found external link: {nxt}")

                    await self.db.execute(
                        "INSERT INTO edges (src, dst, type, status) VALUES (?, ?, ?, ?)",
                        (url, nxt, edge_type, "")
                    )
                    await self.db.commit()

                    # ── JavaScript file URL extraction ─────────────────────
                    if tag == 'script' and path.lower().endswith('.js'):
                        try:
                            js_resp = await session.get(nxt, headers=hdrs, allow_redirects=allow_redirects)
                            js_text = await js_resp.text(errors='ignore')
                            for js_link in re.findall(r'https?://[^\s\'"<>]+', js_text):
                                js_nxt = normalize_url(js_link)
                                jdom = extract_domain(js_nxt)
                                jnetloc = urlparse(js_nxt).netloc

                                await self.db.execute(
                                    "INSERT OR IGNORE INTO pages (url, depth, status, processed) VALUES (?, ?, ?, ?)",
                                    (js_nxt, depth + 1, "", 0)
                                )
                                await self.db.commit()

                                if jdom in self.allowed_domains and (
                                    jnetloc == jdom or jnetloc.endswith(f".{jdom}")
                                ):
                                    if js_nxt not in self.visited:
                                        self.visited.add(js_nxt)
                                        self.queue.put_nowait((js_nxt, depth + 1))
                                        await self.log_queue.put(
                                            f"{Fore.YELLOW}[~] Found internal JS link: {js_nxt}"
                                        )
                                    edge_type_js = "link"
                                else:
                                    self.graph.add_node(js_nxt, status="external")
                                    self.graph.add_edge(url, js_nxt, type="external")
                                    await self.log_queue.put(
                                        f"{Fore.MAGENTA}[!] Found external JS link: {js_nxt}"
                                    )
                                    edge_type_js = "external"

                                await self.db.execute(
                                    "INSERT INTO edges (src, dst, type, status) VALUES (?, ?, ?, ?)",
                                    (url, js_nxt, edge_type_js, "")
                                )
                                await self.db.commit()
                        except Exception as e:
                            await self.log_queue.put(f"{Fore.RED}[!] Error fetching JS {nxt}: {e}")


def main():
    print(BANNER)

    p = argparse.ArgumentParser(
        description="MTA-Crawler — Async scope-limited web crawler → GraphML",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python crawler.py urls.txt
  python crawler.py urls.txt -d 3 -c 20 --delay 0.5
  python crawler.py urls.txt --follow-redirects -o results.graphml
  python crawler.py urls.txt --resume --checkpoint-file prev.db
        """
    )
    p.add_argument("infile",               help="file with one start URL per line")
    p.add_argument("-d", "--depth",        type=int,   default=2,           help="max crawl depth (default: 2)")
    p.add_argument("-c", "--concurrency",  type=int,   default=10,          help="parallel requests (default: 10)")
    p.add_argument("--delay",             type=float, default=0.0,          help="seconds to wait between requests (default: 0)")
    p.add_argument("-H",                  action="append", metavar="'Key: Value'", default=[],
                                           help="add a custom HTTP header (repeatable)")
    p.add_argument("--no-verify",         action="store_true",              help="disable TLS/SSL certificate verification")
    p.add_argument("--follow-redirects",  action="store_true",              help="record and follow 3xx redirects")
    p.add_argument("-o", "--output",      default="crawl.graphml",          help="output GraphML file (default: crawl.graphml)")
    p.add_argument("--checkpoint-file",   default="crawl.db",               help="SQLite file for persistence (default: crawl.db)")
    p.add_argument("--resume",            action="store_true",              help="resume a previous crawl from the checkpoint file")

    args = p.parse_args()

    start_urls = [
        line.strip() for line in open(args.infile, "r")
        if line.strip() and not line.startswith("#")
    ]

    if not start_urls:
        print(f"{Fore.RED}[!] No URLs found in {args.infile}. Exiting.{Style.RESET_ALL}")
        return

    headers = parse_headers(args.H)

    print(f"{Fore.CYAN}[*] Targets  : {len(start_urls)} URL(s)")
    print(f"[*] Depth    : {args.depth}")
    print(f"[*] Workers  : {args.concurrency}")
    print(f"[*] Delay    : {args.delay}s")
    print(f"[*] Output   : {args.output}")
    print(f"[*] DB       : {args.checkpoint_file}")
    print(f"[*] Resume   : {args.resume}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'─' * 60}{Style.RESET_ALL}\n")

    crawler = Crawler(
        start_urls=start_urls,
        max_depth=args.depth,
        delay=args.delay,
        concurrency=args.concurrency,
        headers=headers,
        no_verify=args.no_verify,
        follow_redirects=args.follow_redirects,
        out_file=args.output,
    )

    crawler.checkpoint_file = args.checkpoint_file
    crawler.resume          = args.resume

    asyncio.run(crawler.crawl())


if __name__ == "__main__":
    main()
