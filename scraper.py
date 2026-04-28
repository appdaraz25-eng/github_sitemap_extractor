import asyncio
import aiohttp
import xml.etree.ElementTree as ET
import sqlite3
import random
import os
import logging
import re
import time
import requests
import concurrent.futures
from typing import List, Set, Dict, Optional
from datetime import datetime
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from dateutil import parser as date_parser
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Create required directories before logging starts
os.makedirs("logs",   exist_ok=True)
os.makedirs("output", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/extractor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
#  CONFIGURATION
# =============================================================================

# Execution mode: "threadpool" (default) | "threadpool" | "normal"
EXECUTION_MODE = "asyncio"

# Sites are loaded from this file (one URL per line, lines starting with # ignored)
SITES_FILE = SITES_FILE = "output/newsstes.txt"

# Max DB shard size in bytes (100 MB)
MAX_DB_BYTES = 100 * 1024 * 1024

# ThreadPool workers (used when EXECUTION_MODE = "threadpool")
MAX_WORKERS = 30

# Asyncio concurrency (used when EXECUTION_MODE = "asyncio")
ASYNCIO_CONCURRENCY = 50

# Batch size for processing URLs
BATCH_SIZE = 50

# Percentage of URLs to process per site (100.0 = all)
PROCESS_PERCENTAGE = 1.0

# Save failed URLs to DB? True | False
SAVE_FAILED_URLS = True

# =============================================================================


# =============================================================================
#  Load sites from newssites.txt
# =============================================================================

def load_sites_from_file(filepath: str) -> List[Dict]:
    if not os.path.exists(filepath):
        # Create a sample file so the user knows what to do
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("# Add one news site URL per line\n")
            f.write("# Lines starting with # are ignored\n")
            f.write("# Example:\n")
            f.write("# https://prothomalo.com\n")
            f.write("# https://bdnews24.com\n")
            f.write("https://prothomalo.com\n")
            f.write("https://bdnews24.com\n")
        logger.info(f"Created sample {filepath} — edit it and re-run.")

    sites = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            domain = line.rstrip('/')
            safe   = re.sub(r'[^a-zA-Z0-9._-]',
                            '_',
                            domain.replace('https://', '').replace('http://', ''))
            sites.append({
                "domain":             domain,
                "db_file":            f"output/{safe}.db",
                "process_percentage": PROCESS_PERCENTAGE,
            })
    logger.info(f"Loaded {len(sites)} site(s) from {filepath}")
    return sites


# =============================================================================
#  DB helpers
# =============================================================================

def _get_active_db(base_db_file: str) -> str:
    if not os.path.exists(base_db_file) or os.path.getsize(base_db_file) < MAX_DB_BYTES:
        return base_db_file
    directory = os.path.dirname(base_db_file) or "."
    name, ext = os.path.splitext(os.path.basename(base_db_file))
    idx = 1
    while True:
        candidate = os.path.join(directory, f"{idx}{name}{ext}")
        if not os.path.exists(candidate) or os.path.getsize(candidate) < MAX_DB_BYTES:
            return candidate
        idx += 1


def _all_db_files(base_db_file: str) -> List[str]:
    directory = os.path.dirname(base_db_file) or "."
    name, ext = os.path.splitext(os.path.basename(base_db_file))
    files = [f for f in [base_db_file] if os.path.exists(f)]
    idx = 1
    while True:
        candidate = os.path.join(directory, f"{idx}{name}{ext}")
        if os.path.exists(candidate):
            files.append(candidate)
            idx += 1
        else:
            break
    return files


def _init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS news_articles (
            url             TEXT PRIMARY KEY,
            title           TEXT,
            published_date  TEXT,
            article_content TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS failed_urls (
            url         TEXT PRIMARY KEY,
            reason      TEXT,
            failed_at   TEXT
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_published_date ON news_articles(published_date)')
    conn.commit()
    conn.close()


def _load_all_existing_urls(base_db_file: str) -> Set[str]:
    existing = set()
    for db_path in _all_db_files(base_db_file):
        try:
            conn = sqlite3.connect(db_path)
            existing.update(r[0] for r in conn.execute("SELECT url FROM news_articles").fetchall())
            existing.update(r[0] for r in conn.execute("SELECT url FROM failed_urls").fetchall())
            conn.close()
        except Exception as e:
            logger.error(f"Error reading {db_path}: {e}")
    return existing


def _insert_article(base_db_file: str, m: Dict):
    active = _get_active_db(base_db_file)
    os.makedirs(os.path.dirname(active) or ".", exist_ok=True)
    _init_db(active)
    conn = sqlite3.connect(active)
    try:
        conn.execute(
            'INSERT OR IGNORE INTO news_articles (url, title, published_date, article_content) VALUES (?, ?, ?, ?)',
            (m['url'], (m.get('title') or '')[:500], m.get('published_date') or None, m['article_content'])
        )
        conn.commit()
    finally:
        conn.close()


def _insert_failed(base_db_file: str, url: str, reason: str):
    if not SAVE_FAILED_URLS:
        return
    active = _get_active_db(base_db_file)
    os.makedirs(os.path.dirname(active) or ".", exist_ok=True)
    _init_db(active)
    conn = sqlite3.connect(active)
    try:
        conn.execute(
            'INSERT OR IGNORE INTO failed_urls (url, reason, failed_at) VALUES (?, ?, ?)',
            (url, reason[:500], datetime.now().isoformat())
        )
        conn.commit()
    finally:
        conn.close()


# =============================================================================
#  Shared parsing utilities (mode-agnostic)
# =============================================================================

def _date_from_url(url: str) -> Optional[str]:
    for pattern in [
        r'/(\d{4})/(\d{1,2})/(\d{1,2})/',
        r'/(\d{4})-(\d{1,2})-(\d{1,2})/',
        r'/(\d{4})(\d{2})(\d{2})/',
    ]:
        m = re.search(pattern, url)
        if m:
            try:
                y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if 1900 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
                    return datetime(y, mo, d).strftime("%Y-%m-%d")
            except Exception:
                pass
    return None


def _parse_date_str(s: str):
    try:
        return date_parser.parse(s)
    except Exception:
        return None


def _parse_sitemap_xml(content: str):
    urls, subsitemaps, metadata = set(), set(), {}
    NS = {
        'ns':   'http://www.sitemaps.org/schemas/sitemap/0.9',
        'news': 'http://www.google.com/schemas/sitemap-news/0.9',
    }
    try:
        root = ET.fromstring(content)
        if root.find('.//ns:sitemap', NS) is not None:
            for elem in root.findall('.//ns:sitemap', NS):
                loc = elem.find('ns:loc', NS)
                if loc is not None and loc.text:
                    subsitemaps.add(loc.text.strip())
        else:
            for url_elem in root.findall('.//ns:url', NS):
                loc = url_elem.find('ns:loc', NS)
                if loc is None or not loc.text:
                    continue
                url = loc.text.strip()
                urls.add(url)
                meta = {}
                for tag, key in [
                    ('.//news:publication_date', 'news_date'),
                    ('.//news:title',            'news_title'),
                    ('ns:lastmod',               'lastmod'),
                ]:
                    el = url_elem.find(tag, NS)
                    if el is not None and el.text:
                        meta[key] = el.text.strip()
                d = _date_from_url(url)
                if d:
                    meta['url_date'] = d
                metadata[url] = meta
    except ET.ParseError as e:
        logger.error(f"XML parse error: {e}")
    return urls, subsitemaps, metadata


def _extract_title_from_soup(soup: BeautifulSoup, meta: Dict) -> str:
    if meta.get('news_title'):
        return meta['news_title']
    for tag in (soup.find('h1'), soup.find('title')):
        if tag:
            text = tag.get_text(strip=True)
            if text:
                return text[:500]
    return ""


def _extract_content_from_soup(soup: BeautifulSoup) -> str:
    paragraphs = [
        p.get_text(strip=True)
        for p in soup.find_all('p')
        if len(p.get_text(strip=True)) > 20
    ]
    if not paragraphs:
        return ""
    text = '\n\n'.join(paragraphs)
    return text[:10000] + "..." if len(text) > 10000 else text


def _best_date(url: str, meta: Dict, soup) -> str:
    best = None
    for key in ('news_date', 'url_date', 'lastmod'):
        if meta.get(key):
            p = _parse_date_str(meta[key])
            if p and p.year > 2000:
                best = p
                break
    if not best:
        for sel, attr in [
            ('meta[property="article:published_time"]', 'content'),
            ('meta[name="article:published_time"]',    'content'),
            ('meta[name="date"]',                      'content'),
            ('time[datetime]',                         'datetime'),
            ('[itemprop="datePublished"]',             'content'),
        ]:
            for el in soup.select(sel):
                val = el.get(attr, '')
                if val:
                    p = _parse_date_str(val)
                    if p and p.year > 2000:
                        best = p
                        break
            if best:
                break
    return best.strftime("%d %b %Y, %I:%M %p") if best else ""


HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


# =============================================================================
#  MODE 1: ThreadPool (default — fastest for CPU-bound I/O on Windows)
# =============================================================================

def _fetch_url_sync(url: str, timeout: int = 15) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, headers=HEADERS)
        if r.status_code == 200:
            return r.text
        logger.warning(f"HTTP {r.status_code}: {url}")
    except Exception as e:
        logger.warning(f"Fetch error {url}: {e}")
    return None


def _extract_page_sync(url: str, meta: Dict) -> Dict:
    result = {'url': url, 'title': '', 'published_date': '', 'article_content': '', 'failed': False, 'fail_reason': ''}
    try:
        r = requests.get(url, timeout=10, headers=HEADERS)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            result['title']           = _extract_title_from_soup(soup, meta)
            result['article_content'] = _extract_content_from_soup(soup)
            result['published_date']  = _best_date(url, meta, soup)
            if not result['title'] and not result['article_content']:
                result['failed']      = True
                result['fail_reason'] = 'No title or content extracted'
        else:
            result['failed']      = True
            result['fail_reason'] = f"HTTP {r.status_code}"
    except Exception as e:
        result['failed']      = True
        result['fail_reason'] = str(e)[:300]
    return result


def _crawl_sitemaps_threadpool(domain: str, existing_urls: Set[str]) -> tuple:
    all_urls: Set[str] = set()
    urls_metadata: Dict[str, Dict] = {}
    failed_sitemaps: List[str] = []

    visited, queue = set(), []

    # Step 1: get robots.txt
    robots_content = _fetch_url_sync(f"{domain}/robots.txt", timeout=10) or ""
    matches = re.findall(r'[Ss]itemap:\s*(https?://\S+)', robots_content)
    if matches:
        queue = list(set(matches))
        logger.info(f"Found {len(queue)} sitemap(s) in robots.txt")
    else:
        queue = [
            f"{domain}/sitemap.xml", f"{domain}/sitemap_index.xml",
            f"{domain}/sitemap/sitemap.xml", f"{domain}/sitemap/sitemap_index.xml",
            f"{domain}/sitemap-0.xml", f"{domain}/sitemap1.xml",
            f"{domain}/news_sitemap.xml", f"{domain}/sitemap_news.xml",
        ]
        logger.info("No sitemaps in robots.txt — trying common locations")

    # Step 2: crawl sitemap tree
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        while queue and len(visited) < 200:
            to_fetch = [u for u in queue if u not in visited]
            queue = []
            if not to_fetch:
                break

            futures = {pool.submit(_fetch_url_sync, u): u for u in to_fetch}
            for future, sm_url in futures.items():
                visited.add(sm_url)
                content = future.result()
                if not content:
                    failed_sitemaps.append(sm_url)
                    logger.warning(f"Failed sitemap: {sm_url}")
                    continue
                logger.info(f"Sitemap: {sm_url}")
                urls, subsitemaps, url_meta = _parse_sitemap_xml(content)
                for url in urls:
                    if url not in existing_urls:
                        all_urls.add(url)
                        if url in url_meta:
                            urls_metadata[url] = url_meta[url]
                queue.extend(s for s in subsitemaps if s not in visited)

    return all_urls, urls_metadata, failed_sitemaps


def run_threadpool(site: Dict):
    domain      = site['domain']
    db_file     = site['db_file']
    pct         = site.get('process_percentage', 100.0)

    os.makedirs(os.path.dirname(db_file) or ".", exist_ok=True)
    _init_db(db_file)

    existing_urls = _load_all_existing_urls(db_file)
    logger.info(f"[threadpool][{domain}] {len(existing_urls)} existing URLs")

    all_urls, urls_metadata, failed_sitemaps = _crawl_sitemaps_threadpool(domain, existing_urls)

    # Save failed sitemaps
    for sm in failed_sitemaps:
        _insert_failed(db_file, sm, "Sitemap fetch failed")

    new_urls = [u for u in all_urls if u not in existing_urls]
    logger.info(f"[threadpool][{domain}] {len(new_urls)} new URLs to process")

    if pct < 100.0:
        count    = max(1, int(pct / 100.0 * len(new_urls)))
        new_urls = random.sample(new_urls, count)
        logger.info(f"[threadpool][{domain}] Sampling {pct}% -> {count} URLs")

    saved = failed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_extract_page_sync, url, urls_metadata.get(url, {})): url
            for url in new_urls
        }
        done = 0
        for future in concurrent.futures.as_completed(futures):
            done += 1
            result = future.result()
            if result['failed']:
                failed += 1
                _insert_failed(db_file, result['url'], result['fail_reason'])
            elif result['article_content']:
                _insert_article(db_file, result)
                saved += 1
            else:
                failed += 1
                _insert_failed(db_file, result['url'], 'Empty article content')
            if done % BATCH_SIZE == 0 or done == len(new_urls):
                logger.info(f"[threadpool][{domain}] Progress: {done}/{len(new_urls)}")

    _log_summary(domain, db_file, saved, failed)


# =============================================================================
#  MODE 2: Asyncio
# =============================================================================

async def _fetch_text_async(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                             url: str, timeout: int = 15) -> Optional[str]:
    try:
        async with sem:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                                   headers=HEADERS) as resp:
                if resp.status == 200:
                    return await resp.text(errors='replace')
                logger.warning(f"HTTP {resp.status}: {url}")
    except Exception as e:
        logger.warning(f"Fetch error {url}: {e}")
    return None


async def _extract_page_async(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                               url: str, meta: Dict) -> Dict:
    result = {'url': url, 'title': '', 'published_date': '', 'article_content': '',
              'failed': False, 'fail_reason': ''}
    try:
        async with sem:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10),
                                   headers=HEADERS) as resp:
                if resp.status == 200:
                    soup = BeautifulSoup(await resp.text(errors='replace'), 'html.parser')
                    result['title']           = _extract_title_from_soup(soup, meta)
                    result['article_content'] = _extract_content_from_soup(soup)
                    result['published_date']  = _best_date(url, meta, soup)
                    if not result['title'] and not result['article_content']:
                        result['failed']      = True
                        result['fail_reason'] = 'No title or content extracted'
                else:
                    result['failed']      = True
                    result['fail_reason'] = f"HTTP {resp.status}"
    except Exception as e:
        result['failed']      = True
        result['fail_reason'] = str(e)[:300]
    return result


async def run_asyncio(site: Dict):
    domain  = site['domain']
    db_file = site['db_file']
    pct     = site.get('process_percentage', 100.0)

    os.makedirs(os.path.dirname(db_file) or ".", exist_ok=True)
    _init_db(db_file)

    existing_urls = _load_all_existing_urls(db_file)
    logger.info(f"[asyncio][{domain}] {len(existing_urls)} existing URLs")

    sem = asyncio.Semaphore(ASYNCIO_CONCURRENCY)
    all_urls: Set[str]       = set()
    urls_metadata: Dict      = {}
    failed_sitemaps: List[str] = []

    async with aiohttp.ClientSession() as session:
        # Robots
        robots = await _fetch_text_async(session, sem, f"{domain}/robots.txt", timeout=10) or ""
        matches = re.findall(r'[Ss]itemap:\s*(https?://\S+)', robots)
        queue = list(set(matches)) if matches else [
            f"{domain}/sitemap.xml", f"{domain}/sitemap_index.xml",
            f"{domain}/sitemap/sitemap.xml", f"{domain}/sitemap/sitemap_index.xml",
            f"{domain}/sitemap-0.xml", f"{domain}/sitemap1.xml",
            f"{domain}/news_sitemap.xml", f"{domain}/sitemap_news.xml",
        ]

        # Sitemap tree
        visited = set()
        while queue and len(visited) < 200:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            logger.info(f"Sitemap: {current}")
            content = await _fetch_text_async(session, sem, current)
            if not content:
                failed_sitemaps.append(current)
                continue
            urls, subsitemaps, url_meta = _parse_sitemap_xml(content)
            for url in urls:
                if url not in existing_urls:
                    all_urls.add(url)
                    if url in url_meta:
                        urls_metadata[url] = url_meta[url]
            queue.extend(s for s in subsitemaps if s not in visited)

        for sm in failed_sitemaps:
            _insert_failed(db_file, sm, "Sitemap fetch failed")

        new_urls = [u for u in all_urls if u not in existing_urls]
        logger.info(f"[asyncio][{domain}] {len(new_urls)} new URLs to process")

        if pct < 100.0:
            count    = max(1, int(pct / 100.0 * len(new_urls)))
            new_urls = random.sample(new_urls, count)

        saved = failed = 0
        for i in range(0, len(new_urls), BATCH_SIZE):
            batch = new_urls[i:i + BATCH_SIZE]
            results = await asyncio.gather(
                *[_extract_page_async(session, sem, url, urls_metadata.get(url, {})) for url in batch],
                return_exceptions=True
            )
            for res in results:
                if isinstance(res, dict):
                    if res['failed']:
                        failed += 1
                        _insert_failed(db_file, res['url'], res['fail_reason'])
                    elif res['article_content']:
                        _insert_article(db_file, res)
                        saved += 1
                    else:
                        failed += 1
                        _insert_failed(db_file, res['url'], 'Empty article content')
            logger.info(f"[asyncio][{domain}] Progress: {min(i+BATCH_SIZE, len(new_urls))}/{len(new_urls)}")

    _log_summary(domain, db_file, saved, failed)


# =============================================================================
#  MODE 3: Normal (simple single-threaded requests)
# =============================================================================

def run_normal(site: Dict):
    domain  = site['domain']
    db_file = site['db_file']
    pct     = site.get('process_percentage', 100.0)

    os.makedirs(os.path.dirname(db_file) or ".", exist_ok=True)
    _init_db(db_file)

    existing_urls = _load_all_existing_urls(db_file)
    logger.info(f"[normal][{domain}] {len(existing_urls)} existing URLs")

    all_urls: Set[str]         = set()
    urls_metadata: Dict        = {}
    failed_sitemaps: List[str] = []

    # Robots
    robots = _fetch_url_sync(f"{domain}/robots.txt", timeout=10) or ""
    matches = re.findall(r'[Ss]itemap:\s*(https?://\S+)', robots)
    queue = list(set(matches)) if matches else [
        f"{domain}/sitemap.xml", f"{domain}/sitemap_index.xml",
        f"{domain}/sitemap/sitemap.xml", f"{domain}/sitemap/sitemap_index.xml",
        f"{domain}/sitemap-0.xml", f"{domain}/sitemap1.xml",
        f"{domain}/news_sitemap.xml", f"{domain}/sitemap_news.xml",
    ]

    visited = set()
    while queue and len(visited) < 200:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        logger.info(f"Sitemap: {current}")
        content = _fetch_url_sync(current)
        if not content:
            failed_sitemaps.append(current)
            continue
        urls, subsitemaps, url_meta = _parse_sitemap_xml(content)
        for url in urls:
            if url not in existing_urls:
                all_urls.add(url)
                if url in url_meta:
                    urls_metadata[url] = url_meta[url]
        queue.extend(s for s in subsitemaps if s not in visited)

    for sm in failed_sitemaps:
        _insert_failed(db_file, sm, "Sitemap fetch failed")

    new_urls = [u for u in all_urls if u not in existing_urls]
    logger.info(f"[normal][{domain}] {len(new_urls)} new URLs to process")

    if pct < 100.0:
        count    = max(1, int(pct / 100.0 * len(new_urls)))
        new_urls = random.sample(new_urls, count)

    saved = failed = 0
    for i, url in enumerate(new_urls, 1):
        result = _extract_page_sync(url, urls_metadata.get(url, {}))
        if result['failed']:
            failed += 1
            _insert_failed(db_file, result['url'], result['fail_reason'])
        elif result['article_content']:
            _insert_article(db_file, result)
            saved += 1
        else:
            failed += 1
            _insert_failed(db_file, result['url'], 'Empty article content')
        if i % BATCH_SIZE == 0 or i == len(new_urls):
            logger.info(f"[normal][{domain}] Progress: {i}/{len(new_urls)}")

    _log_summary(domain, db_file, saved, failed)


# =============================================================================
#  Shared summary logger
# =============================================================================

def _log_summary(domain: str, base_db_file: str, saved: int, failed: int):
    logger.info(f"[{domain}] Done — saved: {saved}, failed: {failed}")
    for db_path in _all_db_files(base_db_file):
        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        logger.info(f"  Shard: {db_path}  ({size_mb:.1f} MB)")


# =============================================================================
#  Main
# =============================================================================

def main():
    sites = load_sites_from_file(SITES_FILE)
    if not sites:
        logger.error(f"No sites found in {SITES_FILE}. Add URLs and re-run.")
        return

    logger.info("=" * 60)
    logger.info(f"Started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Mode     : {EXECUTION_MODE}")
    logger.info(f"Sites    : {len(sites)}")
    logger.info(f"Max DB   : {MAX_DB_BYTES // (1024*1024)} MB per shard")
    logger.info(f"Save failed URLs: {SAVE_FAILED_URLS}")
    logger.info("=" * 60)

    for site in sites:
        try:
            if EXECUTION_MODE == "asyncio":
                asyncio.run(run_asyncio(site))
            elif EXECUTION_MODE == "normal":
                run_normal(site)
            else:  # threadpool (default)
                run_threadpool(site)
        except Exception as e:
            logger.error(f"Failed: {site['domain']}: {e}")

    logger.info("All sites done.")


if __name__ == "__main__":
    main()
