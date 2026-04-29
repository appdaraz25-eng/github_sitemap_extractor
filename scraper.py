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
import subprocess

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Create required directories before logging starts
os.makedirs("logs",   exist_ok=True)
os.makedirs("data",   exist_ok=True)   # .db files live here (committed to repo)

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

# Execution mode: "threadpool" (default) | "asyncio" | "normal"
EXECUTION_MODE = "threadpool"

# Sites file — sits in repo root alongside scraper.py
SITES_FILE = "news_sites_list.txt"

# .db files folder — committed back to the repo after every run
DB_FOLDER = "data"

# Max DB shard size in bytes (50 MB)
MAX_DB_BYTES = 50 * 1024 * 1024

# ThreadPool workers
MAX_WORKERS = 20

# Asyncio concurrency
ASYNCIO_CONCURRENCY = 50

# Batch size for processing URLs
BATCH_SIZE = 50

# Percentage of NEW urls to process per run (100.0 = all new urls)
PROCESS_PERCENTAGE = 20.0

# Save failed URLs to DB so they are never retried
SAVE_FAILED_URLS = True

# Commit and push .db files back to GitHub after each site finishes
AUTO_GIT_PUSH = True

# =============================================================================


# =============================================================================
#  Load sites from news_sites_list.txt
# =============================================================================

def load_sites_from_file(filepath: str) -> List[Dict]:
    # Check if file exists - if not, raise error instead of creating sample
    if not os.path.exists(filepath):
        error_msg = f"ERROR: {filepath} not found. Please create this file with your news site URLs (one per line, lines starting with # are ignored)."
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
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
                # All .db files go inside data/ folder which is committed to repo
                "db_file":            os.path.join(DB_FOLDER, f"{safe}.db"),
                "process_percentage": PROCESS_PERCENTAGE,
            })
    
    if not sites:
        error_msg = f"ERROR: {filepath} exists but contains no valid site URLs. Please add at least one URL (one per line, lines starting with # are ignored)."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"Loaded {len(sites)} site(s) from {filepath}")
    return sites


# =============================================================================
#  Git helpers — commit & push .db files back to the repo
# =============================================================================

def _git_commit_and_push(db_file: str, domain: str):
    """
    Stages the .db file (and any shards) then commits and pushes.
    Works on GitHub Actions because the repo is already checked out
    with write access via GITHUB_TOKEN.
    """
    if not AUTO_GIT_PUSH:
        return
    try:
        # Configure git identity (required on GitHub Actions)
        subprocess.run(
            ["git", "config", "user.email", "actions@github.com"],
            check=True, capture_output=True
        )
        subprocess.run(
            ["git", "config", "user.name", "GitHub Actions"],
            check=True, capture_output=True
        )

        # Stage all .db files in data/ (catches shards like 1site.db, 2site.db …)
        subprocess.run(
            ["git", "add", "data/*.db"],
            check=True, capture_output=True
        )

        # Check if there is anything to commit
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            capture_output=True
        )
        if result.returncode == 0:
            logger.info(f"[git] No changes to commit for {domain}")
            return

        commit_msg = f"chore: update scraped data for {domain} [{datetime.now().strftime('%Y-%m-%d %H:%M')} UTC]"
        subprocess.run(
            ["git", "commit", "-m", commit_msg],
            check=True, capture_output=True
        )

        # Pull first to avoid rejection (other sites may have pushed already)
        subprocess.run(
            ["git", "pull", "--rebase", "origin", "main"],
            check=True, capture_output=True
        )

        subprocess.run(
            ["git", "push", "origin", "main"],
            check=True, capture_output=True
        )
        logger.info(f"[git] Pushed updated .db for {domain}")
    except subprocess.CalledProcessError as e:
        logger.error(f"[git] Push failed for {domain}: {e.stderr.decode()[:300]}")


# =============================================================================
#  DB helpers
# =============================================================================

def _get_active_db(base_db_file: str) -> str:
    """Return the current shard to write to (creates a new one if full)."""
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
    """Return all shard paths that exist for a given base db file."""
    directory = os.path.dirname(base_db_file) or "."
    name, ext = os.path.splitext(os.path.basename(base_db_file))
    files = [base_db_file] if os.path.exists(base_db_file) else []
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
    """Create tables and indexes if they don't exist yet."""
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS news_articles (
            url             TEXT PRIMARY KEY,   -- PRIMARY KEY enforces uniqueness
            title           TEXT,
            published_date  TEXT,
            article_content TEXT,
            scraped_at      TEXT               -- timestamp when row was inserted
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS failed_urls (
            url         TEXT PRIMARY KEY,       -- PRIMARY KEY enforces uniqueness
            reason      TEXT,
            failed_at   TEXT
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_published_date ON news_articles(published_date)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_scraped_at     ON news_articles(scraped_at)')
    conn.commit()
    conn.close()


def _load_all_existing_urls(base_db_file: str) -> Set[str]:
    """
    Load every URL (successful + failed) from ALL shards.
    This is the master deduplication set — any URL already here is skipped.
    """
    existing: Set[str] = set()
    for db_path in _all_db_files(base_db_file):
        try:
            conn = sqlite3.connect(db_path)
            existing.update(
                r[0] for r in conn.execute("SELECT url FROM news_articles").fetchall()
            )
            existing.update(
                r[0] for r in conn.execute("SELECT url FROM failed_urls").fetchall()
            )
            conn.close()
        except Exception as e:
            logger.error(f"Error reading {db_path}: {e}")
    return existing


def _url_exists_in_any_shard(base_db_file: str, url: str) -> bool:
    """Extra safety check — confirm a single URL is not already stored."""
    for db_path in _all_db_files(base_db_file):
        try:
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT 1 FROM news_articles WHERE url=? LIMIT 1", (url,)
            ).fetchone()
            conn.close()
            if row:
                return True
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT 1 FROM failed_urls WHERE url=? LIMIT 1", (url,)
            ).fetchone()
            conn.close()
            if row:
                return True
        except Exception:
            pass
    return False


def _insert_article(base_db_file: str, m: Dict):
    """
    Append a new article row — INSERT OR IGNORE means duplicate URLs are
    silently skipped at the database level (second line of defence after
    the in-memory set check).
    """
    active = _get_active_db(base_db_file)
    os.makedirs(os.path.dirname(active) or ".", exist_ok=True)
    _init_db(active)
    conn = sqlite3.connect(active)
    try:
        conn.execute(
            '''INSERT OR IGNORE INTO news_articles
               (url, title, published_date, article_content, scraped_at)
               VALUES (?, ?, ?, ?, ?)''',
            (
                m['url'],
                (m.get('title') or '')[:500],
                m.get('published_date') or None,
                m['article_content'],
                datetime.utcnow().isoformat()
            )
        )
        conn.commit()
    finally:
        conn.close()


def _insert_failed(base_db_file: str, url: str, reason: str):
    """
    Record a failed URL — INSERT OR IGNORE means it will never be
    duplicated even across multiple runs.
    """
    if not SAVE_FAILED_URLS:
        return
    active = _get_active_db(base_db_file)
    os.makedirs(os.path.dirname(active) or ".", exist_ok=True)
    _init_db(active)
    conn = sqlite3.connect(active)
    try:
        conn.execute(
            'INSERT OR IGNORE INTO failed_urls (url, reason, failed_at) VALUES (?, ?, ?)',
            (url, reason[:500], datetime.utcnow().isoformat())
        )
        conn.commit()
    finally:
        conn.close()


def _db_stats(base_db_file: str) -> Dict:
    """Return total article count and failed count across all shards."""
    total_articles = 0
    total_failed   = 0
    for db_path in _all_db_files(base_db_file):
        try:
            conn = sqlite3.connect(db_path)
            total_articles += conn.execute(
                "SELECT COUNT(*) FROM news_articles"
            ).fetchone()[0]
            total_failed += conn.execute(
                "SELECT COUNT(*) FROM failed_urls"
            ).fetchone()[0]
            conn.close()
        except Exception:
            pass
    return {"articles": total_articles, "failed": total_failed}


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
#  MODE 1: ThreadPool
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
    result = {
        'url': url, 'title': '', 'published_date': '',
        'article_content': '', 'failed': False, 'fail_reason': ''
    }
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
    all_urls:      Set[str]    = set()
    urls_metadata: Dict        = {}
    failed_sitemaps: List[str] = []

    visited, queue = set(), []

    robots_content = _fetch_url_sync(f"{domain}/robots.txt", timeout=10) or ""
    matches = re.findall(r'[Ss]itemap:\s*(https?://\S+)', robots_content)
    if matches:
        queue = list(set(matches))
        logger.info(f"Found {len(queue)} sitemap(s) in robots.txt")
    else:
        queue = [
            f"{domain}/sitemap.xml",       f"{domain}/sitemap_index.xml",
            f"{domain}/sitemap/sitemap.xml", f"{domain}/sitemap/sitemap_index.xml",
            f"{domain}/sitemap-0.xml",     f"{domain}/sitemap1.xml",
            f"{domain}/news_sitemap.xml",  f"{domain}/sitemap_news.xml",
        ]
        logger.info("No sitemaps in robots.txt — trying common locations")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        while queue and len(visited) < 200:
            to_fetch = [u for u in queue if u not in visited]
            queue    = []
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
                logger.info(f"Sitemap OK: {sm_url}")
                urls, subsitemaps, url_meta = _parse_sitemap_xml(content)
                for url in urls:
                    # Only add URLs we have never seen before
                    if url not in existing_urls:
                        all_urls.add(url)
                        if url in url_meta:
                            urls_metadata[url] = url_meta[url]
                queue.extend(s for s in subsitemaps if s not in visited)

    return all_urls, urls_metadata, failed_sitemaps


def run_threadpool(site: Dict):
    domain  = site['domain']
    db_file = site['db_file']
    pct     = site.get('process_percentage', 100.0)

    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    _init_db(db_file)

    # Load ALL previously seen URLs (success + failed) for deduplication
    existing_urls = _load_all_existing_urls(db_file)
    stats_before  = _db_stats(db_file)
    logger.info(f"[{domain}] DB has {stats_before['articles']} articles, "
                f"{stats_before['failed']} failed URLs before this run")

    all_urls, urls_metadata, failed_sitemaps = _crawl_sitemaps_threadpool(
        domain, existing_urls
    )

    for sm in failed_sitemaps:
        _insert_failed(db_file, sm, "Sitemap fetch failed")

    # new_urls = sitemap URLs minus everything already in DB
    new_urls = [u for u in all_urls if u not in existing_urls]
    logger.info(f"[{domain}] {len(new_urls)} new URLs found (not in DB yet)")

    if not new_urls:
        logger.info(f"[{domain}] Nothing new to scrape — skipping.")
        _git_commit_and_push(db_file, domain)
        return

    if pct < 100.0:
        count    = max(1, int(pct / 100.0 * len(new_urls)))
        new_urls = random.sample(new_urls, count)
        logger.info(f"[{domain}] Sampling {pct}% → {count} URLs")

    saved = failed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_extract_page_sync, url, urls_metadata.get(url, {})): url
            for url in new_urls
        }
        done = 0
        for future in concurrent.futures.as_completed(futures):
            done  += 1
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
                logger.info(f"[{domain}] Progress {done}/{len(new_urls)} "
                            f"— saved {saved}, failed {failed}")

    _log_summary(domain, db_file, saved, failed)
    _git_commit_and_push(db_file, domain)


# =============================================================================
#  MODE 2: Asyncio
# =============================================================================

async def _fetch_text_async(session, sem, url: str, timeout: int = 15) -> Optional[str]:
    try:
        async with sem:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=timeout), headers=HEADERS
            ) as resp:
                if resp.status == 200:
                    return await resp.text(errors='replace')
                logger.warning(f"HTTP {resp.status}: {url}")
    except Exception as e:
        logger.warning(f"Fetch error {url}: {e}")
    return None


async def _extract_page_async(session, sem, url: str, meta: Dict) -> Dict:
    result = {
        'url': url, 'title': '', 'published_date': '',
        'article_content': '', 'failed': False, 'fail_reason': ''
    }
    try:
        async with sem:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10), headers=HEADERS
            ) as resp:
                if resp.status == 200:
                    soup = BeautifulSoup(
                        await resp.text(errors='replace'), 'html.parser'
                    )
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

    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    _init_db(db_file)

    existing_urls = _load_all_existing_urls(db_file)
    stats_before  = _db_stats(db_file)
    logger.info(f"[{domain}] DB has {stats_before['articles']} articles before this run")

    sem = asyncio.Semaphore(ASYNCIO_CONCURRENCY)
    all_urls:        Set[str]    = set()
    urls_metadata:   Dict        = {}
    failed_sitemaps: List[str]   = []

    async with aiohttp.ClientSession() as session:
        robots = await _fetch_text_async(
            session, sem, f"{domain}/robots.txt", timeout=10
        ) or ""
        matches = re.findall(r'[Ss]itemap:\s*(https?://\S+)', robots)
        queue   = list(set(matches)) if matches else [
            f"{domain}/sitemap.xml",        f"{domain}/sitemap_index.xml",
            f"{domain}/sitemap/sitemap.xml", f"{domain}/sitemap/sitemap_index.xml",
            f"{domain}/sitemap-0.xml",      f"{domain}/sitemap1.xml",
            f"{domain}/news_sitemap.xml",   f"{domain}/sitemap_news.xml",
        ]

        visited = set()
        while queue and len(visited) < 200:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            content = await _fetch_text_async(session, sem, current)
            if not content:
                failed_sitemaps.append(current)
                continue
            logger.info(f"Sitemap OK: {current}")
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
        logger.info(f"[{domain}] {len(new_urls)} new URLs to process")

        if not new_urls:
            logger.info(f"[{domain}] Nothing new — skipping.")
            _git_commit_and_push(db_file, domain)
            return

        if pct < 100.0:
            count    = max(1, int(pct / 100.0 * len(new_urls)))
            new_urls = random.sample(new_urls, count)

        saved = failed = 0
        for i in range(0, len(new_urls), BATCH_SIZE):
            batch   = new_urls[i:i + BATCH_SIZE]
            results = await asyncio.gather(
                *[_extract_page_async(
                    session, sem, url, urls_metadata.get(url, {})
                ) for url in batch],
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
            logger.info(
                f"[{domain}] Progress {min(i+BATCH_SIZE, len(new_urls))}/{len(new_urls)}"
            )

    _log_summary(domain, db_file, saved, failed)
    _git_commit_and_push(db_file, domain)


# =============================================================================
#  MODE 3: Normal (single-threaded)
# =============================================================================

def run_normal(site: Dict):
    domain  = site['domain']
    db_file = site['db_file']
    pct     = site.get('process_percentage', 100.0)

    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    _init_db(db_file)

    existing_urls = _load_all_existing_urls(db_file)
    stats_before  = _db_stats(db_file)
    logger.info(f"[{domain}] DB has {stats_before['articles']} articles before this run")

    all_urls:        Set[str]    = set()
    urls_metadata:   Dict        = {}
    failed_sitemaps: List[str]   = []

    robots = _fetch_url_sync(f"{domain}/robots.txt", timeout=10) or ""
    matches = re.findall(r'[Ss]itemap:\s*(https?://\S+)', robots)
    queue   = list(set(matches)) if matches else [
        f"{domain}/sitemap.xml",        f"{domain}/sitemap_index.xml",
        f"{domain}/sitemap/sitemap.xml", f"{domain}/sitemap/sitemap_index.xml",
        f"{domain}/sitemap-0.xml",      f"{domain}/sitemap1.xml",
        f"{domain}/news_sitemap.xml",   f"{domain}/sitemap_news.xml",
    ]

    visited = set()
    while queue and len(visited) < 200:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        content = _fetch_url_sync(current)
        if not content:
            failed_sitemaps.append(current)
            continue
        logger.info(f"Sitemap OK: {current}")
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
    logger.info(f"[{domain}] {len(new_urls)} new URLs to process")

    if not new_urls:
        logger.info(f"[{domain}] Nothing new — skipping.")
        _git_commit_and_push(db_file, domain)
        return

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
            logger.info(f"[{domain}] Progress {i}/{len(new_urls)}")

    _log_summary(domain, db_file, saved, failed)
    _git_commit_and_push(db_file, domain)


# =============================================================================
#  Summary logger
# =============================================================================

def _log_summary(domain: str, base_db_file: str, saved: int, failed: int):
    stats = _db_stats(base_db_file)
    logger.info(f"[{domain}] Run complete — +{saved} new articles, {failed} failed")
    logger.info(f"[{domain}] DB totals — {stats['articles']} articles, "
                f"{stats['failed']} failed URLs across all shards")
    for db_path in _all_db_files(base_db_file):
        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        logger.info(f"  Shard: {db_path}  ({size_mb:.2f} MB)")


# =============================================================================
#  Main
# =============================================================================

def main():
    try:
        sites = load_sites_from_file(SITES_FILE)
    except (FileNotFoundError, ValueError) as e:
        logger.error(str(e))
        return

    logger.info("=" * 60)
    logger.info(f"Started        : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info(f"Mode           : {EXECUTION_MODE}")
    logger.info(f"Sites          : {len(sites)}")
    logger.info(f"DB folder      : {DB_FOLDER}  (committed to repo)")
    logger.info(f"Max DB shard   : {MAX_DB_BYTES // (1024*1024)} MB")
    logger.info(f"Auto git push  : {AUTO_GIT_PUSH}")
    logger.info(f"Save failed    : {SAVE_FAILED_URLS}")
    logger.info("=" * 60)

    for site in sites:
        try:
            if EXECUTION_MODE == "asyncio":
                asyncio.run(run_asyncio(site))
            elif EXECUTION_MODE == "normal":
                run_normal(site)
            else:
                run_threadpool(site)
        except Exception as e:
            logger.error(f"Failed processing {site['domain']}: {e}")

    logger.info("All sites done.")


if __name__ == "__main__":
    main()
