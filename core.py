import os
import time
import json
import html
import sqlite3
import datetime as dt
import hashlib
import re
import logging
import threading
from collections import defaultdict, deque, namedtuple
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl, urlencode as encode_query

import requests
import feedparser
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit

from scrapers import split_pipe_specs

load_dotenv()

# =========================
# Logging
# =========================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").strip().upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("frontier_deeptech_bot")

# =========================
# Environment helpers
# =========================


def get_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def get_float(name, default):
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def get_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ["1", "true", "yes", "y", "on"]


def get_csv(name, default_values=None):
    raw = os.getenv(name, "")
    if not raw.strip():
        return list(default_values or [])
    for separator in ["|", ";"]:
        raw = raw.replace(separator, ",")
    return [part.strip() for part in raw.split(",") if part.strip()]


def get_feed_specs(name, default_specs=None):
    """Parse feed/page specs from .env.
    Format:
    SOURCE_NAME=https://feed.url|OTHER_SOURCE=https://other.url

    The pipe separator is used because RSS URLs can contain commas or query strings.
    """
    raw = os.getenv(name, "")
    if not raw.strip():
        return list(default_specs or [])
    specs = []
    for part in split_pipe_specs(raw):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            label, url = part.split("=", 1)
        elif "::" in part:
            label, url = part.split("::", 1)
        else:
            label, url = part, part
        label = clean_text(label) if "clean_text" in globals() else label.strip()
        url = url.strip()
        if label and url:
            specs.append((label, url))
    return specs or list(default_specs or [])


# =========================
# Configuration
# =========================

DB_PATH = os.getenv("DB_PATH", "/app/data/frontier_deeptech_trends.db").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openrouter").strip().lower()
LLM_FALLBACK_PROVIDER = os.getenv("LLM_FALLBACK_PROVIDER", "").strip().lower()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-v4-flash").strip()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite").strip()

# Safer default: fetch every 10 minutes, not every minute.
FETCH_MINUTES = get_int("FETCH_MINUTES", 10)
DIGEST_MINUTES = get_int("DIGEST_MINUTES", 360)

# Caps are intentionally broad enough to capture the latest items from each source
# over a 10-minute window, while preventing unlimited token/API pressure.
MAX_CANDIDATES_PER_RUN = get_int("MAX_CANDIDATES_PER_RUN", 180)
MAX_CANDIDATES_PER_FLUSH = get_int("MAX_CANDIDATES_PER_FLUSH", 96)
CANDIDATE_QUEUE_MAX = get_int("CANDIDATE_QUEUE_MAX", 360)

LLM_BATCH_SIZE = max(1, get_int("LLM_BATCH_SIZE", 8))
LLM_MIN_SCORE = get_float("LLM_MIN_SCORE", 7.0)
ALERT_THRESHOLD = get_float("ALERT_THRESHOLD", 8.5)

ARXIV_RESULTS_PER_CATEGORY = get_int("ARXIV_RESULTS_PER_CATEGORY", 6)
HN_NEW_STORIES_PER_RUN = get_int("HN_NEW_STORIES_PER_RUN", 80)
TECHMEME_MAX_ITEMS = get_int("TECHMEME_MAX_ITEMS", 40)

MAX_ALERTS_PER_FLUSH = get_int("MAX_ALERTS_PER_FLUSH", 8)
DIGEST_MAX_ITEMS = get_int("DIGEST_MAX_ITEMS", 20)
SEND_EMPTY_DIGEST = get_bool("SEND_EMPTY_DIGEST", False)
INCLUDE_ALERTS_IN_DIGEST = get_bool("INCLUDE_ALERTS_IN_DIGEST", False)
SEND_RAW_FETCH_REPORT = get_bool("SEND_RAW_FETCH_REPORT", False)

ENABLE_ARXIV = get_bool("ENABLE_ARXIV", True)
ENABLE_HACKERNEWS = get_bool("ENABLE_HACKERNEWS", True)
ENABLE_TECHMEME = get_bool("ENABLE_TECHMEME", True)

DEBUG_LLM = get_bool("DEBUG_LLM", False)
SEND_STARTUP_MESSAGE = get_bool("SEND_STARTUP_MESSAGE", True)

QUEUE_FLUSH_LIMIT = get_int("QUEUE_FLUSH_LIMIT", 32)
CHECK_QUEUE_SECONDS = get_int("CHECK_QUEUE_SECONDS", 60)
FORCE_FLUSH_MINUTES = get_int("FORCE_FLUSH_MINUTES", 30)

MAX_ITEM_RETRIES = get_int("MAX_ITEM_RETRIES", 5)

FEED_FAILURE_THRESHOLD = get_int("FEED_FAILURE_THRESHOLD", 4)
FEED_COOLDOWN_MINUTES = get_int("FEED_COOLDOWN_MINUTES", 180)

HN_FETCH_WORKERS = get_int("HN_FETCH_WORKERS", 8)
ARXIV_FETCH_WORKERS = get_int("ARXIV_FETCH_WORKERS", 4)

HEALTHCHECK_FILE = os.getenv("HEALTHCHECK_FILE", "/app/data/heartbeat.txt").strip()

LLM_TEXT_FIELD_MAX_CHARS = get_int("LLM_TEXT_FIELD_MAX_CHARS", 500)

DEFAULT_ARXIV_CATEGORIES = [
    "q-bio.GN", "q-bio.BM", "q-bio.NC", "q-bio.TO", "q-bio.QM",
    "cs.RO", "cs.CR", "cs.MA", "cs.AI", "cs.CL", "cs.LG", "cs.LO",
    "physics.space-ph", "astro-ph.IM", "physics.ao-ph", "physics.flu-dyn",
    "physics.plasm-ph", "cond-mat.mtrl-sci", "cond-mat.mes-hall", "physics.optics", "physics.app-ph",
]
ARXIV_CATEGORIES = get_csv("ARXIV_CATEGORIES", DEFAULT_ARXIV_CATEGORIES)

SEND_RAW_FETCH_REPORT = get_bool("SEND_RAW_FETCH_REPORT", False)
SEND_RAW_FETCH_JSON = get_bool("SEND_RAW_FETCH_JSON", False)


# =========================
# Expanded high-signal sources
# =========================

# Step 1: specialist frontier deep-tech sources (Biotech, Neurotech, Space, Robotics, Energy/Fusion/SMR/Batteries/Geothermal/CDR, Materials/Nanotech, Crypto/Agents)
ENABLE_SPECIALIST_RSS = get_bool("ENABLE_SPECIALIST_RSS", True)
SPECIALIST_RSS_MAX_ITEMS_PER_FEED = get_int("SPECIALIST_RSS_MAX_ITEMS_PER_FEED", 12)
DEFAULT_SPECIALIST_RSS_FEEDS = [
    ("GEN Biotechnology", "https://www.genengnews.com/feed/"),
    ("Labiotech.eu", "https://www.labiotech.eu/feed/"),
    ("Neuroscience News", "https://neurosciencenews.com/feed/"),
    ("SpaceNews", "https://spacenews.com/feed/"),
    ("Carbon Herald", "https://carbonherald.com/feed/"),
    ("World Nuclear News", "https://www.world-nuclear-news.org/rss"),
    ("ThinkGeoEnergy", "https://www.thinkgeoenergy.com/feed/"),
    ("Energy Storage News", "https://www.energy-storage.news/feed/"),
    ("Phys.org Materials", "https://phys.org/rss-feed/chemistry-news/materials-science/"),
    ("IACR Cryptology ePrint", "https://eprint.iacr.org/rss/rss.xml"),
]

# Step 2: multi-disciplinary deep-tech & scientific research feeds
ENABLE_SUPPLY_CHAIN_RSS = get_bool("ENABLE_SUPPLY_CHAIN_RSS", True)
SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED = get_int("SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED", 12)
DEFAULT_SUPPLY_CHAIN_RSS_FEEDS = [
    ("Ars Technica Science", "https://feeds.arstechnica.com/arstechnica/science"),
    ("Quanta Magazine", "https://www.quantamagazine.org/feed/"),
    ("Canary Media", "https://www.canarymedia.com/rss.rss"),
]

ENABLE_HTML_SCRAPER = get_bool("ENABLE_HTML_SCRAPER", True)
HTML_SCRAPER_MAX_ITEMS_PER_SITE = get_int("HTML_SCRAPER_MAX_ITEMS_PER_SITE", 12)

DEFAULT_HTML_SCRAPER_TARGETS = [
    ("STAT News Biotech", "https://www.statnews.com/category/biotech/;;/20\\d{2}/\\d{2}/\\d{2}/[a-z0-9-]+"),
    ("SpaceNews Launch", "https://spacenews.com/segment/launch/;;/20\\d{2}/[a-z0-9-]+"),
    ("Carbon Herald Capture", "https://carbonherald.com/category/carbon-capture/;;/[a-z0-9-]+/"),
    ("NASASpaceFlight", "https://www.nasaspaceflight.com/"),
    ("Payload Space", "https://payloadspace.com/"),
    ("The Robot Report", "https://www.therobotreport.com/category/news/"),
    ("IEEE Spectrum Robotics", "https://spectrum.ieee.org/topic/robotics/"),
    ("Nanowerk", "https://www.nanowerk.com/"),
    ("SynBioBeta", "https://www.synbiobeta.com/read"),
]

# Step 3: SEC EDGAR polling — market-wide full-text search (EFTS)
ENABLE_SEC_EDGAR = get_bool("ENABLE_SEC_EDGAR", False)
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "Frontier-DeepTech-Trend-Bot/2.3 contact@example.com").strip()
SEC_FORMS = get_csv("SEC_FORMS", ["8-K", "10-Q", "10-K", "S-1", "F-1", "20-F", "6-K"])
SEC_LOOKBACK_DAYS = get_int("SEC_LOOKBACK_DAYS", 3)
SEC_MAX_ITEMS_PER_FETCH = get_int("SEC_MAX_ITEMS_PER_FETCH", 40)
DEFAULT_SEC_EFTS_QUERY = (
    '"CRISPR" OR "base editing" OR "prime editing" OR "de novo protein" OR "mRNA platform" '
    'OR "brain-computer interface" OR "optogenetics" OR "reusable launch vehicle" '
    'OR "nuclear thermal propulsion" OR "optical inter-satellite" OR "humanoid robot" '
    'OR "direct air capture" OR "ocean alkalinity" OR "small modular reactor" OR "molten salt reactor" '
    'OR "solid-state battery" OR "fusion energy" OR "geothermal drilling" '
    'OR "van der Waals" OR "metamaterial" OR "ultra-high-temperature ceramic" '
    'OR "zero-knowledge proof" OR "post-quantum cryptography" OR "fully homomorphic encryption" '
    'OR "test-time compute" OR "autonomous agent"'
)
SEC_EFTS_QUERY = os.getenv("SEC_EFTS_QUERY", DEFAULT_SEC_EFTS_QUERY).strip()

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, text/html, application/json;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

HTML_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


# ==========================================
# Volatile staging queue
# ==========================================

CANDIDATE_QUEUE: list = []
SEEN_URLS_CACHE: dict = {}
SEEN_FINGERPRINTS_CACHE: dict = {}
_queue_lock = Lock()
_last_flush_time: dt.datetime = None  # type: ignore[assignment]

FEED_FAILURE_STATE: dict = {}
FEED_CACHE_HEADERS: dict = {}
_feed_state_lock = Lock()

_thread_local = threading.local()


# =========================
# Small utilities
# =========================


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


_last_flush_time = utc_now()


def clean_text(value):
    if not value:
        return ""
    text = html.unescape(str(value))
    return " ".join(text.replace("\n", " ").replace("\t", " ").split())


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def safe_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ["true", "1", "yes", "y", "on"]
    return bool(value)


def html_escape(value):
    return html.escape(clean_text(value), quote=False)


def normalize_url(url):
    """Reduce duplicate URLs caused by tracking parameters."""
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
        tracking_prefixes = ("utm_",)
        tracking_names = {"fbclid", "gclid", "mc_cid", "mc_eid", "igshid"}
        query_pairs = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            key_lower = key.lower()
            if key_lower.startswith(tracking_prefixes) or key_lower in tracking_names:
                continue
            query_pairs.append((key, value))
        clean_query = encode_query(query_pairs, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, clean_query, ""))
    except Exception:
        return url.strip()


def title_fingerprint(title):
    """Stable article fingerprint for cross-source duplicate protection."""
    cleaned = clean_text(title).lower()
    cleaned = re.sub(r"https?://\S+", " ", cleaned)
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    cleaned = re.sub(r"\b(the|a|an|to|of|and|or|for|in|on|with|by|from|at|as|is|are)\b", " ", cleaned)
    cleaned = " ".join(cleaned.split())[:220]
    if not cleaned:
        return ""
    return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()


def item_fingerprint(item):
    if item.get("content_fingerprint"):
        return item["content_fingerprint"]
    title = item.get("title", "")
    if item.get("source_type") == "sec_filing":
        base = title_fingerprint(title)
        url_part = hashlib.sha256(item.get("url", "").encode("utf-8")).hexdigest()[:16]
        return f"{base}:{url_part}"
    return title_fingerprint(title)


def feed_should_skip_live(feed_url):
    with _feed_state_lock:
        state = FEED_FAILURE_STATE.get(feed_url)
        cooldown_until = state.get("cooldown_until") if state else None
    return bool(cooldown_until and utc_now() < cooldown_until)


def feed_record_result(feed_url, success):
    with _feed_state_lock:
        state = FEED_FAILURE_STATE.setdefault(feed_url, {"consecutive_failures": 0, "cooldown_until": None})
        if success:
            state["consecutive_failures"] = 0
            state["cooldown_until"] = None
        else:
            state["consecutive_failures"] += 1
            if state["consecutive_failures"] >= FEED_FAILURE_THRESHOLD:
                state["cooldown_until"] = utc_now() + dt.timedelta(minutes=FEED_COOLDOWN_MINUTES)


class RSSFetchStatus:
    """Explicit outcome states for RSS/Atom fetches so callers can distinguish
    HTTP 304 Not Modified and active cooldowns from genuine feed failures."""

    OK = "ok"
    NOT_MODIFIED = "not_modified"
    COOLDOWN = "cooldown"
    EMPTY = "empty"
    ERROR = "error"


RSSFetchResult = namedtuple("RSSFetchResult", ["items", "status"])


def _get_feed_cache_headers(feed_url):
    with _feed_state_lock:
        return dict(FEED_CACHE_HEADERS.get(feed_url) or {})


def _update_feed_cache_headers(feed_url, response_headers):
    """Store ETag / Last-Modified validators thread-safely (supports both HTTP 200 and 304 per RFC 9110)."""
    if not response_headers:
        return
    etag = response_headers.get("ETag")
    last_modified = response_headers.get("Last-Modified")
    if not etag and not last_modified:
        return
    with _feed_state_lock:
        prev = FEED_CACHE_HEADERS.get(feed_url) or {}
        FEED_CACHE_HEADERS[feed_url] = {
            "etag": etag or prev.get("etag"),
            "last_modified": last_modified or prev.get("last_modified"),
        }


def ensure_db_directory():
    directory = os.path.dirname(DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)


def chunk_list(values, size):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def fair_cap_items(items, max_items):
    """Interleave items round-robin across sources and apply a global cap without letting one source dominate."""
    if not items:
        return []

    grouped = defaultdict(deque)
    source_order = []
    for item in items:
        source = item.get("source", "unknown")
        if source not in grouped:
            source_order.append(source)
        grouped[source].append(item)

    limit = max_items if max_items > 0 else len(items)
    result = []
    while len(result) < limit and source_order:
        next_order = []
        for source in source_order:
            if grouped[source] and len(result) < limit:
                result.append(grouped[source].popleft())
            if grouped[source]:
                next_order.append(source)
        source_order = next_order
    return result


# =========================
# Database
# =========================


def connect_db():
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        _thread_local.conn = conn
    return conn


def init_db():
    ensure_db_directory()
    conn = connect_db()
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            content_fingerprint TEXT,
            title TEXT,
            source TEXT,
            source_type TEXT,
            published_at TEXT,
            category TEXT,
            hardware_domain TEXT,
            bottleneck_addressed TEXT,
            specific_tech TEXT,
            relevance_score REAL,
            why_it_matters TEXT,
            ai_revolution_thesis TEXT,
            telegram_summary TEXT,
            raw_summary TEXT,
            raw_llm_json TEXT,
            sent_alert INTEGER DEFAULT 0,
            digest_sent INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_urls (
            url TEXT PRIMARY KEY,
            content_fingerprint TEXT,
            title TEXT,
            source TEXT,
            source_type TEXT,
            status TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_fingerprints (
            content_fingerprint TEXT PRIMARY KEY,
            title TEXT,
            status TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    cur.execute("PRAGMA table_info(items)")
    existing_columns = {row[1] for row in cur.fetchall()}
    wanted_columns = {
        "content_fingerprint": "TEXT",
        "hardware_domain": "TEXT",
        "bottleneck_addressed": "TEXT",
        "ai_revolution_thesis": "TEXT",
    }
    for column_name, column_type in wanted_columns.items():
        if column_name not in existing_columns:
            cur.execute(f"ALTER TABLE items ADD COLUMN {column_name} {column_type}")

    cur.execute("PRAGMA table_info(processed_urls)")
    existing_processed_columns = {row[1] for row in cur.fetchall()}
    if "content_fingerprint" not in existing_processed_columns:
        cur.execute("ALTER TABLE processed_urls ADD COLUMN content_fingerprint TEXT")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_fingerprint ON items(content_fingerprint)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_urls_fingerprint ON processed_urls(content_fingerprint)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_alert ON items(sent_alert, relevance_score, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_digest ON items(digest_sent, relevance_score, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_created ON items(created_at)")
    conn.commit()


def get_state(key, default=None):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
    row = cur.fetchone()
    return row["value"] if row else default


def set_state(key, value):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO bot_state (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, str(value)),
    )
    conn.commit()


def get_hn_high_water_mark():
    try:
        return int(get_state("hn_high_water_mark", "0"))
    except (TypeError, ValueError):
        return 0


def set_hn_high_water_mark(value):
    set_state("hn_high_water_mark", value)


def processed_url_exists(url):
    if not url:
        return True
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed_urls WHERE url = ? LIMIT 1", (url,))
    return cur.fetchone() is not None


def processed_fingerprint_exists(content_fingerprint):
    if not content_fingerprint:
        return False
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM processed_fingerprints WHERE content_fingerprint = ? LIMIT 1",
        (content_fingerprint,),
    )
    return cur.fetchone() is not None


def mark_url_processed(item, status):
    url = normalize_url(item.get("url", ""))
    fp = item_fingerprint(item)
    if not url and not fp:
        return
    now = utc_now().isoformat()
    conn = connect_db()
    cur = conn.cursor()

    if url:
        cur.execute(
            """
            INSERT INTO processed_urls (url, content_fingerprint, title, source, source_type, status, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                content_fingerprint = COALESCE(excluded.content_fingerprint, processed_urls.content_fingerprint),
                title = excluded.title,
                source = excluded.source,
                source_type = excluded.source_type,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at
            """,
            (
                url,
                fp,
                item.get("title", ""),
                item.get("source", ""),
                item.get("source_type", ""),
                status,
                now,
                now,
            ),
        )

    if fp:
        cur.execute(
            """
            INSERT INTO processed_fingerprints (content_fingerprint, title, status, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(content_fingerprint) DO UPDATE SET
                title = excluded.title,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at
            """,
            (fp, item.get("title", ""), status, now, now),
        )

    conn.commit()


def insert_item(item, sent_alert=0, digest_sent=0):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO items (
                url, content_fingerprint, title, source, source_type, published_at, category,
                hardware_domain, bottleneck_addressed, specific_tech,
                relevance_score, why_it_matters, ai_revolution_thesis,
                telegram_summary, raw_summary, raw_llm_json,
                sent_alert, digest_sent, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalize_url(item.get("url", "")),
                item_fingerprint(item),
                item.get("title", ""),
                item.get("source", ""),
                item.get("source_type", ""),
                item.get("published_at", utc_now().isoformat()),
                item.get("category", ""),
                item.get("hardware_domain", ""),
                item.get("bottleneck_addressed", ""),
                item.get("specific_tech", ""),
                safe_float(item.get("relevance_score", 0)),
                item.get("why_it_matters", ""),
                item.get("ai_revolution_thesis", ""),
                item.get("telegram_summary", ""),
                item.get("raw_summary", ""),
                item.get("raw_llm_json", ""),
                sent_alert,
                digest_sent,
                utc_now().isoformat(),
            ),
        )
        conn.commit()
        inserted = True
    except sqlite3.IntegrityError:
        conn.rollback()
        inserted = False
    return inserted

# =========================
# Telegram
# =========================


def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram disabled: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID.")
        return False

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    text = message.strip()
    chunks = []

    current = ""
    for raw_line in text.splitlines():
        line = raw_line[:3800]
        proposed = f"{current}\n{line}" if current else line
        if len(proposed) > 3800:
            if current:
                chunks.append(current)
            current = line
        else:
            current = proposed
    if current:
        chunks.append(current)

    success = True
    for chunk in chunks:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        try:
            response = requests.post(api_url, json=payload, timeout=30)
            if response.status_code != 200:
                log.warning(f"Telegram sending issue: {response.status_code} {response.text[:500]}")
                success = False
        except Exception as exc:
            log.warning(f"Telegram sending exception: {repr(exc)}")
            success = False
        time.sleep(1)

    return success

def send_telegram_document(filename, text_content):
    """Sends a text string as a downloadable file to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    
    files = {
        "document": (filename, text_content.encode('utf-8'))
    }
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "caption": "📊 Raw fetched titles (pre-deduplication)"
    }
    
    try:
        response = requests.post(api_url, data=payload, files=files, timeout=30)
        if response.status_code != 200:
            log.warning(f"Telegram document sending issue: {response.status_code} {response.text[:500]}")
            return False
        return True
    except Exception as exc:
        log.warning(f"Telegram document sending exception: {repr(exc)}")
        return False

def format_item_html(row, include_score=True):
    title = html_escape(row["title"])
    url = html_escape(row["url"])
    source = html_escape(row["source"])
    score = safe_float(row["relevance_score"], 0)
    tech = html_escape(row["specific_tech"] or row["category"])
    domain = html_escape(row["hardware_domain"])
    bottleneck = html_escape(row["bottleneck_addressed"])
    why = html_escape(row["why_it_matters"])
    thesis = html_escape(row["ai_revolution_thesis"])
    summary = html_escape(row["telegram_summary"])

    lines = []
    if include_score:
        lines.append(f"<b>{score:.1f}/10</b> · {tech}")
    else:
        lines.append(f"<b>{tech}</b>")
    lines.append(f"<a href=\"{url}\">{title}</a>")
    lines.append(f"Source: {source}")
    if domain or bottleneck:
        lines.append(f"Layer: {domain} · Bottleneck: {bottleneck}")
    if summary:
        lines.append(summary)
    if why:
        lines.append(f"Why it matters: {why}")
    if thesis:
        lines.append(f"Frontier thesis: {thesis}")
    return "\n".join(lines)


def send_immediate_alerts(alert_items):
    if not alert_items:
        return []

    items_to_send = alert_items[:MAX_ALERTS_PER_FLUSH]

    blocks = ["🚨 <b>High-signal Frontier Deep-Tech alert</b>"]
    for item in items_to_send:
        blocks.append(format_item_html(item, include_score=True))

    message = "\n\n".join(blocks)

    if not send_telegram(message):
        log.warning("Immediate alert send failed; leaving these items for the next digest.")
        return []

    log.info(f"Sent {len(items_to_send)} immediate alert items.")
    return [item["url"] for item in items_to_send]


def send_digest():
    conn = connect_db()
    cur = conn.cursor()
    
    time_window = (utc_now() - dt.timedelta(hours=24)).isoformat()
    
    cur.execute(
        """
        SELECT * FROM items
        WHERE digest_sent = 0 
          AND relevance_score >= ? 
          AND created_at >= ?
        ORDER BY relevance_score DESC, created_at DESC
        LIMIT ?
        """,
        (LLM_MIN_SCORE, time_window, DIGEST_MAX_ITEMS),
    )
    rows = cur.fetchall()

    if not rows:
        if SEND_EMPTY_DIGEST:
            send_telegram("🧬 <b>Frontier Deep-Tech digest</b>\nNo new relevant items passed the strict filter.")
        else:
            log.info("No new digest items.")
        return 0

    header = f"🧬 <b>Frontier Deep-Tech digest</b> · {len(rows)} new signal(s)"
    blocks = [header]
    for row in rows:
        blocks.append(format_item_html(row, include_score=True))
    message = "\n\n".join(blocks)

    if not send_telegram(message):
        log.warning("Digest send failed; items remain unsent.")
        return 0

    ids = [row["id"] for row in rows]
    conn = connect_db()
    cur = conn.cursor()
    cur.executemany("UPDATE items SET digest_sent = 1 WHERE id = ?", [(item_id,) for item_id in ids])
    conn.commit()
    log.info(f"Sent {len(ids)} digest items.")
    return len(ids)

def send_raw_json_dump(raw_items):
    """Sends all raw fetched items as an uncleaned JSON file to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    filename = f"raw_fetch_dump_{utc_now().strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        json_content = json.dumps(raw_items, indent=2, ensure_ascii=False)
    except Exception as exc:
        log.warning(f"Failed to serialize raw JSON dump: {repr(exc)}")
        return False

    files = {
        "document": (filename, json_content.encode("utf-8"), "application/json")
    }
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "caption": f"📁 <b>Raw JSON Fetch Dump</b>\nTotal items: {len(raw_items)} (Pre-cleaning & pre-slicing)",
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(api_url, data=payload, files=files, timeout=45)
        if response.status_code != 200:
            log.warning(f"Telegram raw JSON sending issue: {response.status_code} {response.text[:500]}")
            return False
        log.info(f"Sent raw JSON dump ({len(raw_items)} items) to Telegram.")
        return True
    except Exception as exc:
        log.warning(f"Telegram raw JSON sending exception: {repr(exc)}")
        return False
