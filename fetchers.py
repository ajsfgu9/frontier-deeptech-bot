from core import *
from core import _feed_state_lock

# =========================
# Source fetchers
# =========================

ARXIV_HEADERS = {
    "User-Agent": os.getenv(
        "ARXIV_USER_AGENT",
        "Frontier-DeepTech-Trend-Bot/2.4 (+https://github.com/ajsfgu9/frontier-deeptech-bot)",
    ).strip()
}

_arxiv_poll_lock = Lock()
_arxiv_last_poll_time = None

# Tracks the last seen Wayback snapshot timestamp per URL so repeated cycles
# do not re-download and re-parse identical archived snapshots.
_WAYBACK_LAST_TS: dict = {}

# Automatic fallback URLs for legacy or WAF/403/404-prone endpoints so existing
# server .env files recover cleanly without requiring manual URL edits.
DEPRECATED_FEED_URL_FALLBACKS = {
    "https://api.quantamagazine.org/v1/rss": [
        "https://www.quantamagazine.org/feed/",
        "https://api.quantamagazine.org/feed/",
    ],
    "https://www.synbiobeta.com/feed": [
        "https://news.google.com/rss/search?q=site:synbiobeta.com+when:7d&hl=en-US&gl=US&ceid=US:en",
    ],
    "https://spectrum.ieee.org/feeds/topic/robotics.rss": [
        "https://spectrum.ieee.org/rss/robotics/fulltext",
        "https://news.google.com/rss/search?q=site:spectrum.ieee.org+robotics+when:7d&hl=en-US&gl=US&ceid=US:en",
    ],
    "https://feeds.arstechnica.com/arstechnica/science": [
        "https://arstechnica.com/science/feed/",
    ],
    "https://www.nasaspaceflight.com/feed": [
        "https://news.google.com/rss/search?q=site:nasaspaceflight.com+when:7d&hl=en-US&gl=US&ceid=US:en",
    ],
    "https://payloadspace.com/feed": [
        "https://news.google.com/rss/search?q=site:payloadspace.com+when:7d&hl=en-US&gl=US&ceid=US:en",
    ],
    "https://www.therobotreport.com/feed": [
        "https://news.google.com/rss/search?q=site:therobotreport.com+when:7d&hl=en-US&gl=US&ceid=US:en",
    ],
}

_NON_HTML_FALLBACK_HOSTS = {
    "news.google.com",
    "feeds.feedburner.com",
    "rsshub.app",
}


def _fetch_arxiv_atom(url, max_retries=3, timeout=20):
    """Direct fetch + parse of the arXiv Atom API, preserving <category> tags."""
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=ARXIV_HEADERS, timeout=timeout)
            resp.raise_for_status()
            parsed = feedparser.parse(resp.content)
            if parsed.bozo and not parsed.entries:
                raise ValueError(f"feedparser bozo with no entries: {parsed.bozo_exception}")
            return parsed.entries
        except Exception as exc:
            last_exc = exc
            log.warning(f"arXiv fetch attempt {attempt + 1}/{max_retries} failed: {repr(exc)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 1s, 2s, 4s
    log.error(f"arXiv combined fetch failed after {max_retries} attempts: {repr(last_exc)}")
    return []


def _parse_arxiv_entry(entry, category):
    """Field names here MUST match what fetch_and_enqueue()/insert_item() expect."""
    return {
        "title": clean_text(entry.get("title", "")),
        "url": normalize_url(entry.get("link", "")),
        "source": f"arXiv / {category}",
        "source_type": "research",
        "published_at": entry.get("published", entry.get("updated", utc_now().isoformat())),
        "raw_summary": clean_text(entry.get("summary", ""))[:2200],
        "categories": [t.get("term") for t in entry.get("tags", []) if t.get("term")],
    }


def fetch_arxiv():
    """Fetch ARXIV_RESULTS_PER_CATEGORY items for each configured category with
    polite minimum poll spacing and a targeted backfill query for starved
    low-volume categories.
    """
    global _arxiv_last_poll_time

    items = []
    if not ENABLE_ARXIV or not ARXIV_CATEGORIES:
        return items

    with _arxiv_poll_lock:
        if ARXIV_MIN_POLL_MINUTES > 0 and _arxiv_last_poll_time is not None:
            elapsed = utc_now() - _arxiv_last_poll_time
            min_gap = dt.timedelta(minutes=ARXIV_MIN_POLL_MINUTES)
            if elapsed < min_gap:
                remaining_min = (min_gap - elapsed).total_seconds() / 60.0
                log.debug(
                    f"arXiv: skipping poll ({remaining_min:.1f}m until next poll; "
                    f"ARXIV_MIN_POLL_MINUTES={ARXIV_MIN_POLL_MINUTES})"
                )
                return items

    combined_query = "(" + " OR ".join(f"cat:{c}" for c in ARXIV_CATEGORIES) + ")"
    max_results = min(2000, max(ARXIV_RESULTS_PER_CATEGORY * len(ARXIV_CATEGORIES) * 5, 200))

    params = {
        "search_query": combined_query,
        "start": 0,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    url = "https://export.arxiv.org/api/query?" + urlencode(params)

    raw_entries = _fetch_arxiv_atom(url)
    if not raw_entries:
        return items

    buckets = {cat: [] for cat in ARXIV_CATEGORIES}
    bucket_urls = {cat: set() for cat in ARXIV_CATEGORIES}

    def _fill_buckets(entries, target_cats):
        for entry in entries:
            entry_categories = [t.get("term") for t in entry.get("tags", []) if t.get("term")]
            norm_link = normalize_url(entry.get("link", ""))
            for cat in target_cats:
                if (
                    cat in entry_categories
                    and len(buckets[cat]) < ARXIV_RESULTS_PER_CATEGORY
                    and norm_link not in bucket_urls[cat]
                ):
                    bucket_urls[cat].add(norm_link)
                    buckets[cat].append(_parse_arxiv_entry(entry, cat))

    _fill_buckets(raw_entries, ARXIV_CATEGORIES)

    # Low-volume categories (e.g. q-bio.TO, physics.space-ph) are often crowded out
    # of a single combined query by high-volume cs.AI/cs.LG traffic. Run one small
    # targeted backfill query for just the under-filled categories.
    short_cats = [cat for cat in ARXIV_CATEGORIES if len(buckets[cat]) < ARXIV_RESULTS_PER_CATEGORY]
    if short_cats:
        time.sleep(1.5)
        backfill_query = "(" + " OR ".join(f"cat:{c}" for c in short_cats) + ")"
        backfill_max = min(1000, max(ARXIV_RESULTS_PER_CATEGORY * len(short_cats) * 5, 100))
        backfill_params = {
            "search_query": backfill_query,
            "start": 0,
            "max_results": backfill_max,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        backfill_url = "https://export.arxiv.org/api/query?" + urlencode(backfill_params)
        backfill_entries = _fetch_arxiv_atom(backfill_url, max_retries=2, timeout=20)
        if backfill_entries:
            _fill_buckets(backfill_entries, short_cats)

    for cat, bucketed in buckets.items():
        if len(bucketed) < ARXIV_RESULTS_PER_CATEGORY:
            log.info(
                f"arXiv / {cat}: {len(bucketed)}/{ARXIV_RESULTS_PER_CATEGORY} items available after backfill"
            )
        items.extend(bucketed)

    with _arxiv_poll_lock:
        _arxiv_last_poll_time = utc_now()

    return items


def _fetch_hn_item(session, story_id):
    try:
        response = session.get(
            f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json",
            headers=REQUEST_HEADERS,
            timeout=20,
        )
        if response.status_code != 200:
            return None
        hit = response.json() or {}
        if hit.get("type") != "story":
            return None

        title = clean_text(hit.get("title", ""))
        if not title:
            return None

        link = normalize_url(hit.get("url") or f"https://news.ycombinator.com/item?id={story_id}")
        points = hit.get("score") or 0
        comments = hit.get("descendants") or 0
        created_at = dt.datetime.fromtimestamp(hit.get("time", time.time()), tz=dt.timezone.utc).isoformat()
        text = clean_text(hit.get("text", ""))

        return {
            "title": title,
            "url": link,
            "source": "Hacker News / newest",
            "source_type": "community",
            "published_at": created_at,
            "raw_summary": f"Newest Hacker News story. Points: {points}. Comments: {comments}. Text: {text[:1200]}",
        }
    except Exception as exc:
        log.error(f"HN item {story_id} exception: {repr(exc)}")
        return None


def fetch_hackernews():
    items = []
    if not ENABLE_HACKERNEWS:
        return items

    try:
        ids_response = requests.get(
            "https://hacker-news.firebaseio.com/v0/newstories.json",
            headers=REQUEST_HEADERS,
            timeout=30,
        )
        if ids_response.status_code != 200:
            log.warning(f"HN newest IDs failure: {ids_response.status_code} {ids_response.text[:300]}")
            return items
        story_ids = ids_response.json()[:HN_NEW_STORIES_PER_RUN]
    except Exception as exc:
        log.error(f"HN newest IDs exception: {repr(exc)}")
        return items

    last_seen_id = get_hn_high_water_mark()
    fresh_ids = [sid for sid in story_ids if sid > last_seen_id] if last_seen_id else story_ids
    skipped = len(story_ids) - len(fresh_ids)
    if skipped:
        log.info(f"HN: skipping {skipped} id(s) at or below high-water mark {last_seen_id}.")
    if not fresh_ids:
        return items

    workers = max(1, min(HN_FETCH_WORKERS, len(fresh_ids)))
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_fetch_hn_item, session, sid) for sid in fresh_ids]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    items.append(result)

    set_hn_high_water_mark(max(fresh_ids))
    return items


def fetch_techmeme():
    if not ENABLE_TECHMEME:
        return []
    return fetch_standard_rss(
        "https://www.techmeme.com/feed.xml",
        source_label="Techmeme",
        source_type="technology_news",
        max_items=TECHMEME_MAX_ITEMS,
    )


def _is_wayback_snapshot_fresh(timestamp_str):
    if not timestamp_str or len(timestamp_str) < 8:
        return True
    try:
        snap_dt = dt.datetime.strptime(timestamp_str[:8], "%Y%m%d").replace(tzinfo=dt.timezone.utc)
        return (utc_now() - snap_dt).days <= WAYBACK_MAX_AGE_DAYS
    except Exception:
        return True


def _fetch_wayback_snapshot(target_url, headers, label, timeout=25):
    """Fetch the closest fresh Wayback Machine raw (id_) snapshot for target_url.
    Returns (content_bytes_or_none, status) where status is 'ok', 'unchanged', or 'miss'.
    """
    try:
        wb_api_url = f"https://archive.org/wayback/available?url={target_url}"
        wb_resp = requests.get(wb_api_url, timeout=15)
        if wb_resp.status_code != 200:
            return None, "miss"
        snapshots = (wb_resp.json() or {}).get("archived_snapshots") or {}
        closest = snapshots.get("closest") or {}
        if not closest.get("available") or not closest.get("url"):
            return None, "miss"

        ts_str = str(closest.get("timestamp") or "")
        if not _is_wayback_snapshot_fresh(ts_str):
            log.debug(
                f"Wayback snapshot for {label or target_url} is older than "
                f"{WAYBACK_MAX_AGE_DAYS}d (ts={ts_str}); ignoring stale archive."
            )
            return None, "miss"

        with _feed_state_lock:
            last_ts = _WAYBACK_LAST_TS.get(target_url)
        if ts_str and last_ts == ts_str:
            log.debug(f"Wayback snapshot unchanged for {label or target_url} (ts={ts_str}); skipping.")
            return None, "unchanged"

        snapshot_url = re.sub(
            r"/web/(\d+)/",
            r"/web/\1id_/",
            closest["url"].replace("http://", "https://"),
        )
        log.info(f"Wayback snapshot found for {label or target_url} (ts={ts_str or 'unknown'}). Fetching...")
        snap_resp = requests.get(snapshot_url, headers=headers, timeout=timeout)
        if snap_resp.status_code == 200 and snap_resp.content:
            if ts_str:
                with _feed_state_lock:
                    _WAYBACK_LAST_TS[target_url] = ts_str
            return snap_resp.content, "ok"
    except Exception as wb_exc:
        log.info(f"Wayback Machine fallback failed for {label or target_url}: {repr(wb_exc)}")
    return None, "miss"


def _build_rss_header_profiles(feed_url, cached_headers):
    parsed = urlsplit(feed_url)
    referer = f"{parsed.scheme}://{parsed.netloc}/" if parsed.scheme and parsed.netloc else ""
    cond = {}
    if cached_headers.get("etag"):
        cond["If-None-Match"] = cached_headers["etag"]
    if cached_headers.get("last_modified"):
        cond["If-Modified-Since"] = cached_headers["last_modified"]

    profile_default = {**REQUEST_HEADERS, **cond}
    profile_browser = {
        **HTML_BROWSER_HEADERS,
        "Accept": REQUEST_HEADERS["Accept"],
        **({"Referer": referer} if referer else {}),
        **cond,
    }
    profile_rss_bot = {**RSS_BOT_HEADERS, **cond}
    return [profile_default, profile_browser, profile_rss_bot]


def _normalize_rss_entries(feed, feed_url, source_label, source_type, max_items, enriched=False):
    items = []
    source_name = source_label or (
        clean_text(feed.feed.get("title", feed_url))
        if getattr(feed, "feed", None)
        else feed_url
    )

    entries = getattr(feed, "entries", [])[:max_items]
    if not enriched:
        for entry in entries:
            title = clean_text(entry.get("title", ""))
            link = normalize_url(entry.get("link", ""))
            summary = clean_text(entry.get("summary", entry.get("description", "")))
            if not title or not link:
                continue
            items.append({
                "title": title,
                "url": link,
                "source": f"RSS / {source_name}",
                "source_type": source_type,
                "published_at": entry.get("published", entry.get("updated", utc_now().isoformat())),
                "raw_summary": summary[:2200],
            })
        return items

    with requests.Session() as session:
        for entry in entries:
            title = clean_text(entry.get("title", ""))
            link = normalize_url(entry.get("link", ""))
            if not title or not link:
                continue

            body = ""
            if "content" in entry and entry.content and isinstance(entry.content, list):
                body = clean_text(re.sub(r"<[^>]+>", " ", entry.content[0].get("value", "")))
            if not body:
                body = clean_text(
                    re.sub(r"<[^>]+>", " ", entry.get("summary", entry.get("description", "")))
                )

            tags = []
            if "tags" in entry and entry.tags:
                for t in entry.tags:
                    term = clean_text(t.get("term", ""))
                    if term and term not in tags:
                        tags.append(term)

            author = clean_text(entry.get("author", entry.get("dc_creator", "")))

            if len(body) < 120 and link and "news.google.com" not in link:
                meta_snippet = fetch_webpage_summary_snippet(session, link, timeout=6)
                if meta_snippet and len(meta_snippet) > len(body):
                    body = f"{body} (Lead: {meta_snippet})" if body else meta_snippet

            substance = []
            if tags:
                substance.append(f"Tags: {', '.join(tags[:6])}")
            if author:
                substance.append(f"Author: {author}")
            if body:
                substance.append(f"Summary: {body[:2200]}")

            items.append({
                "title": title,
                "url": link,
                "source": f"RSS / {source_name}",
                "source_type": source_type,
                "published_at": entry.get("published", entry.get("updated", utc_now().isoformat())),
                "raw_summary": " | ".join(substance) if substance else title,
            })

    return items


def _fetch_rss_feed_with_status(
    feed_url,
    source_label=None,
    source_type="rss",
    max_items=25,
    enriched=False,
    allow_wayback=True,
):
    """Fetch an RSS/Atom feed with conditional GET, 403 header recovery,
    404/410 fast-cooldown, and optional fresh Wayback fallback.

    Returns (items, status, http_code) where status is one of:
    'ok', 'not_modified', 'cooldown', 'permanent_error', 'failed'.
    """
    label = source_label or feed_url
    max_items = max_items or 25

    if feed_should_skip_live(feed_url):
        log.debug(f"RSS feed on cooldown, skipping attempt: {label} ({feed_url})")
        return [], "cooldown", None

    cached_headers = feed_get_cache_headers(feed_url)
    header_profiles = _build_rss_header_profiles(feed_url, cached_headers)

    feed = None
    raw_content = None
    live_status = None

    for idx, req_headers in enumerate(header_profiles):
        try:
            response = requests.get(feed_url, headers=req_headers, timeout=30)
            live_status = response.status_code

            if live_status == 304:
                feed_record_result(feed_url, success=True)
                return [], "not_modified", 304

            if live_status == 200:
                raw_content = response.content
                parsed_candidate = None
                try:
                    parsed_candidate = feedparser.parse(raw_content)
                except Exception as exc:
                    log.info(f"RSS feedparser parsing issue for {label}: {repr(exc)}")

                if parsed_candidate and getattr(parsed_candidate, "entries", None):
                    feed_set_cache_headers(
                        feed_url,
                        etag=response.headers.get("ETag"),
                        last_modified=response.headers.get("Last-Modified"),
                    )
                    feed = parsed_candidate
                    break

                # Some WAFs return HTTP 200 with an HTML challenge page when headers
                # look mismatched; retry with alternate header profiles if HTML detected.
                snippet = (raw_content or b"")[:400].lower()
                if idx < len(header_profiles) - 1 and (b"<html" in snippet or b"<!doctype html" in snippet):
                    continue
                break

            if live_status in (404, 410):
                entered_cd = feed_record_result(feed_url, success=False, permanent=True)
                suffix = f" (entering {FEED_COOLDOWN_MINUTES}m cooldown)" if entered_cd else ""
                log.info(f"RSS live fetch failed for {label}: {live_status}{suffix}")
                return [], "permanent_error", live_status

            if live_status in (403, 406) and idx < len(header_profiles) - 1:
                continue

            log.info(f"RSS live fetch failed for {label}: {live_status}")
            break
        except requests.exceptions.Timeout:
            log.info(f"RSS timeout for {label}.")
            break
        except Exception as exc:
            log.info(f"RSS requests issue for {label}: {repr(exc)}")
            break

    live_entries_ok = bool(feed and getattr(feed, "entries", None))
    if live_entries_ok:
        feed_record_result(feed_url, success=True)
        items = _normalize_rss_entries(feed, feed_url, source_label, source_type, max_items, enriched=enriched)
        return items, "ok", live_status

    entered_cd = feed_record_result(feed_url, success=False)
    if entered_cd:
        log.info(
            f"RSS feed entering {FEED_COOLDOWN_MINUTES}m cooldown after repeated failures: "
            f"{label} (last status: {live_status or 'error'})"
        )

    if allow_wayback and live_status not in (404, 410):
        reason = (
            "blocked/challenge page or malformed feed"
            if raw_content is not None
            else f"status {live_status or 'request failed'}"
        )
        log.info(f"RSS live path yielded no entries for {label} ({reason}). Trying Wayback Machine...")
        wb_content, wb_status = _fetch_wayback_snapshot(feed_url, REQUEST_HEADERS, label, timeout=30)
        if wb_status == "unchanged":
            return [], "not_modified", live_status
        if wb_status == "ok" and wb_content:
            wb_feed = feedparser.parse(wb_content)
            if getattr(wb_feed, "entries", None):
                items = _normalize_rss_entries(
                    wb_feed, feed_url, source_label, source_type, max_items, enriched=enriched
                )
                return items, "ok", live_status

    return [], "failed", live_status


def fetch_standard_rss(feed_url, source_label=None, source_type="rss", max_items=25):
    """Fetch an RSS/Atom feed with browser/bot header fallback, conditional GET,
    cooldown tracking, and a freshness-checked Wayback Machine fallback.
    """
    items, _, _ = _fetch_rss_feed_with_status(
        feed_url,
        source_label=source_label,
        source_type=source_type,
        max_items=max_items,
        enriched=False,
        allow_wayback=True,
    )
    return items


def fetch_standard_rss_2(feed_url, source_label=None, source_type="custom_rss", max_items=None):
    """Enriched RSS/Atom fetcher: extracts <content:encoded>, tags/categories,
    author, and handles short stubs.
    """
    items, _, _ = _fetch_rss_feed_with_status(
        feed_url,
        source_label=source_label,
        source_type=source_type,
        max_items=max_items or 25,
        enriched=True,
        allow_wayback=True,
    )
    return items


# =========================
# Expanded source fetchers
# =========================


def split_fallback_urls(url_value):
    raw = str(url_value or "")
    return [part.strip() for part in raw.split(";;") if part.strip()]


def expand_candidate_urls(url_value):
    """Split ';;' fallback URLs and automatically append verified alternate
    endpoints for known deprecated or 403/404-prone feed URLs.
    """
    urls = split_fallback_urls(url_value)
    expanded = []
    seen = set()
    for url in urls:
        if url not in seen:
            expanded.append(url)
            seen.add(url)
        lookup_key = url.rstrip("/")
        for fallback in DEPRECATED_FEED_URL_FALLBACKS.get(url) or DEPRECATED_FEED_URL_FALLBACKS.get(lookup_key) or []:
            if fallback not in seen:
                expanded.append(fallback)
                seen.add(fallback)
    return expanded


def _derive_html_fallback_url(feed_url):
    """Convert an RSS feed URL into a safe website homepage URL for HTML fallback.
    Strips feed/API subdomains (e.g. feeds.arstechnica.com -> arstechnica.com)
    and skips non-publisher aggregator hosts like news.google.com.
    """
    parsed = urlsplit(feed_url or "")
    if not parsed.scheme or not parsed.netloc:
        return ""
    host = parsed.netloc.lower()
    if any(blocked in host for blocked in _NON_HTML_FALLBACK_HOSTS):
        return ""
    for prefix in ("feeds.", "feed.", "rss.", "api."):
        if host.startswith(prefix) and len(host) > len(prefix) + 3:
            host = host[len(prefix):]
            break
    return f"{parsed.scheme}://{host}/"


def _fetch_single_feed_spec(label, feed_url, source_type, max_items_per_feed, enriched=False):
    candidate_urls = expand_candidate_urls(feed_url)
    if not candidate_urls:
        return []

    wayback_candidates = []

    # Pass 1: Try all live RSS candidate URLs before touching Wayback or HTML scraping.
    for candidate_url in candidate_urls:
        try:
            fetched, status, http_code = _fetch_rss_feed_with_status(
                candidate_url,
                source_label=label,
                source_type=source_type,
                max_items=max_items_per_feed,
                enriched=enriched,
                allow_wayback=False,
            )
            if status == "ok":
                log.info(f"{source_type} feed: {len(fetched)} items from {label}")
                return fetched
            if status == "not_modified":
                log.debug(f"{source_type} feed: 304 Not Modified for {label}")
                return []
            if status == "failed" and http_code not in (404, 410) and "news.google.com" not in candidate_url:
                wayback_candidates.append(candidate_url)
        except Exception as exc:
            log.error(f"{source_type} issue for {label} / {candidate_url}: {repr(exc)}")

    # Pass 2: Try a fresh Wayback RSS snapshot only for non-404 URLs if every live URL failed.
    for candidate_url in wayback_candidates:
        wb_content, wb_status = _fetch_wayback_snapshot(candidate_url, REQUEST_HEADERS, label, timeout=30)
        if wb_status == "unchanged":
            return []
        if wb_status == "ok" and wb_content:
            wb_feed = feedparser.parse(wb_content)
            if getattr(wb_feed, "entries", None):
                items = _normalize_rss_entries(
                    wb_feed, candidate_url, label, source_type, max_items_per_feed, enriched=enriched
                )
                log.info(f"{source_type} feed: {len(items)} items from {label} (Wayback RSS)")
                return items

    # Pass 3: Fall back to HTML scraper only when all RSS paths genuinely failed (never on 304).
    root_url = _derive_html_fallback_url(candidate_urls[0])
    if root_url:
        log.info(f"{source_type}: RSS failed for {label}; falling back to HTML scraper on {root_url}")
        return scrape_single_html_target(label, root_url, max_items_per_feed)

    return []


def _fetch_feed_specs_concurrent(specs, source_type, max_items_per_feed, enriched=False):
    if not specs:
        return []

    workers = max(1, min(RSS_FETCH_WORKERS, len(specs)))
    if workers == 1:
        items = []
        for label, feed_url in specs:
            items.extend(_fetch_single_feed_spec(label, feed_url, source_type, max_items_per_feed, enriched=enriched))
        return items

    results_by_idx = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_idx = {
            pool.submit(_fetch_single_feed_spec, label, feed_url, source_type, max_items_per_feed, enriched): idx
            for idx, (label, feed_url) in enumerate(specs)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results_by_idx[idx] = future.result() or []
            except Exception as exc:
                label = specs[idx][0]
                log.error(f"{source_type} worker error for {label}: {repr(exc)}")
                results_by_idx[idx] = []

    items = []
    for idx in range(len(specs)):
        items.extend(results_by_idx.get(idx, []))
    return items


def fetch_feed_specs(specs, source_type, max_items_per_feed):
    return _fetch_feed_specs_concurrent(specs, source_type, max_items_per_feed, enriched=False)


def fetch_feed_specs_2(specs, source_type, max_items_per_feed):
    return _fetch_feed_specs_concurrent(specs, source_type, max_items_per_feed, enriched=True)


def fetch_webpage_summary_snippet(session, url, timeout=6):
    """Fallback scraper to grab OpenGraph or meta description for short stubs."""
    try:
        resp = session.get(url, headers=REQUEST_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return ""
        html_text = resp.text
        og_match = re.search(
            r'<meta\s+[^>]*property=[\'"]og:description[\'"][^>]*content=[\'"]([^\'"]+)[\'"]',
            html_text,
            re.IGNORECASE,
        ) or re.search(
            r'<meta\s+[^>]*content=[\'"]([^\'"]+)[\'"][^>]*property=[\'"]og:description[\'"]',
            html_text,
            re.IGNORECASE,
        )
        if og_match:
            return clean_text(og_match.group(1))

        meta_match = re.search(
            r'<meta\s+[^>]*name=[\'"]description[\'"][^>]*content=[\'"]([^\'"]+)[\'"]',
            html_text,
            re.IGNORECASE,
        ) or re.search(
            r'<meta\s+[^>]*content=[\'"]([^\'"]+)[\'"][^>]*name=[\'"]description[\'"]',
            html_text,
            re.IGNORECASE,
        )
        if meta_match:
            return clean_text(meta_match.group(1))
    except Exception:
        pass
    return ""


def fetch_specialist_rss():
    if not ENABLE_SPECIALIST_RSS:
        return []
    specs = get_feed_specs("SPECIALIST_RSS_FEEDS", DEFAULT_SPECIALIST_RSS_FEEDS)
    return fetch_feed_specs_2(specs, "specialist_media", SPECIALIST_RSS_MAX_ITEMS_PER_FEED)


def fetch_supply_chain_rss():
    if not ENABLE_SUPPLY_CHAIN_RSS:
        return []
    specs = get_feed_specs("SUPPLY_CHAIN_RSS_FEEDS", DEFAULT_SUPPLY_CHAIN_RSS_FEEDS)
    return fetch_feed_specs(specs, "supply_chain_media", SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED)


def _unwrap_wayback_url(url_str):
    """Strip Wayback Machine /web/<timestamp>/ prefix if present so canonical domain links are preserved."""
    if not url_str:
        return ""
    return re.sub(r"^https?://web\.archive\.org/web/\d+(?:id_|[a-z_]+)?/", "", url_str.strip())


def _parse_html_articles(html_text, base_url, label, url_pattern, ignore_patterns, max_items):
    """Extract article items from raw HTML via JSON-LD metadata and DOM anchor scanning."""
    site_items = []
    seen_urls = set()
    base_netloc = urlsplit(base_url).netloc.lower().lstrip("www.")
    soup = BeautifulSoup(html_text, "html.parser")

    for script_tag in soup.find_all("script", type=re.compile(r"ld\+json", re.I)):
        raw_json = script_tag.string or script_tag.get_text() or ""
        if not raw_json.strip():
            continue
        try:
            ld_data = json.loads(raw_json)
        except Exception:
            continue
        ld_nodes = ld_data if isinstance(ld_data, list) else [ld_data]
        expanded = []
        for node in ld_nodes:
            if isinstance(node, dict):
                expanded.append(node)
                if isinstance(node.get("@graph"), list):
                    expanded.extend(n for n in node["@graph"] if isinstance(n, dict))
                if isinstance(node.get("itemListElement"), list):
                    for el in node["itemListElement"]:
                        if isinstance(el, dict):
                            expanded.append(el.get("item") if isinstance(el.get("item"), dict) else el)
        for node in expanded:
            if not isinstance(node, dict):
                continue
            raw_link = _unwrap_wayback_url(str(node.get("url") or node.get("@id") or ""))
            title = clean_text(node.get("headline") or node.get("name") or "")
            if not raw_link or len(title) < 15:
                continue
            full_url = urljoin(base_url, raw_link)
            parsed_url = urlsplit(full_url)
            if parsed_url.netloc.lower().lstrip("www.") != base_netloc:
                continue
            if url_pattern and not re.search(url_pattern, full_url):
                continue
            if not url_pattern and (ignore_patterns.search(parsed_url.path) or len(parsed_url.path.strip("/")) < 4):
                continue
            norm_url = normalize_url(full_url)
            if norm_url in seen_urls:
                continue
            seen_urls.add(norm_url)
            pub_date = clean_text(node.get("datePublished") or node.get("dateModified") or utc_now().isoformat())
            summary = clean_text(node.get("description") or title)
            site_items.append({
                "title": title,
                "url": norm_url,
                "source": f"Web / {label}",
                "source_type": "scraped_web",
                "published_at": pub_date,
                "category": "Frontier Deep-Tech",
                "raw_summary": summary[:2200] if summary else title,
            })
            if len(site_items) >= max_items:
                return site_items

    for a in soup.find_all("a", href=True):
        href = _unwrap_wayback_url(a["href"].strip())
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        full_url = _unwrap_wayback_url(urljoin(base_url, href))
        parsed_url = urlsplit(full_url)

        if parsed_url.netloc.lower().lstrip("www.") != base_netloc:
            continue

        if url_pattern:
            if not re.search(url_pattern, full_url):
                continue
        else:
            if ignore_patterns.search(parsed_url.path) or len(parsed_url.path.strip("/")) < 4:
                continue

        norm_url = normalize_url(full_url)
        if norm_url in seen_urls:
            continue

        parent = a.find_parent(["article", "li", "div", "section"])
        title = clean_text(a.get_text())
        if len(title) < 15:
            heading = a.find(["h1", "h2", "h3", "h4", "span"])
            if heading:
                title = clean_text(heading.get_text())
        if len(title) < 15:
            title = clean_text(a.get("title") or a.get("aria-label") or "")
        if len(title) < 15 and parent:
            parent_heading = parent.find(["h1", "h2", "h3", "h4"])
            if parent_heading:
                title = clean_text(parent_heading.get_text())
        if len(title) < 15:
            continue

        seen_urls.add(norm_url)

        pub_date = utc_now().isoformat()
        date_match = re.search(r"/(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", full_url)
        if date_match:
            y, m, d = date_match.groups()
            pub_date = f"{y}-{int(m):02d}-{int(d):02d}T00:00:00+00:00"

        summary = ""
        category = "Frontier Deep-Tech"
        if parent:
            if not date_match:
                time_tag = parent.find("time")
                if time_tag and time_tag.get("datetime"):
                    pub_date = time_tag["datetime"]

            p_tag = parent.find("p")
            if p_tag:
                summary = clean_text(p_tag.get_text())
            else:
                summary = clean_text(parent.get_text()).replace(title, "").strip()[:400]

            cat_tag = parent.find("a", href=re.compile(r"/category/"))
            if cat_tag:
                category = clean_text(cat_tag.get_text())

        site_items.append({
            "title": title,
            "url": norm_url,
            "source": f"Web / {label}",
            "source_type": "scraped_web",
            "published_at": pub_date,
            "category": category,
            "raw_summary": summary[:2200] if summary else title,
        })

        if len(site_items) >= max_items:
            break

    return site_items


def _scrape_wp_json_fallback(base_url, label, max_items):
    """Fallback for WordPress sites when HTML/RSS paths fail.
    Returns (items, status) where status is 'ok', 'not_modified', or 'miss'.
    """
    parsed = urlsplit(base_url)
    wp_api_url = f"{parsed.scheme}://{parsed.netloc}/wp-json/wp/v2/posts?per_page={max_items}&_fields=link,title,excerpt,date_gmt"
    if feed_should_skip_live(wp_api_url):
        return [], "miss"

    cached = feed_get_cache_headers(wp_api_url)
    headers = {
        **HTML_BROWSER_HEADERS,
        "Accept": "application/json,text/plain,*/*",
        "Referer": f"{parsed.scheme}://{parsed.netloc}/",
    }
    if cached.get("etag"):
        headers["If-None-Match"] = cached["etag"]
    if cached.get("last_modified"):
        headers["If-Modified-Since"] = cached["last_modified"]

    try:
        resp = requests.get(wp_api_url, headers=headers, timeout=15)
        if resp.status_code == 304:
            feed_record_result(wp_api_url, success=True)
            return [], "not_modified"
        if resp.status_code in (403, 404, 410):
            feed_record_result(wp_api_url, success=False, permanent=(resp.status_code in (404, 410)))
            return [], "miss"
        if resp.status_code != 200:
            feed_record_result(wp_api_url, success=False)
            return [], "miss"

        posts = resp.json()
        if not isinstance(posts, list):
            feed_record_result(wp_api_url, success=False)
            return [], "miss"
        items = []
        for post in posts[:max_items]:
            link = normalize_url(str(post.get("link") or ""))
            title_obj = post.get("title") or {}
            raw_title = title_obj.get("rendered", "") if isinstance(title_obj, dict) else str(title_obj)
            title = clean_text(re.sub(r"<[^>]+>", " ", raw_title))
            if not link or len(title) < 10:
                continue
            excerpt_obj = post.get("excerpt") or {}
            raw_excerpt = excerpt_obj.get("rendered", "") if isinstance(excerpt_obj, dict) else str(excerpt_obj)
            summary = clean_text(re.sub(r"<[^>]+>", " ", raw_excerpt)) or title
            pub_date = clean_text(post.get("date_gmt") or utc_now().isoformat())
            if pub_date and not pub_date.endswith("Z") and "+" not in pub_date:
                pub_date = f"{pub_date}+00:00"
            items.append({
                "title": title,
                "url": link,
                "source": f"Web / {label}",
                "source_type": "scraped_web",
                "published_at": pub_date,
                "category": "Frontier Deep-Tech",
                "raw_summary": summary[:2200],
            })
        if items:
            feed_record_result(wp_api_url, success=True)
            feed_set_cache_headers(
                wp_api_url,
                etag=resp.headers.get("ETag"),
                last_modified=resp.headers.get("Last-Modified"),
            )
            return items, "ok"
        return [], "miss"
    except Exception:
        feed_record_result(wp_api_url, success=False)
        return [], "miss"


def _scrape_wayback_html_fallback(base_url, label, url_pattern, ignore_patterns, max_items):
    """Fallback that scrapes raw HTML from a recent Wayback Machine snapshot (id_ mode).
    Returns (items, status) where status is 'ok', 'unchanged', or 'miss'.
    """
    content, status = _fetch_wayback_snapshot(base_url, HTML_BROWSER_HEADERS, label, timeout=25)
    if status == "unchanged":
        return [], "unchanged"
    if status != "ok" or not content:
        return [], "miss"
    try:
        html_text = content.decode("utf-8", errors="replace")
        items = _parse_html_articles(html_text, base_url, label, url_pattern, ignore_patterns, max_items)
        return (items, "ok") if items else ([], "miss")
    except Exception:
        return [], "miss"


def _scrape_domain_index_fallback(base_url, label, max_items):
    """Last-resort domain index scraper via Google News RSS with conditional GET caching."""
    clean_domain = urlsplit(base_url).netloc.lower().lstrip("www.")
    if not clean_domain or any(blocked in clean_domain for blocked in _NON_HTML_FALLBACK_HOSTS):
        return []
    query_url = f"https://news.google.com/rss/search?q=site:{clean_domain}+when:7d&hl=en-US&gl=US&ceid=US:en"
    raw_items, status, _ = _fetch_rss_feed_with_status(
        query_url,
        source_label=label,
        source_type="scraped_web",
        max_items=max_items,
        enriched=False,
        allow_wayback=False,
    )
    if status != "ok" or not raw_items:
        return []

    items = []
    for item in raw_items:
        title = item["title"]
        if " - " in title:
            title_parts = title.rsplit(" - ", 1)
            if len(title_parts[0]) >= 15:
                title = title_parts[0].strip()
        summary = clean_text(re.sub(r"<[^>]+>", " ", item.get("raw_summary", "")))
        items.append({
            "title": title,
            "url": item["url"],
            "source": f"Web / {label}",
            "source_type": "scraped_web",
            "published_at": item.get("published_at", utc_now().isoformat()),
            "category": "Frontier Deep-Tech",
            "raw_summary": summary[:2200] if summary else title,
        })
    return items


def scrape_single_html_target(label, target, max_items=None):
    """Scrape a single HTML target spec ('URL;;regex' or 'URL') with 4-stage 403 resilience,
    conditional GET (304) awareness, and per-stage failure cooldowns.
    """
    max_items = max_items or HTML_SCRAPER_MAX_ITEMS_PER_SITE
    ignore_patterns = re.compile(
        r"/(about|contact|privacy|terms|login|signup|register|tag|category|author|cart|search|feed|"
        r"segment|section|events|subscribe|podcasts?|videos?|newsletters?|advertise|sponsors?|"
        r"careers?|staff|team|policies|cookie)/?",
        re.IGNORECASE,
    )
    if ";;" in target:
        base_url, url_pattern = target.split(";;", 1)
        base_url, url_pattern = base_url.strip(), url_pattern.strip()
    else:
        base_url, url_pattern = target.strip(), None

    parsed_base = urlsplit(base_url)
    site_items = []

    # Stage 1: Direct HTML fetch with conditional GET and failure cooldown
    if not feed_should_skip_live(base_url):
        cached = feed_get_cache_headers(base_url)
        headers = {
            **HTML_BROWSER_HEADERS,
            "Referer": f"{parsed_base.scheme}://{parsed_base.netloc}/",
        }
        if cached.get("etag"):
            headers["If-None-Match"] = cached["etag"]
        if cached.get("last_modified"):
            headers["If-Modified-Since"] = cached["last_modified"]

        try:
            resp = requests.get(base_url, headers=headers, timeout=20)
            if resp.status_code == 304:
                feed_record_result(base_url, success=True)
                log.debug(f"html_scraper: 304 Not Modified for '{label}'")
                return []
            if resp.status_code == 200:
                site_items = _parse_html_articles(
                    resp.text, base_url, label, url_pattern, ignore_patterns, max_items
                )
                if site_items:
                    feed_record_result(base_url, success=True)
                    feed_set_cache_headers(
                        base_url,
                        etag=resp.headers.get("ETag"),
                        last_modified=resp.headers.get("Last-Modified"),
                    )
            else:
                entered_cd = feed_record_result(
                    base_url, success=False, permanent=(resp.status_code in (404, 410))
                )
                suffix = f" (entering {FEED_COOLDOWN_MINUTES}m cooldown)" if entered_cd else ""
                log.info(
                    f"html_scraper: direct fetch for '{label}' returned HTTP {resp.status_code}{suffix}; trying fallbacks..."
                )
        except Exception as exc:
            feed_record_result(base_url, success=False)
            log.info(f"html_scraper: direct fetch exception for '{label}': {repr(exc)}; trying fallbacks...")
    else:
        log.debug(f"html_scraper: direct fetch on cooldown for '{label}'; using fallbacks.")

    # Stage 2: WordPress REST API fallback
    if not site_items:
        wp_items, wp_status = _scrape_wp_json_fallback(base_url, label, max_items)
        if wp_status == "not_modified":
            log.debug(f"html_scraper: WP REST API 304 Not Modified for '{label}'")
            return []
        if wp_items:
            site_items = wp_items
            log.info(f"html_scraper: recovered {len(site_items)} items for '{label}' via WP REST API")

    # Stage 3: Fresh Wayback Machine HTML snapshot fallback
    if not site_items:
        wb_items, wb_status = _scrape_wayback_html_fallback(
            base_url, label, url_pattern, ignore_patterns, max_items
        )
        if wb_status == "unchanged":
            log.debug(f"html_scraper: Wayback snapshot unchanged for '{label}'")
            return []
        if wb_items:
            site_items = wb_items
            log.info(f"html_scraper: recovered {len(site_items)} items for '{label}' via Wayback HTML snapshot")

    # Stage 4: Google News domain index fallback (conditional GET cached)
    if not site_items:
        site_items = _scrape_domain_index_fallback(base_url, label, max_items)
        if site_items:
            log.info(f"html_scraper: recovered {len(site_items)} items for '{label}' via domain index fallback")

    site_items.sort(key=lambda x: x["published_at"], reverse=True)
    log.info(f"html_scraper: scraped {len(site_items)} items from '{label}'")
    return site_items


def fetch_universal_html_scrapers():
    """Fetches and parses articles from all sites defined in HTML_SCRAPER_TARGETS."""
    if not ENABLE_HTML_SCRAPER:
        return []

    targets = get_feed_specs("HTML_SCRAPER_TARGETS", DEFAULT_HTML_SCRAPER_TARGETS)
    if not targets:
        return []

    workers = max(1, min(RSS_FETCH_WORKERS, len(targets)))
    if workers == 1:
        all_scraped_items = []
        for label, target in targets:
            try:
                all_scraped_items.extend(scrape_single_html_target(label, target, HTML_SCRAPER_MAX_ITEMS_PER_SITE))
            except Exception as exc:
                log.error(f"html_scraper error scraping '{label}': {repr(exc)}")
        return all_scraped_items

    results_by_idx = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_idx = {
            pool.submit(scrape_single_html_target, label, target, HTML_SCRAPER_MAX_ITEMS_PER_SITE): idx
            for idx, (label, target) in enumerate(targets)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                results_by_idx[idx] = future.result() or []
            except Exception as exc:
                label = targets[idx][0]
                log.error(f"html_scraper error scraping '{label}': {repr(exc)}")
                results_by_idx[idx] = []

    all_scraped_items = []
    for idx in range(len(targets)):
        all_scraped_items.extend(results_by_idx.get(idx, []))
    return all_scraped_items


def fetch_sec_edgar():
    """Search all SEC EDGAR filings via EFTS full-text search for Frontier Deep-Tech signals."""
    items = []
    if not ENABLE_SEC_EDGAR or not SEC_EFTS_QUERY:
        return items

    headers = {
        "User-Agent": SEC_USER_AGENT,
        "Accept": "application/json",
    }
    now_utc = utc_now()
    start_dt = (now_utc - dt.timedelta(days=max(1, SEC_LOOKBACK_DAYS))).strftime("%Y-%m-%d")
    end_dt = now_utc.strftime("%Y-%m-%d")
    allowed_forms = {f.strip().upper() for f in SEC_FORMS if f.strip()}

    params = {
        "q": SEC_EFTS_QUERY,
        "startdt": start_dt,
        "enddt": end_dt,
    }
    if allowed_forms:
        params["forms"] = ",".join(sorted(allowed_forms))

    url = "https://efts.sec.gov/LATEST/search-index"

    try:
        response = requests.get(url, params=params, headers=headers, timeout=35)
        if response.status_code != 200:
            log.warning(f"SEC EFTS issue: {response.status_code} {response.text[:160]}")
            return items

        data = response.json() or {}
        if "errorType" in data:
            log.warning(f"SEC EFTS query error: {data.get('errorMessage', data.get('errorType'))}")
            return items

        hits = (data.get("hits") or {}).get("hits") or []
        seen_accessions = set()

        for hit in hits:
            if len(items) >= SEC_MAX_ITEMS_PER_FETCH:
                break

            src = hit.get("_source") or {}
            adsh = clean_text(src.get("adsh", ""))
            if not adsh or adsh in seen_accessions:
                continue

            form = clean_text(src.get("form", "")).upper()
            root_forms = [clean_text(rf).upper() for rf in (src.get("root_forms") or [])]
            if allowed_forms and form not in allowed_forms and not any(rf in allowed_forms for rf in root_forms):
                continue

            ciks = src.get("ciks") or []
            if not ciks:
                continue
            cik_int = str(int(re.sub(r"\D", "", str(ciks[0])) or 0))
            adsh_nodash = adsh.replace("-", "")
            hit_id = hit.get("_id", "")
            filename = hit_id.split(":", 1)[1] if ":" in hit_id else ""

            if filename:
                filing_url = normalize_url(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{adsh_nodash}/{filename}")
            else:
                filing_url = normalize_url(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{adsh_nodash}/{adsh}-index.htm")

            display_names = src.get("display_names") or []
            company_label = clean_text(display_names[0]) if display_names else f"CIK {ciks[0]}"
            file_date = clean_text(src.get("file_date", ""))
            period_ending = clean_text(src.get("period_ending", ""))
            file_desc = clean_text(src.get("file_description", "") or src.get("file_type", ""))
            sics = [clean_text(s) for s in (src.get("sics") or []) if s]
            form_items = [clean_text(it) for it in (src.get("items") or []) if it]

            seen_accessions.add(adsh)

            title_desc = f": {file_desc}" if file_desc and file_desc.upper() != form else ""
            title = f"{company_label} - Form {form}{title_desc} ({file_date})"

            summary_parts = [f"Filer: {company_label}", f"Form: {form}", f"Filed: {file_date}"]
            if period_ending:
                summary_parts.append(f"Period: {period_ending}")
            if sics:
                summary_parts.append(f"SIC: {', '.join(sics)}")
            if form_items:
                summary_parts.append(f"8-K Items: {', '.join(form_items)}")
            if file_desc:
                summary_parts.append(f"Document: {file_desc}")

            items.append({
                "title": title,
                "url": filing_url,
                "source": "SEC EDGAR / Full-Text Search",
                "source_type": "sec_filing",
                "published_at": f"{file_date}T00:00:00+00:00" if file_date else utc_now().isoformat(),
                "raw_summary": " | ".join(summary_parts)[:2200],
            })

        log.info(f"sec_edgar: {len(items)} frontier filings matched via EFTS ({start_dt} to {end_dt})")
    except Exception as exc:
        log.error(f"SEC EDGAR EFTS exception: {repr(exc)}")

    return items
