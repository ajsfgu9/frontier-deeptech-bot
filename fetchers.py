from core import *

# =========================
# Source fetchers
# =========================


ARXIV_HEADERS = {
    "User-Agent": "frontier-deeptech-trend-bot/1.0 (contact: your-real-email@example.com)"
}


def _fetch_arxiv_atom(url, max_retries=3, timeout=15):
    """Direct fetch + parse of the arXiv Atom API, bypassing fetch_standard_rss
    so category tags survive (fetch_standard_rss normalizes entries down to
    title/url/summary and drops <category> tags, which is what caused every
    bucket to come back 0/20 the first time this was tried). Minimal
    retry/backoff since we lose fetch_standard_rss's shared resilience
    wrapper (Wayback fallback, cooldown, conditional GET) for this source."""
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
    """Field names here MUST match what fetch_and_enqueue()/insert_item()
    expect (url, source, source_type, published_at, raw_summary) — NOT the
    raw feedparser/Atom names (link, published, summary)."""
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
    """Fetch ARXIV_RESULTS_PER_CATEGORY items for each configured category
    in a single combined API call."""
    items = []
    if not ENABLE_ARXIV or not ARXIV_CATEGORIES:
        return items

    combined_query = "(" + " OR ".join(f"cat:{c}" for c in ARXIV_CATEGORIES) + ")"
    max_results = ARXIV_RESULTS_PER_CATEGORY * len(ARXIV_CATEGORIES) * 10

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
    for entry in raw_entries:
        entry_categories = [t.get("term") for t in entry.get("tags", []) if t.get("term")]
        for cat in ARXIV_CATEGORIES:
            if cat in entry_categories and len(buckets[cat]) < ARXIV_RESULTS_PER_CATEGORY:
                buckets[cat].append(_parse_arxiv_entry(entry, cat))

    for cat, bucketed in buckets.items():
        if len(bucketed) < ARXIV_RESULTS_PER_CATEGORY:
            log.warning(
                f"arXiv / {cat}: only {len(bucketed)} of "
                f"{ARXIV_RESULTS_PER_CATEGORY} requested items found in combined fetch"
            )
        items.extend(bucketed)

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

    session = requests.Session()
    workers = max(1, min(HN_FETCH_WORKERS, len(fresh_ids)))
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


def fetch_standard_rss(
    feed_url, source_label=None, source_type="rss", max_items=25
):
  """Fetch an RSS/Atom feed with browser headers, conditional GET,
  cooldown tracking, and a Wayback Machine fallback.
  """
  if feed_should_skip_live(feed_url):
    log.info(
        f"RSS feed on cooldown, skipping attempt: {source_label or feed_url}"
    )
    return []

  items = []
  max_items = max_items or 25
  feed = None
  raw_content = None
  live_status = None
  not_modified = False

  request_headers = dict(REQUEST_HEADERS)
  cached_headers = FEED_CACHE_HEADERS.get(feed_url) or {}
  if cached_headers.get("etag"):
    request_headers["If-None-Match"] = cached_headers["etag"]
  if cached_headers.get("last_modified"):
    request_headers["If-Modified-Since"] = cached_headers["last_modified"]

  try:
    response = requests.get(feed_url, headers=request_headers, timeout=35)
    live_status = response.status_code
    if response.status_code == 304:
      not_modified = True
    elif response.status_code == 200:
      raw_content = response.content
      if response.headers.get("ETag") or response.headers.get("Last-Modified"):
        FEED_CACHE_HEADERS[feed_url] = {
            "etag": response.headers.get("ETag", cached_headers.get("etag")),
            "last_modified": response.headers.get(
                "Last-Modified", cached_headers.get("last_modified")
            ),
        }
    else:
      log.info(
          f"RSS live fetch failed for {source_label or feed_url}:"
          f" {response.status_code}"
      )
  except requests.exceptions.Timeout:
    log.info(f"RSS timeout for {source_label or feed_url}.")
  except Exception as exc:
    log.info(f"RSS requests issue for {source_label or feed_url}: {repr(exc)}")

  if not_modified:
    feed_record_result(feed_url, success=True)
    return items

  if raw_content:
    try:
      feed = feedparser.parse(raw_content)
    except Exception as exc:
      log.info(f"RSS feedparser parsing issue for {feed_url}: {repr(exc)}")
      feed = None

  live_entries_ok = bool(feed and getattr(feed, "entries", None))
  feed_record_result(feed_url, success=live_entries_ok)

  if not live_entries_ok:
    reason = (
        "blocked/challenge page or malformed feed"
        if raw_content is not None
        else f"status {live_status or 'request failed'}"
    )
    log.info(
        f"RSS live path yielded no entries for {source_label or feed_url}"
        f" ({reason}). Trying Wayback Machine..."
    )
    try:
      wb_api_url = f"https://archive.org/wayback/available?url={feed_url}"
      wb_resp = requests.get(wb_api_url, timeout=15)
      if wb_resp.status_code == 200:
        snapshots = wb_resp.json().get("archived_snapshots", {})
        if "closest" in snapshots and snapshots["closest"].get("available"):
          snapshot_url = re.sub(
              r"/web/(\d+)/",
              r"/web/\1id_/",
              snapshots["closest"]["url"].replace("http://", "https://"),
          )
          log.info(
              f"Wayback snapshot found for {source_label or feed_url}."
              " Fetching..."
          )
          snap_resp = requests.get(
              snapshot_url, headers=REQUEST_HEADERS, timeout=30
          )
          if snap_resp.status_code == 200:
            wb_feed = feedparser.parse(snap_resp.content)
            if getattr(wb_feed, "entries", None):
              feed = wb_feed
    except Exception as wb_exc:
      log.info(
          f"Wayback Machine RSS fallback failed for {feed_url}: {repr(wb_exc)}"
      )

  if not feed or not getattr(feed, "entries", None):
    return items

  source_name = source_label or (
      clean_text(feed.feed.get("title", feed_url))
      if getattr(feed, "feed", None)
      else feed_url
  )
  for entry in getattr(feed, "entries", [])[:max_items]:
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
        "published_at": entry.get(
            "published", entry.get("updated", utc_now().isoformat())
        ),
        "raw_summary": summary[:2200],
    })

  return items

def fetch_standard_rss_2(
    feed_url, source_label=None, source_type="custom_rss", max_items=None
):
  """Enriched RSS/Atom fetcher: extracts <content:encoded>, tags/categories,
  author, and handles stubs.
  """
  if feed_should_skip_live(feed_url):
    log.info(
        f"RSS feed on cooldown, skipping attempt: {source_label or feed_url}"
    )
    return []

  items = []
  max_items = max_items or 25
  feed = None
  raw_content = None
  live_status = None
  not_modified = False

  request_headers = dict(REQUEST_HEADERS)
  cached_headers = FEED_CACHE_HEADERS.get(feed_url) or {}
  if cached_headers.get("etag"):
    request_headers["If-None-Match"] = cached_headers["etag"]
  if cached_headers.get("last_modified"):
    request_headers["If-Modified-Since"] = cached_headers["last_modified"]

  try:
    response = requests.get(feed_url, headers=request_headers, timeout=35)
    live_status = response.status_code
    if response.status_code == 304:
      not_modified = True
    elif response.status_code == 200:
      raw_content = response.content
      if response.headers.get("ETag") or response.headers.get("Last-Modified"):
        FEED_CACHE_HEADERS[feed_url] = {
            "etag": response.headers.get("ETag", cached_headers.get("etag")),
            "last_modified": response.headers.get(
                "Last-Modified", cached_headers.get("last_modified")
            ),
        }
    else:
      log.info(
          f"RSS live fetch failed for {source_label or feed_url}:"
          f" {response.status_code}"
      )
  except requests.exceptions.Timeout:
    log.info(f"RSS timeout for {source_label or feed_url}.")
  except Exception as exc:
    log.info(f"RSS requests issue for {source_label or feed_url}: {repr(exc)}")

  if not_modified:
    feed_record_result(feed_url, success=True)
    return items

  if raw_content:
    try:
      feed = feedparser.parse(raw_content)
    except Exception as exc:
      log.info(f"RSS feedparser parsing issue for {feed_url}: {repr(exc)}")
      feed = None

  live_entries_ok = bool(feed and getattr(feed, "entries", None))
  feed_record_result(feed_url, success=live_entries_ok)

  if not live_entries_ok:
    reason = (
        "blocked/challenge page or malformed feed"
        if raw_content is not None
        else f"status {live_status or 'request failed'}"
    )
    log.info(
        f"RSS live path yielded no entries for {source_label or feed_url}"
        f" ({reason}). Trying Wayback Machine..."
    )
    try:
      wb_api_url = f"https://archive.org/wayback/available?url={feed_url}"
      wb_resp = requests.get(wb_api_url, timeout=15)
      if wb_resp.status_code == 200:
        snapshots = wb_resp.json().get("archived_snapshots", {})
        if "closest" in snapshots and snapshots["closest"].get("available"):
          snapshot_url = re.sub(
              r"/web/(\d+)/",
              r"/web/\1id_/",
              snapshots["closest"]["url"].replace("http://", "https://"),
          )
          log.info(
              f"Wayback snapshot found for {source_label or feed_url}."
              " Fetching..."
          )
          snap_resp = requests.get(
              snapshot_url, headers=REQUEST_HEADERS, timeout=30
          )
          if snap_resp.status_code == 200:
            wb_feed = feedparser.parse(snap_resp.content)
            if getattr(wb_feed, "entries", None):
              feed = wb_feed
    except Exception as wb_exc:
      log.info(
          f"Wayback Machine RSS fallback failed for {feed_url}: {repr(wb_exc)}"
      )

  if not feed or not getattr(feed, "entries", None):
    return items

  source_name = source_label or (
      clean_text(feed.feed.get("title", feed_url))
      if getattr(feed, "feed", None)
      else feed_url
  )
  session = requests.Session()

  for entry in getattr(feed, "entries", [])[:max_items]:
    title = clean_text(entry.get("title", ""))
    link = normalize_url(entry.get("link", ""))
    if not title or not link:
      continue

    body = ""
    if "content" in entry and entry.content and isinstance(entry.content, list):
      body = clean_text(
          re.sub(r"<[^>]+>", " ", entry.content[0].get("value", ""))
      )
    if not body:
      body = clean_text(
          re.sub(
              r"<[^>]+>", " ", entry.get("summary", entry.get("description", ""))
          )
      )

    tags = []
    if "tags" in entry and entry.tags:
      for t in entry.tags:
        term = clean_text(t.get("term", ""))
        if term and term not in tags:
          tags.append(term)

    author = clean_text(entry.get("author", entry.get("dc_creator", "")))

    if len(body) < 120 and link:
      meta_snippet = fetch_webpage_summary_snippet(session, link, timeout=6)
      if meta_snippet and len(meta_snippet) > len(body):
        body = f"{body} (Lead: {meta_snippet})"

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
        "published_at": entry.get(
            "published", entry.get("updated", utc_now().isoformat())
        ),
        "raw_summary": " | ".join(substance) if substance else title,
    })

  return items


# =========================
# Expanded source fetchers
# =========================


def split_fallback_urls(url_value):
    raw = str(url_value or "")
    return [part.strip() for part in raw.split(";;") if part.strip()]


def fetch_feed_specs(specs, source_type, max_items_per_feed):
  items = []
  for label, feed_url in specs:
    total_for_label = 0
    candidate_urls = split_fallback_urls(feed_url)

    for candidate_url in candidate_urls:
      try:
        fetched = fetch_standard_rss(
            candidate_url,
            source_label=label,
            source_type=source_type,
            max_items=max_items_per_feed,
        )
        log.info(f"{source_type} feed: {len(fetched)} items from {label}")
        items.extend(fetched)
        total_for_label += len(fetched)
        if fetched:
          break
      except Exception as exc:
        log.error(
            f"{source_type} issue for {label} / {candidate_url}: {repr(exc)}"
        )
    if total_for_label == 0 and candidate_urls:
      parsed_root = urlsplit(candidate_urls[0])
      if parsed_root.scheme and parsed_root.netloc and "news.google.com" not in parsed_root.netloc:
        root_url = f"{parsed_root.scheme}://{parsed_root.netloc}/"
        log.info(f"{source_type}: RSS yielded 0 items for {label}; falling back to HTML scraper on {root_url}")
        scraped_fallback = scrape_single_html_target(label, root_url, max_items_per_feed)
        items.extend(scraped_fallback)
  return items


def fetch_feed_specs_2(specs, source_type, max_items_per_feed):
  items = []
  for label, feed_url in specs:
    total_for_label = 0
    candidate_urls = split_fallback_urls(feed_url)

    for candidate_url in candidate_urls:
      try:
        fetched = fetch_standard_rss_2(
            candidate_url,
            source_label=label,
            source_type=source_type,
            max_items=max_items_per_feed,
        )
        log.info(f"{source_type} feed: {len(fetched)} items from {label}")
        items.extend(fetched)
        total_for_label += len(fetched)
        if fetched:
          break
      except Exception as exc:
        log.error(
            f"{source_type} issue for {label} / {candidate_url}: {repr(exc)}"
        )
    if total_for_label == 0 and candidate_urls:
      parsed_root = urlsplit(candidate_urls[0])
      if parsed_root.scheme and parsed_root.netloc and "news.google.com" not in parsed_root.netloc:
        root_url = f"{parsed_root.scheme}://{parsed_root.netloc}/"
        log.info(f"{source_type}: RSS yielded 0 items for {label}; falling back to HTML scraper on {root_url}")
        scraped_fallback = scrape_single_html_target(label, root_url, max_items_per_feed)
        items.extend(scraped_fallback)
  return items

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
    """Fallback for WordPress sites when HTML/RSS paths return 403."""
    parsed = urlsplit(base_url)
    wp_api_url = f"{parsed.scheme}://{parsed.netloc}/wp-json/wp/v2/posts?per_page={max_items}&_fields=link,title,excerpt,date_gmt"
    headers = {**HTML_BROWSER_HEADERS, "Accept": "application/json,text/plain,*/*", "Referer": f"{parsed.scheme}://{parsed.netloc}/"}
    try:
        resp = requests.get(wp_api_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return []
        posts = resp.json()
        if not isinstance(posts, list):
            return []
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
        return items
    except Exception:
        return []


def _scrape_wayback_html_fallback(base_url, label, url_pattern, ignore_patterns, max_items):
    """Fallback that scrapes raw HTML from a recent Wayback Machine snapshot (id_ mode)."""
    try:
        wb_api_url = f"https://archive.org/wayback/available?url={base_url}"
        wb_resp = requests.get(wb_api_url, timeout=15)
        if wb_resp.status_code != 200:
            return []
        snapshots = (wb_resp.json() or {}).get("archived_snapshots") or {}
        closest = snapshots.get("closest") or {}
        if not closest.get("available") or not closest.get("url"):
            return []
        ts_str = str(closest.get("timestamp") or "")
        if len(ts_str) >= 8:
            try:
                snap_dt = dt.datetime.strptime(ts_str[:8], "%Y%m%d").replace(tzinfo=dt.timezone.utc)
                if (utc_now() - snap_dt).days > 7:
                    return []
            except Exception:
                pass
        snapshot_url = re.sub(
            r"/web/(\d+)/",
            r"/web/\1id_/",
            closest["url"].replace("http://", "https://"),
        )
        snap_resp = requests.get(snapshot_url, headers=HTML_BROWSER_HEADERS, timeout=25)
        if snap_resp.status_code != 200:
            return []
        return _parse_html_articles(snap_resp.text, base_url, label, url_pattern, ignore_patterns, max_items)
    except Exception:
        return []


def _scrape_domain_index_fallback(base_url, label, max_items):
    """Last-resort domain index scraper when Cloudflare blocks direct HTML, WP-JSON, and Wayback."""
    clean_domain = urlsplit(base_url).netloc.lower().lstrip("www.")
    if not clean_domain:
        return []
    query_url = f"https://news.google.com/rss/search?q=site:{clean_domain}+when:7d&hl=en-US&gl=US&ceid=US:en"
    try:
        resp = requests.get(query_url, headers=REQUEST_HEADERS, timeout=20)
        if resp.status_code != 200:
            return []
        feed = feedparser.parse(resp.content)
        items = []
        for entry in getattr(feed, "entries", [])[:max_items]:
            title = clean_text(entry.get("title", ""))
            if " - " in title:
                title_parts = title.rsplit(" - ", 1)
                if len(title_parts[0]) >= 15:
                    title = title_parts[0].strip()
            link = normalize_url(entry.get("link", ""))
            summary = clean_text(re.sub(r"<[^>]+>", " ", entry.get("summary", entry.get("description", ""))))
            if not title or not link:
                continue
            items.append({
                "title": title,
                "url": link,
                "source": f"Web / {label}",
                "source_type": "scraped_web",
                "published_at": entry.get("published", entry.get("updated", utc_now().isoformat())),
                "category": "Frontier Deep-Tech",
                "raw_summary": summary[:2200] if summary else title,
            })
        return items
    except Exception:
        return []


def scrape_single_html_target(label, target, max_items=None):
    """Scrape a single HTML target spec ('URL;;regex' or 'URL') with 4-stage 403 resilience."""
    max_items = max_items or HTML_SCRAPER_MAX_ITEMS_PER_SITE
    ignore_patterns = re.compile(
        r"/(about|contact|privacy|terms|login|signup|register|tag|category|author|cart|search|feed)/?",
        re.IGNORECASE,
    )
    if ";;" in target:
        base_url, url_pattern = target.split(";;", 1)
        base_url, url_pattern = base_url.strip(), url_pattern.strip()
    else:
        base_url, url_pattern = target.strip(), None

    parsed_base = urlsplit(base_url)
    headers = {
        **HTML_BROWSER_HEADERS,
        "Referer": f"{parsed_base.scheme}://{parsed_base.netloc}/",
    }

    site_items = []
    try:
        resp = requests.get(base_url, headers=headers, timeout=20)
        if resp.status_code == 200:
            site_items = _parse_html_articles(
                resp.text, base_url, label, url_pattern, ignore_patterns, max_items
            )
        else:
            log.info(
                f"html_scraper: direct fetch for '{label}' returned HTTP {resp.status_code}; trying fallbacks..."
            )
    except Exception as exc:
        log.info(f"html_scraper: direct fetch exception for '{label}': {repr(exc)}; trying fallbacks...")

    if not site_items:
        site_items = _scrape_wp_json_fallback(base_url, label, max_items)
        if site_items:
            log.info(f"html_scraper: recovered {len(site_items)} items for '{label}' via WP REST API")

    if not site_items:
        site_items = _scrape_wayback_html_fallback(
            base_url, label, url_pattern, ignore_patterns, max_items
        )
        if site_items:
            log.info(f"html_scraper: recovered {len(site_items)} items for '{label}' via Wayback HTML snapshot")

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
    all_scraped_items = []

    for label, target in targets:
        try:
            site_items = scrape_single_html_target(label, target, HTML_SCRAPER_MAX_ITEMS_PER_SITE)
            all_scraped_items.extend(site_items)
        except Exception as exc:
            log.error(f"html_scraper error scraping '{label}': {repr(exc)}")

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
