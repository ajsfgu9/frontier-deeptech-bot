from core import *
from core import _queue_lock, _feed_state_lock, _thread_local
from fetchers import *

_last_flush_time = utc_now()

# =========================
# Gemini / OpenRouter classification
# =========================


def parse_llm_json(text):
    if not text:
        raise ValueError("Empty response text.")

    text = text.strip()
    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()

    parsed = None
    try:
        parsed = json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            parsed = json.loads(text[start:end + 1])

    if not isinstance(parsed, dict):
        raise ValueError("OpenRouter response was not a JSON object.")
    if "results" not in parsed or not isinstance(parsed["results"], list):
        raise ValueError("OpenRouter response did not include a results list.")
    return parsed


def classify_batch(candidates):
    if not OPENROUTER_API_KEY:
        log.error(
            "Missing OPENROUTER_API_KEY. Classification skipped; batch will be requeued "
            "(capped by MAX_ITEM_RETRIES, currently %s)." % MAX_ITEM_RETRIES
        )
        return None

    compact_items = []
    for index, item in enumerate(candidates):
        signals = match_hardware_signals(item)
        matched_domains = ", ".join(s["domain"] for s in signals)
        matched_subs = "; ".join(sub for s in signals for sub in s["subtopics"])
        compact_items.append(
            {
                "index": index,
                "title": item.get("title", "")[:450],
                "source": item.get("source", "")[:120],
                "source_type": item.get("source_type", "")[:80],
                "summary": item.get("raw_summary", "")[:2200],
                "url": item.get("url", "")[:400],
                "published_at": item.get("published_at", "")[:80],
                "detected_hardware_signals": f"{matched_domains} ({matched_subs})"[:350] if matched_subs else "",
            }
        )

    system_prompt = """
You are a strict Frontier Deep-Tech, Hard Science & Algorithmic Systems filter for a monitoring bot.
Goal:
Identify concrete technical breakthroughs, experimental demonstrations, clinical/field trials, engineering milestones, or mathematical/protocol advances across all frontier science, deep-tech, and algorithmic domains outside terrestrial semiconductor/data center hardware. Treat all sources and all legitimate frontier deep-tech domains with equal importance — do NOT give preferential or exclusive weight to any subset of topics.

Examples of relevant frontier deep-tech domains (included, NOT exclusive — evaluate any other high-signal frontier science, energy, materials, biomedical, or computational breakthrough on an identical footing):
- Biotechnology & Synthetic Biology: base and prime genome editing (CRISPR-Cas9/Cas12/Cas13, base/prime editors, pegRNA, epigenome editing), de novo protein design (RFdiffusion, ProteinMPNN, AlphaFold, synthetic enzymes), epigenetic rejuvenation (partial cellular reprogramming, Yamanaka factors), programmable mRNA platforms (saRNA, circRNA, targeted LNP delivery), organoid bioprinting (vascularized 3D tissues, bioinks, organ-on-a-chip), and synthetic gene drives (synthetic chromosomes, minimal genomes, genetic circuits).
- Neurotechnology & Cognitive Interfaces: invasive intracortical brain-computer interfaces (intracortical BCIs, microelectrode arrays, ECoG, endovascular stentrode implants), non-invasive optical and magnetic neural decoders (fNIRS, OPM-MEG, focused ultrasound neuromodulation), and sensory optogenetic prosthetics (retinal/cochlear prostheses, bidirectional cortical neuroprosthetics).
- Aerospace, Space Exploration & Off-World Industry: fully reusable heavy launch vehicles (methalox full-flow staged combustion, rapid reusability, thermal protection systems, orbital propellant depots & cryogenic refueling), in-situ resource extraction (ISRU, lunar regolith oxygen extraction, molten regolith electrolysis, asteroid mining, microgravity manufacturing), nuclear thermal and electric space propulsion (NTP, NEP, Hall-effect/VASIMR/MPD thrusters, fission surface power), and orbital satellite crosslinks (OISL laser crosslinks, VLEO constellations, spaceborne SAR).
- Robotics & Physical Embodiment (Mechanical Kinematics & Actuation): general-purpose humanoid kinetics (bipedal locomotion, whole-body dynamic control, dexterous hand kinematics, quasi-direct drive / cycloidal actuators), soft elastomeric actuators (fluidic elastomers, dielectric elastomer actuators / DEAs), biomimetic artificial muscles (HASEL electrohydraulic actuators, shape-memory alloys, liquid crystal elastomers, CNT yarn muscles), and autonomous magnetic nanorobots (helical microswimmers, intravascular microrobots, biohybrid microrobots).
- Planetary Environmental Engineering: large-scale direct air capture (DAC solid sorbents, liquid calcination, electro-swing / moisture-swing adsorption, geologic CO2 mineralization), ocean alkalinity enhancement (OAE, electrochemical marine CDR / mCDR, direct ocean capture / DOC), and enhanced rock weathering (ERW crushed basalt/olivine/silicate mineralization).
- Pure Cryptography, Frontier Foundation Models, Test-Time Compute & Autonomous Agent Swarms: zero-knowledge proof protocols (zk-SNARKs, zk-STARKs, PlonK, folding schemes, zkVMs), post-quantum lattice cryptography (ML-KEM/Kyber, ML-DSA/Dilithium, SPHINCS+, LWE/RLWE), fully homomorphic encryption (TFHE, CKKS, BFV/BGV, programmable bootstrapping, MPC), Frontier Foundation Models & Test-Time Compute (dynamic inference-time reasoning and verification, process reward models / PRMs, MCTS / tree-of-thought search, formal verification & theorem proving), and Autonomous Agent Swarms (self-directing systems that plan, write code, execute workflows, and recursively optimize their own prompts and architectures).
- Other Frontier Deep-Tech & Hard Science: nuclear fusion & advanced grid/storage chemistry, novel superconductors & metamaterials, advanced industrial/chemical synthesis, or other verifiable scientific and engineering breakthroughs with concrete technical substance.

Reject / mark irrelevant (Permanent Exclusions):
- Terrestrial semiconductor hardware, data center GPUs/TPUs/ASICs, HBM/DRAM memory, CoWoS/TGV/FOPLP chip packaging, EUV lithography, liquid/immersion server cooling, and data center rack power delivery (also exclude standalone onboard inference chips without robotic kinematic/actuation focus).
- Generic corporate earnings, stock market commentary, cryptocurrency token price speculation, marketing partnerships, layoffs, or routine funding announcements without specific technical substance.
- Consumer electronics, routine SaaS web apps, or generic "AI-powered" product launches.

Scoring:
0-3 = Excluded domain (terrestrial semiconductor/data center hardware, generic business/finance, consumer tech) or no technical substance.
4-6 = Superficial mention of a deep-tech topic but weak, vague, or lacking concrete scientific/engineering detail.
7-8 = High-signal technical advance, peer-reviewed preprint, clinical/orbital/field milestone, or concrete protocol release in any non-silicon frontier deep-tech domain with a clear bottleneck addressed.
9-10 = Major scientific breakthrough, clinical trial success, orbital flight demonstration, large-scale engineering deployment, or algorithmic/cryptographic paradigm shift.

Strict rule:
Set relevant=true when score >= 7 and the item presents concrete scientific, engineering, or algorithmic substance in any frontier deep-tech domain outside terrestrial semiconductor/data center hardware. Do NOT reject valid frontier deep-tech breakthroughs merely because they fall outside the named example domains.

Return only valid JSON using this schema:
{
  "results": [
    {
      "index": 0,
      "relevant": true,
      "score": 8,
      "category": "Short domain category name",
      "hardware_domain": "biotech & synthetic biology / neurotechnology & cognitive interfaces / aerospace & off-world industry / robotics & physical embodiment / planetary environmental engineering / cryptography, frontier AI & autonomous agent swarms / other frontier deep-tech",
      "bottleneck_addressed": "Short technical bottleneck addressed",
      "specific_tech": "Short concrete technology name",
      "why_it_matters": "One or two factual sentences explaining the technical advance and mechanism.",
      "ai_revolution_thesis": "One sentence on how this shifts the frontier capability curve in its domain.",
      "telegram_summary": "Short Telegram-ready summary."
    }
  ]
}
"""

    user_prompt = {
        "instruction": "Classify every item for concrete frontier deep-tech, scientific, or algorithmic substance across all non-silicon domains (including, but not limited to, the listed example domains). Exclude terrestrial semiconductor/datacenter hardware and generic business news. Only output JSON.",
        "items": compact_items,
    }

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt.strip()},
            {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)}
        ],
        "temperature": 0.05,
        "response_format": {"type": "json_object"}
    }

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }

    url = "https://openrouter.ai/api/v1/chat/completions"

    for attempt in range(3):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=90,
            )

            if response.status_code == 200:
                data = response.json()
                text = data.get("choices", [{}])[0].get("message", {}).get("content", "")

                if DEBUG_LLM:
                    log.debug(f"OpenRouter raw response:\n{text[:2500]}")

                parsed = parse_llm_json(text)
                return parsed.get("results", [])

            if response.status_code in [429, 500, 502, 503, 504]:
                wait_seconds = 10 * (attempt + 1)
                log.warning(f"OpenRouter temporary error {response.status_code}. Retrying in {wait_seconds}s. {response.text[:500]}")
                time.sleep(wait_seconds)
                continue

            log.error(f"OpenRouter error: {response.status_code} {response.text[:1000]}")
            return None

        except Exception as exc:
            wait_seconds = 10 * (attempt + 1)
            log.warning(f"OpenRouter exception: {repr(exc)}. Retrying in {wait_seconds}s.")
            time.sleep(wait_seconds)

    return None


def classify_and_insert(candidates):
    inserted_count = 0
    processed_count = 0
    failed_items = []
    new_alerts = []

    batches = list(chunk_list(candidates, LLM_BATCH_SIZE))
    total_batches = len(batches)

    for i, batch in enumerate(batches, 1):
        log.info(f"Processing batch {i}/{total_batches} with OpenRouter ({len(batch)} items)...")
        
        results = classify_batch(batch)
        if results is None:
            failed_items.extend(batch)
            continue

        result_by_index = {}
        for result in results:
            try:
                index = int(result.get("index"))
            except Exception:
                continue
            if 0 <= index < len(batch):
                result_by_index[index] = result

        for index, original in enumerate(batch):
            result = result_by_index.get(index)

            if result is None:
                mark_url_processed(original, "rejected")
                processed_count += 1
                continue

            score = safe_float(result.get("score", 0))
            relevant = safe_bool(result.get("relevant"))
            processed_count += 1

            if not relevant or score < LLM_MIN_SCORE:
                mark_url_processed(original, "rejected")
                continue

            cap = LLM_TEXT_FIELD_MAX_CHARS
            enrich_item_with_hardware_signals(original)
            resolved_domain = clean_text(result.get("hardware_domain", "")) or original.get("hardware_domain_hint", "")
            resolved_bottleneck = clean_text(result.get("bottleneck_addressed", "")) or original.get("bottleneck_hint", "")
            item = {
                **original,
                "category": clean_text(result.get("category", ""))[:cap],
                "hardware_domain": resolved_domain[:cap],
                "bottleneck_addressed": resolved_bottleneck[:cap],
                "specific_tech": clean_text(result.get("specific_tech", ""))[:cap],
                "relevance_score": score,
                "why_it_matters": clean_text(result.get("why_it_matters", ""))[:cap],
                "ai_revolution_thesis": clean_text(result.get("ai_revolution_thesis", ""))[:cap],
                "telegram_summary": clean_text(result.get("telegram_summary", ""))[:cap],
                "raw_llm_json": json.dumps(result, ensure_ascii=False),
            }

            is_alert = score >= ALERT_THRESHOLD

            if insert_item(item, sent_alert=0, digest_sent=0):
                inserted_count += 1
                if is_alert:
                    new_alerts.append(item)

            mark_url_processed(original, "accepted")

        time.sleep(2)

    if new_alerts:
        new_alerts.sort(key=lambda x: safe_float(x.get("relevance_score", 0)), reverse=True)
        sent_urls = send_immediate_alerts(new_alerts)
        if sent_urls:
            conn = connect_db()
            cur = conn.cursor()
            digest_flag = 0 if INCLUDE_ALERTS_IN_DIGEST else 1
            cur.executemany(
                "UPDATE items SET sent_alert = 1, digest_sent = ? WHERE url = ?",
                [(digest_flag, url) for url in sent_urls],
            )
            conn.commit()
        skipped = len(new_alerts) - len(sent_urls)
        if skipped > 0:
            log.info(f"{skipped} alert-tier item(s) not sent as immediate alerts this flush; they remain eligible for the next digest.")

    return {
        "inserted": inserted_count,
        "processed": processed_count,
        "failed_items": failed_items,
    }


# =========================
# Source aggregation
# =========================


def fetch_all_sources() -> list:
    """Run every enabled fetcher and return capped items as a flat list."""
    fetchers = [
        ("arxiv", fetch_arxiv),
        ("hackernews", fetch_hackernews),
        ("techmeme", fetch_techmeme),
        ("specialist_rss", fetch_specialist_rss),
        ("supply_chain_rss", fetch_supply_chain_rss),
        ("html_scraper", fetch_universal_html_scrapers),
        ("sec_edgar", fetch_sec_edgar),
    ]
    all_items = []
    for name, fetcher in fetchers:
        try:
            items = fetcher()
            log.info(f"fetch_{name}: {len(items)} items")
            all_items.extend(items)
        except Exception as exc:
            log.error(f"fetch_{name} crash: {repr(exc)}")

    if SEND_RAW_FETCH_JSON and all_items:
        send_raw_json_dump(all_items)

    capped = fair_cap_items(all_items, MAX_CANDIDATES_PER_RUN)
    if len(capped) < len(all_items):
        log.info(f"Global cap applied: {len(capped)}/{len(all_items)} items kept this run.")
    return capped


# =========================
# Runtime — two-phase queue pipeline
# =========================


def touch_healthcheck():
    try:
        directory = os.path.dirname(HEALTHCHECK_FILE)
        target_file = HEALTHCHECK_FILE
        if directory:
            os.makedirs(directory, exist_ok=True)
            if not os.access(directory, os.W_OK):
                fallback_dir = "/tmp/frontier_deeptech_data"
                os.makedirs(fallback_dir, exist_ok=True)
                target_file = os.path.join(fallback_dir, os.path.basename(HEALTHCHECK_FILE))
        with open(target_file, "w") as fh:
            fh.write(utc_now().isoformat())
    except Exception as exc:
        log.warning(f"Could not write healthcheck file {HEALTHCHECK_FILE}: {repr(exc)}")


def fetch_and_enqueue():
    ts = utc_now().strftime("%H:%M:%S")
    log.info(f"[{ts}] Fetching all sources...")
    raw_items = fetch_all_sources()

    if SEND_RAW_FETCH_REPORT and raw_items:
        grouped_titles = defaultdict(list)
        for item in raw_items:
            source = item.get("source", "Unknown Source")
            title = clean_text(item.get("title", "No Title"))
            grouped_titles[source].append(title)

        report_lines = [f"Raw Fetch Report - {utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}"]
        report_lines.append(f"Total items fetched (pre-deduping): {len(raw_items)}")
        report_lines.append("=" * 60)

        for source, titles in sorted(grouped_titles.items()):
            report_lines.append(f"\n[{source}] ({len(titles)} items):")
            for title in titles:
                report_lines.append(f"  - {title}")

        report_content = "\n".join(report_lines)
        filename = f"raw_fetch_{utc_now().strftime('%Y%m%d_%H%M%S')}.txt"
        send_telegram_document(filename, report_content)

    new_items = []
    for item in raw_items:
        url = normalize_url(item.get("url", ""))
        if not url:
            continue
        item["url"] = url
        enrich_item_with_hardware_signals(item)
        fp = item_fingerprint(item)
        item["content_fingerprint"] = fp

        if url in SEEN_URLS_CACHE or fp in SEEN_FINGERPRINTS_CACHE:
            continue
        if processed_url_exists(url) or processed_fingerprint_exists(fp):
            continue
        new_items.append((url, fp, item))

    added = 0
    dropped = 0
    with _queue_lock:
        for url, fp, item in new_items:
            if url in SEEN_URLS_CACHE or fp in SEEN_FINGERPRINTS_CACHE:
                continue
            if len(CANDIDATE_QUEUE) >= CANDIDATE_QUEUE_MAX:
                dropped += 1
                continue

            SEEN_URLS_CACHE[url] = True
            if fp:
                SEEN_FINGERPRINTS_CACHE[fp] = True
            CANDIDATE_QUEUE.append(item)
            added += 1

        if len(SEEN_URLS_CACHE) > 20_000:
            oldest_urls = list(SEEN_URLS_CACHE.keys())[:10000]
            for old_url in oldest_urls:
                del SEEN_URLS_CACHE[old_url]

            oldest_fps = list(SEEN_FINGERPRINTS_CACHE.keys())[:10000]
            for old_fp in oldest_fps:
                del SEEN_FINGERPRINTS_CACHE[old_fp]

            for q_item in CANDIDATE_QUEUE:
                q_url = q_item.get("url", "")
                q_fp = q_item.get("content_fingerprint", "")
                if q_url:
                    SEEN_URLS_CACHE[q_url] = True
                if q_fp:
                    SEEN_FINGERPRINTS_CACHE[q_fp] = True

        depth = len(CANDIDATE_QUEUE)

    if dropped:
        log.warning(f"[{ts}] Queue full. Dropped {dropped} overflow item(s). Increase CANDIDATE_QUEUE_MAX if needed.")
    log.info(f"[{ts}] +{added} new items queued. Depth: {depth}")
    touch_healthcheck()


def requeue_failed_items(items, max_retries=None):
    if not items:
        return 0
    max_retries = MAX_ITEM_RETRIES if max_retries is None else max_retries

    added = 0
    permanently_dropped = []
    with _queue_lock:
        for item in items:
            item["_retry_count"] = item.get("_retry_count", 0) + 1
            if item["_retry_count"] > max_retries:
                permanently_dropped.append(item)
                continue
            if len(CANDIDATE_QUEUE) >= CANDIDATE_QUEUE_MAX:
                break
            CANDIDATE_QUEUE.append(item)
            added += 1

    for item in permanently_dropped:
        mark_url_processed(item, "failed_permanent")
    if permanently_dropped:
        log.error(f"Dropped {len(permanently_dropped)} item(s) after exceeding {max_retries} retries.")

    return added


def flush_queue():
    global _last_flush_time

    with _queue_lock:
        if not CANDIDATE_QUEUE:
            _last_flush_time = utc_now()
            return
        process_count = min(len(CANDIDATE_QUEUE), MAX_CANDIDATES_PER_FLUSH)
        batch = CANDIDATE_QUEUE[:process_count]
        del CANDIDATE_QUEUE[:process_count]
        remaining = len(CANDIDATE_QUEUE)

    ts = utc_now().strftime("%H:%M:%S")
    log.info(f"[{ts}] Flushing {len(batch)} item(s) through OpenRouter. Remaining queued: {remaining}")

    result = classify_and_insert(batch)

    failed_items = result.get("failed_items", [])
    if failed_items:
        requeued = requeue_failed_items(failed_items)
        log.warning(f"Requeued {requeued}/{len(failed_items)} failed item(s) for later retry.")

    inserted = result.get("inserted", 0)
    processed = result.get("processed", 0)
    log.info(f"OpenRouter processed {processed} item(s); inserted {inserted} relevant item(s).")

    _last_flush_time = utc_now()
    touch_healthcheck()


def drain_queue(max_flushes=None):
    if max_flushes is None:
        max_flushes = (CANDIDATE_QUEUE_MAX // max(1, MAX_CANDIDATES_PER_FLUSH)) + 2
    flush_count = 0
    while True:
        with _queue_lock:
            if not CANDIDATE_QUEUE:
                break
        if flush_count >= max_flushes:
            log.warning(f"[{utc_now().strftime('%H:%M:%S')}] Max flush cycles reached this call. Remaining items wait for the next flush check.")
            break
        flush_queue()
        flush_count += 1


def run_pipeline():
    fetch_and_enqueue()
    drain_queue()


def maybe_flush():
    with _queue_lock:
        depth = len(CANDIDATE_QUEUE)
        stale = depth > 0 and (utc_now() - _last_flush_time) >= dt.timedelta(minutes=FORCE_FLUSH_MINUTES)

    if depth == 0:
        return
    if depth >= QUEUE_FLUSH_LIMIT or stale:
        reason = "queue depth" if depth >= QUEUE_FLUSH_LIMIT else "force-flush timeout"
        log.info(f"maybe_flush triggered ({reason}); depth={depth}")
        drain_queue()


def log_flush_heartbeat():
    with _queue_lock:
        depth = len(CANDIDATE_QUEUE)
        last_flush = _last_flush_time.strftime("%Y-%m-%d %H:%M:%S UTC") if _last_flush_time else "Never"
    
    log.info(
        f"⏱️ [FLUSH HEARTBEAT] 1-second background check is active and healthy. "
        f"Current queue depth: {depth}/{CANDIDATE_QUEUE_MAX} | Last flush: {last_flush}"
    )


def print_config():
    log.info("Configuration:")
    log.info(f"DB_PATH: {DB_PATH}")
    log.info(f"OPENROUTER_MODEL: {OPENROUTER_MODEL}")
    log.info(f"FETCH_MINUTES: {FETCH_MINUTES} (fetch and flush now run on independent timers)")
    log.info(f"MAX_CANDIDATES_PER_RUN: {MAX_CANDIDATES_PER_RUN}")
    log.info(f"MAX_CANDIDATES_PER_FLUSH: {MAX_CANDIDATES_PER_FLUSH}")
    log.info(f"CANDIDATE_QUEUE_MAX: {CANDIDATE_QUEUE_MAX}")
    log.info(f"QUEUE_FLUSH_LIMIT: {QUEUE_FLUSH_LIMIT}")
    log.info(f"CHECK_QUEUE_SECONDS: {CHECK_QUEUE_SECONDS}")
    log.info(f"FORCE_FLUSH_MINUTES: {FORCE_FLUSH_MINUTES}")
    log.info(f"MAX_ITEM_RETRIES: {MAX_ITEM_RETRIES}")
    log.info(f"DIGEST_MINUTES: {DIGEST_MINUTES}")
    log.info(f"LLM_BATCH_SIZE: {LLM_BATCH_SIZE}")
    log.info(f"LLM_MIN_SCORE: {LLM_MIN_SCORE}")
    log.info(f"ALERT_THRESHOLD: {ALERT_THRESHOLD}")
    log.info(f"MAX_ALERTS_PER_FLUSH: {MAX_ALERTS_PER_FLUSH}")
    log.info(f"DIGEST_MAX_ITEMS: {DIGEST_MAX_ITEMS}")
    log.info(f"ENABLE_ARXIV: {ENABLE_ARXIV} categories: {ARXIV_CATEGORIES} per category: {ARXIV_RESULTS_PER_CATEGORY} workers: {ARXIV_FETCH_WORKERS}")
    log.info(f"ENABLE_HACKERNEWS: {ENABLE_HACKERNEWS} HN_NEW_STORIES_PER_RUN: {HN_NEW_STORIES_PER_RUN} workers: {HN_FETCH_WORKERS} high_water_mark: {get_hn_high_water_mark()}")
    log.info(f"ENABLE_TECHMEME: {ENABLE_TECHMEME} TECHMEME_MAX_ITEMS: {TECHMEME_MAX_ITEMS}")
    log.info(f"ENABLE_SPECIALIST_RSS: {ENABLE_SPECIALIST_RSS} feeds: {len(get_feed_specs('SPECIALIST_RSS_FEEDS', DEFAULT_SPECIALIST_RSS_FEEDS))} per feed: {SPECIALIST_RSS_MAX_ITEMS_PER_FEED}")
    log.info(f"ENABLE_SUPPLY_CHAIN_RSS: {ENABLE_SUPPLY_CHAIN_RSS} feeds: {len(get_feed_specs('SUPPLY_CHAIN_RSS_FEEDS', DEFAULT_SUPPLY_CHAIN_RSS_FEEDS))} per feed: {SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED}")
    log.info(f"ENABLE_HTML_SCRAPER: {ENABLE_HTML_SCRAPER} targets: {len(get_feed_specs('HTML_SCRAPER_TARGETS', DEFAULT_HTML_SCRAPER_TARGETS))} per site: {HTML_SCRAPER_MAX_ITEMS_PER_SITE}")
    log.info(f"ENABLE_SEC_EDGAR: {ENABLE_SEC_EDGAR} forms: {SEC_FORMS} lookback_days: {SEC_LOOKBACK_DAYS} max_items: {SEC_MAX_ITEMS_PER_FETCH}")
    log.info(f"Telegram token found: {bool(TELEGRAM_BOT_TOKEN)}")
    log.info(f"Telegram chat found: {bool(TELEGRAM_CHAT_ID)}")
    log.info(f"OpenRouter key found: {bool(OPENROUTER_API_KEY)}")


def main():
    init_db()
    print_config()

    if SEND_STARTUP_MESSAGE:
        send_telegram(
            f"✅ Frontier Deep-Tech trend analyzer started.\n"
            f"Fetch every {FETCH_MINUTES} min, flush check every {CHECK_QUEUE_SECONDS}s "
            f"(queue ≥{QUEUE_FLUSH_LIMIT} or {FORCE_FLUSH_MINUTES} min stale triggers a flush)."
        )

    run_pipeline()
    send_digest()

    scheduler = BackgroundScheduler(timezone="UTC")

    scheduler.add_job(
        fetch_and_enqueue,
        "interval",
        minutes=FETCH_MINUTES,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        maybe_flush,
        "interval",
        seconds=CHECK_QUEUE_SECONDS,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        send_digest,
        "interval",
        minutes=DIGEST_MINUTES,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        log_flush_heartbeat,
        "interval",
        hours=1,
        max_instances=1,
        coalesce=True,
    )

    class SilentExecutorsFilter(logging.Filter):
        def filter(self, record):
            msg = record.getMessage()
            if "maybe_flush" in msg and ("Running job" in msg or "executed successfully" in msg):
                return False
            return True

    logging.root.addFilter(SilentExecutorsFilter())
    for handler in logging.root.handlers:
        handler.addFilter(SilentExecutorsFilter())

    for name in ["apscheduler", "apscheduler.scheduler", "apscheduler.executors.default", "apscheduler.executors.pool"]:
        exec_log = logging.getLogger(name)
        exec_log.setLevel(logging.WARNING)
        exec_log.propagate = False
        exec_log.addFilter(SilentExecutorsFilter())

    scheduler.start()
    log.info("Scheduler active. Main thread blocking.")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Scheduler stopped.")

if __name__ == "__main__":
    main()
