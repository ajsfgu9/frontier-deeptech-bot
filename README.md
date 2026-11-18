# Frontier Deep-Tech Trend Analyzer & Alerter (`frontier-deeptech-bot`)

A multi-source scientific, engineering, and deep-tech intelligence pipeline cloned from the [`jijiji88888/ai-hardware-bot`](https://github.com/jijiji88888/ai-hardware-bot) architecture and dedicated to **6 Non-Silicon Frontier Deep-Tech Pillars** (while strictly excluding terrestrial data center silicon, chip packaging, and server rack cooling/power hardware covered by `ai-hardware-bot`).

## 6 Priority Frontier Deep-Tech Pillars Tracked Across All Stages

1. **Biotechnology & Synthetic Biology (`biotech & synthetic biology`)**

   * *Core sub-topics*:

     * Base and prime genome editing (`CRISPR-Cas9/Cas12/Cas13`, base editors, prime editors, `pegRNA`, epigenome editing).

     * De novo protein design (`RFdiffusion`, `ProteinMPNN`, `AlphaFold`, generative biomolecular structure engineering, synthetic enzymes).

     * Epigenetic rejuvenation (partial cellular reprogramming, Yamanaka factors, epigenetic clock reversal).

     * Programmable mRNA platforms (self-amplifying RNA / `saRNA`, circular RNA / `circRNA`, targeted lipid nanoparticle / `LNP` delivery).

     * Organoid bioprinting (vascularized 3D bioprinted tissues, bioinks, organ-on-a-chip).

     * Synthetic gene drives (synthetic chromosomes, minimal genomes, genetic circuits, xenobiology).

2. **Neurotechnology & Cognitive Interfaces (`neurotechnology & cognitive interfaces`)**

   * *Core sub-topics*:

     * Invasive intracortical brain-computer interfaces (`intracortical BCIs`, microelectrode arrays, `ECoG`, endovascular `stentrode` neural implants).

     * Non-invasive optical and magnetic neural decoders (`fNIRS`, optically pumped magnetometer MEG / `OPM-MEG`, focused ultrasound neuromodulation).

     * Sensory optogenetic prosthetics (optogenetic retinal/cochlear prostheses, bidirectional cortical neuroprosthetics, bionic vision).

3. **Aerospace, Space Exploration & Off-World Industry (`aerospace & off-world industry`)**

   * *Core sub-topics*:

     * Fully reusable heavy launch vehicles (methalox full-flow staged combustion, rapid stage reusability, thermal protection systems, orbital propellant depots & cryogenic refueling).

     * In-situ resource extraction (`ISRU`, lunar regolith oxygen extraction, molten regolith electrolysis, asteroid mining, microgravity manufacturing).

     * Nuclear thermal and electric space propulsion (`NTP`, `NEP`, high-power Hall-effect / `VASIMR` / magnetoplasmadynamic thrusters, fission surface power).

     * Orbital satellite crosslinks (optical inter-satellite links / `OISL`, free-space laser crosslinks, `VLEO` constellations, spaceborne `SAR`).

4. **Robotics & Physical Embodiment (`robotics & physical embodiment`)**

   * *Core sub-topics*:

     * General-purpose humanoid kinetics (bipedal locomotion, whole-body control, dexterous hand kinematics, quasi-direct drive / cycloidal actuators).

     * Soft elastomeric actuators (fluidic elastomer actuators, dielectric elastomer actuators / `DEAs`, compliant soft grippers).

     * Biomimetic artificial muscles (`HASEL` electrohydraulic actuators, shape-memory alloy / `SMA` muscles, liquid crystal elastomers / `LCEs`, carbon nanotube yarn muscles).

     * Autonomous magnetic nanorobots (helical microswimmers, magnetically guided intravascular microrobots, biohybrid microrobots).

   * *Mandate boundary*: Tracks mechanical kinematic actuation, soft/biomimetic materials, and physical embodiment; filters out standalone inference silicon and data center GPUs.

5. **Planetary Environmental Engineering (`planetary environmental engineering`)**

   * *Core sub-topics*:

     * Large-scale direct air capture (`DAC`) facilities (solid sorbent contactors, liquid solvent calcination, electro-swing / moisture-swing adsorption, geologic carbon mineralization).

     * Ocean alkalinity enhancement (`OAE`, marine carbon dioxide removal / `mCDR`, electrochemical direct ocean capture / `DOC`).

     * Enhanced rock weathering (`ERW`, crushed basalt/olivine/silicate amendment, terrestrial carbon mineralization kinetics).

6. **Pure Cryptography, Frontier Foundation Models, Test-Time Compute & Autonomous Agent Swarms (`cryptography, frontier AI & autonomous agent swarms`)**

   * *Core sub-topics*:

     * Zero-knowledge proof (`ZKP`) protocols (`zk-SNARKs`, `zk-STARKs`, `PlonK`, folding schemes, `Nova`, `zkVMs`, verifiable computation).

     * Post-quantum lattice cryptography (`ML-KEM` / Kyber, `ML-DSA` / Dilithium, `SPHINCS+`, `FALCON`, `LWE` / `RLWE` lattice cryptography).

     * Fully homomorphic encryption (`FHE`) algorithms (`TFHE`, `CKKS`, `BFV`/`BGV`, programmable bootstrapping, multi-party computation / `MPC`).

     * Frontier Foundation Models & Test-Time Compute (scales machine intelligence by shifting capability from pure pre-training scale to dynamic inference-time reasoning, process reward models / `PRMs`, `MCTS` search, and formal verification).

     * Autonomous Agent Swarms (shifts software from static tools to self-directing multi-agent systems that plan, write code, execute workflows, and recursively optimize their own prompts and architectures).

## Pipeline Architecture

1. **`.env` Control Plane**: Drives `ENABLE_*` switches, per-source caps (`*_MAX_ITEMS*`), `ARXIV_CATEGORIES`, and `Label=URL;;pattern` feed/scraper specifications (`get_feed_specs()`).

2. **Multi-Channel Fetchers + 4-Stage 403-Resilient HTML Scraper**:

   * Pulls candidate preprints and articles from `arXiv` (`q-bio.*`, `cs.RO`, `cs.CR`, `cs.MA`, `cs.AI`, `cs.CL`, `cs.LG`, `cs.LO`, `physics.space-ph`, `astro-ph.IM`, `physics.ao-ph`, `physics.flu-dyn`), `SPECIALIST_RSS_FEEDS` (`GEN Biotechnology`, `Labiotech.eu`, `SynBioBeta`, `Neuroscience News`, `SpaceNews`, `NASASpaceFlight`, `Payload Space`, `The Robot Report`, `IEEE Spectrum Robotics`, `Carbon Herald`, `IACR Cryptology ePrint`), `SUPPLY_CHAIN_RSS_FEEDS` (`Ars Technica Science`, `Quanta Magazine`, `Canary Media`), `Hacker News`, `Techmeme`, and `HTML_SCRAPER_TARGETS` (`STAT News Biotech`, `SpaceNews Launch`, `Carbon Herald Capture`).

   * `scrape_single_html_target()` cascades through (1) Chrome navigation headers + JSON-LD/DOM extraction, (2) WordPress REST API (`/wp-json/wp/v2/posts`), (3) Wayback Machine raw `id_` snapshots, and (4) domain-indexed fallback.

3. **Equal-Weight Taxonomy Scanner & Fair Queue Capping**:

   * `match_hardware_signals()` scans candidate text across all 6 Frontier Deep-Tech Pillars and 25 sub-topics with equal weight.

   * `fair_cap_items()` round-robins equally across all active sources before queueing.

4. **OpenRouter LLM Batch Classification & SQLite WAL Persistence**:

   * `classify_batch()` scores candidates (`0-10`), permanently rejects terrestrial semiconductor/datacenter hardware and generic corporate noise, and persists verified items (`score >= 7.0`) to `/app/data/frontier_deeptech_trends.db` (SQLite WAL mode) with immediate Telegram alerts (`score >= 7.5`) and periodic digests.

## Quick Start (Docker Compose)

```
cp .env.example .env
# Edit .env with your OPENROUTER_API_KEY, TELEGRAM_BOT_TOKEN, and TELEGRAM_CHAT_ID
docker compose up -d --build
docker compose logs -f
```
