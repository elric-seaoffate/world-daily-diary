# World Daily Diary (`world-daily-diary`)

An automated global intelligence gathering, deduplication, historical archiving, and publishing engine built on **Cloudflare Workers**. It ingests 70 global RSS feeds every hour, clusters and deduplicates stories using a hybrid math and lightweight LLM hierarchy, generates deep-dive event reports and impact analyses with Llama 3.1 8B, synthesizes chronological context via a dual-archive historical archivist pass, and publishes structured markdown digests to **AWS R2** and **BookStack**.

---

## Architecture Overview

1. **70-Feed Hourly Scatter-Gather (10 Shards × 7 Feeds):**
   - Cloudflare Workers enforce a 50-subrequest ceiling per invocation. To poll all 70 feeds reliably, `handleHourlyIngest` splits sources into 10 shards of 7.
   - Shard 0 runs inline on the hourly cron; Shards 1–9 are pushed to `NEWS_QUEUE` with staggered delays (`delaySeconds: s * 6`) so Cloudflare executes exactly one shard worker at a time (7 subrequests per worker).
   - When the final shard (`s9`) finishes, `consolidateHourlyShards` merges and deduplicates all 10 shards into a master hourly JSON feed (`rss-hourly-YYYY-MM-DD-HH`) and a master health ledger (`rss-health-YYYY-MM-DD`).

2. **10ms CPU Limit & Deduplication Hierarchy (`Error 1102` Prevention):**
   - **Pre-Tokenization:** 5-character word stems (`_stems`) are computed once on ingest, mapping synonyms and standardizing variations in O(1) time.
   - **Inverted Index Pre-Clustering (`preClusterAndSortByTopic`):** Rapidly groups topic-linked headlines together in memory without heavy nested loops.
   - **Small-Group 18-Item LLM Hierarchy:** Passes structured topic groups to `@cf/meta/llama-3.2-1b-instruct` in parallel batches to merge semantic duplicates securely.

3. **Dual-Archive Historical Context LLM:**
   - After the primary Llama 3.1 8B model writes the event report and impact/framing analysis, a dedicated archivist LLM queries **both** `published_stories` (our previously generated diaries) and `articles` (the 90-day raw RSS historical database).
   - If >=2 non-generic topic stems match, it synthesizes a chronological `### 90-Day Archive Context` paragraph connecting today's event to its past trajectory.

4. **Strict Prose Formatting:**
   - Enforced via category prompt instructions and a rigorous `cleanLlmProse()` post-processor that strips rogue subheadings, bullet points, and numbered lists, guaranteeing clean, flowing markdown prose for BookStack.

---

## Project Structure

- `src/index.ts` — Main Cloudflare Worker script (RSS scatter-gather, queue handler, deduplication hierarchy, LLM synthesis, and API endpoints).
- `config-categories.json` — Category definitions, story caps, and system prompts for event reporting and impact analysis.
- `config-rss-sources.json` — Registry of all 70 global RSS feeds across general news, tech, cyber threat intel, and AI.
- `schema.sql` — D1 database schema for articles, published stories, and tracking logs.
- `ARCHITECTURE_STATE.md` — Locked engineering specifications and invariants.

---

## Database & Storage Bindings

- **D1 Database (`DB`):** Stores deduplicated raw articles and final published diary entries.
- **KV Namespaces:**
  - `CONFIG_KV` — Stores dynamic configurations (`config-categories`, `config-rss-sources`).
  - `OPERATIONAL_KV` — Holds hourly feed shards (`rss-hourly-*`) and health audit ledgers (`rss-health-*`).
- **R2 Bucket (`ARCHIVE_BUCKET`):** Stores daily markdown backups (`daily/YYYY-MM-DD/world-daily-diary-YYYY-MM-DD.md`).
- **Workers Queue (`NEWS_QUEUE`):** Asynchronously dispatches ingestion shards and handles heavy background LLM story synthesis.
- **Workers AI (`AI`):** Runs `@cf/meta/llama-3.1-8b-instruct` (synthesis) and `@cf/meta/llama-3.2-1b-instruct` (clustering).

---

## Automated Cron Schedule

- **Hourly (`0 * * * *`):** Triggers `handleHourlyIngest` to poll all 70 feeds via scatter-gather shards and consolidate hourly records.
- **Daily Dispatch (`5 23 * * *`):** Triggers `handleDailyDispatch` to roll up 24 hours of news, pick top stories across categories, and queue them for AI synthesis.
- **Daily Publish (`10 23 * * *`):** Triggers `handlePublish` to push the completed markdown digest to BookStack and AWS R2, and prune raw RSS articles older than 90 days.

---

## Manual Admin Endpoints

Protected by `ADMIN_INTERVENTION_KEY`:

- `GET /trigger?key=SECRET&action=ingest` — Manually kick off the 70-feed scatter-gather ingest.
- `GET /trigger?key=SECRET&action=dispatch` — Roll up daily articles and dispatch synthesis jobs to the queue.
- `GET /trigger?key=SECRET&action=publish` — Compile and publish today's digest to BookStack and R2.
