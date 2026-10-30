# World Daily Diary - Locked Architectural Rules (Do Not Regress)

1. **Worker URL & Schedule:**
   - URL: `https://world-daily-diary.nigel-ferguson10.workers.dev`
   - Crons: `0 * * * *` (Hourly Ingest) and `5 23 * * *` (23:00 UTC Dispatch, 23:05 UTC Publish).
   - Note: `wrangler.jsonc` has two cron triggers (`0 * * * *` and `5 23 * * *`). Because `0 23 * * *` matches `0 * * * *`, at 23:00 UTC `scheduled()` runs BOTH `handleHourlyIngest` and `handleDailyDispatch`, and at 23:05 UTC it runs `handlePublish`.

2. **84-Feed Hourly Scatter-Gather (12 Shards x 7 Feeds + Hourly Consolidation):**
   - `config-rss-sources.json` holds 84 feeds (`MAX_SHARDS = 16`, `SHARD_SIZE = 7` -> 12 active shards `s0..s11`).
   - Shard 0 runs inline; Shards 1..11 are pushed to `NEWS_QUEUE` with staggered `delaySeconds: s * 6` so Cloudflare executes 1 shard per Worker invocation (7 subrequests/worker).
   - When the final shard (`s11`, at +66s) finishes, `consolidateHourlyShards` merges `s0..s11` into `rss-hourly-YYYY-MM-DD-HH` and `rss-health-YYYY-MM-DD`.

3. **10ms CPU Ceiling (`Error 1102`) & Anti-Contamination Clustering Rules:**
   - Compute 5-char canonical stems once (`_stems`).
   - **Never skip country/actor stems in `preClusterAndSortByTopic`:** Raise the inverted-index frequency ceiling to 120 so 24-hour country stems (`israe`, `brazi`, `spain`, `ukrai`) are never skipped.
   - **Prevent Frankenstein Merges:** Two headlines may only be merged (either deterministically or via the 1B LLM) if they share **at least 2 specific non-generic stems** (`shared >= 2`). Never allow the 1B LLM to merge headlines that only share 1 generic word like `court`, `flaw`, or `hack`.
   - **Clean Summary Merging:** When merging articles, cap appended update snippets and instruct the 8B LLM to write **strictly about the primary headline event** and ignore tangential stories from roundup feeds.

4. **Strict Prose Formatting:**
   - All BookStack output must be pure flowing paragraphs—no internal headers, bullet points, or numbered lists. Enforced via `CONFIG_KV` prompts + `cleanLlmProse()`.

5. **Post-Creation Dual-Archive Historical Context Gate (>= 3 Days Prior & >= 3 Stems):**
   - To prevent yesterday's breaking news from echoing in `### 90-Day Archive Context`, only query `published_stories` and `articles` where `ingested_date <= date(msg.ingested_date, '-3 days')` and require **>= 3 shared stems** with today's headline.

