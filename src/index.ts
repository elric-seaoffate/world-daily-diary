import { WorkflowEntrypoint, WorkflowStep, WorkflowEvent } from 'cloudflare:workers';

export interface Env {
	NEWS_WORKFLOW: Workflow;
	DB: D1Database;
	ARCHIVE_BUCKET: R2Bucket;
	CONFIG_KV: KVNamespace;
	OPERATIONAL_KV: KVNamespace;
	AI: any;

	BOOKSTACK_URL: string;
	MASTER_BOOK_ID: string;
	SYSTEM_TIMEZONE: string;

	BOOKSTACK_TOKEN_ID: string;
	BOOKSTACK_TOKEN_SECRET: string;
	ADMIN_INTERVENTION_KEY: string;
}

type PipelineParams = {
	cycleId: string;
	isDailyRun: boolean;
	expansionTarget?: string;
	expansionFocus?: string;
};

interface RssSource {
	id: string;
	name: string;
	url: string;
	category: 'general' | 'tech' | 'cyber' | 'ai';
	region: string;
	lang: string;
}

interface CategoryConfig {
	id: 'general' | 'tech' | 'cyber' | 'ai';
	name: string;
	min_stories: number;
	max_stories: number;
	prompt_event: string;
	prompt_analysis: string;
}

interface CategoriesConfigFile {
	global_max_stories: number;
	categories: CategoryConfig[];
}

interface ParsedArticle {
	article_id: string;
	canonical_url: string;
	title: string;
	summary: string;
	source_id: string;
	category_hint: string;
	language_code: string;
	published_at: number;
	ingested_date: string;
}

// REPLACE WITH YOUR ACTUAL BOOKSTACK EDITORIAL CONTROL PAGE ID
const CONTROL_PAGE_ID = "319";

// --- CRYPTOGRAPHIC UTILITIES ---

async function getHmacKey(secret: string): Promise<CryptoKey> {
	const encoder = new TextEncoder();
	return await crypto.subtle.importKey(
		'raw',
		encoder.encode(secret),
		{ name: 'HMAC', hash: 'SHA-256' },
		false,
		['sign', 'verify']
	);
}

function uint8ArrayToHex(buffer: ArrayBuffer): string {
	const hashArray = Array.from(new Uint8Array(buffer));
	return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

function hexToUint8Array(hex: string): Uint8Array {
	const bytes = new Uint8Array(Math.ceil(hex.length / 2));
	for (let i = 0; i < bytes.length; i++) {
		bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
	}
	return bytes;
}

export async function generateActionSignature(storyId: string, action: string, secret: string): Promise<string> {
	const key = await getHmacKey(secret);
	const encoder = new TextEncoder();
	const data = encoder.encode(`${storyId}:${action}`);
	const signatureBuffer = await crypto.subtle.sign('HMAC', key, data);
	return uint8ArrayToHex(signatureBuffer);
}

export async function verifyActionSignature(storyId: string, action: string, signatureHex: string, secret: string): Promise<boolean> {
	try {
		const key = await getHmacKey(secret);
		const encoder = new TextEncoder();
		const data = encoder.encode(`${storyId}:${action}`);
		const signatureBytes = hexToUint8Array(signatureHex);
		return await crypto.subtle.verify('HMAC', key, signatureBytes, data);
	} catch (e) {
		return false;
	}
}

async function sha256Hex(input: string): Promise<string> {
	const encoder = new TextEncoder();
	const buf = await crypto.subtle.digest('SHA-256', encoder.encode(input));
	return uint8ArrayToHex(buf);
}

// --- XML / RSS / ATOM PARSING UTILITIES ---

function cleanXmlText(raw: string): string {
	if (!raw) return '';
	let text = raw.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/gi, '$1');
	text = text.replace(/<[^>]+>/g, ' ');
	text = text
		.replace(/&amp;/g, '&')
		.replace(/&lt;/g, '<')
		.replace(/&gt;/g, '>')
		.replace(/&quot;/g, '"')
		.replace(/&#039;/g, "'")
		.replace(/&apos;/g, "'")
		.replace(/&#8217;/g, "'")
		.replace(/&#8220;/g, '"')
		.replace(/&#8221;/g, '"')
		.replace(/&#8211;/g, '-')
		.replace(/&nbsp;/g, ' ');
	return text.replace(/\s+/g, ' ').trim();
}

function extractTagContent(block: string, tagNames: string[]): string {
	for (const tag of tagNames) {
		const regex = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${tag}>`, 'i');
		const match = regex.exec(block);
		if (match && match[1]) {
			return match[1].trim();
		}
	}
	return '';
}

function extractLink(block: string): string {
	const hrefMatch = /<link[^>]+href=["']([^"']+)["'][^>]*>/i.exec(block);
	if (hrefMatch && hrefMatch[1]) {
		return hrefMatch[1].trim();
	}
	const rawLink = extractTagContent(block, ['link', 'guid']);
	return cleanXmlText(rawLink);
}

function isCommercialFluff(title: string, summary: string, url: string): boolean {
	const combined = `${title} ${summary} ${url}`.toLowerCase();
	const spamPatterns = [
		'sponsored',
		'promo code',
		'discount code',
		'best deals',
		'save $',
		'% off',
		'affiliate',
		'buy now',
		'gift guide',
		'black friday',
		'cyber monday',
		'wordle hint',
		'connections hint',
		'crossword'
	];
	return spamPatterns.some(pattern => combined.includes(pattern));
}

function normalizeStoryKey(title: string, url: string): string {
	const cleanedTitle = title
		.toLowerCase()
		.replace(/[^a-z0-9\s]/g, '')
		.split(/\s+/)
		.filter(w => w.length > 3)
		.slice(0, 6)
		.sort()
		.join('-');
	if (cleanedTitle.length >= 10) {
		return cleanedTitle;
	}
	return url.split('?')[0].toLowerCase();
}

async function parseFeedXml(xml: string, source: RssSource, ingestedDate: string): Promise<{ rawCount: number; fluffCount: number; items: ParsedArticle[] }> {
	const itemBlocks: string[] = [];
	const rssItemRegex = /<item(?:\s[^>]*)?>([\s\S]*?)<\/item>/gi;
	const atomEntryRegex = /<entry(?:\s[^>]*)?>([\s\S]*?)<\/entry>/gi;

	let match: RegExpExecArray | null;
	while ((match = rssItemRegex.exec(xml)) !== null) {
		itemBlocks.push(match[1]);
	}
	if (itemBlocks.length === 0) {
		while ((match = atomEntryRegex.exec(xml)) !== null) {
			itemBlocks.push(match[1]);
		}
	}

	let fluffCount = 0;
	const parsedItems: ParsedArticle[] = [];
	const maxItemsPerFeed = 15;

	for (const block of itemBlocks.slice(0, maxItemsPerFeed)) {
		const title = cleanXmlText(extractTagContent(block, ['title']));
		const rawSummary = extractTagContent(block, ['description', 'summary', 'content:encoded', 'content']);
		const summary = cleanXmlText(rawSummary).slice(0, 800);
		const link = extractLink(block);

		if (!title || !link) continue;

		if (isCommercialFluff(title, summary, link)) {
			fluffCount++;
			continue;
		}

		const dateStr = cleanXmlText(extractTagContent(block, ['pubDate', 'published', 'updated', 'dc:date']));
		const parsedDate = dateStr ? Date.parse(dateStr) : NaN;
		const publishedAt = !isNaN(parsedDate) ? Math.floor(parsedDate / 1000) : Math.floor(Date.now() / 1000);

		const storyKey = normalizeStoryKey(title, link);
		const articleId = (await sha256Hex(`${source.category}:${storyKey}`)).slice(0, 32);

		parsedItems.push({
			article_id: articleId,
			canonical_url: link,
			title: title.slice(0, 300),
			summary: summary || title,
			source_id: `${source.id}|${source.name}|${source.region}`,
			category_hint: source.category,
			language_code: source.lang || 'en',
			published_at: publishedAt,
			ingested_date: ingestedDate
		});
	}

	return {
		rawCount: itemBlocks.length,
		fluffCount,
		items: parsedItems
	};
}

// --- BOOKSTACK AUTHORITATIVE PROJECTION ---

async function regenerateControlPage(env: Env): Promise<void> {
	const query = `
		SELECT 
			story_id,
			category_id,
			headline,
			REPLACE(SUBSTR(markdown_body, 1, 140), char(10), ' ') AS teaser_line,
			tracking_status,
			tracking_origin,
			followup_directive,
			expansion_count
		FROM published_stories
		ORDER BY ingested_date DESC, created_at DESC
		LIMIT 100;
	`;

	const { results } = await env.DB.prepare(query).all();

	let markdown = `# Editorial Control Ledger\n\n`;
	markdown += `*Automatically synchronized with Cloudflare D1 state.*\n\n---\n\n`;

	markdown += `## Active Follow-Up Queue\n\n`;
	const activeStories = results.filter((r: any) => r.tracking_status === 1);

	if (activeStories.length === 0) {
		markdown += `*No active stories currently being tracked.*\n\n`;
	} else {
		for (const story of activeStories as any[]) {
			const unfollowSig = await generateActionSignature(story.story_id, 'unfollow', env.ADMIN_INTERVENTION_KEY);
			const expandSig = await generateActionSignature(story.story_id, 'expand', env.ADMIN_INTERVENTION_KEY);

			markdown += `### [${story.category_id.toUpperCase()}] ${story.headline}\n`;
			markdown += `> _${story.teaser_line}..._\n\n`;
			markdown += `- **Origin:** ${story.tracking_origin === 'human_override' ? 'Human Override' : 'LLM Auto'}\n`;
			markdown += `- **Directive:** ${story.followup_directive}\n`;
			markdown += `- **Expansions:** ${story.expansion_count}\n`;
			markdown += `- **Actions:** [Stop Tracking](https://world-daily-diary.nigel-ferguson10.workers.dev/act?id=${story.story_id}&do=unfollow&sig=${unfollowSig}) | [Request Extended Coverage](https://world-daily-diary.nigel-ferguson10.workers.dev/act?id=${story.story_id}&do=expand&sig=${expandSig})\n\n`;
		}
	}

	markdown += `---\n\n## Recent Chronological Ledger\n\n`;
	for (const story of results as any[]) {
		if (story.tracking_status === 1) continue;

		const followSig = await generateActionSignature(story.story_id, 'follow', env.ADMIN_INTERVENTION_KEY);
		markdown += `**[${story.category_id.toUpperCase()}] ${story.headline}**\n`;
		markdown += `_${story.teaser_line}..._  [Track this Story](https://world-daily-diary.nigel-ferguson10.workers.dev/act?id=${story.story_id}&do=follow&sig=${followSig})\n\n`;
	}

	if (CONTROL_PAGE_ID !== "1000") {
		await fetch(`${env.BOOKSTACK_URL}/api/pages/${CONTROL_PAGE_ID}`, {
			method: 'PUT',
			headers: {
				'Content-Type': 'application/json',
				'Authorization': `Token ${env.BOOKSTACK_TOKEN_ID}:${env.BOOKSTACK_TOKEN_SECRET}`
			},
			body: JSON.stringify({
				markdown: markdown
			})
		});
	}
}

// --- AI INFERENCE HELPER ---

async function runLlmPrompt(env: Env, systemPrompt: string, userContent: string): Promise<string> {
	try {
		const response = await env.AI.run('@cf/meta/llama-3.1-8b-instruct', {
			messages: [
				{ role: 'system', content: systemPrompt },
				{ role: 'user', content: userContent }
			],
			max_tokens: 450,
			temperature: 0.3
		});
		if (response && typeof response.response === 'string' && response.response.trim().length > 0) {
			return response.response.trim();
		}
	} catch (err) {
		try {
			const fallback = await env.AI.run('@cf/meta/llama-3-8b-instruct', {
				messages: [
					{ role: 'system', content: systemPrompt },
					{ role: 'user', content: userContent }
				],
				max_tokens: 450,
				temperature: 0.3
			});
			if (fallback && typeof fallback.response === 'string') {
				return fallback.response.trim();
			}
		} catch (innerErr) {
			return `AI synthesis unavailable: ${String(innerErr)}`;
		}
	}
	return 'Summary unavailable.';
}

// --- MASTER WORKFLOW ORCHESTRATOR ---

export class NewsPipelineWorkflow extends WorkflowEntrypoint<Env, PipelineParams> {
	async run(event: WorkflowEvent<PipelineParams>, step: WorkflowStep) {
		const { cycleId, isDailyRun, expansionTarget, expansionFocus } = event.payload;
		const ingestedDate = cycleId.slice(0, 10);

		// Handle on-demand Admin Story Expansion if triggered via /act
		if (expansionTarget) {
			await step.do('expand-tracked-story', async () => {
				const row: any = await this.env.DB.prepare(
					`SELECT story_id, headline, markdown_body, sources_json FROM published_stories WHERE story_id = ?`
				).bind(expansionTarget).first();

				if (!row) return;

				const expansionText = await runLlmPrompt(
					this.env,
					'You are an investigative intelligence analyst. Expand on the existing report with deeper technical, geopolitical, and systemic context.',
					`Headline: ${row.headline}\nFocus Directive: ${expansionFocus || 'Provide deeper structural analysis.'}\nExisting Report:\n${row.markdown_body}`
				);

				const updatedMarkdown = `${row.markdown_body}\n\n---\n### Extended Coverage (${new Date().toISOString().slice(0, 16)} UTC)\n${expansionText}`;

				await this.env.DB.prepare(`
					UPDATE published_stories
					SET markdown_body = ?, expansion_count = expansion_count + 1, last_expanded_at = unixepoch()
					WHERE story_id = ?
				`).bind(updatedMarkdown, expansionTarget).run();

				await regenerateControlPage(this.env);
			});
			return { status: 'Expansion complete', expansionTarget };
		}

		// Step 1: Read RSS sources and Category quotas from CONFIG_KV
		const configData = await step.do('read-config-kv', async () => {
			const rssData = (await this.env.CONFIG_KV.get('config-rss-sources', 'json')) as { sources: RssSource[] } | null;
			const catData = (await this.env.CONFIG_KV.get('config-categories', 'json')) as CategoriesConfigFile | null;

			const defaultCategories: CategoriesConfigFile = {
				global_max_stories: 25,
				categories: [
					{
						id: 'general',
						name: 'World & General News',
						min_stories: 5,
						max_stories: 20,
						prompt_event: 'Summarize the core news event clearly and factually in 2 concise paragraphs.',
						prompt_analysis: 'Analyze the reporting source, its geographic region, and the framing of this story. Identify potential media bias, regional perspective, and any omitted context.'
					},
					{
						id: 'tech',
						name: 'Technology & Engineering',
						min_stories: 2,
						max_stories: 10,
						prompt_event: 'Report the technical event, product release, or engineering milestone factually in 2 concise paragraphs.',
						prompt_analysis: 'Evaluate the long-term industry and architectural impact, open-source vs. proprietary implications, and adoption hurdles.'
					},
					{
						id: 'cyber',
						name: 'Cybersecurity & Threat Intel',
						min_stories: 2,
						max_stories: 10,
						prompt_event: 'Detail the security incident, vulnerability, or threat campaign factually in 2 concise paragraphs.',
						prompt_analysis: 'Analyze the attack vector, threat actor TTPs, affected systems, and concrete defensive mitigations.'
					},
					{
						id: 'ai',
						name: 'Artificial Intelligence',
						min_stories: 2,
						max_stories: 10,
						prompt_event: 'Summarize the AI model release, research paper, or policy development factually in 2 concise paragraphs.',
						prompt_analysis: 'Separate technical reality from marketing hype. Analyze compute/VRAM constraints, open-weights vs. cloud lock-in, and practical deployment utility.'
					}
				]
			};

			return {
				sources: rssData?.sources || [],
				categoriesConfig: catData || defaultCategories
			};
		});

		// Step 2: Fetch and ingest RSS feeds in batches of 10 (7 batches for 70 feeds)
		const sources = configData.sources;
		const batchSize = 10;
		let totalIngested = 0;

		for (let i = 0; i < sources.length; i += batchSize) {
			const batchIndex = Math.floor(i / batchSize);
			const batchSources = sources.slice(i, i + batchSize);

			const batchCount = await step.do(`ingest-rss-batch-${batchIndex}`, async () => {
				let ingestedInBatch = 0;

				const fetchPromises = batchSources.map(async (source) => {
					const startTime = Date.now();
					try {
						const response = await fetch(source.url, {
							headers: {
								'User-Agent': 'WorldDailyDiary/1.0 (+https://seaoffate.net)',
								'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*'
							},
							signal: AbortSignal.timeout(10000)
						});

						const elapsedMs = Date.now() - startTime;
						if (!response.ok) {
							await this.env.DB.prepare(`
								INSERT INTO feed_telemetry (source_id, rss_name, feed_url, total_polls, failed_polls, last_status_code, last_error_reason, last_polled_at)
								VALUES (?, ?, ?, 1, 1, ?, ?, unixepoch())
								ON CONFLICT(source_id) DO UPDATE SET
									total_polls = total_polls + 1,
									failed_polls = failed_polls + 1,
									last_status_code = excluded.last_status_code,
									last_error_reason = excluded.last_error_reason,
									last_polled_at = unixepoch()
							`).bind(source.id, source.name, source.url, response.status, `HTTP ${response.status}`).run();
							return [];
						}

						const xmlText = await response.text();
						const parsed = await parseFeedXml(xmlText, source, ingestedDate);

						await this.env.DB.prepare(`
							INSERT INTO feed_telemetry (source_id, rss_name, feed_url, total_polls, successful_polls, avg_response_ms, avg_xml_bytes, total_raw_items, total_fresh_items, total_fluff_filtered, last_status_code, last_polled_at)
							VALUES (?, ?, ?, 1, 1, ?, ?, ?, ?, ?, 200, unixepoch())
							ON CONFLICT(source_id) DO UPDATE SET
								total_polls = total_polls + 1,
								successful_polls = successful_polls + 1,
								avg_response_ms = (avg_response_ms + excluded.avg_response_ms) / 2,
								avg_xml_bytes = (avg_xml_bytes + excluded.avg_xml_bytes) / 2,
								total_raw_items = total_raw_items + excluded.total_raw_items,
								total_fresh_items = total_fresh_items + excluded.total_fresh_items,
								total_fluff_filtered = total_fluff_filtered + excluded.total_fluff_filtered,
								last_status_code = 200,
								last_error_reason = NULL,
								last_polled_at = unixepoch()
						`).bind(
							source.id,
							source.name,
							source.url,
							elapsedMs,
							xmlText.length,
							parsed.rawCount,
							parsed.items.length,
							parsed.fluffCount
						).run();

						return parsed.items;
					} catch (err) {
						await this.env.DB.prepare(`
							INSERT INTO feed_telemetry (source_id, rss_name, feed_url, total_polls, failed_polls, last_status_code, last_error_reason, last_polled_at)
							VALUES (?, ?, ?, 1, 1, 0, ?, unixepoch())
							ON CONFLICT(source_id) DO UPDATE SET
								total_polls = total_polls + 1,
								failed_polls = failed_polls + 1,
								last_status_code = 0,
								last_error_reason = excluded.last_error_reason,
								last_polled_at = unixepoch()
						`).bind(source.id, source.name, source.url, String(err).slice(0, 120)).run();
						return [];
					}
				});

				const settled = await Promise.all(fetchPromises);
				const allArticles = settled.flat();

				// Batch upsert articles into D1; increment syndication_velocity when corroborated
				const statements = allArticles.map(art =>
					this.env.DB.prepare(`
						INSERT INTO articles (
							article_id, canonical_url, title, summary, source_id,
							category_hint, language_code, syndication_velocity, published_at, ingested_date
						) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
						ON CONFLICT(article_id) DO UPDATE SET
							syndication_velocity = syndication_velocity + 1,
							source_id = CASE
								WHEN INSTR(articles.source_id, excluded.source_id) = 0
								THEN articles.source_id || ' ; ' || excluded.source_id
								ELSE articles.source_id
							END
					`).bind(
						art.article_id,
						art.canonical_url,
						art.title,
						art.summary,
						art.source_id,
						art.category_hint,
						art.language_code,
						art.published_at,
						art.ingested_date
					)
				);

				if (statements.length > 0) {
					await this.env.DB.batch(statements);
					ingestedInBatch = statements.length;
				}

				return ingestedInBatch;
			});

			totalIngested += batchCount;
		}

		// Record hourly operational telemetry in KV
		await step.do('hourly-condenser-snapshot', async () => {
			await this.env.OPERATIONAL_KV.put(
				`rss-hourly-${cycleId}`,
				JSON.stringify({ cycleId, ingestedDate, totalIngested, timestamp: Date.now() }),
				{ expirationTtl: 172800 }
			);
		});

		if (!isDailyRun) {
			return { status: 'Hourly ingestion complete', cycleId, totalIngested };
		}

		// --- 24-HOUR DAILY PUBLISHING BRANCH ---

		// Step 4: Prune articles older than 60 days
		await step.do('daily-d1-pruning', async () => {
			const cutoff = Math.floor(Date.now() / 1000) - (60 * 86400);
			await this.env.DB.prepare(`DELETE FROM articles WHERE created_at < ?`).bind(cutoff).run();
		});

		// Step 5: Two-Pass Quota Story Picker (Min 5/2/2/2, Max 20/10/10/10, Total Max 25)
		const selectedArticles = await step.do('quota-story-picker', async () => {
			const catConfig = configData.categoriesConfig;
			const globalMax = catConfig.global_max_stories || 25;

			const picked: any[] = [];
			const pickedIds = new Set<string>();
			const overflowPool: any[] = [];

			for (const cat of catConfig.categories) {
				const { results } = await this.env.DB.prepare(`
					SELECT article_id, canonical_url, title, summary, source_id, category_hint, syndication_velocity, published_at
					FROM articles
					WHERE ingested_date = ? AND category_hint = ?
					ORDER BY syndication_velocity DESC, published_at DESC
					LIMIT ?
				`).bind(ingestedDate, cat.id, cat.max_stories).all();

				const candidates = (results || []) as any[];

				// Pass 1: Guarantee category minimum
				const minSlice = candidates.slice(0, cat.min_stories);
				for (const item of minSlice) {
					picked.push(item);
					pickedIds.add(item.article_id);
				}

				// Remaining candidates up to category max_stories go into Pass 2 overflow pool
				const remainingSlice = candidates.slice(cat.min_stories, cat.max_stories);
				for (const item of remainingSlice) {
					overflowPool.push(item);
				}
			}

			// Sort overflow pool by syndication_velocity DESC, then published_at DESC
			overflowPool.sort((a, b) => {
				if (b.syndication_velocity !== a.syndication_velocity) {
					return b.syndication_velocity - a.syndication_velocity;
				}
				return b.published_at - a.published_at;
			});

			// Pass 2: Fill remaining slots up to globalMax (25)
			for (const item of overflowPool) {
				if (picked.length >= globalMax) break;
				if (!pickedIds.has(item.article_id)) {
					picked.push(item);
					pickedIds.add(item.article_id);
				}
			}

			return picked;
		});

		// Step 6: Dual-Prompt LLM Execution (Prompt 1: Event Report, Prompt 2: Bias / Category Analysis)
		const publishedStoryIds: string[] = [];
		const categoryMap = new Map<string, CategoryConfig>();
		for (const c of configData.categoriesConfig.categories) {
			categoryMap.set(c.id, c);
		}

		// Process selected stories in small groups of 5 per step so AI calls stay fast and resilient
		const aiBatchSize = 5;
		for (let i = 0; i < selectedArticles.length; i += aiBatchSize) {
			const batchIndex = Math.floor(i / aiBatchSize);
			const storyChunk = selectedArticles.slice(i, i + aiBatchSize);

			const chunkIds = await step.do(`research-and-write-batch-${batchIndex}`, async () => {
				const writtenIds: string[] = [];

				for (const article of storyChunk) {
					const cat = categoryMap.get(article.category_hint) || configData.categoriesConfig.categories[0];

					// Parse source metadata (id|name|region)
					const sourceSegments = String(article.source_id).split(' ; ').map(s => {
						const parts = s.split('|');
						return {
							id: parts[0] || 'unknown',
							name: parts[1] || parts[0] || 'Unknown Source',
							region: parts[2] || 'Global'
						};
					});

					const sourceSummaryText = sourceSegments.map(s => `${s.name} (${s.region})`).join(', ');

					const userContext = [
						`Headline: ${article.title}`,
						`Category: ${cat.name}`,
						`Reporting Source(s) & Region(s): ${sourceSummaryText}`,
						`Corroboration Velocity: ${article.syndication_velocity} feed(s)`,
						`Source Summary: ${article.summary}`,
						`Canonical URL: ${article.canonical_url}`
					].join('\n');

					// LLM Call 1: The News Event Report
					const eventReport = await runLlmPrompt(this.env, cat.prompt_event, userContext);

					// LLM Call 2: Category-Specific or Source Bias Analysis
					const secondHeading = cat.id === 'general'
						? 'Source Bias & Geopolitical Framing'
						: `${cat.name} Impact Analysis`;

					const analysisReport = await runLlmPrompt(this.env, cat.prompt_analysis, userContext);

					const compiledMarkdown = [
						`### The Event`,
						eventReport,
						``,
						`### ${secondHeading}`,
						analysisReport,
						``,
						`**Sources:** ${sourceSummaryText} | [Read Original Article](${article.canonical_url})`
					].join('\n');

					const bodyHash = await sha256Hex(compiledMarkdown);
					const storyId = `story-${ingestedDate}-${article.article_id.slice(0, 12)}`;

					await this.env.DB.prepare(`
						INSERT INTO published_stories (
							story_id, ingested_date, category_id, headline, markdown_body,
							original_body_hash, search_entities_json, sources_json,
							tracking_status, tracking_origin, followup_directive
						) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'llm_auto', NULL)
						ON CONFLICT(story_id) DO UPDATE SET
							markdown_body = excluded.markdown_body,
							original_body_hash = excluded.original_body_hash
					`).bind(
						storyId,
						ingestedDate,
						cat.id,
						article.title,
						compiledMarkdown,
						bodyHash,
						JSON.stringify([cat.id]),
						JSON.stringify(sourceSegments)
					).run();

					writtenIds.push(storyId);
				}

				return writtenIds;
			});

			publishedStoryIds.push(...chunkIds);
		}

		// Step 7: Daily Copier (Archive to R2, Publish Daily Page to BookStack, Update Control Ledger)
		const publicationResult = await step.do('daily-copier-publish', async () => {
			const { results } = await this.env.DB.prepare(`
				SELECT story_id, category_id, headline, markdown_body, sources_json
				FROM published_stories
				WHERE ingested_date = ?
				ORDER BY category_id ASC, created_at ASC
			`).bind(ingestedDate).all();

			const stories = (results || []) as any[];
			if (stories.length === 0) {
				return { publishedCount: 0, bookstackPageId: null };
			}

			// Build the full Daily Edition Markdown
			const categoryOrder: Array<{ id: string; title: string }> = [
				{ id: 'general', title: 'World & General News' },
				{ id: 'tech', title: 'Technology & Engineering' },
				{ id: 'cyber', title: 'Cybersecurity & Threat Intelligence' },
				{ id: 'ai', title: 'Artificial Intelligence' }
			];

			let dailyMarkdown = `# World Daily Diary — ${ingestedDate}\n\n`;
			dailyMarkdown += `*Automated global intelligence digest (${stories.length} curated stories across 70 global feeds).*\n\n---\n\n`;

			for (const section of categoryOrder) {
				const sectionStories = stories.filter(s => s.category_id === section.id);
				if (sectionStories.length === 0) continue;

				dailyMarkdown += `## ${section.title} (${sectionStories.length})\n\n`;
				for (const st of sectionStories) {
					dailyMarkdown += `## ${st.headline}\n\n`;
					dailyMarkdown += `${st.markdown_body}\n\n---\n\n`;
				}
			}

			// 1. Archive full digest to Cloudflare R2
			const r2Key = `daily/${ingestedDate}/world-daily-diary-${ingestedDate}.md`;
			await this.env.ARCHIVE_BUCKET.put(r2Key, dailyMarkdown, {
				httpMetadata: { contentType: 'text/markdown; charset=utf-8' }
			});

			// 2. Publish new Daily Page to BookStack (Book ID 317)
			let bookstackPageId: number | null = null;
			const bsResponse = await fetch(`${this.env.BOOKSTACK_URL}/api/pages`, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
					'Authorization': `Token ${this.env.BOOKSTACK_TOKEN_ID}:${this.env.BOOKSTACK_TOKEN_SECRET}`
				},
				body: JSON.stringify({
					book_id: Number(this.env.MASTER_BOOK_ID),
					name: `Daily Intelligence Diary - ${ingestedDate}`,
					markdown: dailyMarkdown
				})
			});

			if (bsResponse.ok) {
				const bsJson: any = await bsResponse.json();
				bookstackPageId = bsJson.id || null;
			}

			// 3. Mark stories as synced in D1
			await this.env.DB.prepare(`
				UPDATE published_stories
				SET r2_synced = 1,
					bookstack_synced = CASE WHEN ? IS NOT NULL THEN 1 ELSE 0 END,
					bookstack_page_id = ?
				WHERE ingested_date = ?
			`).bind(bookstackPageId, bookstackPageId, ingestedDate).run();

			// 4. Refresh the Editorial Control Ledger
			await regenerateControlPage(this.env);

			return { publishedCount: stories.length, bookstackPageId, r2Key };
		});

		return {
			status: '24-hour cycle complete',
			cycleId,
			totalIngested,
			selectedCount: selectedArticles.length,
			publicationResult
		};
	}
}

// --- EDGE WORKER HANDLERS ---

export default {
	async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		const date = new Date(event.scheduledTime || Date.now());
		const hour = date.getUTCHours();
		const cycleId = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}-${String(hour).padStart(2, '0')}`;
		const isDailyRun = (hour === 23);

		await env.NEWS_WORKFLOW.create({
			id: `pipeline-${cycleId}-${Date.now()}`,
			params: { cycleId, isDailyRun }
		});
	},

	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		// Manual Test Trigger Endpoint: /trigger?key=YOUR_ADMIN_INTERVENTION_KEY&daily=true
		if (url.pathname === '/trigger') {
			const key = url.searchParams.get('key');
			if (!key || key !== env.ADMIN_INTERVENTION_KEY) {
				return new Response('Unauthorized', { status: 401 });
			}
			const isDailyRun = url.searchParams.get('daily') === 'true';
			const now = new Date();
			const cycleId = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}-${String(now.getUTCHours()).padStart(2, '0')}`;
			const instanceId = `manual-${cycleId}-${Date.now()}`;

			await env.NEWS_WORKFLOW.create({
				id: instanceId,
				params: { cycleId, isDailyRun }
			});

			return new Response(
				JSON.stringify({ status: 'Workflow started', instanceId, cycleId, isDailyRun }, null, 2),
				{ headers: { 'Content-Type': 'application/json' } }
			);
		}

		if (url.pathname !== '/act') {
			return new Response('Not found', { status: 404 });
		}

		const storyId = url.searchParams.get('id');
		const action = url.searchParams.get('do');
		const signature = url.searchParams.get('sig');

		if (!storyId || !action || !signature) {
			return new Response('Missing parameters', { status: 400 });
		}

		const isValid = await verifyActionSignature(storyId, action, signature, env.ADMIN_INTERVENTION_KEY);

		if (!isValid) {
			return new Response('Forbidden: Invalid cryptographic signature.', { status: 403 });
		}

		if (request.method === 'GET') {
			const html = `
				<!DOCTYPE html>
				<html>
				<head>
					<title>Confirm Action</title>
					<style>
						body { font-family: system-ui, sans-serif; background: #f4f4f5; display: flex; justify-content: center; padding-top: 50px; }
						.card { background: white; padding: 24px; border-radius: 8px; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); width: 100%; max-width: 500px; }
						button { background: #2563eb; color: white; border: none; padding: 10px 16px; border-radius: 4px; cursor: pointer; font-weight: bold; width: 100%; margin-top: 16px;}
						button:hover { background: #1d4ed8; }
						input[type="text"] { width: 100%; padding: 8px; margin-top: 8px; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; }
					</style>
				</head>
				<body>
					<div class="card">
						<h2>Confirm Action: ${action.toUpperCase()}</h2>
						<p>Story ID: <code>${storyId}</code></p>
						<form method="POST" action="${url.pathname}${url.search}">
							${action === 'follow' ? '<label>Custom Follow-up Directive (Optional):<input type="text" name="directive" placeholder="e.g., Track parliamentary votes"></label>' : ''}
							${action === 'expand' ? '<label>Focus Area (Optional):<input type="text" name="focus" placeholder="e.g., Focus on supply chain impacts"></label>' : ''}
							<button type="submit">Confirm Execution</button>
						</form>
					</div>
				</body>
				</html>
			`;
			return new Response(html, { headers: { 'Content-Type': 'text/html' } });
		} else if (request.method === 'POST') {
			const formData = await request.formData().catch(() => new FormData());
			const operatorInput = String(formData.get('directive') || formData.get('focus') || '');

			if (action === 'follow') {
				const directive = operatorInput || 'Track developments and overnight updates.';
				await env.DB.prepare(`
					UPDATE published_stories 
					SET tracking_status = 1, tracking_origin = 'human_override', followup_directive = ? 
					WHERE story_id = ?
				`).bind(directive, storyId).run();

			} else if (action === 'unfollow') {
				await env.DB.prepare(`
					UPDATE published_stories 
					SET tracking_status = 0 
					WHERE story_id = ?
				`).bind(storyId).run();

			} else if (action === 'expand') {
				await env.NEWS_WORKFLOW.create({
					id: `expansion-${storyId}-${Date.now()}`,
					params: {
						cycleId: 'expansion',
						isDailyRun: false,
						expansionTarget: storyId,
						expansionFocus: operatorInput
					}
				});
				return new Response('Expansion workflow triggered. The story will be updated shortly.', { status: 200 });
			}

			await regenerateControlPage(env);

			return Response.redirect(`${env.BOOKSTACK_URL}/books/world-daily-diary/page/editorial-control`, 303);
		}

		return new Response('Method not allowed', { status: 405 });
	}
};
