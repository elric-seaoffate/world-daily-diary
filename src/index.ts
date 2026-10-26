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
	skipIngestion?: boolean;
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
	text = text.replace(/<[^>]+>/g, ' ');
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
		'sponsored', 'promo code', 'discount code', 'best deals', 'save $', '% off',
		'affiliate', 'buy now', 'gift guide', 'black friday', 'cyber monday',
		'wordle hint', 'connections hint', 'crossword'
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
	if (cleanedTitle.length >= 10) return cleanedTitle;
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

	for (const block of itemBlocks.slice(0, 15)) {
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

	return { rawCount: itemBlocks.length, fluffCount, items: parsedItems };
}

// --- BOOKSTACK AUTHORITATIVE PROJECTION ---

async function regenerateControlPage(env: Env): Promise<void> {
	try {
		const query = `
			SELECT story_id, category_id, headline,
				REPLACE(SUBSTR(markdown_body, 1, 140), char(10), ' ') AS teaser_line,
				tracking_status
			FROM published_stories
			ORDER BY ingested_date DESC, created_at DESC
			LIMIT 10;
		`;
		const { results } = await env.DB.prepare(query).all();

		let markdown = `# Editorial Control Ledger\n\n*Automatically synchronized with Cloudflare D1 state.*\n\n---\n\n## Recent Intelligence Stories (${(results || []).length})\n\n`;
		for (const story of (results || [])) {
			markdown += `- **[${story.category_id.toUpperCase()}]** ${story.headline}\n  > _${story.teaser_line}..._\n\n`;
		}

		if (CONTROL_PAGE_ID !== "1000") {
			await fetch(`${env.BOOKSTACK_URL}/api/pages/${CONTROL_PAGE_ID}`, {
				method: 'PUT',
				headers: {
					'Content-Type': 'application/json',
					'Authorization': `Token ${env.BOOKSTACK_TOKEN_ID}:${env.BOOKSTACK_TOKEN_SECRET}`
				},
				body: JSON.stringify({ markdown })
			});
		}
	} catch {}
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
		return `AI synthesis unavailable: ${String(err)}`;
	}
	return 'Summary unavailable.';
}

// --- MASTER WORKFLOW ORCHESTRATOR ---

export class NewsPipelineWorkflow extends WorkflowEntrypoint<Env, PipelineParams> {
	async run(event: WorkflowEvent<PipelineParams>, step: WorkflowStep) {
		const { cycleId, isDailyRun, skipIngestion, expansionTarget, expansionFocus } = event.payload;
		const ingestedDate = cycleId.slice(0, 10);

		if (expansionTarget) {
			await step.do('expand-tracked-story', async () => {
				const row: any = await this.env.DB.prepare(
					`SELECT story_id, headline, markdown_body FROM published_stories WHERE story_id = ?`
				).bind(expansionTarget).first();
				if (!row) return;

				const expansionText = await runLlmPrompt(
					this.env,
					'You are an investigative intelligence analyst. Expand on the existing report with deeper context.',
					`Headline: ${row.headline}\nFocus: ${expansionFocus || 'Deeper analysis'}\nReport:\n${row.markdown_body}`
				);

				const updatedMarkdown = `${row.markdown_body}\n\n---\n### Extended Coverage\n${expansionText}`;
				await this.env.DB.prepare(`UPDATE published_stories SET markdown_body = ?, expansion_count = expansion_count + 1 WHERE story_id = ?`).bind(updatedMarkdown, expansionTarget).run();
				await regenerateControlPage(this.env);
			});
			return { status: 'Expansion complete', expansionTarget };
		}

		const configData = await step.do('read-config-kv', async () => {
			const rssData = (await this.env.CONFIG_KV.get('config-rss-sources', 'json')) as { sources: RssSource[] } | null;
			const catData = (await this.env.CONFIG_KV.get('config-categories', 'json')) as CategoriesConfigFile | null;
			return { sources: rssData?.sources || [], categoriesConfig: catData! };
		});

		let totalIngested = 0;
		if (!skipIngestion) {
			const sources = configData.sources;
			for (let i = 0; i < sources.length; i += 10) {
				const batchIndex = Math.floor(i / 10);
				const batchSources = sources.slice(i, i + 10);
				if (batchIndex > 0 && batchIndex % 2 === 0) {
					await step.sleep(`yield-${batchIndex}`, '2 seconds');
				}
				const count = await step.do(`ingest-batch-${batchIndex}`, async () => {
					const promises = batchSources.map(async (source) => {
						try {
							const res = await fetch(source.url, { headers: { 'User-Agent': 'WorldDailyDiary/1.0' }, signal: AbortSignal.timeout(10000) });
							if (!res.ok) return [];
							const xml = await res.text();
							const parsed = await parseFeedXml(xml, source, ingestedDate);
							return parsed.items;
						} catch { return []; }
					});
					const items = (await Promise.all(promises)).flat();
					if (items.length > 0) {
						const stmts = items.map(art => this.env.DB.prepare(`
							INSERT INTO articles (article_id, canonical_url, title, summary, source_id, category_hint, language_code, syndication_velocity, published_at, ingested_date)
							VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
							ON CONFLICT(article_id) DO UPDATE SET syndication_velocity = syndication_velocity + 1
						`).bind(art.article_id, art.canonical_url, art.title, art.summary, art.source_id, art.category_hint, art.language_code, art.published_at, art.ingested_date));
						await this.env.DB.batch(stmts);
					}
					return items.length;
				});
				totalIngested += count;
			}
		}

		if (!isDailyRun) return { status: 'Hourly complete', totalIngested };

		await step.sleep('yield-before-picker', '2 seconds');

		const selectedArticles = await step.do('quota-story-picker', async () => {
			const catConfig = configData.categoriesConfig;
			const picked: any[] = [];
			const pickedIds = new Set<string>();
			const overflow: any[] = [];

			for (const cat of catConfig.categories) {
				const { results } = await this.env.DB.prepare(`
					SELECT article_id, canonical_url, title, summary, source_id, category_hint, syndication_velocity, published_at
					FROM articles WHERE ingested_date = ? AND category_hint = ?
					ORDER BY syndication_velocity DESC, published_at DESC LIMIT ?
				`).bind(ingestedDate, cat.id, cat.max_stories).all();

				const items = (results || []) as any[];
				items.slice(0, cat.min_stories).forEach(i => { picked.push(i); pickedIds.add(i.article_id); });
				items.slice(cat.min_stories).forEach(i => overflow.push(i));
			}

			overflow.sort((a, b) => b.syndication_velocity - a.syndication_velocity);
			for (const item of overflow) {
				if (picked.length >= catConfig.global_max_stories) break;
				if (!pickedIds.has(item.article_id)) { picked.push(item); pickedIds.add(item.article_id); }
			}
			return picked;
		});

		const categoryMap = new Map<string, CategoryConfig>();
		configData.categoriesConfig.categories.forEach(c => categoryMap.set(c.id, c));

		for (let idx = 0; idx < selectedArticles.length; idx++) {
			const article = selectedArticles[idx];
			await step.do(`ai-story-${idx}`, async () => {
				const cat = categoryMap.get(article.category_hint) || configData.categoriesConfig.categories[0];
				const sourceSegments = String(article.source_id).split(' ; ').map(s => {
					const p = s.split('|');
					return { name: p[1] || p[0], region: p[2] || 'Global' };
				});
				const sourceSummaryText = sourceSegments.map(s => `${s.name} (${s.region})`).join(', ');

				const userContext = [
					`Headline: ${article.title}`,
					`Category: ${cat.name}`,
					`Sources: ${sourceSummaryText}`,
					`Velocity: ${article.syndication_velocity}`,
					`Summary: ${article.summary}`,
					`URL: ${article.canonical_url}`
				].join('\n');

				const eventReport = await runLlmPrompt(this.env, cat.prompt_event, userContext);
				const secondHeading = cat.id === 'general' ? 'Source Bias & Geopolitical Framing' : `${cat.name} Impact Analysis`;
				const analysisReport = await runLlmPrompt(this.env, cat.prompt_analysis, userContext);

				const compiledMarkdown = [
					`### The Event`, eventReport, ``,
					`### ${secondHeading}`, analysisReport, ``,
					`**Sources:** ${sourceSummaryText} | [Read Original](${article.canonical_url})`
				].join('\n');

				const bodyHash = await sha256Hex(compiledMarkdown);
				const storyId = `story-${ingestedDate}-${article.article_id.slice(0, 12)}`;

				await this.env.DB.prepare(`
					INSERT INTO published_stories (story_id, ingested_date, category_id, headline, markdown_body, original_body_hash, search_entities_json, sources_json, tracking_status, tracking_origin)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'llm_auto')
					ON CONFLICT(story_id) DO UPDATE SET markdown_body = excluded.markdown_body, original_body_hash = excluded.original_body_hash
				`).bind(storyId, ingestedDate, cat.id, article.title, compiledMarkdown, bodyHash, JSON.stringify([cat.id]), JSON.stringify(sourceSegments)).run();
			});
		}

		await step.sleep('yield-before-r2', '2 seconds');

		// Step 7A: Archive full digest to Cloudflare R2
		await step.do('archive-to-r2', async () => {
			const { results } = await this.env.DB.prepare(`
				SELECT category_id, headline, markdown_body FROM published_stories WHERE ingested_date = ? ORDER BY category_id ASC
			`).bind(ingestedDate).all();
			const stories = (results || []) as any[];

			let dailyMarkdown = `# World Daily Diary — ${ingestedDate}\n\n*Automated global intelligence digest (${stories.length} stories across 70 feeds).*\n\n---\n\n`;
			for (const st of stories) {
				dailyMarkdown += `## ${st.headline}\n\n${st.markdown_body}\n\n---\n\n`;
			}

			await this.env.ARCHIVE_BUCKET.put(`daily/${ingestedDate}/world-daily-diary-${ingestedDate}.md`, dailyMarkdown, {
				httpMetadata: { contentType: 'text/markdown; charset=utf-8' }
			});
			return { archivedCount: stories.length };
		});

		await step.sleep('yield-before-bookstack', '2 seconds');

		// Step 7B: Publish Daily Page to BookStack
		const bookstackResult = await step.do('publish-daily-page', async () => {
			const { results } = await this.env.DB.prepare(`
				SELECT category_id, headline, markdown_body FROM published_stories WHERE ingested_date = ? ORDER BY category_id ASC
			`).bind(ingestedDate).all();
			const stories = (results || []) as any[];

			let dailyMarkdown = `# World Daily Diary — ${ingestedDate}\n\n*Automated global intelligence digest (${stories.length} stories across 70 feeds).*\n\n---\n\n`;
			for (const st of stories) {
				dailyMarkdown += `## ${st.headline}\n\n${st.markdown_body}\n\n---\n\n`;
			}

			let bookstackPageId: number | null = null;
			try {
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
			} catch {}

			await this.env.DB.prepare(`
				UPDATE published_stories
				SET r2_synced = 1,
					bookstack_synced = CASE WHEN ? IS NOT NULL THEN 1 ELSE 0 END,
					bookstack_page_id = ?
				WHERE ingested_date = ?
			`).bind(bookstackPageId, bookstackPageId, ingestedDate).run();

			return { publishedCount: stories.length, bookstackPageId };
		});

		await step.sleep('yield-before-ledger', '2 seconds');

		// Step 7C: Update Editorial Control Ledger
		await step.do('update-control-ledger', async () => {
			await regenerateControlPage(this.env);
			return { status: 'Control ledger updated' };
		});

		return { status: 'Daily complete', bookstackResult };
	}
}

export default {
	async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		const date = new Date(event.scheduledTime || Date.now());
		const hour = date.getUTCHours();
		const cycleId = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}-${String(hour).padStart(2, '0')}`;
		await env.NEWS_WORKFLOW.create({ id: `pipeline-${cycleId}-${Date.now()}`, params: { cycleId, isDailyRun: hour === 23 } });
	},
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		if (url.pathname === '/trigger') {
			const key = url.searchParams.get('key');
			if (!key || key !== env.ADMIN_INTERVENTION_KEY) return new Response('Unauthorized', { status: 401 });
			const isDailyRun = url.searchParams.get('daily') === 'true';
			const skipIngestion = url.searchParams.get('skipIngestion') === 'true';
			const now = new Date();
			const cycleId = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}-${String(now.getUTCHours()).padStart(2, '0')}`;
			const instanceId = `manual-${cycleId}-${Date.now()}`;
			await env.NEWS_WORKFLOW.create({ id: instanceId, params: { cycleId, isDailyRun, skipIngestion } });
			return new Response(JSON.stringify({ status: 'Started', instanceId }, null, 2), { headers: { 'Content-Type': 'application/json' } });
		}
		return new Response('Not found', { status: 404 });
	}
};


