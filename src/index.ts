export interface Env {
	DB: D1Database;
	ARCHIVE_BUCKET: R2Bucket;
	CONFIG_KV: KVNamespace;
	OPERATIONAL_KV: KVNamespace;
	NEWS_QUEUE: Queue;
	AI: any;

	BOOKSTACK_URL: string;
	MASTER_BOOK_ID: string;
	SYSTEM_TIMEZONE: string;

	BOOKSTACK_TOKEN_ID: string;
	BOOKSTACK_TOKEN_SECRET: string;
	ADMIN_INTERVENTION_KEY: string;
}

interface RssSource { id: string; name: string; url: string; category: string; region: string; lang: string; }
interface CategoryConfig { id: string; name: string; min_stories: number; max_stories: number; prompt_event: string; prompt_analysis: string; }
interface CategoriesConfigFile { global_max_stories: number; categories: CategoryConfig[]; }
interface QueueMessage { article_id: string; title: string; summary: string; source_id: string; canonical_url: string; category_id: string; category_name: string; syndication_velocity: number; prompt_event: string; prompt_analysis: string; ingested_date: string; }

const CONTROL_PAGE_ID = "319";

async function sha256Hex(input: string): Promise<string> {
	const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(input));
	return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

function cleanXmlText(raw: string): string {
	if (!raw) return '';
	let text = raw.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/gi, '$1')
		.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
		.replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&apos;/g, "'")
		.replace(/&#8217;/g, "'").replace(/&#8220;/g, '"').replace(/&#8221;/g, '"')
		.replace(/&#8211;/g, '-').replace(/&nbsp;/g, ' ');
	return text.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function extractTagContent(block: string, tagNames: string[]): string {
	for (const tag of tagNames) {
		const match = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${tag}>`, 'i').exec(block);
		if (match && match[1]) return match[1].trim();
	}
	return '';
}

function extractLink(block: string): string {
	const hrefMatch = /<link[^>]+href=["']([^"']+)["'][^>]*>/i.exec(block);
	if (hrefMatch && hrefMatch[1]) return hrefMatch[1].trim();
	return cleanXmlText(extractTagContent(block, ['link', 'guid']));
}

function isCommercialFluff(title: string, summary: string, url: string): boolean {
	const combined = `${title} ${summary} ${url}`.toLowerCase();
	const spamPatterns = ['sponsored', 'promo code', 'discount code', 'best deals', 'save $', '% off', 'affiliate', 'buy now', 'gift guide', 'black friday', 'cyber monday', 'wordle hint', 'connections hint', 'crossword'];
	return spamPatterns.some(pattern => combined.includes(pattern));
}

// 4-word semantic key for robust grouping
function getSortKey(title: string): string {
	const stopWords = new Set(['the', 'and', 'of', 'to', 'in', 'a', 'for', 'on', 'with', 'as', 'by', 'at', 'from', 'an', 'be', 'this', 'that', 'it', 'is', 'was', 'are', 'says', 'said', 'will', 'new', 'after', 'over', 'has', 'have', 'not', 'out', 'up', 'who', 'which', 'its', 'about', 'more', 'their', 'they']);
	const words = title.toLowerCase().replace(/[^a-z0-9\s]/g, '').split(/\s+/).filter(w => w.length > 3 && !stopWords.has(w));
	return Array.from(new Set(words)).sort().slice(0, 4).join('-');
}

async function parseFeedXml(xml: string, source: RssSource, ingestedDate: string): Promise<any[]> {
	const itemBlocks: string[] = [];
	let match: RegExpExecArray | null;
	const rssItemRegex = /<item(?:\s[^>]*)?>([\s\S]*?)<\/item>/gi;
	while ((match = rssItemRegex.exec(xml)) !== null) itemBlocks.push(match[1]);
	if (itemBlocks.length === 0) {
		const atomRegex = /<entry(?:\s[^>]*)?>([\s\S]*?)<\/entry>/gi;
		while ((match = atomRegex.exec(xml)) !== null) itemBlocks.push(match[1]);
	}

	const parsedItems: any[] = [];
	for (const block of itemBlocks.slice(0, 15)) {
		const title = cleanXmlText(extractTagContent(block, ['title']));
		const summary = cleanXmlText(extractTagContent(block, ['description', 'summary', 'content:encoded', 'content'])).slice(0, 800);
		const link = extractLink(block);
		if (!title || !link || isCommercialFluff(title, summary, link)) continue;

		const dateStr = cleanXmlText(extractTagContent(block, ['pubDate', 'published', 'updated', 'dc:date']));
		const parsedDate = dateStr ? Date.parse(dateStr) : NaN;
		const publishedAt = !isNaN(parsedDate) ? Math.floor(parsedDate / 1000) : Math.floor(Date.now() / 1000);
		
		// Immutable foundational ID
		const articleId = (await sha256Hex(`${source.category}:${title.toLowerCase().slice(0, 20)}`)).slice(0, 32);

		parsedItems.push({
			article_id: articleId,
			canonical_url: link,
			title: title.slice(0, 300),
			summary: summary || title,
			source_id: `${source.name} (${source.region})`,
			category_hint: source.category || 'general',
			language_code: source.lang || 'en',
			published_at: publishedAt,
			ingested_date: ingestedDate
		});
	}
	return parsedItems;
}

async function runLlmPrompt(env: Env, baseSystemPrompt: string, userContent: string): Promise<string> {
	const strictSystemPrompt = `${baseSystemPrompt}\n\n[CRITICAL INSTRUCTION]: Base your analysis strictly on the provided sources. Do NOT include any meta-commentary, notes about attribution, or mention of system instructions in your response.`;
	try {
		const response = await env.AI.run('@cf/meta/llama-3.2-3b-instruct', { messages: [{ role: 'system', content: strictSystemPrompt }, { role: 'user', content: userContent }], max_tokens: 800, temperature: 0.3 });
		if (response && typeof response.response === 'string' && response.response.trim().length > 0) return response.response.trim();
	} catch (err) {
		try {
			const fallback = await env.AI.run('@cf/meta/llama-2-7b-chat-int8', { messages: [{ role: 'system', content: strictSystemPrompt }, { role: 'user', content: userContent }], max_tokens: 800, temperature: 0.3 });
			if (fallback && typeof fallback.response === 'string') return fallback.response.trim();
		} catch (innerErr) { return `AI synthesis unavailable: ${String(innerErr)}`; }
	}
	return 'Summary unavailable.';
}

function parseLlmJsonGroups(text: string): number[][] | null {
	try {
		const match = text.match(/\[\s*\[.*?\]\s*\]/s);
		if (match) return JSON.parse(match[0]);
	} catch (e) {}
	return null;
}

async function groupStoriesHierarchically(env: Env, rawArticles: any[]): Promise<Map<string, any>> {
	// STAGE 1: LOCAL CHUNKING
	let level1Clusters: any[] = [];
	const chunkSize = 40;
	
	for (let i = 0; i < rawArticles.length; i += chunkSize) {
		const chunk = rawArticles.slice(i, i + chunkSize);
		const headlineList = chunk.map((art, idx) => `[${idx}] ${art.title}`).join('\n');
		const systemPrompt = `You are a strict news desk editor. Group these headlines that report on the exact same underlying event into unified clusters. Return ONLY a valid JSON array of arrays containing numeric indices. Example: [[0, 3], [1], [2, 4, 5]]`;

		let groups: number[][] | null = null;
		try {
			const res = await env.AI.run('@cf/meta/llama-3.2-1b-instruct', { messages: [{ role: 'system', content: systemPrompt }, { role: 'user', content: headlineList }], max_tokens: 500, temperature: 0.1 });
			groups = parseLlmJsonGroups(res.response);
		} catch (e) {}

		if (groups && Array.isArray(groups)) {
			for (const group of groups) {
				if (!Array.isArray(group) || group.length === 0) continue;
				const masterArt = { ...chunk[group[0]] };
				for (let j = 1; j < group.length; j++) {
					const idx = group[j];
					if (idx < 0 || idx >= chunk.length) continue;
					const art = chunk[idx];
					masterArt.syndication_velocity = (masterArt.syndication_velocity || 1) + 1;
					if (art.source_id && !masterArt.source_id.includes(art.source_id)) {
						masterArt.source_id += ` ; ${art.source_id}`;
						masterArt.summary += `\n---\nUpdate Source: ${art.source_id}\n${art.summary || ''}`;
					}
				}
				level1Clusters.push(masterArt);
			}
		} else {
			// SMART FALLBACK: If LLM fails, group mathematically by semantic keyword signature
			const fallbackMap = new Map<string, any>();
			for (const art of chunk) {
				const sKey = getSortKey(art.title);
				if (fallbackMap.has(sKey)) {
					const existing = fallbackMap.get(sKey);
					existing.syndication_velocity = (existing.syndication_velocity || 1) + 1;
					if (art.source_id && !existing.source_id.includes(art.source_id)) {
						existing.source_id += ` ; ${art.source_id}`;
						existing.summary += `\n---\nUpdate Source: ${art.source_id}\n${art.summary || ''}`;
					}
				} else {
					fallbackMap.set(sKey, { ...art });
				}
			}
			level1Clusters.push(...Array.from(fallbackMap.values()));
		}
	}

	// STAGE 2: GLOBAL MERGE
	const finalAggregatedMap = new Map<string, any>();
	const stage2ChunkSize = 80;
	
	for (let i = 0; i < level1Clusters.length; i += stage2ChunkSize) {
		const chunk = level1Clusters.slice(i, i + stage2ChunkSize);
		const headlineList = chunk.map((art, idx) => `[${idx}] ${art.title}`).join('\n');
		const systemPrompt = `You are a Global News Director. These headlines are from different global regions. Group the headlines that report on the EXACT same worldwide news event into unified clusters. Return ONLY a valid JSON array of arrays containing numeric indices. Example: [[0, 3], [1], [2, 4, 5]]`;

		let groups: number[][] | null = null;
		try {
			const res = await env.AI.run('@cf/meta/llama-3.2-1b-instruct', { messages: [{ role: 'system', content: systemPrompt }, { role: 'user', content: headlineList }], max_tokens: 500, temperature: 0.1 });
			groups = parseLlmJsonGroups(res.response);
		} catch (e) {}

		if (groups && Array.isArray(groups)) {
			for (const group of groups) {
				if (!Array.isArray(group) || group.length === 0) continue;
				const masterArt = { ...chunk[group[0]] };
				
				// CRITICAL FIX: Use the original immutable article_id. No more phantom fallback/cluster IDs!
				const clusterId = masterArt.article_id;

				for (let j = 1; j < group.length; j++) {
					const idx = group[j];
					if (idx < 0 || idx >= chunk.length) continue;
					const art = chunk[idx];
					
					masterArt.syndication_velocity = (masterArt.syndication_velocity || 1) + (art.syndication_velocity || 1);
					const combinedSources = new Set([...masterArt.source_id.split(' ; '), ...art.source_id.split(' ; ')]);
					masterArt.source_id = Array.from(combinedSources).join(' ; ');
					masterArt.summary += `\n---\nUpdate Source: ${art.source_id}\n${art.summary || ''}`;
				}
				finalAggregatedMap.set(clusterId, masterArt);
			}
		} else {
			// CRITICAL FIX: Retain original stable IDs on failure to prevent D1 duplicates
			for (const art of chunk) finalAggregatedMap.set(art.article_id, art);
		}
	}

	return finalAggregatedMap;
}

async function regenerateControlPage(env: Env): Promise<void> {
	try {
		const { results } = await env.DB.prepare(`SELECT story_id, category_id, headline, REPLACE(SUBSTR(markdown_body, 1, 140), char(10), ' ') AS teaser_line FROM published_stories ORDER BY ingested_date DESC, created_at DESC LIMIT 15`).all();
		let markdown = `# Editorial Control Ledger\n\n*Automatically synchronized with Cloudflare D1 state.*\n\n---\n\n## Recent Intelligence Stories\n\n`;
		for (const story of (results || [])) markdown += `- **[${story.category_id.toUpperCase()}]** ${story.headline}\n  > _${story.teaser_line}..._\n\n`;
		await fetch(`${env.BOOKSTACK_URL}/api/pages/${CONTROL_PAGE_ID}`, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'Authorization': `Token ${env.BOOKSTACK_TOKEN_ID}:${env.BOOKSTACK_TOKEN_SECRET}` }, body: JSON.stringify({ markdown }) });
	} catch (err) {}
}

async function handleHourlyIngest(env: Env, cycleId: string) {
	const rssData = (await env.CONFIG_KV.get('config-rss-sources', 'json')) as { sources: RssSource[] } | null;
	const sources = rssData?.sources || [];
	const hourlyItems: any[] = [];
	const sourceHealthReport: any = {};

	for (let i = 0; i < sources.length; i += 10) {
		const batchSources = sources.slice(i, i + 10);
		const promises = batchSources.map(async (source) => {
			try {
				const res = await fetch(source.url, { 
					headers: { 
						'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
						'Accept': 'application/rss+xml, application/xml, text/xml, */*'
					}, 
					signal: AbortSignal.timeout(15000) 
				});
				if (!res.ok) {
					sourceHealthReport[`${source.name} (${source.region})`] = `FAILED (HTTP ${res.status})`;
					return [];
				}
				const items = await parseFeedXml(await res.text(), source, cycleId.slice(0, 10));
				sourceHealthReport[`${source.name} (${source.region})`] = items.length > 0 ? `SUCCESS (${items.length} items)` : 'EMPTY';
				return items;
			} catch (err: any) {
				sourceHealthReport[`${source.name} (${source.region})`] = `FAILED (${err.name === 'TimeoutError' ? 'Timeout' : 'Error'})`;
				return [];
			}
		});
		const items = (await Promise.all(promises)).flat();
		hourlyItems.push(...items);
	}

	const hourlyKey = `rss-hourly-${cycleId}`;
	const healthKey = `rss-health-${cycleId}`;
	
	await env.OPERATIONAL_KV.put(hourlyKey, JSON.stringify({ timestamp: Date.now(), items: hourlyItems }), { expirationTtl: 129600 });
	await env.OPERATIONAL_KV.put(healthKey, JSON.stringify(sourceHealthReport), { expirationTtl: 129600 });

	return { status: 'Hourly condensation complete', hour: cycleId, itemsCollected: hourlyItems.length };
}

async function handleDailyDispatch(env: Env, cycleId: string) {
	const ingestedDate = cycleId.slice(0, 10);
	const catData = (await env.CONFIG_KV.get('config-categories', 'json')) as CategoriesConfigFile | null;
	if (!catData) throw new Error('No category config found in KV');

	const allRawArticles: any[] = [];

	for (let h = 0; h < 24; h++) {
		const hourStr = String(h).padStart(2, '0');
		const hourlyKey = `rss-hourly-${ingestedDate}-${hourStr}`;
		const rawData = await env.OPERATIONAL_KV.get(hourlyKey, 'json') as { items: any[] } | null;
		
		if (rawData && rawData.items) {
			for (const art of rawData.items) {
				if (art.article_id && art.title) allRawArticles.push(art);
			}
		}
	}

	if (allRawArticles.length === 0) throw new Error(`No hourly records found in OPERATIONAL_KV for date ${ingestedDate}`);

	const seenSourceSignatures = new Set<string>();
	const filteredRawArticles: any[] = [];
	for (const art of allRawArticles) {
		const signature = `${art.source_id}:${art.title.toLowerCase().trim()}`;
		if (!seenSourceSignatures.has(signature)) {
			seenSourceSignatures.add(signature);
			filteredRawArticles.push(art);
		}
	}

	// Semantic sorting ensuring global coverage chunks properly
	filteredRawArticles.sort((a, b) => getSortKey(a.title).localeCompare(getSortKey(b.title)));

	const allArticlesMap = await groupStoriesHierarchically(env, filteredRawArticles);
	const uniqueArticles = Array.from(allArticlesMap.values());

	const batchSize = 10;
	for (let i = 0; i < uniqueArticles.length; i += batchSize) {
		const chunk = uniqueArticles.slice(i, i + batchSize);
		const stmts = chunk.map(art => env.DB.prepare(`
			INSERT INTO articles (article_id, canonical_url, title, summary, source_id, category_hint, language_code, syndication_velocity, published_at, ingested_date)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(article_id) DO UPDATE SET 
			syndication_velocity = excluded.syndication_velocity,
			source_id = excluded.source_id,
			summary = excluded.summary
		`).bind(
			art.article_id,
			art.canonical_url || '',
			art.title || '',
			art.summary || '',
			art.source_id || 'Unknown',
			art.category_hint || 'general',
			art.language_code || 'en',
			art.syndication_velocity || 1,
			art.published_at || Math.floor(Date.now() / 1000),
			art.ingested_date || ingestedDate
		));
		await env.DB.batch(stmts);
	}

	const picked: any[] = [];
	const pickedIds = new Set<string>();
	const overflow: any[] = [];
	const globalMax = catData.global_max_stories || 24;

	for (const cat of catData.categories) {
		const { results } = await env.DB.prepare(`SELECT article_id, canonical_url, title, summary, source_id, category_hint, syndication_velocity FROM articles WHERE ingested_date = ? AND category_hint = ? ORDER BY syndication_velocity DESC, published_at DESC LIMIT ?`).bind(ingestedDate, cat.id, cat.max_stories).all();
		const items = (results || []) as any[];
		items.slice(0, cat.min_stories).forEach(i => { picked.push(i); pickedIds.add(i.article_id); });
		items.slice(cat.min_stories).forEach(i => overflow.push(i));
	}
	overflow.sort((a, b) => b.syndication_velocity - a.syndication_velocity);
	for (const item of overflow) {
		if (picked.length >= globalMax) break;
		if (!pickedIds.has(item.article_id)) { picked.push(item); pickedIds.add(item.article_id); }
	}

	const categoryMap = new Map<string, CategoryConfig>();
	catData.categories.forEach(c => categoryMap.set(c.id, c));

	const messages: QueueMessage[] = picked.map(article => {
		const cat = categoryMap.get(article.category_hint) || catData.categories[0];
		return {
			article_id: article.article_id,
			title: article.title || 'Untitled',
			summary: article.summary || '',
			source_id: article.source_id || 'Unknown',
			canonical_url: article.canonical_url || '',
			category_id: cat.id,
			category_name: cat.name,
			syndication_velocity: article.syndication_velocity || 1,
			prompt_event: cat.prompt_event,
			prompt_analysis: cat.prompt_analysis,
			ingested_date: ingestedDate
		};
	});

	if (messages.length > 0) {
		const batchSend = messages.map(msg => ({ body: msg }));
		await env.NEWS_QUEUE.sendBatch(batchSend);
	}
	return { status: '24-hour Hierarchical LLM-clustered rollup complete & dispatched to queue', uniqueArticlesCount: uniqueArticles.length, dispatchedStories: messages.length };
}

async function handlePublish(env: Env, cycleId: string) {
	const ingestedDate = cycleId.slice(0, 10);
	const { results } = await env.DB.prepare(`SELECT category_id, headline, markdown_body FROM published_stories WHERE ingested_date = ? ORDER BY category_id ASC`).bind(ingestedDate).all();
	if (!results || results.length === 0) return { status: 'No stories to publish' };

	const now = new Date();
	const hours = String(now.getUTCHours()).padStart(2, '0');
	const mins = String(now.getUTCMinutes()).padStart(2, '0');
	const timeStr = `${hours}:${mins}`;

	let healthAuditMarkdown = `\n\n---\n\n## Operational Feed Health & Source Audit Ledger\n\n| Source Publisher & Region | Status / Item Yield |\n| :--- | :--- |\n`;
	try {
		const healthData = await env.OPERATIONAL_KV.get(`rss-health-${ingestedDate}-01`, 'json') as Record<string, string> | null;
		if (healthData) {
			for (const [src, status] of Object.entries(healthData)) {
				healthAuditMarkdown += `| ${src} | ${status} |\n`;
			}
		} else {
			healthAuditMarkdown += `| Operational Audit | Simulated / Standard Ingestion Active |\n`;
		}
	} catch (e) {
		healthAuditMarkdown += `| Operational Audit | Audit Data Unavailable |\n`;
	}

	let dailyMarkdown = `*Automated global intelligence digest (${results.length} stories) — Generated at ${timeStr} UTC.*\n\n---\n\n`;
	
	for (const st of results as any[]) {
		dailyMarkdown += `## ${st.headline}\n\n${st.markdown_body}\n\n---\n\n`;
	}

	dailyMarkdown += healthAuditMarkdown;

	await env.ARCHIVE_BUCKET.put(`daily/${ingestedDate}/world-daily-diary-${ingestedDate}.md`, dailyMarkdown, { httpMetadata: { contentType: 'text/markdown; charset=utf-8' } });

	let bookstackPageId = null;
	try {
		const bsResponse = await fetch(`${env.BOOKSTACK_URL}/api/pages`, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json', 'Authorization': `Token ${env.BOOKSTACK_TOKEN_ID}:${env.BOOKSTACK_TOKEN_SECRET}` },
			body: JSON.stringify({ book_id: Number(env.MASTER_BOOK_ID), name: `World Events - ${ingestedDate} at ${timeStr}`, markdown: dailyMarkdown })
		});
		if (bsResponse.ok) {
			bookstackPageId = (await bsResponse.json() as any).id;
			await env.DB.prepare(`UPDATE published_stories SET r2_synced = 1, bookstack_synced = 1, bookstack_page_id = ? WHERE ingested_date = ?`).bind(bookstackPageId, ingestedDate).run();
		}
	} catch (e) {}

	await regenerateControlPage(env);
	return { status: 'Publish complete', bookstackPageId, storiesPublished: results.length };
}

export default {
	async queue(batch: MessageBatch<QueueMessage>, env: Env): Promise<void> {
		for (const message of batch.messages) {
			const msg = message.body;
			
			const userContext = `Headline: ${msg.title}\nCategory: ${msg.category_name}\nVerified Contributing Sources: ${msg.source_id}\nSyndication Velocity (Update Count): ${msg.syndication_velocity}\nComposite Summaries & Updates across Feeds:\n${msg.summary}\nURL: ${msg.canonical_url}`;
			
			const eventReport = await runLlmPrompt(env, msg.prompt_event, userContext);
			const analysisReport = await runLlmPrompt(env, msg.prompt_analysis, userContext);
			
			const secondHeading = msg.category_id === 'general' ? 'Source Bias & Geopolitical Framing' : `${msg.category_name} Impact Analysis`;
			
			const compiledMarkdown = `### The Event\n${eventReport}\n\n### ${secondHeading}\n${analysisReport}\n\n**Verified Sources:** ${msg.source_id} | [Primary Link](${msg.canonical_url})`;
			
			const bodyHash = await sha256Hex(compiledMarkdown);
			const storyId = `story-${msg.ingested_date}-${msg.article_id.slice(0, 12)}`;
			
			await env.DB.prepare(`
				INSERT INTO published_stories (story_id, ingested_date, category_id, headline, markdown_body, original_body_hash, search_entities_json, sources_json, tracking_status, tracking_origin)
				VALUES (?, ?, ?, ?, ?, ?, '[]', '[]', 0, 'queue_auto')
				ON CONFLICT(story_id) DO UPDATE SET markdown_body = excluded.markdown_body, original_body_hash = excluded.original_body_hash
			`).bind(storyId, msg.ingested_date, msg.category_id, msg.title, compiledMarkdown, bodyHash).run();
			message.ack();
		}
	},
	
	async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		const date = new Date(event.scheduledTime || Date.now());
		const hour = date.getUTCHours();
		const cycleId = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}-${String(hour).padStart(2, '0')}`;
		
		await handleHourlyIngest(env, cycleId);
		if (hour === 23) {
			await handleDailyDispatch(env, cycleId);
		} else if (hour === 23 && date.getUTCMinutes() === 5) {
			await handlePublish(env, cycleId);
		}
	},

	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		if (url.pathname === '/trigger') {
			const key = url.searchParams.get('key');
			if (!key || key !== env.ADMIN_INTERVENTION_KEY) return new Response('Unauthorized', { status: 401 });
			const action = url.searchParams.get('action');
			const ingestedDate = '2026-10-27';
			const cycleId = `${ingestedDate}-01`;
			
			try {
				if (action === 'ingest') return new Response(JSON.stringify(await handleHourlyIngest(env, cycleId), null, 2), { headers: { 'Content-Type': 'application/json' } });
				
				if (action === 'simulate') {
					const hoursParam = url.searchParams.get('hours') || '3';
					const targetHours = parseInt(hoursParam, 10);
					const baseKey = `rss-hourly-${ingestedDate}-01`;
					const baseData = await env.OPERATIONAL_KV.get(baseKey, 'json');
					if (!baseData) return new Response(JSON.stringify({ error: 'Run action=ingest first to capture source items' }), { status: 400 });

					for (let h = 0; h < targetHours; h++) {
						const hourStr = String(h).padStart(2, '0');
						const key = `rss-hourly-${ingestedDate}-${hourStr}`;
						await env.OPERATIONAL_KV.put(key, JSON.stringify(baseData), { expirationTtl: 129600 });
					}
					return new Response(JSON.stringify({ status: `Simulated ${targetHours} hourly snapshots successfully` }), { headers: { 'Content-Type': 'application/json' } });
				}

				if (action === 'dispatch') return new Response(JSON.stringify(await handleDailyDispatch(env, cycleId), null, 2), { headers: { 'Content-Type': 'application/json' } });
				if (action === 'publish') return new Response(JSON.stringify(await handlePublish(env, cycleId), null, 2), { headers: { 'Content-Type': 'application/json' } });
				
				return new Response('Specify ?action=ingest, simulate&hours=N, dispatch, or publish', { status: 400 });
			} catch (err: any) {
				return new Response(JSON.stringify({ error: err.message, stack: err.stack }, null, 2), { status: 500, headers: { 'Content-Type': 'application/json' } });
			}
		}
		return new Response('Not found', { status: 404 });
	}
};
