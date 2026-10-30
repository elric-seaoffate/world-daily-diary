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

interface StoryQueueMessage {
	job_type?: 'story_synthesis';
	article_id: string;
	title: string;
	summary: string;
	source_id: string;
	canonical_url: string;
	category_id: string;
	category_name: string;
	syndication_velocity: number;
	prompt_event: string;
	prompt_analysis: string;
	ingested_date: string;
	published_at: number;
}

interface IngestBatchQueueMessage {
	job_type: 'ingest_shard';
	cycle_id: string;
	ingested_date: string;
	shard_index: number;
	total_shards: number;
	sources: RssSource[];
}

type QueuePayload = StoryQueueMessage | IngestBatchQueueMessage;

const CONTROL_PAGE_ID = "319";
const SHARD_SIZE = 7;
const MAX_SHARDS = 16; // Supports up to 112 feeds (84 feeds = 12 shards: s0..s11)

// Includes generic journalism, legal, cyber, and AI category words so unrelated stories never false-merge
const STOP_WORDS = new Set([
	'the', 'and', 'of', 'to', 'in', 'a', 'for', 'on', 'with', 'as', 'by', 'at', 'from',
	'an', 'be', 'this', 'that', 'it', 'is', 'was', 'are', 'says', 'said', 'will', 'new',
	'after', 'over', 'has', 'have', 'not', 'out', 'up', 'who', 'which', 'its', 'about',
	'more', 'their', 'they', 'into', 'amid', 'what', 'when', 'where', 'why', 'how', 'could',
	'would', 'should', 'than', 'then', 'them', 'some', 'such', 'only', 'other', 'been', 'were',
	'also', 'most', 'first', 'last', 'year', 'years', 'world', 'news', 'report', 'reports',
	'live', 'update', 'updates', 'official', 'officials', 'least', 'between', 'two', 'three',
	'model', 'models', 'open', 'weight', 'weights', 'multimodal', 'flaw', 'flaws', 'bug', 'bugs',
	'patch', 'patches', 'patched', 'exploit', 'exploits', 'exploited', 'security', 'cyber', 'system',
	'systems', 'release', 'releases', 'launch', 'launches', 'build', 'built', 'chain', 'chains',
	'working', 'agent', 'agents', 'using', 'under', 'state', 'government', 'leader', 'leaders',
	'country', 'people', 'court', 'high', 'judge', 'trial', 'case', 'loses', 'wins', 'alleged',
	'hacker', 'hackers', 'malware', 'steal', 'plant', 'critical', 'vulnerability', 'vulnerabilities',
	'party', 'parties', 'political', 'drama', 'unfolds', 'begins', 'continues', 'search', 'days'
]);

// Canonicalize common news synonyms so multi-angle headlines on the same event produce identical stems
const SYNONYM_MAP: Record<string, string> = {
	'killed': 'dead', 'kills': 'dead', 'kill': 'dead', 'deaths': 'dead', 'death': 'dead', 'deadly': 'dead', 'fatal': 'dead', 'mourning': 'dead',
	'collide': 'crash', 'collides': 'crash', 'collision': 'crash', 'crashes': 'crash', 'crashed': 'crash', 'derail': 'crash', 'accident': 'crash', 'wreckage': 'crash',
	'trains': 'train', 'rail': 'train', 'railway': 'train',
	'elections': 'elect', 'election': 'elect', 'voters': 'elect', 'voter': 'elect', 'votes': 'elect', 'vote': 'elect', 'voting': 'elect', 'ballots': 'elect', 'ballot': 'elect', 'polls': 'elect', 'poll': 'elect', 'counted': 'elect', 'count': 'elect', 'majority': 'elect', 'seats': 'elect',
	'israeli': 'israe', 'israelis': 'israe',
	'brazilian': 'brazi', 'brazils': 'brazi',
	'spanish': 'spain',
	'serbian': 'belgr', 'serbia': 'belgr', 'belgrade': 'belgr',
	'shoots': 'shoot', 'shot': 'shoot', 'shooting': 'shoot',
	'assailant': 'attac', 'attacker': 'attac', 'attacks': 'attac', 'attacked': 'attac',
	'flee': 'leave', 'flees': 'leave', 'fled': 'leave', 'leaves': 'leave', 'left': 'leave', 'heads': 'leave', 'depart': 'leave', 'arrives': 'leave', 'arrived': 'leave',
	'talks': 'call', 'talk': 'call', 'speaks': 'call', 'phone': 'call',
	'truce': 'cease', 'ceasefire': 'cease'
};

async function sha256Hex(input: string): Promise<string> {
	const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(input));
	return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

function normalizeAscii(input: string): string {
	return (input || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

function cleanXmlText(raw: string): string {
	if (!raw) return '';
	let text = raw.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/gi, '$1')
		.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
		.replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&apos;/g, "'")
		.replace(/&#8217;/g, "'").replace(/&#8216;/g, "'")
		.replace(/&#8220;/g, '"').replace(/&#8221;/g, '"')
		.replace(/&#8211;/g, '-').replace(/&#8212;/g, '-').replace(/&nbsp;/g, ' ');
	return text.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function cleanLlmProse(raw: string): string {
	if (!raw) return '';
	const bannedStandaloneHeaders = /^(event brief|intelligence brief|technical briefing|threat intelligence report|research and development brief|historical context.*|key stakeholders.*|motives and interests.*|motives and objectives.*|immediate consequences.*|geopolitical and economic.*|economic consequences.*|conclusion|summary|overview|impact analysis.*|framing analysis.*|90-day archive context.*)$/i;

	const lines = raw
		.replace(/\r\n/g, '\n')
		.split('\n')
		.map(line => line.trim())
		.filter(line => line.length > 0)
		.filter(line => !/^#+\s+/.test(line))
		.filter(line => !bannedStandaloneHeaders.test(line.replace(/[*_:]/g, '').trim()))
		.map(line => {
			let cleaned = line
				.replace(/^[-*•]\s+/, '')
				.replace(/^\d+[\.\)]\s+/, '')
				.replace(/^\*\*[^*]{1,60}\*\*:\s*/, '')
				.replace(/^(Historical Context|Key Stakeholders(?: and Motives)?|Motives(?: and Interests| and Objectives)?|Immediate Consequences|Geopolitical (?:and Economic )?(?:Implications|Consequences)|Economic Consequences|Conclusion):\s*/i, '')
				.replace(/\*\*/g, '');
			return cleaned.trim();
		})
		.filter(line => line.length > 0);

	return lines.join('\n\n');
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
	const spamPatterns = [
		'sponsored', 'promo code', 'discount code', 'best deals', 'save $', '% off',
		'affiliate', 'buy now', 'gift guide', 'black friday', 'cyber monday',
		'wordle hint', 'connections hint', 'crossword'
	];
	return spamPatterns.some(pattern => combined.includes(pattern));
}

function computeStemsOnce(title: string): string[] {
	const normalized = normalizeAscii(title).toLowerCase().replace(/[^a-z0-9\s]/g, ' ');
	const words = normalized.split(/\s+/).filter(w => w.length >= 3 && !/^\d+$/.test(w) && !STOP_WORDS.has(w));
	const stems = words.map(w => {
		const mapped = SYNONYM_MAP[w] || w;
		return mapped.slice(0, 5);
	});
	return Array.from(new Set(stems));
}

function countSharedStems(stemsA: string[], stemsB: string[]): number {
	if (!stemsA?.length || !stemsB?.length) return 0;
	const setB = new Set(stemsB);
	let shared = 0;
	for (const s of stemsA) {
		if (setB.has(s)) shared++;
	}
	return shared;
}

function mergeTwoArticles(masterArt: any, incomingArt: any): void {
	masterArt.syndication_velocity = (masterArt.syndication_velocity || 1) + (incomingArt.syndication_velocity || 1);
	const masterSources = (masterArt.source_id || 'Unknown').split(' ; ').filter(Boolean);
	const incomingSources = (incomingArt.source_id || 'Unknown').split(' ; ').filter(Boolean);
	const combinedSources = new Set([...masterSources, ...incomingSources]);
	masterArt.source_id = Array.from(combinedSources).join(' ; ');

	if (incomingArt.summary && !masterArt.summary.includes(incomingArt.summary.slice(0, 80))) {
		masterArt.summary = `${masterArt.summary}\n---\n[${incomingArt.source_id} — ${incomingArt.title}]: ${incomingArt.summary}`.slice(0, 3500);
	}
	if (incomingArt.published_at && incomingArt.published_at > (masterArt.published_at || 0)) {
		masterArt.published_at = incomingArt.published_at;
	}
	// Intentionally keep masterArt._stems focused on the master headline so clusters never snowball into unrelated topics
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

	const nowSec = Math.floor(Date.now() / 1000);
	const oldestAllowedSec = nowSec - (72 * 3600);
	const parsedItems: any[] = [];

	for (const block of itemBlocks.slice(0, 15)) {
		const title = cleanXmlText(extractTagContent(block, ['title']));
		const summary = cleanXmlText(extractTagContent(block, ['description', 'summary', 'content:encoded', 'content'])).slice(0, 800);
		const link = extractLink(block);
		if (!title || !link || isCommercialFluff(title, summary, link)) continue;

		const dateStr = cleanXmlText(extractTagContent(block, ['pubDate', 'published', 'updated', 'dc:date']));
		const parsedDate = dateStr ? Date.parse(dateStr) : NaN;
		const publishedAt = !isNaN(parsedDate) ? Math.floor(parsedDate / 1000) : nowSec;

		if (!isNaN(parsedDate) && publishedAt < oldestAllowedSec) continue;

		const normalizedTitleKey = normalizeAscii(title).toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 48);
		const articleId = (await sha256Hex(`${source.category}:${normalizedTitleKey}`)).slice(0, 32);

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

async function consolidateHourlyShards(env: Env, cycleId: string, ingestedDate: string, totalShards: number): Promise<number> {
	const shardKeys = Array.from({ length: totalShards }, (_, s) => `rss-hourly-${cycleId}-s${s}`);
	const healthKeys = Array.from({ length: totalShards }, (_, s) => `rss-health-${ingestedDate}-s${s}`);

	const [shardPayloads, healthPayloads, existingDailyHealth] = await Promise.all([
		Promise.all(shardKeys.map(k => env.OPERATIONAL_KV.get(k, 'json') as Promise<{ items: any[] } | null>)),
		Promise.all(healthKeys.map(k => env.OPERATIONAL_KV.get(k, 'json') as Promise<Record<string, string> | null>)),
		env.OPERATIONAL_KV.get(`rss-health-${ingestedDate}`, 'json') as Promise<Record<string, string> | null>
	]);

	const hourlyMap = new Map<string, any>();
	for (const payload of shardPayloads) {
		if (!payload || !payload.items) continue;
		for (const item of payload.items) {
			if (!item || !item.article_id) continue;
			const existing = hourlyMap.get(item.article_id);
			if (existing) {
				if (item.source_id && !existing.source_id.includes(item.source_id)) {
					mergeTwoArticles(existing, item);
				}
			} else {
				hourlyMap.set(item.article_id, { ...item });
			}
		}
	}

	const consolidatedItems = Array.from(hourlyMap.values());
	if (consolidatedItems.length > 0) {
		await env.OPERATIONAL_KV.put(
			`rss-hourly-${cycleId}`,
			JSON.stringify({ timestamp: Date.now(), items: consolidatedItems }),
			{ expirationTtl: 129600 }
		);
	}

	const mergedHealth: Record<string, string> = { ...(existingDailyHealth || {}) };
	for (const hMap of healthPayloads) {
		if (hMap) Object.assign(mergedHealth, hMap);
	}
	if (Object.keys(mergedHealth).length > 0) {
		await env.OPERATIONAL_KV.put(`rss-health-${ingestedDate}`, JSON.stringify(mergedHealth), { expirationTtl: 129600 });
	}

	return consolidatedItems.length;
}

async function pollFeedShardAndSave(
	env: Env,
	cycleId: string,
	ingestedDate: string,
	shardIndex: number,
	totalShards: number,
	sources: RssSource[]
) {
	const shardItems: any[] = [];
	const shardHealth: Record<string, string> = {};

	const promises = sources.map(async (source) => {
		try {
			const res = await fetch(source.url, {
				headers: {
					'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
					'Accept': 'application/rss+xml, application/xml, text/xml, application/atom+xml, */*'
				},
				signal: AbortSignal.timeout(12000)
			});
			if (!res.ok) {
				shardHealth[`${source.name} (${source.region})`] = `FAILED (HTTP ${res.status})`;
				return [];
			}
			const items = await parseFeedXml(await res.text(), source, ingestedDate);
			shardHealth[`${source.name} (${source.region})`] = items.length > 0 ? `SUCCESS (${items.length} items)` : 'EMPTY';
			return items;
		} catch (err: any) {
			const reason = err?.name === 'TimeoutError' ? 'Timeout' : (err?.message || 'Fetch Error').slice(0, 40);
			shardHealth[`${source.name} (${source.region})`] = `FAILED (${reason})`;
			return [];
		}
	});

	const results = await Promise.all(promises);
	shardItems.push(...results.flat());

	const shardDataKey = `rss-hourly-${cycleId}-s${shardIndex}`;
	const shardHealthKey = `rss-health-${ingestedDate}-s${shardIndex}`;

	await env.OPERATIONAL_KV.put(shardDataKey, JSON.stringify({ timestamp: Date.now(), items: shardItems }), { expirationTtl: 129600 });
	await env.OPERATIONAL_KV.put(shardHealthKey, JSON.stringify(shardHealth), { expirationTtl: 129600 });

	if (shardIndex === totalShards - 1) {
		await consolidateHourlyShards(env, cycleId, ingestedDate, totalShards);
	}

	return { shardIndex, sourcesPolled: sources.length, itemsCollected: shardItems.length };
}

async function handleHourlyIngest(env: Env, cycleId: string) {
	const rssData = (await env.CONFIG_KV.get('config-rss-sources', 'json')) as { sources: RssSource[] } | null;
	const allSources = rssData?.sources || [];
	const ingestedDate = cycleId.slice(0, 10);

	const shards: RssSource[][] = [];
	for (let i = 0; i < allSources.length; i += SHARD_SIZE) {
		shards.push(allSources.slice(i, i + SHARD_SIZE));
	}

	if (shards.length === 0) return { status: 'No RSS sources configured' };
	const totalShards = shards.length;

	const queueJobs: { body: IngestBatchQueueMessage; delaySeconds: number }[] = [];
	for (let s = 1; s < totalShards; s++) {
		queueJobs.push({
			body: {
				job_type: 'ingest_shard',
				cycle_id: cycleId,
				ingested_date: ingestedDate,
				shard_index: s,
				total_shards: totalShards,
				sources: shards[s]
			},
			delaySeconds: s * 6
		});
	}
	if (queueJobs.length > 0) {
		await env.NEWS_QUEUE.sendBatch(queueJobs);
	}

	const shard0Result = await pollFeedShardAndSave(env, cycleId, ingestedDate, 0, totalShards, shards[0]);

	return {
		status: `Full ${allSources.length}-feed scatter-gather initiated (${SHARD_SIZE} feeds/worker across ${totalShards} shards; final shard consolidates hourly feed at +${(totalShards - 1) * 6}s)`,
		cycleId,
		totalConfiguredFeeds: allSources.length,
		shardSize: SHARD_SIZE,
		totalShards,
		shard0Result,
		queuedShards: queueJobs.length
	};
}

async function runLlmPrompt(env: Env, baseSystemPrompt: string, userContent: string): Promise<string> {
	const strictSystemPrompt = `${baseSystemPrompt}\n\n[CRITICAL INSTRUCTION]: Focus strictly on the primary event named in the Headline. Ignore any unrelated or tangential news items that may appear in roundup summaries. Write ONLY continuous, flowing paragraphs of prose. Do NOT output any titles, headings, subheadings, bullet points, numbered lists, bold labels, or meta-commentary.`;
	try {
		const response = await env.AI.run('@cf/meta/llama-3.1-8b-instruct', {
			messages: [
				{ role: 'system', content: strictSystemPrompt },
				{ role: 'user', content: userContent }
			],
			max_tokens: 900,
			temperature: 0.2
		});
		if (response && typeof response.response === 'string' && response.response.trim().length > 0) {
			return cleanLlmProse(response.response);
		}
	} catch (err) {
		try {
			const fallback = await env.AI.run('@cf/meta/llama-3.2-3b-instruct', {
				messages: [
					{ role: 'system', content: strictSystemPrompt },
					{ role: 'user', content: userContent }
				],
				max_tokens: 900,
				temperature: 0.2
			});
			if (fallback && typeof fallback.response === 'string' && fallback.response.trim().length > 0) {
				return cleanLlmProse(fallback.response);
			}
		} catch (innerErr) {
			return `AI synthesis unavailable: ${String(innerErr)}`;
		}
	}
	return 'Summary unavailable.';
}

function parseLlmJsonGroups(text: string): number[][] | null {
	if (!text) return null;
	try {
		const start = text.indexOf('[');
		const end = text.lastIndexOf(']');
		if (start !== -1 && end !== -1 && end >= start) {
			const parsed = JSON.parse(text.substring(start, end + 1));
			if (Array.isArray(parsed)) return parsed;
		}
	} catch (e) {}
	return null;
}

// Fast O(N * k) Inverted-Index Pre-Deduplication & Topic Sorting (<1ms CPU)
function preClusterAndSortByTopic(articles: any[]): { multiTopicArticles: any[]; singletons: any[] } {
	const n = articles.length;
	if (n === 0) return { multiTopicArticles: [], singletons: [] };

	const stemToIndices = new Map<string, number[]>();
	for (let i = 0; i < n; i++) {
		const stems: string[] = articles[i]._stems || [];
		for (const s of stems) {
			let list = stemToIndices.get(s);
			if (!list) {
				list = [];
				stemToIndices.set(s, list);
			}
			list.push(i);
		}
	}

	const absorbed = new Uint8Array(n);
	const survivors: any[] = [];

	for (let i = 0; i < n; i++) {
		if (absorbed[i]) continue;
		const master = articles[i];
		const stems: string[] = master._stems || [];
		const overlapCounts = new Map<number, number>();

		for (const s of stems) {
			const matches = stemToIndices.get(s) || [];
			// Raised to 120 so 24-hour country/actor stems (Israel, Brazil, Spain) are never skipped
			if (matches.length > 120) continue;
			for (const idx of matches) {
				if (idx <= i || absorbed[idx]) continue;
				overlapCounts.set(idx, (overlapCounts.get(idx) || 0) + 1);
			}
		}

		for (const [candidateIdx, sharedCount] of overlapCounts.entries()) {
			const cand = articles[candidateIdx];
			const minLen = Math.min(stems.length, (cand._stems || []).length);
			// Merge when >= 3 canonical stems match (e.g. flavi + bolso + brazi, or spain + train + crash)
			if (sharedCount >= 3 || (minLen >= 3 && sharedCount / minLen >= 0.65)) {
				mergeTwoArticles(master, cand);
				absorbed[candidateIdx] = 1;
			}
		}
		survivors.push(master);
	}

	const survivorStemFreq = new Map<string, number>();
	for (const art of survivors) {
		for (const s of (art._stems || [])) {
			survivorStemFreq.set(s, (survivorStemFreq.get(s) || 0) + 1);
		}
	}

	const multiTopicArticles: any[] = [];
	const singletons: any[] = [];

	for (const art of survivors) {
		const sharedStems = (art._stems || []).filter((s: string) => (survivorStemFreq.get(s) || 0) >= 2);
		if (sharedStems.length > 0) {
			sharedStems.sort((a: string, b: string) => (survivorStemFreq.get(a) || 99) - (survivorStemFreq.get(b) || 99) || a.localeCompare(b));
			art._topicKey = sharedStems.slice(0, 3).join('-');
			multiTopicArticles.push(art);
		} else {
			singletons.push(art);
		}
	}

	multiTopicArticles.sort((a, b) => (a._topicKey || '').localeCompare(b._topicKey || ''));
	return { multiTopicArticles, singletons };
}

async function clusterSmallGroupWithLlm(env: Env, chunk: any[]): Promise<any[]> {
	if (chunk.length <= 1) return chunk;
	const headlineList = chunk.map((art, idx) => `[${idx}] ${art.title}`).join('\n');
	const systemPrompt = `You are a strict news editor. Group headlines ONLY if they report on the EXACT same specific event (such as the same election, the same train crash, or the same leader's trip). Never merge two different court cases, two different cyber vulnerabilities, or two different companies. Return ONLY a JSON array of integer arrays for duplicate groups of 2 or more, like [[0, 2], [4, 5]]. If none match, return [].`;

	let groups: number[][] | null = null;
	try {
		const res = await env.AI.run('@cf/meta/llama-3.2-1b-instruct', {
			messages: [
				{ role: 'system', content: systemPrompt },
				{ role: 'user', content: headlineList }
			],
			max_tokens: 220,
			temperature: 0.0
		});
		groups = parseLlmJsonGroups(res.response);
	} catch (e) {}

	const seenIndices = new Set<number>();
	const mergedChunk: any[] = [];

	if (groups && Array.isArray(groups)) {
		for (const group of groups) {
			if (!Array.isArray(group) || group.length < 2) continue;
			const validIndices = group.filter(idx => typeof idx === 'number' && idx >= 0 && idx < chunk.length && !seenIndices.has(idx));
			if (validIndices.length < 2) continue;

			const masterIdx = validIndices[0];
			const masterStems: string[] = chunk[masterIdx]._stems || [];

			// ANTI-FRANKENSTEIN GATE: Require at least 2 shared non-generic stems between headlines to allow an LLM merge
			const verifiedSecondaries: number[] = [];
			for (let j = 1; j < validIndices.length; j++) {
				const idx = validIndices[j];
				const candStems: string[] = chunk[idx]._stems || [];
				if (countSharedStems(masterStems, candStems) >= 2) {
					verifiedSecondaries.push(idx);
				}
			}

			if (verifiedSecondaries.length === 0) continue;

			seenIndices.add(masterIdx);
			const masterArt = { ...chunk[masterIdx] };
			for (const idx of verifiedSecondaries) {
				seenIndices.add(idx);
				mergeTwoArticles(masterArt, chunk[idx]);
			}
			mergedChunk.push(masterArt);
		}
	}

	for (let idx = 0; idx < chunk.length; idx++) {
		if (!seenIndices.has(idx)) {
			mergedChunk.push(chunk[idx]);
		}
	}
	return mergedChunk;
}

async function groupStoriesHierarchically(env: Env, rawArticles: any[]) {
	const byCategory = new Map<string, any[]>();
	for (const art of rawArticles) {
		const cat = art.category_hint || 'general';
		let list = byCategory.get(cat);
		if (!list) {
			list = [];
			byCategory.set(cat, list);
		}
		list.push(art);
	}

	const finalAggregatedMap = new Map<string, any>();
	const smallGroupSize = 18;

	for (const [, catArticles] of byCategory.entries()) {
		const { multiTopicArticles, singletons } = preClusterAndSortByTopic(catArticles);

		const stage1Chunks: any[][] = [];
		for (let i = 0; i < multiTopicArticles.length; i += smallGroupSize) {
			stage1Chunks.push(multiTopicArticles.slice(i, i + smallGroupSize));
		}

		const stage1Survivors: any[] = [];
		for (let i = 0; i < stage1Chunks.length; i += 4) {
			const batch = stage1Chunks.slice(i, i + 4);
			const res = await Promise.all(batch.map(c => clusterSmallGroupWithLlm(env, c)));
			stage1Survivors.push(...res.flat());
		}

		let finalCatClusters = stage1Survivors;
		if (stage1Survivors.length > smallGroupSize) {
			const stage2Pre = preClusterAndSortByTopic(stage1Survivors);
			const stage2Chunks: any[][] = [];
			for (let i = 0; i < stage2Pre.multiTopicArticles.length; i += smallGroupSize) {
				stage2Chunks.push(stage2Pre.multiTopicArticles.slice(i, i + smallGroupSize));
			}
			const stage2Merged: any[] = [];
			for (let i = 0; i < stage2Chunks.length; i += 4) {
				const batch = stage2Chunks.slice(i, i + 4);
				const res = await Promise.all(batch.map(c => clusterSmallGroupWithLlm(env, c)));
				stage2Merged.push(...res.flat());
			}
			finalCatClusters = [...stage2Merged, ...stage2Pre.singletons];
		}

		for (const art of [...finalCatClusters, ...singletons]) {
			if (art && art.article_id) {
				delete art._stems;
				delete art._topicKey;
				finalAggregatedMap.set(art.article_id, art);
			}
		}
	}

	return finalAggregatedMap;
}

async function regenerateControlPage(env: Env): Promise<void> {
	try {
		const { results } = await env.DB.prepare(`
			SELECT story_id, category_id, headline, REPLACE(SUBSTR(markdown_body, 1, 140), char(10), ' ') AS teaser_line
			FROM published_stories
			ORDER BY ingested_date DESC, created_at DESC LIMIT 15
		`).all();
		let markdown = `# Editorial Control Ledger\n\n*Automatically synchronized with Cloudflare D1 state.*\n\n---\n\n## Recent Intelligence Stories\n\n`;
		for (const story of (results || [])) {
			markdown += `- **[${String(story.category_id).toUpperCase()}]** ${story.headline}\n  > _${story.teaser_line}..._\n\n`;
		}
		await fetch(`${env.BOOKSTACK_URL}/api/pages/${CONTROL_PAGE_ID}`, {
			method: 'PUT',
			headers: {
				'Content-Type': 'application/json',
				'Authorization': `Token ${env.BOOKSTACK_TOKEN_ID}:${env.BOOKSTACK_TOKEN_SECRET}`
			},
			body: JSON.stringify({ markdown })
		});
	} catch (err) {}
}

async function handleDailyDispatch(env: Env, cycleId: string) {
	const ingestedDate = cycleId.slice(0, 10);
	const catData = (await env.CONFIG_KV.get('config-categories', 'json')) as CategoriesConfigFile | null;
	if (!catData) throw new Error('No category config found in KV');

	const masterHourKeys = Array.from({ length: 24 }, (_, h) => `rss-hourly-${ingestedDate}-${String(h).padStart(2, '0')}`);
	const masterSnapshots = await Promise.all(
		masterHourKeys.map(k => env.OPERATIONAL_KV.get(k, 'json') as Promise<{ items: any[] } | null>)
	);

	const missingShardKeys: string[] = [];
	for (let h = 0; h < 24; h++) {
		if (!masterSnapshots[h] || !masterSnapshots[h]?.items?.length) {
			const hourStr = String(h).padStart(2, '0');
			for (let s = 0; s < MAX_SHARDS; s++) {
				missingShardKeys.push(`rss-hourly-${ingestedDate}-${hourStr}-s${s}`);
			}
		}
	}

	const fallbackSnapshots: ({ items: any[] } | null)[] = [];
	for (let i = 0; i < missingShardKeys.length; i += 32) {
		const batch = missingShardKeys.slice(i, i + 32);
		const res = await Promise.all(
			batch.map(k => env.OPERATIONAL_KV.get(k, 'json') as Promise<{ items: any[] } | null>)
		);
		fallbackSnapshots.push(...res);
	}

	const allSnapshots = [...masterSnapshots, ...fallbackSnapshots];

	const rawByIdMap = new Map<string, any>();
	for (const snap of allSnapshots) {
		if (!snap || !snap.items) continue;
		for (const art of snap.items) {
			if (!art || !art.article_id || !art.title) continue;
			const existing = rawByIdMap.get(art.article_id);
			if (existing) {
				if (art.source_id && !existing.source_id.includes(art.source_id)) {
					mergeTwoArticles(existing, art);
				}
			} else {
				rawByIdMap.set(art.article_id, {
					...art,
					ingested_date: ingestedDate,
					syndication_velocity: art.syndication_velocity || 1,
					_stems: computeStemsOnce(art.title)
				});
			}
		}
	}

	const filteredRawArticles = Array.from(rawByIdMap.values());
	if (filteredRawArticles.length === 0) {
		throw new Error(`No hourly records found in OPERATIONAL_KV for date ${ingestedDate}`);
	}

	const allArticlesMap = await groupStoriesHierarchically(env, filteredRawArticles);
	const uniqueArticles = Array.from(allArticlesMap.values()).filter(art => art && art.article_id);

	await env.DB.prepare(`DELETE FROM articles WHERE ingested_date = ?`).bind(ingestedDate).run();

	const batchSize = 50;
	for (let i = 0; i < uniqueArticles.length; i += batchSize) {
		const chunk = uniqueArticles.slice(i, i + batchSize);
		const stmts = chunk.map(art => env.DB.prepare(`
			INSERT INTO articles (article_id, canonical_url, title, summary, source_id, category_hint, language_code, syndication_velocity, published_at, ingested_date)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(article_id) DO UPDATE SET
				syndication_velocity = excluded.syndication_velocity,
				source_id = excluded.source_id,
				summary = excluded.summary,
				published_at = excluded.published_at,
				ingested_date = excluded.ingested_date
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
			ingestedDate
		));
		await env.DB.batch(stmts);
	}

	const picked: any[] = [];
	const pickedIds = new Set<string>();
	const overflow: any[] = [];
	const globalMax = catData.global_max_stories || 24;

	// Final safety check during selection: never pick two stories on the same day that share >= 2 canonical stems
	const pickedStemsList: string[][] = [];
	const isDuplicateOfPicked = (candidateTitle: string): boolean => {
		const candStems = computeStemsOnce(candidateTitle);
		return pickedStemsList.some(existingStems => countSharedStems(candStems, existingStems) >= 2);
	};

	for (const cat of catData.categories) {
		const { results } = await env.DB.prepare(`
			SELECT article_id, canonical_url, title, summary, source_id, category_hint, syndication_velocity, published_at
			FROM articles
			WHERE ingested_date = ?
			  AND category_hint = ?
			  AND SUBSTR(article_id, 1, 12) NOT IN (
				SELECT SUBSTR(story_id, 18, 12) FROM published_stories WHERE ingested_date < ?
			  )
			ORDER BY syndication_velocity DESC, published_at DESC
			LIMIT ?
		`).bind(ingestedDate, cat.id, ingestedDate, (cat.max_stories || 5) * 2).all();

		const items = (results || []) as any[];
		let catPickedCount = 0;
		for (const item of items) {
			if (pickedIds.has(item.article_id) || isDuplicateOfPicked(item.title)) continue;
			if (catPickedCount < cat.min_stories) {
				picked.push(item);
				pickedIds.add(item.article_id);
				pickedStemsList.push(computeStemsOnce(item.title));
				catPickedCount++;
			} else if (catPickedCount < (cat.max_stories || 5)) {
				overflow.push(item);
				catPickedCount++;
			}
		}
	}

	overflow.sort((a, b) => b.syndication_velocity - a.syndication_velocity);
	for (const item of overflow) {
		if (picked.length >= globalMax) break;
		if (!pickedIds.has(item.article_id) && !isDuplicateOfPicked(item.title)) {
			picked.push(item);
			pickedIds.add(item.article_id);
			pickedStemsList.push(computeStemsOnce(item.title));
		}
	}

	await env.DB.prepare(`DELETE FROM published_stories WHERE ingested_date = ?`).bind(ingestedDate).run();

	const categoryMap = new Map<string, CategoryConfig>();
	catData.categories.forEach(c => categoryMap.set(c.id, c));

	const messages: StoryQueueMessage[] = picked.map(article => {
		const cat = categoryMap.get(article.category_hint) || catData.categories[0];
		return {
			job_type: 'story_synthesis',
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
			ingested_date: ingestedDate,
			published_at: article.published_at || Math.floor(Date.now() / 1000)
		};
	});

	if (messages.length > 0) {
		const batchSend = messages.map(msg => ({ body: msg }));
		await env.NEWS_QUEUE.sendBatch(batchSend);
	}

	return {
		status: '24-hour Hybrid Topic-Sorted LLM Hierarchy complete & dispatched to queue',
		ingestedDate,
		rawUniqueCount: filteredRawArticles.length,
		clusteredEventsCount: uniqueArticles.length,
		dispatchedStories: messages.length
	};
}

async function handlePublish(env: Env, cycleId: string) {
	const ingestedDate = cycleId.slice(0, 10);
	const { results } = await env.DB.prepare(`
		SELECT category_id, headline, markdown_body
		FROM published_stories
		WHERE ingested_date = ?
		ORDER BY category_id ASC, created_at ASC
	`).bind(ingestedDate).all();

	if (!results || results.length === 0) return { status: 'No stories to publish', ingestedDate };

	const now = new Date();
	const hours = String(now.getUTCHours()).padStart(2, '0');
	const mins = String(now.getUTCMinutes()).padStart(2, '0');
	const timeStr = `${hours}:${mins}`;

	let healthAuditMarkdown = `\n\n---\n\n## Operational Feed Health & Source Audit Ledger\n\n| Source Publisher & Region | Status / Item Yield |\n| :--- | :--- |\n`;
	try {
		const healthKeys = [
			`rss-health-${ingestedDate}`,
			...Array.from({ length: MAX_SHARDS }, (_, s) => `rss-health-${ingestedDate}-s${s}`)
		];
		const healthMaps = await Promise.all(
			healthKeys.map(k => env.OPERATIONAL_KV.get(k, 'json') as Promise<Record<string, string> | null>)
		);
		const combinedHealth: Record<string, string> = {};
		for (const hMap of healthMaps) {
			if (hMap) Object.assign(combinedHealth, hMap);
		}

		if (Object.keys(combinedHealth).length > 0) {
			for (const [src, status] of Object.entries(combinedHealth)) {
				healthAuditMarkdown += `| ${src} | ${status} |\n`;
			}
		} else {
			healthAuditMarkdown += `| Operational Audit | Standard Ingestion Active |\n`;
		}
	} catch (e) {
		healthAuditMarkdown += `| Operational Audit | Audit Data Unavailable |\n`;
	}

	let dailyMarkdown = `*Automated global intelligence digest (${results.length} stories) — Generated at ${timeStr} UTC.*\n\n---\n\n`;

	for (const st of results as any[]) {
		dailyMarkdown += `## ${st.headline}\n\n${st.markdown_body}\n\n---\n\n`;
	}

	dailyMarkdown += healthAuditMarkdown;

	await env.ARCHIVE_BUCKET.put(`daily/${ingestedDate}/world-daily-diary-${ingestedDate}.md`, dailyMarkdown, {
		httpMetadata: { contentType: 'text/markdown; charset=utf-8' }
	});

	let bookstackPageId = null;
	try {
		const bsResponse = await fetch(`${env.BOOKSTACK_URL}/api/pages`, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'Authorization': `Token ${env.BOOKSTACK_TOKEN_ID}:${env.BOOKSTACK_TOKEN_SECRET}`
			},
			body: JSON.stringify({
				book_id: Number(env.MASTER_BOOK_ID),
				name: `World Events - ${ingestedDate} at ${timeStr}`,
				markdown: dailyMarkdown
			})
		});
		if (bsResponse.ok) {
			bookstackPageId = (await bsResponse.json() as any).id || null;
			await env.DB.prepare(`
				UPDATE published_stories
				SET r2_synced = 1, bookstack_synced = 1, bookstack_page_id = ?
				WHERE ingested_date = ?
			`).bind(bookstackPageId, ingestedDate).run();
		}
	} catch (e) {}

	const ninetyDaysAgoDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
	await env.DB.prepare(`DELETE FROM articles WHERE ingested_date < ?`).bind(ninetyDaysAgoDate).run();

	await regenerateControlPage(env);
	return { status: 'Publish complete', ingestedDate, bookstackPageId, storiesPublished: results.length };
}

export default {
	async queue(batch: MessageBatch<QueuePayload>, env: Env): Promise<void> {
		for (const message of batch.messages) {
			const payload = message.body;

			if (payload.job_type === 'ingest_shard') {
				await pollFeedShardAndSave(
					env,
					payload.cycle_id,
					payload.ingested_date,
					payload.shard_index,
					payload.total_shards || 12,
					payload.sources
				);
				message.ack();
				continue;
			}

			const msg = payload as StoryQueueMessage;

			const formatter = new Intl.DateTimeFormat('en-GB', {
				timeZone: env.SYSTEM_TIMEZONE || 'Europe/London',
				year: 'numeric',
				month: 'short',
				day: 'numeric',
				hour: '2-digit',
				minute: '2-digit',
				timeZoneName: 'short'
			});
			const formattedTime = formatter.format(new Date((msg.published_at || Math.floor(Date.now() / 1000)) * 1000));

			const userContext = `Primary Headline: ${msg.title}\nCategory: ${msg.category_name}\nReported Time: ${formattedTime}\nVerified Contributing Sources: ${msg.source_id}\nSyndication Velocity (Update Count): ${msg.syndication_velocity}\nSource Material (Focus ONLY on reports matching the Primary Headline):\n${msg.summary}\nURL: ${msg.canonical_url}`;

			// Step 1: Primary LLM creates the event report and impact/framing analysis
			const [eventReport, analysisReport] = await Promise.all([
				runLlmPrompt(env, msg.prompt_event, userContext),
				runLlmPrompt(env, msg.prompt_analysis, userContext)
			]);

			// Step 2: Query historical records strictly >= 3 days older than msg.ingested_date so yesterday's breaking news never echoes
			const ingestedMs = Date.parse(`${msg.ingested_date}T00:00:00Z`) || Date.now();
			const threeDaysBeforeIngest = new Date(ingestedMs - 3 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
			const ninetyDaysBeforeIngest = new Date(ingestedMs - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

			const [{ results: pastDiaryStories }, { results: pastRssArticles }] = await Promise.all([
				env.DB.prepare(`
					SELECT ingested_date, headline
					FROM published_stories
					WHERE category_id = ? AND ingested_date <= ? AND ingested_date >= ?
					ORDER BY ingested_date DESC LIMIT 40
				`).bind(msg.category_id, threeDaysBeforeIngest, ninetyDaysBeforeIngest).all(),
				env.DB.prepare(`
					SELECT ingested_date, title, source_id, SUBSTR(summary, 1, 280) AS summary_snippet
					FROM articles
					WHERE category_hint = ? AND ingested_date <= ? AND ingested_date >= ?
					ORDER BY syndication_velocity DESC, published_at DESC LIMIT 80
				`).bind(msg.category_id, threeDaysBeforeIngest, ninetyDaysBeforeIngest).all()
			]);

			const currentStems = computeStemsOnce(msg.title);

			// Require >= 3 shared specific stems so coincidental words (like "key" or "apple") don't trigger unrelated history
			const isTrueHistoricalPrecursor = (pastTitle: string) => {
				const pastStems = computeStemsOnce(pastTitle);
				return countSharedStems(currentStems, pastStems) >= 3;
			};

			const matchingDiary = (pastDiaryStories || [])
				.filter((s: any) => isTrueHistoricalPrecursor(String(s.headline || '')))
				.slice(0, 4);

			const matchingRss = (pastRssArticles || [])
				.filter((a: any) => isTrueHistoricalPrecursor(String(a.title || '')))
				.slice(0, 6);

			let historicReportRaw = 'NONE';
			if (matchingDiary.length > 0 || matchingRss.length > 0) {
				let archiveDossier = '';
				if (matchingDiary.length > 0) {
					archiveDossier += `PREVIOUSLY PUBLISHED DIARY ARCHIVE:\n` +
						matchingDiary.map((s: any) => `[${s.ingested_date}] ${s.headline}`).join('\n') + `\n\n`;
				}
				if (matchingRss.length > 0) {
					archiveDossier += `90-DAY HISTORICAL RSS ARCHIVE:\n` +
						matchingRss.map((a: any) => `[${a.ingested_date} | ${a.source_id}] ${a.title}: ${String(a.summary_snippet || '').replace(/\s+/g, ' ')}`).join('\n');
				}

				const historyPrompt = `You are a strict historical intelligence archivist. Today's breaking headline is: "${msg.title}". Below are older records from our 90-day archive. If these older records genuinely document earlier precursor events to today's exact story, write a single concise paragraph summarizing ONLY those earlier archived events and their dates as background context. If the older records are about a different topic or different product, output the single word NONE and nothing else. Write only flowing prose with no headers, bullet points, or lists.`;

				historicReportRaw = await runLlmPrompt(env, historyPrompt, archiveDossier);
			}

			const secondHeading = msg.category_id === 'general'
				? 'Source Bias & Geopolitical Framing'
				: `${msg.category_name} Impact Analysis`;

			let compiledMarkdown = `### The Event\n${eventReport}\n\n`;

			const cleanHistory = (historicReportRaw || '').trim();
			const hasValidHistory =
				(matchingDiary.length > 0 || matchingRss.length > 0) &&
				cleanHistory.length > 30 &&
				!/^none\b/i.test(cleanHistory) &&
				!cleanHistory.toLowerCase().includes('none of the') &&
				!cleanHistory.toLowerCase().includes('no direct connection') &&
				!cleanHistory.toLowerCase().includes('not directly related') &&
				!cleanHistory.toLowerCase().includes('different topic') &&
				!cleanHistory.toLowerCase().includes('ai synthesis unavailable') &&
				!cleanHistory.toLowerCase().includes('summary unavailable');

			if (hasValidHistory) {
				compiledMarkdown += `### 90-Day Archive Context\n${cleanHistory}\n\n`;
			}

			compiledMarkdown += `### ${secondHeading}\n${analysisReport}\n\n**Published:** ${formattedTime} | **Verified Sources:** ${msg.source_id} | [Primary Link](${msg.canonical_url})`;

			const bodyHash = await sha256Hex(compiledMarkdown);
			const storyId = `story-${msg.ingested_date}-${msg.article_id.slice(0, 12)}`;

			await env.DB.prepare(`
				INSERT INTO published_stories (story_id, ingested_date, category_id, headline, markdown_body, original_body_hash, search_entities_json, sources_json, tracking_status, tracking_origin)
				VALUES (?, ?, ?, ?, ?, ?, '[]', '[]', 0, 'queue_auto')
				ON CONFLICT(story_id) DO UPDATE SET
					markdown_body = excluded.markdown_body,
					original_body_hash = excluded.original_body_hash
			`).bind(storyId, msg.ingested_date, msg.category_id, msg.title, compiledMarkdown, bodyHash).run();

			message.ack();
		}
	},

	async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		const date = new Date(event.scheduledTime || Date.now());
		const hour = date.getUTCHours();
		const minute = date.getUTCMinutes();
		const cycleId = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}-${String(hour).padStart(2, '0')}`;

		if (minute === 0) {
			await handleHourlyIngest(env, cycleId);
		}

		if (hour === 23 && minute === 0) {
			await handleDailyDispatch(env, cycleId);
		} else if (hour === 23 && minute === 5) {
			await handlePublish(env, cycleId);
		}
	},

	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		if (url.pathname === '/trigger') {
			const key = url.searchParams.get('key');
			if (!key || key !== env.ADMIN_INTERVENTION_KEY) return new Response('Unauthorized', { status: 401 });

			const action = url.searchParams.get('action');
			const dateParam = url.searchParams.get('date');
			const now = new Date();
			const ingestedDate = dateParam || now.toISOString().slice(0, 10);
			const currentHour = String(now.getUTCHours()).padStart(2, '0');
			const cycleId = `${ingestedDate}-${currentHour}`;

			try {
				if (action === 'ingest') {
					return new Response(JSON.stringify(await handleHourlyIngest(env, cycleId), null, 2), {
						headers: { 'Content-Type': 'application/json' }
					});
				}

				if (action === 'dispatch') {
					return new Response(JSON.stringify(await handleDailyDispatch(env, cycleId), null, 2), {
						headers: { 'Content-Type': 'application/json' }
					});
				}

				if (action === 'publish') {
					return new Response(JSON.stringify(await handlePublish(env, cycleId), null, 2), {
						headers: { 'Content-Type': 'application/json' }
					});
				}

				return new Response('Specify ?action=ingest, dispatch, or publish', { status: 400 });
			} catch (err: any) {
				return new Response(JSON.stringify({ error: err.message, stack: err.stack }, null, 2), {
					status: 500,
					headers: { 'Content-Type': 'application/json' }
				});
			}
		}
		return new Response('Not found', { status: 404 });
	}
};
