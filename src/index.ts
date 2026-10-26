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
};

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

// --- MASTER WORKFLOW ORCHESTRATOR ---

export class NewsPipelineWorkflow extends WorkflowEntrypoint<Env, PipelineParams> {
	async run(event: WorkflowEvent<PipelineParams>, step: WorkflowStep) {
		const { cycleId, isDailyRun } = event.payload;

		const rssSources = await step.do('read-config-sources', async () => {
			const data = await this.env.CONFIG_KV.get('config-rss-sources', 'json');
			return data || { sources: [] };
		});

		const fetcherKeys = await step.do('dispatch-rss-fetchers', async () => {
			return [];
		});

		await step.do('hourly-condenser', async () => {
		});

		if (!isDailyRun) {
			return { status: "Hourly cycle complete", cycleId };
		}

		const dailyStats = await step.do('daily-condenser-phase1', async () => {
			return { uniqueArticles: 0 };
		});

		await step.do('daily-condenser-phase2', async () => {
		});

		const pickedStories = await step.do('category-story-pickers', async () => {
			return [];
		});

		await step.do('research-and-write', async () => {
		});

		await step.do('daily-copier', async () => {
		});

		return { status: "24-hour cycle complete", cycleId };
	}
}

// --- EDGE WORKER HANDLERS ---

export default {
	async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		const date = new Date(event.cron);
		const hour = date.getUTCHours();
		const cycleId = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}-${String(hour).padStart(2, '0')}`;
		const isDailyRun = (hour === 23);

		await env.NEWS_WORKFLOW.create({
			id: `pipeline-${cycleId}`,
			params: { cycleId, isDailyRun }
		});
	},

	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		
		if (url.pathname !== '/act') {
			return new Response("Not found", { status: 404 });
		}

		const storyId = url.searchParams.get('id');
		const action = url.searchParams.get('do');
		const signature = url.searchParams.get('sig');

		if (!storyId || !action || !signature) {
			return new Response("Missing parameters", { status: 400 });
		}

		const isValid = await verifyActionSignature(storyId, action, signature, env.ADMIN_INTERVENTION_KEY);
		
		if (!isValid) {
			return new Response("Forbidden: Invalid cryptographic signature.", { status: 403 });
		}

		if (request.method === "GET") {
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
		} else if (request.method === "POST") {
			const formData = await request.formData().catch(() => new FormData());
			const operatorInput = formData.get('directive') || formData.get('focus') || null;
			
			return new Response(`Action '${action}' confirmed for ${storyId}. D1 mutation logic pending.`, { status: 200 });
		}

		return new Response("Method not allowed", { status: 405 });
	}
};
