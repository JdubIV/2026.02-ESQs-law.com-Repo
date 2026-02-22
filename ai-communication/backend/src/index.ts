/**
 * ESQs Law - Cloudflare Workers API
 * Complete backend running on Cloudflare edge
 */

export interface Env {
	DB: D1Database;
	MEMORY_DB: D1Database;
	DOCUMENTS: R2Bucket;
	SESSIONS: KVNamespace;
	CACHE: KVNamespace;
	AI: any;
	MEMORY_INDEX: VectorizeIndex;
	OPENAI_API_KEY: string;
	ANTHROPIC_API_KEY: string;
	XAI_API_KEY: string;
	GROQ_API_KEY: string;
	GEMINI_API_KEY: string;
	GOOGLE_OAUTH_CLIENT_ID: string;
	GOOGLE_OAUTH_CLIENT_SECRET: string;
	GOOGLE_REFRESH_TOKEN: string;
	MICROSOFT_CLIENT_ID: string;
	MICROSOFT_CLIENT_SECRET: string;
	MICROSOFT_REFRESH_TOKEN: string;
	ONEDRIVE_FOLDER_ID: string;
	AUTH_SECRET: string;
	ENVIRONMENT: string;
}

const corsHeaders = {
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
	'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

function json(data: any, status = 200): Response {
	return new Response(JSON.stringify(data), {
		status,
		headers: { 'Content-Type': 'application/json', ...corsHeaders },
	});
}

function err(message: string, status = 500): Response {
	return json({ success: false, error: message }, status);
}

// Levenshtein distance for fuzzy name matching
function levenshtein(a: string, b: string): number {
	const m = a.length, n = b.length;
	if (m === 0) return n;
	if (n === 0) return m;
	const dp: number[][] = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
	for (let i = 0; i <= m; i++) dp[i][0] = i;
	for (let j = 0; j <= n; j++) dp[0][j] = j;
	for (let i = 1; i <= m; i++) {
		for (let j = 1; j <= n; j++) {
			dp[i][j] = a[i-1] === b[j-1] ? dp[i-1][j-1] : 1 + Math.min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]);
		}
	}
	return dp[m][n];
}

function fuzzyNameMatch(a: string, b: string): boolean {
	if (a === b) return true;
	if (a.length < 3 || b.length < 3) return false;
	if (a.includes(b) || b.includes(a)) return true;
	const len = Math.max(a.length, b.length);
	return levenshtein(a, b) <= Math.max(1, Math.floor(len / 4));
}

/** Mountain Time helpers — all dates/times should use these instead of raw UTC */
function mtnNow(): Date {
	return new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Denver' }));
}

function mtnToday(): string {
	return new Date().toLocaleDateString('en-CA', { timeZone: 'America/Denver' }); // YYYY-MM-DD
}

function mtnISO(): string {
	const d = new Date();
	const parts = new Intl.DateTimeFormat('en-US', {
		timeZone: 'America/Denver',
		year: 'numeric', month: '2-digit', day: '2-digit',
		hour: '2-digit', minute: '2-digit', second: '2-digit',
		hour12: false,
	}).formatToParts(d);
	const p: Record<string, string> = {};
	for (const { type, value } of parts) p[type] = value;
	return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:${p.second}`;
}

/** RAG Memory Helpers — embed, search, store */
async function embedText(ai: any, text: string): Promise<number[]> {
	if (!ai) throw new Error('AI binding not available');
	const truncated = text.substring(0, 8000); // BGE model max ~512 tokens, truncate long inputs
	const result = await ai.run('@cf/baai/bge-base-en-v1.5', { text: [truncated] });
	if (!result?.data?.[0]) throw new Error('Embedding returned no data');
	return result.data[0];
}

async function ragSearch(ai: any, index: VectorizeIndex, query: string, topK = 6, filter?: VectorizeVectorMetadataFilter): Promise<VectorizeMatch[]> {
	const queryVec = await embedText(ai, query);
	const results = await index.query(queryVec, { topK, filter, returnMetadata: 'all' });
	return results.matches || [];
}

async function ragStore(ai: any, index: VectorizeIndex, db: D1Database, chunk: {
	id: string; type: string; source: string; content: string;
	clientName?: string; caseNumber?: string;
}) {
	const vec = await embedText(ai, chunk.content);
	await index.upsert([{
		id: chunk.id,
		values: vec,
		metadata: {
			chunk_type: chunk.type,
			source: chunk.source,
			client_name: chunk.clientName || '',
			case_number: chunk.caseNumber || '',
			preview: chunk.content.substring(0, 200),
		}
	}]);
	await db.prepare(
		`INSERT OR REPLACE INTO memory_chunks (id, chunk_type, source, client_name, case_number, content, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
	).bind(chunk.id, chunk.type, chunk.source, chunk.clientName || null, chunk.caseNumber || null, chunk.content, mtnISO()).run();
}

function hashString(str: string): string {
	let hash = 0;
	for (let i = 0; i < str.length; i++) {
		const char = str.charCodeAt(i);
		hash = ((hash << 5) - hash) + char;
		hash |= 0;
	}
	return Math.abs(hash).toString(36);
}

/** Predictive Intelligence Engine — Bayesian weighted frequency analysis */
interface PredictionResult {
	activity_type: string;
	activity_subtype: string | null;
	predictions: { outcome: string; probability: number; count: number }[];
	most_likely: string;
	most_likely_pct: number;
	sample_size: number;
	confidence: 'low' | 'moderate' | 'high';
	trend: 'increasing' | 'stable' | 'decreasing' | 'insufficient';
	summary: string;
	by_role?: Record<string, { most_likely: string; probability: number; count: number; summary: string }>;
}

function computePredictions(logs: any[]): PredictionResult[] {
	// Group by (activity_type, activity_subtype)
	const groups = new Map<string, any[]>();
	for (const log of logs) {
		const key = `${log.activity_type}|${log.activity_subtype || ''}`;
		if (!groups.has(key)) groups.set(key, []);
		groups.get(key)!.push(log);
	}

	const now = Date.now();
	const SIX_MONTHS = 180 * 24 * 60 * 60 * 1000;
	const TWELVE_MONTHS = 365 * 24 * 60 * 60 * 1000;
	const PRIOR_STRENGTH = 2; // Bayesian smoothing — real data dominates after 3+ observations

	const results: PredictionResult[] = [];

	for (const [key, entries] of groups) {
		const [activity_type, activity_subtype] = key.split('|');

		// Count outcomes with recency weighting
		const weightedCounts: Record<string, number> = {};
		let totalWeighted = 0;

		// Sort by date for trend analysis
		const sorted = entries.sort((a: any, b: any) => (a.date || '').localeCompare(b.date || ''));

		for (const e of sorted) {
			const outcome = (e.outcome || 'unknown').toLowerCase();
			const eventDate = new Date(e.date).getTime();
			const age = now - eventDate;
			// Recency weight: last 6 months 2x, 6-12 months 1x, older 0.5x
			const weight = age < SIX_MONTHS ? 2.0 : age < TWELVE_MONTHS ? 1.0 : 0.5;
			weightedCounts[outcome] = (weightedCounts[outcome] || 0) + weight;
			totalWeighted += weight;
		}

		// Bayesian smoothing with uniform prior across observed outcomes
		const outcomes = Object.keys(weightedCounts);
		const priorRate = 1 / Math.max(outcomes.length, 2); // uniform prior
		const predictions: { outcome: string; probability: number; count: number }[] = [];

		for (const outcome of outcomes) {
			const raw = weightedCounts[outcome];
			const smoothed = (raw + PRIOR_STRENGTH * priorRate) / (totalWeighted + PRIOR_STRENGTH);
			const actualCount = entries.filter((e: any) => (e.outcome || '').toLowerCase() === outcome).length;
			predictions.push({ outcome, probability: Math.round(smoothed * 100), count: actualCount });
		}
		predictions.sort((a, b) => b.probability - a.probability);

		const n = entries.length;
		const confidence: 'low' | 'moderate' | 'high' = n >= 10 ? 'high' : n >= 5 ? 'moderate' : 'low';

		// Trend: compare last 3 vs overall for most likely outcome
		let trend: 'increasing' | 'stable' | 'decreasing' | 'insufficient' = 'insufficient';
		if (n >= 5 && predictions.length > 0) {
			const topOutcome = predictions[0].outcome;
			const last3 = sorted.slice(-3);
			const last3Rate = last3.filter((e: any) => (e.outcome || '').toLowerCase() === topOutcome).length / last3.length;
			const overallRate = predictions[0].probability / 100;
			if (last3Rate > overallRate + 0.15) trend = 'increasing';
			else if (last3Rate < overallRate - 0.15) trend = 'decreasing';
			else trend = 'stable';
		}

		const topPred = predictions[0];
		const label = activity_subtype ? `${activity_type} (${activity_subtype})` : activity_type;
		const summary = topPred
			? `${label}: LIKELY ${topPred.outcome.toUpperCase()} (${topPred.probability}%, n=${n}, confidence: ${confidence}, trend: ${trend})`
			: `${label}: insufficient data`;

		// Party role segmentation — break down by plaintiff/defendant/petitioner/respondent
		const by_role: Record<string, { most_likely: string; probability: number; count: number; summary: string }> = {};
		const roleEntries = entries.filter((e: any) => e.party_role && e.party_role.trim() !== '');
		if (roleEntries.length >= 2) {
			const roleGroups = new Map<string, any[]>();
			for (const e of roleEntries) {
				const r = (e.party_role as string).toLowerCase();
				if (!roleGroups.has(r)) roleGroups.set(r, []);
				roleGroups.get(r)!.push(e);
			}
			for (const [role, rEntries] of roleGroups) {
				if (rEntries.length < 1) continue;
				const rCounts: Record<string, number> = {};
				let rTotal = 0;
				for (const e of rEntries) {
					const o = (e.outcome || 'unknown').toLowerCase();
					const age = now - new Date(e.date).getTime();
					const w = age < SIX_MONTHS ? 2.0 : age < TWELVE_MONTHS ? 1.0 : 0.5;
					rCounts[o] = (rCounts[o] || 0) + w;
					rTotal += w;
				}
				const rOutcomes = Object.keys(rCounts);
				const rPrior = 1 / Math.max(rOutcomes.length, 2);
				let best = '', bestPct = 0;
				for (const o of rOutcomes) {
					const pct = Math.round(((rCounts[o] + PRIOR_STRENGTH * rPrior) / (rTotal + PRIOR_STRENGTH)) * 100);
					if (pct > bestPct) { best = o; bestPct = pct; }
				}
				by_role[role] = {
					most_likely: best, probability: bestPct, count: rEntries.length,
					summary: `as ${role.toUpperCase()}: ${best.toUpperCase()} ${bestPct}% (n=${rEntries.length})`
				};
			}
		}

		results.push({
			activity_type,
			activity_subtype: activity_subtype || null,
			predictions,
			most_likely: topPred?.outcome || 'unknown',
			most_likely_pct: topPred?.probability || 0,
			sample_size: n,
			confidence,
			trend,
			summary,
			...(Object.keys(by_role).length > 0 && { by_role }),
		});
	}

	return results.sort((a, b) => b.sample_size - a.sample_size);
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		if (request.method === 'OPTIONS') {
			return new Response(null, { headers: corsHeaders });
		}

		const url = new URL(request.url);
		const path = url.pathname;

		try {
			// ═══════════════════════════════════════════════════════════════
			// HEALTH
			// ═══════════════════════════════════════════════════════════════
			if (path === '/' || path === '/health' || path === '/api/health') {
				return json({
					status: 'operational',
					version: '2.0.0-cloudflare',
					platform: 'Cloudflare Workers + D1 + R2',
					timestamp: mtnISO()
				});
			}

			// ═══════════════════════════════════════════════════════════════
			// AUTH
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/auth/login' && request.method === 'POST') {
				const { email, password } = await request.json() as any;
				if (!password) return err('Password required', 400);

				const adminPass = env.AUTH_SECRET;
				if (!adminPass) return err('AUTH_SECRET not configured', 500);
				let userRole = 'admin';
				let userName = email || 'admin';
				if (password !== adminPass) {
					const user = await env.DB.prepare('SELECT * FROM users WHERE email = ? AND active = 1').bind(email).first() as any;
					if (!user) return err('Invalid credentials', 401);
					userRole = user.role || 'client';
					userName = user.full_name || email;
				}

				const token = crypto.randomUUID();
				await env.SESSIONS.put(token, JSON.stringify({ email: email || 'admin', role: userRole, name: userName, loginTime: mtnISO() }), { expirationTtl: 86400 });
				return json({ success: true, token, user: { email: email || 'admin', role: userRole, name: userName }, redirect: '/' });
			}
			
			if (path === '/api/auth/logout' && request.method === 'POST') {
				const auth = request.headers.get('Authorization');
				if (auth?.startsWith('Bearer ')) await env.SESSIONS.delete(auth.substring(7));
				return json({ success: true });
			}
			
			if (path === '/api/auth/status') {
				const auth = request.headers.get('Authorization');
				if (auth?.startsWith('Bearer ')) {
					const session = await env.SESSIONS.get(auth.substring(7));
					if (session) {
						const s = JSON.parse(session);
						return json({ authenticated: true, user: s, role: s.role || 'admin' });
					}
				}
				return json({ authenticated: false });
			}

			// ═══════════════════════════════════════════════════════════════
			// CLIENTS
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/clients' && request.method === 'GET') {
				const status = url.searchParams.get('status');
				const search = url.searchParams.get('search');
				let q = 'SELECT * FROM clients WHERE 1=1';
				const p: any[] = [];
				if (status) { q += ' AND status = ?'; p.push(status); }
				if (search) { q += ' AND (name LIKE ? OR email LIKE ?)'; p.push(`%${search}%`, `%${search}%`); }
				q += ' ORDER BY name LIMIT 100';
				const { results } = await env.DB.prepare(q).bind(...p).all();
				return json({ success: true, clients: results });
			}
			
			if (path === '/api/clients' && request.method === 'POST') {
				const { name, email = null, phone = null, address = null, city = null, state = null, zip = null, notes = null } = await request.json() as any;
				if (!name) return err('Client name required', 400);
				const r = await env.DB.prepare(
					`INSERT INTO clients (name, email, phone, address, city, state, zip, notes, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
				).bind(name, email, phone, address, city, state, zip, notes, mtnISO()).run();
				return json({ success: true, id: r.meta.last_row_id });
			}
			
			const clientMatch = path.match(/^\/api\/clients\/(\d+)$/);
			if (clientMatch) {
				const id = clientMatch[1];
				if (request.method === 'GET') {
					const client = await env.DB.prepare('SELECT * FROM clients WHERE id = ?').bind(id).first();
					return client ? json({ success: true, client }) : err('Not found', 404);
				}
				if (request.method === 'PUT') {
					const { name, email, phone, address, city, state, zip, notes, status } = await request.json() as any;
					await env.DB.prepare('UPDATE clients SET name=?, email=?, phone=?, address=?, city=?, state=?, zip=?, notes=?, status=? WHERE id=?')
						.bind(name, email, phone, address, city, state, zip, notes, status, id).run();
					return json({ success: true });
				}
				if (request.method === 'DELETE') {
					await env.DB.prepare('DELETE FROM clients WHERE id = ?').bind(id).run();
					return json({ success: true });
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// CASES
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/cases' && request.method === 'GET') {
				const clientId = url.searchParams.get('client_id');
				const status = url.searchParams.get('status');
				let q = 'SELECT c.*, cl.name as client_name FROM cases c LEFT JOIN clients cl ON c.client_id = cl.id WHERE 1=1';
				const p: any[] = [];
				if (clientId) { q += ' AND c.client_id = ?'; p.push(clientId); }
				if (status) { q += ' AND c.status = ?'; p.push(status); }
				q += ' ORDER BY c.created_date DESC';
				const { results } = await env.DB.prepare(q).bind(...p).all();
				return json({ success: true, cases: results });
			}
			
			if (path === '/api/cases' && request.method === 'POST') {
				const { client_id, case_number = null, case_type, state, court, facts = null, notes = null } = await request.json() as any;
				if (!client_id || !case_type || !state || !court) return err('Missing required fields', 400);
				const r = await env.DB.prepare(
					`INSERT INTO cases (client_id, case_number, case_type, state, court, facts, notes, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
				).bind(client_id, case_number, case_type, state, court, facts, notes, mtnISO()).run();
				return json({ success: true, id: r.meta.last_row_id });
			}

			// ═══════════════════════════════════════════════════════════════
			// TASKS
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/tasks' && request.method === 'GET') {
				const caseId = url.searchParams.get('case_id');
				const status = url.searchParams.get('status');
				let q = 'SELECT t.*, c.case_number FROM tasks t LEFT JOIN cases c ON t.case_id = c.id WHERE 1=1';
				const p: any[] = [];
				if (caseId) { q += ' AND t.case_id = ?'; p.push(caseId); }
				if (status) { q += ' AND t.status = ?'; p.push(status); }
				q += ' ORDER BY t.due_date ASC';
				const { results } = await env.DB.prepare(q).bind(...p).all();
				return json({ success: true, tasks: results });
			}
			
			if (path === '/api/tasks' && request.method === 'POST') {
				const { case_id, title, description, due_date, priority, assigned_to } = await request.json() as any;
				if (!title) return err('Task title required', 400);
				const r = await env.DB.prepare(
					`INSERT INTO tasks (case_id, title, description, due_date, priority, assigned_to, created_date) VALUES (?, ?, ?, ?, ?, ?, ?)`
				).bind(case_id, title, description, due_date, priority || 'Medium', assigned_to, mtnISO()).run();
				return json({ success: true, id: r.meta.last_row_id });
			}
			
			const taskMatch = path.match(/^\/api\/tasks\/(\d+)$/);
			if (taskMatch) {
				const id = taskMatch[1];
				if (request.method === 'PUT') {
					const { title, description, due_date, priority, status, assigned_to } = await request.json() as any;
					await env.DB.prepare('UPDATE tasks SET title=?, description=?, due_date=?, priority=?, status=?, assigned_to=? WHERE id=?')
						.bind(title, description, due_date, priority, status, assigned_to, id).run();
					return json({ success: true });
				}
				if (request.method === 'DELETE') {
					await env.DB.prepare('DELETE FROM tasks WHERE id = ?').bind(id).run();
					return json({ success: true });
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// CALENDAR
			// ═══════════════════════════════════════════════════════════════
			if ((path === '/api/calendar' || path === '/api/calendar/events') && request.method === 'GET') {
				const start = url.searchParams.get('start');
				const end = url.searchParams.get('end');
				const month = url.searchParams.get('month');
				let q = 'SELECT cal.*, c.case_number, cl.name as client_name FROM calendar cal LEFT JOIN cases c ON cal.case_id = c.id LEFT JOIN clients cl ON c.client_id = cl.id WHERE 1=1';
				const p: any[] = [];
				if (month) {
					const [y, m] = month.split('-').map(Number);
					const nextMonth = m === 12 ? `${y+1}-01-01` : `${y}-${String(m+1).padStart(2,'0')}-01`;
					q += ' AND cal.event_date >= ? AND cal.event_date < ?'; p.push(`${month}-01`, nextMonth);
				}
				if (start) { q += ' AND cal.event_date >= ?'; p.push(start); }
				if (end) { q += ' AND cal.event_date <= ?'; p.push(end); }
				q += ' ORDER BY cal.event_date ASC';
				const { results } = await env.DB.prepare(q).bind(...p).all();
				return json({ success: true, events: results });
			}
			
			if (path === '/api/calendar' && request.method === 'POST') {
				const { case_id, title, event_type, event_date, description, location } = await request.json() as any;
				if (!title || !event_type || !event_date) return err('Missing required fields', 400);
				const r = await env.DB.prepare(
					`INSERT INTO calendar (case_id, title, event_type, event_date, description, location) VALUES (?, ?, ?, ?, ?, ?)`
				).bind(case_id, title, event_type, event_date, description, location).run();
				return json({ success: true, id: r.meta.last_row_id });
			}

			// ═══════════════════════════════════════════════════════════════
			// TIMECARD
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/timecard' && request.method === 'GET') {
				const caseId = url.searchParams.get('case_id');
				const start = url.searchParams.get('start');
				const end = url.searchParams.get('end');
				const month = url.searchParams.get('month');
				let q = 'SELECT te.*, c.case_number, cl.name as client_name FROM time_entries te LEFT JOIN cases c ON te.case_id = c.id LEFT JOIN clients cl ON c.client_id = cl.id WHERE 1=1';
				const p: any[] = [];
				if (caseId) { q += ' AND te.case_id = ?'; p.push(caseId); }
				if (month) {
					const [y, m] = month.split('-').map(Number);
					const nextMonth = m === 12 ? `${y+1}-01-01` : `${y}-${String(m+1).padStart(2,'0')}-01`;
					q += ' AND te.date >= ? AND te.date < ?'; p.push(`${month}-01`, nextMonth);
				}
				if (start) { q += ' AND te.date >= ?'; p.push(start); }
				if (end) { q += ' AND te.date <= ?'; p.push(end); }
				q += ' ORDER BY te.date DESC';
				const { results } = await env.DB.prepare(q).bind(...p).all();
				const totalHours = (results as any[]).reduce((s, e) => s + (e.hours || 0), 0);
				const totalAmount = (results as any[]).reduce((s, e) => s + ((e.hours || 0) * (e.rate || 0)), 0);
				return json({ success: true, entries: results, totals: { hours: totalHours, amount: totalAmount } });
			}
			
			if (path === '/api/timecard' && request.method === 'POST') {
				const { case_id, date, hours, rate, description } = await request.json() as any;
				if (!case_id || !date || !hours || !description) return err('Missing required fields', 400);
				const r = await env.DB.prepare(
					`INSERT INTO time_entries (case_id, date, hours, rate, description) VALUES (?, ?, ?, ?, ?)`
				).bind(case_id, date, hours, rate || 350, description).run();
				return json({ success: true, id: r.meta.last_row_id });
			}

			if (path === '/api/timecard/bulk' && request.method === 'POST') {
				const { entries, rate: defaultRate } = await request.json() as any;
				if (!entries || !Array.isArray(entries) || entries.length === 0) return err('entries array required', 400);

				// Build case_number → case_id lookup
				const { results: allCases } = await env.DB.prepare('SELECT id, case_number FROM cases').all();
				const caseMap: Record<string, number> = {};
				for (const c of allCases as any[]) {
					if (c.case_number) caseMap[c.case_number] = c.id;
				}

				let inserted = 0, skipped = 0;
				const errors: string[] = [];
				const stmts: any[] = [];

				for (const entry of entries) {
					const { case_number, date, hours, rate, description, category, source } = entry;
					if (!date || !hours || !description) { skipped++; errors.push(`Missing fields: ${JSON.stringify(entry)}`); continue; }

					const caseId = case_number ? caseMap[case_number] : null;
					if (!caseId) {
						skipped++;
						if (case_number && case_number !== 'OFFICE-HOURS') errors.push(`Case not found: ${case_number}`);
						continue;
					}

					// Dedup check: skip if (case_id, date, description) already exists
					const existing = await env.DB.prepare(
						'SELECT id FROM time_entries WHERE case_id = ? AND date = ? AND description = ?'
					).bind(caseId, date, description).first();
					if (existing) { skipped++; continue; }

					stmts.push(
						env.DB.prepare('INSERT INTO time_entries (case_id, date, hours, rate, description) VALUES (?, ?, ?, ?, ?)')
							.bind(caseId, date, hours, rate || defaultRate || 350, description)
					);
					inserted++;
				}

				if (stmts.length > 0) {
					// D1 batch — max 100 per batch
					for (let i = 0; i < stmts.length; i += 100) {
						await env.DB.batch(stmts.slice(i, i + 100));
					}
				}

				return json({ success: true, inserted, skipped, errors: errors.slice(0, 20), total: entries.length });
			}

			// ═══════════════════════════════════════════════════════════════
			// AI BRIDGE (Synthia Oracle — Claude as RAID Driver)
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/bridges/message' && request.method === 'POST') {
				const { message, context, sessionId, mode, with_rag, jurisdiction, dashboardState, clientName: reqClientName } = await request.json() as any;
				if (!message) return err('Message required', 400);

				// Determine session role — controls what intel data is injected
				let sessionRole = 'admin'; // default for backward compat (no token = internal call)
				const authHeader = request.headers.get('Authorization');
				if (authHeader?.startsWith('Bearer ')) {
					const sess = await env.SESSIONS.get(authHeader.substring(7));
					if (sess) { sessionRole = (JSON.parse(sess).role || 'admin'); }
				}
				const isInternalUser = ['admin', 'attorney', 'paralegal'].includes(sessionRole);

				// Extract active client from dashboard state or request
				const activeClient = dashboardState?.activeClient || reqClientName || '';

				// --- STEP 1: Cache check (KV first, then D1) ---
				// Skip cache for action/command messages (add, delete, update, complete, refresh) and email-related messages
				const isActionMessage = /\b(add|create|schedule|move|reschedule|change|update|edit|delete|remove|cancel|complete|mark done|refresh calendar|sync calendar)\b/i.test(message) && /\b(hearing|deadline|event|appointment|court date|calendar|meeting|sentencing|pretrial|arraignment|conference|review|motion|plea)\b/i.test(message) || /\b(refresh|sync)\s*(the\s*)?(calendar|deadlines)\b/i.test(message) || /\b(when\s+is|when\s+are|when\s+does|when\s+do|when\s+must|when\s+should|what\s+(?:is|are)\s+the\s+(?:filing\s+)?deadline|calculate\s+(?:the\s+)?deadline|compute\s+(?:the\s+)?deadline|file\s+by\s+when|days?\s+to\s+(?:respond|answer|file|oppose)|how\s+(?:many|long)\s+(?:days?|time))\b/i.test(message);
				const isEmailMessage = /\b(email|inbox|mail|send|reply|respond|forward|archive|correspondence|gmail)\b/i.test(message);
				const isAlertMessage = /\b(hearing|alert|change|schedule|reschedul|cancel|continu|notice|judicialink|court\s*date|docket|calendar)\b/i.test(message);
				const cacheKey = `chat:${hashString(message + (context || ''))}`;
				const kvCached = (!isActionMessage && !isEmailMessage && !isAlertMessage) ? await env.CACHE.get(cacheKey) : null;
				if (kvCached) {
					const cached = JSON.parse(kvCached);
					// Store in chat history even for cached responses
					if (sessionId) {
						ctx.waitUntil((async () => {
							await env.DB.prepare(`INSERT INTO chat_messages (session_id, role, content) VALUES (?, 'user', ?)`).bind(sessionId, message).run();
							await env.DB.prepare(`INSERT INTO chat_messages (session_id, role, content) VALUES (?, 'assistant', ?)`).bind(sessionId, cached.consensus).run();
						})());
					}
					return json({ ...cached, cached: true });
				}

				// --- STEP 1b: Detect intel logging commands (attorney/admin only) ---
				let intelLogResult: any = null;
				const logMatch = isInternalUser ? message.match(/\b(?:log|note|record)\s+(?:that\s+)?(?:judge\s+)(.+?)\s+(?:denied|granted|sustained|overruled|continued|deferred|ruled|decided|sentenced|allowed|rejected|approved|dismissed)\s+(.+)/i) : null;
				const ocLogMatch = !logMatch ? message.match(/\b(?:log|note|record)\s+(?:that\s+)?(?:oc|opposing counsel|counsel|attorney)\s+(.+?)\s+(?:was|is|became|showed|exhibited|filed|offered|responded|agreed|refused|caved|pushed back)\s+(.+)/i) : null;
				if (logMatch) {
					try {
						const judgeName = logMatch[1].trim().replace(/\b\w/g, c => c.toUpperCase());
						const actionDesc = logMatch[0];
						const outcomeMatch = actionDesc.match(/\b(denied|granted|sustained|overruled|continued|deferred|dismissed|rejected|approved|allowed|sentenced|ruled)\b/i);
						const outcome = outcomeMatch ? outcomeMatch[1].toLowerCase() : 'noted';
						let actType = 'hearing_behavior', actSub = '';
						if (/motion\s+to\s+dismiss/i.test(actionDesc)) { actType = 'motion_ruling'; actSub = 'motion_to_dismiss'; }
						else if (/motion\s+to\s+compel/i.test(actionDesc)) { actType = 'motion_ruling'; actSub = 'motion_to_compel'; }
						else if (/motion\s+in\s+limine/i.test(actionDesc)) { actType = 'motion_ruling'; actSub = 'motion_in_limine'; }
						else if (/motion\s+to\s+suppress/i.test(actionDesc)) { actType = 'motion_ruling'; actSub = 'motion_to_suppress'; }
						else if (/motion/i.test(actionDesc)) { actType = 'motion_ruling'; actSub = 'motion_generic'; }
						else if (/plea\s+(?:abeyance|deal|agreement|bargain)/i.test(actionDesc)) { actType = 'plea_decision'; actSub = actionDesc.match(/plea\s+(\w+)/i)?.[1]?.toLowerCase() || ''; }
						else if (/sentence|sentenc/i.test(actionDesc)) { actType = 'sentencing'; }
						else if (/bail|bond/i.test(actionDesc)) { actType = 'bail_decision'; }
						else if (/custod/i.test(actionDesc)) { actType = 'custody_ruling'; }
						else if (/continu/i.test(actionDesc)) { actType = 'continuance'; }
						else if (/objection|evidence|exhibit/i.test(actionDesc)) { actType = 'evidentiary_ruling'; }
						const caseMatch = actionDesc.match(/(?:in|for|on)\s+(?:the\s+)?(\w+)\s+case/i);
						const caseNum = caseMatch?.[1] || '';
						await env.MEMORY_DB.prepare(
							`INSERT INTO judge_activity_log (judge_name, case_number, client_name, activity_type, activity_subtype, outcome, details, our_position, date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
						).bind(judgeName, caseNum, activeClient || '', actType, actSub, outcome, actionDesc, '', mtnToday(), mtnISO()).run();
						intelLogResult = { type: 'judge', name: judgeName, activity_type: actType, activity_subtype: actSub, outcome, case_number: caseNum };
						ctx.waitUntil(ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
							id: `judge_log_${Date.now()}`, type: 'judge_activity', source: 'chat_log',
							content: `[judge_activity] ${judgeName} ${outcome} ${actSub || actType}. ${actionDesc}`,
							clientName: activeClient || '',
						}));
					} catch (jlErr: any) { console.error('Judge log parse error:', jlErr.message); }
				} else if (ocLogMatch) {
					try {
						const counselName = ocLogMatch[1].trim().replace(/\b\w/g, c => c.toUpperCase());
						const actionDesc = ocLogMatch[0];
						const outcomeMatch = actionDesc.match(/\b(evasive|aggressive|cooperative|late|compliant|favorable|unfavorable|neutral|refused|caved|pushed\s+back|agreed|filed|offered)\b/i);
						const outcome = outcomeMatch ? outcomeMatch[1].toLowerCase().replace(/\s+/g, '_') : 'noted';
						let actType = 'communication_style', actSub = '';
						if (/discover/i.test(actionDesc)) { actType = 'discovery_response'; }
						else if (/negoti|settle|offer|deal/i.test(actionDesc)) { actType = 'negotiation'; }
						else if (/motion|filed/i.test(actionDesc)) { actType = 'motion_filed'; }
						else if (/hear/i.test(actionDesc)) { actType = 'hearing_behavior'; }
						else if (/trial/i.test(actionDesc)) { actType = 'trial_tactic'; }
						else if (/deadline|late|comply/i.test(actionDesc)) { actType = 'deadline_compliance'; }
						else if (/ethic/i.test(actionDesc)) { actType = 'ethical_issue'; }
						const caseMatch = actionDesc.match(/(?:in|for|on)\s+(?:the\s+)?(\w+)\s+case/i);
						const caseNum = caseMatch?.[1] || '';
						await env.MEMORY_DB.prepare(
							`INSERT INTO oc_activity_log (counsel_name, case_number, client_name, activity_type, activity_subtype, outcome, details, our_position, date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
						).bind(counselName, caseNum, activeClient || '', actType, actSub, outcome, actionDesc, '', mtnToday(), mtnISO()).run();
						intelLogResult = { type: 'oc', name: counselName, activity_type: actType, outcome, case_number: caseNum };
						ctx.waitUntil(ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
							id: `oc_log_${Date.now()}`, type: 'oc_activity', source: 'chat_log',
							content: `[oc_activity] ${counselName} ${outcome} during ${actType}. ${actionDesc}`,
							clientName: activeClient || '',
						}));
					} catch (olErr: any) { console.error('OC log parse error:', olErr.message); }
				}
				// Attorney self-log detection: "log that I won/lost/missed..." (attorney/admin only)
				if (!logMatch && !ocLogMatch && isInternalUser) {
					const attLogMatch = message.match(/\b(?:log|note|record)\s+(?:that\s+)?(?:I|my|we|our)\s+(.+)/i);
					if (attLogMatch) {
						try {
							const actionDesc = attLogMatch[0];
							const outcomeMatch = actionDesc.match(/\b(won|lost|missed|late|error|mistake|success|effective|ineffective|failed|nailed|crushed|blew|forgot|strong|weak)\b/i);
							const outcome = outcomeMatch ? outcomeMatch[1].toLowerCase() : 'noted';
							let actType = 'strategic_decision', actSub = '';
							if (/motion/i.test(actionDesc)) { actType = 'motion_outcome'; actSub = actionDesc.match(/motion\s+to\s+(\w+)/i)?.[0]?.toLowerCase().replace(/\s+/g, '_') || ''; }
							else if (/hear/i.test(actionDesc)) { actType = 'hearing_performance'; }
							else if (/negoti|plea|deal|settle/i.test(actionDesc)) { actType = 'negotiation'; }
							else if (/trial|cross|direct|exam/i.test(actionDesc)) { actType = 'trial_performance'; }
							else if (/deadline|late|miss|forgot/i.test(actionDesc)) { actType = 'deadline_compliance'; }
							else if (/research|brief|memo/i.test(actionDesc)) { actType = 'research_quality'; }
							else if (/argument|oral/i.test(actionDesc)) { actType = 'argument_effectiveness'; }
							else if (/error|mistake|procedur/i.test(actionDesc)) { actType = 'procedural_error'; }
							else if (/client/i.test(actionDesc)) { actType = 'client_management'; }
							const caseMatch = actionDesc.match(/(?:in|for|on)\s+(?:the\s+)?(\w+)\s+case/i);
							await env.MEMORY_DB.prepare(
								`INSERT INTO attorney_activity_log (attorney_name, role, case_number, client_name, activity_type, activity_subtype, outcome, details, date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
							).bind('JWA3', 'our_firm', caseMatch?.[1] || '', activeClient || '', actType, actSub, outcome, actionDesc, mtnToday(), mtnISO()).run();
							intelLogResult = { type: 'attorney', name: 'JWA3', activity_type: actType, outcome, case_number: caseMatch?.[1] || '' };
							ctx.waitUntil(ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
								id: `attorney_log_${Date.now()}`, type: 'attorney_activity', source: 'chat_log',
								content: `[attorney_activity] JWA3 ${outcome} ${actType}. ${actionDesc}`,
								clientName: activeClient || '',
							}));
						} catch (alErr: any) { console.error('Attorney log parse error:', alErr.message); }
					}
				}

				// Case summary update detection: "update facts on [case]", "the discovery deadline is [date]", "OC's email is [email]"
				if (isInternalUser && !intelLogResult) {
					const csUpdateMatch = message.match(/\b(?:update|set|add|change)\s+(?:the\s+)?(?:facts?|charges?|notes?|discovery\s+deadline|trial\s+date|statute|sol|dispositive\s+deadline)\s+(?:on|for|in)\s+(.+)/i);
					if (csUpdateMatch) {
						// Let AI handle via tool use — just flag in context so Synthia knows to use PATCH
						intelLogResult = { type: 'case_summary_update_hint', hint: csUpdateMatch[0] };
					}
				}

				// --- STEP 2: Load context from MEMORY_DB ---
				let memoryContext = '';
				if (intelLogResult) {
					const logLabel = intelLogResult.type === 'judge' ? 'Judge' : intelLogResult.type === 'oc' ? 'OC' : 'Attorney';
					memoryContext += `\n## ✅ Intel Log Created (just now)\nLogged ${logLabel}: ${intelLogResult.name} | ${intelLogResult.activity_type}${intelLogResult.activity_subtype ? '/' + intelLogResult.activity_subtype : ''} | Outcome: ${intelLogResult.outcome}${intelLogResult.case_number ? ' | Case: ' + intelLogResult.case_number : ''}\nConfirm this to the user and mention how it affects predictions if relevant.${intelLogResult.type === 'attorney' ? ' Ask: "What lesson should we remember for next time?"' : ''}\n`;
				}
				try {
					// Party cache — critical for case work
					const parties = await env.MEMORY_DB.prepare(
						`SELECT client_name, client_role, case_number, opposing_party, opposing_role, judge, court, case_type FROM party_cache ORDER BY last_verified DESC LIMIT 30`
					).all();
					if (parties.results?.length) {
						memoryContext += '\n## Active Cases (party_cache)\n';
						for (const p of parties.results as any[]) {
							memoryContext += `- ${p.client_name} (${p.client_role}) | Case: ${p.case_number} | vs ${p.opposing_party} (${p.opposing_role}) | Judge: ${p.judge} | ${p.court} | ${p.case_type}\n`;
						}
					}

					// Upcoming deadlines
					const deadlines = await env.MEMORY_DB.prepare(
						`SELECT client_name, case_number, deadline_type, description, due_date, court FROM deadlines WHERE status IN ('active', 'pending') AND due_date >= ? ORDER BY due_date ASC LIMIT 15`
					).bind(mtnToday()).all();
					if (deadlines.results?.length) {
						memoryContext += '\n## Upcoming Deadlines\n';
						for (const d of deadlines.results as any[]) {
							memoryContext += `- ${d.due_date}: ${d.deadline_type} — ${d.description} (${d.client_name}, ${d.case_number}, ${d.court})\n`;
						}
					}

					// Schedule change alerts (from pending_tasks — same data the dashboard shows)
					const alerts = await env.MEMORY_DB.prepare(
						`SELECT id, task_type, description, created_at FROM pending_tasks WHERE task_type IN ('schedule_change', 'new_hearings') ORDER BY created_at DESC LIMIT 35`
					).all();
					if (alerts.results?.length) {
						memoryContext += `\n## ⚠️ Schedule Change Alerts (${alerts.results.length} — these are the alerts shown on the dashboard UI)\n`;
						memoryContext += `These are REAL hearing schedule changes detected by the case monitor. When the user asks about "alerts", "changes", "what changed", "hearing changes" — THIS is what they mean.\n`;
						for (const a of alerts.results as any[]) {
							memoryContext += `- [${(a.created_at as string || '').substring(0, 10)}] ${a.description}\n`;
						}
					}

					// Court contacts
					const contacts = await env.MEMORY_DB.prepare(
						`SELECT name, role, email, phone FROM court_contacts LIMIT 10`
					).all();
					if (contacts.results?.length) {
						memoryContext += '\n## Court Contacts\n';
						for (const c of contacts.results as any[]) {
							memoryContext += `- ${c.name} — ${c.role} — ${c.email || ''} ${c.phone || ''}\n`;
						}
					}

					// === INTEL SECTIONS — attorney/admin only, NOT for client portal ===
					if (isInternalUser) {
					// Judge intelligence — strategic profiles for case prep
					const judges = await env.MEMORY_DB.prepare(
						`SELECT judge_name, court, district, tendencies, sentencing_patterns, motion_preferences, plea_disposition, notes, cases_before, win_rate, last_appearance, ja_name, ja_email, ja_phone FROM judge_intel`
					).all();
					if (judges.results?.length) {
						memoryContext += '\n## 🧑‍⚖️ Judge Intelligence\n';
						for (const j of judges.results as any[]) {
							memoryContext += `### ${j.judge_name} — ${j.court}${j.district ? ', ' + j.district : ''}\n`;
							if (j.tendencies) memoryContext += `  Tendencies: ${j.tendencies}\n`;
							if (j.sentencing_patterns) memoryContext += `  Sentencing: ${j.sentencing_patterns}\n`;
							if (j.motion_preferences) memoryContext += `  Motions: ${j.motion_preferences}\n`;
							if (j.plea_disposition) memoryContext += `  Plea Deals: ${j.plea_disposition}\n`;
							if (j.win_rate) memoryContext += `  Win Rate: ${j.win_rate}\n`;
							if (j.cases_before) memoryContext += `  Cases Before: ${j.cases_before}\n`;
							if (j.notes) memoryContext += `  Notes: ${j.notes}\n`;
							if (j.ja_name) memoryContext += `  JA: ${j.ja_name}${j.ja_email ? ' (' + j.ja_email + ')' : ''}${j.ja_phone ? ' ' + j.ja_phone : ''}\n`;
						}
					}

					// Opposing counsel intelligence — litigation strategy
					const counsel = await env.MEMORY_DB.prepare(
						`SELECT counsel_name, firm, phone, email, bar_number, practice_areas, negotiation_style, litigation_tendencies, cases_against, outcomes, win_rate_against, strengths, weaknesses, notes, last_case, last_date FROM opposing_counsel_intel`
					).all();
					if (counsel.results?.length) {
						memoryContext += '\n## ⚔️ Opposing Counsel Intelligence\n';
						for (const oc of counsel.results as any[]) {
							memoryContext += `### ${oc.counsel_name}${oc.firm ? ' — ' + oc.firm : ''}${oc.bar_number ? ' (Bar #' + oc.bar_number + ')' : ''}\n`;
							if (oc.email) memoryContext += `  Contact: ${oc.email}${oc.phone ? ', ' + oc.phone : ''}\n`;
							if (oc.practice_areas) memoryContext += `  Practice: ${oc.practice_areas}\n`;
							if (oc.negotiation_style) memoryContext += `  Negotiation: ${oc.negotiation_style}\n`;
							if (oc.litigation_tendencies) memoryContext += `  Litigation: ${oc.litigation_tendencies}\n`;
							if (oc.strengths) memoryContext += `  Strengths: ${oc.strengths}\n`;
							if (oc.weaknesses) memoryContext += `  Weaknesses: ${oc.weaknesses}\n`;
							if (oc.cases_against) memoryContext += `  Cases Against: ${oc.cases_against}\n`;
							if (oc.outcomes) memoryContext += `  Outcomes: ${oc.outcomes}\n`;
							if (oc.win_rate_against) memoryContext += `  Win Rate Against: ${oc.win_rate_against}\n`;
							if (oc.notes) memoryContext += `  Notes: ${oc.notes}\n`;
						}
					}

					// Predictive Intelligence — computed from activity logs
					try {
						const judgeLogs = await env.MEMORY_DB.prepare(
							`SELECT judge_name, activity_type, activity_subtype, outcome, party_role, date FROM judge_activity_log ORDER BY date DESC`
						).all();
						if (judgeLogs.results?.length) {
							const byJudge: Record<string, any[]> = {};
							for (const l of judgeLogs.results as any[]) {
								(byJudge[l.judge_name] ||= []).push(l);
							}
							let predSection = '';
							for (const [name, logs] of Object.entries(byJudge)) {
								const preds = computePredictions(logs);
								if (preds.length) {
									predSection += `  **${name}** PREDICTIONS (from ${logs.length} logged events):\n`;
									for (const p of preds.slice(0, 5)) {
										predSection += `    → ${p.summary}\n`;
										if (p.by_role) {
											for (const [role, rd] of Object.entries(p.by_role)) {
												predSection += `      ↳ ${rd.summary}\n`;
											}
										}
									}
								}
							}
							if (predSection) {
								memoryContext += '\n## 📊 Judge Predictive Analytics\n' + predSection;
							}
						}

						const ocLogs = await env.MEMORY_DB.prepare(
							`SELECT counsel_name, activity_type, activity_subtype, outcome, party_role, date FROM oc_activity_log ORDER BY date DESC`
						).all();
						if (ocLogs.results?.length) {
							const byOC: Record<string, any[]> = {};
							for (const l of ocLogs.results as any[]) {
								(byOC[l.counsel_name] ||= []).push(l);
							}
							let predSection = '';
							for (const [name, logs] of Object.entries(byOC)) {
								const preds = computePredictions(logs);
								if (preds.length) {
									predSection += `  **${name}** PREDICTIONS (from ${logs.length} logged interactions):\n`;
									for (const p of preds.slice(0, 5)) {
										predSection += `    → ${p.summary}\n`;
										if (p.by_role) {
											for (const [role, rd] of Object.entries(p.by_role)) {
												predSection += `      ↳ ${rd.summary}\n`;
											}
										}
									}
								}
							}
							if (predSection) {
								memoryContext += '\n## 📊 OC Predictive Analytics\n' + predSection;
							}
						}
					} catch (predErr: any) {
						console.error('Prediction context error (non-fatal):', predErr.message);
					}

					// Attorney Performance Analytics (including JWA3)
					try {
						const attLogs = await env.MEMORY_DB.prepare(
							`SELECT attorney_name, activity_type, activity_subtype, outcome, party_role, date, lesson_learned FROM attorney_activity_log ORDER BY date DESC`
						).all();
						if (attLogs.results?.length) {
							const byAtt: Record<string, any[]> = {};
							for (const l of attLogs.results as any[]) { (byAtt[l.attorney_name] ||= []).push(l); }
							let attSection = '';
							for (const [name, logs] of Object.entries(byAtt)) {
								const preds = computePredictions(logs);
								const lessons = logs.filter(l => l.lesson_learned).slice(0, 5);
								if (preds.length || lessons.length) {
									attSection += `  **${name}** (${logs.length} logged activities):\n`;
									for (const p of preds.slice(0, 4)) {
										attSection += `    → ${p.summary}\n`;
										if (p.by_role) {
											for (const [role, rd] of Object.entries(p.by_role)) {
												attSection += `      ↳ ${rd.summary}\n`;
											}
										}
									}
									for (const l of lessons) { attSection += `    💡 LESSON (${l.date}): ${l.lesson_learned}\n`; }
								}
							}
							if (attSection) { memoryContext += '\n## 📊 Attorney Performance Analytics\n' + attSection; }
						}
					} catch (_) {}

					// Judge Ruling Rationale — WHY judges rule the way they do (pro/con analysis)
					try {
						const rationale = await env.MEMORY_DB.prepare(
							`SELECT judge_name, activity_type, activity_subtype, actual_outcome, typical_outcome, ruling_reasoning, reversal_factors, specific_arguments, applicability_notes, is_reversal FROM judge_ruling_rationale ORDER BY date DESC LIMIT 50`
						).all();
						if (rationale.results?.length) {
							const byJudge: Record<string, any[]> = {};
							for (const r of rationale.results as any[]) { (byJudge[r.judge_name] ||= []).push(r); }
							let ratSection = '';
							for (const [name, entries] of Object.entries(byJudge)) {
								const byType: Record<string, any[]> = {};
								for (const e of entries) {
									const key = `${e.activity_type}${e.activity_subtype ? '/' + e.activity_subtype : ''}`;
									(byType[key] ||= []).push(e);
								}
								ratSection += `  **${name}** — Ruling Rationale:\n`;
								for (const [type, typeEntries] of Object.entries(byType)) {
									const pros = typeEntries.filter(e => ['granted','sustained','approved','allowed'].includes(e.actual_outcome));
									const cons = typeEntries.filter(e => ['denied','overruled','rejected','dismissed'].includes(e.actual_outcome));
									ratSection += `    ${type}:\n`;
									if (pros.length) { ratSection += `      ✅ GRANTS/SUSTAINS WHEN: ${pros.map(p => p.ruling_reasoning || p.specific_arguments || 'not recorded').join('; ')}\n`; }
									if (cons.length) { ratSection += `      ❌ DENIES/OVERRULES WHEN: ${cons.map(p => p.ruling_reasoning || p.specific_arguments || 'not recorded').join('; ')}\n`; }
									const reversals = typeEntries.filter(e => e.is_reversal);
									if (reversals.length) { ratSection += `      ⚠️ WENT AGAINST TYPE (${reversals.length}x): ${reversals.map(r => r.specific_arguments || r.ruling_reasoning || 'factors not recorded').join('; ')}\n`; }
									if (typeEntries.some(e => e.applicability_notes)) { ratSection += `      📝 HOW TO APPLY: ${typeEntries.filter(e => e.applicability_notes).map(e => e.applicability_notes).join('; ')}\n`; }
								}
							}
							if (ratSection) { memoryContext += '\n## 🧠 Judge Ruling Rationale (Pro/Con)\n' + ratSection; }
						}
					} catch (_) {}

					// Judicial Thinking Resources
					try {
						const resources = await env.MEMORY_DB.prepare(
							`SELECT title, resource_type, jurisdiction, topic, summary, source FROM judicial_resources ORDER BY last_updated DESC LIMIT 15`
						).all();
						if (resources.results?.length) {
							memoryContext += `\n## 📚 Judicial Thinking Resources (${resources.results.length})\n`;
							for (const r of resources.results as any[]) {
								memoryContext += `- **${r.title}** (${r.resource_type}, ${r.jurisdiction}): ${(r.summary || '').substring(0, 200)}${r.source ? ' [' + r.source + ']' : ''}\n`;
							}
						}
					} catch (_) {}
					} // end isInternalUser intel gate

					// Case summaries — consolidated case intelligence (enriched with intel, facts, OC, judge data)
					const caseSummaries = await env.MEMORY_DB.prepare(
						`SELECT * FROM case_summaries WHERE status = 'active' ORDER BY next_event_date ASC`
					).all();
					if (caseSummaries.results?.length) {
						memoryContext += `\n## 📋 Case Summaries (${caseSummaries.results.length} active)\n`;
						for (const cs of caseSummaries.results as any[]) {
							memoryContext += `- **${cs.client_name}** (${cs.case_number}) — ${cs.case_type}, ${cs.court}\n`;
							memoryContext += `  Role: ${cs.client_role} vs ${cs.opposing_party} | Judge: ${cs.judge}\n`;
							if (cs.opposing_counsel) memoryContext += `  OC: ${cs.opposing_counsel}`;
							if (cs.opposing_counsel_firm) memoryContext += ` (${cs.opposing_counsel_firm})`;
							if (cs.opposing_counsel_phone || cs.opposing_counsel_email) memoryContext += ` — ${cs.opposing_counsel_phone || ''} ${cs.opposing_counsel_email || ''}`;
							if (cs.opposing_counsel) memoryContext += '\n';
							if (cs.client_phone || cs.client_email) memoryContext += `  Client Contact: ${cs.client_phone || ''} ${cs.client_email || ''}${cs.client_address ? ' | ' + cs.client_address : ''}\n`;
							if (cs.facts) memoryContext += `  Facts: ${(cs.facts as string).substring(0, 500)}\n`;
							if (cs.charges) memoryContext += `  Charges: ${cs.charges}\n`;
							if (cs.additional_parties) memoryContext += `  Additional Parties: ${cs.additional_parties}\n`;
							// Key deadlines
							const dlParts: string[] = [];
							if (cs.discovery_deadline) dlParts.push(`Discovery: ${cs.discovery_deadline}`);
							if (cs.dispositive_deadline) dlParts.push(`Dispositive: ${cs.dispositive_deadline}`);
							if (cs.trial_date) dlParts.push(`Trial: ${cs.trial_date}`);
							if (cs.statute_of_limitations) dlParts.push(`SOL: ${cs.statute_of_limitations}`);
							if (dlParts.length > 0) memoryContext += `  Key Deadlines: ${dlParts.join(' | ')}\n`;
							if (cs.next_event) memoryContext += `  Next: ${cs.next_event} on ${cs.next_event_date}\n`;
							if (cs.judge_prediction) memoryContext += `  🔮 Judge Intel: ${(cs.judge_prediction as string).substring(0, 300)}\n`;
							if (cs.oc_prediction) memoryContext += `  🔮 OC Intel: ${(cs.oc_prediction as string).substring(0, 300)}\n`;
							if (cs.reversal_factors) memoryContext += `  ⚖️ Reversal Factors: ${(cs.reversal_factors as string).substring(0, 300)}\n`;
							if (cs.notes) memoryContext += `  Notes: ${(cs.notes as string).substring(0, 300)}\n`;
							if (cs.file_count) memoryContext += `  Files: ${cs.file_count}\n`;
						}
					}

					// Case files — document inventory per client
					try {
						const fileCounts = await env.MEMORY_DB.prepare(
							`SELECT client_name, COUNT(*) as total_files, GROUP_CONCAT(DISTINCT file_type) as file_types FROM case_files GROUP BY client_name ORDER BY client_name LIMIT 50`
						).all();
						if (fileCounts.results?.length) {
							memoryContext += `\n## 📁 Case File Inventory\n`;
							for (const f of fileCounts.results as any[]) {
								memoryContext += `- ${f.client_name}: ${f.total_files} files (${f.file_types || 'various'})\n`;
							}
						}
					} catch (_) { /* table may not exist */ }

					// Timecards — recent work summary
					try {
						// Compute 7 days ago in Mountain Time
						const sevenDaysAgo = new Date(mtnNow());
						sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
						const sevenDaysAgoStr = sevenDaysAgo.toISOString().split('T')[0];
						const recentTime = await env.MEMORY_DB.prepare(
							`SELECT date, client_name, hours, description FROM timecards WHERE date >= ? ORDER BY date DESC LIMIT 20`
						).bind(sevenDaysAgoStr).all();
						if (recentTime.results?.length) {
							const totalHrs = (recentTime.results as any[]).reduce((sum: number, t: any) => sum + (parseFloat(t.hours) || 0), 0);
							memoryContext += `\n## ⏱️ Recent Timecards (last 7 days — ${totalHrs.toFixed(1)}h total)\n`;
							for (const t of recentTime.results as any[]) {
								memoryContext += `- ${t.date}: ${t.hours}h — ${t.client_name || 'General'} — ${(t.description || '').substring(0, 100)}\n`;
							}
						}
					} catch (_) { /* table may not exist */ }

					// Compressed memory summaries (older conversation blocks)
					const summaries = await env.MEMORY_DB.prepare(
						`SELECT summary, started_at FROM sessions WHERE id LIKE 'summary_%' ORDER BY started_at DESC LIMIT 12`
					).all();
					if (summaries.results?.length) {
						memoryContext += '\n## Conversation History (compressed summaries, oldest first)\n';
						const sorted = (summaries.results as any[]).reverse();
						for (const s of sorted) {
							memoryContext += `[${s.started_at}]: ${(s.summary || '').substring(0, 1200)}\n---\n`;
						}
					}
					// NOTE: Recent conversation is NOT included here — it goes into Claude's messages array
					// as proper multi-turn history (conversationTurns), which is far more effective than flat text.
				} catch (memErr: any) {
					console.error('Memory context error:', memErr.message);
				}

				// --- STEP 2-RAG: Semantic memory retrieval ---
				let ragContext = '';
				try {
					const matches = await ragSearch(env.AI, env.MEMORY_INDEX, message, 8);
					if (matches.length > 0) {
						const chunks: string[] = [];
						for (const m of matches) {
							if ((m.score || 0) < 0.60) continue; // Lowered threshold for better recall
							const row = await env.DB.prepare('SELECT content, chunk_type, source FROM memory_chunks WHERE id = ?').bind(m.id).first() as any;
							if (row) {
								// High-relevance chunks get full content, lower ones get truncated
								const maxLen = (m.score || 0) >= 0.80 ? 1500 : (m.score || 0) >= 0.70 ? 1000 : 600;
								chunks.push(`[${row.chunk_type}/${row.source}, relevance: ${(m.score || 0).toFixed(2)}]: ${row.content.substring(0, maxLen)}`);
							}
						}
						if (chunks.length > 0) {
							ragContext = '\n## Retrieved Memories (RAG — semantically matched to current query)\n' + chunks.join('\n---\n');
						}
					}
					if (ragContext) console.log(`RAG: ${matches.filter(m => (m.score || 0) >= 0.65).length} relevant chunks retrieved`);
				} catch (ragErr: any) {
					console.error('RAG search error:', ragErr.message);
				}

				// --- STEP 2-EMAIL: Fetch recent emails for active client and inject into context ---
				let emailContextStr = '';
				let emailAction: string | null = null;
				if (activeClient) {
					try {
						const graphToken = await getGraphToken();
						if (graphToken) {
							// Search emails by client's last name
							const nameParts = activeClient.split(/\s+/).filter((p: string) => p.length > 2);
							const searchTerm = nameParts[nameParts.length - 1] || activeClient;
							const graphUrl = `https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$top=5&$search="${encodeURIComponent(searchTerm)}"&$select=subject,from,toRecipients,receivedDateTime,bodyPreview,id,hasAttachments,conversationId`;
							const emailRes = await fetch(graphUrl, { headers: { 'Authorization': `Bearer ${graphToken}` } });
							const emailData = await emailRes.json() as any;

							if (emailData.value?.length > 0) {
								emailContextStr = `\n## 📧 Recent Emails for ${activeClient.toUpperCase()} (${emailData.value.length})\n`;
								for (let i = 0; i < emailData.value.length; i++) {
									const e = emailData.value[i];
									const fromEmail = e.from?.emailAddress?.address?.toLowerCase() || '';
									const fromName = e.from?.emailAddress?.name || fromEmail;
									// Quick identity resolution
									let who = fromName;
									if (fromEmail.endsWith('@utcourts.gov')) {
										const cc = await env.MEMORY_DB.prepare('SELECT name, title FROM court_contacts WHERE LOWER(email) = ?').bind(fromEmail).first() as any;
										if (cc) who = `${cc.name} (${cc.title})`;
										else who = `${fromName} (Utah Courts)`;
									} else if (fromEmail === 'pd@dianepitcher.com' || fromEmail === 'diane@dianepitcher.com' || fromEmail === 'esqslaw@gmail.com') {
										who = `${fromName} (Our Firm)`;
									} else {
										const oc = await env.MEMORY_DB.prepare('SELECT counsel_name, firm FROM opposing_counsel_intel WHERE LOWER(email) = ?').bind(fromEmail).first() as any;
										if (oc) who = `${oc.counsel_name} (Opposing Counsel, ${oc.firm})`;
									}
									emailContextStr += `  ${i + 1}. ${(e.receivedDateTime || '').substring(0, 10)} | FROM: ${who} | SUBJECT: ${e.subject || '(no subject)'}\n     Preview: ${(e.bodyPreview || '').substring(0, 150)}\n     [email_id: ${e.id?.substring(0, 30)}...]\n`;
								}
								emailContextStr += `When user says "reply to that email", "email them back", etc., use the most relevant email above. You have full email send capability.\n`;
								memoryContext += emailContextStr;
							}
						}
					} catch (emailCtxErr: any) {
						console.warn('Email context fetch error (non-fatal):', emailCtxErr.message);
					}

					// Also search Gmail (esqslaw@gmail.com) for client emails
					try {
						const gmailToken = await getGmailToken();
						if (gmailToken) {
							const nameParts = activeClient.split(/\s+/).filter((p: string) => p.length > 2);
							const searchTerm = nameParts[nameParts.length - 1] || activeClient;
							const gmailUrl = `https://www.googleapis.com/gmail/v1/users/me/messages?maxResults=5&q=${encodeURIComponent(searchTerm)}`;
							const gmailRes = await fetch(gmailUrl, { headers: { 'Authorization': `Bearer ${gmailToken}` } });
							const gmailData = await gmailRes.json() as any;
							if (gmailData.messages?.length > 0) {
								let gmailCtx = `\n## 📧 Gmail (esqslaw@gmail.com) — Recent for ${activeClient.toUpperCase()} (${gmailData.messages.length})\n`;
								for (let i = 0; i < Math.min(gmailData.messages.length, 5); i++) {
									const msgRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${gmailData.messages[i].id}?format=metadata&metadataHeaders=Subject&metadataHeaders=From&metadataHeaders=Date`, { headers: { 'Authorization': `Bearer ${gmailToken}` } });
									const msg = await msgRes.json() as any;
									const headers = msg.payload?.headers || [];
									const subject = headers.find((h: any) => h.name === 'Subject')?.value || '(no subject)';
									const from = headers.find((h: any) => h.name === 'From')?.value || 'Unknown';
									const date = headers.find((h: any) => h.name === 'Date')?.value || '';
									gmailCtx += `  ${i + 1}. ${date.substring(0, 16)} | FROM: ${from} | SUBJECT: ${subject}\n     [gmail_id: ${gmailData.messages[i].id}]\n`;
								}
								memoryContext += gmailCtx;
							}
						}
					} catch (gmailCtxErr: any) {
						console.warn('Gmail context fetch error (non-fatal):', gmailCtxErr.message);
					}
				}

				// --- STEP 2-EMAIL-GMAIL: Fetch recent Gmail when user asks about email (even without activeClient) ---
				if (isEmailMessage && !activeClient) {
					try {
						const gmailToken = await getGmailToken();
						if (gmailToken) {
							// Extract search term from message (e.g. "Diane's email" → "Diane", "employment contract" → "employment contract")
							let gmailQuery = 'newer_than:7d';
							const nameInMsg = message.match(/(?:from|diane|pitcher|employment|contract|email\s+(?:from|about))\s+(\w+)/i);
							if (nameInMsg) gmailQuery = nameInMsg[0];
							else if (/diane/i.test(message)) gmailQuery = 'from:diane';
							else if (/employment|contract/i.test(message)) gmailQuery = 'subject:(employment OR contract)';

							const gmailUrl = `https://www.googleapis.com/gmail/v1/users/me/messages?maxResults=8&q=${encodeURIComponent(gmailQuery)}`;
							const gmailRes = await fetch(gmailUrl, { headers: { 'Authorization': `Bearer ${gmailToken}` } });
							const gmailData = await gmailRes.json() as any;
							if (gmailData.messages?.length > 0) {
								let gmailCtx = `\n## 📧 Gmail Inbox (esqslaw@gmail.com) — ${gmailData.messages.length} matching emails\n`;
								for (let i = 0; i < Math.min(gmailData.messages.length, 8); i++) {
									const msgRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${gmailData.messages[i].id}?format=full`, { headers: { 'Authorization': `Bearer ${gmailToken}` } });
									const msg = await msgRes.json() as any;
									const headers = msg.payload?.headers || [];
									const subject = headers.find((h: any) => h.name === 'Subject')?.value || '(no subject)';
									const from = headers.find((h: any) => h.name === 'From')?.value || 'Unknown';
									const date = headers.find((h: any) => h.name === 'Date')?.value || '';
									// Extract body text (plain text part)
									let bodyText = '';
									if (msg.payload?.parts) {
										const textPart = msg.payload.parts.find((p: any) => p.mimeType === 'text/plain');
										if (textPart?.body?.data) {
											bodyText = atob(textPart.body.data.replace(/-/g, '+').replace(/_/g, '/'));
										}
									} else if (msg.payload?.body?.data) {
										bodyText = atob(msg.payload.body.data.replace(/-/g, '+').replace(/_/g, '/'));
									}
									gmailCtx += `  ${i + 1}. ${date.substring(0, 24)} | FROM: ${from} | SUBJECT: ${subject}\n`;
									if (bodyText) gmailCtx += `     Body: ${bodyText.substring(0, 400)}\n`;
									gmailCtx += `     [gmail_id: ${gmailData.messages[i].id}]\n`;
								}
								memoryContext += gmailCtx;
							}
						}
					} catch (gmailReadErr: any) {
						console.warn('Gmail read error (non-fatal):', gmailReadErr.message);
					}
				}

				// --- STEP 2-ALERTS: Fetch JudiciaLink / court alert emails (independent of active client) ---
				// JudiciaLink alerts come from support@judicialink.com and contain hearing changes for ALL cases
				// Court notices come from @utcourts.gov — these are critical and should always be in context
				if (isAlertMessage || isEmailMessage) {
					try {
						const alertGraphToken = await getGraphToken();
						if (alertGraphToken) {
							// Fetch recent JudiciaLink alerts (all types) — include body for event extraction
							const jlUrl = `https://graph.microsoft.com/v1.0/me/messages?$top=20&$search="from:judicialink.com"&$select=subject,receivedDateTime,body,id,hasAttachments,from`;
							const jlRes = await fetch(jlUrl, { headers: { 'Authorization': `Bearer ${alertGraphToken}` } });
							const jlData = await jlRes.json() as any;

							// Fetch hearing-specific emails (schedule changes, continuances, etc.)
							const hearingUrl = `https://graph.microsoft.com/v1.0/me/messages?$top=15&$search="subject:hearing OR subject:continued OR subject:reschedule OR subject:schedule change OR subject:cancelled OR subject:vacated"&$select=subject,from,receivedDateTime,bodyPreview,id`;
							const hearingRes = await fetch(hearingUrl, { headers: { 'Authorization': `Bearer ${alertGraphToken}` } });
							const hearingData = await hearingRes.json() as any;

							// Fetch recent @utcourts.gov emails
							const courtUrl = `https://graph.microsoft.com/v1.0/me/messages?$top=10&$search="from:utcourts.gov"&$select=subject,from,receivedDateTime,bodyPreview,id`;
							const courtRes = await fetch(courtUrl, { headers: { 'Authorization': `Bearer ${alertGraphToken}` } });
							const courtData = await courtRes.json() as any;

							let alertStr = '';
							const seenIds = new Set<string>();
							console.log(`[ALERTS] JudiciaLink: ${jlData.value?.length || 0}, Hearing: ${hearingData.value?.length || 0}, Court: ${courtData.value?.length || 0}${jlData.error ? ', JL error: ' + JSON.stringify(jlData.error) : ''}${courtData.error ? ', Court error: ' + JSON.stringify(courtData.error) : ''}${hearingData.error ? ', Hearing error: ' + JSON.stringify(hearingData.error) : ''}`);

							if (jlData.value?.length > 0) {
								alertStr += `\n## ⚠️ JudiciaLink Court Notifications (${jlData.value.length} recent)\n`;
								alertStr += `These contain hearing settings, schedule changes, document filings, and case activity. When user asks about hearings or changes, reference the Event(s) and Document(s) below.\n`;
								for (let i = 0; i < jlData.value.length; i++) {
									const a = jlData.value[i];
									seenIds.add(a.id);
									// Extract case info from body — strip HTML, get the key sections
									const bodyText = (a.body?.content || '').replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
									// Extract case number
									const caseMatch = bodyText.match(/RE:\s*(\d{9,12})/);
									const caseNum = caseMatch ? caseMatch[1] : '';
									// Extract events (hearings set/changed)
									const eventMatch = bodyText.match(/Event\(s\):\s*(.*?)(?=Document\(s\)|This notice|$)/i);
									const events = eventMatch ? eventMatch[1].trim().substring(0, 300) : '';
									// Extract documents filed
									const docMatch = bodyText.match(/Document\(s\)\s*(?:Filed|Submitted):\s*(.*?)(?=(?:Judge:|This notice|The following))/i);
									const docs = docMatch ? docMatch[1].trim().substring(0, 200) : '';
									// Extract case title from subject
									const titleMatch = (a.subject || '').match(/--\s*(.+?)(?:,\s*\d|$)/);
									const caseTitle = titleMatch ? titleMatch[1].trim() : '';

									alertStr += `  ${i + 1}. ${(a.receivedDateTime || '').substring(0, 10)} | ${caseTitle || a.subject || '(no subject)'}${caseNum ? ' [' + caseNum + ']' : ''}\n`;
									if (events) alertStr += `     📅 EVENTS: ${events}\n`;
									if (docs) alertStr += `     📄 FILED: ${docs}\n`;
									if (!events && !docs) {
										// Fallback: show body excerpt
										const excerpt = bodyText.substring(bodyText.indexOf('NOTICE'), bodyText.indexOf('This notice')).substring(0, 300) || bodyText.substring(0, 300);
										alertStr += `     ${excerpt}\n`;
									}
								}
							}

							// Hearing-specific emails (may include non-JudiciaLink court emails about hearings)
							if (hearingData.value?.length > 0) {
								const newHearingEmails = hearingData.value.filter((h: any) => !seenIds.has(h.id));
								if (newHearingEmails.length > 0) {
									alertStr += `\n## 📅 Hearing-Related Emails (${newHearingEmails.length} additional)\n`;
									for (let i = 0; i < newHearingEmails.length; i++) {
										const h = newHearingEmails[i];
										const fromName = h.from?.emailAddress?.name || h.from?.emailAddress?.address || 'Unknown';
										alertStr += `  ${i + 1}. ${(h.receivedDateTime || '').substring(0, 16)} | FROM: ${fromName} | ${h.subject || '(no subject)'}\n     ${(h.bodyPreview || '').substring(0, 250)}\n`;
									}
								}
							}

							if (courtData.value?.length > 0) {
								const newCourtEmails = courtData.value.filter((c: any) => !seenIds.has(c.id));
								if (newCourtEmails.length > 0) {
									alertStr += `\n## 🏛️ Utah Courts Emails (${newCourtEmails.length} recent)\n`;
									for (let i = 0; i < newCourtEmails.length; i++) {
										const c = newCourtEmails[i];
										const fromName = c.from?.emailAddress?.name || c.from?.emailAddress?.address || 'Court';
										alertStr += `  ${i + 1}. ${(c.receivedDateTime || '').substring(0, 16)} | FROM: ${fromName} | ${c.subject || '(no subject)'}\n     ${(c.bodyPreview || '').substring(0, 250)}\n`;
									}
								}
							}

							if (alertStr) {
								memoryContext += alertStr;
							}
						}
					} catch (alertErr: any) {
						console.warn('Alert email fetch error (non-fatal):', alertErr.message);
					}
				}

				// --- Email action intent detection ---
				if (message) {
					if (/\b(send|email|write|draft|reply|respond|forward)\b.*\b(email|message|reply|response|them|back|her|him|court|client|counsel)\b/i.test(message) ||
						(/\b(write\s+to)\b/i.test(message) && /\b(them|court|clerk|judge|counsel|opposing|client)\b/i.test(message))) {
						emailAction = 'compose';
					}
					if (/\b(archive|save|file|store|pdf)\b.*\b(email|emails|correspondence|communications)\b/i.test(message) ||
						/\b(email|emails)\b.*\b(to pdf|as pdf|archive|file|save)\b/i.test(message)) {
						emailAction = 'archive';
					}
					if (/\b(check|show|read|pull up|open|get|what)\b.*\b(email|emails|inbox|mail|messages)\b/i.test(message) ||
						/\b(email|emails|inbox|mail)\b.*\b(from|about|regarding)\b/i.test(message)) {
						emailAction = 'read';
					}
				}

				// --- STEP 2a: URCP/URCrimP/URAP Rule 6 Deadline Computation Engine ---
				// Strict compliance: zero deviation from Utah Rules of Procedure
				// Case type dictates which rules apply — no exceptions
				async function computeRule6Date(
					triggerDate: string,
					days: number,
					direction: 'after' | 'before',
					serviceType: string,
					mailAddDays: number,
					envRef: Env
				): Promise<{ date: string; extended: boolean; extendedFrom: string; reason: string }> {
					// Rule 6(a): Computation of time
					// (1) Exclude the day of the event that triggers the period
					// (2) Count every day, including intermediate Saturdays, Sundays, and legal holidays
					// (3) Include the last day of the period — BUT if last day is Saturday, Sunday, or legal holiday,
					//     the period runs until the end of the next day that is not a Saturday, Sunday, or legal holiday
					// (4) If served by mail: +7 calendar days (Rule 6(d))

					let totalDays = days;
					// Add mail service days if applicable (Rule 6(d))
					if (serviceType === 'mail' && mailAddDays > 0) {
						totalDays += mailAddDays;
					}

					const trigger = new Date(triggerDate + 'T12:00:00Z');
					let target: Date;

					if (direction === 'after') {
						// Exclude trigger day, start counting from next day
						target = new Date(trigger);
						target.setUTCDate(target.getUTCDate() + totalDays);
					} else {
						// "before" — count backwards
						target = new Date(trigger);
						target.setUTCDate(target.getUTCDate() - totalDays);
					}

					const originalDate = target.toISOString().split('T')[0];
					let extended = false;
					let reason = '';

					// Rule 6(a)(3): If last day falls on Sat/Sun/holiday, extend to next business day
					let maxExtensions = 14; // safety limit
					while (maxExtensions > 0) {
						const dow = target.getUTCDay();
						if (dow === 0 || dow === 6) {
							extended = true;
							reason = dow === 0 ? 'Sunday' : 'Saturday';
							if (direction === 'after') {
								target.setUTCDate(target.getUTCDate() + 1);
							} else {
								target.setUTCDate(target.getUTCDate() - 1);
							}
							maxExtensions--;
							continue;
						}
						// Check court holidays
						const checkDate = target.toISOString().split('T')[0];
						try {
							const holiday = await envRef.MEMORY_DB.prepare(
								`SELECT holiday_name FROM court_holidays WHERE holiday_date = ?`
							).bind(checkDate).first() as any;
							if (holiday) {
								extended = true;
								reason = holiday.holiday_name;
								if (direction === 'after') {
									target.setUTCDate(target.getUTCDate() + 1);
								} else {
									target.setUTCDate(target.getUTCDate() - 1);
								}
								maxExtensions--;
								continue;
							}
						} catch (e) { /* non-fatal */ }
						break; // Not a weekend or holiday — done
					}

					return {
						date: target.toISOString().split('T')[0],
						extended,
						extendedFrom: extended ? originalDate : '',
						reason: extended ? `Extended past ${reason} per URCP Rule 6(a)` : ''
					};
				}

				// Resolve case type → procedure rules (no exceptions, no mixing)
				async function getCaseRules(caseType: string, envRef: Env): Promise<string> {
					try {
						const mapping = await envRef.MEMORY_DB.prepare(
							`SELECT procedure_rules FROM case_type_map WHERE case_type_input = ?`
						).bind(caseType.toLowerCase()).first() as any;
						if (mapping) return mapping.procedure_rules;
					} catch (e) { /* fallback */ }
					// Fallback inference
					if (/criminal|felony|misdemeanor|dui/i.test(caseType)) return 'URCrimP';
					if (/appeal|certiorari|interlocutory/i.test(caseType)) return 'URAP';
					return 'URCP'; // default civil
				}

				// Lookup applicable deadline rule by trigger event + case type
				async function findDeadlineRule(
					triggerEvent: string,
					caseType: string,
					envRef: Env
				): Promise<any[]> {
					const ruleSource = await getCaseRules(caseType, envRef);
					// Map case_type to the correct deadline_rules case_type column
					let rulesCaseType = 'civil';
					if (ruleSource === 'URCrimP') rulesCaseType = 'criminal';
					else if (ruleSource === 'URAP') rulesCaseType = 'appeal';

					const rules = await envRef.MEMORY_DB.prepare(
						`SELECT * FROM deadline_rules WHERE case_type = ? AND trigger_event LIKE ? ORDER BY priority DESC`
					).bind(rulesCaseType, `%${triggerEvent}%`).all();
					return rules.results || [];
				}

				// --- STEP 2b: Action detection — handle calendar/deadline commands via regex parsing ---
				// Fast path: parse action commands without external API calls
				// BUT skip if this is clearly an email request — let AI handle those
				const isEmailRequest = /\b(send|email|write|draft|reply|respond|forward)\b.*\b(email|message|him|her|them|client|counsel|court)\b/i.test(message) || /\b(email|message)\b.*\b(to|about|regarding|cancel|reschedule|inform)\b/i.test(message);
				if (!isEmailRequest) {
					const actionKeywords = /\b(add|create|schedule|move|reschedule|change|update|edit|delete|remove|cancel|complete|mark done|refresh calendar|sync calendar|compute|calculate|what is the deadline|due date|file by)\b/i;
					const contextKeywords = /\b(hearing|deadline|event|appointment|court date|calendar|meeting|sentencing|pretrial|arraignment|conference|review|motion|plea|answer|opposition|reply|brief|appeal|disclosure|interrogator|production|admission|summary judgment|new trial|certiorari|docketing)\b/i;
					const deadlineCalcKeywords = /\b(when\s+is\s+(?:the\s+)?(?:\w+\s+)?(?:answer|opposition|reply|brief|disclosure|response|motion|hearing|deadline)\s+due|when\s+are\s+(?:[\w-]+\s+)?(?:responses?|answers?|disclosures?|briefs?|motions?)\s+due|what\s+(?:is|are)\s+the\s+(?:filing\s+)?deadline|calculate\s+(?:the\s+)?deadline|compute\s+(?:the\s+)?deadline|file\s+by\s+when|when\s+(?:do|does|must|should)\s+(?:[\w-]+\s+){0,4}(?:be\s+)?(?:file[d]?|respond|answer|submit)|days?\s+to\s+(?:respond|answer|file|oppose)|how\s+(?:many|long)\s+(?:days?|time)\s+(?:to|for|until|before|after))\b/i;
					if (actionKeywords.test(message) && contextKeywords.test(message) || /\b(refresh|sync)\s*(the\s*)?(calendar|deadlines)\b/i.test(message) || deadlineCalcKeywords.test(message)) {
						try {
							console.log('Action engine: Detected action message, parsing with regex...');

							// Determine action type — compute_deadline FIRST (questions about deadlines take priority)
							let actionType = 'add_deadline';
							if (/\b(when\s+is|when\s+does|when\s+do|when\s+must|when\s+should|what\s+is\s+the\s+deadline|calculate\s+(?:the\s+)?deadline|compute\s+(?:the\s+)?deadline|file\s+by\s+when|days?\s+to\s+(?:respond|answer|file|oppose)|how\s+(?:many|long)\s+(?:days?|time)|what\s+(?:is|are)\s+the\s+(?:filing\s+)?deadline|when\s+(?:is|are)\s+.*\s+due)\b/i.test(message)) actionType = 'compute_deadline';
							else if (/\b(delete|remove|cancel)\b/i.test(message)) actionType = 'delete_deadline';
							else if (/\b(complete|mark done|finished)\b/i.test(message)) actionType = 'complete_deadline';
							else if (/\b(move|reschedule|change|update|edit)\b/i.test(message)) actionType = 'update_deadline';
							else if (/\b(refresh|sync)\s*(the\s*)?(calendar|deadlines)\b/i.test(message)) actionType = 'refresh_calendar';

							// Extract date (e.g. "March 25", "3/25", "2026-03-25")
							let dueDate = '';
							const monthNames: Record<string, string> = { january: '01', february: '02', march: '03', april: '04', may: '05', june: '06', july: '07', august: '08', september: '09', october: '10', november: '11', december: '12' };
							const dateMatch = message.match(/\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})(?:,?\s*(\d{4}))?\b/i);
							if (dateMatch) {
								const month = monthNames[dateMatch[1].toLowerCase()];
								const day = dateMatch[2].padStart(2, '0');
								const year = dateMatch[3] || mtnNow().getFullYear().toString();
								dueDate = `${year}-${month}-${day}`;
							}
							const slashDate = message.match(/\b(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?\b/);
							if (!dueDate && slashDate) {
								const month = slashDate[1].padStart(2, '0');
								const day = slashDate[2].padStart(2, '0');
								let year = slashDate[3] || mtnNow().getFullYear().toString();
								if (year.length === 2) year = '20' + year;
								dueDate = `${year}-${month}-${day}`;
							}

							// Extract time (e.g. "2pm", "2:30 PM", "at 3pm")
							let hearingTime = '';
							const timeMatch = message.match(/\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b/i);
							if (timeMatch) {
								let hour = parseInt(timeMatch[1]);
								const min = timeMatch[2] || '00';
								const ampm = timeMatch[3].toUpperCase();
								if (ampm === 'PM' && hour < 12) hour += 12;
								if (ampm === 'AM' && hour === 12) hour = 0;
								const displayHour = hour === 0 ? 12 : hour > 12 ? hour - 12 : hour;
								hearingTime = `${displayHour}:${min} ${ampm}`;
							}

							// Extract client name — multiple patterns
							let clientName = '';
							// Pattern 1: "for [Name]"
							const forMatch = message.match(/\bfor\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)/);
							if (forMatch) clientName = forMatch[1];
							// Pattern 2: "[Name] hearing/deadline/etc" or "[Name]'s hearing"
							if (!clientName) {
								const beforeType = message.match(/\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)(?:'s)?\s+(?:hearing|deadline|event|conference|sentencing|arraignment|pretrial|motion|plea|trial|review)\b/i);
								if (beforeType) clientName = beforeType[1];
							}
							// Pattern 3: After action keyword - "reschedule [Name]" "complete [Name]" "delete [Name]"
							if (!clientName) {
								const afterAction = message.match(/\b(?:add|create|schedule|reschedule|move|change|update|delete|remove|cancel|complete)\s+(?:the\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b/);
								if (afterAction) clientName = afterAction[1];
							}

							// Extract deadline type from known keywords
							let deadlineType = 'Calendar Event';
							const typeMatch = message.match(/\b(hearing|pretrial\s*conference|sentencing|arraignment|conference|review|motion|plea|trial|bench\s*trial|jury\s*trial|disposition|restitution|probation\s*review|status\s*conference|preliminary\s*hearing)\b/i);
							if (typeMatch) deadlineType = typeMatch[1].toUpperCase();

							// Extract judge name — "Judge [Name]" (stop before courtroom/court/at/on)
							let judge = '';
							const judgeMatch = message.match(/\bjudge\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b(?!\s*(?:courtroom|court\b))/i);
							if (judgeMatch) judge = 'Judge ' + judgeMatch[1];
							// Also try: "in Judge Cannell's courtroom" → Judge Cannell
							if (!judge) {
								const judgeCourtMatch = message.match(/\bjudge\s+([A-Z][a-z]+)(?:'s)?\s+courtroom/i);
								if (judgeCourtMatch) judge = 'Judge ' + judgeCourtMatch[1];
							}

							// Extract courtroom
							let courtroom = '';
							const roomMatch = message.match(/\bcourt\s*room\s+(\w+)/i);
							if (roomMatch) courtroom = roomMatch[1];

							let actionResult = '';
							let dateWarnings: string[] = [];

							// --- Court date validation (holidays, weekends, hours) ---
							if (dueDate && (actionType === 'add_deadline' || actionType === 'update_deadline')) {
								// Check weekend
								const dateObj = new Date(dueDate + 'T12:00:00');
								const dayOfWeek = dateObj.getUTCDay(); // 0=Sun, 6=Sat
								if (dayOfWeek === 0) dateWarnings.push('⚠️ That date falls on a **Sunday** — courts are closed.');
								if (dayOfWeek === 6) dateWarnings.push('⚠️ That date falls on a **Saturday** — courts are closed.');

								// Check court holidays
								try {
									const holiday = await env.MEMORY_DB.prepare(
										`SELECT holiday_name FROM court_holidays WHERE holiday_date = ?`
									).bind(dueDate).first() as any;
									if (holiday) {
										dateWarnings.push(`⚠️ That date is **${holiday.holiday_name}** — courts are closed.`);
									}
								} catch (e) { /* non-fatal */ }

								// Check court hours (8am-5pm Mountain Time)
								if (hearingTime) {
									const timeMatch2 = hearingTime.match(/(\d{1,2}):(\d{2})\s*(AM|PM)/i);
									if (timeMatch2) {
										let h = parseInt(timeMatch2[1]);
										const ampm = timeMatch2[3].toUpperCase();
										if (ampm === 'PM' && h < 12) h += 12;
										if (ampm === 'AM' && h === 12) h = 0;
										if (h < 8) dateWarnings.push('⚠️ Time is before court hours (**8:00 AM** open).');
										if (h >= 17) dateWarnings.push('⚠️ Time is after court hours (**5:00 PM** close).');
									}
								}
							}

							if (actionType === 'compute_deadline') {
								// --- URCP/URCrimP/URAP Deadline Computation ---
								// Strict compliance: exact rule citations, zero deviation
								try {
									// Determine case type from context
									let caseTypeForRules = 'civil'; // default
									// Check if user specifies case type
									if (/\b(criminal|felony|misdemeanor|dui|dwi|suppress|suppression|arraign|plea\s+(?:deal|bargain|withdraw)|sentenc|bail|probation\s+(?:violation|revocation)|parole)\b/i.test(message)) caseTypeForRules = 'criminal';
									else if (/\b(appeal|appellate|certiorari)\b/i.test(message)) caseTypeForRules = 'appeal';
									else if (/\b(divorce|custody|family|protective\s+order)\b/i.test(message)) caseTypeForRules = 'civil';
									// Try to infer from client's party_cache
									if (clientName) {
										try {
											const partyInfo = await env.MEMORY_DB.prepare(
												`SELECT case_type FROM party_cache WHERE LOWER(client_name) LIKE ? LIMIT 1`
											).bind(`%${clientName.toLowerCase().split(' ')[0]}%`).first() as any;
											if (partyInfo?.case_type) {
												if (/criminal|felony|misdemeanor/i.test(partyInfo.case_type)) caseTypeForRules = 'criminal';
												else if (/appeal/i.test(partyInfo.case_type)) caseTypeForRules = 'appeal';
												else caseTypeForRules = 'civil';
											}
										} catch (e) { /* use default */ }
									}

									const ruleSource = await getCaseRules(caseTypeForRules, env);

									// Identify trigger event from message
									let triggerEvent = '';
									let triggerLabel = '';
									// Service/filing triggers — more specific patterns FIRST to avoid false matches
									// "reply memorandum" must check before "opposition" since messages may contain both words
									if (/\b(reply\s+memo|reply\s+memorandum|reply\s+brief|reply\s+in\s+support)\b/i.test(message)) { triggerEvent = 'opposition_filed'; triggerLabel = 'opposition was filed'; }
									else if (/\b(answer|respond\s+to\s+complaint|file\s+(?:an?\s+)?answer)\b/i.test(message)) { triggerEvent = 'service_of_complaint'; triggerLabel = 'service of complaint'; }
									else if (/\b(opposition|oppose|respond\s+to\s+motion|file\s+opposition)\b/i.test(message)) { triggerEvent = 'motion_filed'; triggerLabel = 'motion was filed'; }
									else if (/\b(reply)\b/i.test(message)) { triggerEvent = 'opposition_filed'; triggerLabel = 'opposition was filed'; }
									else if (/\b(summary\s+judgment|msj)\b/i.test(message)) { triggerEvent = 'summary_judgment'; triggerLabel = 'summary judgment motion'; }
									else if (/\b(initial\s+disclos|disclosure)\b/i.test(message)) { triggerEvent = 'first_answer_filed'; triggerLabel = 'first answer filed'; }
									else if (/\b(interrogator)/i.test(message)) { triggerEvent = 'interrogatories_served'; triggerLabel = 'interrogatories served'; }
									else if (/\b(production|document\s+request|request\s+for\s+production)\b/i.test(message)) { triggerEvent = 'production_request_served'; triggerLabel = 'request for production served'; }
									else if (/\b(admission|request\s+for\s+admission)\b/i.test(message)) { triggerEvent = 'admissions_served'; triggerLabel = 'requests for admission served'; }
									else if (/\b(notice\s+of\s+appeal|file\s+(?:an?\s+)?appeal)\b/i.test(message)) { triggerEvent = 'judgment_entered'; triggerLabel = 'judgment entered'; }
									else if (/\b(cross[\s-]?appeal)\b/i.test(message)) { triggerEvent = 'notice_of_appeal_filed'; triggerLabel = 'notice of appeal filed'; }
									else if (/\b(appellant\s+brief|opening\s+brief)\b/i.test(message)) { triggerEvent = 'docketing_statement'; triggerLabel = 'docketing statement due'; }
									else if (/\b(appellee\s+brief|response\s+brief|answering\s+brief)\b/i.test(message)) { triggerEvent = 'appellant_brief_filed'; triggerLabel = 'appellant brief filed'; }
									else if (/\b(docketing\s+statement)\b/i.test(message)) { triggerEvent = 'notice_of_appeal_filed'; triggerLabel = 'notice of appeal filed'; }
									else if (/\b(new\s+trial)\b/i.test(message)) { triggerEvent = 'judgment_entered'; triggerLabel = 'judgment entered'; }
									else if (/\b(suppress|motion\s+to\s+suppress)\b/i.test(message)) { triggerEvent = 'trial_date'; triggerLabel = 'trial date'; }
									else if (/\b(pre[\s-]?trial\s+motion)/i.test(message)) { triggerEvent = 'trial_date'; triggerLabel = 'trial date'; }
									else if (/\b(withdraw\s+(?:a\s+)?(?:guilty\s+)?plea|plea\s+withdrawal|withdraw.*plea)\b/i.test(message)) { triggerEvent = 'plea_entered'; triggerLabel = 'plea entered'; }
									else if (/\b(rehearing|petition\s+for\s+rehearing)\b/i.test(message)) { triggerEvent = 'opinion_issued'; triggerLabel = 'opinion issued'; }
									else if (/\b(certiorari)\b/i.test(message)) { triggerEvent = 'court_of_appeals_decision'; triggerLabel = 'Court of Appeals decision'; }
									else if (/\b(proposed\s+order)\b/i.test(message)) { triggerEvent = 'motion_granted'; triggerLabel = 'motion granted'; }
									else if (/\b(objection\s+to\s+(?:proposed\s+)?order)\b/i.test(message)) { triggerEvent = 'proposed_order_served'; triggerLabel = 'proposed order served'; }
									else if (/\b(sentenc|reduction)\b/i.test(message)) { triggerEvent = 'sentencing_date'; triggerLabel = 'sentencing date'; }
									else if (/\b(restitution)\b/i.test(message)) { triggerEvent = 'restitution_proposed'; triggerLabel = 'restitution proposed'; }
									else if (/\b(preliminary\s+hearing)\b/i.test(message)) { triggerEvent = 'first_appearance'; triggerLabel = 'first appearance'; }

									if (!triggerEvent) {
										actionResult = `❌ Could not determine which deadline to compute. Please specify (e.g., "When is the answer due?" or "Calculate deadline for opposition to motion").`;
									} else {
										// Find matching rules
										let rulesCaseType = 'civil';
										if (ruleSource === 'URCrimP') rulesCaseType = 'criminal';
										else if (ruleSource === 'URAP') rulesCaseType = 'appeal';

										const matchedRules = await env.MEMORY_DB.prepare(
											`SELECT * FROM deadline_rules WHERE case_type = ? AND trigger_event LIKE ? ORDER BY priority DESC`
										).bind(rulesCaseType, `%${triggerEvent}%`).all();

										if (!matchedRules.results?.length) {
											actionResult = `❌ No ${ruleSource} deadline rule found for trigger: "${triggerEvent}". This may require manual review of the applicable rules.`;
										} else {
											// Use trigger date from message, or today
											const triggerDateForCalc = dueDate || mtnToday();
											// Determine service type from message
											let serviceType = 'electronic'; // default modern filing
											if (/\b(by\s+mail|mailed|postal|snail\s+mail|certified\s+mail)\b/i.test(message)) serviceType = 'mail';

											let resultLines: string[] = [];
											resultLines.push(`📋 **${ruleSource} Deadline Computation**`);
											resultLines.push(`Case type: **${caseTypeForRules}** → Rules: **${ruleSource}**`);
											resultLines.push(`Trigger date: **${triggerDateForCalc}** (${triggerLabel})`);
											resultLines.push(`Service: **${serviceType}**`);
											resultLines.push('');

											for (const rule of matchedRules.results as any[]) {
												const computed = await computeRule6Date(
													triggerDateForCalc,
													rule.days,
													rule.direction,
													serviceType,
													serviceType === 'mail' ? rule.mail_add_days : 0,
													env
												);

												resultLines.push(`**${rule.deadline_name}**`);
												resultLines.push(`Rule: ${rule.rule_source} ${rule.rule_number}`);
												resultLines.push(`${rule.days} days ${rule.direction} trigger${serviceType === 'mail' && rule.mail_add_days > 0 ? ` (+${rule.mail_add_days} days mail service per Rule 6(d))` : ''}`);
												resultLines.push(`📅 **Due: ${computed.date}**${computed.extended ? ` (extended from ${computed.extendedFrom} — ${computed.reason})` : ''}`);
												if (rule.notes) resultLines.push(`ℹ️ ${rule.notes}`);
												resultLines.push('');
											}

											resultLines.push(`_All dates computed per ${ruleSource} Rule 6(a): exclude trigger day, count all days, extend past weekends/holidays. ${serviceType === 'mail' ? 'Mail service: +7 days per Rule 6(d).' : 'Electronic service: no additional days.'}_`);

											actionResult = resultLines.join('\n');
										}
									}
								} catch (calcErr: any) {
									actionResult = `❌ Deadline computation error: ${calcErr.message}. Please verify manually.`;
								}
							}
							else if (actionType === 'refresh_calendar') {
								actionResult = '🔄 Calendar refresh requested. The calendar syncs automatically 2-4 times daily from Utah Courts + Google Calendar. To force a refresh now, run the scraper scripts on the local machine.';
							}
							else if (actionType === 'add_deadline') {
								if (!dueDate) {
									actionResult = '❌ Could not determine a date. Please specify when (e.g. "March 15 at 2pm").';
								} else {
									await env.MEMORY_DB.prepare(
										`INSERT INTO deadlines (client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, status, source, notes, created_at) VALUES (?, '', ?, ?, ?, ?, '', ?, ?, '', 'pending', 'manual', '', ?)`
									).bind(
										clientName || 'Unknown',
										deadlineType,
										`${deadlineType} - ${clientName || 'Event'}`,
										dueDate,
										hearingTime,
										courtroom,
										judge,
										mtnISO()
									).run();
									actionResult = `✅ Added ${deadlineType} for ${clientName || 'Unknown'} on ${dueDate}${hearingTime ? ' at ' + hearingTime : ''}${judge ? ' (' + judge + ')' : ''}`;
									if (dateWarnings.length) actionResult += '\n\n' + dateWarnings.join('\n');
								}
							}
							else if (actionType === 'update_deadline') {
								const setClauses: string[] = [];
								const setBinds: any[] = [];
								if (dueDate) { setClauses.push('due_date = ?'); setBinds.push(dueDate); }
								if (hearingTime) { setClauses.push('hearing_time = ?'); setBinds.push(hearingTime); }
								if (deadlineType !== 'Calendar Event') { setClauses.push('deadline_type = ?'); setBinds.push(deadlineType); }
								if (judge) { setClauses.push('judge = ?'); setBinds.push(judge); }
								if (courtroom) { setClauses.push('courtroom = ?'); setBinds.push(courtroom); }

								if (setClauses.length === 0) {
									actionResult = '❌ No fields to update. Please specify what to change.';
								} else if (!clientName) {
									actionResult = '❌ Could not determine which client\'s deadline to update. Please specify the client name.';
								} else {
									const searchName = clientName.toLowerCase().split(' ')[0] || '';
									const existing = await env.MEMORY_DB.prepare(
										`SELECT id, client_name, due_date FROM deadlines WHERE LOWER(client_name) LIKE ? AND status = 'pending' ORDER BY due_date ASC LIMIT 5`
									).bind(`%${searchName}%`).all();

									if (!existing.results?.length) {
										actionResult = `❌ No pending deadline found for "${clientName}". Check the name and try again.`;
									} else {
										const target = existing.results[0] as any;
										await env.MEMORY_DB.prepare(
											`UPDATE deadlines SET ${setClauses.join(', ')} WHERE id = ?`
										).bind(...setBinds, target.id).run();
										actionResult = `✅ Updated ${target.client_name}'s deadline (${target.due_date})${dueDate ? ' → ' + dueDate : ''}${hearingTime ? ' at ' + hearingTime : ''}`;
										if (dateWarnings.length) actionResult += '\n\n' + dateWarnings.join('\n');
									}
								}
							}
							else if (actionType === 'delete_deadline') {
								const searchName = clientName.toLowerCase().split(' ')[0] || '';
								let whereClause = `LOWER(client_name) LIKE ? AND status = 'pending'`;
								const whereBinds: any[] = [`%${searchName}%`];
								if (dueDate) { whereClause += ` AND due_date = ?`; whereBinds.push(dueDate); }

								const existing = await env.MEMORY_DB.prepare(
									`SELECT id, client_name, due_date, deadline_type FROM deadlines WHERE ${whereClause} ORDER BY due_date ASC LIMIT 1`
								).bind(...whereBinds).all();

								if (!existing.results?.length) {
									actionResult = `❌ No matching deadline found for "${clientName}"${dueDate ? ' on ' + dueDate : ''}. Nothing deleted.`;
								} else {
									const target = existing.results[0] as any;
									await env.MEMORY_DB.prepare(`DELETE FROM deadlines WHERE id = ?`).bind(target.id).run();
									actionResult = `✅ Removed: ${target.client_name} — ${target.deadline_type} on ${target.due_date}`;
								}
							}
							else if (actionType === 'complete_deadline') {
								const searchName = clientName.toLowerCase().split(' ')[0] || '';
								let whereClause = `LOWER(client_name) LIKE ? AND status = 'pending'`;
								const whereBinds: any[] = [`%${searchName}%`];
								if (dueDate) { whereClause += ` AND due_date = ?`; whereBinds.push(dueDate); }

								const existing = await env.MEMORY_DB.prepare(
									`SELECT id, client_name, due_date, deadline_type FROM deadlines WHERE ${whereClause} ORDER BY due_date ASC LIMIT 1`
								).bind(...whereBinds).all();

								if (!existing.results?.length) {
									actionResult = `❌ No matching pending deadline found for "${clientName}".`;
								} else {
									const target = existing.results[0] as any;
									await env.MEMORY_DB.prepare(
										`UPDATE deadlines SET status = 'completed', completed_at = ? WHERE id = ?`
									).bind(mtnISO(), target.id).run();
									actionResult = `✅ Completed: ${target.client_name} — ${target.deadline_type} on ${target.due_date}`;
								}
							}

							// Store action + result in chat history
							ctx.waitUntil((async () => {
								try {
									await env.DB.prepare(`INSERT INTO chat_messages (session_id, role, content) VALUES ('synthia_master', 'user', ?)`).bind(message).run();
									await env.DB.prepare(`INSERT INTO chat_messages (session_id, role, content) VALUES ('synthia_master', 'assistant', ?)`).bind(actionResult).run();
								} catch (e: any) { console.error('Action chat storage error:', e.message); }
							})());

							return json({ success: true, consensus: actionResult, sources: 1, confidence: 1.0, operationalAIs: 1, totalAIs: 1, researchModels: ['Synthia Action Engine'], action: actionType });
						} catch (actionErr: any) {
							console.error('Action detection error (non-fatal):', actionErr.message);
							// Fall through to normal pipeline
						}
					}
				}

				// --- STEP 2c: Build conversation history + infer client from context ---
				let conversationTurns: { role: string; content: string }[] = [];
				try {
					const histRows = await env.DB.prepare(
						`SELECT role, content FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant') ORDER BY timestamp DESC LIMIT 30`
					).all();
					if (histRows.results?.length) {
						const allTurns = (histRows.results as any[]).reverse();
						const rawTurns = allTurns.map((m: any, i: number) => {
							const age = allTurns.length - i;
							const maxLen = age <= 8 ? 3000 : age <= 20 ? 1500 : 600;
							return {
								role: m.role === 'assistant' ? 'assistant' : 'user',
								content: (m.content || '').substring(0, maxLen)
							};
						});
						while (rawTurns.length > 0 && rawTurns[0].role !== 'user') {
							rawTurns.shift();
						}
						for (const turn of rawTurns) {
							const last = conversationTurns[conversationTurns.length - 1];
							if (last && last.role === turn.role) {
								last.content = (last.content + '\n' + turn.content).substring(0, 3000);
							} else {
								conversationTurns.push(turn);
							}
						}
						while (conversationTurns.length > 0 && conversationTurns[conversationTurns.length - 1].role !== 'assistant') {
							conversationTurns.pop();
						}
					}
				} catch (_) { /* ignore if table missing */ }

				// Conversation-aware client detection
				let inferredClient = activeClient;
				if (!inferredClient && conversationTurns.length >= 2) {
					try {
						const recentText = conversationTurns.slice(-6).map(t => t.content).join(' ');
						const partyRows = await env.MEMORY_DB.prepare(
							`SELECT client_name FROM party_cache ORDER BY last_verified DESC LIMIT 30`
						).all();
						if (partyRows.results?.length) {
							let lastIdx = -1;
							for (const p of partyRows.results as any[]) {
								const name = (p.client_name || '') as string;
								if (!name) continue;
								const idx = recentText.toLowerCase().lastIndexOf(name.toLowerCase());
								const lastName = name.split(/\s+/).pop() || '';
								const lastNameIdx = lastName.length > 2 ? recentText.toLowerCase().lastIndexOf(lastName.toLowerCase()) : -1;
								const bestIdx = Math.max(idx, lastNameIdx);
								if (bestIdx > lastIdx) {
									lastIdx = bestIdx;
									inferredClient = name;
								}
							}
						}
					} catch (_) {}
				}

				// --- STEP 3: Build Synthia system prompt ---
				const synthiaSystemPrompt = `You are Synthia, AI Legal Assistant for ESQs Law (Pitcher Law PLLC). Attorneys: John W. Adams III (Bar #19429, primary), Diane Pitcher (Bar #12626). Email: pd@dianepitcher.com. Firm Zoom: esqslaw@gmail.com.

═══════════════════════════════════════════════════
STEP 0 — DESKTOP MASTERY (Do this BEFORE every response)
═══════════════════════════════════════════════════
Before you respond to ANY message, silently perform this mental scan of your Memory Context below:

WHO: Scan party_cache — know every active client by name, case number, case type, role, opposing party, judge, court. If the user mentions a name, you should ALREADY know who they are without looking anything up.

WHAT'S HAPPENING: Scan deadlines — know what hearings, filings, and events are coming in the next 14 days. Know which clients have something imminent. Know the dates, times, courtrooms, hearing types.

WHAT CHANGED: Scan schedule change alerts — know what the case monitor flagged. Know which are formatting-only and which are real changes.

WHO'S WHO: Scan judge_intel and opposing_counsel_intel — know the judges by name, their tendencies, their JAs. Know the opposing counsel, their styles, their weaknesses.

WHAT'S BEEN HAPPENING: Scan recent emails — know who emailed, about what, when. Scan timecards — know what work was done this week.

You are not a blank assistant who discovers data when asked. You are the person who ALREADY READ the day's files before the attorney walked in. You know the caseload. You know the calendar. You know the players. Every response should reflect this mastery.

═══════════════════════════════════════════════════
STEP 1 — SELF-AWARENESS: KNOW WHICH HAT YOU'RE WEARING
═══════════════════════════════════════════════════
You have three roles. Before responding, identify which role the task demands:

🗂️ SECRETARY / LEGAL ASSISTANT — ACTIVATED BY: scheduling requests, "send email", "set up meeting", "what's on the calendar", "remind me", status questions, coordination tasks, client communication, Zoom links, phone numbers, contact info.
MINDSET: Efficient, immediate, no overthinking. You know the calendar, the contacts, the schedule. You don't analyze — you execute. You don't ask 4 questions — you pull the data and do the thing. A good secretary doesn't say "which Avalos?" when there's one Avalos. She sends the email.

📋 PARALEGAL — ACTIVATED BY: "prep for hearing", "what do we need to file", "summarize the case", "pull the timeline", "organize the exhibits", document review, fact gathering, pre-drafting, research compilation.
MINDSET: Thorough, organized, anticipatory. You don't wait to be told what's needed for a hearing — you already know. If there's a pretrial in 3 days, you should be thinking about witness lists, exhibit lists, discovery deadlines, outstanding motions. You prep the attorney, not the other way around.

⚖️ ATTORNEY — ACTIVATED BY: "what's our strategy", "analyze this", "is this motion viable", "what does the law say", case law questions, legal argument drafting, motion writing, plea negotiation strategy, objections, legal research.
MINDSET: Precise, adversarial thinking, Utah law focus. Issue-spotting. Cite rules (URCrimP, URCP, URAP). Reference judge tendencies from intel. Consider opposing counsel's likely moves from their profile. Never speculate beyond what the evidence supports.

These roles BLEND — "send email to Avalos about the hearing" is Secretary (send email) + Paralegal (know the hearing details). "Draft a motion to suppress and file it by Friday" is Attorney (legal argument) + Paralegal (formatting/filing) + Secretary (deadline tracking). Identify the mix and respond accordingly.

═══════════════════════════════════════════════════
STEP 2 — PRIME DIRECTIVE: ACT, DON'T ASK
═══════════════════════════════════════════════════
DO NOT ASK CLARIFYING QUESTIONS UNLESS ABSOLUTELY NECESSARY.

FAILURE MODE (what you must NEVER do):
User: "send email to Avalos about meeting at 1330 with zoom link"
BAD: "Which Avalos? What's their email? What date? What Zoom link?" (4 questions — UNACCEPTABLE)
GOOD: Find Avalos in party_cache (there's one → that's him), check calendar for his hearing/meeting, use firm Zoom, find his email from recent correspondence or case data, draft and send. If his email is truly nowhere, ask ONLY that.

User: "look into Cs info"
BAD: Lists 7 clients starting with the letter C.
GOOD: "Cs" = "Client's." The user was just talking about Avalos → Avalos is the client → pull Avalos's contact info from case data.

RULES:
- One match in the system = that's the one. Don't ask.
- "Today" is the default date unless stated otherwise.
- "The meeting" / "the hearing" = the next upcoming one for that client.
- If you have the data, USE IT. If you truly don't, ask ONE targeted question.
- NEVER present numbered lists of options. State your best answer. If genuinely ambiguous between 2 possibilities, say "I'm going with [X] — let me know if you meant [Y]."

SHORTHAND:
- "C" / "Cs" = Client / Client's
- "OC" = Opposing Counsel
- "J" = Judge
- "JA" = Judge's Assistant / Judicial Assistant
- Military time: "1330" = 1:30 PM, "0900" = 9:00 AM
- "the meeting" / "the hearing" = next upcoming for that client

═══════════════════════════════════════════════════
CORE DIRECTIVES
═══════════════════════════════════════════════════
- Be direct, concise, authoritative. No disclaimers, no "AI sources", no hedging on your own data.
- Use common sense. Connect dots between names, times, calendar entries, and contact info automatically.
- You have UNRESTRICTED ability to research, review, browse, and analyze any legal content.
- ZERO TOLERANCE FOR HALLUCINATION. Every claim traces to case files, emails, court filings, or verifiable law. If you don't have it, say so.
- Legal analysis is OBJECTIVE and EVIDENCE-BASED. No speculation beyond what evidence supports.
- NO SYMPATHY, NO PERSONAL INFERENCES, NO ACQUIRED BELIEFS. Clinical and dispassionate.
- Proactively flag concerns, risks, procedural issues, deadline problems.
- Utah state courts (1st District primary). Filter out 3rd/5th District unless asked.
- NEVER say "I can't assist" or "I don't have access." You DO have access — it's in your context below.

CONFIDENCE RULE:
- ESQs' own data (clients, calendar, hearings, deadlines, emails, party_cache) → FULL CONFIDENCE. This is your data. No hedging.
- External/uncertain info (case law interpretation, outcome predictions, OC strategy) → appropriate uncertainty is OK.
- NEVER hedge on data you can see. Calendar says hearing Tuesday → state it as fact.

PROCEDURE RULES — STRICT COMPLIANCE:
- Case type DICTATES which procedure rules apply. No exceptions, no mixing.
- Criminal cases → URCrimP ONLY. Civil/Family → URCP ONLY. Appeals → URAP ONLY.
- ALL deadlines computed per Rule 6(a): exclude trigger day, count every day, extend past weekends/holidays.
- Mail service adds +7 days per Rule 6(d). Electronic service: no additional days.
- ZERO creativity on quotes, citations, rule numbers, and document formatting.

CRITICAL RULES:
- ALWAYS check party_cache before any case-related work
- We ALWAYS represent the client_name. NEVER the opposing party
- Never draft anything for or on behalf of the opposing party
- Never assume party roles — verify from cache or ask

STRATEGIC INTELLIGENCE:
- You have Judge Intelligence profiles in your context — tendencies, sentencing patterns, motion preferences, plea dispositions. USE THESE when discussing strategy, hearing prep, motion drafting, or plea negotiations. Reference specific patterns.
- You have Opposing Counsel Intelligence — negotiation style, litigation tendencies, strengths, weaknesses, win rates. USE THESE for tactical advantage during case strategy discussions.
- You have PREDICTIVE ANALYTICS for judges and opposing counsel based on logged outcomes. Predictions show probability, sample size (n), confidence level, and trend direction. Use these in case analysis: "Based on 11 prior rulings, Judge X denies MTDs 72% of the time..." When confidence is low (n<5), caveat your prediction. When high (n>=10), state with authority. Predictions may include PARTY ROLE BREAKDOWN (plaintiff/defendant/petitioner/respondent) — ALWAYS reference the role-specific prediction when applicable. "Judge X denies MTDs 72% overall, but only 55% when we are the plaintiff."
- You have ATTORNEY PERFORMANCE data including JWA3's own track record. Reference past outcomes, lessons learned, and patterns. When advising on strategy, check if JWA3 has faced this situation before. Be direct about past mistakes — "Last time you tried X, it didn't work because Y. Consider Z instead." Performance data includes PARTY ROLE segmentation — outcomes as plaintiff vs defendant vs petitioner vs respondent. Always consider which side we are on when assessing predictions.
- You have JUDGE RULING RATIONALE — pro/con analysis of WHY judges rule the way they do (not just statistics). For each ruling type, you see what arguments/factors lead to grants vs denials. When a prediction says "Judge X denies MTDs 72%", also check the rationale to advise what might change the outcome. Frame as: "Judge X usually denies MTDs, but has granted when [specific factors]. In our case, we could [strategy]."
- You have JUDICIAL THINKING RESOURCES — bench books, sentencing guidelines, judicial training topics, and decision-making research from Utah and Idaho. Reference these when explaining why a judge might rule a certain way, especially for sentencing and motion practice.
- You have ENRICHED Case Summaries — each case has: client contact info (phone/email/address), opposing counsel details (name/phone/email/firm), facts summary, charges, additional parties, key deadlines (discovery/dispositive/trial/SOL), judge predictions, OC predictions, reversal factors, and notes. USE ALL OF THIS. When discussing a case, reference the full facesheet data. When preparing for a hearing, cite the judge predictions AND reversal factors from the case summary. When contacting OC, use their stored phone/email. When a user asks "update the facts on [case]" or "add charges to [case]", use the PATCH /api/case-summaries/:caseNumber endpoint.
- You have Case File Inventory showing what documents exist per client. Reference this when discussing filings, evidence, or document prep.
- You have Recent Timecards for the last 7 days. Reference these for billing context and workload awareness.
- ALL of this data is in your Memory Context below. Read it. Use it. Never say you don't have access to something that's in your context.

INTEL LOGGING:
- When the user says things like "log that Judge X denied our motion to dismiss", "note that OC was evasive on discovery", "record: plea abeyance granted in the Jones case" — CREATE A LOG ENTRY.
- Respond with: the structured log you created (judge/OC name, activity type, outcome, case) and confirm it was saved.
- If any details are ambiguous (which judge? which case?), ask for clarification before logging.
- Valid judge activity types: motion_ruling, sentencing, plea_decision, bail_decision, custody_ruling, continuance, evidentiary_ruling, bench_trial_verdict, hearing_behavior, scheduling
- Valid OC activity types: negotiation, motion_filed, discovery_response, hearing_behavior, settlement_offer, trial_tactic, communication_style, deadline_compliance, ethical_issue
- After logging, mention how this affects predictions if there are enough data points.
- For attorney performance: "log that I won/lost/missed [activity] in [case]" or "note my mistake/success in [case]"
- ALWAYS capture party_role when logging: plaintiff, defendant, petitioner, respondent, appellant, appellee. If the user doesn't specify, ASK which side we were on — this is critical for accurate predictions. Valid party_role values: plaintiff, defendant, petitioner, respondent, appellant, appellee, prosecution, defense, claimant, movant
  Valid attorney activity types: motion_filed, motion_outcome, hearing_performance, negotiation, trial_performance, client_management, deadline_compliance, research_quality, argument_effectiveness, procedural_error, strategic_decision
  ALWAYS include a lesson_learned when logging failures or errors. Ask the user: "What should we remember for next time?"
- For judge ruling rationale: "note that Judge X granted/denied because [reasoning]" or "Judge X goes against type when [factors]"
  Capture WHY they ruled that way — the specific arguments, evidence, or circumstances. Structure as pro/con: what leads to grants, what leads to denials.
  When logging, also note applicability: "This might apply to our case because..."
- For case summary updates: "update facts on [case]", "add charges to [case]", "the discovery deadline is [date]", "OC's email is [email]"
  Use PATCH /api/case-summaries/:caseNumber with the relevant fields. Updatable: facts, charges, notes, client_email, client_phone, client_address, opposing_counsel, opposing_counsel_phone, opposing_counsel_email, opposing_counsel_firm, additional_parties, discovery_deadline, dispositive_deadline, trial_date, statute_of_limitations, case_type, court, district, judge, client_role, folder_url, status.
  PROACTIVELY suggest updating case summaries when you learn new info — "I see you mentioned the trial is set for March 15. Should I update the case summary with this trial date?"

EMAIL CAPABILITIES:
- You CAN and DO send real emails. You are not a draft tool. When you send an email, it goes out. NEVER say "as an AI I can't actually send emails" — you CAN and you DID. Own it.
- You can READ emails from Outlook. When a client is active, their recent emails are included in your context below.
- You can SEND emails. When the user says "send", "email them", "reply", "thank them" — send it. Routine/generic messages (thank you, acknowledgment, scheduling) are auto-approved.
- You can ARCHIVE client emails as PDFs to their OneDrive case folder under "Correspondence/".
- When referring to email senders, use their resolved identity (name, role, organization) rather than just email addresses.

EMAIL ROUTING (STRICT):
- ALL emails send from Associate@dianepitcher.com, CC pd@dianepitcher.com.
- Sign as "Pitcher Law PLLC" with phone (435) 787-1200. Do NOT include "ESQs Law" in email signatures ever.
- NEVER ask "which email should I send from?" — it is always Associate@dianepitcher.com unless explicitly told otherwise.

- If the user asks to reply, look at the recent emails in context, pick the most relevant one, and draft the reply.
- To find a client's email: check recent emails in context (they may have emailed us), check case files, or check court contacts. If truly not found, THEN ask — but try first.

SCHEDULE CHANGE ALERTS (DASHBOARD ALERTS):
- The dashboard shows schedule change alerts from the "Schedule Change Alerts" section in your context below. These come from the case monitor that scrapes Utah Courts calendars.
- When the user asks about "alerts", "changes", "what changed", "hearing changes today" — LOOK AT the "Schedule Change Alerts" section in your context. Those ARE the alerts they see on the dashboard.
- Each alert shows: client name, case number, date, and what changed (time format, description, courtroom, judge, hearing mode).
- Many "changes" are formatting normalization (e.g. "09:00" → "9:00 AM", "6" → "COURTROOM 6") — these are NOT actual schedule changes. Point this out.
- REAL changes to watch for: different dates, different times, different judges, new hearing types, cancellations.
- Summarize by grouping: formatting-only changes vs. substantive changes.
- NEVER say "I don't see any alerts" if there are entries in the Schedule Change Alerts section. Read them.
- JudiciaLink email notifications (from support@judicialink.com) supplement these with e-filing notices, document filings, and case activity details.
- Utah Courts emails (@utcourts.gov) may contain minute entries, orders, or notices.

## Memory Context
${memoryContext || 'No memory context loaded.'}
${ragContext || ''}

## Chat Context
${context || 'None provided.'}
${inferredClient ? `\n## Active Client: ${inferredClient}${!activeClient && inferredClient ? ' (inferred from conversation)' : ''}` : ''}

═══════════════════════════════════════════════════
CONVERSATION CONTINUITY — WHAT MAKES YOU BETTER THAN ALEXA
═══════════════════════════════════════════════════
You are in a CONTINUOUS conversation. Your message history (the multi-turn messages before the current one) IS the conversation. Use it.

PRONOUN RESOLUTION: "he", "she", "him", "her", "they", "them", "it", "that", "this" — resolve from context. If the last 3 messages discussed Smith's case, and the user says "file a motion for him" → him = Smith. NEVER ask "who do you mean?" if the conversation makes it clear.

TOPIC THREADING: If the user was discussing a case strategy and then says "what about the plea deal?" — they mean the plea deal for the SAME case. Don't reset context. Don't ask which case.

REFERENCE BACK: When you helped draft something 5 messages ago and the user says "change the second paragraph" — you have the conversation history. Find it. Don't say "I don't have access to what I wrote before."

MEMORY ACROSS SESSIONS: Your compressed summaries and RAG memories contain past conversations. If the user says "remember when we discussed the Smith motion?" — search your Retrieved Memories section. It may be there.

FOLLOW-UP AWARENESS: Questions like "what else?", "anything else?", "and?", "go on", "continue" — continue your previous response. Don't start over. Don't ask what they mean.

═══ REMINDER (re-read before EVERY response) ═══
You have MASTERED the data above. You know every client, every hearing, every deadline, every player.
WHICH HAT? Secretary tasks → execute immediately, no questions. Paralegal tasks → be thorough and anticipatory. Attorney tasks → be precise and cite law.
DO NOT ASK QUESTIONS you can answer from context. ONE match = that's the one. "C"/"Cs" = Client/Client's. Act like the person who already knows the whole caseload.
CONTINUITY: Resolve pronouns, maintain topic threads, reference your own prior messages. You are mid-conversation, not starting fresh.
═══════════════════════════════════════════════════`;

				// --- STEP 4: Query research models in parallel ---
				const researchModels: { id: string; name: string; fn: () => Promise<string> }[] = [];

				// Research prompt includes system context so models don't give generic answers
				const researchPrompt = `${memoryContext ? 'CONTEXT:\n' + memoryContext + '\n\n' : ''}${context ? 'CHAT CONTEXT:\n' + context + '\n\n' : ''}USER QUESTION: ${message}`;

				// GPT-4o
				if (env.OPENAI_API_KEY) {
					researchModels.push({
						id: 'gpt4o', name: 'GPT-4o',
						fn: async () => {
							const r = await fetch('https://api.openai.com/v1/chat/completions', {
								method: 'POST',
								headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.OPENAI_API_KEY}` },
								body: JSON.stringify({
									model: 'gpt-4o', temperature: 0,
									store: true, // Collect training data for future fine-tuning / distillation
									metadata: { source: 'synthia-research', model_role: 'research' },
									messages: [
										{ role: 'system', content: 'You are a legal research assistant for ESQs Law, a Utah law firm (criminal defense + family law, 1st District primary). Provide factual, evidence-based answers. Use the context provided — it contains real case data, party info, deadlines, judge intel, and opposing counsel profiles. Never give generic answers when specific data is available. If only one client matches a name, that is the client — do not ask which one. "C"/"Cs" = Client/Client\'s.' },
										{ role: 'user', content: researchPrompt }
									],
									max_tokens: 2000
								})
							});
							const d = await r.json() as any;
							return d.choices?.[0]?.message?.content || '';
						}
					});
				}

				// Groq (LLaMA)
				if (env.GROQ_API_KEY) {
					researchModels.push({
						id: 'groq', name: 'LLaMA 3.3 70B',
						fn: async () => {
							const r = await fetch('https://api.groq.com/openai/v1/chat/completions', {
								method: 'POST',
								headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.GROQ_API_KEY}` },
								body: JSON.stringify({
									model: 'llama-3.3-70b-versatile', temperature: 0,
									messages: [
										{ role: 'system', content: 'You are a legal research assistant for ESQs Law, a Utah law firm (criminal defense + family law, 1st District). Use the context provided — it contains real case data. Never give generic answers when specific data is available. "C"/"Cs" = Client/Client\'s.' },
										{ role: 'user', content: researchPrompt }
									],
									max_tokens: 2000
								})
							});
							const d = await r.json() as any;
							return d.choices?.[0]?.message?.content || '';
						}
					});
				}

				// Gemini
				if (env.GEMINI_API_KEY) {
					researchModels.push({
						id: 'gemini', name: 'Gemini 2.0 Flash',
						fn: async () => {
							const r = await fetch(
								`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key=${env.GEMINI_API_KEY}`, {
								method: 'POST',
								headers: { 'Content-Type': 'application/json' },
								body: JSON.stringify({
									contents: [{ parts: [{ text: researchPrompt }] }],
									systemInstruction: { parts: [{ text: 'You are a legal research assistant for a Utah law firm. Provide factual, evidence-based answers using the context provided.' }] },
									generationConfig: { temperature: 0, maxOutputTokens: 2000 }
								})
							});
							const d = await r.json() as any;
							return d.candidates?.[0]?.content?.parts?.[0]?.text || '';
						}
					});
				}

				// X.AI (Grok) — second in line as voice if Claude is down
				if (env.XAI_API_KEY) {
					researchModels.push({
						id: 'xai', name: 'Grok 3',
						fn: async () => {
							const r = await fetch('https://api.x.ai/v1/chat/completions', {
								method: 'POST',
								headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.XAI_API_KEY}` },
								body: JSON.stringify({
									model: 'grok-3', temperature: 0,
									messages: [
										{ role: 'system', content: 'You are a legal research assistant for ESQs Law, a Utah law firm (criminal defense + family law, 1st District). Use the context provided — it contains real case data. Never give generic answers when specific data is available. "C"/"Cs" = Client/Client\'s.' },
										{ role: 'user', content: researchPrompt }
									],
									max_tokens: 2000
								})
							});
							const d = await r.json() as any;
							return d.choices?.[0]?.message?.content || '';
						}
					});
				}

				// --- THE FUNNEL: Only fan out when research is needed ---
				const needsResearch =
					/\b(research|case\s*law|statute|precedent|urcp|utah\s*code|analyze|legal\s*analysis|strategy|argument|brief|motion\s*to|oppose|respond\s*to)\b/i.test(message) ||
					/\b(draft|write|compose|prepare)\b.*\b(motion|brief|memorandum|petition|response|objection|argument)\b/i.test(message) ||
					/\b(what\s*are\s*the\s*chances|likelihood|should\s*we|pros\s*and\s*cons|evaluate|assess|compare)\b/i.test(message) ||
					/\b(research\s*this|look\s*into|dig\s*into|investigate)\b/i.test(message);

				const timeoutFn = (ms: number) => new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), ms));
				let consensus = '';
				let totalSources = 0;
				let validResults: { id: string; name: string; content: string; success: boolean }[] = [];

				// Helper: query Claude directly (with prompt caching for system prompt + multi-turn memory)
				const queryClaudeDirect = async (userContent: string): Promise<string> => {
					// Build messages: conversation history + current user message
					const msgs = [...conversationTurns, { role: 'user', content: userContent }];
					const claudeRes = await fetch('https://api.anthropic.com/v1/messages', {
						method: 'POST',
						headers: {
							'Content-Type': 'application/json',
							'x-api-key': env.ANTHROPIC_API_KEY,
							'anthropic-version': '2023-06-01',
							'anthropic-beta': 'prompt-caching-2024-07-31'
						},
						body: JSON.stringify({
							model: 'claude-sonnet-4-20250514',
							max_tokens: 4000,
							temperature: 0.15,
							system: [
								{
									type: 'text',
									text: synthiaSystemPrompt,
									cache_control: { type: 'ephemeral' }
								}
							],
							messages: msgs
						})
					});
					const claudeData = await claudeRes.json() as any;
					if (claudeData.error) {
						console.error('Claude API error:', JSON.stringify(claudeData.error));
						throw new Error(claudeData.error.message || 'Claude API error');
					}
					// Log cache performance
					if (claudeData.usage) {
						const cached = claudeData.usage.cache_read_input_tokens || 0;
						const created = claudeData.usage.cache_creation_input_tokens || 0;
						if (cached > 0) console.log(`Claude prompt cache HIT: ${cached} tokens read from cache`);
						if (created > 0) console.log(`Claude prompt cache WRITE: ${created} tokens cached for future calls`);
					}
					return claudeData.content?.[0]?.text || '';
				};

				// Helper: query Grok directly (second voice — also multi-turn)
				const queryGrokDirect = async (userContent: string): Promise<string> => {
					if (!env.XAI_API_KEY) return '';
					const grokMsgs: { role: string; content: string }[] = [
						{ role: 'system', content: synthiaSystemPrompt },
						...conversationTurns,
						{ role: 'user', content: userContent }
					];
					const r = await fetch('https://api.x.ai/v1/chat/completions', {
						method: 'POST',
						headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.XAI_API_KEY}` },
						body: JSON.stringify({
							model: 'grok-3', temperature: 0.15,
							messages: grokMsgs,
							max_tokens: 4000
						})
					});
					const d = await r.json() as any;
					return d.choices?.[0]?.message?.content || '';
				};

				// Role anchor — prepended to every user message to prevent mid-conversation drift
				const roleAnchor = `[SYNTHIA: You have mastered the desktop data. Identify your hat (Secretary/Paralegal/Attorney) for this task. Execute using context — do NOT ask questions you can answer yourself. "C"/"Cs"=Client's. One match=use it.${inferredClient ? ` Active client context: ${inferredClient}.` : ''}]\n\n`;

				if (needsResearch) {
					// WIDE END: Fan out to research AIs, Claude synthesizes
					console.log('Funnel wide → dispatching to research AIs');
					const researchPromises = researchModels.map(async (model) => {
						try {
							const content = await Promise.race([model.fn(), timeoutFn(25000)]) as string;
							return { id: model.id, name: model.name, content, success: true };
						} catch (e: any) {
							console.error(`Research model ${model.id} failed:`, e.message);
							return { id: model.id, name: model.name, content: '', success: false };
						}
					});

					const allResults = await Promise.all(researchPromises);
					validResults = allResults.filter(r => r.success && r.content.length > 10);

					if (!env.ANTHROPIC_API_KEY) {
						consensus = validResults[0]?.content || 'No AI services available.';
						totalSources = validResults.length;
					} else {
						let synthesisInput = '';
						if (validResults.length > 0) {
							synthesisInput = '\n\n## Research from other AI models (synthesize these into one answer):\n';
							for (const r of validResults) {
								synthesisInput += `\n### ${r.name}:\n${r.content.substring(0, 3000)}\n`;
							}
						}
						try {
							consensus = await queryClaudeDirect(roleAnchor + message + synthesisInput);
							totalSources = validResults.length + 1;
							if (!consensus) {
								consensus = validResults[0]?.content || 'AI synthesis failed.';
							}
						} catch (claudeErr: any) {
							console.error('Claude RAID driver error:', claudeErr.message);
							// Grok is second voice
							try {
								consensus = await queryGrokDirect(roleAnchor + message + synthesisInput);
								totalSources = validResults.length + 1;
							} catch {
								consensus = validResults[0]?.content || 'AI synthesis error.';
							}
						}
					}
				} else {
					// NARROW NECK: Claude handles directly
					console.log('Funnel narrow → Claude direct');
					if (env.ANTHROPIC_API_KEY) {
						try {
							consensus = await queryClaudeDirect(roleAnchor + message);
							totalSources = 1;
						} catch (claudeErr: any) {
							console.error('Claude direct error:', claudeErr.message);
							// Grok is second voice
							try {
								consensus = await queryGrokDirect(roleAnchor + message);
								totalSources = 1;
							} catch {
								// Last resort: fan out
								const researchPromises = researchModels.map(async (model) => {
									try {
										const content = await Promise.race([model.fn(), timeoutFn(25000)]) as string;
										return { id: model.id, name: model.name, content, success: true };
									} catch (e: any) {
										return { id: model.id, name: model.name, content: '', success: false };
									}
								});
								const allResults = await Promise.all(researchPromises);
								validResults = allResults.filter(r => r.success && r.content.length > 10);
								consensus = validResults[0]?.content || 'All AI services unavailable.';
								totalSources = validResults.length;
							}
						}
					} else {
						// No Claude key — Grok direct, then fan out
						try {
							consensus = await queryGrokDirect(roleAnchor + message);
							totalSources = 1;
						} catch {
							consensus = 'No AI services available. Please configure API keys.';
						}
					}
				}

				// --- STEP 5b: Handle email send — Graph (Associate@) default, Gmail (esqslaw) when specified ---
				let emailActionResult: any = null;
				if (emailAction === 'compose' && consensus) {
					try {
						// Parse email details from AI response
						const subjectMatch = consensus.match(/\*?\*?Subject:\*?\*?\s*(.+?)(?:\n|$)/i);
						const bodyStart = consensus.match(/(?:Dear|Hi|Hello|Good (?:morning|afternoon|evening))\s+[^\n]+\n/i);
						const bodyEnd = consensus.match(/(?:Best regards|Sincerely|Thank you|Regards|Respectfully),?\s*\n/i);

						if (subjectMatch && bodyStart) {
							const subject = subjectMatch[1].trim();
							const startIdx = bodyStart.index!;
							const endIdx = bodyEnd ? bodyEnd.index! + bodyEnd[0].length : consensus.length;
							const body = consensus.substring(startIdx, endIdx).trim();

							// Find recipient email from AI response
							let recipientEmail = '';
							const emailInResponse = consensus.match(/[\w.+-]+@[\w.-]+\.\w+/);
							if (emailInResponse && !emailInResponse[0].includes('dianepitcher') && !emailInResponse[0].includes('esqslaw')) {
								recipientEmail = emailInResponse[0];
							}

							// Send via Graph API from Associate@ (SendAs permission granted in Exchange)
							if (recipientEmail) {
								try {
									const token = await getGraphToken();
									const mailPayload: any = {
										message: {
											subject,
											body: { contentType: 'HTML', content: body.replace(/\n/g, '<br>') },
											toRecipients: [{ emailAddress: { address: recipientEmail } }],
											ccRecipients: [{ emailAddress: { address: 'pd@dianepitcher.com' } }],
											from: { emailAddress: { address: 'Associate@dianepitcher.com', name: 'Pitcher Law PLLC' } }
										},
										saveToSentItems: true
									};
									const sendRes = await fetch('https://graph.microsoft.com/v1.0/me/sendMail', {
										method: 'POST',
										headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
										body: JSON.stringify(mailPayload)
									});
									if (sendRes.status === 202 || sendRes.status === 200) {
										emailActionResult = { type: 'compose', status: 'sent', from: 'Associate@dianepitcher.com', to: recipientEmail, cc: 'pd@dianepitcher.com', subject };
										console.log(`Email SENT via Graph: sender=Associate@ to=${recipientEmail} subject="${subject}"`);
									} else {
										const errBody = await sendRes.text();
										console.error(`Graph send failed (${sendRes.status}):`, errBody.substring(0, 200));
										// Fallback to Gmail
										const gmailResult = await sendViaGmail(recipientEmail, subject, body, 'pd@dianepitcher.com');
										emailActionResult = gmailResult.success
											? { type: 'compose', status: 'sent', from: 'esqslaw@gmail.com', to: recipientEmail, cc: 'pd@dianepitcher.com', subject, note: 'Graph failed, sent via Gmail fallback' }
											: { type: 'compose', status: 'send_failed', error: gmailResult.error, to: recipientEmail };
									}
								} catch (graphErr: any) {
									console.error('Graph send error, trying Gmail fallback:', graphErr.message);
									const gmailResult = await sendViaGmail(recipientEmail, subject, body, 'pd@dianepitcher.com');
									emailActionResult = gmailResult.success
										? { type: 'compose', status: 'sent', from: 'esqslaw@gmail.com', to: recipientEmail, cc: 'pd@dianepitcher.com', subject, note: 'Graph failed, sent via Gmail fallback' }
										: { type: 'compose', status: 'send_failed', error: gmailResult.error, to: recipientEmail };
								}
							} else {
								emailActionResult = { type: 'compose', status: 'no_recipient', note: 'Could not determine recipient email address. Email drafted but not sent.', subject };
								console.warn('Email compose: no recipient email found in AI response or context');
							}
						} else {
							emailActionResult = { type: 'compose', status: 'parse_failed', note: 'Could not parse email structure from AI response.' };
						}
					} catch (emailErr: any) {
						console.error('Email send error:', emailErr.message);
						emailActionResult = { type: 'compose', status: 'error', error: emailErr.message };
					}
				} else if (emailAction === 'archive' && inferredClient) {
					emailActionResult = { type: 'archive', status: 'pending', note: `Archive emails for ${inferredClient} — use POST /api/email/archive endpoint to execute.` };
				} else if (emailAction === 'read') {
					emailActionResult = { type: 'read', status: 'complete', note: 'Email context included in AI response.' };
				}

				// --- STEP 6: Cache the result ---
				const confidence = totalSources >= 4 ? 0.95 : totalSources >= 2 ? 0.85 : totalSources >= 1 ? 0.7 : 0.5;
				const result: any = {
					success: true,
					consensus,
					sources: totalSources,
					confidence,
					operationalAIs: validResults.length + (env.ANTHROPIC_API_KEY ? 1 : 0),
					totalAIs: researchModels.length + (env.ANTHROPIC_API_KEY ? 1 : 0),
					researchModels: validResults.map(r => r.name),
					...(emailActionResult && { emailAction: emailActionResult }),
					...(intelLogResult && { intelLog: intelLogResult }),
					...(inferredClient && { activeClient: inferredClient })
				};

				// Cache in KV (24h for general, 1h for case-specific) — skip for actions, emails, alerts
				const ttl = message.toLowerCase().match(/case|client|hearing|deadline|motion/) ? 3600 : 86400;
				if (!emailAction && !isAlertMessage && !isActionMessage && !intelLogResult) {
					ctx.waitUntil(env.CACHE.put(cacheKey, JSON.stringify(result), { expirationTtl: ttl }));
				}

				// --- STEP 7: Store in chat history (continuous thread) ---
				// ALWAYS use synthia_master — one permanent thread across all devices
				ctx.waitUntil((async () => {
					try {
						await env.DB.prepare(`INSERT INTO chat_messages (session_id, role, content) VALUES ('synthia_master', 'user', ?)`).bind(message).run();
						await env.DB.prepare(`INSERT INTO chat_messages (session_id, role, content) VALUES ('synthia_master', 'assistant', ?)`).bind(consensus).run();

						// --- STEP 7a-RAG: Store exchange as memory chunk ---
						try {
							const chunkId = `chat_${Date.now()}`;
							const chunkContent = `User: ${message}\nSynthia: ${consensus.substring(0, 2500)}`;
							await ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
								id: chunkId, type: 'conversation', source: 'chat',
								content: chunkContent, clientName: inferredClient || '',
							});
						} catch (ragStoreErr: any) {
							console.error('RAG store error:', ragStoreErr.message);
						}

						// --- STEP 7b: Auto-summarize if messages exceed threshold ---
						// Every 80+ unsummarized messages, compress oldest 50 into a summary
						const countRes = await env.DB.prepare(
							`SELECT COUNT(*) as cnt FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant')`
						).first() as any;
						if ((countRes?.cnt || 0) > 80 && env.ANTHROPIC_API_KEY) {
							const { results: oldest } = await env.DB.prepare(
								`SELECT id, role, content, timestamp FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant') ORDER BY timestamp ASC LIMIT 50`
							).all();
							if (oldest?.length >= 50) {
								// Preserve turn structure with timestamps for better recall
								const text = (oldest as any[]).map(m => `[${(m.timestamp as string || '').substring(0, 16)} ${m.role.toUpperCase()}]: ${(m.content || '').substring(0, 500)}`).join('\n\n');
								const sRes = await fetch('https://api.anthropic.com/v1/messages', {
									method: 'POST',
									headers: { 'Content-Type': 'application/json', 'x-api-key': env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
									body: JSON.stringify({
										model: 'claude-sonnet-4-20250514', max_tokens: 1200, temperature: 0,
										messages: [{ role: 'user', content: `Compress this conversation block into a detailed internal memory note (max 800 words). This is private — NOT public.\n\nSTRUCTURE YOUR SUMMARY AS:\n\n## Topics Discussed\n- Topic 1: what was discussed, what was decided, key details\n- Topic 2: ...\n\n## Client Matters Referenced\n- Client Name (case #): what was discussed about their case, any decisions, tasks\n\n## Actions Taken\n- What was completed (specific details: files, endpoints, emails sent, documents drafted)\n- What's pending or was deferred\n\n## Key Facts & Decisions\n- Concrete facts, dates, names, numbers that should be remembered\n- Decisions made and reasoning\n\nPreserve ALL client names, case numbers, dates, and specific details. These summaries are the AI's long-term memory — vague summaries are useless.\n\n${text}` }]
									})
								});
								const sData = await sRes.json() as any;
								const summary = sData.content?.[0]?.text;
								if (summary) {
									await env.MEMORY_DB.prepare(
										`INSERT INTO sessions (id, summary, started_at) VALUES (?, ?, ?)`
									).bind(`summary_${Date.now()}`, summary, mtnISO()).run();
									const oldIds = (oldest as any[]).map(m => m.id);
									await env.DB.prepare(
										`DELETE FROM chat_messages WHERE id IN (${oldIds.map(() => '?').join(',')}) AND session_id = 'synthia_master'`
									).bind(...oldIds).run();
									await env.DB.prepare(
										`INSERT INTO chat_messages (session_id, role, content) VALUES ('synthia_master', 'summary', ?)`
									).bind(`📋 Auto-Summary: ${summary.substring(0, 500)}`).run();
									console.log(`Auto-summarized ${oldest.length} messages`);
								}
							}
						}
					} catch (e: any) {
						console.error('Chat storage error:', e.message);
					}
				})());

				return json(result);
			}

			// ═══════════════════════════════════════════════════════════════
			// CHAT (Continuous Thread)
			// ═══════════════════════════════════════════════════════════════

			// Get continuous thread with topic markers
			if (path === '/api/chat/thread' && request.method === 'GET') {
				const limit = parseInt(url.searchParams.get('limit') || '100');
				const before = url.searchParams.get('before'); // timestamp for pagination
				let q = `SELECT content, role, timestamp FROM chat_messages WHERE session_id = 'synthia_master'`;
				const p: any[] = [];
				if (before) { q += ' AND timestamp < ?'; p.push(before); }
				q += ' ORDER BY timestamp DESC LIMIT ?';
				p.push(limit);
				const { results } = await env.DB.prepare(q).bind(...p).all();
				// Return chronological (oldest first)
				const messages = (results as any[]).reverse();
				return json({ success: true, messages, hasMore: results.length === limit });
			}

			// Insert topic marker (visual "new chat" divider)
			if (path === '/api/chat/topic' && request.method === 'POST') {
				const { label } = await request.json() as any;
				const topicLabel = label || `New Topic — ${new Date().toLocaleString('en-US', { timeZone: 'America/Denver' })}`;
				await env.DB.prepare(
					`INSERT INTO chat_messages (session_id, role, content) VALUES ('synthia_master', 'topic_marker', ?)`
				).bind(topicLabel).run();
				return json({ success: true, topic: topicLabel });
			}

			// List all topic markers (for sidebar navigation)
			if (path === '/api/chat/topics' && request.method === 'GET') {
				const { results } = await env.DB.prepare(
					`SELECT content, timestamp FROM chat_messages WHERE session_id = 'synthia_master' AND role = 'topic_marker' ORDER BY timestamp DESC LIMIT 50`
				).all();
				return json({ success: true, topics: results });
			}

			// --- RAG Memory Endpoints ---

			// GET /api/rag/status — chunk counts by type
			if (path === '/api/rag/status' && request.method === 'GET') {
				const { results } = await env.DB.prepare(
					`SELECT chunk_type, COUNT(*) as cnt FROM memory_chunks GROUP BY chunk_type`
				).all();
				const total = (results as any[]).reduce((s: number, r: any) => s + r.cnt, 0);
				return json({ success: true, total, by_type: results });
			}

			// GET /api/gmail/oauth — start OAuth flow
			if (path === '/api/gmail/oauth' && request.method === 'GET') {
				const clientId = env.GOOGLE_OAUTH_CLIENT_ID;
				const redirectUri = 'https://api.esqs-law.com/api/gmail/callback';
				const authUrl = 'https://accounts.google.com/o/oauth2/v2/auth?' + new URLSearchParams({
					client_id: clientId,
					redirect_uri: redirectUri,
					response_type: 'code',
					scope: 'https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.modify https://www.googleapis.com/auth/drive.readonly',
					access_type: 'offline',
					prompt: 'consent',
					login_hint: 'esqslaw@gmail.com'
				}).toString();
				return Response.redirect(authUrl, 302);
			}

			// GET /api/gmail/callback — OAuth callback, exchange code for refresh token
			if (path === '/api/gmail/callback' && request.method === 'GET') {
				const code = url.searchParams.get('code');
				if (!code) return json({ success: false, error: 'No code in callback' }, 400);
				const clientId = env.GOOGLE_OAUTH_CLIENT_ID;
				const clientSecret = env.GOOGLE_OAUTH_CLIENT_SECRET;
				const redirectUri = 'https://api.esqs-law.com/api/gmail/callback';
				const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
					method: 'POST',
					headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
					body: new URLSearchParams({
						code,
						client_id: clientId,
						client_secret: clientSecret,
						redirect_uri: redirectUri,
						grant_type: 'authorization_code'
					})
				});
				const tokenData = await tokenRes.json() as any;
				if (tokenData.refresh_token) {
					return json({
						success: true,
						message: 'Got refresh token! Run the command below to save it as a Worker secret.',
						command: `echo ${tokenData.refresh_token} | npx wrangler secret put GOOGLE_REFRESH_TOKEN`,
						refresh_token: tokenData.refresh_token,
					});
				}
				return json({ success: false, error: 'No refresh_token in response', data: tokenData });
			}

			// GET /api/gmail/test — verify Gmail OAuth and read capability
			if (path === '/api/gmail/test' && request.method === 'GET') {
				try {
					// Diagnostic: check if env vars are set
					const hasClientId = !!env.GOOGLE_OAUTH_CLIENT_ID;
					const hasClientSecret = !!env.GOOGLE_OAUTH_CLIENT_SECRET;
					const hasRefreshToken = !!env.GOOGLE_REFRESH_TOKEN;
					const refreshLen = (env.GOOGLE_REFRESH_TOKEN || '').length;
					if (!hasRefreshToken) {
						return json({ success: false, error: 'GOOGLE_REFRESH_TOKEN not set', hasClientId, hasClientSecret, hasRefreshToken, refreshLen });
					}
					const token = await getGmailToken();
					const q = url.searchParams.get('q') || 'newer_than:3d';
					const listRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages?maxResults=3&q=${encodeURIComponent(q)}`, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const listData = await listRes.json() as any;
					if (!listData.messages?.length) return json({ success: true, token: 'valid', query: q, messages: 0 });
					const previews: any[] = [];
					for (const m of listData.messages.slice(0, 3)) {
						const msgRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${m.id}?format=metadata&metadataHeaders=Subject&metadataHeaders=From&metadataHeaders=Date`, {
							headers: { 'Authorization': `Bearer ${token}` }
						});
						const msg = await msgRes.json() as any;
						const headers = msg.payload?.headers || [];
						previews.push({
							id: m.id,
							from: headers.find((h: any) => h.name === 'From')?.value || '?',
							subject: headers.find((h: any) => h.name === 'Subject')?.value || '?',
							date: headers.find((h: any) => h.name === 'Date')?.value || '?',
						});
					}
					return json({ success: true, token: 'valid', query: q, messages: listData.messages.length, previews });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// GET /api/google/token-info — debug token scopes
			if (path === '/api/google/token-info' && request.method === 'GET') {
				try {
					const token = await getGmailToken();
					const infoRes = await fetch(`https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=${token}`);
					const info = await infoRes.json() as any;
					return json({ success: true, scope: info.scope, email: info.email, expires_in: info.expires_in });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// POST /api/rag/seed?source=judges|oc|cases|sessions|chat&offset=0
			// Paginated seeder — call per source to stay under subrequest limits
			if (path === '/api/rag/seed' && request.method === 'POST') {
				const source = url.searchParams.get('source') || 'judges';
				const offset = parseInt(url.searchParams.get('offset') || '0');
				const BATCH = 12; // ~3 subrequests each = 36, well under 50
				let seeded = 0;
				let total = 0;

				try {
					if (source === 'judges') {
						const { results: judges } = await env.MEMORY_DB.prepare('SELECT * FROM judge_intel').all();
						total = judges?.length || 0;
						for (const j of ((judges || []) as any[]).slice(offset, offset + BATCH)) {
							const content = `Judge: ${j.judge_name}, Court: ${j.court}${j.district ? ', ' + j.district : ''}. Tendencies: ${j.tendencies || 'N/A'}. Sentencing: ${j.sentencing_patterns || 'N/A'}. Motions: ${j.motion_preferences || 'N/A'}. Plea deals: ${j.plea_disposition || 'N/A'}. Win rate: ${j.win_rate || 'N/A'}. JA: ${j.ja_name || 'N/A'}. Notes: ${j.notes || ''}`;
							await ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
								id: `judge_${j.judge_name?.replace(/\s+/g, '_').toLowerCase() || seeded}`,
								type: 'case_knowledge', source: 'judge_intel', content,
							});
							seeded++;
						}
					} else if (source === 'oc') {
						const { results: counsel } = await env.MEMORY_DB.prepare('SELECT * FROM opposing_counsel_intel').all();
						total = counsel?.length || 0;
						for (const oc of ((counsel || []) as any[]).slice(offset, offset + BATCH)) {
							const content = `Opposing Counsel: ${oc.counsel_name}${oc.firm ? ', ' + oc.firm : ''}. Bar: ${oc.bar_number || 'N/A'}. Practice: ${oc.practice_areas || 'N/A'}. Negotiation style: ${oc.negotiation_style || 'N/A'}. Litigation: ${oc.litigation_tendencies || 'N/A'}. Strengths: ${oc.strengths || 'N/A'}. Weaknesses: ${oc.weaknesses || 'N/A'}. Cases against: ${oc.cases_against || 'N/A'}. Outcomes: ${oc.outcomes || 'N/A'}. Notes: ${oc.notes || ''}`;
							await ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
								id: `oc_${oc.counsel_name?.replace(/\s+/g, '_').toLowerCase() || seeded}`,
								type: 'case_knowledge', source: 'oc_intel', content,
							});
							seeded++;
						}
					} else if (source === 'cases') {
						const { results: cases } = await env.MEMORY_DB.prepare(`SELECT * FROM case_summaries WHERE status = 'active'`).all();
						total = cases?.length || 0;
						for (const cs of ((cases || []) as any[]).slice(offset, offset + BATCH)) {
							const content = `Case: ${cs.client_name} (${cs.case_number}), ${cs.case_type} at ${cs.court}. Role: ${cs.client_role} vs ${cs.opposing_party}. Judge: ${cs.judge}. Summary: ${cs.summary || 'N/A'}. Next event: ${cs.next_event || 'None'} on ${cs.next_event_date || 'TBD'}.`;
							await ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
								id: `case_${cs.case_number || seeded}`,
								type: 'case_knowledge', source: 'case_summary', content,
								clientName: cs.client_name || '', caseNumber: cs.case_number || '',
							});
							seeded++;
						}
					} else if (source === 'sessions') {
						const { results: sessions } = await env.MEMORY_DB.prepare(
							`SELECT id, summary, started_at FROM sessions WHERE summary IS NOT NULL ORDER BY started_at ASC LIMIT ? OFFSET ?`
						).bind(BATCH, offset).all();
						total = (await env.MEMORY_DB.prepare(`SELECT COUNT(*) as cnt FROM sessions WHERE summary IS NOT NULL`).first() as any)?.cnt || 0;
						for (const s of (sessions || []) as any[]) {
							await ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
								id: `summary_${s.id}`,
								type: 'conversation', source: 'session_summary',
								content: `[${s.started_at}] ${(s.summary as string).substring(0, 2000)}`,
							});
							seeded++;
						}
					} else if (source === 'chat') {
						const { results: msgs } = await env.DB.prepare(
							`SELECT role, content, timestamp FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant') ORDER BY timestamp ASC LIMIT ? OFFSET ?`
						).bind(BATCH * 2, offset).all();
						total = (await env.DB.prepare(`SELECT COUNT(*) as cnt FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant')`).first() as any)?.cnt || 0;
						const msgList = msgs as any[];
						for (let i = 0; i < msgList.length - 1; i += 2) {
							if (msgList[i].role === 'user' && msgList[i + 1]?.role === 'assistant') {
								const content = `User: ${msgList[i].content.substring(0, 800)}\nSynthia: ${msgList[i + 1].content.substring(0, 1200)}`;
								await ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
									id: `chat_hist_${offset + i}`,
									type: 'conversation', source: 'chat', content,
								});
								seeded++;
							}
						}
					}
				} catch (e: any) {
					return json({ success: false, source, offset, seeded, error: e.message });
				}

				const nextOffset = offset + (source === 'chat' ? BATCH * 2 : BATCH);
				const done = nextOffset >= total;
				return json({ success: true, source, seeded, total, offset, done, nextOffset: done ? null : nextOffset });
			}

			// Export full session for local backup (D1 → JSON dump)
			if (path === '/api/chat/export' && request.method === 'GET') {
				try {
					const since = url.searchParams.get('since'); // ISO timestamp for incremental sync
					let q = `SELECT id, session_id, role, content, timestamp FROM chat_messages WHERE session_id = 'synthia_master'`;
					const p: any[] = [];
					if (since) { q += ' AND timestamp > ?'; p.push(since); }
					q += ' ORDER BY timestamp ASC';
					const { results: messages } = await env.DB.prepare(q).bind(...p).all();

					// Also export ALL session summaries from MEMORY_DB (manual + auto)
					let sq = `SELECT id, summary, started_at, ended_at, topics, pending_items FROM sessions WHERE summary IS NOT NULL`;
					const sp: any[] = [];
					if (since) { sq += ' AND started_at > ?'; sp.push(since); }
					sq += ' ORDER BY started_at ASC';
					const { results: summaries } = await env.MEMORY_DB.prepare(sq).bind(...sp).all();

					// Export party_cache snapshot
					const { results: parties } = await env.MEMORY_DB.prepare(
						`SELECT * FROM party_cache ORDER BY last_verified DESC`
					).all();

					// Export deadlines snapshot
					const { results: deadlines } = await env.MEMORY_DB.prepare(
						`SELECT * FROM deadlines WHERE status IN ('active', 'pending') ORDER BY due_date ASC`
					).all();

					return json({
						success: true,
						exported_at: mtnISO(),
						since: since || null,
						messages: messages || [],
						summaries: summaries || [],
						party_cache: parties || [],
						deadlines: deadlines || [],
						counts: {
							messages: messages?.length || 0,
							summaries: summaries?.length || 0,
							parties: parties?.length || 0,
							deadlines: deadlines?.length || 0
						}
					});
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// Get upcoming deadlines/calendar for always-on display
			if (path === '/api/chat/deadlines' && request.method === 'GET') {
				const limit = parseInt(url.searchParams.get('limit') || '50');
				const includePast = url.searchParams.get('include_past') === 'true';
				try {
					let q = `SELECT client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, case_url, virtual_link, court_address, court_phone, status, source FROM deadlines WHERE status IN ('active', 'pending')`;
					const params: any[] = [];
					if (!includePast) { q += ` AND due_date >= ?`; params.push(mtnToday()); }
					q += ` ORDER BY due_date ASC, hearing_time ASC LIMIT ?`;
					params.push(limit);
					const { results } = await env.MEMORY_DB.prepare(q).bind(...params).all();
					return json({ success: true, deadlines: results });
				} catch (e: any) {
					return json({ success: true, deadlines: [], error: e.message });
				}
			}

			// Change detection alerts (polled by dashboard)
			if (path === '/api/alerts/pending' && request.method === 'GET') {
				try {
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT id, task_type, description, created_at FROM pending_tasks WHERE status = 'pending' AND task_type IN ('schedule_change', 'new_hearings') ORDER BY created_at DESC LIMIT 20`
					).all();
					// Mark as read
					if (results.length > 0) {
						const ids = (results as any[]).map(r => r.id).join(',');
						await env.MEMORY_DB.prepare(`UPDATE pending_tasks SET status = 'completed' WHERE id IN (${ids})`).run();
					}
					return json({ success: true, alerts: results });
				} catch (e: any) {
					return json({ success: true, alerts: [], error: e.message });
				}
			}

			// Case summaries (stored in D1, generated by scripts/generate-case-summaries.js)
			if (path === '/api/case-summaries' && request.method === 'GET') {
				try {
					const clientName = url.searchParams.get('client');
					const withEvents = url.searchParams.get('upcoming') === 'true';
					let q = `SELECT * FROM case_summaries WHERE status = 'active'`;
					const binds: any[] = [];
					if (clientName) {
						// Fuzzy: "Buttars, Garrett" → match "GARRETT SYVERINE BUTTARS"
						const parts = clientName.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter((p: string) => p.length >= 2);
						if (parts.length > 0) {
							parts.forEach((p: string) => { q += ` AND LOWER(client_name) LIKE ?`; binds.push('%' + p.toLowerCase() + '%'); });
						} else {
							q += ` AND client_name LIKE ?`;
							binds.push('%' + clientName + '%');
						}
						// If fuzzy finds nothing, try misspelling fallback
						const testStmt = env.MEMORY_DB.prepare(q);
						const testResult = binds.length > 0 ? await testStmt.bind(...binds).all() : await testStmt.all();
						if (testResult.results.length === 0 && parts.length > 0) {
							// Levenshtein fallback
							const { results: allCsNames } = await env.MEMORY_DB.prepare(`SELECT DISTINCT client_name FROM case_summaries WHERE status='active'`).all();
							const matched = (allCsNames as any[]).filter(r => {
								const cp = (r.client_name as string).toLowerCase().replace(/[^a-z\s]/g,'').split(/\s+/).filter((x:string)=>x.length>=2);
								let hits=0;
								for(const sp of parts){const sl=sp.toLowerCase();for(const c of cp){const th=Math.min(sl.length,c.length)<=4?1:2;if(c.includes(sl)||sl.includes(c)||levenshtein(sl,c)<=th){hits++;break;}}}
								return hits>=parts.length;
							});
							if (matched.length > 0) {
								const names = matched.map(r=>`'${(r.client_name as string).replace(/'/g,"''")}'`).join(',');
								q = `SELECT * FROM case_summaries WHERE status = 'active' AND client_name IN (${names})`;
								if (withEvents) q += ` AND next_event_date IS NOT NULL AND next_event_date >= '${mtnToday()}'`;
								q += ` ORDER BY next_event_date ASC NULLS LAST, client_name ASC`;
								binds.length = 0; // clear binds since names are inlined
							}
						}
					}
					if (withEvents) {
						q += ` AND next_event_date IS NOT NULL AND next_event_date >= '${mtnToday()}'`;
					}
					q += ` ORDER BY next_event_date ASC NULLS LAST, client_name ASC`;
					const stmt = env.MEMORY_DB.prepare(q);
					const { results } = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
					return json({ success: true, summaries: results, count: results.length });
				} catch (e: any) {
					return json({ success: false, summaries: [], error: e.message });
				}
			}

			// GET /api/case-summaries/deep-scan/status — Check routine scan progress
			if (path === '/api/case-summaries/deep-scan/status' && request.method === 'GET') {
				const stateRaw = await env.CACHE.get('deep-scan-state');
				const state = stateRaw ? JSON.parse(stateRaw) : { offset: 0, last_run: 'never', total_scanned: 0, total_updated: 0, cycle: 0 };
				const countRow = await env.MEMORY_DB.prepare(`SELECT COUNT(*) as cnt FROM case_summaries WHERE status = 'active'`).first() as any;
				const totalActive = countRow?.cnt || 0;
				const needsData = await env.MEMORY_DB.prepare(
					`SELECT COUNT(*) as cnt FROM case_summaries WHERE status = 'active' AND (facts IS NULL OR facts = '' OR opposing_counsel IS NULL OR opposing_counsel = '')`
				).first() as any;
				return json({
					success: true,
					scan_state: state,
					total_active_cases: totalActive,
					cases_needing_data: needsData?.cnt || 0,
					progress_pct: totalActive > 0 ? Math.round((state.offset / totalActive) * 100) : 0,
					next_run: 'Cron: every 6 hours (0 */6 * * *)',
				});
			}

			// GET /api/case-summaries/deep-scan/tokens — Local script token proxy
			// Returns fresh Graph + Google tokens so local scripts don't need secrets
			if (path === '/api/case-summaries/deep-scan/tokens' && request.method === 'GET') {
				try {
					const graph = await getGraphToken();
					let google = '';
					try { google = await getGmailToken(); } catch {}
					return json({
						success: true,
						graph_token: graph,
						google_token: google,
						onedrive_folder_id: env.ONEDRIVE_FOLDER_ID || '',
						anthropic_key: env.ANTHROPIC_API_KEY || '',
						expires_in: 3000,
					});
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// GET /api/case-summaries/deep-scan/file-proxy?id=ITEM_ID&source=onedrive|gdrive
			// Downloads a file through the Worker so local script doesn't need Graph tokens
			if (path === '/api/case-summaries/deep-scan/file-proxy' && request.method === 'GET') {
				try {
					const fileId = url.searchParams.get('id');
					const source = url.searchParams.get('source') || 'onedrive';
					if (!fileId) return json({ success: false, error: 'id required' }, 400);

					if (source === 'gdrive') {
						const gToken = await getGmailToken();
						const res = await fetch(
							`https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`,
							{ headers: { 'Authorization': `Bearer ${gToken}` } }
						);
						return new Response(res.body, { headers: { 'Content-Type': res.headers.get('Content-Type') || 'application/octet-stream' } });
					} else {
						const token = await getGraphToken();
						const itemRes = await fetch(
							`https://graph.microsoft.com/v1.0/me/drive/items/${fileId}`,
							{ headers: { 'Authorization': `Bearer ${token}` } }
						);
						const itemData = await itemRes.json() as any;
						const dlUrl = itemData['@microsoft.graph.downloadUrl'];
						if (!dlUrl) return json({ success: false, error: 'No download URL' }, 404);
						const fileRes = await fetch(dlUrl);
						return new Response(fileRes.body, { headers: { 'Content-Type': fileRes.headers.get('Content-Type') || 'application/octet-stream' } });
					}
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Single case summary by client + case number
			if (path.startsWith('/api/case-summaries/') && request.method === 'GET') {
				try {
					const caseNumber = decodeURIComponent(path.replace('/api/case-summaries/', ''));
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT * FROM case_summaries WHERE case_number = ?`
					).bind(caseNumber).all();
					if (results.length === 0) {
						return json({ success: false, error: 'Not found' }, 404);
					}
					return json({ success: true, summary: results[0] });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// PATCH case summary — manual updates to facts, charges, notes, OC contact, deadlines, etc.
			if (path.startsWith('/api/case-summaries/') && request.method === 'PATCH') {
				try {
					const caseNumber = decodeURIComponent(path.replace('/api/case-summaries/', ''));
					const body = await request.json() as any;
					// Updatable fields — never overwrite auto-computed fields like summary, predictions unless explicitly sent
					const updatableFields = [
						'facts', 'charges', 'notes', 'client_email', 'client_phone', 'client_address',
						'opposing_counsel', 'opposing_counsel_phone', 'opposing_counsel_email', 'opposing_counsel_firm',
						'opposing_party', 'opposing_role', 'additional_parties',
						'discovery_deadline', 'dispositive_deadline', 'trial_date', 'statute_of_limitations',
						'case_type', 'court', 'district', 'judge', 'client_role', 'folder_url', 'status'
					];
					const sets: string[] = [];
					const binds: any[] = [];
					for (const field of updatableFields) {
						if (body[field] !== undefined) {
							sets.push(`${field} = ?`);
							binds.push(body[field]);
						}
					}
					if (sets.length === 0) return err('No updatable fields provided', 400);
					sets.push('updated_at = ?');
					binds.push(mtnToday());
					binds.push(caseNumber);
					// If client_name is provided, scope by both
					let where = 'case_number = ?';
					if (body.client_name) {
						where += ' AND client_name = ?';
						binds.push(body.client_name);
					}
					await env.MEMORY_DB.prepare(
						`UPDATE case_summaries SET ${sets.join(', ')} WHERE ${where}`
					).bind(...binds).run();
					// Return updated record
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT * FROM case_summaries WHERE case_number = ?`
					).bind(caseNumber).all();
					return json({ success: true, updated: results.length, summary: results[0] || null });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// Refresh/regenerate all case summaries from party_cache + deadlines + case_files + intel
			// Called after scrape, file sync, or manually from dashboard
			if (path === '/api/case-summaries/refresh' && request.method === 'POST') {
				try {
					const today = mtnToday();

					// 1. Load all party_cache entries (with client contact info)
					const { results: parties } = await env.MEMORY_DB.prepare(
						`SELECT client_name, case_number, case_type, client_role, opposing_party, opposing_role, opposing_counsel, judge, court, district, additional_parties, folder_url, client_email, client_phone, client_address FROM party_cache ORDER BY client_name`
					).all();

					// 2. Load ALL deadlines (upcoming + key milestone types even if past)
					const { results: deadlines } = await env.MEMORY_DB.prepare(
						`SELECT client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, status FROM deadlines WHERE status IN ('active','pending') AND due_date >= ? ORDER BY due_date ASC, hearing_time ASC`
					).bind(mtnToday()).all();

					// 3. Load file counts per client
					const { results: fileCounts } = await env.MEMORY_DB.prepare(
						`SELECT client_name, COUNT(*) as cnt FROM case_files WHERE source = 'open' GROUP BY client_name`
					).all();

					// 4. Load judge intel — activity logs for predictions
					let judgeLogs: any[] = [];
					try {
						const { results: jl } = await env.MEMORY_DB.prepare(
							`SELECT judge_name, activity_type, activity_subtype, outcome, party_role, date FROM judge_activity_log ORDER BY date DESC`
						).all();
						judgeLogs = jl as any[];
					} catch (_) {}

					// 5. Load OC intel — activity logs for predictions
					let ocLogs: any[] = [];
					try {
						const { results: ol } = await env.MEMORY_DB.prepare(
							`SELECT counsel_name, activity_type, activity_subtype, outcome, party_role, date FROM oc_activity_log ORDER BY date DESC`
						).all();
						ocLogs = ol as any[];
					} catch (_) {}

					// 6. Load judge reversal factors
					let reversals: any[] = [];
					try {
						const { results: rf } = await env.MEMORY_DB.prepare(
							`SELECT judge_name, activity_type, typical_outcome, actual_outcome, reversal_factors, specific_arguments FROM judge_ruling_rationale ORDER BY date DESC`
						).all();
						reversals = rf as any[];
					} catch (_) {}

					// 7. Load existing case_summaries to preserve manually-entered fields
					const { results: existingSummaries } = await env.MEMORY_DB.prepare(
						`SELECT client_name, case_number, facts, charges, notes, opposing_counsel_phone, opposing_counsel_email, opposing_counsel_firm, discovery_deadline, dispositive_deadline, trial_date, statute_of_limitations FROM case_summaries`
					).all();
					const existingMap: Record<string, any> = {};
					for (const es of existingSummaries as any[]) {
						existingMap[`${es.client_name}|${es.case_number}`] = es;
					}

					// Index deadlines by case_number and client_name
					const dlByCase: Record<string, any[]> = {};
					const dlByName: Record<string, any[]> = {};
					for (const d of deadlines as any[]) {
						if (d.case_number) { if (!dlByCase[d.case_number]) dlByCase[d.case_number] = []; dlByCase[d.case_number].push(d); }
						const n = (d.client_name as string).toUpperCase().trim();
						if (!dlByName[n]) dlByName[n] = []; dlByName[n].push(d);
					}

					// Index file counts
					const fileMap: Record<string, number> = {};
					for (const f of fileCounts as any[]) { fileMap[f.client_name] = f.cnt; }

					// Index judge logs by judge name (lowered)
					const judgeLogsByName: Record<string, any[]> = {};
					for (const jl of judgeLogs) {
						const jn = (jl.judge_name || '').toLowerCase();
						if (!judgeLogsByName[jn]) judgeLogsByName[jn] = [];
						judgeLogsByName[jn].push(jl);
					}

					// Index OC logs by counsel name (lowered)
					const ocLogsByName: Record<string, any[]> = {};
					for (const ol of ocLogs) {
						const cn = (ol.counsel_name || '').toLowerCase();
						if (!ocLogsByName[cn]) ocLogsByName[cn] = [];
						ocLogsByName[cn].push(ol);
					}

					// Index reversals by judge name
					const reversalsByJudge: Record<string, any[]> = {};
					for (const r of reversals) {
						const jn = (r.judge_name || '').toLowerCase();
						if (!reversalsByJudge[jn]) reversalsByJudge[jn] = [];
						reversalsByJudge[jn].push(r);
					}

					let upserted = 0, errors = 0;
					const batch: any[] = [];

					for (const p of parties as any[]) {
						const name = p.client_name as string;
						const caseNum = p.case_number as string;
						const existing = existingMap[`${name}|${caseNum}`] || {};

						// Match deadlines
						let caseDl = dlByCase[caseNum] || [];
						if (caseDl.length === 0) caseDl = dlByName[name.toUpperCase().trim()] || [];

						const nextEv = caseDl.length > 0 ? caseDl[0] : null;
						const nextEvDesc = nextEv ? `${nextEv.deadline_type}${nextEv.hearing_time ? ' at ' + nextEv.hearing_time : ''} on ${nextEv.due_date}${nextEv.court ? ' — ' + nextEv.court : ''}` : null;
						const nextEvDate = nextEv?.due_date || null;

						// Fuzzy file count
						let fc = 0;
						const parts = name.split(' ');
						const last = parts[parts.length - 1]?.toUpperCase() || '';
						const first = parts[0]?.toUpperCase() || '';
						for (const [fn, cnt] of Object.entries(fileMap)) {
							const fnUp = fn.toUpperCase();
							if (fnUp.includes(last) && fnUp.includes(first)) { fc = cnt; break; }
							if (last.length > 2 && fnUp.includes(last + ',')) { fc = cnt; break; }
						}

						// === JUDGE PREDICTION ===
						let judgePred = '';
						if (p.judge) {
							const jn = (p.judge as string).toLowerCase();
							const jLogs = judgeLogsByName[jn] || [];
							if (jLogs.length >= 2) {
								const preds = computePredictions(jLogs);
								judgePred = preds.map(pr => {
									let line = `${pr.activity_type}${pr.activity_subtype ? '/' + pr.activity_subtype : ''}: ${pr.most_likely.toUpperCase()} ${pr.most_likely_pct}% (n=${pr.sample_size}, ${pr.confidence})`;
									if (pr.by_role) {
										for (const [role, rd] of Object.entries(pr.by_role)) {
											line += ` | ${rd.summary}`;
										}
									}
									return line;
								}).join('; ');
							}
						}

						// === OC PREDICTION ===
						let ocPred = '';
						if (p.opposing_counsel) {
							const cn = (p.opposing_counsel as string).toLowerCase();
							const oLogs = ocLogsByName[cn] || [];
							if (oLogs.length >= 2) {
								const preds = computePredictions(oLogs);
								ocPred = preds.map(pr =>
									`${pr.activity_type}${pr.activity_subtype ? '/' + pr.activity_subtype : ''}: ${pr.most_likely.toUpperCase()} ${pr.most_likely_pct}% (n=${pr.sample_size}, ${pr.confidence})`
								).join('; ');
							}
						}

						// === REVERSAL FACTORS ===
						let revFactors = '';
						if (p.judge) {
							const jn = (p.judge as string).toLowerCase();
							const jRevs = reversalsByJudge[jn] || [];
							if (jRevs.length > 0) {
								revFactors = jRevs.map(r => {
									const factors = typeof r.reversal_factors === 'string' ? r.reversal_factors : JSON.stringify(r.reversal_factors || []);
									return `${r.activity_type}: typically ${r.typical_outcome} but ${r.actual_outcome} — factors: ${factors}${r.specific_arguments ? ' (' + r.specific_arguments + ')' : ''}`;
								}).join('; ');
							}
						}

						// Build summary (richer with OC and judge intel)
						let s = `${p.case_type || 'Unknown'} case`;
						if (p.court) s += ` in ${p.court}`;
						s += `. ${name} is the ${p.client_role || 'party'}.`;
						if (p.opposing_party) { s += ` Opposing: ${p.opposing_party}`; if (p.opposing_role) s += ` (${p.opposing_role})`; s += '.'; }
						if (p.opposing_counsel) s += ` Opposing counsel: ${p.opposing_counsel}.`;
						if (p.judge) s += ` Judge: ${p.judge}.`;
						if (caseDl.length > 0) {
							s += ` ${caseDl.length} upcoming event${caseDl.length > 1 ? 's' : ''}.`;
							s += ` Next: ${caseDl[0].deadline_type}`;
							if (caseDl[0].hearing_time) s += ` at ${caseDl[0].hearing_time}`;
							s += ` on ${caseDl[0].due_date}`;
							if (caseDl[0].courtroom) s += ` in ${caseDl[0].courtroom}`;
							if (caseDl[0].hearing_mode) s += ` (${caseDl[0].hearing_mode})`;
							s += '.';
						} else { s += ' No upcoming hearings scheduled.'; }
						if (fc > 0) s += ` ${fc} documents on file.`;
						if (judgePred) s += ` JUDGE INTEL: ${judgePred}.`;
						if (ocPred) s += ` OC INTEL: ${ocPred}.`;

						// Preserve manually-entered fields (facts, charges, notes, OC contact, key deadlines)
						const facts = existing.facts || '';
						const charges = existing.charges || '';
						const notes = existing.notes || '';
						const ocPhone = existing.opposing_counsel_phone || '';
						const ocEmail = existing.opposing_counsel_email || '';
						const ocFirm = existing.opposing_counsel_firm || '';
						const discDl = existing.discovery_deadline || '';
						const dispDl = existing.dispositive_deadline || '';
						const trialDt = existing.trial_date || '';
						const solDt = existing.statute_of_limitations || '';

						batch.push(env.MEMORY_DB.prepare(
							`INSERT INTO case_summaries (client_name, case_number, case_type, court, client_role, opposing_party, opposing_role, opposing_counsel, judge, district, summary, next_event, next_event_date, status, file_count, client_email, client_phone, client_address, additional_parties, folder_url, opposing_counsel_phone, opposing_counsel_email, opposing_counsel_firm, facts, charges, discovery_deadline, dispositive_deadline, trial_date, statute_of_limitations, judge_prediction, oc_prediction, reversal_factors, notes, created_at, updated_at)
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
							ON CONFLICT(client_name, case_number) DO UPDATE SET
								case_type=excluded.case_type, court=excluded.court, client_role=excluded.client_role,
								opposing_party=excluded.opposing_party, opposing_role=excluded.opposing_role,
								opposing_counsel=excluded.opposing_counsel, judge=excluded.judge, district=excluded.district,
								summary=excluded.summary, next_event=excluded.next_event, next_event_date=excluded.next_event_date,
								file_count=excluded.file_count, client_email=excluded.client_email, client_phone=excluded.client_phone,
								client_address=excluded.client_address, additional_parties=excluded.additional_parties,
								folder_url=excluded.folder_url, judge_prediction=excluded.judge_prediction,
								oc_prediction=excluded.oc_prediction, reversal_factors=excluded.reversal_factors,
								updated_at=excluded.updated_at`
						).bind(
							name, caseNum, p.case_type, p.court, p.client_role, p.opposing_party,
							p.opposing_role || '', p.opposing_counsel || '', p.judge, p.district || '',
							s, nextEvDesc, nextEvDate, fc,
							p.client_email || '', p.client_phone || '', p.client_address || '',
							p.additional_parties || '', p.folder_url || '',
							ocPhone, ocEmail, ocFirm, facts, charges,
							discDl, dispDl, trialDt, solDt,
							judgePred, ocPred, revFactors, notes, today, today
						));
						upserted++;
					}

					// Execute in batches of 50 (D1 batch limit ~100)
					for (let i = 0; i < batch.length; i += 50) {
						await env.MEMORY_DB.batch(batch.slice(i, i + 50));
					}

					return json({ success: true, upserted, errors, date: today, parties: (parties as any[]).length, deadlines: (deadlines as any[]).length, judgeLogsLoaded: judgeLogs.length, ocLogsLoaded: ocLogs.length, reversalsLoaded: reversals.length });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// DEEP SCAN — Extract facts, charges, OC contact, deadlines from case files
			// Reads OneDrive files, uses AI to extract structured data, updates case_summaries
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/case-summaries/deep-scan' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const clientFilter = body.client || null;        // single client name
					const batchSize = Math.min(body.batch || 5, 5); // max 5 per request (Workers subrequest limit)
					const offset = body.offset || 0;
					const dryRun = body.dry_run || false;            // preview without writing

					const token = await getGraphToken();

					// Get cases to scan
					let casesQuery = `SELECT cs.*, pc.folder_url, pc.folder_path FROM case_summaries cs LEFT JOIN party_cache pc ON cs.client_name = pc.client_name AND cs.case_number = pc.case_number WHERE cs.status = 'active'`;
					const binds: any[] = [];
					if (clientFilter) {
						const parts = clientFilter.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter((p: string) => p.length >= 2);
						for (const p of parts) {
							casesQuery += ` AND LOWER(cs.client_name) LIKE ?`;
							binds.push(`%${p.toLowerCase()}%`);
						}
					}
					casesQuery += ` ORDER BY cs.client_name ASC LIMIT ? OFFSET ?`;
					binds.push(batchSize, offset);

					const stmt = env.MEMORY_DB.prepare(casesQuery);
					const { results: cases } = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();

					if (cases.length === 0) return json({ success: true, scanned: 0, message: 'No cases to scan at this offset' });

					const scanResults: any[] = [];

					for (const cs of cases as any[]) {
						const clientName = cs.client_name as string;
						const caseNum = cs.case_number as string;
						const result: any = { client: clientName, case_number: caseNum, files_found: 0, extracted: {} };

						try {
							// 1. Find client folder in OneDrive
							const searchUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${env.ONEDRIVE_FOLDER_ID}/children?$top=200`;
							const folderRes = await fetch(searchUrl, { headers: { 'Authorization': `Bearer ${token}` } });
							const folderData = await folderRes.json() as any;
							// Smart folder match with fuzzy name matching
							const nameParts = clientName.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter((p: string) => p.length >= 2).map((p: string) => p.toLowerCase());
							const last = nameParts[nameParts.length - 1];
							const first = nameParts[0];
							const clientFolder = (folderData.value || []).find((f: any) => {
								if (!f.folder) return false;
								const fn = f.name.toLowerCase().replace(/[^a-z\s]/g, ' ');
								const fParts = fn.trim().split(/\s+/).filter((p: string) => p.length >= 2);
								// Tier 1: All name parts appear (exact substring)
								if (nameParts.every((p: string) => fn.includes(p))) return true;
								// Tier 2: First + last both appear (handles middle names)
								if (nameParts.length >= 2 && fn.includes(first) && fn.includes(last)) return true;
								// Tier 3: Fuzzy — first + last both fuzzy-match folder parts
								const lastFuzzy = fParts.some((fp: string) => fuzzyNameMatch(last, fp));
								const firstFuzzy = fParts.some((fp: string) => fuzzyNameMatch(first, fp));
								if (lastFuzzy && firstFuzzy) return true;
								// Tier 4: Last name exact starts-with (>4 chars)
								if (last && last.length > 4 && fn.startsWith(last)) return true;
								// Tier 5: Last name fuzzy only (>5 chars)
								if (last && last.length > 5 && lastFuzzy) return true;
								return false;
							});

							if (!clientFolder) {
								result.error = 'No OneDrive folder found';
								scanResults.push(result);
								continue;
							}

							// 2. List files in folder (and subfolders 1 level deep)
							const allFiles: any[] = [];
							const topFiles = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${clientFolder.id}/children?$top=100&$orderby=lastModifiedDateTime desc`, {
								headers: { 'Authorization': `Bearer ${token}` }
							}).then(r => r.json()) as any;

							for (const f of (topFiles.value || [])) {
								if (f.folder) {
									// Scan subfolder too
									try {
										const subFiles = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${f.id}/children?$top=50`, {
											headers: { 'Authorization': `Bearer ${token}` }
										}).then(r => r.json()) as any;
										for (const sf of (subFiles.value || [])) {
											allFiles.push({ ...sf, subfolder: f.name });
										}
									} catch (_) {}
								} else {
									allFiles.push(f);
								}
							}

							result.files_found = allFiles.length;

							// 3. Identify key documents by filename patterns
							// Prefer .docx over .pdf for same doc (better text extraction)
							const keyDocs: Record<string, any> = {};
							const prefer = (cat: string, f: any) => {
								const existing = keyDocs[cat];
								if (!existing) { keyDocs[cat] = f; return; }
								const newExt = (f.name || '').split('.').pop()?.toLowerCase();
								const oldExt = (existing.name || '').split('.').pop()?.toLowerCase();
								if (newExt === 'docx' && oldExt !== 'docx') keyDocs[cat] = f; // prefer docx
							};
							for (const f of allFiles) {
								const fn = (f.name || '').toLowerCase();
								const ext = fn.split('.').pop() || '';
								if (!['pdf', 'docx', 'doc', 'txt', 'rtf'].includes(ext)) continue;

								// Categorize by filename — broad patterns
								if (/noa|notice.*appear|entry.*appear/i.test(fn)) prefer('noa', f);
								else if (/complaint|information|charging|indictment|citation|arraign|criminal.*contract/i.test(fn)) prefer('charging', f);
								else if (/schedul.*order|case.*manage|cmo|pretrial.*conf|pre.*trial/i.test(fn)) prefer('scheduling', f);
								else if (/police.*report|incident.*report|probable.*cause|arrest/i.test(fn)) prefer('police_report', f);
								else if (/petition|motion.*dismiss|answer|plea|change.*plea/i.test(fn)) prefer('pleading', f);
								else if (/discovery|interrogat|request.*produc|request.*discov|rfd|rfp|rog/i.test(fn)) prefer('discovery', f);
								else if (/cover.*sheet|face.*sheet|intake|contract|fee.*agree/i.test(fn)) prefer('coversheet', f);
								else if (/order(?!.*continue)|minute|ruling|judgment|sentence/i.test(fn)) prefer('court_order', f);
								else if (/letter|corresp|email/i.test(fn) && !keyDocs['correspondence']) prefer('correspondence', f);
								else if (/stipulat|motion/i.test(fn) && !keyDocs['motion']) prefer('motion', f);
							}

							result.key_docs = Object.fromEntries(
								Object.entries(keyDocs).map(([k, v]) => [k, v.name])
							);

							if (Object.keys(keyDocs).length === 0) {
								result.error = 'No key documents identified by filename';
								scanResults.push(result);
								continue;
							}

							// 4. Download and extract text from key documents (max 3 to stay within time limits)
							// For DOCX/PPTX: use Graph API content?format=pdf to get a text-extractable version
							// For PDF: use basic Tj/TJ extraction (works for text-based PDFs)
							// For TXT: read directly
							const docsToRead = Object.entries(keyDocs).slice(0, 3);
							let combinedText = '';

							for (const [docType, file] of docsToRead) {
								try {
									const ext = (file.name || '').split('.').pop()?.toLowerCase();

									if (ext === 'docx' || ext === 'doc' || ext === 'pptx') {
										// Use Graph API to convert to plain text via content endpoint
										// Graph can extract content from Office files natively
										try {
											// Method 1: Try Graph search excerpt (free text from indexed content)
											const searchRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${file.id}/content`, {
												headers: { 'Authorization': `Bearer ${token}` }
											});
											const docBuf = await searchRes.arrayBuffer();
											const bytes = new Uint8Array(docBuf);

											// DOCX is a ZIP — find word/document.xml entry and decompress
											let text = '';
											// Search for PK headers and find word/document.xml
											for (let i = 0; i < bytes.length - 30; i++) {
												if (bytes[i] === 0x50 && bytes[i+1] === 0x4B && bytes[i+2] === 0x03 && bytes[i+3] === 0x04) {
													// ZIP local file header
													const fnLen = bytes[i+26] | (bytes[i+27] << 8);
													const exLen = bytes[i+28] | (bytes[i+29] << 8);
													const compMethod = bytes[i+8] | (bytes[i+9] << 8);
													const compSize = bytes[i+18] | (bytes[i+19] << 8) | (bytes[i+20] << 16) | (bytes[i+21] << 24);
													const headerEnd = i + 30 + fnLen + exLen;
													const fileName = new TextDecoder().decode(bytes.slice(i+30, i+30+fnLen));

													if (fileName === 'word/document.xml' && compSize > 0) {
														const compData = bytes.slice(headerEnd, headerEnd + compSize);
														if (compMethod === 8) {
															// Deflate — use DecompressionStream
															try {
																const ds = new DecompressionStream('deflate-raw');
																const writer = ds.writable.getWriter();
																writer.write(compData);
																writer.close();
																const reader = ds.readable.getReader();
																const chunks: Uint8Array[] = [];
																while (true) {
																	const { done, value } = await reader.read();
																	if (done) break;
																	chunks.push(value);
																}
																const totalLen = chunks.reduce((s, c) => s + c.length, 0);
																const merged = new Uint8Array(totalLen);
																let offset = 0;
																for (const c of chunks) { merged.set(c, offset); offset += c.length; }
																const xml = new TextDecoder().decode(merged);
																const wtMatches = xml.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
																for (const m of wtMatches) text += m.replace(/<[^>]+>/g, '') + ' ';
															} catch (_) {}
														} else if (compMethod === 0) {
															// Stored (uncompressed)
															const xml = new TextDecoder().decode(compData);
															const wtMatches = xml.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
															for (const m of wtMatches) text += m.replace(/<[^>]+>/g, '') + ' ';
														}
														break;
													}
												}
											}
											if (text.trim().length > 20) {
												combinedText += `\n--- ${docType.toUpperCase()} (${file.name}) ---\n${text.substring(0, 4000)}\n`;
											}
										} catch (_) {}
									} else if (ext === 'pdf') {
										const itemRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${file.id}`, {
											headers: { 'Authorization': `Bearer ${token}` }
										});
										const itemData = await itemRes.json() as any;
										const dlUrl = itemData['@microsoft.graph.downloadUrl'];
										if (!dlUrl) continue;
										const pdfRes = await fetch(dlUrl);
										const pdfBuf = await pdfRes.arrayBuffer();
										const bytes = new Uint8Array(pdfBuf);
										const raw = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
										let text = '';

										// Method 1: Standard (text) Tj patterns
										const tjMatches = raw.match(/\(([^)]{2,})\)\s*Tj/g) || [];
										for (const m of tjMatches) {
											const inner = m.replace(/^\(/, '').replace(/\)\s*Tj$/, '');
											if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner + ' ';
										}

										// Method 2: BT...ET blocks with text objects
										const btBlocks = raw.match(/BT\s[\s\S]{5,2000}?ET/g) || [];
										for (const block of btBlocks.slice(0, 30)) {
											const innerMatches = block.match(/\(([^)]{2,})\)/g) || [];
											for (const m of innerMatches) {
												const inner = m.replace(/[()]/g, '');
												if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner + ' ';
											}
										}

										// Method 3: TJ arrays [(text) num (text) num] TJ
										const tjArrays = raw.match(/\[([^\]]{5,})\]\s*TJ/g) || [];
										for (const arr of tjArrays.slice(0, 50)) {
											const parts = arr.match(/\(([^)]+)\)/g) || [];
											for (const p of parts) {
												const inner = p.replace(/[()]/g, '');
												if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner;
											}
											text += ' ';
										}

										// Method 4: Hex-encoded text <hex> Tj
										const hexMatches = raw.match(/<([0-9A-Fa-f]{4,})>\s*Tj/g) || [];
										for (const hm of hexMatches.slice(0, 30)) {
											const hex = hm.replace(/</, '').replace(/>\s*Tj/, '');
											try {
												let decoded = '';
												for (let hi = 0; hi < hex.length; hi += 2) {
													const code = parseInt(hex.substr(hi, 2), 16);
													if (code >= 32 && code < 127) decoded += String.fromCharCode(code);
												}
												if (decoded.length > 1) text += decoded + ' ';
											} catch (_) {}
										}

										// Method 5: Stream blocks — look for readable text in decompressed streams
										if (text.trim().length < 50) {
											// Try FlateDecode streams
											const streamStarts: number[] = [];
											let searchFrom = 0;
											while (true) {
												const idx = raw.indexOf('stream\r\n', searchFrom);
												if (idx === -1) break;
												streamStarts.push(idx + 8);
												searchFrom = idx + 8;
												if (streamStarts.length > 15) break;
											}
											for (const sStart of streamStarts.slice(0, 10)) {
												const sEnd = raw.indexOf('endstream', sStart);
												if (sEnd === -1 || sEnd - sStart > 100000) continue;
												const streamBytes = bytes.slice(sStart, sEnd);
												try {
													const ds = new DecompressionStream('deflate');
													const writer = ds.writable.getWriter();
													writer.write(streamBytes);
													writer.close();
													const reader = ds.readable.getReader();
													const chunks: Uint8Array[] = [];
													let totalBytes = 0;
													while (totalBytes < 50000) {
														const { done, value } = await reader.read();
														if (done) break;
														chunks.push(value);
														totalBytes += value.length;
													}
													const totalLen = chunks.reduce((s, c) => s + c.length, 0);
													const merged = new Uint8Array(totalLen);
													let off = 0;
													for (const c of chunks) { merged.set(c, off); off += c.length; }
													const decoded = new TextDecoder('utf-8', { fatal: false }).decode(merged);
													// Look for text operators in decompressed content
													const innerTj = decoded.match(/\(([^)]{2,})\)\s*Tj/g) || [];
													for (const m of innerTj) {
														const inner = m.replace(/^\(/, '').replace(/\)\s*Tj$/, '');
														if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner + ' ';
													}
													const innerTJ = decoded.match(/\[([^\]]{5,})\]\s*TJ/g) || [];
													for (const arr of innerTJ) {
														const parts = arr.match(/\(([^)]+)\)/g) || [];
														for (const p of parts) {
															const inner = p.replace(/[()]/g, '');
															if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner;
														}
														text += ' ';
													}
												} catch (_) { /* stream wasn't FlateDecode or corrupt */ }
											}
										}

										// Clean up extracted text
										text = text.replace(/\\n/g, '\n').replace(/\\r/g, '').replace(/\s{3,}/g, ' ').trim();

										if (text.length > 20) {
											combinedText += `\n--- ${docType.toUpperCase()} (${file.name}) ---\n${text.substring(0, 4000)}\n`;
										}
									} else if (ext === 'txt') {
										const itemRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${file.id}`, {
											headers: { 'Authorization': `Bearer ${token}` }
										});
										const itemData = await itemRes.json() as any;
										const dlUrl = itemData['@microsoft.graph.downloadUrl'];
										if (!dlUrl) continue;
										const txtRes = await fetch(dlUrl);
										const text = await txtRes.text();
										combinedText += `\n--- ${docType.toUpperCase()} (${file.name}) ---\n${text.substring(0, 4000)}\n`;
									}
								} catch (fileErr: any) {
									result.extracted[`${docType}_error`] = fileErr.message;
								}
							}

							if (combinedText.length < 50) {
								result.error = 'Could not extract readable text from documents';
								scanResults.push(result);
								continue;
							}

							// 5. Use Workers AI to extract structured data
							// Include party context to prevent OC confusion
							const clientRole = cs.client_role || '';
							const opposingParty = cs.opposing_party || '';
							const existingOC = cs.opposing_counsel || '';
							const extractionPrompt = `You are a legal document analyzer. Extract CONCISE info from these case documents.

CLIENT: ${clientName} (${clientRole || 'our client'})
OPPOSING PARTY: ${opposingParty || 'unknown'}
OUR FIRM: Pitcher Law PLLC (attorneys: Diane Pitcher, John Adams). Emails: dianepitcher.com, esqslaw@gmail.com
${existingOC ? `KNOWN OPPOSING COUNSEL: ${existingOC}` : ''}

CRITICAL: Opposing counsel is the OTHER side's attorney — NOT our firm. Pitcher Law, Diane Pitcher, John Adams, Marie, anything @dianepitcher.com or @esqslaw is OUR firm, not OC. If you can only find our firm's info, leave OC fields empty.

RULES: Facts 2-3 sentences MAX. Charges: names + degrees only. Use "" for not found.

Respond with ONLY valid JSON:
{"facts":"","charges":"","oc_name":"","oc_phone":"","oc_email":"","oc_firm":"","discovery_deadline":"","trial_date":"","dispositive_deadline":"","statute_of_limitations":"","additional_parties":""}

Documents:
${combinedText.substring(0, 5500)}`;

							try {
								const aiRes = await env.AI.run('@cf/meta/llama-3.1-8b-instruct', {
									messages: [{ role: 'user', content: extractionPrompt }],
									max_tokens: 1000,
									temperature: 0.1,
								}) as any;
								const aiText = aiRes.response || '';

								// Parse JSON from AI response
								const jsonMatch = aiText.match(/\{[\s\S]*\}/);
								if (jsonMatch) {
									const extracted = JSON.parse(jsonMatch[0]);

									// ═══ PARTY VERIFICATION — prevent attorney-as-OC confusion ═══
									// Hard filter: reject OC fields that match our own firm
									const ownFirmPatterns = /pitcher\s*law|diane\s*pitcher|john\s*adams|dianepitcher\.com|esqslaw|marie@|associate@|^pitcher|^adams/i;
									const isOwnFirm = (val: string) => val && ownFirmPatterns.test(val);

									if (isOwnFirm(extracted.oc_name || '') || isOwnFirm(extracted.oc_firm || '') || isOwnFirm(extracted.oc_email || '')) {
										// AI confused our firm for OC — nuke all OC fields
										extracted.oc_name = '';
										extracted.oc_phone = '';
										extracted.oc_email = '';
										extracted.oc_firm = '';
										result.party_verification = 'REJECTED — AI returned our own firm as OC';
									}

									// Also reject if extracted OC name matches our client
									if (extracted.oc_name && clientName.toLowerCase().includes(extracted.oc_name.toLowerCase().split(' ')[0])) {
										extracted.oc_name = '';
										extracted.oc_phone = '';
										extracted.oc_email = '';
										extracted.oc_firm = '';
										result.party_verification = 'REJECTED — AI returned our client as OC';
									}

									// Cross-reference: if party_cache already has OC and AI found a different one, prefer party_cache
									if (existingOC && extracted.oc_name && extracted.oc_name.toLowerCase() !== existingOC.toLowerCase()) {
										// party_cache OC comes from court records — more authoritative
										extracted.oc_name = ''; // don't overwrite with AI guess
										result.party_verification = (result.party_verification || '') + ' | OC mismatch with court records — kept existing';
									}

									result.extracted = extracted;

									// 6. Update case_summaries (unless dry_run)
									if (!dryRun) {
										const updates: string[] = [];
										const vals: any[] = [];

										if (extracted.facts && !cs.facts) { updates.push('facts = ?'); vals.push(extracted.facts); }
										if (extracted.charges && !cs.charges) { updates.push('charges = ?'); vals.push(extracted.charges); }
										if (extracted.oc_name && !cs.opposing_counsel) { updates.push('opposing_counsel = ?'); vals.push(extracted.oc_name); }
										if (extracted.oc_phone && !cs.opposing_counsel_phone) { updates.push('opposing_counsel_phone = ?'); vals.push(extracted.oc_phone); }
										if (extracted.oc_email && !cs.opposing_counsel_email) { updates.push('opposing_counsel_email = ?'); vals.push(extracted.oc_email); }
										if (extracted.oc_firm && !cs.opposing_counsel_firm) { updates.push('opposing_counsel_firm = ?'); vals.push(extracted.oc_firm); }
										if (extracted.discovery_deadline && !cs.discovery_deadline) { updates.push('discovery_deadline = ?'); vals.push(extracted.discovery_deadline); }
										if (extracted.trial_date && !cs.trial_date) { updates.push('trial_date = ?'); vals.push(extracted.trial_date); }
										if (extracted.dispositive_deadline && !cs.dispositive_deadline) { updates.push('dispositive_deadline = ?'); vals.push(extracted.dispositive_deadline); }
										if (extracted.statute_of_limitations && !cs.statute_of_limitations) { updates.push('statute_of_limitations = ?'); vals.push(extracted.statute_of_limitations); }
										if (extracted.additional_parties && !cs.additional_parties) { updates.push('additional_parties = ?'); vals.push(extracted.additional_parties); }

										if (updates.length > 0) {
											updates.push('updated_at = ?');
											vals.push(mtnToday());
											vals.push(clientName);
											vals.push(caseNum);
											await env.MEMORY_DB.prepare(
												`UPDATE case_summaries SET ${updates.join(', ')} WHERE client_name = ? AND case_number = ?`
											).bind(...vals).run();
											result.fields_updated = updates.length - 1; // minus the updated_at
										} else {
											result.fields_updated = 0;
											result.note = 'All extractable fields already populated';
										}
									}
								} else {
									result.extracted = { raw: aiText, error: 'Could not parse JSON from AI response' };
								}
							} catch (aiErr: any) {
								result.extracted = { error: `AI extraction failed: ${aiErr.message}` };
							}

						} catch (caseErr: any) {
							result.error = caseErr.message;
						}

						scanResults.push(result);
					}

					return json({
						success: true,
						scanned: scanResults.length,
						total_active: (await env.MEMORY_DB.prepare(`SELECT COUNT(*) as cnt FROM case_summaries WHERE status = 'active'`).first() as any)?.cnt || 0,
						offset,
						batch_size: batchSize,
						next_offset: offset + batchSize,
						dry_run: dryRun,
						results: scanResults,
					});
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// XCHANGE DOCKET VERIFICATION
			// Local server pushes docket snapshots, Worker stores + serves them
			// ═══════════════════════════════════════════════════════════════

			// GET /api/xchange/docket/:caseNumber — Get cached docket data
			if (path.startsWith('/api/xchange/docket/') && request.method === 'GET') {
				try {
					const caseNumber = decodeURIComponent(path.replace('/api/xchange/docket/', ''));
					const row = await env.MEMORY_DB.prepare(
						`SELECT * FROM xchange_dockets WHERE case_number = ?`
					).bind(caseNumber).first();
					if (!row) return json({ success: false, error: 'No docket data cached', caseNumber });
					return json({ success: true, docket: row });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// GET /api/xchange/docket — List all cached dockets (with optional ?client= filter)
			if (path === '/api/xchange/docket' && request.method === 'GET') {
				try {
					const client = url.searchParams.get('client');
					let results;
					if (client) {
						const parts = client.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter((p: string) => p.length >= 2);
						if (parts.length > 0) {
							const conditions = parts.map(() => `LOWER(client_name) LIKE ?`);
							const binds = parts.map((p: string) => `%${p.toLowerCase()}%`);
							const { results: r } = await env.MEMORY_DB.prepare(
								`SELECT * FROM xchange_dockets WHERE ${conditions.join(' AND ')} ORDER BY updated_at DESC`
							).bind(...binds).all();
							results = r;
						} else {
							const { results: r } = await env.MEMORY_DB.prepare(
								`SELECT * FROM xchange_dockets WHERE LOWER(client_name) LIKE ? ORDER BY updated_at DESC`
							).bind(`%${client.toLowerCase()}%`).all();
							results = r;
						}
					} else {
						const { results: r } = await env.MEMORY_DB.prepare(
							`SELECT * FROM xchange_dockets ORDER BY updated_at DESC LIMIT 100`
						).all();
						results = r;
					}
					return json({ success: true, dockets: results, count: (results as any[]).length });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// POST /api/xchange/docket — Store/update docket data (called by local server after Puppeteer lookup)
			if (path === '/api/xchange/docket' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const { caseNumber, clientName, caseName, court, judge, caseType, filingDate, status, hearings, docketEntries, rawText } = body;
					if (!caseNumber) return err('caseNumber required', 400);

					const today = mtnToday();
					const hearingsJson = JSON.stringify(hearings || []);
					const docketJson = JSON.stringify(docketEntries || []);
					const nextHearing = (hearings || []).length > 0 ? hearings[0].date : null;

					await env.MEMORY_DB.prepare(`
						INSERT INTO xchange_dockets (case_number, client_name, case_name, court, judge, case_type, filing_date, status, hearings_json, docket_json, next_hearing, raw_text, created_at, updated_at)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
						ON CONFLICT(case_number) DO UPDATE SET
							client_name = COALESCE(?, client_name),
							case_name = COALESCE(?, case_name),
							court = COALESCE(?, court),
							judge = COALESCE(?, judge),
							case_type = COALESCE(?, case_type),
							filing_date = COALESCE(?, filing_date),
							status = COALESCE(?, status),
							hearings_json = ?,
							docket_json = ?,
							next_hearing = ?,
							raw_text = ?,
							updated_at = ?
					`).bind(
						caseNumber, clientName || null, caseName || null, court || null, judge || null,
						caseType || null, filingDate || null, status || null, hearingsJson, docketJson,
						nextHearing, (rawText || '').substring(0, 2000), today, today,
						// ON CONFLICT binds
						clientName || null, caseName || null, court || null, judge || null,
						caseType || null, filingDate || null, status || null,
						hearingsJson, docketJson, nextHearing,
						(rawText || '').substring(0, 2000), today
					).run();

					return json({ success: true, caseNumber, updated: today });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// POST /api/xchange/bulk-push — Store multiple docket snapshots at once
			if (path === '/api/xchange/bulk-push' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const { dockets } = body;
					if (!dockets || !Array.isArray(dockets)) return err('dockets array required', 400);

					const today = mtnToday();
					let upserted = 0;
					const batch: any[] = [];

					for (const d of dockets) {
						if (!d.caseNumber) continue;
						const hearingsJson = JSON.stringify(d.hearings || []);
						const docketJson = JSON.stringify(d.docketEntries || []);
						const nextHearing = (d.hearings || []).length > 0 ? d.hearings[0].date : null;

						batch.push(env.MEMORY_DB.prepare(`
							INSERT INTO xchange_dockets (case_number, client_name, case_name, court, judge, case_type, filing_date, status, hearings_json, docket_json, next_hearing, raw_text, created_at, updated_at)
							VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
							ON CONFLICT(case_number) DO UPDATE SET
								client_name = COALESCE(?, client_name),
								case_name = COALESCE(?, case_name),
								court = COALESCE(?, court),
								judge = COALESCE(?, judge),
								hearings_json = ?, docket_json = ?, next_hearing = ?, updated_at = ?
						`).bind(
							d.caseNumber, d.clientName || null, d.caseName || null, d.court || null,
							d.judge || null, d.caseType || null, d.filingDate || null, d.status || null,
							hearingsJson, docketJson, nextHearing, (d.rawText || '').substring(0, 2000), today, today,
							d.clientName || null, d.caseName || null, d.court || null, d.judge || null,
							hearingsJson, docketJson, nextHearing, today
						));
						upserted++;
					}

					for (let i = 0; i < batch.length; i += 50) {
						await env.MEMORY_DB.batch(batch.slice(i, i + 50));
					}

					return json({ success: true, upserted, date: today });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// OFFLINE SYNC — process queued writes from offline dashboard
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/sync/queue' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const { items } = body; // Array of { method, url, body, meta, timestamp }
					if (!items || !Array.isArray(items)) return err('items array required', 400);

					const results: any[] = [];
					for (const item of items) {
						try {
							// Build a sub-request to the appropriate endpoint
							const subUrl = new URL(item.url.replace('https://api.esqs-law.com', ''), 'https://api.esqs-law.com');
							const subPath = subUrl.pathname;

							// For chat messages — always append (no conflict)
							if (subPath.includes('/api/bridges/message')) {
								results.push({ url: item.url, status: 'forwarded', conflict: false });
								continue;
							}

							// For deadline updates — check if server version is newer
							if (subPath.includes('/api/deadlines') && item.body?.id) {
								const existing = await env.MEMORY_DB.prepare(
									`SELECT updated_at FROM deadlines WHERE id = ?`
								).bind(item.body.id).first() as any;

								if (existing && existing.updated_at && item.timestamp) {
									const serverTime = new Date(existing.updated_at).getTime();
									if (serverTime > item.timestamp) {
										results.push({
											url: item.url, status: 'conflict',
											conflict: true,
											existing: existing,
											localData: item.body,
											message: 'Server version is newer — both versions preserved'
										});
										continue;
									}
								}
							}

							// Default: no conflict detected, mark as processable
							results.push({ url: item.url, status: 'ok', conflict: false });
						} catch (e: any) {
							results.push({ url: item.url, status: 'error', error: e.message });
						}
					}

					return json({ success: true, results, processed: items.length });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// Legal document templates listing
			if (path === '/api/templates' && request.method === 'GET') {
				const templates = [
					{ id: 'noa', name: 'Notice of Appearance', category: 'General', path: 'LegalTemplates/NOA_Template.docx' },
					{ id: 'motion_generic', name: 'Motion (Generic)', category: 'Motions', path: 'LegalTemplates/Motion_Generic_Template.docx' },
					{ id: 'motion_compel', name: 'Motion to Compel Discovery', category: 'Motions', path: 'LegalTemplates/Motion_Compel_Discovery_Template.docx' },
					{ id: 'motion_virtual', name: 'Motion for Virtual Hearing', category: 'Motions', path: 'LegalTemplates/Motion_Virtual_Hearing_Template.docx' },
					{ id: 'interrogatories', name: 'Interrogatories', category: 'Discovery', path: 'LegalTemplates/Interrogatories_Template.docx' },
					{ id: 'req_production', name: 'Requests for Production', category: 'Discovery', path: 'LegalTemplates/Requests_Production_Template.docx' },
					{ id: 'cover_sheet', name: 'Master Cover Sheet', category: 'General', path: 'LegalTemplates/00_Master_Cover_Sheet_Template.docx' },
					{ id: 'acceptance_service', name: 'Acceptance of Service', category: 'General', path: 'templates/utah_acceptance_service.docx' },
					{ id: 'affidavit', name: 'Affidavit', category: 'General', path: 'templates/utah_affidavit.docx' },
					{ id: 'subpoena', name: 'Subpoena for Trial', category: 'General', path: 'templates/subpoena_trial.docx' },
					{ id: 'stipulation', name: 'Stipulation', category: 'General', path: 'templates/stipulation.docx' },
					{ id: 'petition_modify', name: 'Petition to Modify Custody', category: 'Family', path: 'templates/petition_modify_custody.docx' },
					{ id: 'order_show_cause', name: 'Order to Show Cause', category: 'Motions', path: 'templates/order_show_cause.docx' },
					{ id: 'plea_abeyance', name: 'Plea in Abeyance', category: 'Criminal', path: 'templates/plea_abeyance.docx' },
					{ id: 'motion_contempt', name: 'Motion for Contempt', category: 'Motions', path: 'templates/motion_contempt.docx' },
					{ id: 'motion_default', name: 'Motion for Default Judgment', category: 'Motions', path: 'templates/motion_default_judgment.docx' },
					{ id: 'initial_disclosures', name: 'Initial Disclosures (Respondent)', category: 'Discovery', path: 'templates/initial_disclosures_respondent.docx' },
					{ id: 'trial_outline', name: 'Trial Outline', category: 'Trial', path: 'templates/trial_outline.docx' },
					{ id: 'withdrawal', name: 'Withdrawal of Counsel', category: 'General', path: 'templates/withdrawal.docx' },
					{ id: 'motion_remote', name: 'Motion to Appear Remotely', category: 'Motions', path: 'templates/motion_appear_remotely.docx' },
				];
				const category = url.searchParams.get('category');
				const filtered = category ? templates.filter(t => t.category.toLowerCase() === category.toLowerCase()) : templates;
				return json({ success: true, templates: filtered, categories: [...new Set(templates.map(t => t.category))] });
			}

			// ═══════════════════════════════════════════════════════════════
			// CASE FILES (from MEMORY_DB case_files index)
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/case-files/search' && request.method === 'GET') {
				const client = url.searchParams.get('client');
				const source = url.searchParams.get('source'); // 'open' or 'closed'
				const fileType = url.searchParams.get('type');
				const limit = parseInt(url.searchParams.get('limit') || '50');

				let q = 'SELECT client_name, file_name, file_path, file_type, file_size, source, last_modified FROM case_files WHERE 1=1';
				const p: any[] = [];
				if (client) { q += ' AND client_name LIKE ?'; p.push(`%${client}%`); }
				if (source) { q += ' AND source = ?'; p.push(source); }
				if (fileType) { q += ' AND file_type = ?'; p.push(fileType); }
				q += ' ORDER BY last_modified DESC LIMIT ?';
				p.push(limit);

				try {
					const { results } = await env.MEMORY_DB.prepare(q).bind(...p).all();
					return json({ success: true, files: results, count: results.length });
				} catch (e: any) {
					return json({ success: true, files: [], error: e.message });
				}
			}

			if (path === '/api/case-files/clients' && request.method === 'GET') {
				try {
					const statusFilter = url.searchParams.get('status') || 'active';
					// Map status to case_files.source: active→open, closed→closed, all→all
					const sourceFilter = statusFilter === 'active' ? 'open' : statusFilter === 'closed' ? 'closed' : 'all';
					// Get file counts per client — filtered by source (open/closed)
					const fileQuery = sourceFilter === 'all'
						? `SELECT client_name, source, COUNT(*) as file_count FROM case_files GROUP BY client_name, source ORDER BY client_name`
						: `SELECT client_name, source, COUNT(*) as file_count FROM case_files WHERE source = ? GROUP BY client_name, source ORDER BY client_name`;
					const fileStmt = sourceFilter === 'all'
						? env.MEMORY_DB.prepare(fileQuery)
						: env.MEMORY_DB.prepare(fileQuery).bind(sourceFilter);
					const { results: fileCounts } = await fileStmt.all();
					// Get case info from party_cache — filter by status (active/closed/all)
					const caseQuery = statusFilter === 'all'
						? `SELECT client_name, case_number, case_type, client_role, court, status, folder_url FROM party_cache ORDER BY last_verified DESC`
						: `SELECT client_name, case_number, case_type, client_role, court, status, folder_url FROM party_cache WHERE status = ? OR status IS NULL ORDER BY last_verified DESC`;
					const caseStmt = statusFilter === 'all'
						? env.MEMORY_DB.prepare(caseQuery)
						: env.MEMORY_DB.prepare(caseQuery).bind(statusFilter);
					const { results: caseInfo } = await caseStmt.all();
					// Build lookup: client_name → array of cases
					const caseMap = new Map<string, any[]>();
					for (const ci of caseInfo as any[]) {
						const key = ci.client_name?.toLowerCase()?.trim();
						if (!key) continue;
						if (!caseMap.has(key)) caseMap.set(key, []);
						caseMap.get(key)!.push({ case_number: ci.case_number, case_type: ci.case_type, client_role: ci.client_role, court: ci.court, status: ci.status || 'active', folder_url: ci.folder_url || null });
					}
					// Merge: for each client from case_files, attach matching party_cache cases
					const clients = (fileCounts as any[]).map(fc => {
						const key = fc.client_name?.toLowerCase()?.trim();
						// Try exact match first, then partial match (folder name "Last, First" vs court name "FIRST LAST")
						let cases = caseMap.get(key) || null;
						if (!cases) {
							const nameParts = (fc.client_name || '').replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter((p: string) => p.length >= 2).map((p: string) => p.toLowerCase());
							if (nameParts.length > 0) {
								for (const [k, v] of caseMap) {
									const match = nameParts.every((np: string) => k.includes(np));
									if (match) { cases = v; break; }
								}
							}
						}
						return { ...fc, cases: cases || [] };
					});
					return json({ success: true, clients });
				} catch (e: any) {
					return json({ success: true, clients: [], error: e.message });
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// CLIENT FACESHEET / CASE HISTORY
			// Combined: party_cache + deadlines + case_files → one view
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/client/facesheet' && request.method === 'GET') {
				const clientName = url.searchParams.get('client');
				if (!clientName) return err('client parameter required', 400);

				// Fuzzy name matching: "Buttars, Garrett" → search for rows containing both "buttars" AND "garrett"
				// Handles folder names (Last, First) vs court names (FIRST MIDDLE LAST)
				// Also loose on spelling: if strict match fails, falls back to longest name part only
				// Misspelling tolerance: if all else fails, Levenshtein distance match against all names
				const nameParts = clientName.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter(p => p.length >= 2);
				const buildFuzzyWhere = (col: string, loose = false) => {
					if (nameParts.length === 0) return { clause: `${col} LIKE ?`, binds: [`%${clientName}%`] };
					if (loose) {
						const longest = nameParts.reduce((a, b) => a.length >= b.length ? a : b);
						return { clause: `LOWER(${col}) LIKE ?`, binds: [`%${longest.toLowerCase()}%`] };
					}
					const conditions = nameParts.map(() => `LOWER(${col}) LIKE ?`);
					return { clause: conditions.join(' AND '), binds: nameParts.map(p => `%${p.toLowerCase()}%`) };
				};

				// Check if any name part is "close enough" to any word in a candidate name (uses top-level levenshtein)
				const namePartsMatch = (candidateName: string): boolean => {
					const candidateParts = candidateName.toLowerCase().replace(/[^a-z\s]/g, '').split(/\s+/).filter(p => p.length >= 2);
					let matchedParts = 0;
					for (const searchPart of nameParts) {
						const sp = searchPart.toLowerCase();
						for (const cp of candidateParts) {
							// Dynamic threshold: 1 for short words (<=4), 2 for longer
							const thresh = Math.min(sp.length, cp.length) <= 4 ? 1 : 2;
							if (cp.includes(sp) || sp.includes(cp) || levenshtein(sp, cp) <= thresh) {
								matchedParts++; break;
							}
						}
					}
					// All name parts must match (no slack for misspelling mode)
					return matchedParts >= nameParts.length;
				};

				try {
					// 1. Party cache (case info) — fuzzy match, with loose fallback
					let pcWhere = buildFuzzyWhere('client_name');
					let pcStmt = env.MEMORY_DB.prepare(
						`SELECT client_name, client_role, case_number, opposing_party, opposing_role, opposing_counsel, judge, court, case_type, district, status, folder_url FROM party_cache WHERE ${pcWhere.clause} ORDER BY last_verified DESC`
					);
					let { results: parties } = await pcStmt.bind(...pcWhere.binds).all();
					// Loose fallback if strict match found nothing
					if (parties.length === 0 && nameParts.length > 1) {
						pcWhere = buildFuzzyWhere('client_name', true);
						pcStmt = env.MEMORY_DB.prepare(
							`SELECT client_name, client_role, case_number, opposing_party, opposing_role, opposing_counsel, judge, court, case_type, district, status, folder_url FROM party_cache WHERE ${pcWhere.clause} ORDER BY last_verified DESC`
						);
						({ results: parties } = await pcStmt.bind(...pcWhere.binds).all());
					}
					// Misspelling fallback: Levenshtein match against all party_cache names
					if (parties.length === 0 && nameParts.length > 0) {
						const { results: allNames } = await env.MEMORY_DB.prepare(
							`SELECT DISTINCT client_name FROM party_cache`
						).all();
						const matched = (allNames as any[]).filter(r => namePartsMatch(r.client_name));
						if (matched.length > 0) {
							const matchedNames = matched.map(r => `'${(r.client_name as string).replace(/'/g, "''")}'`).join(',');
							const { results: fuzzyParties } = await env.MEMORY_DB.prepare(
								`SELECT client_name, client_role, case_number, opposing_party, opposing_role, opposing_counsel, judge, court, case_type, district, status, folder_url FROM party_cache WHERE client_name IN (${matchedNames}) ORDER BY last_verified DESC`
							).all();
							parties = fuzzyParties;
						}
					}

					// 2. All deadlines — fuzzy match, with loose fallback
					let dlWhere = buildFuzzyWhere('client_name');
					let dlStmt = env.MEMORY_DB.prepare(
						`SELECT client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, case_url, virtual_link, court_address, status, source, created_at, completed_at, notes FROM deadlines WHERE ${dlWhere.clause} ORDER BY due_date DESC`
					);
					let { results: deadlines } = await dlStmt.bind(...dlWhere.binds).all();
					if (deadlines.length === 0 && nameParts.length > 1) {
						dlWhere = buildFuzzyWhere('client_name', true);
						dlStmt = env.MEMORY_DB.prepare(
							`SELECT client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, case_url, virtual_link, court_address, status, source, created_at, completed_at, notes FROM deadlines WHERE ${dlWhere.clause} ORDER BY due_date DESC`
						);
						({ results: deadlines } = await dlStmt.bind(...dlWhere.binds).all());
					}
					// Misspelling fallback for deadlines
					if (deadlines.length === 0 && nameParts.length > 0) {
						const { results: allDlNames } = await env.MEMORY_DB.prepare(
							`SELECT DISTINCT client_name FROM deadlines`
						).all();
						const matchedDl = (allDlNames as any[]).filter(r => namePartsMatch(r.client_name));
						if (matchedDl.length > 0) {
							const matchedDlNames = matchedDl.map(r => `'${(r.client_name as string).replace(/'/g, "''")}'`).join(',');
							const { results: fuzzyDeadlines } = await env.MEMORY_DB.prepare(
								`SELECT client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, case_url, virtual_link, court_address, status, source, created_at, completed_at, notes FROM deadlines WHERE client_name IN (${matchedDlNames}) ORDER BY due_date DESC`
							).all();
							deadlines = fuzzyDeadlines;
						}
					}

					// 3. Case files — fuzzy match (folder names "Last, First" vs court names "FIRST MIDDLE LAST")
					let cfWhere = buildFuzzyWhere('client_name');
					let cfStmt = env.MEMORY_DB.prepare(
						`SELECT client_name, file_name, file_path, file_type, file_size, source, last_modified FROM case_files WHERE ${cfWhere.clause} ORDER BY last_modified DESC LIMIT 50`
					);
					let { results: files } = await cfStmt.bind(...cfWhere.binds).all();
					// Loose fallback for case files
					if (files.length === 0 && nameParts.length > 1) {
						const looseCf = buildFuzzyWhere('client_name', true);
						const { results: looseFiles } = await env.MEMORY_DB.prepare(
							`SELECT client_name, file_name, file_path, file_type, file_size, source, last_modified FROM case_files WHERE ${looseCf.clause} ORDER BY last_modified DESC LIMIT 50`
						).bind(...looseCf.binds).all();
						files = looseFiles;
					}
					// Levenshtein fallback for case files (misspelling tolerance)
					if (files.length === 0 && nameParts.length > 0) {
						const { results: allCfNames } = await env.MEMORY_DB.prepare(
							`SELECT DISTINCT client_name FROM case_files WHERE source = 'open'`
						).all();
						const matchedCf = (allCfNames as any[]).filter(r => namePartsMatch(r.client_name));
						if (matchedCf.length > 0) {
							const matchedCfNames = matchedCf.map(r => `'${(r.client_name as string).replace(/'/g, "''")}'`).join(',');
							const { results: fuzzyFiles } = await env.MEMORY_DB.prepare(
								`SELECT client_name, file_name, file_path, file_type, file_size, source, last_modified FROM case_files WHERE client_name IN (${matchedCfNames}) ORDER BY last_modified DESC LIMIT 50`
							).all();
							files = fuzzyFiles;
						}
					}

					// Get OneDrive download links for remote file access from any device
					let oneDriveFiles: any[] = [];
					try {
						if (env.MICROSOFT_CLIENT_ID && env.ONEDRIVE_FOLDER_ID) {
							const token = await getGraphToken();
							const searchUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${env.ONEDRIVE_FOLDER_ID}/children`;
							const folderRes = await fetch(searchUrl, { headers: { 'Authorization': `Bearer ${token}` } });
							const folderData = await folderRes.json() as any;
							const lcParts = nameParts.map(p => p.toLowerCase());
							const clientFolder = (folderData.value || []).find((f: any) => {
								const fn = f.name.toLowerCase();
								return lcParts.every(p => fn.includes(p));
							});
							if (clientFolder) {
								const filesUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${clientFolder.id}/children?$top=50&$orderby=lastModifiedDateTime desc`;
								const filesRes = await fetch(filesUrl, { headers: { 'Authorization': `Bearer ${token}` } });
								const filesData = await filesRes.json() as any;
								oneDriveFiles = (filesData.value || []).map((f: any) => ({
									file_name: f.name,
									file_size: f.size,
									last_modified: f.lastModifiedDateTime,
									file_type: f.name.split('.').pop()?.toLowerCase() || '',
									download_url: `https://api.esqs-law.com/api/onedrive/file?id=${f.id}`,
									web_url: `https://api.esqs-law.com/api/onedrive/file?id=${f.id}`,
									source: 'onedrive'
								}));
							}
						}
					} catch (odErr: any) {
						console.error('OneDrive facesheet lookup (non-fatal):', odErr.message);
					}

					// Google Drive files (esqslaw@gmail.com — ESQs case files)
					let gdriveFiles: any[] = [];
					try {
						if (env.GOOGLE_OAUTH_CLIENT_ID && env.GOOGLE_REFRESH_TOKEN) {
							const gToken = await getGmailToken();
							// Search for folders matching client name parts
							const longest = nameParts.reduce((a: string, b: string) => a.length >= b.length ? a : b, '');
							if (longest.length >= 2) {
								const folderRes = await fetch(`https://www.googleapis.com/drive/v3/files?${new URLSearchParams({
									q: `mimeType = 'application/vnd.google-apps.folder' and trashed = false and name contains '${longest.replace(/'/g, "\\'")}'`,
									fields: 'files(id, name)',
									pageSize: '10',
								})}`, { headers: { 'Authorization': `Bearer ${gToken}` } });
								const folderData = await folderRes.json() as any;
								const lcParts = nameParts.map((p: string) => p.toLowerCase());
								const matched = (folderData.files || []).filter((f: any) => lcParts.every((p: string) => f.name.toLowerCase().includes(p)));
								for (const folder of matched.slice(0, 2)) {
									const filesRes = await fetch(`https://www.googleapis.com/drive/v3/files?${new URLSearchParams({
										q: `'${folder.id}' in parents and trashed = false`,
										fields: 'files(id, name, mimeType, size, modifiedTime, webViewLink)',
										pageSize: '50',
										orderBy: 'modifiedTime desc',
									})}`, { headers: { 'Authorization': `Bearer ${gToken}` } });
									const filesData = await filesRes.json() as any;
									for (const f of (filesData.files || [])) {
										gdriveFiles.push({
											file_name: f.name,
											file_size: f.size ? parseInt(f.size) : null,
											last_modified: f.modifiedTime,
											file_type: f.mimeType?.includes('folder') ? 'folder' : (f.name.split('.').pop()?.toLowerCase() || ''),
											download_url: `https://api.esqs-law.com/api/gdrive/download/${f.id}`,
											web_url: f.webViewLink || `https://api.esqs-law.com/api/gdrive/download/${f.id}`,
											source: 'gdrive',
											folder: folder.name,
										});
									}
								}
							}
						}
					} catch (gdErr: any) {
						console.error('Google Drive facesheet lookup (non-fatal):', gdErr.message);
					}

					// Build facesheet — enriched with case_summaries intel data
					const party = (parties as any[])[0] || null;

					// Pull enriched case_summaries data for this client (has intel, facts, charges, etc.)
					let csData: any[] = [];
					try {
						const csWhere = buildFuzzyWhere('client_name');
						const { results: csRows } = await env.MEMORY_DB.prepare(
							`SELECT * FROM case_summaries WHERE ${csWhere.clause}`
						).bind(...csWhere.binds).all();
						csData = csRows as any[];
					} catch (_) {}
					const cs = csData.length > 0 ? csData[0] : null;

					// Pull judge intel if judge is known
					let judgeIntel: any = null;
					const judgeName = party?.judge || cs?.judge;
					if (judgeName) {
						try {
							const jn = (judgeName as string).toLowerCase();
							const { results: jLogs } = await env.MEMORY_DB.prepare(
								`SELECT activity_type, activity_subtype, outcome, party_role, date FROM judge_activity_log WHERE LOWER(judge_name) LIKE ? ORDER BY date DESC`
							).bind(`%${jn}%`).all();
							if (jLogs.length >= 2) {
								judgeIntel = { predictions: computePredictions(jLogs as any[]), log_count: jLogs.length };
							}
							// Reversal factors
							const { results: revs } = await env.MEMORY_DB.prepare(
								`SELECT activity_type, typical_outcome, actual_outcome, reversal_factors, specific_arguments, date FROM judge_ruling_rationale WHERE LOWER(judge_name) LIKE ? ORDER BY date DESC`
							).bind(`%${jn}%`).all();
							if (revs.length > 0) {
								if (!judgeIntel) judgeIntel = {};
								judgeIntel.reversals = revs;
							}
						} catch (_) {}
					}

					// Pull OC intel if opposing_counsel is known
					let ocIntel: any = null;
					const ocName = party?.opposing_counsel || cs?.opposing_counsel;
					if (ocName) {
						try {
							const cn = (ocName as string).toLowerCase();
							const { results: oLogs } = await env.MEMORY_DB.prepare(
								`SELECT activity_type, activity_subtype, outcome, party_role, date FROM oc_activity_log WHERE LOWER(counsel_name) LIKE ? ORDER BY date DESC`
							).bind(`%${cn}%`).all();
							if (oLogs.length >= 2) {
								ocIntel = { predictions: computePredictions(oLogs as any[]), log_count: oLogs.length };
							}
						} catch (_) {}
					}

					const facesheet = {
						client_name: party?.client_name || cs?.client_name || clientName,
						case_number: party?.case_number || cs?.case_number || null,
						case_type: party?.case_type || cs?.case_type || null,
						client_role: party?.client_role || cs?.client_role || null,
						opposing_party: party?.opposing_party || cs?.opposing_party || null,
						opposing_role: party?.opposing_role || cs?.opposing_role || null,
						opposing_counsel: party?.opposing_counsel || cs?.opposing_counsel || null,
						judge: judgeName || null,
						court: party?.court || cs?.court || null,
						district: party?.district || cs?.district || null,
						// Client contact
						client_email: cs?.client_email || null,
						client_phone: cs?.client_phone || null,
						client_address: cs?.client_address || null,
						// OC details
						opposing_counsel_phone: cs?.opposing_counsel_phone || null,
						opposing_counsel_email: cs?.opposing_counsel_email || null,
						opposing_counsel_firm: cs?.opposing_counsel_firm || null,
						// Case substance
						facts: cs?.facts || null,
						charges: cs?.charges || null,
						additional_parties: cs?.additional_parties || party?.additional_parties || null,
						notes: cs?.notes || null,
						// Key deadlines
						discovery_deadline: cs?.discovery_deadline || null,
						dispositive_deadline: cs?.dispositive_deadline || null,
						trial_date: cs?.trial_date || null,
						statute_of_limitations: cs?.statute_of_limitations || null,
						// Intel overlays
						judge_prediction: cs?.judge_prediction || null,
						oc_prediction: cs?.oc_prediction || null,
						reversal_factors: cs?.reversal_factors || null,
						// File/folder
						folder_url: party?.folder_url || cs?.folder_url || null,
						file_count: cs?.file_count || 0,
						// All cases for this client
						all_cases: parties,
						all_case_summaries: csData,
					};

					// Build timeline (merge deadlines into chronological history)
					const timeline = (deadlines as any[]).map(d => ({
						date: d.due_date,
						time: d.hearing_time,
						type: d.deadline_type,
						description: d.description,
						court: d.court,
						courtroom: d.courtroom,
						judge: d.judge,
						hearing_mode: d.hearing_mode,
						case_url: d.case_url,
						virtual_link: d.virtual_link,
						court_address: d.court_address,
						status: d.status,
						source: d.source,
						case_number: d.case_number,
						notes: d.notes,
						completed_at: d.completed_at,
					}));

					return json({
						success: true,
						facesheet,
						timeline,
						files,
						oneDriveFiles,
						gdriveFiles,
						intel: {
							judge: judgeIntel,
							opposing_counsel: ocIntel,
						},
						counts: {
							cases: (parties as any[]).length,
							events: (deadlines as any[]).length,
							files: (files as any[]).length,
							oneDriveFiles: oneDriveFiles.length,
							gdriveFiles: gdriveFiles.length,
						}
					});
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// DOCUMENTS (R2)
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/documents' && request.method === 'GET') {
				const caseId = url.searchParams.get('case_id');
				let q = 'SELECT * FROM documents WHERE 1=1';
				const p: any[] = [];
				if (caseId) { q += ' AND case_id = ?'; p.push(caseId); }
				q += ' ORDER BY created_date DESC';
				const { results } = await env.DB.prepare(q).bind(...p).all();
				return json({ success: true, documents: results });
			}
			
			if (path === '/api/documents/upload' && request.method === 'POST') {
				const formData = await request.formData();
				const file = formData.get('file') as File;
				const caseId = formData.get('case_id') as string;
				const docType = formData.get('doc_type') as string;
				if (!file) return err('No file provided', 400);
				
				const r2Key = `documents/${caseId || 'general'}/${Date.now()}-${file.name}`;
				await env.DOCUMENTS.put(r2Key, file.stream(), { customMetadata: { originalName: file.name } });
				const r = await env.DB.prepare(
					`INSERT INTO documents (case_id, doc_name, doc_type, r2_key, file_size, created_date) VALUES (?, ?, ?, ?, ?, ?)`
				).bind(caseId, file.name, docType, r2Key, file.size, mtnISO()).run();
				return json({ success: true, id: r.meta.last_row_id, r2_key: r2Key });
			}
			
			const docMatch = path.match(/^\/api\/documents\/download\/(\d+)$/);
			if (docMatch && request.method === 'GET') {
				const doc = await env.DB.prepare('SELECT * FROM documents WHERE id = ?').bind(docMatch[1]).first() as any;
				if (!doc) return err('Not found', 404);
				const obj = await env.DOCUMENTS.get(doc.r2_key);
				if (!obj) return err('File not found', 404);
				return new Response(obj.body, {
					headers: { 'Content-Disposition': `attachment; filename="${doc.doc_name}"`, ...corsHeaders }
				});
			}

			// GET /api/onedrive/file?id=xxx or ?path=xxx — proxy file view (inline preview + download option)
			if (path === '/api/onedrive/file' && request.method === 'GET') {
				const filePath = url.searchParams.get('path');
				const itemId = url.searchParams.get('id');
				const forceDownload = url.searchParams.get('download') === '1';
				if (!filePath && !itemId) return err('path or id required', 400);
				try {
					const token = await getGraphToken();
					let item: any = null;

					if (itemId) {
						const res = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${itemId}`, {
							headers: { 'Authorization': `Bearer ${token}` }
						});
						item = await res.json() as any;
					} else if (filePath) {
						const encodedPath = encodeURIComponent(filePath).replace(/%2F/g, '/');
						const res = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${env.ONEDRIVE_FOLDER_ID}:/${encodedPath}`, {
							headers: { 'Authorization': `Bearer ${token}` }
						});
						item = await res.json() as any;
					}

					const downloadUrl = item?.['@microsoft.graph.downloadUrl'];
					if (!downloadUrl) return err('File not found', 404);

					const fileName = item.name || 'document';
					const ext = fileName.split('.').pop()?.toLowerCase() || '';

					// If ?download=1, redirect to direct download
					if (forceDownload) {
						return Response.redirect(downloadUrl, 302);
					}

					// For PDFs, stream inline so browser previews them
					if (ext === 'pdf') {
						const fileRes = await fetch(downloadUrl);
						return new Response(fileRes.body, {
							headers: {
								'Content-Type': 'application/pdf',
								'Content-Disposition': `inline; filename="${fileName}"`,
								...corsHeaders
							}
						});
					}

					// For everything else (docx, xlsx, etc), show a preview page with download button
					const esc = (s: string) => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
					const safeName = esc(fileName);
					const sizeKB = ((item.size || 0) / 1024).toFixed(1);
					const modified = (item.lastModifiedDateTime || '').substring(0, 10);
					const downloadLink = `https://api.esqs-law.com/api/onedrive/file?${itemId ? 'id=' + encodeURIComponent(itemId) : 'path=' + encodeURIComponent(filePath || '')}&download=1`;
					// Use Office Online preview if available
					const previewUrl = itemId ? `https://graph.microsoft.com/v1.0/me/drive/items/${encodeURIComponent(itemId)}/preview` : null;
					let embedUrl = '';
					if (previewUrl) {
						try {
							const prevRes = await fetch(previewUrl, {
								method: 'POST',
								headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
								body: '{}'
							});
							const prevData = await prevRes.json() as any;
							embedUrl = prevData.getUrl || '';
						} catch (_) {}
					}

					const html = `<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${safeName} — ESQs Law</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#1a1a2e;color:#e0e0e0;height:100vh;display:flex;flex-direction:column}
.toolbar{background:#16213e;padding:12px 20px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #0f3460}
.toolbar h3{font-size:15px;color:#e94560}
.file-info{font-size:12px;color:#888;margin-left:12px}
.btn{background:#e94560;color:#fff;border:none;padding:8px 20px;border-radius:6px;cursor:pointer;font-size:14px;text-decoration:none;display:inline-flex;align-items:center;gap:6px}
.btn:hover{background:#c73650}
.preview{flex:1;display:flex;align-items:center;justify-content:center}
.preview iframe{width:100%;height:100%;border:none}
.no-preview{text-align:center;padding:40px}
.no-preview .icon{font-size:64px;margin-bottom:16px}
.no-preview p{color:#888;margin:8px 0}
</style></head><body>
<div class="toolbar">
<div style="display:flex;align-items:center">
<h3>📄 ${safeName}</h3>
<span class="file-info">${sizeKB} KB · Modified ${modified}</span>
</div>
<a href="${downloadLink}" class="btn">⬇ Download</a>
</div>
<div class="preview">
${embedUrl ? `<iframe src="${esc(embedUrl)}"></iframe>` : `
<div class="no-preview">
<div class="icon">📄</div>
<h2>${safeName}</h2>
<p>${esc(ext.toUpperCase())} file · ${sizeKB} KB</p>
<p style="margin-top:20px"><a href="${downloadLink}" class="btn" style="font-size:16px;padding:12px 32px">⬇ Download File</a></p>
</div>`}
</div>
</body></html>`;
					return new Response(html, {
						headers: { 'Content-Type': 'text/html; charset=utf-8', ...corsHeaders }
					});
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// EMAIL (Microsoft Graph API — Outlook)
			// ═══════════════════════════════════════════════════════════════

			// Helper: Get Microsoft Graph access token (OAuth2 refresh token flow)
			// (defined once here, used by both email and OneDrive)
			async function getGraphToken(): Promise<string> {
				const cached = await env.CACHE.get('ms_graph_token');
				if (cached) return cached;
				const tokenUrl = `https://login.microsoftonline.com/common/oauth2/v2.0/token`;
				const res = await fetch(tokenUrl, {
					method: 'POST',
					headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
					body: new URLSearchParams({
						client_id: env.MICROSOFT_CLIENT_ID,
						client_secret: env.MICROSOFT_CLIENT_SECRET,
						refresh_token: env.MICROSOFT_REFRESH_TOKEN,
						grant_type: 'refresh_token',
						scope: 'https://graph.microsoft.com/User.Read https://graph.microsoft.com/Files.ReadWrite https://graph.microsoft.com/Files.ReadWrite.All https://graph.microsoft.com/Mail.Read https://graph.microsoft.com/Mail.Send https://graph.microsoft.com/Mail.ReadWrite offline_access',
					})
				});
				const data = await res.json() as any;
				if (!data.access_token) {
					console.error('Graph token error:', JSON.stringify(data));
					throw new Error(`Failed to get Graph token: ${data.error_description || data.error || 'Unknown error'}`);
				}
				await env.CACHE.put('ms_graph_token', data.access_token, { expirationTtl: 3000 });
				if (data.refresh_token) await env.CACHE.put('ms_refresh_token_latest', data.refresh_token);
				return data.access_token;
			}

			// --- Google OAuth2 token helper (esqslaw@gmail.com — Gmail + Drive) ---
			async function getGmailToken(): Promise<string> {
				const cached = await env.CACHE.get('gmail_access_token');
				if (cached) return cached;
				const res = await fetch('https://oauth2.googleapis.com/token', {
					method: 'POST',
					headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
					body: new URLSearchParams({
						grant_type: 'refresh_token',
						client_id: env.GOOGLE_OAUTH_CLIENT_ID,
						client_secret: env.GOOGLE_OAUTH_CLIENT_SECRET,
						refresh_token: env.GOOGLE_REFRESH_TOKEN,
					})
				});
				const data = await res.json() as any;
				if (!data.access_token) {
					console.error('Google token error:', JSON.stringify(data));
					throw new Error(`Failed to get Google token: ${data.error_description || data.error || 'Unknown error'}`);
				}
				// Log granted scope for debugging
				if (data.scope) console.log('Google token scopes:', data.scope);
				await env.CACHE.put('gmail_access_token', data.access_token, { expirationTtl: 3000 });
				return data.access_token;
			}

			async function sendViaGmail(to: string, subject: string, body: string, cc?: string): Promise<{ success: boolean; error?: string }> {
				try {
					const token = await getGmailToken();
					// ALL emails send from esqslaw@gmail.com directly (no alias — alias can't deliver externally)
					const messageParts = [
						'From: Pitcher Law PLLC <esqslaw@gmail.com>',
						`To: ${to}`,
						cc ? `Cc: ${cc}` : '',
						`Subject: ${subject}`,
						'MIME-Version: 1.0',
						'Content-Type: text/html; charset=utf-8',
						'',
						body.replace(/\n/g, '<br>')
					].filter(line => line);

					const raw = btoa(unescape(encodeURIComponent(messageParts.join('\r\n'))))
						.replace(/\+/g, '-')
						.replace(/\//g, '_')
						.replace(/=+$/, '');

					const sendRes = await fetch('https://www.googleapis.com/gmail/v1/users/me/messages/send', {
						method: 'POST',
						headers: {
							'Authorization': `Bearer ${token}`,
							'Content-Type': 'application/json'
						},
						body: JSON.stringify({ raw })
					});

					if (sendRes.ok) {
						const result = await sendRes.json() as any;
						console.log(`Gmail SENT: from=esqslaw@gmail.com to=${to} cc=${cc || 'none'} subject="${subject}" msgId=${result.id}`);
						return { success: true };
					} else {
						const errText = await sendRes.text();
						console.error(`Gmail send failed (${sendRes.status}):`, errText);
						return { success: false, error: `Gmail API ${sendRes.status}: ${errText}` };
					}
				} catch (e: any) {
					console.error('Gmail send error:', e.message);
					return { success: false, error: e.message };
				}
			}

			// GET /api/email/inbox — Read recent emails (optionally search/filter by client)
			if (path === '/api/email/inbox' && request.method === 'GET') {
				try {
					const token = await getGraphToken();
					const url = new URL(request.url);
					const search = url.searchParams.get('search') || '';
					const limit = Math.min(parseInt(url.searchParams.get('limit') || '15'), 50);
					const folder = url.searchParams.get('folder') || 'inbox';

					let graphUrl = `https://graph.microsoft.com/v1.0/me/mailFolders/${folder}/messages?$top=${limit}&$select=subject,from,toRecipients,ccRecipients,receivedDateTime,bodyPreview,body,isRead,conversationId,id,hasAttachments`;
					if (search) {
						graphUrl += `&$search="${encodeURIComponent(search)}"`;
					} else {
						graphUrl += `&$orderby=receivedDateTime desc`;
					}

					const emailRes = await fetch(graphUrl, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const emailData = await emailRes.json() as any;

					if (emailData.error) {
						return json({ success: false, error: emailData.error.message }, 500);
					}

					// Resolve contact identities from D1
					const emails = await Promise.all((emailData.value || []).map(async (e: any) => {
						const fromEmail = e.from?.emailAddress?.address?.toLowerCase() || '';
						let senderIdentity = null;

						// Check court_contacts
						if (fromEmail.endsWith('@utcourts.gov')) {
							const cc = await env.MEMORY_DB.prepare(
								'SELECT name, title, court, phone FROM court_contacts WHERE LOWER(email) = ?'
							).bind(fromEmail).first() as any;
							if (cc) senderIdentity = { name: cc.name, role: cc.title, org: cc.court, phone: cc.phone };
							else senderIdentity = { name: fromEmail.split('@')[0], role: 'Utah Courts Staff', org: 'Utah Courts' };
						}

						// Check opposing_counsel_intel
						if (!senderIdentity) {
							const oc = await env.MEMORY_DB.prepare(
								'SELECT counsel_name, firm, phone FROM opposing_counsel_intel WHERE LOWER(email) = ?'
							).bind(fromEmail).first() as any;
							if (oc) senderIdentity = { name: oc.counsel_name, role: 'Opposing Counsel', org: oc.firm, phone: oc.phone };
						}

						// Known firm addresses
						if (!senderIdentity && (fromEmail === 'pd@dianepitcher.com' || fromEmail === 'diane@dianepitcher.com' || fromEmail === 'esqslaw@gmail.com')) {
							senderIdentity = { name: 'ESQs Law', role: 'Our Firm', org: 'Pitcher Law PLLC' };
						}

						return {
							id: e.id,
							subject: e.subject || '(no subject)',
							from: e.from?.emailAddress?.address || '',
							fromName: e.from?.emailAddress?.name || '',
							to: (e.toRecipients || []).map((r: any) => r.emailAddress?.address).join(', '),
							cc: (e.ccRecipients || []).map((r: any) => r.emailAddress?.address).join(', '),
							date: e.receivedDateTime,
							preview: e.bodyPreview || '',
							body: e.body?.content || '',
							isRead: e.isRead,
							hasAttachments: e.hasAttachments,
							conversationId: e.conversationId,
							senderIdentity
						};
					}));

					return json({ success: true, emails, count: emails.length });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// GET /api/email/client/:name — Get emails for a specific client
			const emailClientMatch = path.match(/^\/api\/email\/client\/(.+)$/);
			if (emailClientMatch && request.method === 'GET') {
				const clientName = decodeURIComponent(emailClientMatch[1]);
				try {
					const token = await getGraphToken();
					const nameParts = clientName.split(/\s+/).filter((p: string) => p.length > 2);
					const searchTerm = nameParts[nameParts.length - 1] || clientName;

					const graphUrl = `https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$top=15&$search="${encodeURIComponent(searchTerm)}"&$select=subject,from,toRecipients,receivedDateTime,bodyPreview,body,isRead,conversationId,id,hasAttachments`;

					const emailRes = await fetch(graphUrl, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const emailData = await emailRes.json() as any;
					if (emailData.error) return json({ success: false, error: emailData.error.message }, 500);

					const emails = (emailData.value || []).map((e: any) => ({
						id: e.id,
						subject: e.subject || '(no subject)',
						from: e.from?.emailAddress?.address || '',
						fromName: e.from?.emailAddress?.name || '',
						date: e.receivedDateTime,
						preview: e.bodyPreview || '',
						body: e.body?.content || '',
						isRead: e.isRead,
						hasAttachments: e.hasAttachments,
						conversationId: e.conversationId
					}));

					return json({ success: true, client: clientName, emails, count: emails.length });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// POST /api/email/send — Send an email
			if (path === '/api/email/send' && request.method === 'POST') {
				try {
					const { to, subject, body, cc, replyToId, isHtml } = await request.json() as any;
					if (!to || !subject) return json({ success: false, error: 'to and subject required' }, 400);

					const token = await getGraphToken();
					const toRecipients = to.split(',').map((e: string) => ({ emailAddress: { address: e.trim() } }));
					const ccRecipients = cc ? cc.split(',').map((e: string) => ({ emailAddress: { address: e.trim() } })) : [];

					if (replyToId) {
						// Reply to existing thread
						const replyRes = await fetch(`https://graph.microsoft.com/v1.0/me/messages/${replyToId}/reply`, {
							method: 'POST',
							headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
							body: JSON.stringify({ comment: body || '' })
						});
						return json({ success: replyRes.status === 202 || replyRes.status === 200, type: 'reply' });
					}

					// Send via Graph API from Associate@ (SendAs granted)
					const mailBody = {
						message: {
							subject,
							body: { contentType: (isHtml !== false) ? 'HTML' : 'Text', content: body || '' },
							toRecipients,
							...(ccRecipients.length > 0 && { ccRecipients }),
							from: { emailAddress: { address: 'Associate@dianepitcher.com', name: 'Pitcher Law PLLC' } }
						},
						saveToSentItems: true
					};
					const sendRes = await fetch('https://graph.microsoft.com/v1.0/me/sendMail', {
						method: 'POST',
						headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
						body: JSON.stringify(mailBody)
					});
					return json({ success: sendRes.status === 202 || sendRes.status === 200, type: 'new', from: 'Associate@dianepitcher.com' });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// ONEDRIVE / FILES (Microsoft Graph API)
			// ═══════════════════════════════════════════════════════════════

			// Note: getGraphToken() is defined above in the Email section (~line 3072)

			// List files for a client from OneDrive
			const filesListMatch = path.match(/^\/api\/files\/list\/(.+)$/);
			if (filesListMatch && request.method === 'GET') {
				const clientName = decodeURIComponent(filesListMatch[1]);
				
				try {
					const token = await getGraphToken();
					// Use /me/drive for user's OneDrive - search in Open Cases folder by ID
					const searchUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${env.ONEDRIVE_FOLDER_ID}/children`;
					const folderRes = await fetch(searchUrl, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const folderData = await folderRes.json() as any;
					
					// Find folder matching client name
					const clientFolder = (folderData.value || []).find((f: any) => 
						f.name.toLowerCase().includes(clientName.toLowerCase()) || 
						clientName.toLowerCase().includes(f.name.toLowerCase())
					);
					
					if (!clientFolder) {
						return json({ success: true, files: [], message: 'No folder found for client', availableFolders: (folderData.value || []).map((f: any) => f.name).slice(0, 10) });
					}
					
					// Get files in client folder
					const filesUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${clientFolder.id}/children`;
					const filesRes = await fetch(filesUrl, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const filesData = await filesRes.json() as any;
					
					const files = (filesData.value || []).map((f: any) => ({
						id: f.id,
						name: f.name,
						size: f.size,
						modified: f.lastModifiedDateTime,
						type: f.file ? f.file.mimeType : 'folder',
						downloadUrl: f['@microsoft.graph.downloadUrl'] || null
					}));
					
					return json({ success: true, files, client: clientName });
				} catch (error: any) {
					console.error('OneDrive files error:', error);
					return json({ success: false, error: error.message, files: [] });
				}
			}
			
			// ═══════════════════════════════════════════════════════════════
			// GOOGLE DRIVE (esqslaw@gmail.com — ESQs case files)
			// Uses same Google OAuth creds as Gmail (getGmailToken)
			// ═══════════════════════════════════════════════════════════════

			// GET /api/gdrive/files — List files/folders in Google Drive
			// ?q=search term  ?folderId=xxx  ?pageSize=50  ?pageToken=xxx
			if (path === '/api/gdrive/files' && request.method === 'GET') {
				try {
					const token = await getGmailToken();
					const folderId = url.searchParams.get('folderId') || 'root';
					const searchQuery = url.searchParams.get('q') || '';
					const pageSize = Math.min(parseInt(url.searchParams.get('pageSize') || '50'), 100);
					const pageToken = url.searchParams.get('pageToken') || '';

					// Build Drive API query
					let driveQ = `'${folderId}' in parents and trashed = false`;
					if (searchQuery) {
						// Search by name within the folder (or globally if folderId=root)
						driveQ = searchQuery.length > 0
							? `name contains '${searchQuery.replace(/'/g, "\\'")}' and trashed = false`
							: driveQ;
					}

					const params = new URLSearchParams({
						q: driveQ,
						fields: 'nextPageToken, files(id, name, mimeType, size, modifiedTime, parents, webViewLink, iconLink, thumbnailLink)',
						pageSize: String(pageSize),
						orderBy: 'folder, name',
						supportsAllDrives: 'true',
						includeItemsFromAllDrives: 'true',
					});
					if (pageToken) params.set('pageToken', pageToken);

					const driveRes = await fetch(`https://www.googleapis.com/drive/v3/files?${params}`, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const driveData = await driveRes.json() as any;

					if (driveData.error) {
						return json({ success: false, error: driveData.error.message || 'Drive API error' }, driveData.error.code || 500);
					}

					const files = (driveData.files || []).map((f: any) => ({
						id: f.id,
						name: f.name,
						mimeType: f.mimeType,
						isFolder: f.mimeType === 'application/vnd.google-apps.folder',
						size: f.size ? parseInt(f.size) : null,
						modified: f.modifiedTime,
						webUrl: f.webViewLink || null,
						icon: f.iconLink || null,
						thumbnail: f.thumbnailLink || null,
					}));

					return json({
						success: true,
						folderId,
						files,
						count: files.length,
						nextPageToken: driveData.nextPageToken || null
					});
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// GET /api/gdrive/client/:name — List case files for a client from Google Drive
			// Searches for a folder matching the client name, then lists its contents
			const gdriveClientMatch = path.match(/^\/api\/gdrive\/client\/(.+)$/);
			if (gdriveClientMatch && request.method === 'GET') {
				const clientName = decodeURIComponent(gdriveClientMatch[1]);
				try {
					const token = await getGmailToken();

					// Step 1: Find folder(s) matching client name
					const nameParts = clientName.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter((p: string) => p.length >= 2);
					// Search for folders containing all name parts
					let folderQ = `mimeType = 'application/vnd.google-apps.folder' and trashed = false`;
					if (nameParts.length > 0) {
						// Google Drive search only supports single `name contains` — use longest part for best match
						const longest = nameParts.reduce((a: string, b: string) => a.length >= b.length ? a : b);
						folderQ += ` and name contains '${longest.replace(/'/g, "\\'")}'`;
					} else {
						folderQ += ` and name contains '${clientName.replace(/'/g, "\\'")}'`;
					}

					const folderRes = await fetch(`https://www.googleapis.com/drive/v3/files?${new URLSearchParams({
						q: folderQ,
						fields: 'files(id, name, modifiedTime)',
						pageSize: '20',
						orderBy: 'modifiedTime desc',
					})}`, { headers: { 'Authorization': `Bearer ${token}` } });
					const folderData = await folderRes.json() as any;

					if (folderData.error) {
						return json({ success: false, error: folderData.error.message }, folderData.error.code || 500);
					}

					// Filter folders — all name parts must appear in folder name
					const lcParts = nameParts.map((p: string) => p.toLowerCase());
					const matchedFolders = (folderData.files || []).filter((f: any) => {
						const fn = f.name.toLowerCase();
						return lcParts.every((p: string) => fn.includes(p));
					});

					if (matchedFolders.length === 0) {
						// Return available folder names for debugging
						return json({
							success: true,
							client: clientName,
							files: [],
							folders: [],
							message: 'No matching folder found in Google Drive',
							searchedFolders: (folderData.files || []).map((f: any) => f.name).slice(0, 15)
						});
					}

					// Step 2: List files in each matched folder
					const allFiles: any[] = [];
					const folderNames: string[] = [];
					for (const folder of matchedFolders.slice(0, 3)) {
						folderNames.push(folder.name);
						const filesRes = await fetch(`https://www.googleapis.com/drive/v3/files?${new URLSearchParams({
							q: `'${folder.id}' in parents and trashed = false`,
							fields: 'files(id, name, mimeType, size, modifiedTime, webViewLink, iconLink)',
							pageSize: '100',
							orderBy: 'modifiedTime desc',
						})}`, { headers: { 'Authorization': `Bearer ${token}` } });
						const filesData = await filesRes.json() as any;

						for (const f of (filesData.files || [])) {
							allFiles.push({
								id: f.id,
								name: f.name,
								mimeType: f.mimeType,
								isFolder: f.mimeType === 'application/vnd.google-apps.folder',
								size: f.size ? parseInt(f.size) : null,
								modified: f.modifiedTime,
								webUrl: f.webViewLink || null,
								icon: f.iconLink || null,
								folder: folder.name,
								source: 'gdrive',
							});
						}
					}

					return json({
						success: true,
						client: clientName,
						folders: folderNames,
						files: allFiles,
						count: allFiles.length
					});
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// GET /api/gdrive/download/:fileId — Download/preview a Google Drive file
			const gdriveDownloadMatch = path.match(/^\/api\/gdrive\/download\/(.+)$/);
			if (gdriveDownloadMatch && request.method === 'GET') {
				const fileId = decodeURIComponent(gdriveDownloadMatch[1]);
				const forceDownload = url.searchParams.get('download') === '1';
				try {
					const token = await getGmailToken();

					// Get file metadata first
					const metaRes = await fetch(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?fields=id,name,mimeType,size,webViewLink,webContentLink`, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const meta = await metaRes.json() as any;
					if (meta.error) return json({ success: false, error: meta.error.message }, meta.error.code || 404);

					const fileName = meta.name || 'document';
					const mimeType = meta.mimeType || 'application/octet-stream';

					// Google Docs/Sheets/Slides → export as PDF
					const isGoogleDoc = mimeType.startsWith('application/vnd.google-apps.');
					if (isGoogleDoc) {
						const exportMime = mimeType.includes('spreadsheet') ? 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
							: mimeType.includes('presentation') ? 'application/vnd.openxmlformats-officedocument.presentationml.presentation'
							: 'application/pdf';
						const exportExt = mimeType.includes('spreadsheet') ? 'xlsx' : mimeType.includes('presentation') ? 'pptx' : 'pdf';
						const exportRes = await fetch(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}/export?mimeType=${encodeURIComponent(exportMime)}`, {
							headers: { 'Authorization': `Bearer ${token}` }
						});
						if (!exportRes.ok) return json({ success: false, error: `Export failed: ${exportRes.status}` }, 500);
						return new Response(exportRes.body, {
							headers: {
								'Content-Type': exportMime,
								'Content-Disposition': `${forceDownload ? 'attachment' : 'inline'}; filename="${fileName}.${exportExt}"`,
								...corsHeaders
							}
						});
					}

					// Regular files — stream directly
					const fileRes = await fetch(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?alt=media`, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					if (!fileRes.ok) return json({ success: false, error: `Download failed: ${fileRes.status}` }, 500);

					const ext = fileName.split('.').pop()?.toLowerCase() || '';
					const disposition = (ext === 'pdf' && !forceDownload) ? 'inline' : (forceDownload ? 'attachment' : 'inline');

					return new Response(fileRes.body, {
						headers: {
							'Content-Type': mimeType,
							'Content-Disposition': `${disposition}; filename="${fileName}"`,
							...corsHeaders
						}
					});
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// PREDICTIVE INTELLIGENCE — Judge & OC Activity Logs + Predictions
			// RESTRICTED: admin/attorney only — NOT available to client portal
			// ═══════════════════════════════════════════════════════════════

			if (path.startsWith('/api/intel/')) {
				const auth = request.headers.get('Authorization');
				let sessionRole = 'anonymous';
				if (auth?.startsWith('Bearer ')) {
					const session = await env.SESSIONS.get(auth.substring(7));
					if (session) {
						const s = JSON.parse(session);
						sessionRole = s.role || 'admin';
					}
				}
				// Allow: admin, attorney, paralegal. Block: client, anonymous
				const allowedRoles = ['admin', 'attorney', 'paralegal'];
				if (!allowedRoles.includes(sessionRole)) {
					return err('Unauthorized — intel endpoints require attorney-level access', 403);
				}
			}

			// POST /api/intel/judge/log — Log a judge activity
			if (path === '/api/intel/judge/log' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const { judge_name, case_number, client_name, activity_type, activity_subtype, outcome, details, our_position, opposing_counsel, severity_context, party_role, date } = body;
					if (!judge_name || !activity_type || !outcome) return err('judge_name, activity_type, and outcome required', 400);
					const logDate = date || mtnToday();
					await env.MEMORY_DB.prepare(
						`INSERT INTO judge_activity_log (judge_name, case_number, client_name, activity_type, activity_subtype, outcome, details, our_position, opposing_counsel, severity_context, party_role, date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(judge_name, case_number || null, client_name || null, activity_type, activity_subtype || null, outcome, details || null, our_position || null, opposing_counsel || null, severity_context || null, party_role || '', logDate, mtnISO()).run();
					// RAG store for semantic search
					try {
						const content = `[Judge Activity] ${judge_name} — ${activity_type}${activity_subtype ? '/' + activity_subtype : ''}: ${outcome}${party_role ? ' (we were ' + party_role + ')' : ''}. ${details || ''} Case: ${case_number || 'N/A'}, Client: ${client_name || 'N/A'}, Date: ${logDate}`;
						await ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
							id: `jlog_${Date.now()}`, type: 'case_knowledge', source: 'judge_activity',
							content, clientName: client_name || '', caseNumber: case_number || '',
						});
					} catch (_) {}
					return json({ success: true, message: `Logged: ${judge_name} — ${activity_type} → ${outcome}` });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/judge/log/:name — Get all logs for a judge
			if (path.startsWith('/api/intel/judge/log/') && request.method === 'GET') {
				try {
					const judgeName = decodeURIComponent(path.replace('/api/intel/judge/log/', ''));
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT * FROM judge_activity_log WHERE LOWER(judge_name) LIKE ? ORDER BY date DESC`
					).bind(`%${judgeName.toLowerCase()}%`).all();
					return json({ success: true, judge: judgeName, logs: results, count: results.length });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/intel/oc/log — Log an OC activity
			if (path === '/api/intel/oc/log' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const { counsel_name, firm, case_number, client_name, activity_type, activity_subtype, outcome, details, our_position, strength_shown, weakness_shown, party_role, date } = body;
					if (!counsel_name || !activity_type || !outcome) return err('counsel_name, activity_type, and outcome required', 400);
					const logDate = date || mtnToday();
					await env.MEMORY_DB.prepare(
						`INSERT INTO oc_activity_log (counsel_name, firm, case_number, client_name, activity_type, activity_subtype, outcome, details, our_position, strength_shown, weakness_shown, party_role, date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(counsel_name, firm || null, case_number || null, client_name || null, activity_type, activity_subtype || null, outcome, details || null, our_position || null, strength_shown || null, weakness_shown || null, party_role || '', logDate, mtnISO()).run();
					// RAG store
					try {
						const content = `[OC Activity] ${counsel_name}${firm ? ' (' + firm + ')' : ''} — ${activity_type}${activity_subtype ? '/' + activity_subtype : ''}: ${outcome}${party_role ? ' (we were ' + party_role + ')' : ''}. ${details || ''} Case: ${case_number || 'N/A'}, Date: ${logDate}`;
						await ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
							id: `oclog_${Date.now()}`, type: 'case_knowledge', source: 'oc_activity',
							content, clientName: client_name || '', caseNumber: case_number || '',
						});
					} catch (_) {}
					return json({ success: true, message: `Logged: ${counsel_name} — ${activity_type} → ${outcome}` });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/oc/log/:name — Get all logs for an OC
			if (path.startsWith('/api/intel/oc/log/') && request.method === 'GET') {
				try {
					const counselName = decodeURIComponent(path.replace('/api/intel/oc/log/', ''));
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT * FROM oc_activity_log WHERE LOWER(counsel_name) LIKE ? ORDER BY date DESC`
					).bind(`%${counselName.toLowerCase()}%`).all();
					return json({ success: true, counsel: counselName, logs: results, count: results.length });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/judge/predict/:name — Predictions for a judge
			if (path.startsWith('/api/intel/judge/predict/') && request.method === 'GET') {
				try {
					const judgeName = decodeURIComponent(path.replace('/api/intel/judge/predict/', ''));
					const { results: logs } = await env.MEMORY_DB.prepare(
						`SELECT * FROM judge_activity_log WHERE LOWER(judge_name) LIKE ? ORDER BY date ASC`
					).bind(`%${judgeName.toLowerCase()}%`).all();
					// Also get static profile
					const profile = await env.MEMORY_DB.prepare(
						`SELECT * FROM judge_intel WHERE LOWER(judge_name) LIKE ?`
					).bind(`%${judgeName.toLowerCase()}%`).first();
					const predictions = computePredictions(logs as any[]);
					// Ruling rationale — why they rule the way they do + reversal factors
					const { results: rationale } = await env.MEMORY_DB.prepare(
						`SELECT activity_type, activity_subtype, actual_outcome, ruling_reasoning, reversal_factors, specific_arguments, applicability_notes, is_reversal, case_type, date FROM judge_ruling_rationale WHERE LOWER(judge_name) LIKE ? ORDER BY date DESC LIMIT 30`
					).bind(`%${judgeName.toLowerCase()}%`).all();
					const reversals = (rationale || []).filter((r: any) => r.is_reversal);
					return json({ success: true, judge: judgeName, profile, predictions, rationale: rationale || [], reversals, total_logs: logs.length });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/oc/predict/:name — Predictions for an OC
			if (path.startsWith('/api/intel/oc/predict/') && request.method === 'GET') {
				try {
					const counselName = decodeURIComponent(path.replace('/api/intel/oc/predict/', ''));
					const { results: logs } = await env.MEMORY_DB.prepare(
						`SELECT * FROM oc_activity_log WHERE LOWER(counsel_name) LIKE ? ORDER BY date ASC`
					).bind(`%${counselName.toLowerCase()}%`).all();
					const profile = await env.MEMORY_DB.prepare(
						`SELECT * FROM opposing_counsel_intel WHERE LOWER(counsel_name) LIKE ?`
					).bind(`%${counselName.toLowerCase()}%`).first();
					const predictions = computePredictions(logs as any[]);
					return json({ success: true, counsel: counselName, profile, predictions, total_logs: logs.length });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/predictions — All predictions (dashboard overview)
			if (path === '/api/intel/predictions' && request.method === 'GET') {
				try {
					// Judge predictions
					const { results: judgeLogs } = await env.MEMORY_DB.prepare('SELECT * FROM judge_activity_log ORDER BY date ASC').all();
					const judgeGroups = new Map<string, any[]>();
					for (const l of judgeLogs as any[]) {
						const n = l.judge_name as string;
						if (!judgeGroups.has(n)) judgeGroups.set(n, []);
						judgeGroups.get(n)!.push(l);
					}
					const judgePredictions: any[] = [];
					for (const [name, logs] of judgeGroups) {
						judgePredictions.push({ name, predictions: computePredictions(logs), total_logs: logs.length });
					}
					// OC predictions
					const { results: ocLogs } = await env.MEMORY_DB.prepare('SELECT * FROM oc_activity_log ORDER BY date ASC').all();
					const ocGroups = new Map<string, any[]>();
					for (const l of ocLogs as any[]) {
						const n = l.counsel_name as string;
						if (!ocGroups.has(n)) ocGroups.set(n, []);
						ocGroups.get(n)!.push(l);
					}
					const ocPredictions: any[] = [];
					for (const [name, logs] of ocGroups) {
						ocPredictions.push({ name, predictions: computePredictions(logs), total_logs: logs.length });
					}
					// Attorney predictions
					const { results: attLogs } = await env.MEMORY_DB.prepare('SELECT * FROM attorney_activity_log ORDER BY date ASC').all();
					const attGroups = new Map<string, any[]>();
					for (const l of attLogs as any[]) {
						const n = l.attorney_name as string;
						if (!attGroups.has(n)) attGroups.set(n, []);
						attGroups.get(n)!.push(l);
					}
					const attPredictions: any[] = [];
					for (const [name, logs] of attGroups) {
						const { results: lessons } = await env.MEMORY_DB.prepare(
							`SELECT lesson_learned, activity_type, date FROM attorney_activity_log WHERE attorney_name = ? AND lesson_learned IS NOT NULL AND lesson_learned != '' ORDER BY date DESC LIMIT 10`
						).bind(name).all();
						attPredictions.push({ name, predictions: computePredictions(logs), lessons: lessons || [], total_logs: logs.length });
					}
					return json({ success: true, judges: judgePredictions, opposing_counsel: ocPredictions, attorneys: attPredictions });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/intel/report/gdrive — Generate dossier report and push to Google Drive
			if (path === '/api/intel/report/gdrive' && request.method === 'POST') {
				try {
					const token = await getGmailToken();
					const body = await request.json() as any;
					const entityType = body.type || 'judge'; // 'judge' or 'oc'
					const entityName = body.name || '';      // specific name, or '' for all

					// Gather data
					let reportTitle = '';
					let reportContent = '';

					if (entityType === 'judge') {
						const nameFilter = entityName ? `WHERE LOWER(judge_name) LIKE '%${entityName.toLowerCase().replace(/'/g, "''")}%'` : '';
						const { results: logs } = await env.MEMORY_DB.prepare(`SELECT * FROM judge_activity_log ${nameFilter} ORDER BY judge_name, date ASC`).all();
						const { results: profiles } = await env.MEMORY_DB.prepare(`SELECT * FROM judge_intel ${nameFilter.replace('judge_activity_log', 'judge_intel')}`).all();

						// Group logs by judge
						const grouped = new Map<string, any[]>();
						for (const l of logs as any[]) {
							const n = l.judge_name as string;
							if (!grouped.has(n)) grouped.set(n, []);
							grouped.get(n)!.push(l);
						}

						reportTitle = entityName ? `Judge Dossier — ${entityName}` : 'Judge Intelligence Report — All Judges';
						reportContent = `# ${reportTitle}\nGenerated: ${mtnISO()}\n\n`;

						for (const [name, jLogs] of grouped) {
							const profile = (profiles as any[]).find(p => (p.judge_name as string).toLowerCase() === name.toLowerCase());
							const preds = computePredictions(jLogs);

							reportContent += `## ${name}\n`;
							if (profile) {
								reportContent += `Court: ${profile.court || 'N/A'} | District: ${profile.district || 'N/A'}\n`;
								reportContent += `Tendencies: ${profile.tendencies || 'N/A'}\n`;
								reportContent += `Sentencing: ${profile.sentencing_patterns || 'N/A'}\n`;
								reportContent += `Motion Preferences: ${profile.motion_preferences || 'N/A'}\n`;
								reportContent += `JA: ${profile.ja_name || 'N/A'} (${profile.ja_email || ''} ${profile.ja_phone || ''})\n\n`;
							}
							reportContent += `### Predictions (${jLogs.length} logged events)\n`;
							for (const p of preds) {
								reportContent += `- ${p.summary}\n`;
							}
							// Ruling rationale + reversal factors
							const { results: rationale } = await env.MEMORY_DB.prepare(
								`SELECT * FROM judge_ruling_rationale WHERE LOWER(judge_name) LIKE ? ORDER BY date DESC LIMIT 20`
							).bind(`%${name.toLowerCase()}%`).all();
							if (rationale?.length) {
								const reversals = rationale.filter((r: any) => r.is_reversal);
								reportContent += `\n### Ruling Rationale (${rationale.length} entries, ${reversals.length} reversals)\n`;
								for (const r of rationale as any[]) {
									reportContent += `- ${r.date}: ${r.activity_type}${r.activity_subtype ? '/' + r.activity_subtype : ''} → ${r.actual_outcome}${r.is_reversal ? ' ⚠️ REVERSAL (typical: ' + r.typical_outcome + ')' : ''}\n`;
									if (r.ruling_reasoning) reportContent += `  Reasoning: ${r.ruling_reasoning}\n`;
									if (r.specific_arguments) reportContent += `  Key Arguments: ${r.specific_arguments}\n`;
									if (r.applicability_notes) reportContent += `  Applicability: ${r.applicability_notes}\n`;
								}
							}
							reportContent += `\n### Activity Log\n`;
							for (const l of jLogs.slice(-20)) {
								reportContent += `- ${l.date}: ${l.activity_type}${l.activity_subtype ? '/' + l.activity_subtype : ''} → ${l.outcome}${l.details ? ' — ' + l.details : ''} (${l.case_number || 'N/A'})\n`;
							}
							reportContent += '\n---\n\n';
						}
					} else if (entityType === 'oc') {
						const nameFilter = entityName ? `WHERE LOWER(counsel_name) LIKE '%${entityName.toLowerCase().replace(/'/g, "''")}%'` : '';
						const { results: logs } = await env.MEMORY_DB.prepare(`SELECT * FROM oc_activity_log ${nameFilter} ORDER BY counsel_name, date ASC`).all();
						const { results: profiles } = await env.MEMORY_DB.prepare(`SELECT * FROM opposing_counsel_intel ${nameFilter.replace('oc_activity_log', 'opposing_counsel_intel')}`).all();

						const grouped = new Map<string, any[]>();
						for (const l of logs as any[]) {
							const n = l.counsel_name as string;
							if (!grouped.has(n)) grouped.set(n, []);
							grouped.get(n)!.push(l);
						}

						reportTitle = entityName ? `OC Dossier — ${entityName}` : 'Opposing Counsel Intelligence Report — All OC';
						reportContent = `# ${reportTitle}\nGenerated: ${mtnISO()}\n\n`;

						for (const [name, ocLogs] of grouped) {
							const profile = (profiles as any[]).find(p => (p.counsel_name as string).toLowerCase() === name.toLowerCase());
							const preds = computePredictions(ocLogs);

							reportContent += `## ${name}\n`;
							if (profile) {
								reportContent += `Firm: ${profile.firm || 'N/A'} | Bar: ${profile.bar_number || 'N/A'}\n`;
								reportContent += `Negotiation Style: ${profile.negotiation_style || 'N/A'}\n`;
								reportContent += `Litigation Tendencies: ${profile.litigation_tendencies || 'N/A'}\n`;
								reportContent += `Strengths: ${profile.strengths || 'N/A'}\n`;
								reportContent += `Weaknesses: ${profile.weaknesses || 'N/A'}\n\n`;
							}
							reportContent += `### Predictions (${ocLogs.length} logged events)\n`;
							for (const p of preds) {
								reportContent += `- ${p.summary}\n`;
							}
							reportContent += `\n### Activity Log\n`;
							for (const l of ocLogs.slice(-20)) {
								reportContent += `- ${l.date}: ${l.activity_type}${l.activity_subtype ? '/' + l.activity_subtype : ''} → ${l.outcome}${l.details ? ' — ' + l.details : ''} (${l.case_number || 'N/A'})\n`;
							}
							reportContent += '\n---\n\n';
						}
					} else if (entityType === 'attorney') {
						const nameFilter = entityName ? `WHERE LOWER(attorney_name) LIKE '%${entityName.toLowerCase().replace(/'/g, "''")}%'` : '';
						const { results: logs } = await env.MEMORY_DB.prepare(`SELECT * FROM attorney_activity_log ${nameFilter} ORDER BY attorney_name, date ASC`).all();
						const grouped = new Map<string, any[]>();
						for (const l of logs as any[]) {
							const n = l.attorney_name as string;
							if (!grouped.has(n)) grouped.set(n, []);
							grouped.get(n)!.push(l);
						}
						reportTitle = entityName ? `Attorney Dossier — ${entityName}` : 'Attorney Performance Report — All Attorneys';
						reportContent = `# ${reportTitle}\nGenerated: ${mtnISO()}\n\n`;
						for (const [name, aLogs] of grouped) {
							const preds = computePredictions(aLogs);
							const lessons = aLogs.filter(l => l.lesson_learned);
							reportContent += `## ${name} (${aLogs.length} logged activities)\n`;
							reportContent += `### Predictions\n`;
							for (const p of preds) { reportContent += `- ${p.summary}\n`; }
							if (lessons.length) {
								reportContent += `\n### Lessons Learned (${lessons.length})\n`;
								for (const l of lessons.slice(-15)) {
									reportContent += `- ${l.date}: ${l.activity_type} → ${l.outcome}: **${l.lesson_learned}**\n`;
								}
							}
							reportContent += `\n### Activity Log\n`;
							for (const l of aLogs.slice(-20)) {
								reportContent += `- ${l.date}: ${l.activity_type}${l.activity_subtype ? '/' + l.activity_subtype : ''} → ${l.outcome}${l.details ? ' — ' + l.details : ''} (${l.case_number || 'N/A'})\n`;
							}
							reportContent += '\n---\n\n';
						}
					} else if (entityType === 'case_summary') {
						// Full case facesheet report — all enriched data
						const nameFilter = entityName ? `AND (LOWER(client_name) LIKE '%${entityName.toLowerCase().replace(/'/g, "''")}%' OR case_number LIKE '%${entityName.replace(/'/g, "''")}%')` : '';
						const { results: summaries } = await env.MEMORY_DB.prepare(`SELECT * FROM case_summaries WHERE status = 'active' ${nameFilter} ORDER BY client_name ASC`).all();
						reportTitle = entityName ? `Case Facesheet — ${entityName}` : 'Case Facesheet Report — All Active Cases';
						reportContent = `# ${reportTitle}\nGenerated: ${mtnISO()}\nTotal Cases: ${summaries.length}\n\n`;
						for (const cs of summaries as any[]) {
							reportContent += `## ${cs.client_name} — ${cs.case_number || 'No Case #'}\n`;
							reportContent += `Case Type: ${cs.case_type || 'N/A'} | Court: ${cs.court || 'N/A'} | District: ${cs.district || 'N/A'}\n`;
							reportContent += `Client Role: ${cs.client_role || 'N/A'}\n`;
							if (cs.client_phone || cs.client_email) reportContent += `Client Contact: ${cs.client_phone || ''} ${cs.client_email || ''}${cs.client_address ? ' | ' + cs.client_address : ''}\n`;
							reportContent += `Opposing Party: ${cs.opposing_party || 'N/A'}${cs.opposing_role ? ' (' + cs.opposing_role + ')' : ''}\n`;
							if (cs.opposing_counsel) {
								reportContent += `Opposing Counsel: ${cs.opposing_counsel}`;
								if (cs.opposing_counsel_firm) reportContent += ` (${cs.opposing_counsel_firm})`;
								reportContent += '\n';
								if (cs.opposing_counsel_phone || cs.opposing_counsel_email) reportContent += `  OC Contact: ${cs.opposing_counsel_phone || ''} ${cs.opposing_counsel_email || ''}\n`;
							}
							reportContent += `Judge: ${cs.judge || 'N/A'}\n`;
							if (cs.additional_parties) reportContent += `Additional Parties: ${cs.additional_parties}\n`;
							if (cs.facts) reportContent += `\n### Facts\n${cs.facts}\n`;
							if (cs.charges) reportContent += `\n### Charges\n${cs.charges}\n`;
							// Key deadlines
							const dlParts: string[] = [];
							if (cs.discovery_deadline) dlParts.push(`Discovery: ${cs.discovery_deadline}`);
							if (cs.dispositive_deadline) dlParts.push(`Dispositive: ${cs.dispositive_deadline}`);
							if (cs.trial_date) dlParts.push(`Trial: ${cs.trial_date}`);
							if (cs.statute_of_limitations) dlParts.push(`SOL: ${cs.statute_of_limitations}`);
							if (dlParts.length > 0) reportContent += `\n### Key Deadlines\n${dlParts.join('\n')}\n`;
							if (cs.next_event) reportContent += `Next Event: ${cs.next_event} on ${cs.next_event_date}\n`;
							if (cs.judge_prediction) reportContent += `\n### Judge Predictions\n${cs.judge_prediction}\n`;
							if (cs.oc_prediction) reportContent += `\n### OC Predictions\n${cs.oc_prediction}\n`;
							if (cs.reversal_factors) reportContent += `\n### Reversal Factors\n${cs.reversal_factors}\n`;
							if (cs.notes) reportContent += `\n### Notes\n${cs.notes}\n`;
							reportContent += `Files: ${cs.file_count || 0}${cs.folder_url ? ' | Folder: ' + cs.folder_url : ''}\n`;
							reportContent += `Updated: ${cs.updated_at || 'N/A'}\n`;
							reportContent += '\n---\n\n';
						}
					}

					// Find or create "ESQs Intel Reports" folder in Google Drive
					const folderSearchRes = await fetch(`https://www.googleapis.com/drive/v3/files?${new URLSearchParams({
						q: "mimeType = 'application/vnd.google-apps.folder' and name = 'ESQs Intel Reports' and trashed = false",
						fields: 'files(id, name)',
					})}`, { headers: { 'Authorization': `Bearer ${token}` } });
					const folderSearchData = await folderSearchRes.json() as any;
					let folderId: string;
					if (folderSearchData.files?.length > 0) {
						folderId = folderSearchData.files[0].id;
					} else {
						// Create the folder
						const createFolderRes = await fetch('https://www.googleapis.com/drive/v3/files', {
							method: 'POST',
							headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
							body: JSON.stringify({ name: 'ESQs Intel Reports', mimeType: 'application/vnd.google-apps.folder' })
						});
						const newFolder = await createFolderRes.json() as any;
						folderId = newFolder.id;
					}

					// Create or update Google Doc
					const safeTitle = reportTitle.replace(/[^\w\s—-]/g, '');
					// Check if doc already exists
					const docSearchRes = await fetch(`https://www.googleapis.com/drive/v3/files?${new URLSearchParams({
						q: `'${folderId}' in parents and name = '${safeTitle.replace(/'/g, "\\'")}' and trashed = false`,
						fields: 'files(id, name)',
					})}`, { headers: { 'Authorization': `Bearer ${token}` } });
					const docSearchData = await docSearchRes.json() as any;

					let docId: string;
					if (docSearchData.files?.length > 0) {
						// Delete old version and recreate (simpler than partial update)
						docId = docSearchData.files[0].id;
						await fetch(`https://www.googleapis.com/drive/v3/files/${docId}`, {
							method: 'DELETE',
							headers: { 'Authorization': `Bearer ${token}` }
						});
					}

					// Upload as plain text file (Google will auto-convert if we set mimeType)
					const boundary = '---esqs-intel-report---';
					const metadata = JSON.stringify({ name: safeTitle, parents: [folderId], mimeType: 'application/vnd.google-apps.document' });
					const multipartBody = `--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${metadata}\r\n--${boundary}\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n${reportContent}\r\n--${boundary}--`;

					const uploadRes = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,webViewLink', {
						method: 'POST',
						headers: {
							'Authorization': `Bearer ${token}`,
							'Content-Type': `multipart/related; boundary=${boundary}`
						},
						body: multipartBody
					});
					const uploadData = await uploadRes.json() as any;

					return json({
						success: true,
						report: { title: safeTitle, docId: uploadData.id, webUrl: uploadData.webViewLink, folder: 'ESQs Intel Reports' },
						stats: { type: entityType, name: entityName || 'all', contentLength: reportContent.length }
					});
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/intel/scan-case — Deep scan for a new case: gather judge + OC intel from existing data
			// Called when a new case is assigned — searches OneDrive files, emails, JudiciaLink, existing logs
			if (path === '/api/intel/scan-case' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const { judge_name, opposing_counsel, case_number, client_name } = body;
					if (!judge_name && !opposing_counsel) return err('judge_name or opposing_counsel required', 400);

					const findings: any = { judge: null, oc: null };

					// 1. Gather existing judge data
					if (judge_name) {
						const { results: judgeLogs } = await env.MEMORY_DB.prepare(
							`SELECT * FROM judge_activity_log WHERE LOWER(judge_name) LIKE ? ORDER BY date ASC`
						).bind(`%${judge_name.toLowerCase()}%`).all();
						const judgeProfile = await env.MEMORY_DB.prepare(
							`SELECT * FROM judge_intel WHERE LOWER(judge_name) LIKE ?`
						).bind(`%${judge_name.toLowerCase()}%`).first();
						const predictions = computePredictions(judgeLogs as any[]);

						// Search JudiciaLink emails for this judge
						let judicialinkMentions = 0;
						try {
							const gToken = await getGmailToken();
							const jlRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages?maxResults=20&q=${encodeURIComponent(`from:judicialink.com "${judge_name}"`)}`, {
								headers: { 'Authorization': `Bearer ${gToken}` }
							});
							const jlData = await jlRes.json() as any;
							judicialinkMentions = jlData.resultSizeEstimate || 0;
						} catch (_) {}

						// Search OneDrive for filings mentioning this judge
						let onedriveHits = 0;
						try {
							const graphToken = await getGraphToken();
							const searchRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/root/search(q='${encodeURIComponent(judge_name)}')?$top=10&$select=name,id`, {
								headers: { 'Authorization': `Bearer ${graphToken}` }
							});
							const searchData = await searchRes.json() as any;
							onedriveHits = searchData.value?.length || 0;
						} catch (_) {}

						findings.judge = {
							name: judge_name,
							profile: judgeProfile,
							logged_activities: judgeLogs.length,
							predictions,
							judicialink_mentions: judicialinkMentions,
							onedrive_file_hits: onedriveHits,
							recommendation: predictions.length > 0
								? `${predictions.map(p => p.summary).join('; ')}`
								: judgeProfile
									? `Static profile available but no logged activities yet. Start logging rulings to build predictions.`
									: `No data on this judge. Begin logging all interactions immediately.`
						};
					}

					// 2. Gather existing OC data
					if (opposing_counsel) {
						const { results: ocLogs } = await env.MEMORY_DB.prepare(
							`SELECT * FROM oc_activity_log WHERE LOWER(counsel_name) LIKE ? ORDER BY date ASC`
						).bind(`%${opposing_counsel.toLowerCase()}%`).all();
						const ocProfile = await env.MEMORY_DB.prepare(
							`SELECT * FROM opposing_counsel_intel WHERE LOWER(counsel_name) LIKE ?`
						).bind(`%${opposing_counsel.toLowerCase()}%`).first();
						const predictions = computePredictions(ocLogs as any[]);

						findings.oc = {
							name: opposing_counsel,
							profile: ocProfile,
							logged_activities: ocLogs.length,
							predictions,
							recommendation: predictions.length > 0
								? `${predictions.map(p => p.summary).join('; ')}`
								: ocProfile
									? `Static profile available but no logged activities yet. Start logging interactions to build predictions.`
									: `No data on this counsel. Begin logging all interactions immediately.`
						};
					}

					return json({ success: true, case_number, client_name, findings });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ═══════════════════════════════════════════════════════════════
			// ATTORNEY BEHAVIOR LOG + JUDGE RULING RATIONALE + JUDICIAL RESOURCES
			// ═══════════════════════════════════════════════════════════════

			// POST /api/intel/attorney/log — Log attorney activity (including self)
			if (path === '/api/intel/attorney/log' && request.method === 'POST') {
				try {
					const { attorney_name, role, party_role, case_number, client_name, activity_type, activity_subtype, outcome, details, lesson_learned, judge, opposing_counsel, date } = await request.json() as any;
					if (!attorney_name || !activity_type || !outcome) return err('attorney_name, activity_type, outcome required', 400);
					await env.MEMORY_DB.prepare(
						`INSERT INTO attorney_activity_log (attorney_name, role, party_role, case_number, client_name, activity_type, activity_subtype, outcome, details, lesson_learned, judge, opposing_counsel, date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(attorney_name, role || 'our_firm', party_role || '', case_number || '', client_name || '', activity_type, activity_subtype || '', outcome, details || '', lesson_learned || '', judge || '', opposing_counsel || '', date || mtnToday(), mtnISO()).run();
					// RAG store for semantic search
					ctx.waitUntil(ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
						id: `attorney_log_${Date.now()}`, type: 'attorney_activity', source: 'api',
						content: `[attorney_activity] ${attorney_name}${party_role ? ' (as ' + party_role + ')' : ''} ${activity_type}/${activity_subtype || ''}: ${outcome}. ${details || ''} ${lesson_learned ? 'LESSON: ' + lesson_learned : ''}`,
						clientName: client_name || '',
					}));
					return json({ success: true, message: `Logged: ${attorney_name}${party_role ? ' (as ' + party_role + ')' : ''} — ${activity_type} → ${outcome}${lesson_learned ? ' (lesson recorded)' : ''}` });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/attorney/log/:name — Get all logs for an attorney
			if (path.startsWith('/api/intel/attorney/log/') && request.method === 'GET') {
				try {
					const name = decodeURIComponent(path.replace('/api/intel/attorney/log/', ''));
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT * FROM attorney_activity_log WHERE LOWER(attorney_name) LIKE ? ORDER BY date DESC`
					).bind(`%${name.toLowerCase()}%`).all();
					return json({ success: true, attorney: name, logs: results, total: results?.length || 0 });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/attorney/predict/:name — Predictions for an attorney
			if (path.startsWith('/api/intel/attorney/predict/') && request.method === 'GET') {
				try {
					const name = decodeURIComponent(path.replace('/api/intel/attorney/predict/', ''));
					const { results: logs } = await env.MEMORY_DB.prepare(
						`SELECT activity_type, activity_subtype, outcome, party_role, date FROM attorney_activity_log WHERE LOWER(attorney_name) LIKE ? ORDER BY date DESC`
					).bind(`%${name.toLowerCase()}%`).all();
					const predictions = computePredictions(logs || []);
					// Also get lessons learned
					const { results: lessons } = await env.MEMORY_DB.prepare(
						`SELECT lesson_learned, activity_type, outcome, date, case_number FROM attorney_activity_log WHERE LOWER(attorney_name) LIKE ? AND lesson_learned IS NOT NULL AND lesson_learned != '' ORDER BY date DESC LIMIT 20`
					).bind(`%${name.toLowerCase()}%`).all();
					return json({ success: true, attorney: name, predictions, lessons: lessons || [], total_logs: logs?.length || 0 });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/intel/judge/rationale — Log why a judge ruled a certain way
			if (path === '/api/intel/judge/rationale' && request.method === 'POST') {
				try {
					const { judge_name, activity_type, activity_subtype, typical_outcome, actual_outcome, reversal_factors, specific_arguments, ruling_reasoning, case_number, case_type, our_role, party_role, applicability_notes, date } = await request.json() as any;
					if (!judge_name || !activity_type || !actual_outcome) return err('judge_name, activity_type, actual_outcome required', 400);
					const isReversal = typical_outcome && typical_outcome !== actual_outcome ? 1 : 0;
					const rfStr = typeof reversal_factors === 'string' ? reversal_factors : JSON.stringify(reversal_factors || []);
					await env.MEMORY_DB.prepare(
						`INSERT INTO judge_ruling_rationale (judge_name, activity_type, activity_subtype, typical_outcome, actual_outcome, reversal_factors, specific_arguments, ruling_reasoning, case_number, case_type, our_role, party_role, applicability_notes, is_reversal, date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(judge_name, activity_type, activity_subtype || '', typical_outcome || '', actual_outcome, rfStr, specific_arguments || '', ruling_reasoning || '', case_number || '', case_type || '', our_role || '', party_role || '', applicability_notes || '', isReversal, date || mtnToday(), mtnISO()).run();
					ctx.waitUntil(ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
						id: `judge_rationale_${Date.now()}`, type: 'judge_rationale', source: 'api',
						content: `[judge_rationale] ${judge_name} ${actual_outcome} ${activity_type}/${activity_subtype || ''}${party_role ? ' (we were ' + party_role + ')' : ''}.${isReversal ? ' REVERSAL from typical ' + typical_outcome + '.' : ''} Reasoning: ${ruling_reasoning || specific_arguments || 'not specified'}. ${applicability_notes ? 'Applicability: ' + applicability_notes : ''}`,
						clientName: '',
					}));
					return json({ success: true, message: `Logged: ${judge_name} — ${activity_type} → ${actual_outcome}${isReversal ? ' (REVERSAL from typical ' + typical_outcome + ')' : ''}`, is_reversal: isReversal });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/judge/rationale/:name — Get ruling rationale for a judge
			if (path.startsWith('/api/intel/judge/rationale/') && request.method === 'GET') {
				try {
					const name = decodeURIComponent(path.replace('/api/intel/judge/rationale/', ''));
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT * FROM judge_ruling_rationale WHERE LOWER(judge_name) LIKE ? ORDER BY date DESC`
					).bind(`%${name.toLowerCase()}%`).all();
					const reversals = (results || []).filter((r: any) => r.is_reversal);
					return json({ success: true, judge: name, rationale: results, reversals, total: results?.length || 0, reversal_count: reversals.length });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/intel/resources — Add a judicial resource
			if (path === '/api/intel/resources' && request.method === 'POST') {
				try {
					const { resource_type, title, source, jurisdiction, applicable_to, topic, summary, last_updated } = await request.json() as any;
					if (!resource_type || !title) return err('resource_type, title required', 400);
					// Dedup by title + jurisdiction
					const existing = await env.MEMORY_DB.prepare(
						`SELECT id FROM judicial_resources WHERE LOWER(title) = ? AND jurisdiction = ?`
					).bind(title.toLowerCase(), jurisdiction || 'utah').first();
					if (existing) {
						await env.MEMORY_DB.prepare(
							`UPDATE judicial_resources SET summary = ?, last_updated = ?, source = ? WHERE id = ?`
						).bind(summary || '', last_updated || mtnToday(), source || '', existing.id).run();
						return json({ success: true, message: `Updated existing resource: ${title}`, id: existing.id });
					}
					const res = await env.MEMORY_DB.prepare(
						`INSERT INTO judicial_resources (resource_type, title, source, jurisdiction, applicable_to, topic, summary, last_updated, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(resource_type, title, source || '', jurisdiction || 'utah', applicable_to || '', topic || '', summary || '', last_updated || mtnToday(), mtnISO()).run();
					// RAG store
					if (summary) {
						ctx.waitUntil(ragStore(env.AI, env.MEMORY_INDEX, env.DB, {
							id: `judicial_resource_${Date.now()}`, type: 'judicial_resource', source: 'api',
							content: `[judicial_resource] ${title} (${resource_type}, ${jurisdiction}): ${summary}`,
							clientName: '',
						}));
					}
					return json({ success: true, message: `Added resource: ${title}` });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/resources — List/search judicial resources
			if (path === '/api/intel/resources' && request.method === 'GET') {
				try {
					const topic = url.searchParams.get('topic');
					const jurisdiction = url.searchParams.get('jurisdiction');
					const type = url.searchParams.get('type');
					let q = 'SELECT * FROM judicial_resources WHERE 1=1';
					const params: any[] = [];
					if (topic) { q += ' AND topic LIKE ?'; params.push(`%${topic}%`); }
					if (jurisdiction) { q += ' AND jurisdiction = ?'; params.push(jurisdiction); }
					if (type) { q += ' AND resource_type = ?'; params.push(type); }
					q += ' ORDER BY last_updated DESC, created_at DESC';
					const stmt = params.length ? env.MEMORY_DB.prepare(q).bind(...params) : env.MEMORY_DB.prepare(q);
					const { results } = await stmt.all();
					return json({ success: true, resources: results, total: results?.length || 0 });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intel/resources/:topic — Shorthand for topic filter
			if (path.startsWith('/api/intel/resources/') && request.method === 'GET') {
				try {
					const topic = decodeURIComponent(path.replace('/api/intel/resources/', ''));
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT * FROM judicial_resources WHERE topic LIKE ? OR applicable_to LIKE ? ORDER BY last_updated DESC`
					).bind(`%${topic}%`, `%${topic}%`).all();
					return json({ success: true, topic, resources: results, total: results?.length || 0 });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ═══════════════════════════════════════════════════════════════
			// 404
			// ═══════════════════════════════════════════════════════════════
			return err(`Not found: ${path}`, 404);

		} catch (error: any) {
			console.error('Worker error:', error);
			return err(error.message || 'Internal server error', 500);
		}
	},

	// ═══════════════════════════════════════════════════════════════
	// SCHEDULED (CRON) — Routine deep scan of case documents
	// Runs every 6 hours. Scans 5 cases per run, cycling through all active cases.
	// Uses KV key "deep-scan-state" to track offset + last run metadata.
	// ═══════════════════════════════════════════════════════════════
	async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
		const BATCH_SIZE = 5;
		const KV_KEY = 'deep-scan-state';

		try {
			// Load scan state from KV
			const stateRaw = await env.CACHE.get(KV_KEY);
			let state: { offset: number; last_run: string; total_scanned: number; total_updated: number; cycle: number } = stateRaw
				? JSON.parse(stateRaw)
				: { offset: 0, last_run: '', total_scanned: 0, total_updated: 0, cycle: 1 };

			// Count total active cases
			const countRow = await env.MEMORY_DB.prepare(`SELECT COUNT(*) as cnt FROM case_summaries WHERE status = 'active'`).first() as any;
			const totalActive = countRow?.cnt || 0;

			if (totalActive === 0) {
				console.log('[CRON deep-scan] No active cases. Skipping.');
				return;
			}

			// Reset offset if we've cycled through all cases
			if (state.offset >= totalActive) {
				state.offset = 0;
				state.cycle += 1;
				state.total_scanned = 0;
				state.total_updated = 0;
			}

			// Get batch of cases
			const { results: cases } = await env.MEMORY_DB.prepare(
				`SELECT cs.*, pc.folder_url, pc.folder_path FROM case_summaries cs LEFT JOIN party_cache pc ON cs.client_name = pc.client_name AND cs.case_number = pc.case_number WHERE cs.status = 'active' ORDER BY cs.client_name ASC LIMIT ? OFFSET ?`
			).bind(BATCH_SIZE, state.offset).all();

			if (cases.length === 0) {
				state.offset = 0;
				await env.CACHE.put(KV_KEY, JSON.stringify(state));
				return;
			}

			// Get Graph token (inline — can't use fetch-scoped getGraphToken)
			let token: string;
			try {
				const cached = await env.CACHE.get('ms_graph_token');
				if (cached) {
					token = cached;
				} else {
					const tokenUrl = `https://login.microsoftonline.com/common/oauth2/v2.0/token`;
					const res = await fetch(tokenUrl, {
						method: 'POST',
						headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
						body: new URLSearchParams({
							client_id: env.MICROSOFT_CLIENT_ID,
							client_secret: env.MICROSOFT_CLIENT_SECRET,
							refresh_token: env.MICROSOFT_REFRESH_TOKEN,
							grant_type: 'refresh_token',
							scope: 'https://graph.microsoft.com/User.Read https://graph.microsoft.com/Files.ReadWrite https://graph.microsoft.com/Files.ReadWrite.All offline_access',
						})
					});
					const data = await res.json() as any;
					if (!data.access_token) throw new Error(data.error_description || data.error || 'Token failed');
					await env.CACHE.put('ms_graph_token', data.access_token, { expirationTtl: 3000 });
					token = data.access_token;
				}
			} catch (e: any) {
				console.error('[CRON deep-scan] Graph token failed:', e.message);
				return;
			}

			let fieldsUpdated = 0;
			let casesScanned = 0;

			for (const cs of cases as any[]) {
				const clientName = cs.client_name as string;
				const caseNum = cs.case_number as string;
				casesScanned++;

				try {
					// Skip if all key fields already populated
					if (cs.facts && cs.charges && cs.opposing_counsel) continue;

					// 1. Find client folder in OneDrive
					const searchUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${env.ONEDRIVE_FOLDER_ID}/children?$top=200`;
					const folderRes = await fetch(searchUrl, { headers: { 'Authorization': `Bearer ${token}` } });
					const folderData = await folderRes.json() as any;
					const nameParts = clientName.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter((p: string) => p.length >= 2).map((p: string) => p.toLowerCase());
					const last = nameParts[nameParts.length - 1];
					const first = nameParts[0];
					const clientFolder = (folderData.value || []).find((f: any) => {
						if (!f.folder) return false;
						const fn = f.name.toLowerCase().replace(/[^a-z\s]/g, ' ');
						const fParts = fn.trim().split(/\s+/).filter((p: string) => p.length >= 2);
						if (nameParts.every((p: string) => fn.includes(p))) return true;
						if (nameParts.length >= 2 && fn.includes(first) && fn.includes(last)) return true;
						const lastFuzzy = fParts.some((fp: string) => fuzzyNameMatch(last, fp));
						const firstFuzzy = fParts.some((fp: string) => fuzzyNameMatch(first, fp));
						if (lastFuzzy && firstFuzzy) return true;
						if (last && last.length > 4 && fn.startsWith(last)) return true;
						if (last && last.length > 5 && lastFuzzy) return true;
						return false;
					});

					if (!clientFolder) continue;

					// 2. List files (top + 1 level subfolders)
					const allFiles: any[] = [];
					const topFiles = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${clientFolder.id}/children?$top=100&$orderby=lastModifiedDateTime desc`, {
						headers: { 'Authorization': `Bearer ${token}` }
					}).then(r => r.json()) as any;

					for (const f of (topFiles.value || [])) {
						if (f.folder) {
							try {
								const subFiles = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${f.id}/children?$top=50`, {
									headers: { 'Authorization': `Bearer ${token}` }
								}).then(r => r.json()) as any;
								for (const sf of (subFiles.value || [])) allFiles.push({ ...sf, subfolder: f.name });
							} catch (_) {}
						} else {
							allFiles.push(f);
						}
					}

					// 3. Identify key documents
					const keyDocs: Record<string, any> = {};
					const prefer = (cat: string, f: any) => {
						const existing = keyDocs[cat];
						if (!existing) { keyDocs[cat] = f; return; }
						const newExt = (f.name || '').split('.').pop()?.toLowerCase();
						const oldExt = (existing.name || '').split('.').pop()?.toLowerCase();
						if (newExt === 'docx' && oldExt !== 'docx') keyDocs[cat] = f;
					};
					for (const f of allFiles) {
						const fn = (f.name || '').toLowerCase();
						const ext = fn.split('.').pop() || '';
						if (!['pdf', 'docx', 'doc', 'txt', 'rtf'].includes(ext)) continue;
						if (/noa|notice.*appear|entry.*appear/i.test(fn)) prefer('noa', f);
						else if (/complaint|information|charging|indictment|citation|arraign|criminal.*contract/i.test(fn)) prefer('charging', f);
						else if (/schedul.*order|case.*manage|cmo|pretrial.*conf|pre.*trial/i.test(fn)) prefer('scheduling', f);
						else if (/police.*report|incident.*report|probable.*cause|arrest/i.test(fn)) prefer('police_report', f);
						else if (/petition|motion.*dismiss|answer|plea|change.*plea/i.test(fn)) prefer('pleading', f);
						else if (/discovery|interrogat|request.*produc|request.*discov|rfd|rfp|rog/i.test(fn)) prefer('discovery', f);
						else if (/cover.*sheet|face.*sheet|intake|contract|fee.*agree/i.test(fn)) prefer('coversheet', f);
						else if (/order(?!.*continue)|minute|ruling|judgment|sentence/i.test(fn)) prefer('court_order', f);
						else if (/letter|corresp|email/i.test(fn) && !keyDocs['correspondence']) prefer('correspondence', f);
						else if (/stipulat|motion/i.test(fn) && !keyDocs['motion']) prefer('motion', f);
					}

					if (Object.keys(keyDocs).length === 0) continue;

					// 4. Download & extract text (max 3 docs)
					const docsToRead = Object.entries(keyDocs).slice(0, 3);
					let combinedText = '';

					for (const [docType, file] of docsToRead) {
						try {
							const ext = (file.name || '').split('.').pop()?.toLowerCase();

							if (ext === 'docx' || ext === 'doc') {
								// DOCX: download, find word/document.xml in ZIP, decompress, extract w:t tags
								const dlRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${file.id}/content`, {
									headers: { 'Authorization': `Bearer ${token}` }
								});
								const docBuf = await dlRes.arrayBuffer();
								const bytes = new Uint8Array(docBuf);
								let text = '';
								for (let i = 0; i < bytes.length - 30; i++) {
									if (bytes[i] === 0x50 && bytes[i+1] === 0x4B && bytes[i+2] === 0x03 && bytes[i+3] === 0x04) {
										const fnLen = bytes[i+26] | (bytes[i+27] << 8);
										const exLen = bytes[i+28] | (bytes[i+29] << 8);
										const compMethod = bytes[i+8] | (bytes[i+9] << 8);
										const compSize = bytes[i+18] | (bytes[i+19] << 8) | (bytes[i+20] << 16) | (bytes[i+21] << 24);
										const headerEnd = i + 30 + fnLen + exLen;
										const fileName = new TextDecoder().decode(bytes.slice(i+30, i+30+fnLen));
										if (fileName === 'word/document.xml' && compSize > 0) {
											const compData = bytes.slice(headerEnd, headerEnd + compSize);
											if (compMethod === 8) {
												try {
													const ds = new DecompressionStream('deflate-raw');
													const writer = ds.writable.getWriter();
													writer.write(compData);
													writer.close();
													const reader = ds.readable.getReader();
													const chunks: Uint8Array[] = [];
													while (true) {
														const { done, value } = await reader.read();
														if (done) break;
														chunks.push(value);
													}
													const totalLen = chunks.reduce((s, c) => s + c.length, 0);
													const merged = new Uint8Array(totalLen);
													let off = 0;
													for (const c of chunks) { merged.set(c, off); off += c.length; }
													const xml = new TextDecoder().decode(merged);
													const wtMatches = xml.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
													for (const m of wtMatches) text += m.replace(/<[^>]+>/g, '') + ' ';
												} catch (_) {}
											} else if (compMethod === 0) {
												const xml = new TextDecoder().decode(compData);
												const wtMatches = xml.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
												for (const m of wtMatches) text += m.replace(/<[^>]+>/g, '') + ' ';
											}
											break;
										}
									}
								}
								if (text.trim().length > 20) combinedText += `\n--- ${docType.toUpperCase()} (${file.name}) ---\n${text.substring(0, 4000)}\n`;
							} else if (ext === 'pdf') {
								const itemRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${file.id}`, {
									headers: { 'Authorization': `Bearer ${token}` }
								});
								const itemData = await itemRes.json() as any;
								const dlUrl = itemData['@microsoft.graph.downloadUrl'];
								if (!dlUrl) continue;
								const pdfRes = await fetch(dlUrl);
								const pdfBuf = await pdfRes.arrayBuffer();
								const pdfBytes = new Uint8Array(pdfBuf);
								const raw = new TextDecoder('utf-8', { fatal: false }).decode(pdfBytes);
								let text = '';
								// Method 1: (text) Tj
								const m1 = raw.match(/\(([^)]{2,})\)\s*Tj/g) || [];
								for (const m of m1) { const inner = m.replace(/^\(/, '').replace(/\)\s*Tj$/, ''); if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner + ' '; }
								// Method 2: BT...ET blocks
								const m2 = raw.match(/BT\s[\s\S]{5,2000}?ET/g) || [];
								for (const block of m2.slice(0, 30)) { const parts = block.match(/\(([^)]{2,})\)/g) || []; for (const p of parts) { const inner = p.replace(/[()]/g, ''); if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner + ' '; } }
								// Method 3: TJ arrays
								const m3 = raw.match(/\[([^\]]{5,})\]\s*TJ/g) || [];
								for (const arr of m3.slice(0, 50)) { const parts = arr.match(/\(([^)]+)\)/g) || []; for (const p of parts) { const inner = p.replace(/[()]/g, ''); if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner; } text += ' '; }
								// Method 4: Hex text
								const m4 = raw.match(/<([0-9A-Fa-f]{4,})>\s*Tj/g) || [];
								for (const hm of m4.slice(0, 30)) { const hex = hm.replace(/</, '').replace(/>\s*Tj/, ''); try { let d = ''; for (let hi = 0; hi < hex.length; hi += 2) { const c = parseInt(hex.substr(hi, 2), 16); if (c >= 32 && c < 127) d += String.fromCharCode(c); } if (d.length > 1) text += d + ' '; } catch (_) {} }
								// Method 5: Decompress FlateDecode streams
								if (text.trim().length < 50) {
									const sStarts: number[] = [];
									let sf = 0;
									while (sStarts.length < 15) { const idx = raw.indexOf('stream\r\n', sf); if (idx === -1) break; sStarts.push(idx + 8); sf = idx + 8; }
									for (const ss of sStarts.slice(0, 10)) {
										const se = raw.indexOf('endstream', ss);
										if (se === -1 || se - ss > 100000) continue;
										try {
											const ds = new DecompressionStream('deflate');
											const w = ds.writable.getWriter(); w.write(pdfBytes.slice(ss, se)); w.close();
											const r = ds.readable.getReader(); const ch: Uint8Array[] = []; let tb = 0;
											while (tb < 50000) { const { done, value } = await r.read(); if (done) break; ch.push(value); tb += value.length; }
											const tl = ch.reduce((s, c) => s + c.length, 0); const mg = new Uint8Array(tl); let o = 0; for (const c of ch) { mg.set(c, o); o += c.length; }
											const dc = new TextDecoder('utf-8', { fatal: false }).decode(mg);
											const it = dc.match(/\(([^)]{2,})\)\s*Tj/g) || [];
											for (const m of it) { const inner = m.replace(/^\(/, '').replace(/\)\s*Tj$/, ''); if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner + ' '; }
											const ia = dc.match(/\[([^\]]{5,})\]\s*TJ/g) || [];
											for (const arr of ia) { const parts = arr.match(/\(([^)]+)\)/g) || []; for (const p of parts) { const inner = p.replace(/[()]/g, ''); if (inner.length > 1 && !/^[\\\/\d\s]+$/.test(inner)) text += inner; } text += ' '; }
										} catch (_) {}
									}
								}
								text = text.replace(/\\n/g, '\n').replace(/\\r/g, '').replace(/\s{3,}/g, ' ').trim();
								if (text.length > 20) combinedText += `\n--- ${docType.toUpperCase()} (${file.name}) ---\n${text.substring(0, 4000)}\n`;
							} else if (ext === 'txt') {
								const txtRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${file.id}/content`, {
									headers: { 'Authorization': `Bearer ${token}` }
								});
								const text = await txtRes.text();
								combinedText += `\n--- ${docType.toUpperCase()} (${file.name}) ---\n${text.substring(0, 4000)}\n`;
							}
						} catch (_) {}
					}

					if (combinedText.length < 50) continue;

					// 5. AI extraction with party context
					const cronOCExisting = cs.opposing_counsel || '';
					const cronExtPrompt = `You are a legal document analyzer. Extract CONCISE info.\n\nCLIENT: ${clientName} (${cs.client_role || 'our client'})\nOPPOSING PARTY: ${cs.opposing_party || 'unknown'}\nOUR FIRM: Pitcher Law PLLC (Diane Pitcher, John Adams). Emails: @dianepitcher.com, @esqslaw\n${cronOCExisting ? 'KNOWN OC: ' + cronOCExisting : ''}\n\nCRITICAL: OC is the OTHER side's attorney. Pitcher Law/Diane Pitcher/John Adams/dianepitcher.com = OUR firm, NOT OC.\n\nRULES: Facts 2-3 sentences. Charges: names + degrees only. Use "" for not found.\n\nJSON only:\n{"facts":"","charges":"","oc_name":"","oc_phone":"","oc_email":"","oc_firm":"","discovery_deadline":"","trial_date":"","dispositive_deadline":"","statute_of_limitations":"","additional_parties":""}\n\nDocuments:\n${combinedText.substring(0, 5500)}`;

					try {
						const aiRes = await env.AI.run('@cf/meta/llama-3.1-8b-instruct', {
							messages: [{ role: 'user', content: cronExtPrompt }],
							max_tokens: 1000,
							temperature: 0.1,
						}) as any;
						const aiText = aiRes.response || '';
						const jsonMatch = aiText.match(/\{[\s\S]*\}/);
						if (jsonMatch) {
							const extracted = JSON.parse(jsonMatch[0]);

							// ═══ PARTY VERIFICATION — prevent attorney-as-OC ═══
							const ownFirmRx = /pitcher\s*law|diane\s*pitcher|john\s*adams|dianepitcher\.com|esqslaw|marie@|associate@|^pitcher|^adams/i;
							const isSelf = (v: string) => v && ownFirmRx.test(v);
							if (isSelf(extracted.oc_name || '') || isSelf(extracted.oc_firm || '') || isSelf(extracted.oc_email || '')) {
								extracted.oc_name = ''; extracted.oc_phone = ''; extracted.oc_email = ''; extracted.oc_firm = '';
							}
							if (extracted.oc_name && clientName.toLowerCase().includes(extracted.oc_name.toLowerCase().split(' ')[0])) {
								extracted.oc_name = ''; extracted.oc_phone = ''; extracted.oc_email = ''; extracted.oc_firm = '';
							}
							if (cronOCExisting && extracted.oc_name && extracted.oc_name.toLowerCase() !== cronOCExisting.toLowerCase()) {
								extracted.oc_name = '';
							}

							const updates: string[] = [];
							const vals: any[] = [];

							if (extracted.facts && !cs.facts) { updates.push('facts = ?'); vals.push(extracted.facts); }
							if (extracted.charges && !cs.charges) { updates.push('charges = ?'); vals.push(extracted.charges); }
							if (extracted.oc_name && !cs.opposing_counsel) { updates.push('opposing_counsel = ?'); vals.push(extracted.oc_name); }
							if (extracted.oc_phone && !cs.opposing_counsel_phone) { updates.push('opposing_counsel_phone = ?'); vals.push(extracted.oc_phone); }
							if (extracted.oc_email && !cs.opposing_counsel_email) { updates.push('opposing_counsel_email = ?'); vals.push(extracted.oc_email); }
							if (extracted.oc_firm && !cs.opposing_counsel_firm) { updates.push('opposing_counsel_firm = ?'); vals.push(extracted.oc_firm); }
							if (extracted.discovery_deadline && !cs.discovery_deadline) { updates.push('discovery_deadline = ?'); vals.push(extracted.discovery_deadline); }
							if (extracted.trial_date && !cs.trial_date) { updates.push('trial_date = ?'); vals.push(extracted.trial_date); }
							if (extracted.dispositive_deadline && !cs.dispositive_deadline) { updates.push('dispositive_deadline = ?'); vals.push(extracted.dispositive_deadline); }
							if (extracted.statute_of_limitations && !cs.statute_of_limitations) { updates.push('statute_of_limitations = ?'); vals.push(extracted.statute_of_limitations); }
							if (extracted.additional_parties && !cs.additional_parties) { updates.push('additional_parties = ?'); vals.push(extracted.additional_parties); }

							if (updates.length > 0) {
								updates.push('updated_at = ?');
								vals.push(new Date().toISOString().split('T')[0]);
								vals.push(clientName);
								vals.push(caseNum);
								await env.MEMORY_DB.prepare(
									`UPDATE case_summaries SET ${updates.join(', ')} WHERE client_name = ? AND case_number = ?`
								).bind(...vals).run();
								fieldsUpdated += updates.length - 1;
							}
						}
					} catch (_) {}
				} catch (_) {}
			}

			// Update state
			state.offset += BATCH_SIZE;
			state.last_run = new Date().toISOString();
			state.total_scanned += casesScanned;
			state.total_updated += fieldsUpdated;

			await env.CACHE.put(KV_KEY, JSON.stringify(state));
			console.log(`[CRON deep-scan] Cycle ${state.cycle} | Offset ${state.offset}/${totalActive} | Scanned ${casesScanned} | Fields updated ${fieldsUpdated}`);
		} catch (e: any) {
			console.error('[CRON deep-scan] Error:', e.message);
		}
	},
};
