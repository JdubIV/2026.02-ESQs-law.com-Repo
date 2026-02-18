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
	OPENAI_API_KEY: string;
	ANTHROPIC_API_KEY: string;
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

function hashString(str: string): string {
	let hash = 0;
	for (let i = 0; i < str.length; i++) {
		const char = str.charCodeAt(i);
		hash = ((hash << 5) - hash) + char;
		hash |= 0;
	}
	return Math.abs(hash).toString(36);
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
					timestamp: new Date().toISOString()
				});
			}

			// ═══════════════════════════════════════════════════════════════
			// AUTH
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/auth/login' && request.method === 'POST') {
				const { email, password } = await request.json() as any;
				if (!password) return err('Password required', 400);
				
				const adminPass = env.AUTH_SECRET || 'admin123';
				if (password !== adminPass) {
					const user = await env.DB.prepare('SELECT * FROM users WHERE email = ? AND active = 1').bind(email).first();
					if (!user) return err('Invalid credentials', 401);
				}
				
				const token = crypto.randomUUID();
				await env.SESSIONS.put(token, JSON.stringify({ email: email || 'admin', loginTime: new Date().toISOString() }), { expirationTtl: 86400 });
				return json({ success: true, token, user: { email: email || 'admin' }, redirect: '/' });
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
					if (session) return json({ authenticated: true, user: JSON.parse(session) });
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
				const { name, email, phone, address, city, state, zip, notes } = await request.json() as any;
				if (!name) return err('Client name required', 400);
				const r = await env.DB.prepare(
					`INSERT INTO clients (name, email, phone, address, city, state, zip, notes, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`
				).bind(name, email, phone, address, city, state, zip, notes).run();
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
				const { client_id, case_number, case_type, state, court, facts, notes } = await request.json() as any;
				if (!client_id || !case_type || !state || !court) return err('Missing required fields', 400);
				const r = await env.DB.prepare(
					`INSERT INTO cases (client_id, case_number, case_type, state, court, facts, notes, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))`
				).bind(client_id, case_number, case_type, state, court, facts, notes).run();
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
					`INSERT INTO tasks (case_id, title, description, due_date, priority, assigned_to, created_date) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`
				).bind(case_id, title, description, due_date, priority || 'Medium', assigned_to).run();
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
				let q = 'SELECT cal.*, c.case_number FROM calendar cal LEFT JOIN cases c ON cal.case_id = c.id WHERE 1=1';
				const p: any[] = [];
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
				let q = 'SELECT te.*, c.case_number FROM time_entries te LEFT JOIN cases c ON te.case_id = c.id WHERE 1=1';
				const p: any[] = [];
				if (caseId) { q += ' AND te.case_id = ?'; p.push(caseId); }
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
				).bind(case_id, date, hours, rate || 250, description).run();
				return json({ success: true, id: r.meta.last_row_id });
			}

			// ═══════════════════════════════════════════════════════════════
			// SUGGESTIONS (placeholder)
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/suggestions' && request.method === 'GET') {
				// TODO: Implement AI-powered suggestions based on tasks, calendar, etc.
				return json({ success: true, suggestions: [] });
			}

			// ═══════════════════════════════════════════════════════════════
			// AI BRIDGE (Synthia Oracle — Claude as RAID Driver)
			// ═══════════════════════════════════════════════════════════════
			if (path === '/api/bridges/message' && request.method === 'POST') {
				const { message, context, sessionId, mode, with_rag, jurisdiction, dashboardState, clientName: reqClientName } = await request.json() as any;
				if (!message) return err('Message required', 400);

				// Extract active client from dashboard state or request
				const activeClient = dashboardState?.activeClient || reqClientName || '';

				// --- STEP 1: Cache check (KV first, then D1) ---
				// Skip cache for action/command messages (add, delete, update, complete, refresh) and email-related messages
				const isActionMessage = /\b(add|create|schedule|move|reschedule|change|update|edit|delete|remove|cancel|complete|mark done|refresh calendar|sync calendar)\b/i.test(message) && /\b(hearing|deadline|event|appointment|court date|calendar|meeting|sentencing|pretrial|arraignment|conference|review|motion|plea)\b/i.test(message) || /\b(refresh|sync)\s*(the\s*)?(calendar|deadlines)\b/i.test(message) || /\b(when\s+is|when\s+are|when\s+does|when\s+do|when\s+must|when\s+should|what\s+(?:is|are)\s+the\s+(?:filing\s+)?deadline|calculate\s+(?:the\s+)?deadline|compute\s+(?:the\s+)?deadline|file\s+by\s+when|days?\s+to\s+(?:respond|answer|file|oppose)|how\s+(?:many|long)\s+(?:days?|time))\b/i.test(message);
				const isEmailMessage = /\b(email|inbox|mail|send|reply|forward|archive|correspondence)\b/i.test(message);
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

				// --- STEP 2: Load context from MEMORY_DB ---
				let memoryContext = '';
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
						`SELECT client_name, case_number, deadline_type, description, due_date, court FROM deadlines WHERE status IN ('active', 'pending') AND due_date >= date('now') ORDER BY due_date ASC LIMIT 15`
					).all();
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

					// Case summaries — consolidated case intelligence
					const caseSummaries = await env.MEMORY_DB.prepare(
						`SELECT client_name, case_number, case_type, court, client_role, opposing_party, judge, summary, next_event, next_event_date, status, file_count FROM case_summaries WHERE status = 'active' ORDER BY next_event_date ASC`
					).all();
					if (caseSummaries.results?.length) {
						memoryContext += `\n## 📋 Case Summaries (${caseSummaries.results.length} active)\n`;
						for (const cs of caseSummaries.results as any[]) {
							memoryContext += `- **${cs.client_name}** (${cs.case_number}) — ${cs.case_type}, ${cs.court}\n`;
							memoryContext += `  Role: ${cs.client_role} vs ${cs.opposing_party} | Judge: ${cs.judge}\n`;
							if (cs.summary) memoryContext += `  Summary: ${(cs.summary as string).substring(0, 300)}\n`;
							if (cs.next_event) memoryContext += `  Next: ${cs.next_event} on ${cs.next_event_date}\n`;
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
						const recentTime = await env.MEMORY_DB.prepare(
							`SELECT date, client_name, hours, description FROM timecards WHERE date >= date('now', '-7 days') ORDER BY date DESC LIMIT 20`
						).all();
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
						`SELECT summary, started_at FROM sessions WHERE id LIKE 'summary_%' ORDER BY started_at DESC LIMIT 10`
					).all();
					if (summaries.results?.length) {
						memoryContext += '\n## Conversation History (compressed summaries, oldest first)\n';
						const sorted = (summaries.results as any[]).reverse();
						for (const s of sorted) {
							memoryContext += `[${s.started_at}]: ${(s.summary || '').substring(0, 600)}\n---\n`;
						}
					}

					// Recent conversation context (rolling window — last 20 messages in full)
					const recentMsgs = await env.DB.prepare(
						`SELECT role, content, timestamp FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant') ORDER BY timestamp DESC LIMIT 20`
					).all();
					if (recentMsgs.results?.length) {
						memoryContext += '\n## Recent Conversation (last 20 messages)\n';
						const msgs = (recentMsgs.results as any[]).reverse();
						for (const m of msgs) {
							memoryContext += `[${m.role}]: ${(m.content || '').substring(0, 400)}\n`;
						}
					}
				} catch (memErr: any) {
					console.error('Memory context error:', memErr.message);
				}

				// --- STEP 2-EMAIL: Fetch recent emails for active client and inject into context ---
				let emailContextStr = '';
				let emailAction: string | null = null;
				if (activeClient) {
					try {
						// getGraphToken is defined further down — hoist it here
						const cached = await env.CACHE.get('ms_graph_token');
						let graphToken = cached;
						if (!graphToken) {
							const tokenUrl = `https://login.microsoftonline.com/common/oauth2/v2.0/token`;
							const tRes = await fetch(tokenUrl, {
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
							const tData = await tRes.json() as any;
							if (tData.access_token) {
								graphToken = tData.access_token;
								await env.CACHE.put('ms_graph_token', tData.access_token, { expirationTtl: 3000 });
								if (tData.refresh_token) await env.CACHE.put('ms_refresh_token_latest', tData.refresh_token);
							}
						}

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
				}

				// --- STEP 2-ALERTS: Fetch JudiciaLink / court alert emails (independent of active client) ---
				// JudiciaLink alerts come from support@judicialink.com and contain hearing changes for ALL cases
				// Court notices come from @utcourts.gov — these are critical and should always be in context
				if (isAlertMessage || isEmailMessage) {
					try {
						// Reuse graphToken from above, or get fresh one
						let alertGraphToken: string | null = null;
						const cachedToken = await env.CACHE.get('ms_graph_token');
						if (cachedToken) {
							alertGraphToken = cachedToken;
						} else {
							const tokenUrl = `https://login.microsoftonline.com/common/oauth2/v2.0/token`;
							const tRes = await fetch(tokenUrl, {
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
							const tData = await tRes.json() as any;
							if (tData.access_token) {
								alertGraphToken = tData.access_token;
								await env.CACHE.put('ms_graph_token', tData.access_token, { expirationTtl: 3000 });
							}
						}

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
						/\b(email|message|write\s+to)\b/i.test(message) && /\b(them|court|clerk|judge|counsel|opposing|client)\b/i.test(message)) {
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
				if (true && !isEmailRequest) {
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
								const year = dateMatch[3] || new Date().getFullYear().toString();
								dueDate = `${year}-${month}-${day}`;
							}
							const slashDate = message.match(/\b(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?\b/);
							if (!dueDate && slashDate) {
								const month = slashDate[1].padStart(2, '0');
								const day = slashDate[2].padStart(2, '0');
								let year = slashDate[3] || new Date().getFullYear().toString();
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
								hearingTime = `${hour > 12 ? hour - 12 : hour}:${min} ${ampm}`;
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
											const triggerDateForCalc = dueDate || new Date().toISOString().split('T')[0];
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
										`INSERT INTO deadlines (client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, status, source, notes, created_at) VALUES (?, '', ?, ?, ?, ?, '', ?, ?, '', 'pending', 'manual', '', datetime('now'))`
									).bind(
										clientName || 'Unknown',
										deadlineType,
										`${deadlineType} - ${clientName || 'Event'}`,
										dueDate,
										hearingTime,
										courtroom,
										judge
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
										`UPDATE deadlines SET status = 'completed', completed_at = datetime('now') WHERE id = ?`
									).bind(target.id).run();
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
- You have Case Summaries with consolidated narratives, next events, and file counts. Use these for quick case status overviews.
- You have Case File Inventory showing what documents exist per client. Reference this when discussing filings, evidence, or document prep.
- You have Recent Timecards for the last 7 days. Reference these for billing context and workload awareness.
- ALL of this data is in your Memory Context below. Read it. Use it. Never say you don't have access to something that's in your context.

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

## Chat Context
${context || 'None provided.'}
${activeClient ? `\n## Active Client: ${activeClient}` : ''}

═══ REMINDER (re-read before EVERY response) ═══
You have MASTERED the data above. You know every client, every hearing, every deadline, every player.
WHICH HAT? Secretary tasks → execute immediately, no questions. Paralegal tasks → be thorough and anticipatory. Attorney tasks → be precise and cite law.
DO NOT ASK QUESTIONS you can answer from context. ONE match = that's the one. "C"/"Cs" = Client/Client's. Act like the person who already knows the whole caseload.
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

				// Helper: query Claude directly (with prompt caching for system prompt)
				const queryClaudeDirect = async (userContent: string): Promise<string> => {
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
							temperature: 0,
							system: [
								{
									type: 'text',
									text: synthiaSystemPrompt,
									cache_control: { type: 'ephemeral' }
								}
							],
							messages: [{ role: 'user', content: userContent }]
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

				// Helper: query Grok directly (second voice)
				const queryGrokDirect = async (userContent: string): Promise<string> => {
					if (!env.XAI_API_KEY) return '';
					const r = await fetch('https://api.x.ai/v1/chat/completions', {
						method: 'POST',
						headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.XAI_API_KEY}` },
						body: JSON.stringify({
							model: 'grok-3', temperature: 0,
							messages: [
								{ role: 'system', content: synthiaSystemPrompt },
								{ role: 'user', content: userContent }
							],
							max_tokens: 4000
						})
					});
					const d = await r.json() as any;
					return d.choices?.[0]?.message?.content || '';
				};

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

					// Role anchor — prepended to every user message to prevent mid-conversation drift
					const roleAnchor = `[SYNTHIA: You have mastered the desktop data. Identify your hat (Secretary/Paralegal/Attorney) for this task. Execute using context — do NOT ask questions you can answer yourself. "C"/"Cs"=Client's. One match=use it.]\n\n`;

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
					const roleAnchor = `[SYNTHIA: You have mastered the desktop data. Identify your hat (Secretary/Paralegal/Attorney) for this task. Execute using context — do NOT ask questions you can answer yourself. "C"/"Cs"=Client's. One match=use it.]\n\n`;
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
				} else if (emailAction === 'archive' && activeClient) {
					emailActionResult = { type: 'archive', status: 'pending', note: `Archive emails for ${activeClient} — use POST /api/email/archive endpoint to execute.` };
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
					...(activeClient && { activeClient })
				};

				// Cache in KV (24h for general, 1h for case-specific) — skip cache for email actions
				const ttl = message.toLowerCase().match(/case|client|hearing|deadline|motion/) ? 3600 : 86400;
				if (!emailAction && !isAlertMessage) {
					ctx.waitUntil(env.CACHE.put(cacheKey, JSON.stringify(result), { expirationTtl: ttl }));
				}

				// --- STEP 7: Store in chat history (continuous thread) ---
				// ALWAYS use synthia_master — one permanent thread across all devices
				ctx.waitUntil((async () => {
					try {
						await env.DB.prepare(`INSERT INTO chat_messages (session_id, role, content) VALUES ('synthia_master', 'user', ?)`).bind(message).run();
						await env.DB.prepare(`INSERT INTO chat_messages (session_id, role, content) VALUES ('synthia_master', 'assistant', ?)`).bind(consensus).run();

						// --- STEP 7b: Auto-summarize if messages exceed threshold ---
						// Every 60+ unsummarized messages, compress oldest 50 into a summary
						const countRes = await env.DB.prepare(
							`SELECT COUNT(*) as cnt FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant')`
						).first() as any;
						if ((countRes?.cnt || 0) > 60 && env.ANTHROPIC_API_KEY) {
							const { results: oldest } = await env.DB.prepare(
								`SELECT id, role, content, timestamp FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant') ORDER BY timestamp ASC LIMIT 40`
							).all();
							if (oldest?.length >= 40) {
								const text = (oldest as any[]).map(m => `[${m.role}]: ${(m.content || '').substring(0, 300)}`).join('\n');
								const sRes = await fetch('https://api.anthropic.com/v1/messages', {
									method: 'POST',
									headers: { 'Content-Type': 'application/json', 'x-api-key': env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
									body: JSON.stringify({
										model: 'claude-sonnet-4-20250514', max_tokens: 800, temperature: 0,
										messages: [{ role: 'user', content: `Compress this conversation block into a concise internal memory note for the AI and user (max 500 words). This is private — NOT public, NOT for court websites.\n\nInclude:\n1. FACTUAL SUMMARY: What specifically was discussed, built, changed, or decided? Include concrete details (file names, endpoints, code changes, client situations, case facts).\n2. CLIENT CONTEXT: Names, case numbers, case types, key facts about their situation.\n3. TASKS & OUTCOMES: What was completed, what's still pending, what failed and why.\n4. DECISIONS: Any choices made and the reasoning.\n5. TECHNICAL DETAILS: Specific files modified, APIs called, database changes, deployments.\n\nDo NOT include court website URLs or external links. Write as direct internal notes.\n\n${text}` }]
									})
								});
								const sData = await sRes.json() as any;
								const summary = sData.content?.[0]?.text;
								if (summary) {
									await env.MEMORY_DB.prepare(
										`INSERT INTO sessions (id, summary, started_at) VALUES (?, ?, datetime('now'))`
									).bind(`summary_${Date.now()}`, summary).run();
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
						exported_at: new Date().toISOString(),
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
					if (!includePast) q += ` AND due_date >= date('now')`;
					q += ` ORDER BY due_date ASC, hearing_time ASC LIMIT ?`;
					const { results } = await env.MEMORY_DB.prepare(q).bind(limit).all();
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
					let q = `SELECT client_name, case_number, case_type, court, client_role, opposing_party, judge, summary, next_event, next_event_date, status, file_count, updated_at FROM case_summaries WHERE status = 'active'`;
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
							const lev = (a: string, b: string): number => {
								const m=a.length,n=b.length; if(!m)return n; if(!n)return m;
								const d:number[][]=Array.from({length:m+1},()=>Array(n+1).fill(0));
								for(let i=0;i<=m;i++)d[i][0]=i; for(let j=0;j<=n;j++)d[0][j]=j;
								for(let i=1;i<=m;i++)for(let j=1;j<=n;j++)d[i][j]=Math.min(d[i-1][j]+1,d[i][j-1]+1,d[i-1][j-1]+(a[i-1]===b[j-1]?0:1));
								return d[m][n];
							};
							const { results: allCsNames } = await env.MEMORY_DB.prepare(`SELECT DISTINCT client_name FROM case_summaries WHERE status='active'`).all();
							const matched = (allCsNames as any[]).filter(r => {
								const cp = (r.client_name as string).toLowerCase().replace(/[^a-z\s]/g,'').split(/\s+/).filter((x:string)=>x.length>=2);
								let hits=0;
								for(const sp of parts){const sl=sp.toLowerCase();for(const c of cp){const th=Math.min(sl.length,c.length)<=4?1:2;if(c.includes(sl)||sl.includes(c)||lev(sl,c)<=th){hits++;break;}}}
								return hits>=parts.length;
							});
							if (matched.length > 0) {
								const names = matched.map(r=>`'${(r.client_name as string).replace(/'/g,"''")}'`).join(',');
								q = `SELECT client_name, case_number, case_type, court, client_role, opposing_party, judge, summary, next_event, next_event_date, status, file_count, updated_at FROM case_summaries WHERE status = 'active' AND client_name IN (${names})`;
								if (withEvents) q += ` AND next_event_date IS NOT NULL AND next_event_date >= date('now')`;
								q += ` ORDER BY next_event_date ASC NULLS LAST, client_name ASC`;
								binds.length = 0; // clear binds since names are inlined
							}
						}
					}
					if (withEvents) {
						q += ` AND next_event_date IS NOT NULL AND next_event_date >= date('now')`;
					}
					q += ` ORDER BY next_event_date ASC NULLS LAST, client_name ASC`;
					const stmt = env.MEMORY_DB.prepare(q);
					const { results } = binds.length > 0 ? await stmt.bind(...binds).all() : await stmt.all();
					return json({ success: true, summaries: results, count: results.length });
				} catch (e: any) {
					return json({ success: false, summaries: [], error: e.message });
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

			// Refresh/regenerate all case summaries from party_cache + deadlines + case_files
			// Called after scrape, file sync, or manually from dashboard
			if (path === '/api/case-summaries/refresh' && request.method === 'POST') {
				try {
					const today = new Date().toISOString().substring(0, 10);

					// 1. Load all party_cache entries
					const { results: parties } = await env.MEMORY_DB.prepare(
						`SELECT client_name, case_number, case_type, client_role, opposing_party, opposing_role, opposing_counsel, judge, court, district FROM party_cache ORDER BY client_name`
					).all();

					// 2. Load upcoming deadlines
					const { results: deadlines } = await env.MEMORY_DB.prepare(
						`SELECT client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, status FROM deadlines WHERE status IN ('active','pending') AND due_date >= date('now') ORDER BY due_date ASC, hearing_time ASC`
					).all();

					// 3. Load file counts per client
					const { results: fileCounts } = await env.MEMORY_DB.prepare(
						`SELECT client_name, COUNT(*) as cnt FROM case_files WHERE source = 'open' GROUP BY client_name`
					).all();

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

					let upserted = 0, errors = 0;
					const batch: any[] = [];

					for (const p of parties as any[]) {
						const name = p.client_name as string;
						const caseNum = p.case_number as string;

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

						// Build summary
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

						batch.push(env.MEMORY_DB.prepare(
							`INSERT INTO case_summaries (client_name, case_number, case_type, court, client_role, opposing_party, judge, summary, next_event, next_event_date, status, file_count, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?) ON CONFLICT(client_name, case_number) DO UPDATE SET case_type=excluded.case_type, court=excluded.court, client_role=excluded.client_role, opposing_party=excluded.opposing_party, judge=excluded.judge, summary=excluded.summary, next_event=excluded.next_event, next_event_date=excluded.next_event_date, file_count=excluded.file_count, updated_at=excluded.updated_at`
						).bind(name, caseNum, p.case_type, p.court, p.client_role, p.opposing_party, p.judge, s, nextEvDesc, nextEvDate, fc, today, today));
						upserted++;
					}

					// Execute in batches of 50 (D1 batch limit ~100)
					for (let i = 0; i < batch.length; i += 50) {
						await env.MEMORY_DB.batch(batch.slice(i, i + 50));
					}

					return json({ success: true, upserted, errors, date: today, parties: (parties as any[]).length, deadlines: (deadlines as any[]).length });
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
					if (!caseNumber) return errJson('caseNumber required', 400);

					const today = new Date().toISOString().substring(0, 10);
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
					if (!dockets || !Array.isArray(dockets)) return errJson('dockets array required', 400);

					const today = new Date().toISOString().substring(0, 10);
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
					if (!items || !Array.isArray(items)) return errJson('items array required', 400);

					const results: any[] = [];
					for (const item of items) {
						try {
							// Build a sub-request to the appropriate endpoint
							const subUrl = new URL(item.url.replace(API_BASE || '', ''), 'https://api.esqs-law.com');
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

			// Auto-summary: summarize older messages into compressed memory
			if (path === '/api/chat/summarize' && request.method === 'POST') {
				try {
					// Count total messages
					const countResult = await env.DB.prepare(
						`SELECT COUNT(*) as cnt FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant')`
					).first() as any;
					const totalMessages = countResult?.cnt || 0;

					if (totalMessages < 50) {
						return json({ success: true, message: 'Not enough messages to summarize yet', total: totalMessages });
					}

					// Get oldest 50 messages that haven't been summarized
					const { results: oldMessages } = await env.DB.prepare(
						`SELECT id, role, content, timestamp FROM chat_messages WHERE session_id = 'synthia_master' AND role IN ('user', 'assistant') ORDER BY timestamp ASC LIMIT 50`
					).all();

					if (!oldMessages?.length || !env.ANTHROPIC_API_KEY) {
						return json({ success: false, error: 'No messages to summarize or no API key' });
					}

					// Ask Claude to summarize
					const messagesText = (oldMessages as any[]).map(m => `[${m.role}]: ${m.content}`).join('\n');
					const summaryRes = await fetch('https://api.anthropic.com/v1/messages', {
						method: 'POST',
						headers: {
							'Content-Type': 'application/json',
							'x-api-key': env.ANTHROPIC_API_KEY,
							'anthropic-version': '2023-06-01'
						},
						body: JSON.stringify({
							model: 'claude-sonnet-4-20250514',
							max_tokens: 1000,
							temperature: 0,
							messages: [{
								role: 'user',
								content: `Summarize this conversation block into a concise internal memory note for the AI and user (max 500 words). This is private — NOT public, NOT for court websites.\n\nInclude:\n1. FACTUAL SUMMARY: What specifically was discussed, built, changed, or decided? Include concrete details (file names, endpoints, code changes, client situations, case facts).\n2. CLIENT CONTEXT: Names, case numbers, case types, key facts about their situation.\n3. TASKS & OUTCOMES: What was completed, what's still pending, what failed and why.\n4. DECISIONS: Any choices made and the reasoning.\n5. TECHNICAL DETAILS: Specific files modified, APIs called, database changes, deployments.\n\nDo NOT include court website URLs or external links. Write as direct internal notes.\n\n${messagesText}`
							}]
						})
					});

					const summaryData = await summaryRes.json() as any;
					const summary = summaryData.content?.[0]?.text || 'Summary generation failed';

					// Store summary in MEMORY_DB
					await env.MEMORY_DB.prepare(
						`INSERT INTO sessions (id, summary, started_at) VALUES (?, ?, datetime('now'))`
					).bind(`summary_${Date.now()}`, summary).run();

					// Delete the old messages that were summarized (keep the thread clean)
					const oldIds = (oldMessages as any[]).map(m => m.id);
					if (oldIds.length > 0) {
						await env.DB.prepare(
							`DELETE FROM chat_messages WHERE id IN (${oldIds.map(() => '?').join(',')}) AND session_id = 'synthia_master'`
						).bind(...oldIds).run();
					}

					// Insert summary as a system message in the thread
					await env.DB.prepare(
						`INSERT INTO chat_messages (session_id, role, content) VALUES ('synthia_master', 'summary', ?)`
					).bind(`📋 Auto-Summary: ${summary}`).run();

					return json({ success: true, summarized: oldMessages.length, summary: summary.substring(0, 200) + '...' });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// Legacy session endpoints (backward compatible)
			const chatMatch = path.match(/^\/api\/chat\/session\/([^/]+)$/);
			if (chatMatch) {
				const sessionId = chatMatch[1];
				if (request.method === 'GET') {
					const { results } = await env.DB.prepare('SELECT content, role, timestamp FROM chat_messages WHERE session_id = ? ORDER BY timestamp ASC').bind(sessionId).all();
					return json({ success: true, messages: results });
				}
				if (request.method === 'POST') {
					const { role, content } = await request.json() as any;
					if (!content) return err('Missing content', 400);
					await env.DB.prepare('INSERT INTO chat_messages (session_id, role, content) VALUES (?, ?, ?)').bind(sessionId, role || 'user', content).run();
					return json({ success: true });
				}
			}

			if (path === '/api/chat/all' && request.method === 'GET') {
				const { results } = await env.DB.prepare('SELECT session_id, role, content, timestamp FROM chat_messages ORDER BY timestamp DESC LIMIT 500').all();
				return json({ success: true, messages: results });
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

				// Levenshtein distance for misspelling tolerance
				const levenshtein = (a: string, b: string): number => {
					const m = a.length, n = b.length;
					if (m === 0) return n; if (n === 0) return m;
					const dp: number[][] = Array.from({length: m+1}, () => Array(n+1).fill(0));
					for (let i = 0; i <= m; i++) dp[i][0] = i;
					for (let j = 0; j <= n; j++) dp[0][j] = j;
					for (let i = 1; i <= m; i++)
						for (let j = 1; j <= n; j++)
							dp[i][j] = Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1] + (a[i-1]===b[j-1]?0:1));
					return dp[m][n];
				};
				// Check if any name part is "close enough" to any word in a candidate name
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
									download_url: f['@microsoft.graph.downloadUrl'] || null,
									web_url: f.webUrl || null,
									source: 'onedrive'
								}));
							}
						}
					} catch (odErr: any) {
						console.error('OneDrive facesheet lookup (non-fatal):', odErr.message);
					}

					// Build facesheet
					const party = (parties as any[])[0] || null;
					const facesheet = {
						client_name: party?.client_name || clientName,
						case_number: party?.case_number || null,
						case_type: party?.case_type || null,
						client_role: party?.client_role || null,
						opposing_party: party?.opposing_party || null,
						opposing_role: party?.opposing_role || null,
						opposing_counsel: party?.opposing_counsel || null,
						judge: party?.judge || null,
						court: party?.court || null,
						district: party?.district || null,
						all_cases: parties,
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
						counts: {
							cases: (parties as any[]).length,
							events: (deadlines as any[]).length,
							files: (files as any[]).length,
							oneDriveFiles: oneDriveFiles.length,
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
					`INSERT INTO documents (case_id, doc_name, doc_type, r2_key, file_size, created_date) VALUES (?, ?, ?, ?, ?, datetime('now'))`
				).bind(caseId, file.name, docType, r2Key, file.size).run();
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

			// --- Gmail OAuth2 token + send helper (esqslaw@gmail.com) ---
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
					console.error('Gmail token error:', JSON.stringify(data));
					throw new Error(`Failed to get Gmail token: ${data.error_description || data.error || 'Unknown error'}`);
				}
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

			// Note: getGraphToken is defined above in the Email section

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
			
			// Sync all client folders from OneDrive to D1 cache
			if (path === '/api/onedrive/sync' && request.method === 'POST') {
				try {
					const token = await getGraphToken();
					// Get all folders in Open Cases using user's OneDrive
					const foldersUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${env.ONEDRIVE_FOLDER_ID}/children`;
					const res = await fetch(foldersUrl, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const data = await res.json() as any;
					
					let synced = 0;
					for (const folder of (data.value || [])) {
						if (folder.folder) {
							// Cache folder info in KV
							await env.CACHE.put(
								`onedrive:folder:${folder.name}`,
								JSON.stringify({ id: folder.id, name: folder.name, childCount: folder.folder.childCount }),
								{ expirationTtl: 3600 }
							);
							synced++;
						}
					}
					
					return json({ success: true, synced, message: `Synced ${synced} client folders` });
				} catch (error: any) {
					return json({ success: false, error: error.message });
				}
			}
			
			// ═══════════════════════════════════════════════════════════════
			// LOCAL FILE SYNC (bypasses OAuth - reads from local OneDrive sync)
			// ═══════════════════════════════════════════════════════════════
			
			// Ingest files from local sync script
			if (path === '/api/files/ingest' && request.method === 'POST') {
				const { files, client } = await request.json() as any;
				if (!files || !Array.isArray(files)) return err('files array required', 400);
				
				// Store in KV cache by client
				const key = client ? `files:client:${client}` : 'files:all';
				await env.CACHE.put(key, JSON.stringify({
					files,
					lastSync: new Date().toISOString(),
					count: files.length
				}), { expirationTtl: 86400 }); // 24 hour cache
				
				return json({ success: true, ingested: files.length, client });
			}
			
			// Bulk ingest all client folders
			if (path === '/api/files/ingest-all' && request.method === 'POST') {
				const { clients } = await request.json() as any;
				if (!clients || !Array.isArray(clients)) return err('clients array required', 400);
				
				let totalFiles = 0;
				for (const c of clients) {
					if (c.name && c.files) {
						await env.CACHE.put(`files:client:${c.name}`, JSON.stringify({
							files: c.files,
							lastSync: new Date().toISOString(),
							count: c.files.length
						}), { expirationTtl: 86400 });
						totalFiles += c.files.length;
					}
				}
				
				// Store client list
				await env.CACHE.put('files:clients', JSON.stringify({
					clients: clients.map((c: any) => ({ name: c.name, fileCount: c.files?.length || 0 })),
					lastSync: new Date().toISOString()
				}), { expirationTtl: 86400 });
				
				return json({ success: true, clients: clients.length, totalFiles });
			}
			
			// Get files for a client (from cache)
			if (path === '/api/files/client' && request.method === 'GET') {
				const clientName = url.searchParams.get('name');
				if (!clientName) return err('name parameter required', 400);
				
				const cached = await env.CACHE.get(`files:client:${clientName}`);
				if (cached) {
					return json({ success: true, ...JSON.parse(cached) });
				}
				
				// Try partial match
				const clientList = await env.CACHE.get('files:clients');
				if (clientList) {
					const { clients } = JSON.parse(clientList);
					const match = clients.find((c: any) => 
						c.name.toLowerCase().includes(clientName.toLowerCase()) ||
						clientName.toLowerCase().includes(c.name.toLowerCase())
					);
					if (match) {
						const matchedCache = await env.CACHE.get(`files:client:${match.name}`);
						if (matchedCache) {
							return json({ success: true, ...JSON.parse(matchedCache), matchedFrom: clientName });
						}
					}
				}
				
				return json({ success: true, files: [], message: 'No files cached for this client. Run sync script.' });
			}
			
			// List all cached clients
			if (path === '/api/files/clients' && request.method === 'GET') {
				const cached = await env.CACHE.get('files:clients');
				if (cached) {
					return json({ success: true, ...JSON.parse(cached) });
				}
				return json({ success: true, clients: [], message: 'No clients cached. Run sync script.' });
			}

			// OneDrive webhook for real-time updates
			if (path === '/api/onedrive/webhook') {
				// Validation request from Microsoft
				const validationToken = url.searchParams.get('validationToken');
				if (validationToken) {
					return new Response(validationToken, {
						status: 200,
						headers: { 'Content-Type': 'text/plain' }
					});
				}
				
				// Actual change notification
				if (request.method === 'POST') {
					const body = await request.json() as any;
					console.log('OneDrive webhook notification:', JSON.stringify(body));
					// Queue a sync (using waitUntil to not block response)
					ctx.waitUntil((async () => {
						// Invalidate cache so next request fetches fresh data
						await env.CACHE.delete('ms_graph_token');
					})());
					return new Response(null, { status: 202 });
				}
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
};
