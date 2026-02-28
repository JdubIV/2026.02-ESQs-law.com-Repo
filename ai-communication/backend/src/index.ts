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
	COURTLISTENER_API_TOKEN: string;
	ZOOM_ACCOUNT_ID: string;
	ZOOM_CLIENT_ID: string;
	ZOOM_CLIENT_SECRET: string;
	PERSONAL_MS_REFRESH_TOKEN: string;
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

// ═══════════════════════════════════════════════════════════════
// COURTLISTENER CITATION VERIFICATION ENGINE
// Intercepts AI responses, verifies case law citations, flags phantom law
// ═══════════════════════════════════════════════════════════════

interface CitationResult {
	found: number;
	valid: number;
	invalid: number;
	ambiguous: number;
	validCitations: { citation: string; case_name?: string; url?: string; court?: string; year?: number }[];
	invalidCitations: string[];
	ambiguousCitations: string[];
	overallResult: 'pass' | 'flag' | 'fail';
}

// Regex patterns to detect legal citations in text
const CITATION_PATTERNS = [
	// Standard reporter citations: 123 U.S. 456, 123 F.3d 456, 123 P.3d 456, etc.
	/\d{1,4}\s+(?:U\.S\.|S\.\s?Ct\.|L\.\s?Ed(?:\.\s?2d)?|F\.\s?(?:2d|3d|4th)|F\.\s?Supp(?:\.\s?(?:2d|3d))?|P\.\s?(?:2d|3d)|A\.\s?(?:2d|3d)?|So\.\s?(?:2d|3d)?|N\.E\.\s?(?:2d|3d)?|N\.W\.\s?(?:2d)?|S\.E\.\s?(?:2d)?|S\.W\.\s?(?:2d|3d)?|Cal\.\s?(?:2d|3d|4th|5th)?|N\.Y\.\s?(?:2d|3d)?)\s+\d{1,5}/g,
	// Utah-specific: 2024 UT 12, 2024 UT App 45
	/\d{4}\s+UT\s+(?:App\s+)?\d{1,4}/g,
	// Parallel citations with Utah reporter
	/\d{1,4}\s+Utah\s+(?:2d\s+)?\d{1,5}/g,
];

// Check if text contains legal citations worth verifying
function containsCitations(text: string): boolean {
	return CITATION_PATTERNS.some(p => {
		p.lastIndex = 0; // Reset regex state
		return p.test(text);
	});
}

// Extract all citations from text
function extractCitations(text: string): string[] {
	const citations = new Set<string>();
	for (const pattern of CITATION_PATTERNS) {
		pattern.lastIndex = 0;
		let match;
		while ((match = pattern.exec(text)) !== null) {
			citations.add(match[0].trim());
		}
	}
	return Array.from(citations);
}

// Utah court IDs in CourtListener
const UTAH_COURTS = ['utahsupremecourt', 'utah', 'utahctapp'];

// Extract searchable legal query from conversational user message
function extractLegalQuery(message: string): string {
	let q = message
		.replace(/\b(draft|write|compose|prepare|create|generate|make|research|find|look\s+up|search\s+for)\s+(a\s+|an\s+|the\s+|me\s+)?/gi, '')
		.replace(/\b(for|regarding|about|concerning|re:?)\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)?\b/g, '') // strip "for Smith"
		.replace(/\b(for|regarding|about|concerning|re:?)\s+[A-Z]{2,}(\s+[A-Z]{2,})?\b/g, '') // strip "for BOYACK"
		.replace(/\b(please|can you|could you|would you|i need|we need|help me|tell me|what'?s the law on)\b/gi, '')
		.replace(/\b(case\s*#?\s*\d{6,12})\b/g, '') // strip case numbers
		.replace(/\b(in\s+utah|utah\s+law)\b/gi, 'Utah') // preserve jurisdiction
		.replace(/\s+/g, ' ').trim();
	// If too short after stripping, return original with minimal cleanup
	if (q.length < 10) q = message.replace(/\b(please|can you|could you|help me)\b/gi, '').trim();
	// Cap at 200 chars for CourtListener query length limits
	return q.substring(0, 200);
}

async function verifyCitationsCourtListener(
	text: string,
	token: string,
	env: Env
): Promise<CitationResult> {
	const result: CitationResult = {
		found: 0, valid: 0, invalid: 0, ambiguous: 0,
		validCitations: [], invalidCitations: [], ambiguousCitations: [],
		overallResult: 'pass'
	};

	// Extract citations from text
	const rawCitations = extractCitations(text);
	if (rawCitations.length === 0) return result;
	result.found = rawCitations.length;

	const clHeaders = {
		'Authorization': `Token ${token}`,
		'Content-Type': 'application/json',
	};

	// --- METHOD 1: Citation Lookup API (bulk text scan) ---
	// Returns array of citation objects. Each has: citation, status (200=found, 404=not found),
	// clusters[] with case details when found
	try {
		const lookupRes = await fetch('https://www.courtlistener.com/api/rest/v4/citation-lookup/', {
			method: 'POST',
			headers: clHeaders,
			body: JSON.stringify({ text: text.substring(0, 64000) }) // API limit
		});

		if (lookupRes.ok) {
			const lookupData = await lookupRes.json() as any;
			// Response is an array of citation match objects
			const citations = Array.isArray(lookupData) ? lookupData : (lookupData.citations || []);
			for (const c of citations) {
				const citText = c.citation || c.normalized_citations?.[0] || '';
				if (c.status === 200 && c.clusters?.length > 0) {
					// VERIFIED — found in CourtListener database
					const cluster = c.clusters[0];
					result.valid++;
					result.validCitations.push({
						citation: citText,
						case_name: cluster.case_name || cluster.case_name_short || '',
						url: cluster.absolute_url ? `https://www.courtlistener.com${cluster.absolute_url}` : '',
						court: cluster.court_id || '',
						year: cluster.date_filed ? parseInt(cluster.date_filed.substring(0, 4)) : undefined
					});
				} else if (c.status === 200 && (!c.clusters || c.clusters.length === 0)) {
					// Citation format recognized but no matching case
					result.ambiguous++;
					result.ambiguousCitations.push(citText);
				} else if (c.status === 404 || (c.status && c.status !== 200)) {
					// NOT FOUND — potential phantom citation
					result.invalid++;
					result.invalidCitations.push(citText);
				}
			}
			// Track which raw citations were covered by the lookup
			const coveredCitations = citations.map((c: any) => c.citation || c.normalized_citations?.[0] || '');
			// If lookup handled all citations, return early
			if (result.valid + result.invalid + result.ambiguous >= result.found) {
				result.overallResult = result.invalid > 0 ? 'fail' : result.ambiguous > 0 ? 'flag' : 'pass';
				return result;
			}
		}
	} catch (lookupErr: any) {
		console.error('CourtListener citation-lookup error:', lookupErr.message);
	}

	// --- METHOD 2: Individual citation search for anything not caught by bulk lookup ---
	const allChecked = new Set([
		...result.validCitations.map(v => v.citation),
		...result.invalidCitations,
		...result.ambiguousCitations
	]);
	const uncheckedCitations = rawCitations.filter(c =>
		!allChecked.has(c) &&
		!result.validCitations.some(v => v.citation.includes(c) || c.includes(v.citation))
	);

	for (const citation of uncheckedCitations.slice(0, 10)) { // Limit to 10 individual checks
		try {
			// Use citation-lookup for individual citation (most reliable)
			const singleRes = await fetch('https://www.courtlistener.com/api/rest/v4/citation-lookup/', {
				method: 'POST',
				headers: clHeaders,
				body: JSON.stringify({ text: citation })
			});
			if (singleRes.ok) {
				const singleData = await singleRes.json() as any;
				const matches = Array.isArray(singleData) ? singleData : [];
				if (matches.length > 0 && matches[0].status === 200 && matches[0].clusters?.length > 0) {
					const cluster = matches[0].clusters[0];
					result.valid++;
					result.validCitations.push({
						citation,
						case_name: cluster.case_name || cluster.case_name_short || '',
						url: cluster.absolute_url ? `https://www.courtlistener.com${cluster.absolute_url}` : '',
						court: cluster.court_id || '',
						year: cluster.date_filed ? parseInt(cluster.date_filed.substring(0, 4)) : undefined
					});
				} else {
					// Not found via citation-lookup — mark as invalid (likely phantom)
					result.invalid++;
					result.invalidCitations.push(citation);
				}
			} else {
				result.ambiguous++;
				result.ambiguousCitations.push(citation);
			}
		} catch (searchErr: any) {
			console.error(`CourtListener verify error for "${citation}":`, searchErr.message);
			result.ambiguous++;
			result.ambiguousCitations.push(citation);
		}
	}

	result.overallResult = result.invalid > 0 ? 'fail' : result.ambiguous > 0 ? 'flag' : 'pass';
	return result;
}

// Verify judge names against CourtListener People API
async function verifyJudgeName(judgeName: string, token: string): Promise<{ verified: boolean; matchedName?: string; id?: number; positions?: string[] }> {
	if (!judgeName || judgeName.length < 2) return { verified: false };
	try {
		// Extract last name (handle "Judge Smith", "Hon. Smith", etc.)
		const cleaned = judgeName.replace(/^(judge|justice|hon\.?|the honorable)\s+/i, '').trim();
		const lastName = cleaned.split(/\s+/).pop() || cleaned;

		const res = await fetch(
			`https://www.courtlistener.com/api/rest/v4/people/?name_last=${encodeURIComponent(lastName)}`,
			{ headers: { 'Authorization': `Token ${token}` } }
		);
		if (!res.ok) return { verified: false };
		const data = await res.json() as any;
		if (data.results?.length > 0) {
			// If first name was provided, try to find best match
			const firstName = cleaned.split(/\s+/).length > 1 ? cleaned.split(/\s+/)[0].toLowerCase() : '';
			let match = data.results[0];
			if (firstName) {
				const betterMatch = data.results.find((p: any) =>
					(p.name_first || '').toLowerCase().startsWith(firstName)
				);
				if (betterMatch) match = betterMatch;
			}
			return {
				verified: true,
				matchedName: `${match.name_first || ''} ${match.name_middle || ''} ${match.name_last || ''}`.replace(/\s+/g, ' ').trim(),
				id: match.id,
				positions: (match.positions || []).map((p: any) => typeof p === 'string' ? p : '').filter(Boolean)
			};
		}
		return { verified: false };
	} catch {
		return { verified: false };
	}
}

// Log evaluation result to D1
async function logEval(
	db: D1Database,
	evalType: string,
	source: string,
	result: CitationResult,
	responseSnippet: string
): Promise<void> {
	try {
		const id = `eval_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
		await db.prepare(
			`INSERT INTO eval_log (id, timestamp, eval_type, source, citations_found, citations_valid, citations_invalid, citations_ambiguous, invalid_citations, ambiguous_citations, valid_citations, overall_result, response_snippet) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		).bind(
			id, mtnISO(), evalType, source,
			result.found, result.valid, result.invalid, result.ambiguous,
			JSON.stringify(result.invalidCitations),
			JSON.stringify(result.ambiguousCitations),
			JSON.stringify(result.validCitations.map(v => v.citation)),
			result.overallResult,
			responseSnippet.substring(0, 500)
		).run();
	} catch (e: any) {
		console.error('Eval log write error:', e.message);
	}
}

// Strip or annotate invalid citations from response text
function annotateResponse(text: string, result: CitationResult): string {
	let annotated = text;

	// For each invalid citation, wrap with warning (use negative lookbehind to avoid double-annotation)
	for (const bad of result.invalidCitations) {
		const escaped = bad.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
		// Only replace if not already inside an annotation marker
		annotated = annotated.replace(
			new RegExp(`(?<!UNVERIFIED: )(?<!\\[⚡ verify\\])${escaped}(?![^\\[]*\\]\\*\\*)`, 'g'),
			`⚠️ **[UNVERIFIED: ${bad}]**`
		);
	}

	// For ambiguous citations, add softer flag (skip already-flagged)
	for (const amb of result.ambiguousCitations) {
		const escaped = amb.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
		annotated = annotated.replace(
			new RegExp(`(?<!UNVERIFIED: )${escaped}(?! \\[⚡)`, 'g'),
			`${amb} [⚡ verify]`
		);
	}

	// Add verification footer if any issues found
	if (result.invalid > 0 || result.ambiguous > 0) {
		annotated += '\n\n---\n⚠️ **Citation Verification Alert**';
		if (result.invalid > 0) {
			annotated += `\n🔴 **${result.invalid} citation(s) could NOT be verified** against CourtListener's database of 9M+ decisions: ${result.invalidCitations.join('; ')}`;
		}
		if (result.ambiguous > 0) {
			annotated += `\n🟡 **${result.ambiguous} citation(s) need manual verification**: ${result.ambiguousCitations.join('; ')}`;
		}
		if (result.valid > 0) {
			annotated += `\n🟢 ${result.valid} citation(s) verified.`;
		}
	}

	return annotated;
}

// ═══════════════════════════════════════════════════════════════
// SHEPARDIZE ENGINE — Negative Treatment Detection
// Uses CourtListener cited-by + opinion text to detect if a case
// has been overruled, reversed, abrogated, distinguished, etc.
// Signal emojis: 🪶 best law | 👍 good law | 👌 caution | 👎 bad law
// ═══════════════════════════════════════════════════════════════

interface ShepardizeResult {
	citation: string;
	case_name: string;
	cluster_id: string;
	cluster_url: string;
	date_filed: string;
	court: string;
	signal: '🟢' | '🔵' | '🟡' | '🔴';
	signal_label: string;
	signal_color: string; // green, blue, yellow, red
	total_citing: number;
	negative_treatments: {
		type: string; // 'overruled', 'reversed', 'abrogated', 'superseded', 'distinguished', 'questioned', 'criticized', 'limited'
		citing_case: string;
		citing_citation: string;
		court: string;
		date: string;
		snippet: string; // The sentence containing the negative treatment
		url: string;
	}[];
	positive_treatments: {
		type: string; // 'followed', 'affirmed', 'cited approvingly', 'relied upon'
		citing_case: string;
		citing_citation: string;
		court: string;
		date: string;
		url: string;
	}[];
	summary: string;
	precedential_status: string;
}

const NEGATIVE_TREATMENT_PATTERNS: { type: string; patterns: RegExp[] }[] = [
	{ type: 'overruled', patterns: [
		/\boverrul(?:ed|ing)\b/i, /\bexpressly\s+overrul/i, /\bno\s+longer\s+good\s+law\b/i,
		/\babrogat(?:ed|ing)\s+(?:by|in)\b/i
	]},
	{ type: 'reversed', patterns: [
		/\brevers(?:ed|ing)\s+(?:and|on|in|the)\b/i, /\bvacated\s+(?:and|by|in)\b/i,
		/\bset\s+aside\b/i
	]},
	{ type: 'superseded', patterns: [
		/\bsupersed(?:ed|ing)\s+by\b/i, /\breplaced\s+by\b/i,
		/\babrogat(?:ed|ing)\s+by\s+statute\b/i
	]},
	{ type: 'distinguished', patterns: [
		/\bdistinguish(?:ed|ing)\b/i, /\binapplicable\b/i, /\bnot\s+controlling\b/i,
		/\bfactually\s+distinguishable\b/i
	]},
	{ type: 'questioned', patterns: [
		/\bquestion(?:ed|ing)\s+(?:the|whether|by)\b/i, /\bcast(?:s|ing)?\s+doubt\b/i,
		/\bdoubt(?:ed|ful|ing)\b/i, /\bundermin(?:ed|ing)\b/i
	]},
	{ type: 'criticized', patterns: [
		/\bcriticiz(?:ed|ing)\b/i, /\bdisapproved?\b/i, /\brejected?\s+(?:the|this)?\s*(?:reasoning|holding|analysis)\b/i,
		/\bdeclined?\s+to\s+follow\b/i
	]},
	{ type: 'limited', patterns: [
		/\blimit(?:ed|ing)\s+(?:the|to)\b/i, /\bnarrow(?:ed|ing|ly)\s+(?:the|its|construed)\b/i,
		/\bconfin(?:ed|ing)\s+(?:to|the)\b/i
	]},
];

const POSITIVE_TREATMENT_PATTERNS: { type: string; patterns: RegExp[] }[] = [
	{ type: 'followed', patterns: [/\bfollow(?:ed|ing)\b/i, /\badher(?:ed|ing)\s+to\b/i, /\breaffirm(?:ed|ing)\b/i] },
	{ type: 'affirmed', patterns: [/\baffirm(?:ed|ing)\b/i, /\bupheld\b/i, /\buphold(?:ing)?\b/i] },
	{ type: 'cited approvingly', patterns: [/\bapprov(?:ed|ingly)\b/i, /\bendors(?:ed|ing)\b/i, /\brelied?\s+(?:on|upon)\b/i] },
];

async function shepardize(citation: string, token: string): Promise<ShepardizeResult> {
	const clHeaders = { 'Authorization': `Token ${token}`, 'Content-Type': 'application/json' };
	const authHeader = { 'Authorization': `Token ${token}` };

	// Step 1: Resolve citation to cluster
	const lookupRes = await fetch('https://www.courtlistener.com/api/rest/v4/citation-lookup/', {
		method: 'POST', headers: clHeaders,
		body: JSON.stringify({ text: citation })
	});
	if (!lookupRes.ok) throw new Error(`Citation lookup failed: ${lookupRes.status}`);
	const lookupData = await lookupRes.json() as any;
	const matches = Array.isArray(lookupData) ? lookupData : [];

	if (matches.length === 0 || !matches[0].clusters?.length) {
		throw new Error(`Citation not found in CourtListener: ${citation}`);
	}

	const cluster = matches[0].clusters[0];
	const clusterId = cluster.id.toString();
	const caseName = cluster.case_name || cluster.case_name_short || '';
	const clusterUrl = cluster.absolute_url ? `https://www.courtlistener.com${cluster.absolute_url}` : '';
	const dateFiled = cluster.date_filed || '';
	const court = cluster.court_id || cluster.court || '';
	const precedentialStatus = cluster.precedential_status || 'Unknown';

	// Step 2: Get total citing count
	const countRes = await fetch(
		`https://www.courtlistener.com/api/rest/v4/search/?q=cites%3A(${clusterId})&type=o&page_size=1`,
		{ headers: authHeader }
	);
	const countData = countRes.ok ? await countRes.json() as any : { count: 0 };
	const totalCiting = countData.count || 0;

	// Step 3: TARGETED negative treatment searches
	// Instead of scanning random opinions, search for opinions that cite this case AND contain negative language
	const negativeTreatments: ShepardizeResult['negative_treatments'] = [];
	const positiveTreatments: ShepardizeResult['positive_treatments'] = [];

	const negativeSearchTerms = [
		{ type: 'overruled', terms: ['overruled', 'overruling', 'no longer good law', 'abrogated'] },
		{ type: 'reversed', terms: ['reversed', 'vacated', 'set aside'] },
		{ type: 'superseded', terms: ['superseded by statute', 'abrogated by statute', 'legislatively overruled'] },
		{ type: 'questioned', terms: ['questioned', 'cast doubt', 'undermined'] },
		{ type: 'criticized', terms: ['criticized', 'disapproved', 'declined to follow', 'rejected the reasoning'] },
	];

	// Short name for matching in opinion text (e.g., "Bowers" from "Bowers v. Hardwick")
	const shortName = caseName.split(/\s+v\.?\s+/)[0]?.trim() || caseName;

	// Strategy: Search for citing opinions that contain BOTH the negative term AND our case name
	// CourtListener full-text search handles the filtering — no need to fetch opinion text
	// Use full case name for searching to avoid false positives
	// "overruled Strickland" matches "overruled Strickland's objection" — bad
	// "overruled" "Strickland v. Washington" — requires both terms in the opinion — good
	const caseRef = caseName; // "Strickland v. Washington"
	const negativeSearchPhrases: { type: string; query: string }[] = [
		{ type: 'overruled', query: `cites:(${clusterId}) ("overruled" OR "overruling" OR "no longer good law") "${caseRef}"` },
		{ type: 'reversed', query: `cites:(${clusterId}) ("reversed" OR "vacated" OR "set aside") "${caseRef}"` },
		{ type: 'superseded', query: `cites:(${clusterId}) ("superseded" OR "abrogated by statute" OR "legislatively overruled") "${caseRef}"` },
		{ type: 'questioned', query: `cites:(${clusterId}) ("questioned" OR "cast doubt" OR "undermined") "${caseRef}"` },
		{ type: 'criticized', query: `cites:(${clusterId}) ("criticized" OR "disapproved" OR "declined to follow") "${caseRef}"` },
	];

	// Run all negative treatment searches in parallel
	// Strategy: search finds candidates, then verify ONE opinion text per type
	// to confirm the negative term is directed AT our case (not just coexisting)
	const negSearchPromises = negativeSearchPhrases.map(async ({ type, query }) => {
		try {
			const q = encodeURIComponent(query);
			const res = await fetch(
				`https://www.courtlistener.com/api/rest/v4/search/?q=${q}&type=o&page_size=10`,
				{ headers: authHeader }
			);
			if (!res.ok) return;
			const data = await res.json() as any;
			if (data.count === 0) return;

			// Verify: fetch ONE opinion text to confirm the negative term is about our case
			const negTerms = type === 'overruled' ? ['overruled', 'overruling', 'no longer good law']
				: type === 'reversed' ? ['reversed', 'vacated', 'set aside']
				: type === 'superseded' ? ['superseded', 'abrogated']
				: type === 'questioned' ? ['questioned', 'cast doubt', 'undermined']
				: ['criticized', 'disapproved', 'declined to follow'];

			let verified = false;
			let checkedOpinions = 0;
			for (const r of (data.results || []).slice(0, 10)) {
				if (verified || checkedOpinions >= 5) break;
				// Try first opinion per result (relevance-ordered, so best matches come first)
				const opId = r.opinions?.[0]?.id;
				if (!opId) continue;

				try {
					const opRes = await fetch(`https://www.courtlistener.com/api/rest/v4/opinions/${opId}/`, { headers: authHeader });
					if (!opRes.ok) continue;
					checkedOpinions++;
					const opData = await opRes.json() as any;
					const fullText = (opData.plain_text || opData.html_with_citations || opData.html || '')
						.replace(/<[^>]+>/g, ' ').substring(0, 80000);
					const fullTextLower = fullText.toLowerCase();

					// Skip this opinion entirely if it doesn't mention our case
					const caseRefs = [caseRef.toLowerCase(), `${shortName} v.`.toLowerCase()];
					const hasCaseRef = caseRefs.some(ref => fullTextLower.includes(ref));
					if (!hasCaseRef) { checkedOpinions--; continue; }

					// Windowed proximity search: find each mention of our case name,
					// then check if a negative treatment term appears within 200 chars
					for (const ref of caseRefs) {
						let searchFrom = 0;
						while (searchFrom < fullTextLower.length && !verified) {
							const idx = fullTextLower.indexOf(ref, searchFrom);
							if (idx < 0) break;

							const windowStart = Math.max(0, idx - 200);
							const windowEnd = Math.min(fullText.length, idx + ref.length + 200);
							const windowText = fullTextLower.substring(windowStart, windowEnd);

							const matchedTerm = negTerms.find(t => windowText.includes(t.toLowerCase()));
							if (matchedTerm) {
								const termIdx = windowText.indexOf(matchedTerm.toLowerCase());
								const caseIdxInWindow = idx - windowStart;
								// False-positive check 1: term BEFORE case — "overruled under [case]"
								if (termIdx < caseIdxInWindow) {
									const between = windowText.substring(termIdx + matchedTerm.length, caseIdxInWindow);
									if (/\b(under|pursuant to|applying|per|following|standard|test|analysis|framework|set forth in|established in|requirements of|as required by)\b/i.test(between)) {
										;
										searchFrom = idx + ref.length;
										continue;
									}
								}
								// False-positive check 2: term AFTER case — "[case]...test...undermined" (quoting standard)
								if (termIdx > caseIdxInWindow) {
									const between = windowText.substring(caseIdxInWindow + ref.length, termIdx);
									if (/\b(test|standard|set forth|requirements|benchmark|two-prong|prong|deficient|performance|prejudice|inquiry|analysis|framework|holding)\b/i.test(between)) {
										;
										searchFrom = idx + ref.length;
										continue;
									}
									// For mild terms appearing AFTER case name, require the CASE to be the object of criticism
									// "Miranda v. Arizona has been questioned" = real negative treatment
									// "Miranda v. Arizona ... a suspect is questioned" = ordinary usage
									const mildTerms = ['questioned', 'cast doubt', 'undermined', 'criticized', 'disapproved'];
									if (mildTerms.includes(matchedTerm.toLowerCase())) {
										const hasDirectCriticism = /\b(has been|was|were|been)\s+(questioned|criticized|undermined|disapproved|cast doubt)/i.test(between)
											|| /\b(questioned|criticized|undermined)\s+by\b/i.test(between);
										if (!hasDirectCriticism) {
											;
											searchFrom = idx + ref.length;
											continue;
										}
									}
								}

								const snippet = fullText.substring(windowStart, windowEnd).trim();
								negativeTreatments.push({
									type,
									citing_case: r.caseName || r.case_name || '',
									citing_citation: r.citation?.[0] || '',
									court: r.court || '',
									date: r.dateFiled || '',
									snippet: snippet.substring(0, 300),
									url: r.absolute_url ? `https://www.courtlistener.com${r.absolute_url}` : ''
								});
								verified = true;
								;
							}
							searchFrom = idx + ref.length;
						}
						if (verified) break;
					}
				} catch { /* skip opinion fetch errors */ }
			}
		} catch { /* skip search errors */ }
	});

	// Positive: recent citers
	const posSearchPromise = (async () => {
		try {
			const res = await fetch(
				`https://www.courtlistener.com/api/rest/v4/search/?q=cites%3A(${clusterId})&type=o&order_by=dateFiled+desc&page_size=10`,
				{ headers: authHeader }
			);
			if (!res.ok) return;
			const data = await res.json() as any;
			for (const r of (data.results || []).slice(0, 5)) {
				positiveTreatments.push({
					type: 'cited',
					citing_case: r.caseName || r.case_name || '',
					citing_citation: r.citation?.[0] || '',
					court: r.court || '',
					date: r.dateFiled || '',
					url: r.absolute_url ? `https://www.courtlistener.com${r.absolute_url}` : ''
				});
			}
		} catch { /* skip */ }
	})();

	await Promise.all([...negSearchPromises, posSearchPromise]);

	// Deduplicate negative treatments (same citing case, keep worst type)
	const seenCases = new Set<string>();
	const deduped = negativeTreatments.filter(t => {
		const key = `${t.citing_case}-${t.type}`;
		if (seenCases.has(key)) return false;
		seenCases.add(key);
		return true;
	});
	negativeTreatments.length = 0;
	negativeTreatments.push(...deduped);

	// Step 4: Determine signal
	// 👎 = overruled, reversed, or superseded
	// 👌 = questioned, criticized, distinguished, limited — caution
	// 👍 = good law, actively cited, no negatives
	// 🪶 = best law — heavily cited, strong authority, no negatives

	const severeNeg = negativeTreatments.filter(t => ['overruled', 'reversed', 'superseded'].includes(t.type));
	const mildNeg = negativeTreatments.filter(t => ['distinguished', 'questioned', 'criticized', 'limited'].includes(t.type));

	let signal: ShepardizeResult['signal'];
	let signalLabel: string;
	let signalColor: string;

	if (severeNeg.length > 0) {
		signal = '🔴'; signalLabel = 'Negative — Overruled/Reversed'; signalColor = 'red';
	} else if (mildNeg.length >= 3) {
		signal = '🟡'; signalLabel = 'Caution — Multiple Negative Treatments'; signalColor = 'yellow';
	} else if (mildNeg.length > 0 && positiveTreatments.length <= mildNeg.length) {
		signal = '🟡'; signalLabel = 'Caution — Questioned/Distinguished'; signalColor = 'yellow';
	} else if (totalCiting >= 20 && negativeTreatments.length === 0 && positiveTreatments.length >= 3) {
		signal = '🟢'; signalLabel = 'Strongly Positive — Widely Followed'; signalColor = 'green';
	} else if (negativeTreatments.length === 0 && totalCiting >= 5) {
		signal = '🟢'; signalLabel = 'Strongly Positive — Good Law'; signalColor = 'green';
	} else if (negativeTreatments.length === 0) {
		signal = '🔵'; signalLabel = 'Positive — Good Law'; signalColor = 'blue';
	} else {
		signal = '🟡'; signalLabel = 'Caution — Mixed Treatment'; signalColor = 'yellow';
	}

	// Step 5: Build summary
	const summaryParts: string[] = [];
	summaryParts.push(`${signal} **${caseName}** (${citation})`);
	summaryParts.push(`Cited by ${totalCiting} case(s). Precedential status: ${precedentialStatus}.`);

	if (severeNeg.length > 0) {
		summaryParts.push(`⚠️ **NEGATIVE TREATMENT**: ${severeNeg.map(t => `${t.type} by ${t.citing_case} (${t.date})`).join('; ')}`);
	}
	if (mildNeg.length > 0) {
		summaryParts.push(`⚡ Caution: ${mildNeg.map(t => `${t.type} by ${t.citing_case}`).join('; ')}`);
	}
	if (positiveTreatments.length > 0) {
		summaryParts.push(`✅ Recently cited by: ${positiveTreatments.slice(0, 5).map(t => `${t.citing_case} (${t.date})`).join('; ')}`);
	}

	return {
		citation,
		case_name: caseName,
		cluster_id: clusterId,
		cluster_url: clusterUrl,
		date_filed: dateFiled,
		court,
		signal,
		signal_label: signalLabel,
		signal_color: signalColor,
		total_citing: totalCiting,
		negative_treatments: negativeTreatments,
		positive_treatments: positiveTreatments.slice(0, 10),
		summary: summaryParts.join('\n'),
		precedential_status: precedentialStatus
	};
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

			// --- STEP 2a-2: Deadline Timeline Engine — cascade + backward timeline + reminders ---

			// detectTriggerEventFromEmail: Pure regex detection of filing events from email text
			function detectTriggerEventFromEmail(
				subject: string, bodySnippet: string
			): { triggerEvent: string; triggerLabel: string; serviceType: 'electronic' | 'mail' } | null {
				const text = `${subject} ${bodySnippet}`.substring(0, 2000);

				// Detect service type
				const serviceType: 'electronic' | 'mail' = /\b(by\s+mail|mailed|certified\s+mail|first[\s-]class|postal)\b/i.test(text) ? 'mail' : 'electronic';

				// Filing event patterns — order matters (more specific first)
				let triggerEvent = '';
				let triggerLabel = '';

				// Court filing notices (JudiciaLink / utcourts.gov) — detect what was filed
				if (/\b(motion\s+to\s+compel|compel\s+discovery)\b/i.test(text)) { triggerEvent = 'motion_filed'; triggerLabel = 'motion to compel filed'; }
				else if (/\b(motion\s+for\s+summary\s+judgment|summary\s+judgment\s+(motion\s+)?filed)\b/i.test(text)) { triggerEvent = 'summary_judgment_filed'; triggerLabel = 'summary judgment motion filed'; }
				else if (/\b(opposition|memorandum\s+in\s+opposition|response\s+to\s+motion)\s+(filed|entered)/i.test(text)) { triggerEvent = 'opposition_filed'; triggerLabel = 'opposition filed'; }
				else if (/\b(reply\s+memo|reply\s+memorandum|reply\s+in\s+support)\s+(filed|entered)/i.test(text)) { triggerEvent = 'reply_filed'; triggerLabel = 'reply memorandum filed'; }
				else if (/\b(motion\s+(?:to\s+\w+\s+)?(?:filed|entered))|(?:filed|entered).*\bmotion\b/i.test(text)) { triggerEvent = 'motion_filed'; triggerLabel = 'motion filed'; }
				else if (/\b(complaint|petition)\s+(filed|served|entered)/i.test(text)) { triggerEvent = 'service_of_complaint'; triggerLabel = 'complaint served'; }
				else if (/\b(answer|responsive\s+pleading)\s+(filed|entered)/i.test(text)) { triggerEvent = 'first_answer_filed'; triggerLabel = 'answer filed'; }
				else if (/\b(notice\s+of\s+appeal)\s+(filed|entered)/i.test(text)) { triggerEvent = 'notice_of_appeal_filed'; triggerLabel = 'notice of appeal filed'; }
				else if (/\b(judgment|order)\s+(entered|signed|filed)/i.test(text)) { triggerEvent = 'judgment_entered'; triggerLabel = 'judgment entered'; }
				else if (/\b(interrogator\w*)\s+(served|filed|propounded)/i.test(text)) { triggerEvent = 'interrogatories_served'; triggerLabel = 'interrogatories served'; }
				else if (/\b(request\s+for\s+production|document\s+request|rfp)\s+(served|filed|propounded)/i.test(text)) { triggerEvent = 'production_request_served'; triggerLabel = 'request for production served'; }
				else if (/\b(request\s+for\s+admission|admission)\s+(served|filed|propounded)/i.test(text)) { triggerEvent = 'admissions_served'; triggerLabel = 'requests for admission served'; }
				else if (/\b(proposed\s+order)\s+(served|filed|submitted)/i.test(text)) { triggerEvent = 'proposed_order_served'; triggerLabel = 'proposed order served'; }
				else if (/\b(appellant\s+brief|opening\s+brief)\s+(filed|entered)/i.test(text)) { triggerEvent = 'appellant_brief_filed'; triggerLabel = 'appellant brief filed'; }
				else if (/\b(appellee\s+brief|response\s+brief|answering\s+brief)\s+(filed|entered)/i.test(text)) { triggerEvent = 'appellee_brief_filed'; triggerLabel = 'appellee brief filed'; }
				else if (/\b(plea)\s+(entered|accepted)/i.test(text)) { triggerEvent = 'plea_entered'; triggerLabel = 'plea entered'; }
				else if (/\b(sentence|sentencing)\s+(entered|imposed|pronounced)/i.test(text)) { triggerEvent = 'sentence_entered'; triggerLabel = 'sentence entered'; }
				else if (/\b(restitution)\s+(proposed|ordered|filed)/i.test(text)) { triggerEvent = 'restitution_proposed'; triggerLabel = 'restitution proposed'; }

				if (!triggerEvent) return null;
				return { triggerEvent, triggerLabel, serviceType };
			}

			// cascadeDeadlinesFromEvent: Auto-generate downstream deadlines from a filing event
			async function cascadeDeadlinesFromEvent(
				triggerEvent: string,
				triggerDate: string,
				caseInfo: { client_name: string; case_number: string; case_type?: string },
				serviceType: 'electronic' | 'mail',
				env: Env,
				sourceCtx?: { emailId?: string; parentDeadlineId?: number }
			): Promise<{ created: number; deadlines: Array<{ name: string; date: string; rule: string; extended: boolean }> }> {
				try {
					const ruleSource = await getCaseRules(caseInfo.case_type || '', env);
					let rulesCaseType = 'civil';
					if (ruleSource === 'URCrimP') rulesCaseType = 'criminal';
					else if (ruleSource === 'URAP') rulesCaseType = 'appeal';

					const matchedRules = await env.MEMORY_DB.prepare(
						`SELECT * FROM deadline_rules WHERE case_type = ? AND trigger_event LIKE ? ORDER BY priority DESC`
					).bind(rulesCaseType, `%${triggerEvent}%`).all();

					if (!matchedRules.results?.length) return { created: 0, deadlines: [] };

					const cascadeGroup = crypto.randomUUID();
					const createdDeadlines: Array<{ name: string; date: string; rule: string; extended: boolean }> = [];
					let created = 0;

					for (const rule of matchedRules.results as any[]) {
						const computed = await computeRule6Date(
							triggerDate,
							rule.days,
							rule.direction,
							serviceType,
							serviceType === 'mail' ? (rule.mail_add_days || 0) : 0,
							env
						);

						// Dedup: same case + date + type already exists?
						const dup = await env.MEMORY_DB.prepare(
							`SELECT id FROM deadlines WHERE case_number = ? AND due_date = ? AND deadline_type = ? AND status IN ('active','pending') LIMIT 1`
						).bind(caseInfo.case_number, computed.date, rule.deadline_name).first();
						if (dup) continue;

						// Also dedup by description (some rules map to same deadline_type)
						const dupDesc = await env.MEMORY_DB.prepare(
							`SELECT id FROM deadlines WHERE case_number = ? AND due_date = ? AND description LIKE ? AND status IN ('active','pending') LIMIT 1`
						).bind(caseInfo.case_number, computed.date, `%${rule.deadline_name}%`).first();
						if (dupDesc) continue;

						// Determine reminder_days based on urgency
						let reminderDays = '7,3,1,0';
						if (rule.days >= 28) reminderDays = '14,7,3,1,0';
						if (rule.days >= 60) reminderDays = '30,14,7,3,1,0';

						await env.MEMORY_DB.prepare(
							`INSERT INTO deadlines (client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, status, source, notes, created_at, parent_deadline_id, cascade_group, trigger_event, service_type, rule_source, rule_number, reminder_days) VALUES (?, ?, ?, ?, ?, '', '', '', '', '', 'active', 'auto-cascade', ?, ?, ?, ?, ?, ?, ?, ?, ?)`
						).bind(
							caseInfo.client_name,
							caseInfo.case_number,
							rule.deadline_name,
							`${rule.deadline_name}${computed.extended ? ` (extended from ${computed.extendedFrom})` : ''}`,
							computed.date,
							`Auto-cascaded: ${triggerEvent} on ${triggerDate}. ${rule.rule_source} ${rule.rule_number}: ${rule.days}d ${rule.direction}${serviceType === 'mail' && rule.mail_add_days ? ` +${rule.mail_add_days}d mail` : ''}`,
							mtnISO(),
							sourceCtx?.parentDeadlineId || null,
							cascadeGroup,
							triggerEvent,
							serviceType,
							rule.rule_source || '',
							rule.rule_number || '',
							reminderDays
						).run();

						createdDeadlines.push({
							name: rule.deadline_name,
							date: computed.date,
							rule: `${rule.rule_source} ${rule.rule_number}`,
							extended: computed.extended
						});
						created++;
					}

					// Log cascade
					if (created > 0) {
						await env.MEMORY_DB.prepare(
							`INSERT INTO deadline_cascade_log (cascade_group, trigger_email_id, trigger_event, trigger_date, case_number, client_name, service_type, deadlines_created, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
						).bind(cascadeGroup, sourceCtx?.emailId || null, triggerEvent, triggerDate, caseInfo.case_number, caseInfo.client_name, serviceType, created, mtnISO()).run();
					}

					return { created, deadlines: createdDeadlines };
				} catch (e: any) {
					console.error('[cascade] Error:', e.message);
					return { created: 0, deadlines: [] };
				}
			}

			// buildBackwardTimeline: From an anchor date, compute all upstream deadlines
			async function buildBackwardTimeline(
				anchorDate: string,
				anchorEvent: string,
				caseInfo: { client_name: string; case_number: string; case_type?: string },
				serviceType: 'electronic' | 'mail',
				env: Env
			): Promise<Array<{ deadline_name: string; due_date: string; days_offset: number; direction: string; rule: string; extended: boolean; reason: string }>> {
				const ruleSource = await getCaseRules(caseInfo.case_type || '', env);
				let rulesCaseType = 'civil';
				if (ruleSource === 'URCrimP') rulesCaseType = 'criminal';
				else if (ruleSource === 'URAP') rulesCaseType = 'appeal';

				// Get all rules for this case type that relate to the anchor event (backward from anchor)
				const backwardRules = await env.MEMORY_DB.prepare(
					`SELECT * FROM deadline_rules WHERE case_type = ? AND trigger_event LIKE ? ORDER BY days DESC`
				).bind(rulesCaseType, `%${anchorEvent}%`).all();

				// Also get all "before" direction rules for this case type (universal backward deadlines)
				const beforeRules = await env.MEMORY_DB.prepare(
					`SELECT * FROM deadline_rules WHERE case_type = ? AND direction = 'before' ORDER BY days DESC`
				).bind(rulesCaseType).all();

				// Combine and deduplicate
				const allRules = new Map<string, any>();
				for (const r of [...(backwardRules.results || []), ...(beforeRules.results || [])] as any[]) {
					const key = `${r.trigger_event}_${r.deadline_name}`;
					if (!allRules.has(key)) allRules.set(key, r);
				}

				const timeline: Array<{ deadline_name: string; due_date: string; days_offset: number; direction: string; rule: string; extended: boolean; reason: string }> = [];

				// Add the anchor event itself
				timeline.push({
					deadline_name: `⚓ ${anchorEvent.replace(/_/g, ' ').toUpperCase()}`,
					due_date: anchorDate,
					days_offset: 0,
					direction: 'anchor',
					rule: '',
					extended: false,
					reason: 'Anchor date'
				});

				for (const rule of allRules.values()) {
					const computed = await computeRule6Date(
						anchorDate,
						rule.days,
						rule.direction,
						serviceType,
						serviceType === 'mail' ? (rule.mail_add_days || 0) : 0,
						env
					);
					timeline.push({
						deadline_name: rule.deadline_name,
						due_date: computed.date,
						days_offset: rule.days,
						direction: rule.direction,
						rule: `${rule.rule_source} ${rule.rule_number}`,
						extended: computed.extended,
						reason: computed.extended ? `Extended from ${computed.extendedFrom}: ${computed.reason}` : ''
					});
				}

				// Sort chronologically
				timeline.sort((a, b) => a.due_date.localeCompare(b.due_date));
				return timeline;
			}

			// --- checkAndSendReminders: Cron-triggered reminder engine ---
			async function checkAndSendReminders(env: any): Promise<{ sent: number; errors: number }> {
				let sent = 0, errors = 0;
				try {
					// Get active deadlines within next 8 days
					const cutoff = new Date();
					cutoff.setDate(cutoff.getDate() + 8);
					const cutoffStr = cutoff.toISOString().split('T')[0];
					const today = mtnToday();

					const deadlines = await env.MEMORY_DB.prepare(
						`SELECT id, client_name, case_number, deadline_type, description, due_date, reminder_days, hearing_time, court, courtroom, judge FROM deadlines WHERE status IN ('active','pending') AND due_date >= ? AND due_date <= ? ORDER BY due_date ASC`
					).bind(today, cutoffStr).all();

					for (const dl of (deadlines.results || []) as any[]) {
						const reminderDays = (dl.reminder_days || '7,3,1,0').split(',').map((d: string) => parseInt(d.trim())).filter((d: number) => !isNaN(d));
						const dueDate = new Date(dl.due_date + 'T12:00:00');
						const now = new Date();
						const daysUntil = Math.floor((dueDate.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));

						for (const threshold of reminderDays) {
							if (daysUntil !== threshold) continue;

							const reminderType = threshold === 0 ? 'day_of' : `${threshold}d_before`;

							// Check if already sent
							const already = await env.MEMORY_DB.prepare(
								`SELECT id FROM deadline_reminders_sent WHERE deadline_id = ? AND reminder_type = ? LIMIT 1`
							).bind(dl.id, reminderType).first();
							if (already) continue;

							// Build email
							const urgency = threshold === 0 ? '[TODAY]' : threshold === 1 ? '[TOMORROW]' : threshold <= 3 ? '[' + threshold + ' DAYS]' : '[' + threshold + ' DAYS]';
							const subject = `${urgency} ${dl.deadline_type.replace(/_/g, ' ').toUpperCase()} - ${dl.client_name}`;
							const details = [
								dl.description,
								dl.hearing_time ? `Time: ${dl.hearing_time}` : '',
								dl.court ? `Court: ${dl.court}` : '',
								dl.courtroom ? `Courtroom: ${dl.courtroom}` : '',
								dl.judge ? `Judge: ${dl.judge}` : '',
								dl.case_number ? `Case: ${dl.case_number}` : ''
							].filter(Boolean).join('<br>');

							const body = `<div style="font-family:Georgia,serif;max-width:600px">
								<h2 style="color:#8B0000;margin-bottom:8px">${urgency}</h2>
								<h3>${dl.deadline_type.replace(/_/g, ' ').toUpperCase()}</h3>
								<p><strong>Client:</strong> ${dl.client_name}</p>
								<p><strong>Due:</strong> ${dl.due_date}</p>
								<p>${details}</p>
								<hr style="border:1px solid #ddd">
								<p style="font-size:12px;color:#666">Pitcher Law PLLC — Automated Deadline Reminder</p>
							</div>`;

							try {
								const result = await sendViaGmail('esqslaw@gmail.com', subject, body);
								if (result.success) {
									await env.MEMORY_DB.prepare(
										`INSERT INTO deadline_reminders_sent (deadline_id, reminder_type, sent_at, recipient, created_at) VALUES (?, ?, ?, ?, ?)`
									).bind(dl.id, reminderType, mtnISO(), 'esqslaw@gmail.com', mtnISO()).run();
									sent++;
									console.log(`[reminder] Sent ${reminderType} for deadline ${dl.id}: ${dl.client_name} ${dl.deadline_type} due ${dl.due_date}`);
								} else {
									errors++;
								}
							} catch (e: any) {
								console.error(`[reminder] Send error for ${dl.id}:`, e.message);
								errors++;
							}
						}
					}
				} catch (e: any) {
					console.error('[reminder] checkAndSendReminders error:', e.message);
					errors++;
				}
				return { sent, errors };
			}

			// --- sendMorningBriefing: Daily 7AM MT summary email ---
			async function sendMorningBriefing(env: any): Promise<void> {
				try {
					const today = mtnToday();

					// Dedup: check if already sent today
					const already = await env.MEMORY_DB.prepare(
						`SELECT id FROM deadline_reminders_sent WHERE reminder_type = 'morning_briefing' AND sent_at LIKE ? LIMIT 1`
					).bind(`${today}%`).first();
					if (already) { console.log('[briefing] Already sent today'); return; }

					// Get today's deadlines
					const todayDL = await env.MEMORY_DB.prepare(
						`SELECT id, client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge FROM deadlines WHERE status IN ('active','pending') AND due_date = ? ORDER BY hearing_time ASC, client_name ASC`
					).bind(today).all();

					// Get this week's deadlines (next 7 days excluding today)
					const weekEnd = new Date();
					weekEnd.setDate(weekEnd.getDate() + 7);
					const weekEndStr = weekEnd.toISOString().split('T')[0];
					const weekDL = await env.MEMORY_DB.prepare(
						`SELECT id, client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge FROM deadlines WHERE status IN ('active','pending') AND due_date > ? AND due_date <= ? ORDER BY due_date ASC, client_name ASC`
					).bind(today, weekEndStr).all();

					const todayItems = (todayDL.results || []) as any[];
					const weekItems = (weekDL.results || []) as any[];

					if (todayItems.length === 0 && weekItems.length === 0) {
						console.log('[briefing] No deadlines today or this week — skipping');
						return;
					}

					// Build HTML
					const formatDL = (dl: any, showDate = false) => {
						const parts = [
							`<strong>${dl.deadline_type.replace(/_/g, ' ').toUpperCase()}</strong>`,
							`${dl.client_name}${dl.case_number ? ' (' + dl.case_number + ')' : ''}`,
							showDate ? `Due: ${dl.due_date}` : '',
							dl.hearing_time ? `Time: ${dl.hearing_time}` : '',
							dl.court ? `${dl.court}${dl.courtroom ? ', ' + dl.courtroom : ''}` : '',
							dl.judge ? `Judge: ${dl.judge}` : '',
							dl.description ? `<em>${dl.description}</em>` : ''
						].filter(Boolean);
						return `<li style="margin-bottom:10px">${parts.join(' — ')}</li>`;
					};

					let todaySection = '';
					if (todayItems.length > 0) {
						todaySection = `<h2 style="color:#8B0000;border-bottom:2px solid #8B0000;padding-bottom:4px">🔴 TODAY — ${today}</h2>
							<ul style="list-style:none;padding-left:0">${todayItems.map(d => formatDL(d)).join('')}</ul>`;
					} else {
						todaySection = `<h2 style="color:#228B22;border-bottom:2px solid #228B22;padding-bottom:4px">✅ TODAY — ${today}</h2><p>No deadlines today.</p>`;
					}

					let weekSection = '';
					if (weekItems.length > 0) {
						weekSection = `<h2 style="color:#B8860B;border-bottom:2px solid #B8860B;padding-bottom:4px">📅 THIS WEEK</h2>
							<ul style="list-style:none;padding-left:0">${weekItems.map(d => formatDL(d, true)).join('')}</ul>`;
					}

					const body = `<div style="font-family:Georgia,serif;max-width:650px">
						<h1 style="color:#333;margin-bottom:4px">☀️ Morning Briefing</h1>
						<p style="color:#666;margin-top:0">${new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}</p>
						${todaySection}
						${weekSection}
						<hr style="border:1px solid #ddd;margin-top:20px">
						<p style="font-size:12px;color:#666">Pitcher Law PLLC — ${todayItems.length} today, ${weekItems.length} this week</p>
					</div>`;

					const subject = `Morning Briefing: ${todayItems.length} today, ${weekItems.length} this week - ${today}`;
					const result = await sendViaGmail('esqslaw@gmail.com', subject, body);

					if (result.success) {
						// Record as sent (use deadline_id = 0 for briefing)
						await env.MEMORY_DB.prepare(
							`INSERT INTO deadline_reminders_sent (deadline_id, reminder_type, sent_at, recipient, created_at) VALUES (0, 'morning_briefing', ?, ?, ?)`
						).bind(mtnISO(), 'esqslaw@gmail.com', mtnISO()).run();
						console.log(`[briefing] Sent morning briefing: ${todayItems.length} today, ${weekItems.length} this week`);
					}
				} catch (e: any) {
					console.error('[briefing] sendMorningBriefing error:', e.message);
				}
			}

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
			// MOBILE PWA — Synthia on the go
			// ═══════════════════════════════════════════════════════════════
			if (path === '/app/manifest.json') {
				return new Response(JSON.stringify({
					name: 'Synthia - Pitcher Law',
					short_name: 'Synthia',
					description: 'AI Legal Assistant',
					start_url: '/app',
					scope: '/app',
					display: 'standalone',
					background_color: '#0f0f1a',
					theme_color: '#800020',
					orientation: 'portrait',
					icons: [
						{ src: '/app/logo.png', sizes: '192x192', type: 'image/png' },
						{ src: '/app/logo.png', sizes: '512x512', type: 'image/png' },
					]
				}), { headers: { 'Content-Type': 'application/manifest+json', ...corsHeaders } });
			}

			if (path === '/app/download/android') {
				try {
					const obj = await env.DOCUMENTS.get('assets/Synthia.apk');
					if (obj) return new Response(obj.body, { headers: { 'Content-Type': 'application/vnd.android.package-archive', 'Content-Disposition': 'attachment; filename="Synthia.apk"', ...corsHeaders } });
				} catch {}
				return err('APK not found', 404);
			}

			if (path === '/app/icon-192.svg' || path === '/app/icon-512.svg' || path === '/app/logo.png') {
				try {
					const obj = await env.DOCUMENTS.get('assets/logo.png');
					if (obj) return new Response(obj.body, { headers: { 'Content-Type': 'image/png', 'Cache-Control': 'public, max-age=604800', ...corsHeaders } });
				} catch {}
				const size = path.includes('512') ? 512 : 192;
				const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${size} ${size}"><rect width="${size}" height="${size}" rx="${size*0.15}" fill="#0f0f1a"/><text x="50%" y="52%" dominant-baseline="middle" text-anchor="middle" font-family="Georgia,serif" font-size="${size*0.45}" fill="#800020" font-weight="bold">S</text><text x="50%" y="78%" dominant-baseline="middle" text-anchor="middle" font-family="system-ui" font-size="${size*0.09}" fill="#666">PITCHER LAW</text></svg>`;
				return new Response(svg, { headers: { 'Content-Type': 'image/svg+xml', 'Cache-Control': 'public, max-age=604800', ...corsHeaders } });
			}

			if (path === '/app/sw.js') {
				const sw = `const CACHE_NAME='synthia-v7';const STATIC=['/app','/app/manifest.json','/app/logo.png'];
self.addEventListener('install',e=>{e.waitUntil(caches.open(CACHE_NAME).then(c=>c.addAll(STATIC)));self.skipWaiting()});
self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==CACHE_NAME).map(k=>caches.delete(k)))));self.clients.claim()});
self.addEventListener('fetch',e=>{if(e.request.method!=='GET')return;if(e.request.url.includes('/api/'))return;e.respondWith(caches.match(e.request).then(r=>r||fetch(e.request).then(res=>{if(res.ok){const c=res.clone();caches.open(CACHE_NAME).then(ca=>ca.put(e.request,c))}return res}).catch(()=>caches.match('/app'))))});`;
				return new Response(sw, { headers: { 'Content-Type': 'application/javascript', 'Service-Worker-Allowed': '/', ...corsHeaders } });
			}

			if (path === '/app' || path === '/app/') {
				const appHtml = `<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no,viewport-fit=cover">
<meta name="theme-color" content="#800020"><meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<link rel="manifest" href="/app/manifest.json"><link rel="apple-touch-icon" href="/app/logo.png">
<title>Synthia</title>
<style>
:root{--bg:#0f0f1a;--surface:#1a1a2e;--surface2:#16213e;--accent:#800020;--accent2:#a0324e;--text:#e0e0e0;--text2:#999;--border:#2a2a3e;--green:#22c55e;--radius:12px}
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{height:100%;overflow:hidden}
body{font-family:-apple-system,BlinkMacSystemFont,'SF Pro',system-ui,sans-serif;background:var(--bg);color:var(--text);display:flex;flex-direction:column;padding-top:env(safe-area-inset-top);padding-bottom:env(safe-area-inset-bottom)}

/* Login */
.login{position:fixed;inset:0;background:var(--bg);display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:100;gap:20px}
.login.hidden{display:none}
.login h1{font-family:Georgia,serif;color:var(--accent);font-size:2.5em;letter-spacing:2px}
.login p{color:var(--text2);font-size:14px}
.login input{width:280px;padding:14px 18px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);color:var(--text);font-size:16px;text-align:center;letter-spacing:4px;outline:none}
.login input:focus{border-color:var(--accent)}
.login button{width:280px;padding:14px;background:var(--accent);color:#fff;border:none;border-radius:var(--radius);font-size:16px;font-weight:600;cursor:pointer}
.login button:active{background:var(--accent2)}
.login .err{color:#ef4444;font-size:13px;min-height:20px}

/* Header */
header{background:var(--surface);padding:12px 16px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);flex-shrink:0}
header h2{font-family:Georgia,serif;font-size:18px;color:var(--accent)}
header .status{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--text2)}
header .dot{width:8px;height:8px;border-radius:50%;background:var(--green)}
.hdr-btn{background:none;border:none;color:var(--text2);font-size:20px;cursor:pointer;padding:4px 8px}

/* Quick Actions */
.quick-bar{display:flex;gap:8px;padding:10px 16px;overflow-x:auto;flex-shrink:0;-webkit-overflow-scrolling:touch;scrollbar-width:none}
.quick-bar::-webkit-scrollbar{display:none}
.qbtn{flex-shrink:0;padding:8px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:20px;color:var(--text);font-size:13px;cursor:pointer;white-space:nowrap}
.qbtn:active{background:var(--accent);border-color:var(--accent);color:#fff}

/* Messages */
.messages{flex:1;overflow-y:auto;padding:12px 16px;display:flex;flex-direction:column;gap:10px;-webkit-overflow-scrolling:touch}
.msg{max-width:88%;padding:12px 16px;border-radius:18px;font-size:15px;line-height:1.5;word-wrap:break-word;animation:fadeIn .2s}
.msg.user{align-self:flex-end;background:var(--accent);color:#fff;border-bottom-right-radius:4px}
.msg.assistant{align-self:flex-start;background:var(--surface);border:1px solid var(--border);border-bottom-left-radius:4px}
.msg.assistant code{background:var(--surface2);padding:2px 5px;border-radius:4px;font-size:13px}
.msg.assistant pre{background:var(--surface2);padding:10px;border-radius:8px;overflow-x:auto;margin:8px 0;font-size:13px}
.msg.assistant ul,.msg.assistant ol{margin:6px 0 6px 20px}
.msg.assistant strong{color:#e8c547}
.msg.assistant a{color:#60a5fa}
.msg.typing{color:var(--text2);font-style:italic}
.msg-time{font-size:10px;color:var(--text2);margin-top:2px}
.msg.user .msg-time{text-align:right}
@keyframes fadeIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}

/* Input area */
.input-area{flex-shrink:0;padding:10px 12px;background:var(--surface);border-top:1px solid var(--border);display:flex;gap:8px;align-items:flex-end}
.input-area textarea{flex:1;padding:12px 16px;background:var(--bg);border:1px solid var(--border);border-radius:22px;color:var(--text);font-size:16px;resize:none;outline:none;max-height:120px;min-height:44px;line-height:1.4;font-family:inherit}
.input-area textarea:focus{border-color:var(--accent)}
.input-area textarea::placeholder{color:var(--text2)}
.ibtn{width:44px;height:44px;border-radius:50%;border:none;display:flex;align-items:center;justify-content:center;cursor:pointer;flex-shrink:0;font-size:20px}
.send-btn{background:var(--accent);color:#fff}
.send-btn:active{background:var(--accent2)}
.send-btn:disabled{background:var(--surface2);color:var(--text2)}
.mic-btn{background:var(--surface2);color:var(--text)}
.mic-btn.recording{background:#ef4444;color:#fff;animation:pulse 1s infinite}
@keyframes pulse{0%,100%{transform:scale(1)}50%{transform:scale(1.1)}}

/* Side panel */
.panel-overlay{position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:50;display:none}
.panel-overlay.open{display:block}
.side-panel{position:fixed;top:0;right:-300px;width:280px;height:100%;background:var(--surface);z-index:51;transition:right .25s;padding:20px;overflow-y:auto}
.side-panel.open{right:0}
.side-panel h3{color:var(--accent);margin-bottom:16px;font-family:Georgia,serif}
.panel-item{padding:12px;background:var(--surface2);border-radius:var(--radius);margin-bottom:8px;font-size:14px;cursor:pointer;border:1px solid var(--border)}
.panel-item:active{border-color:var(--accent)}
.panel-item .label{color:var(--text2);font-size:11px;margin-bottom:4px}

/* Loading */
.loading{display:inline-flex;gap:4px;padding:4px 0}
.loading span{width:8px;height:8px;background:var(--text2);border-radius:50%;animation:bounce .6s infinite alternate}
.loading span:nth-child(2){animation-delay:.2s}
.loading span:nth-child(3){animation-delay:.4s}
@keyframes bounce{to{transform:translateY(-8px);opacity:.3}}

/* Markdown in messages */
.msg.assistant h1,.msg.assistant h2,.msg.assistant h3{margin:8px 0 4px;color:var(--accent)}
.msg.assistant h1{font-size:18px}.msg.assistant h2{font-size:16px}.msg.assistant h3{font-size:15px}
.msg.assistant p{margin:4px 0}
.msg.assistant blockquote{border-left:3px solid var(--accent);padding-left:12px;margin:8px 0;color:var(--text2)}
.msg.assistant hr{border:none;border-top:1px solid var(--border);margin:8px 0}
.msg.assistant table{border-collapse:collapse;margin:8px 0;font-size:13px;width:100%;overflow-x:auto;display:block}
.msg.assistant th,.msg.assistant td{border:1px solid var(--border);padding:6px 10px;text-align:left}
.msg.assistant th{background:var(--surface2)}

.word-btn{margin-top:8px;padding:6px 14px;background:var(--accent);color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer;display:inline-block}
.word-btn:active{opacity:.7}
.msg-actions{display:flex;gap:6px;margin-top:6px;flex-wrap:wrap}
.msg-action-btn{padding:4px 10px;background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:4px;font-size:12px;cursor:pointer}
.msg-action-btn:active{background:var(--accent);color:#fff}

/* Toggle switch */
.toggle{position:relative;display:inline-block;width:44px;height:24px;flex-shrink:0}
.toggle input{opacity:0;width:0;height:0}
.toggle .slider{position:absolute;inset:0;background:var(--surface2);border-radius:24px;transition:.2s;cursor:pointer}
.toggle .slider:before{content:'';position:absolute;height:18px;width:18px;left:3px;bottom:3px;background:var(--text2);border-radius:50%;transition:.2s}
.toggle input:checked+.slider{background:var(--accent)}
.toggle input:checked+.slider:before{transform:translateX(20px);background:#fff}

/* Settings inputs in panel */
.side-panel select,.side-panel input[type=text]{-webkit-appearance:none}
.side-panel select:focus,.side-panel input[type=text]:focus{border-color:var(--accent)}
</style></head><body>

<!-- Login Screen -->
<div class="login" id="loginScreen">
<img src="/app/logo.png" alt="ESQs Law" style="width:140px;height:auto;margin-bottom:8px;border-radius:12px">
<h1>Synthia</h1>
<p>Pitcher Law PLLC</p>
<input type="password" id="pinInput" placeholder="PIN" inputmode="numeric" pattern="[0-9]*" autocomplete="off">
<div class="err" id="loginErr"></div>
<button onclick="doLogin()">Sign In</button>
</div>

<!-- App -->
<header>
<h2>Synthia</h2>
<div style="display:flex;align-items:center;gap:8px">
<div class="status"><div class="dot" id="statusDot"></div><span id="statusText">Online</span></div>
<button class="hdr-btn" onclick="togglePanel()" aria-label="Menu">☰</button>
</div>
</header>

<div class="quick-bar" id="quickBar">
<button class="qbtn" data-msg="What's on my calendar today?">📅 Today</button>
<button class="qbtn" data-msg="Show me upcoming deadlines">⏰ Deadlines</button>
<button class="qbtn" data-msg="Process my emails">📧 Process Emails</button>
<button class="qbtn" data-msg="Show my email queue drafts">📝 Email Queue</button>
<button class="qbtn" data-msg="Any new court alerts?">⚖️ Court Alerts</button>
<button class="qbtn" data-msg="Show active cases">📂 Cases</button>
<button class="qbtn" data-msg="What unmatched emails need review?">❓ Unmatched</button>
</div>

<div class="messages" id="messages"></div>

<div class="input-area">
<button class="ibtn mic-btn" id="micBtn" onclick="toggleMic()" aria-label="Voice input">🎤</button>
<textarea id="msgInput" rows="1" placeholder="Message Synthia..." enterkeyhint="send"></textarea>
<button class="ibtn send-btn" id="sendBtn" onclick="sendMessage()" disabled aria-label="Send">➤</button>
</div>

<!-- Side Panel -->
<div class="panel-overlay" id="panelOverlay" onclick="togglePanel()"></div>
<div class="side-panel" id="sidePanel">
<div style="text-align:center;margin-bottom:12px"><img src="/app/logo.png" alt="ESQs Law" style="width:64px;height:auto;border-radius:8px"></div>
<h3>⚡ Actions</h3>
<div class="panel-item" onclick="quickSend('Process my emails from both Outlook and Gmail')"><div class="label">EMAIL</div>Full Email Scan</div>
<div class="panel-item" onclick="quickSend('Show all hearings this week with details')"><div class="label">CALENDAR</div>This Week's Hearings</div>
<div class="panel-item" onclick="quickSend('Show me the email queue - pending drafts for review')"><div class="label">QUEUE</div>Review Email Drafts</div>
<div class="panel-item" onclick="quickSend('Show my communication timeline for today')"><div class="label">COMMS</div>Today's Communications</div>
<div class="panel-item" onclick="quickSend('Show all active deadlines sorted by urgency')"><div class="label">DEADLINES</div>Active Deadlines</div>
<div class="panel-item" onclick="quickSend('Give me a case status overview for all active cases')"><div class="label">CASES</div>Case Overview</div>
<div class="panel-item" onclick="quickSend('Search my personal OneDrive for recent files')"><div class="label">PERSONAL</div>Personal OneDrive</div>
<div class="panel-item" onclick="quickSend('Show intake submissions pending review')"><div class="label">INTAKE</div>Pending Intake</div>

<h3 style="margin-top:16px">⚙️ Settings</h3>
<div class="panel-item" style="padding:10px">
<div class="label">ACTIVE CLIENT</div>
<input type="text" id="setClient" placeholder="e.g. Avalos, Smith" style="width:100%;padding:8px 10px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:14px;margin-top:4px;outline:none" onchange="saveSettings()">
</div>
<div class="panel-item" style="padding:10px">
<div class="label">JURISDICTION</div>
<select id="setJurisdiction" style="width:100%;padding:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:14px;margin-top:4px;outline:none" onchange="saveSettings()">
<option value="utah">Utah (Default)</option>
<option value="federal">Federal</option>
<option value="idaho">Idaho</option>
<option value="wyoming">Wyoming</option>
</select>
</div>
<div class="panel-item" style="padding:10px;display:flex;justify-content:space-between;align-items:center">
<div><div class="label">RAG CONTEXT</div>Case memory + knowledge</div>
<label class="toggle"><input type="checkbox" id="setRAG" checked onchange="saveSettings()"><span class="slider"></span></label>
</div>
<div class="panel-item" style="padding:10px;display:flex;justify-content:space-between;align-items:center">
<div><div class="label">VOICE AUTO-SEND</div>Send on speech end</div>
<label class="toggle"><input type="checkbox" id="setVoiceAuto" checked onchange="saveSettings()"><span class="slider"></span></label>
</div>
<div class="panel-item" style="padding:10px;display:flex;justify-content:space-between;align-items:center">
<div><div class="label">CONTINUOUS LISTEN</div>Keep mic on after send</div>
<label class="toggle"><input type="checkbox" id="setContinuous" onchange="saveSettings()"><span class="slider"></span></label>
</div>
<div class="panel-item" style="padding:10px;display:flex;justify-content:space-between;align-items:center"><div><div class="label">VOICE RESPONSE</div>Synthia speaks replies</div><label class="toggle"><input type="checkbox" id="setTTS" onchange="saveSettings()"><span class="slider"></span></label></div>

<h3 style="margin-top:16px">👤 Session</h3>
<div class="panel-item" style="padding:10px">
<div class="label">SIGNED IN AS</div>
<div id="sessionInfo" style="font-size:13px;margin-top:4px">Loading...</div>
</div>
<div class="panel-item" onclick="newTopic()"><div class="label">SESSION</div>New Topic</div>
<div class="panel-item" onclick="doLogout()" style="border-color:#ef4444"><div class="label" style="color:#ef4444">SESSION</div>Sign Out</div>
</div>

<script>
const API='https://api.esqs-law.com';
let token=localStorage.getItem('synthia_token');
let sessionId='synthia_master';
const msgBox=document.getElementById('messages');
const input=document.getElementById('msgInput');
const sendBtn=document.getElementById('sendBtn');
const micBtn=document.getElementById('micBtn');
let isRecording=false, recognition=null;

// Settings — persisted in localStorage
let settings=JSON.parse(localStorage.getItem('synthia_settings')||'{}');
function loadSettings(){
const s=settings;
document.getElementById('setClient').value=s.activeClient||'';
document.getElementById('setJurisdiction').value=s.jurisdiction||'utah';
document.getElementById('setRAG').checked=s.withRag!==false;
document.getElementById('setVoiceAuto').checked=s.voiceAutoSend!==false;
document.getElementById('setContinuous').checked=!!s.continuousListen;
document.getElementById('setTTS').checked=!!s.ttsEnabled;
}
function saveSettings(){
settings.activeClient=document.getElementById('setClient').value.trim();
settings.jurisdiction=document.getElementById('setJurisdiction').value;
settings.withRag=document.getElementById('setRAG').checked;
settings.voiceAutoSend=document.getElementById('setVoiceAuto').checked;
settings.continuousListen=document.getElementById('setContinuous').checked;
settings.ttsEnabled=document.getElementById('setTTS').checked;
localStorage.setItem('synthia_settings',JSON.stringify(settings));
}

// Auto-resize textarea
input.addEventListener('input',()=>{
input.style.height='auto';
input.style.height=Math.min(input.scrollHeight,120)+'px';
sendBtn.disabled=!input.value.trim();
});
input.addEventListener('keydown',e=>{
if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendMessage()}
});

// Check auth on load
if(token){document.getElementById('loginScreen').classList.add('hidden');loadHistory();loadSettings();loadSessionInfo()}
else{document.getElementById('pinInput').focus()}
async function loadSessionInfo(){
try{
const r=await fetch(API+'/api/auth/status',{headers:{'Authorization':'Bearer '+token}});
const d=await r.json();
if(d.user){document.getElementById('sessionInfo').innerHTML='<strong>'+d.user.name+'</strong><br>Role: '+(d.user.role||'admin')+'<br>Session: Active'}
else{document.getElementById('sessionInfo').textContent='Unknown'}
}catch(e){document.getElementById('sessionInfo').textContent='Error loading'}
}

document.getElementById('pinInput').addEventListener('keydown',e=>{if(e.key==='Enter')doLogin()});

async function doLogin(){
const pin=document.getElementById('pinInput').value;
if(!pin){document.getElementById('loginErr').textContent='Enter your PIN';return}
try{
const r=await fetch(API+'/api/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pin})});
const d=await r.json();
if(d.success&&d.token){
token=d.token;localStorage.setItem('synthia_token',token);localStorage.setItem('synthia_user',d.user?.name||'User');
document.getElementById('loginScreen').classList.add('hidden');loadHistory();loadSettings();loadSessionInfo();
}else{document.getElementById('loginErr').textContent=d.error||'Invalid PIN'}
}catch(e){document.getElementById('loginErr').textContent='Connection error'}
}

function doLogout(){
token=null;localStorage.removeItem('synthia_token');localStorage.removeItem('synthia_user');
document.getElementById('loginScreen').classList.remove('hidden');
document.getElementById('pinInput').value='';
msgBox.innerHTML='';togglePanel();
}

// Chat
async function sendMessage(override){
const text=override||input.value.trim();
if(!text)return;
if(!override){input.value='';input.style.height='auto';sendBtn.disabled=true}
addMsg(text,'user');
const typingEl=addMsg('<div class="loading"><span></span><span></span><span></span></div>','assistant',true);
try{
const r=await fetch(API+'/api/bridges/message',{
method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},
body:JSON.stringify({message:text,sessionId,mode:'openai',with_rag:settings.withRag!==false,jurisdiction:settings.jurisdiction||'utah',clientName:settings.activeClient||'',dashboardState:{activeClient:settings.activeClient||''}})
});
const d=await r.json();
if(d.error==='Unauthorized'){doLogout();return}
typingEl.remove();
const reply=d.consensus||d.response||d.answer||d.error||'No response';
const formatted=formatMd(reply);
const msgEl=addMsg(formatted,'assistant');
speakReply(reply);
if(isDocContent(reply)){
var docTitle=getDocTitle(reply);
var wordBtn=document.createElement('button');
wordBtn.textContent='\ud83d\udcc4 Open in Word';
wordBtn.style.cssText='margin-top:8px;padding:6px 14px;background:var(--accent);color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer;display:block';
wordBtn.onclick=function(){exportToWord(formatted,docTitle)};
msgEl.appendChild(wordBtn);
}
}catch(e){typingEl.remove();addMsg('⚠️ Connection error — check your signal','assistant')}
}

function addMsg(html,role,raw){
const div=document.createElement('div');
div.className='msg '+role;
if(raw)div.innerHTML=html;else if(role==='user')div.textContent=html;else div.innerHTML=html;
const time=document.createElement('div');time.className='msg-time';
time.textContent=new Date().toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit',hour12:true});
div.appendChild(time);msgBox.appendChild(div);
msgBox.scrollTop=msgBox.scrollHeight;
return div;
}

// Markdown lite
function formatMd(t){
if(!t)return'';
t=t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
// Code blocks
t=t.replace(/\`\`\`(\\w*)\\n?([\\s\\S]*?)\`\`\`/g,'<pre><code>$2</code></pre>');
t=t.replace(/\`([^\`]+)\`/g,'<code>$1</code>');
// Headers
t=t.replace(/^### (.+)$/gm,'<h3>$1</h3>');
t=t.replace(/^## (.+)$/gm,'<h2>$1</h2>');
t=t.replace(/^# (.+)$/gm,'<h1>$1</h1>');
// Bold, italic
t=t.replace(/\\*\\*(.+?)\\*\\*/g,'<strong>$1</strong>');
t=t.replace(/\\*(.+?)\\*/g,'<em>$1</em>');
// Lists
t=t.replace(/^[\\-\\*] (.+)$/gm,'<li>$1</li>');
t=t.replace(/^(\\d+)\\. (.+)$/gm,'<li>$2</li>');
// Links
t=t.replace(/\\[([^\\]]+)\\]\\(([^)]+)\\)/g,'<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
// Blockquote
t=t.replace(/^&gt; (.+)$/gm,'<blockquote>$1</blockquote>');
// Horizontal rule
t=t.replace(/^---$/gm,'<hr>');
// Tables
t=t.replace(/^\\|(.+)\\|$/gm,function(m,content){
const cells=content.split('|').map(c=>c.trim());
if(cells.every(c=>/^[\\-:]+$/.test(c)))return'';
const tag=cells.some(c=>/^[\\-:]+$/.test(c))?'td':'td';
return'<tr>'+cells.map(c=>'<'+tag+'>'+c+'</'+tag+'>').join('')+'</tr>';
});
t=t.replace(/(<tr>.*<\\/tr>\\n?)+/g,'<table>$&</table>');
// Paragraphs
t=t.replace(/\\n\\n/g,'</p><p>');
t=t.replace(/\\n/g,'<br>');
if(!t.startsWith('<'))t='<p>'+t+'</p>';
return t;
}

// Quick actions
document.querySelectorAll('.qbtn').forEach(b=>{
b.addEventListener('click',()=>sendMessage(b.dataset.msg));
});
function quickSend(msg){togglePanel();setTimeout(()=>sendMessage(msg),200)}

// Voice input — Web Speech API
function initSpeech(){
if(!('webkitSpeechRecognition' in window)&&!('SpeechRecognition' in window)){
micBtn.style.display='none';return;
}
const SR=window.SpeechRecognition||window.webkitSpeechRecognition;
recognition=new SR();
recognition.continuous=false;recognition.interimResults=true;recognition.lang='en-US';
recognition.onresult=e=>{
let transcript='';
for(let i=e.resultIndex;i<e.results.length;i++){transcript+=e.results[i][0].transcript}
input.value=transcript;input.style.height='auto';input.style.height=Math.min(input.scrollHeight,120)+'px';
sendBtn.disabled=!transcript.trim();
if(e.results[e.results.length-1].isFinal){
if(settings.voiceAutoSend!==false){stopMic();if(transcript.trim())setTimeout(()=>{sendMessage();if(settings.continuousListen)setTimeout(startMic,800)},300)}
else{stopMic()}
}
};
recognition.onerror=e=>{console.log('Speech error:',e.error);stopMic()};
recognition.onend=()=>{if(!settings.continuousListen)stopMic()};
}
function toggleMic(){isRecording?stopMic():startMic()}
function startMic(){
stopTTS();
if(!recognition)initSpeech();

if(!recognition)return;
try{recognition.start();isRecording=true;micBtn.classList.add('recording');micBtn.textContent='⏹'}catch(e){console.log('Mic error:',e)}
}
function stopMic(){
try{recognition?.stop()}catch(e){}
isRecording=false;micBtn.classList.remove('recording');micBtn.textContent='🎤';
}
initSpeech();

// TTS - speak Synthia replies
function speakReply(text){
if(!settings.ttsEnabled||!window.speechSynthesis)return;
window.speechSynthesis.cancel();
var clean=text.replace(/<[^>]*>/g,'').replace(/\*\*([^*]+)\*\*/g,'$1').replace(/\*([^*]+)\*/g,'$1').replace(/#+\s*/g,'').replace(/\x60\x60\x60[^\x60]*\x60\x60\x60/g,'').replace(/\x60([^\x60]+)\x60/g,'$1').replace(/\[([^\]]+)\]\([^)]+\)/g,'$1').replace(/---/g,'').replace(/\|[^|]+\|/g,'').replace(/\n{2,}/g,'. ').replace(/\n/g,', ').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').trim();
if(!clean)return;
var chunks=[];var sentences=clean.match(/[^.!?]+[.!?]+|[^.!?]+$/g)||[clean];var buf='';
for(var i=0;i<sentences.length;i++){var s=sentences[i];if((buf+s).length>180){if(buf)chunks.push(buf.trim());buf=s}else{buf+=s}}
if(buf)chunks.push(buf.trim());
var voices=speechSynthesis.getVoices();
var fem=voices.find(function(v){return/samantha|karen|victoria|zira|female/i.test(v.name)})||voices.find(function(v){return v.lang.startsWith('en')});
chunks.forEach(function(chunk){
var u=new SpeechSynthesisUtterance(chunk);u.rate=1.05;u.pitch=1.0;u.lang='en-US';
if(fem)u.voice=fem;
speechSynthesis.speak(u);
});
}
function stopTTS(){if(window.speechSynthesis)window.speechSynthesis.cancel()}
if(window.speechSynthesis&&speechSynthesis.onvoiceschanged!==undefined){speechSynthesis.onvoiceschanged=function(){}}

// Document detection + Word export
function isDocContent(text){
var plain=text.replace(/<[^>]*>/g,'');
return /^(IN THE|BEFORE THE|COMES NOW|MOTION|PETITION|ORDER|AFFIDAVIT|STIPULATION|NOTICE OF|MEMORANDUM|DECLARATION)/im.test(plain)||
(/\b(plaintiff|defendant|petitioner|respondent|appellant|appellee)\b/i.test(plain)&&/\b(court|case\s*no|hereby|wherefore|respectfully)\b/i.test(plain))||
(plain.match(/^\d+\.\s/gm)||[]).length>=3;
}
function getDocTitle(text){
var plain=text.replace(/<[^>]*>/g,'');
var m=plain.match(/(MOTION[^\n]{0,60}|PETITION[^\n]{0,60}|ORDER[^\n]{0,60}|AFFIDAVIT[^\n]{0,60}|STIPULATION[^\n]{0,60}|NOTICE OF[^\n]{0,60}|MEMORANDUM[^\n]{0,60}|DECLARATION[^\n]{0,60}|RESPONSE[^\n]{0,60}|REPLY[^\n]{0,60}|BRIEF[^\n]{0,60}|OBJECTION[^\n]{0,60})/im);
return m?m[0].trim().substring(0,80):'Legal Document';
}
function exportToWord(rawText,title){
var clean=rawText.replace(/<br\s*\/?>/gi,'\n').replace(/<\/p>/gi,'\n\n').replace(/<\/h[123]>/gi,'\n\n').replace(/<\/li>/gi,'\n').replace(/<hr\s*\/?>/gi,'\n---\n').replace(/<[^>]*>/g,'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&quot;/g,'"').replace(/&#39;/g,"'");
var header='<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns="http://www.w3.org/TR/REC-html40"><head><meta charset="utf-8"><style>body{font-family:"Times New Roman",serif;font-size:12pt;line-height:2;margin:1in}h1,h2,h3{font-family:"Times New Roman",serif;text-align:center;font-weight:bold}p{text-align:justify;text-indent:0.5in;margin:0 0 6pt 0}.caption{text-align:center;font-weight:bold;font-size:14pt;margin-bottom:24pt;text-transform:uppercase}.court-header{text-align:center;font-weight:bold;font-size:12pt;margin-bottom:12pt}</style></head><body>';
var captionHtml='<div class="caption">'+title.toUpperCase()+'</div>';
var paras=clean.split(/\n+/);var bodyHtml='';
for(var i=0;i<paras.length;i++){var line=paras[i].trim();if(!line)continue;
if(/^#+\s/.test(line)){bodyHtml+='<h2>'+line.replace(/^#+\s*/,'')+'</h2>'}
else if(/^\d+\.\s/.test(line)){bodyHtml+='<p style="text-indent:0">'+line+'</p>'}
else{bodyHtml+='<p>'+line+'</p>'}}
var footer='</body></html>';
var blob=new Blob([header+captionHtml+bodyHtml+footer],{type:'application/msword'});
var a=document.createElement('a');a.href=URL.createObjectURL(blob);
a.download=title.replace(/[^a-zA-Z0-9 ]/g,'').replace(/ +/g,'_').substring(0,50)+'.doc';
document.body.appendChild(a);a.click();document.body.removeChild(a);URL.revokeObjectURL(a.href);
}



// Load chat history
async function loadHistory(){
try{
const r=await fetch(API+'/api/chat/thread?limit=40',{headers:{'Authorization':'Bearer '+token}});
const d=await r.json();
if(d.messages){
d.messages.forEach(m=>{
if(m.role==='topic_marker'){
const div=document.createElement('div');div.style.cssText='text-align:center;padding:8px;color:var(--text2);font-size:12px;border-top:1px solid var(--border);margin:8px 0';
div.textContent='— '+m.content+' —';msgBox.appendChild(div);
}else if(m.role==='user'||m.role==='assistant'){
addMsg(m.role==='user'?m.content:formatMd(m.content),m.role);
}
});
}
}catch(e){console.log('History load error:',e)}
}

function newTopic(){
fetch(API+'/api/chat/topic',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},body:JSON.stringify({label:'Mobile — '+new Date().toLocaleString('en-US',{timeZone:'America/Denver'})})});
const div=document.createElement('div');div.style.cssText='text-align:center;padding:8px;color:var(--text2);font-size:12px;border-top:1px solid var(--border);margin:8px 0';
div.textContent='— New Topic —';msgBox.appendChild(div);togglePanel();
}

// Side panel
function togglePanel(){
document.getElementById('sidePanel').classList.toggle('open');
document.getElementById('panelOverlay').classList.toggle('open');
}

// Register SW
if('serviceWorker' in navigator){navigator.serviceWorker.register('/app/sw.js').catch(()=>{})}

// Focus input on load
setTimeout(()=>{if(token)input.focus()},300);

// Force ALL links to open in real browser (PWA standalone traps them)
document.addEventListener('click',function(e){
var a=e.target;
while(a&&a.tagName!=='A')a=a.parentElement;
if(!a||!a.href)return;
var href=a.href;
if(href.indexOf('#')===0||href.indexOf('javascript:')===0)return;
if(href.indexOf('api.esqs-law.com/app')>-1)return;
e.preventDefault();
e.stopPropagation();
var isAndroid=/android/i.test(navigator.userAgent);
var isStandalone=window.matchMedia('(display-mode: standalone)').matches||window.navigator.standalone;
if(isStandalone&&isAndroid){
try{var u=new URL(href);location.href='intent://'+u.host+u.pathname+u.search+u.hash+'#Intent;scheme='+u.protocol.replace(':','')+ ';action=android.intent.action.VIEW;end'}catch(err){location.href=href}
}else{
var w=window.open(href,'_blank');
if(!w){location.href=href}
}
},true);
// Prevent zoom on double tap (iOS)
let lastTouchEnd=0;
document.addEventListener('touchend',e=>{const now=Date.now();if(now-lastTouchEnd<=300)e.preventDefault();lastTouchEnd=now},false);
</script>
</body></html>`;
				return new Response(appHtml, { headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', ...corsHeaders } });
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

			// Dashboard timecard summary (today/week/unbilled stats + entries)
			if (path === '/api/timecard/summary' && request.method === 'GET') {
				try {
					const today = mtnToday();
					// Week start (Monday)
					const d = new Date(today + 'T12:00:00Z');
					const day = d.getUTCDay();
					const diff = day === 0 ? 6 : day - 1;
					d.setUTCDate(d.getUTCDate() - diff);
					const weekStart = d.toISOString().split('T')[0];

					const [todayRes, weekRes, unbilledRes, entriesRes] = await Promise.all([
						env.MEMORY_DB.prepare(`SELECT COALESCE(SUM(hours), 0) as total FROM timecards WHERE date = ?`).bind(today).first(),
						env.MEMORY_DB.prepare(`SELECT COALESCE(SUM(hours), 0) as total FROM timecards WHERE date >= ?`).bind(weekStart).first(),
						env.MEMORY_DB.prepare(`SELECT COALESCE(SUM(hours), 0) as total, COUNT(*) as cnt FROM timecards WHERE billed = 0 OR billed IS NULL`).first(),
						env.MEMORY_DB.prepare(`SELECT client, case_number, category, description, hours, date, source FROM timecards WHERE date >= ? ORDER BY created_at DESC LIMIT 20`).bind(weekStart).all(),
					]);

					const todaysEntries = ((entriesRes as any).results || []).filter((e: any) => e.date === today);

					return json({
						success: true,
						todayHours: (todayRes as any)?.total || 0,
						weekHours: (weekRes as any)?.total || 0,
						unbilledHours: (unbilledRes as any)?.total || 0,
						unbilledCount: (unbilledRes as any)?.cnt || 0,
						todaysEntries,
						entries: (entriesRes as any).results || [],
					});
				} catch (e: any) {
					return json({ success: true, todayHours: 0, weekHours: 0, unbilledHours: 0, unbilledCount: 0, todaysEntries: [], entries: [], error: e.message });
				}
			}

			// Dashboard time entry (writes to MEMORY_DB timecards)
			if (path === '/api/timecard/add' && request.method === 'POST') {
				const body = await request.json() as any;
				const { client, case_number, case_type, category, description, date, hours, court, notes, billed, source } = body;
				if (!description || !category || !date || !hours) return err('Missing required fields (description, category, date, hours)', 400);
				const now = new Date().toISOString();
				const id = `tc-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
				const billedVal = billed === 'non-billable' ? 0 : 1;
				try {
					await env.MEMORY_DB.prepare(
						`INSERT INTO timecards (id, client, case_number, case_type, description, category, date, hours, billed_hours, court, source, billed, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(id, client || '', case_number || '', case_type || '', description, category, date, hours, billedVal ? hours : 0, court || '', source || 'dashboard', billedVal, notes || '', now).run();
					return json({ success: true, id });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// Complete a deadline → mark completed + create PENDING REVIEW timecard
			if (path === '/api/timecard/complete-deadline' && request.method === 'POST') {
				const { client_name, case_number, due_date, deadline_type, court, deadline_id, hours } = await request.json() as any;
				try {
					// Mark deadline as completed
					if (deadline_id) {
						await env.MEMORY_DB.prepare(`UPDATE deadlines SET status = 'completed' WHERE id = ?`).bind(deadline_id).run();
					} else if (client_name && due_date && deadline_type) {
						await env.MEMORY_DB.prepare(`UPDATE deadlines SET status = 'completed' WHERE client_name = ? AND due_date = ? AND deadline_type = ? AND status IN ('active', 'pending')`).bind(client_name, due_date, deadline_type).run();
					}

					// Auto-map category from deadline type
					const dt = (deadline_type || '').toLowerCase();
					let category = 'Appeared - Hearing';
					if (dt.includes('pretrial')) category = 'Appeared - Pretrial Conference';
					else if (dt.includes('arraign')) category = 'Appeared - Arraignment';
					else if (dt.includes('preliminary')) category = 'Appeared - Preliminary Hearing';
					else if (dt.includes('plea')) category = 'Appeared - Change of Plea';
					else if (dt.includes('evidentiary')) category = 'Appeared - Evidentiary Hearing';
					else if (dt.includes('motion')) category = 'Appeared - Motion Hearing';
					else if (dt.includes('review')) category = 'Appeared - Review Hearing';
					else if (dt.includes('protective')) category = 'Appeared - Protective Order Hearing';
					else if (dt.includes('sentenc')) category = 'Appeared - Sentencing';
					else if (dt.includes('trial')) category = 'Appeared - Trial';
					else if (dt.includes('discovery')) category = 'Case Prep - Discovery Review';

					// Create timecard entry (goes straight into timesheet for review at submission)
					const tcId = `tc-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
					const now = new Date().toISOString();
					const entryHours = hours || 1.0;
					const desc = `${client_name} - ${deadline_type}${due_date ? ' ' + due_date : ''}`;

					await env.MEMORY_DB.prepare(
						`INSERT INTO timecards (id, client, case_number, case_type, description, category, date, hours, billed_hours, court, source, billed, notes, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(tcId, client_name || '', case_number || '', '', desc, category, due_date || mtnToday(), entryHours, entryHours, court || '', 'deadline-complete', 1, '', 'approved', now).run();

					return json({ success: true, timecard_id: tcId, hours: entryHours, category });
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
			}

			// Get pending review timecard entries
			if (path === '/api/timecard/pending-review' && request.method === 'GET') {
				try {
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT id, client, case_number, case_type, description, category, date, hours, billed_hours, court, source, notes, status, created_at FROM timecards WHERE status = 'pending_review' ORDER BY date DESC, created_at DESC`
					).all();
					return json({ success: true, entries: results });
				} catch (e: any) {
					return json({ success: true, entries: [], error: e.message });
				}
			}

			// Review a timecard entry (approve / edit+approve / reject)
			if (path === '/api/timecard/review' && request.method === 'POST') {
				const { id, action, hours, category, notes, description } = await request.json() as any;
				if (!id || !action) return err('id and action required', 400);
				try {
					if (action === 'approve') {
						const updates: string[] = ['status = ?'];
						const params: any[] = ['approved'];
						if (hours !== undefined) { updates.push('hours = ?', 'billed_hours = ?'); params.push(hours, hours); }
						if (category) { updates.push('category = ?'); params.push(category); }
						if (notes !== undefined) { updates.push('notes = ?'); params.push(notes); }
						if (description) { updates.push('description = ?'); params.push(description); }
						updates.push('updated_at = ?'); params.push(new Date().toISOString());
						params.push(id);
						await env.MEMORY_DB.prepare(`UPDATE timecards SET ${updates.join(', ')} WHERE id = ?`).bind(...params).run();
						return json({ success: true, status: 'approved' });
					} else if (action === 'reject') {
						await env.MEMORY_DB.prepare(`DELETE FROM timecards WHERE id = ? AND status = 'pending_review'`).bind(id).run();
						return json({ success: true, status: 'rejected' });
					} else {
						return err('action must be approve or reject', 400);
					}
				} catch (e: any) {
					return json({ success: false, error: e.message });
				}
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

					// Email processing pipeline status
					try {
						const lastRun = await env.MEMORY_DB.prepare(
							`SELECT MAX(created_at) as created_at, COUNT(*) as cnt FROM processed_emails`
						).first() as any;
						const unmatchedCount = await env.MEMORY_DB.prepare(
							`SELECT COUNT(*) as cnt FROM processed_emails WHERE processing_status = 'unmatched'`
						).first() as any;
						const recentDeadlines = await env.MEMORY_DB.prepare(
							`SELECT COUNT(*) as cnt FROM deadlines WHERE source = 'email-auto' AND created_at >= ?`
						).bind(new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()).first() as any;
						if (lastRun?.created_at || (unmatchedCount?.cnt || 0) > 0) {
							memoryContext += '\n## Email Processing Pipeline\n';
							if (lastRun?.created_at) memoryContext += `- Last processed: ${lastRun.created_at}\n`;
							if ((recentDeadlines?.cnt || 0) > 0) memoryContext += `- Deadlines auto-extracted (last 24h): ${recentDeadlines.cnt}\n`;
							if ((unmatchedCount?.cnt || 0) > 0) memoryContext += `- ⚠️ Unmatched emails awaiting review: ${unmatchedCount.cnt}\n`;
						}
					} catch {}

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
							memoryContext += `- **${cs.client_name}** (${cs.case_number}) — ${cs.case_type}, ${cs.court}${cs.assigned_attorney ? ' [' + cs.assigned_attorney + ']' : ''}\n`;
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
							`SELECT date, client as client_name, hours, description FROM timecards WHERE date >= ? ORDER BY date DESC LIMIT 20`
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
					if (/\b(process|scan|run)\b.*\b(email|emails|inbox)\b/i.test(message) ||
						/\b(check|scan)\s+(emails?|inbox)\s+for\s+(deadline|filing|attach)/i.test(message) ||
						/\b(file|organize)\s+(my\s+)?(email|emails|attachments)\b/i.test(message) ||
						/\b(go\s+through|sort\s+through|review|audit)\b.*\b(email|emails|inbox)\b/i.test(message) ||
						/\b(download|save|get)\b.*\b(attachment|attachments)\b/i.test(message) ||
						/\b(attachment|attachments)\b.*\b(where|filed|folder|proper|correct|supposed)\b/i.test(message) ||
						/\b(ensure|make\s+sure|verify|confirm)\b.*\b(attachment|file|document)\b/i.test(message)) {
						emailAction = 'pipeline';
					}
				}

				// Rule 6 engine + Timeline engine functions hoisted to fetch handler scope (see above)

				// --- STEP 2b: Action detection — handle calendar/deadline commands via regex parsing ---
				// Fast path: parse action commands without external API calls
				// BUT skip if this is clearly an email request — let AI handle those
				const isEmailRequest = /\b(send|email|write|draft|reply|respond|forward)\b.*\b(email|message|him|her|them|client|counsel|court)\b/i.test(message) || /\b(email|message)\b.*\b(to|about|regarding|cancel|reschedule|inform)\b/i.test(message);
				if (!isEmailRequest) {
					const actionKeywords = /\b(add|create|schedule|move|reschedule|change|update|edit|delete|remove|cancel|complete|mark done|refresh calendar|sync calendar|compute|calculate|what is the deadline|due date|file by|build timeline|cascade|set remind|zoom|set up zoom|create zoom)/i;
					const contextKeywords = /\b(hearing|deadline|event|appointment|court date|calendar|meeting|sentencing|pretrial|arraignment|conference|review|motion|plea|answer|opposition|reply|brief|appeal|disclosure|interrogator|production|admission|summary judgment|new trial|certiorari|docketing|timeline|remind|trial date|zoom|zoom meeting|client meeting)\b/i;
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
							else if (/\b(build|generate|create|show)\s*(a\s+|the\s+)?(full\s+|complete\s+)?timeline\b/i.test(message) || /\btimeline\s+(for|from)\b/i.test(message)) actionType = 'build_timeline';
							else if (/\bcascade\s+(deadlines?|from)\b/i.test(message) || /\b(auto[- ]?generate|generate\s+all)\s+deadlines?\b/i.test(message)) actionType = 'cascade_deadlines';
							else if (/\bset\s+remind/i.test(message) || /\breminder\s*(days?|pref|setting)/i.test(message) || /\bremind\s+me\s+(\d+)/i.test(message)) actionType = 'set_reminders';
							else if (/\b(zoom|set\s*up\s*zoom|create\s*zoom|schedule\s*zoom)\b/i.test(message) && /\b(for|with|meeting|link|call)\b/i.test(message)) actionType = 'zoom_meeting';
							else if (/\b(create|schedule|add)\s+(a\s+)?(client\s+)?meeting\b/i.test(message) && !isCourtOrIntakeEvent(message)) actionType = 'create_calendar_event';

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

							// --- Shared Zoom/Calendar chat helpers ---
							function chatResolveClient(existing: string, msg: string): string {
								if (existing) return existing;
								const m = msg.match(/(?:for|with)\s+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+)*)/i);
								return m ? m[1] : '';
							}
							function chatParseStartTime(msg: string, dateStr: string): string {
								const tm = msg.match(/(?:at|@)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/i);
								if (dateStr && tm) {
									let hr = parseInt(tm[1]);
									const min = tm[2] || '00';
									const ampm = (tm[3] || '').toLowerCase();
									if (ampm === 'pm' && hr < 12) hr += 12;
									if (ampm === 'am' && hr === 12) hr = 0;
									return `${dateStr}T${hr.toString().padStart(2, '0')}:${min}:00`;
								}
								if (dateStr) return `${dateStr}T14:00:00`;
								const tmrw = new Date(); tmrw.setDate(tmrw.getDate() + 1);
								return `${tmrw.toISOString().split('T')[0]}T14:00:00`;
							}
							function chatEndTime(st: string, mins: number = 30): string {
								const d = new Date(st); d.setMinutes(d.getMinutes() + mins);
								return d.toISOString().replace(/\.\d{3}Z$/, '');
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
							// --- Build Timeline command ---
							else if (actionType === 'build_timeline') {
								// Extract anchor event (trial, hearing, etc.) and date
								const anchorEventMatch = message.match(/\b(trial|hearing|sentencing|arraignment|pretrial|conference|motion|discovery cutoff|expert disclosure)\s+(?:date\s+)?(?:on\s+|of\s+|is\s+)?/i);
								const anchorEvent = anchorEventMatch ? anchorEventMatch[1].toLowerCase().replace(/\s+/g, '_') : 'trial';

								// Resolve client
								let timelineClient = clientName;
								if (!timelineClient) {
									// Try to extract from message
									const forMatch = message.match(/(?:for|of|re:?)\s+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+)*)/);
									if (forMatch) timelineClient = forMatch[1];
								}

								if (!dueDate) {
									actionResult = `❌ Please specify a date. Example: "build timeline for BOYACK from trial date March 15"`;
								} else {
									// Determine case type from DB
									let caseType = 'civil';
									if (timelineClient) {
										const caseRow = await env.MEMORY_DB.prepare('SELECT case_type, case_number FROM party_cache WHERE UPPER(client_name) LIKE ? LIMIT 1').bind(`%${timelineClient.toUpperCase()}%`).first() as any;
										if (caseRow?.case_type) caseType = caseRow.case_type;
									}

									const timeline = await buildBackwardTimeline(
										dueDate,
										anchorEvent,
										{ client_name: timelineClient || 'Unknown', case_number: '', case_type: caseType },
										'electronic',
										env
									);

									if (!timeline.length) {
										actionResult = `⚠️ No timeline rules found for "${anchorEvent}" in ${caseType} cases.`;
									} else {
										const lines = timeline.map(t => {
											const marker = t.direction === 'anchor' ? '⚓' : (t.extended ? '⚠️' : '📅');
											const ext = t.reason ? ` *(${t.reason})*` : '';
											return `${marker} **${t.due_date}** — ${t.deadline_name}${t.rule ? ` [${t.rule}]` : ''}${ext}`;
										});
										actionResult = `📋 **Timeline for ${timelineClient || 'case'}** (${anchorEvent.replace(/_/g, ' ')} on ${dueDate}):\n\n${lines.join('\n')}`;
									}
								}
							}
							// --- Cascade Deadlines command ---
							else if (actionType === 'cascade_deadlines') {
								// Parse: "cascade deadlines for BOYACK from motion filed on March 10"
								const triggerMatch = message.match(/\b(motion|complaint|answer|notice of appeal|interrogatories?|rfu|rfp|admissions?|proposed order|appellant brief|appellee brief|plea|sentence|judgment|restitution)\s*(?:filed|served|entered|proposed)?\b/i);
								const triggerEvent = triggerMatch ? triggerMatch[1].toLowerCase().replace(/\s+/g, '_') + '_filed' : null;

								let cascadeClient = clientName;
								if (!cascadeClient) {
									const forMatch = message.match(/(?:for|of|re:?)\s+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+)*)/);
									if (forMatch) cascadeClient = forMatch[1];
								}

								if (!triggerEvent) {
									actionResult = `❌ Please specify the trigger event. Example: "cascade deadlines for BOYACK from motion filed on March 10"`;
								} else if (!dueDate) {
									actionResult = `❌ Please specify the trigger date. Example: "cascade from motion filed on March 10"`;
								} else if (!cascadeClient) {
									actionResult = `❌ Please specify the client. Example: "cascade deadlines for BOYACK from motion filed on March 10"`;
								} else {
									// Resolve case info
									const caseRow = await env.MEMORY_DB.prepare('SELECT case_number, case_type FROM party_cache WHERE UPPER(client_name) LIKE ? LIMIT 1').bind(`%${cascadeClient.toUpperCase()}%`).first() as any;
									const caseInfo = {
										client_name: cascadeClient,
										case_number: caseRow?.case_number || '',
										case_type: caseRow?.case_type || 'civil'
									};

									const cascade = await cascadeDeadlinesFromEvent(triggerEvent, dueDate, caseInfo, 'electronic', env, { chatCommand: true });
									if (cascade.created === 0) {
										actionResult = `⚠️ No new deadlines generated from "${triggerEvent.replace(/_/g, ' ')}" on ${dueDate} for ${cascadeClient}. (May already exist or no matching rules.)`;
									} else {
										const lines = cascade.deadlines.map((d: any) => `📅 **${d.date}** — ${d.name} [${d.rule}]`);
										actionResult = `✅ **Cascaded ${cascade.created} deadlines** for ${cascadeClient} from ${triggerEvent.replace(/_/g, ' ')} on ${dueDate}:\n\n${lines.join('\n')}`;
									}
								}
							}
							// --- Set Reminders command ---
							else if (actionType === 'set_reminders') {
								// Parse: "set reminders to 14,7,3,1,0 for BOYACK" or "remind me 14,7,3,1 days before deadlines for BOYACK"
								const daysMatch = message.match(/(\d+(?:\s*,\s*\d+)+)/);
								const reminderDays = daysMatch ? daysMatch[1].replace(/\s/g, '') : null;

								let reminderClient = clientName;
								if (!reminderClient) {
									const forMatch = message.match(/(?:for|of|re:?)\s+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+)*)/);
									if (forMatch) reminderClient = forMatch[1];
								}

								if (!reminderDays) {
									actionResult = `❌ Please specify reminder days. Example: "set reminders to 14,7,3,1,0 for BOYACK"`;
								} else if (!reminderClient) {
									actionResult = `❌ Please specify the client. Example: "set reminders to 7,3,1,0 for BOYACK"`;
								} else {
									const updated = await env.MEMORY_DB.prepare(
										`UPDATE deadlines SET reminder_days = ? WHERE UPPER(client_name) LIKE ? AND status IN ('active','pending')`
									).bind(reminderDays, `%${reminderClient.toUpperCase()}%`).run();
									const count = updated.meta?.changes || 0;
									actionResult = count > 0
										? `✅ Updated ${count} deadline(s) for ${reminderClient}: reminders at ${reminderDays.split(',').map((d: string) => d === '0' ? 'day-of' : `${d}d before`).join(', ')}`
										: `⚠️ No active deadlines found for "${reminderClient}" to update.`;
								}
							}

							// --- ZOOM MEETING ---
							else if (actionType === 'zoom_meeting') {
								const zoomClient = chatResolveClient(clientName, message);
								const topic = zoomClient ? `Meeting — ${zoomClient}` : 'Pitcher Law Meeting';
								const startTime = chatParseStartTime(message, dueDate);
								const meeting = await createZoomMeeting(topic, startTime, 30);
								try {
									await createGoogleCalendarEvent({
										summary: topic, start: startTime, end: chatEndTime(startTime),
										location: meeting.join_url,
										description: `🔗 Zoom Meeting\nJoin: ${meeting.join_url}\nPassword: ${meeting.password}`,
									});
								} catch (calErr: any) { console.error('Calendar event creation failed:', calErr.message); }
								actionResult = `✅ Zoom meeting created${zoomClient ? ` for ${zoomClient}` : ''}\n📅 ${startTime.replace('T', ' at ')}\n🔗 Join: ${meeting.join_url}\n🔑 Password: ${meeting.password}\n📆 Added to Google Calendar`;
							}

							// --- CREATE CALENDAR EVENT (with auto-Zoom for non-hearing) ---
							else if (actionType === 'create_calendar_event') {
								const evtClient = chatResolveClient(clientName, message);
								const summary = evtClient ? `Meeting — ${evtClient}` : 'Client Meeting';
								const startTime = chatParseStartTime(message, dueDate);
								const endTime = chatEndTime(startTime);
								let zoomInfo = '', zoomDesc = '', zoomLoc = '';
								if (!isCourtOrIntakeEvent(summary)) {
									try {
										const zm = await createZoomMeeting(summary, startTime, 30);
										zoomLoc = zm.join_url;
										zoomDesc = `\n\n🔗 Zoom Meeting\nJoin: ${zm.join_url}\nPassword: ${zm.password}`;
										zoomInfo = `\n🔗 Zoom: ${zm.join_url}`;
									} catch (zErr: any) { console.error('Zoom for calendar event failed:', zErr.message); }
								}
								await createGoogleCalendarEvent({
									summary, start: startTime, end: endTime, location: zoomLoc,
									description: `Meeting with ${evtClient || 'client'}${zoomDesc}`,
								});
								actionResult = `✅ Calendar event created: ${summary}\n📅 ${startTime.replace('T', ' at ')}${zoomInfo}`;
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

				// --- STEP 2-FILES: Fetch file content when user asks to review/read/analyze a document ---
				const isFileReviewRequest = /\b(review|read|check|look\s+at|analyze|examine|pull\s+up|open|show\s+me|what\s+(?:does|is\s+in)|summarize|scan|go\s+(?:over|through))\b.*\b(psi|pre.?sentence|facesheet|face\s*sheet|contract|motion|plea|order|minute|docket|filing|report|document|letter|agreement|stipulation|affidavit|declaration|complaint|answer|brief|memo|notice|subpoena|warrant|bail|bond|sentencing|probation|discovery|interrogator|exhibit|transcript|file|pdf|doc)\b/i.test(message) ||
					/\b(psi|pre.?sentence|facesheet|contract|motion|plea|order|filing|report|document|agreement|stipulation|affidavit|declaration|complaint|brief|memo|transcript)\b.*\b(review|read|check|analyze|examine|scan|errors?|inaccurac|wrong|incorrect|mistake)\b/i.test(message) ||
					/\b(what(?:'s| is| are))\b.*\b(in\s+(?:the|her|his|their|our|my))\b.*\b(psi|file|folder|document|report|facesheet)\b/i.test(message);

				if (isFileReviewRequest && (activeClient || inferredClient)) {
					const fileClient = activeClient || inferredClient || '';
					try {
						console.log(`File review detected for client: ${fileClient}`);
						const token = await getGraphToken();
						if (token && env.ONEDRIVE_FOLDER_ID) {
							// Strip punctuation from name parts for matching
							const fileNameParts = fileClient.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter((p: string) => p.length > 1);
							const odSearchUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${env.ONEDRIVE_FOLDER_ID}/children?$top=200`;
							const odFolderRes = await fetch(odSearchUrl, { headers: { 'Authorization': `Bearer ${token}` } });
							const odFolderData = await odFolderRes.json() as any;
							const lcNameParts = fileNameParts.map((p: string) => p.toLowerCase());
							// Helper: find client folder with fuzzy matching
							const findClientFolder = (items: any[]) => {
								let f = items.find((f: any) => lcNameParts.every((p: string) => f.name.toLowerCase().includes(p)));
								if (!f && lcNameParts.length > 1) {
									const fl = [lcNameParts[0], lcNameParts[lcNameParts.length - 1]];
									if (fl[0] !== fl[1]) f = items.find((i: any) => fl.every((p: string) => i.name.toLowerCase().includes(p)));
								}
								if (!f && lcNameParts.length > 0) {
									const wp = lcNameParts[0];
									f = items.find((i: any) => { const fn = i.name.toLowerCase(); const idx = fn.indexOf(wp); return idx >= 0 && (idx === 0 || /[^a-z]/.test(fn[idx - 1])) && (idx + wp.length >= fn.length || /[^a-z]/.test(fn[idx + wp.length])); });
								}
								return f;
							};
							// Collect files from a drive folder + subfolders
							const collectDriveFiles = async (folderId: string, drivePrefix: string) => {
								const url = `https://graph.microsoft.com/v1.0/${drivePrefix}drive/items/${folderId}/children?$top=100&$orderby=lastModifiedDateTime desc`;
								const res = await fetch(url, { headers: { 'Authorization': `Bearer ${token}` } });
								const data = await res.json() as any;
								let files = (data.value || []).map((f: any) => ({ ...f, _drivePrefix: drivePrefix }));
								const subs = files.filter((f: any) => f.folder);
								for (const sf of subs.slice(0, 5)) {
									try {
										const sfRes = await fetch(`https://graph.microsoft.com/v1.0/${drivePrefix}drive/items/${sf.id}/children?$top=50`, { headers: { 'Authorization': `Bearer ${token}` } });
										const sfData = await sfRes.json() as any;
										files = files.concat((sfData.value || []).map((f: any) => ({ ...f, _subfolder: sf.name, _drivePrefix: drivePrefix })));
									} catch (_) {}
								}
								return files;
							};
							// Search BOTH drives and merge (Associate priority, dedup by name)
							let allClientFiles: any[] = [];
							const seenFileNames = new Set<string>();
							const assocFolder = findClientFolder(odFolderData.value || []);
							if (assocFolder) {
								const aFiles = await collectDriveFiles(assocFolder.id, 'me/');
								for (const f of aFiles) { seenFileNames.add(f.name); allClientFiles.push(f); }
							}
							try {
								const dianeOcRes = await fetch('https://graph.microsoft.com/v1.0/users/diane@dianepitcher.com/drive/items/01U5K3O7VWI7BU54HJQJBLWG76HRSFVGF7/children?$top=200', { headers: { 'Authorization': `Bearer ${token}` } });
								const dianeOcData = await dianeOcRes.json() as any;
								const dianeFolder = findClientFolder(dianeOcData.value || []);
								if (dianeFolder) {
									const dFiles = await collectDriveFiles(dianeFolder.id, 'users/diane@dianepitcher.com/');
									for (const f of dFiles) { if (!seenFileNames.has(f.name)) { seenFileNames.add(f.name); allClientFiles.push(f); } }
								}
							} catch (_) {}

							if (allClientFiles.length > 0) {
								// Match requested document type
								const msgLower = message.toLowerCase();
								const fileKeywords: Record<string, string[]> = {
									'psi': ['psi', 'pre-sentence', 'presentence', 'pre sentence', 'pre sentance', 'pre-sentance', 'investigation report'],
									'facesheet': ['facesheet', 'face sheet', 'face_sheet', 'coversheet'],
									'contract': ['contract', 'agreement', 'retainer', 'engagement'],
									'motion': ['motion'], 'plea': ['plea', 'change of plea'],
									'order': ['order'], 'minute': ['minute entry', 'minute'],
									'sentencing': ['sentencing', 'sentence'],
									'discovery': ['discovery', 'interrogator', 'request for production', 'rfp', 'rogs'],
									'transcript': ['transcript'], 'complaint': ['complaint', 'information', 'charging'],
									'brief': ['brief', 'memorandum', 'memo'],
									'affidavit': ['affidavit', 'declaration'], 'stipulation': ['stipulation'],
									'probation': ['probation', 'ap&p'], 'report': ['report'],
								};

								let matchedClientFiles: any[] = [];
								for (const [, keywords] of Object.entries(fileKeywords)) {
									if (keywords.some(kw => msgLower.includes(kw))) {
										const kwMatches = allClientFiles.filter((f: any) => {
											if (f.folder) return false;
											const fn = (f.name || '').toLowerCase();
											return keywords.some(kw => fn.includes(kw));
										});
										matchedClientFiles = matchedClientFiles.concat(kwMatches);
									}
								}

								// Fallback: general word matching
								if (matchedClientFiles.length === 0) {
									const msgWords = msgLower.match(/\b[a-z]{3,}\b/g) || [];
									const skipW = ['review','read','check','look','analyze','examine','pull','open','show','what','does','the','her','his','their','our','file','document','can','you','please','that','this'];
									const searchW = msgWords.filter(w => !skipW.includes(w));
									matchedClientFiles = allClientFiles.filter((f: any) => {
										if (f.folder) return false;
										const fn = (f.name || '').toLowerCase();
										return searchW.some(w => fn.includes(w));
									});
								}

								// Download and extract text from matched files (max 2)
								if (matchedClientFiles.length > 0) {
									let fileContentStr = `\n## 📄 Document Content Retrieved from ${fileClient}'s Case Files\n`;
									const filesToRead = matchedClientFiles.slice(0, 2);

									for (const mf of filesToRead) {
										try {
											const mfExt = (mf.name || '').split('.').pop()?.toLowerCase();
											fileContentStr += `\n### ${mf.name} (${((mf.size || 0) / 1024).toFixed(0)} KB, modified ${(mf.lastModifiedDateTime || '').substring(0, 10)})\n`;

											if ((mf.size || 0) > 5 * 1024 * 1024) {
												fileContentStr += `[File too large — ${((mf.size || 0) / 1024 / 1024).toFixed(1)} MB. View at: ${mf.webUrl || 'https://api.esqs-law.com/api/onedrive/file?id=' + mf.id}]\n`;
												continue;
											}

											const mfDlRes = await fetch(`https://graph.microsoft.com/v1.0/${mf._drivePrefix || 'me/'}drive/items/${mf.id}/content`, {
												headers: { 'Authorization': `Bearer ${token}` }
											});

											if (mfExt === 'pdf') {
												const pdfBuf = await mfDlRes.arrayBuffer();
												const pdfBytes = new Uint8Array(pdfBuf);
												const pdfRaw = new TextDecoder('latin1').decode(pdfBytes);
												let pdfExtracted = '';
												const btEtBlocks = pdfRaw.match(/BT[\s\S]*?ET/g) || [];
												for (const block of btEtBlocks) {
													const tjMatches = block.match(/\(([^)]*)\)\s*Tj/g) || [];
													for (const tjm of tjMatches) { pdfExtracted += (tjm.match(/\(([^)]*)\)/)?.[1] || '') + ' '; }
													const tjArrays = block.match(/\[([^\]]*)\]\s*TJ/g) || [];
													for (const tja of tjArrays) {
														const inner = tja.match(/\[([^\]]*)\]/)?.[1] || '';
														const parts = inner.match(/\(([^)]*)\)/g) || [];
														for (const pt of parts) { pdfExtracted += (pt.match(/\(([^)]*)\)/)?.[1] || ''); }
														pdfExtracted += ' ';
													}
												}
												if (pdfExtracted.trim().length < 100) {
													const streamParts = pdfRaw.match(/stream\r?\n([\s\S]*?)\r?\nendstream/g) || [];
													for (const sp of streamParts) {
														const spContent = sp.replace(/^stream\r?\n/, '').replace(/\r?\nendstream$/, '');
														const readable = spContent.replace(/[^\x20-\x7E\r\n]/g, ' ').replace(/\s{3,}/g, ' ').trim();
														if (readable.length > 50 && !/^[0-9\s.]+$/.test(readable)) { pdfExtracted += readable.substring(0, 2000) + ' '; }
													}
												}
												const cleanPdf = pdfExtracted.replace(/\\n/g, '\n').replace(/\\r/g, '').replace(/\s{2,}/g, ' ').trim();
												if (cleanPdf.length > 20) {
													fileContentStr += cleanPdf.substring(0, 8000) + (cleanPdf.length > 8000 ? '\n[...truncated...]' : '') + '\n';
												} else {
													fileContentStr += `[PDF may be scanned/image. View at: ${mf.webUrl || 'https://api.esqs-law.com/api/onedrive/file?id=' + mf.id}]\n`;
												}
											} else if (mfExt === 'docx' || mfExt === 'doc') {
												const docBuf = await mfDlRes.arrayBuffer();
												const docBytes = new Uint8Array(docBuf);
												let docText = '';
												for (let di = 0; di < docBytes.length - 30; di++) {
													if (docBytes[di] === 0x50 && docBytes[di+1] === 0x4B && docBytes[di+2] === 0x03 && docBytes[di+3] === 0x04) {
														const dfnLen = docBytes[di+26] | (docBytes[di+27] << 8);
														const dexLen = docBytes[di+28] | (docBytes[di+29] << 8);
														const dcompMethod = docBytes[di+8] | (docBytes[di+9] << 8);
														const dcompSize = docBytes[di+18] | (docBytes[di+19] << 8) | (docBytes[di+20] << 16) | (docBytes[di+21] << 24);
														const dheaderEnd = di + 30 + dfnLen + dexLen;
														const dentryName = new TextDecoder().decode(docBytes.slice(di+30, di+30+dfnLen));
														if (dentryName === 'word/document.xml' && dcompSize > 0) {
															const dcompData = docBytes.slice(dheaderEnd, dheaderEnd + dcompSize);
															if (dcompMethod === 8) {
																try {
																	const dds = new DecompressionStream('deflate-raw');
																	const dwriter = dds.writable.getWriter();
																	dwriter.write(dcompData);
																	dwriter.close();
																	const dreader = dds.readable.getReader();
																	const dchunks: Uint8Array[] = [];
																	while (true) { const { done, value } = await dreader.read(); if (done) break; dchunks.push(value); }
																	const dtotalLen = dchunks.reduce((s, c) => s + c.length, 0);
																	const dmerged = new Uint8Array(dtotalLen);
																	let doff = 0;
																	for (const dc of dchunks) { dmerged.set(dc, doff); doff += dc.length; }
																	const dxml = new TextDecoder().decode(dmerged);
																	const dwtMatches = dxml.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
																	docText = dwtMatches.map(dm => dm.replace(/<[^>]+>/g, '')).join(' ');
																} catch (_) {}
															} else if (dcompMethod === 0) {
																const draw = docBytes.slice(dheaderEnd, dheaderEnd + dcompSize);
																const dxml2 = new TextDecoder().decode(draw);
																const dwtM2 = dxml2.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
																docText = dwtM2.map(dm2 => dm2.replace(/<[^>]+>/g, '')).join(' ');
															}
															break;
														}
													}
												}
												if (docText.length > 20) {
													fileContentStr += docText.substring(0, 8000) + (docText.length > 8000 ? '\n[...truncated...]' : '') + '\n';
												} else {
													fileContentStr += `[DOCX extraction limited. View at: ${mf.webUrl || 'https://api.esqs-law.com/api/onedrive/file?id=' + mf.id}]\n`;
												}
											} else if (mfExt === 'txt' || mfExt === 'csv') {
												const txtContent = await mfDlRes.text();
												fileContentStr += txtContent.substring(0, 8000) + (txtContent.length > 8000 ? '\n[...truncated...]' : '') + '\n';
											} else {
												fileContentStr += `[${(mfExt || '').toUpperCase()} file — view at: ${mf.webUrl || 'https://api.esqs-law.com/api/onedrive/file?id=' + mf.id}]\n`;
											}
										} catch (mfErr: any) {
											fileContentStr += `[Error reading ${mf.name}: ${mfErr.message}]\n`;
										}
									}

									if (matchedClientFiles.length > 2) {
										fileContentStr += `\n### Other matching files (${matchedClientFiles.length - 2} more):\n`;
										for (const xf of matchedClientFiles.slice(2, 8)) {
											fileContentStr += `- ${xf.name} (${((xf.size || 0) / 1024).toFixed(0)} KB) — [View](${xf.webUrl || 'https://api.esqs-law.com/api/onedrive/file?id=' + xf.id})\n`;
										}
									}
									memoryContext += fileContentStr;
									console.log(`File review: Injected content from ${filesToRead.length} file(s) for ${fileClient}`);
								} else {
									// No match — list available files
									const availFiles = allClientFiles.filter((f: any) => !f.folder).slice(0, 20);
									if (availFiles.length > 0) {
										let fileListStr = `\n## 📁 Files in ${fileClient}'s Case Folder (no exact match — listing all)\n`;
										for (const af of availFiles) {
											fileListStr += `- ${af.name} (${((af.size || 0) / 1024).toFixed(0)} KB, ${(af.lastModifiedDateTime || '').substring(0, 10)}) — [View](${af.webUrl || 'https://api.esqs-law.com/api/onedrive/file?id=' + af.id})\n`;
										}
										memoryContext += fileListStr;
									}
								}
							}
						}
					} catch (fileReviewErr: any) {
						console.warn('File review fetch error (non-fatal):', fileReviewErr.message);
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

DEPLOY MODE — MULTI-AI RAPID EXECUTION:
When the user says "DEPLOY" (or any variant: "deploy mode", "deploy this"), activate DEPLOY MODE:
- Execute tasks using ALL available AI capabilities simultaneously — parallel API calls, batch processing, concurrent operations.
- Do NOT ask for confirmation on intermediate steps — chain actions automatically.
- Use Workers AI, Google Calendar API, Graph API, Gmail API, D1 queries, R2 operations ALL AT ONCE when applicable.
- For email processing: trigger /api/email/process with source=both and auto-continuation.
- For document generation: generate, file, AND notify in one flow.
- For case prep: pull case summary, deadlines, judge intel, OC intel, file inventory, and recent emails ALL in parallel.
- DEPLOY mode means: maximum speed, maximum concurrency, minimum human-in-the-loop. If something fails, log it and keep going — don't stop the chain.
- When DEPLOY is used with a specific task (e.g., "DEPLOY hearing prep for Avalos"), execute the FULL workflow end-to-end without pausing.

STRATEGIC INTELLIGENCE:
- You have Judge Intelligence profiles in your context — tendencies, sentencing patterns, motion preferences, plea dispositions. USE THESE when discussing strategy, hearing prep, motion drafting, or plea negotiations. Reference specific patterns.
- You have Opposing Counsel Intelligence — negotiation style, litigation tendencies, strengths, weaknesses, win rates. USE THESE for tactical advantage during case strategy discussions.
- You have PREDICTIVE ANALYTICS for judges and opposing counsel based on logged outcomes. Predictions show probability, sample size (n), confidence level, and trend direction. Use these in case analysis: "Based on 11 prior rulings, Judge X denies MTDs 72% of the time..." When confidence is low (n<5), caveat your prediction. When high (n>=10), state with authority. Predictions may include PARTY ROLE BREAKDOWN (plaintiff/defendant/petitioner/respondent) — ALWAYS reference the role-specific prediction when applicable. "Judge X denies MTDs 72% overall, but only 55% when we are the plaintiff."
- You have ATTORNEY PERFORMANCE data including JWA3's own track record. Reference past outcomes, lessons learned, and patterns. When advising on strategy, check if JWA3 has faced this situation before. Be direct about past mistakes — "Last time you tried X, it didn't work because Y. Consider Z instead." Performance data includes PARTY ROLE segmentation — outcomes as plaintiff vs defendant vs petitioner vs respondent. Always consider which side we are on when assessing predictions.
- You have JUDGE RULING RATIONALE — pro/con analysis of WHY judges rule the way they do (not just statistics). For each ruling type, you see what arguments/factors lead to grants vs denials. When a prediction says "Judge X denies MTDs 72%", also check the rationale to advise what might change the outcome. Frame as: "Judge X usually denies MTDs, but has granted when [specific factors]. In our case, we could [strategy]."
- You have JUDICIAL THINKING RESOURCES — bench books, sentencing guidelines, judicial training topics, and decision-making research from Utah and Idaho. Reference these when explaining why a judge might rule a certain way, especially for sentencing and motion practice.
- You have ENRICHED Case Summaries — each case has: client contact info (phone/email/address), opposing counsel details (name/phone/email/firm), facts summary, charges, additional parties, key deadlines (discovery/dispositive/trial/SOL), judge predictions, OC predictions, reversal factors, and notes. USE ALL OF THIS. When discussing a case, reference the full facesheet data. When preparing for a hearing, cite the judge predictions AND reversal factors from the case summary. When contacting OC, use their stored phone/email. When a user asks "update the facts on [case]" or "add charges to [case]", use the PATCH /api/case-summaries/:caseNumber endpoint.
- You have Case File Inventory showing what documents exist per client. Reference this when discussing filings, evidence, or document prep.
- You have FILE CONTENT RETRIEVAL — when you're asked to review, read, or analyze a specific document (PSI, facesheet, motion, contract, etc.), the system automatically fetches the file content from OneDrive and injects it into your context. If you see a "📄 Document Content Retrieved" section in your Memory Context, that IS the file content. READ IT AND USE IT. Do NOT ask the user to upload a file that is already in your context.
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
  Use PATCH /api/case-summaries/:caseNumber with the relevant fields. Updatable: facts, charges, notes, client_email, client_phone, client_address, opposing_counsel, opposing_counsel_phone, opposing_counsel_email, opposing_counsel_firm, additional_parties, discovery_deadline, dispositive_deadline, trial_date, statute_of_limitations, case_type, court, district, judge, client_role, folder_url, status, assigned_attorney.
  assigned_attorney identifies whose client this is: 'JWA3' for ESQs/John Adams clients, 'DPL' for Diane Pitcher clients, or other attorney codes. Always set this when you know whose client it is.
  FACTS FIELD FORMAT — REQUIRED: Facts must be a brief memory-jogging narrative of the alleged events. Use client role abbreviations (D=Defendant, W=Wife, H=Husband, C=Client, P=Plaintiff). Examples:
    "D accused of breaking glass of mother's back door. D was not in town when event occurred."
    "W petitioned for div 6 Oct 2024. Both W and H have been arguing about custody for 2 years."
    "C wants to file complaint against company that sold her a blender and it exploded after only 2 uses."
    "D arrested for DUI on June 28, 2025. BAC .12, prior conviction within 10 years. Driving on E 462 W Center St, Logan."
  NOT legal elements. NOT procedure. Just what allegedly happened — enough to remember the case at a glance.
  PROACTIVELY suggest updating case summaries when you learn new info — "I see you mentioned the trial is set for March 15. Should I update the case summary with this trial date?"

EMAIL CAPABILITIES:
- You CAN and DO send real emails. You are not a draft tool. When you send an email, it goes out. NEVER say "as an AI I can't actually send emails" — you CAN and you DID. Own it.
- You can READ emails from Outlook. When a client is active, their recent emails are included in your context below.
- You can SEND emails. When the user says "send", "email them", "reply", "thank them" — send it. Routine/generic messages (thank you, acknowledgment, scheduling) are auto-approved.
- You can ARCHIVE client emails as PDFs to their OneDrive case folder under "Correspondence/".
- You CAN and DO download attachments and file them to OneDrive client folders. This is NOT hypothetical — the pipeline does it automatically.

TWO ONEDRIVES — WORK vs PERSONAL:
- WORK OneDrive (Associate@dianepitcher.com) — /api/onedrive/* — Case files, exhibits, client folders. Used by email pipeline.
- PERSONAL OneDrive (JWA3's personal Microsoft account) — /api/personal-onedrive/* — Personal files, personal documents. NOT case files.
  - /api/personal-onedrive/list?folder_id=xxx — Browse folders (omit folder_id for root)
  - /api/personal-onedrive/search?q=xxx — Search files
  - /api/personal-onedrive/file?id=xxx — View/download file
  - /api/personal-onedrive/status — Check connection
- When user says "my files", "my personal OneDrive", "my personal documents" → use /api/personal-onedrive/*
- When user says "case files", "client folder", "exhibits" → use /api/onedrive/* (work)
- If ambiguous, ask which OneDrive.
- You can PROCESS EMAILS automatically: "process my emails", "go through my emails", "download attachments", "file attachments"
  This scans both Outlook and Gmail, matches emails to active cases, DOWNLOADS attachments, FILES them to the correct OneDrive client folder, and extracts deadlines.
  Deadlines extracted from emails are auto-inserted with source='email-auto' and appear in the deadline tracker.
  Unmatched emails are flagged for manual review — user can say "file that email under [client]" to reassign.
  The pipeline also runs automatically every 2 hours via cron.
- When referring to email senders, use their resolved identity (name, role, organization) rather than just email addresses.

CRITICAL — SELF-VERIFICATION & HONESTY ABOUT PIPELINE RESULTS:
When the email pipeline runs, the system VERIFIES results from the database BEFORE you generate your response. You will see a section labeled "EMAIL PIPELINE RESULTS — VERIFIED FROM DATABASE" injected into your context. This is GROUND TRUTH queried directly from D1 tables (processed_emails, email_filed_attachments, deadlines).

YOUR ONLY JOB when pipeline results are present: REPORT THE VERIFIED DATA EXACTLY.
- Read the "EMAILS PROCESSED" list → report each email by subject and sender
- Read the "ATTACHMENTS FILED TO ONEDRIVE" list → report each file by name, size, and destination path
- Read the "DEADLINES EXTRACTED" list → report each deadline by date, type, and client
- Read the "UNMATCHED EMAILS" list → report which emails couldn't be matched to a case
- The numbers in "SUMMARY" are authoritative. Use those exact numbers.
- If the verified data says 0 attachments → say 0. If it says 5 → list all 5 by name and path.
- NEVER add items that aren't in the verified data. NEVER omit items that are.
- NEVER fabricate filenames, folder paths, or download confirmations.
- NEVER say "I cannot download/file attachments" — you CAN. The pipeline does it. If it failed, report the failure honestly.
- NEVER say "as an AI I cannot access file systems" — you have Graph API + OneDrive access. The pipeline uses it. Own your capabilities.
- If ALL emails were previously processed (dedup skipped them), report that: "All emails in this time window were already processed in a prior run."

CLIENT EMAIL QUEUE SYSTEM:
- When asked to email a CLIENT (not OC, not court — a client), use the email queue instead of sending directly.
- POST to /api/email-queue with template_id, variables, client_name, to_address → creates a draft for attorney review.
- Available templates: welcome, hearing-reminder, motion-filed, deadline-approaching, document-request, case-resolution, status-update.
- To preview a template: POST /api/email-templates/{id}/preview with variables.
- Client emails ALWAYS go to queue (status='draft') for attorney approval — never send directly to clients.
- Internal emails, OC emails, court emails → send directly as before.
- The email queue dashboard at /api/email-queue shows pending drafts. Approve via POST /api/email-queue/{id}/approve.

CLIENT CONTACT LOGGING (CRITICAL — 3 purposes):
Every client interaction must be logged via POST /api/communications. This is not optional. It serves:
1. TIMESHEETS: duration_minutes + billable flag → auto-aggregated at /api/timesheet?from=YYYY-MM-DD&to=YYYY-MM-DD
2. MALPRACTICE DEFENSE: advice_given flag, interaction_summary, follow_up tracking → exportable at /api/malpractice-log/{clientName}
3. CLIENT INTELLIGENCE: client_sentiment, personality notes → builds profile at /api/client-profiles/{clientName}

When logging a client call/meeting/email, ALWAYS include:
- duration_minutes (even an estimate — 6 min minimum for billing)
- interaction_summary (what was discussed — this is the malpractice shield)
- advice_given: true if legal advice was provided
- follow_up_required + follow_up_date if anything needs follow-up
- client_sentiment: how the client seemed (cooperative, frustrated, confused, relieved, anxious, hostile, grateful)

Client personality profiles (PUT /api/client-profiles/{name}):
- communication_style: "prefers brief updates" / "wants detailed explanations" / "calls frequently" / "email only"
- emotional_tendencies: "anxious about proceedings" / "impatient with process" / "very trusting" / "skeptical"
- decision_style: "decisive" / "needs time to think" / "defers to spouse" / "wants all options"
- key_concerns: what matters most to them personally
- risk_factors: unrealistic expectations, non-responsive, difficult personality
- Personality notes APPEND (don't overwrite) — each update adds a dated entry

PROACTIVE CONTACT LOGGING: After every client interaction you participate in (sending an email, discussing a case, taking a call note), LOG IT. Don't wait to be asked. Include the duration estimate and a brief summary.

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
							temperature: 0,
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
							model: 'grok-3', temperature: 0,
							messages: grokMsgs,
							max_tokens: 4000
						})
					});
					const d = await r.json() as any;
					return d.choices?.[0]?.message?.content || '';
				};

				// --- STEP 4-PRE: Execute pipeline BEFORE AI call so results can be injected into context ---
				let pipelineContextInjection = '';
				let emailActionResult: any = null;
				if (emailAction === 'pipeline') {
					try {
						// Determine how far back to scan based on user message
						let pipelineHoursBack = 24;
						if (/since\s+monday/i.test(message)) {
							const now = new Date();
							const dayOfWeek = now.getUTCDay();
							const daysSinceMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
							pipelineHoursBack = (daysSinceMonday * 24) + now.getUTCHours() + 12;
						} else if (/last\s+(\d+)\s+days?/i.test(message)) {
							const dm = message.match(/last\s+(\d+)\s+days?/i);
							pipelineHoursBack = Math.min(parseInt(dm![1]) * 24, 168);
						} else if (/last\s+few\s+days|past\s+few\s+days|recent|this\s+week/i.test(message)) {
							pipelineHoursBack = 120;
						} else if (/last\s+week|past\s+week/i.test(message)) {
							pipelineHoursBack = 168;
						} else if (/since\s+(tuesday|wednesday|thursday|friday|saturday|sunday)/i.test(message)) {
							const dayNames = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'];
							const targetDay = dayNames.indexOf(message.match(/since\s+(\w+day)/i)![1].toLowerCase());
							if (targetDay >= 0) {
								const now = new Date();
								const current = now.getUTCDay();
								const diff = current >= targetDay ? current - targetDay : 7 - (targetDay - current);
								pipelineHoursBack = (diff * 24) + now.getUTCHours() + 12;
							}
						}

						console.log(`[pipeline-pre] Running pipeline: ${pipelineHoursBack}h back (${Math.round(pipelineHoursBack / 24)} days)`);
						const pipeResult = await processEmailPipeline('both', Math.min(pipelineHoursBack, 168), env);

						// --- POST-PIPELINE VERIFICATION: Query DB for ground truth ---
						const sinceISO = new Date(Date.now() - pipelineHoursBack * 60 * 60 * 1000).toISOString();
						const verifiedEmails = await env.MEMORY_DB.prepare(
							`SELECT pe.subject, pe.from_email, pe.from_name, pe.matched_client, pe.matched_case_number, pe.processing_status, pe.attachments_filed, pe.deadlines_extracted, pe.received_date
							 FROM processed_emails pe
							 WHERE pe.created_at >= ?
							 ORDER BY pe.received_date DESC
							 LIMIT 60`
						).bind(sinceISO).all();

						const verifiedAttachments = await env.MEMORY_DB.prepare(
							`SELECT efa.original_filename, efa.filed_path, efa.client_name, efa.case_number, efa.file_size, efa.created_at
							 FROM email_filed_attachments efa
							 WHERE efa.created_at >= ?
							 ORDER BY efa.created_at DESC
							 LIMIT 50`
						).bind(sinceISO).all();

						const verifiedDeadlines = await env.MEMORY_DB.prepare(
							`SELECT d.client_name, d.case_number, d.deadline_type, d.due_date, d.description, d.source
							 FROM deadlines d
							 WHERE d.created_at >= ? AND d.source LIKE '%email%'
							 ORDER BY d.created_at DESC
							 LIMIT 20`
						).bind(sinceISO).all();

						// Build verified context injection for AI
						const processedList = (verifiedEmails.results || []);
						const filedList = (verifiedAttachments.results || []);
						const deadlineList = (verifiedDeadlines.results || []);

						pipelineContextInjection = `\n\n═══════════════════════════════════════════════════
📧 EMAIL PIPELINE RESULTS — VERIFIED FROM DATABASE (Ground Truth)
═══════════════════════════════════════════════════
Pipeline scanned last ${Math.round(pipelineHoursBack / 24)} day(s). These are REAL results from the database — report them EXACTLY.

SUMMARY: ${pipeResult.totalProcessed} emails processed | ${pipeResult.totalFiled} attachments filed to OneDrive | ${pipeResult.totalDeadlines} deadlines extracted | ${pipeResult.totalUnmatched} unmatched emails

`;
						if (processedList.length > 0) {
							pipelineContextInjection += `EMAILS PROCESSED (${processedList.length}):\n`;
							for (const pe of processedList as any[]) {
								const status = pe.processing_status === 'unmatched' ? '⚠️ UNMATCHED' : `✅ → ${pe.matched_client || 'unknown'}`;
								pipelineContextInjection += `  • "${pe.subject}" from ${pe.from_name || pe.from_email} (${(pe.received_date || '').substring(0, 10)}) — ${status}${pe.attachments_filed > 0 ? ` [${pe.attachments_filed} attachment(s) filed]` : ''}${pe.deadlines_extracted > 0 ? ` [${pe.deadlines_extracted} deadline(s)]` : ''}\n`;
							}
						} else {
							pipelineContextInjection += `EMAILS PROCESSED: None found in this time window (all may have been previously processed — dedup prevents reprocessing).\n`;
						}

						if (filedList.length > 0) {
							pipelineContextInjection += `\nATTACHMENTS FILED TO ONEDRIVE (${filedList.length}):\n`;
							for (const f of filedList as any[]) {
								const sizeKB = f.file_size ? `${Math.round((f.file_size as number) / 1024)}KB` : 'unknown size';
								pipelineContextInjection += `  ✅ ${f.original_filename} (${sizeKB}) → ${f.filed_path} [${f.client_name}/${f.case_number}]\n`;
							}
						}

						if (deadlineList.length > 0) {
							pipelineContextInjection += `\nDEADLINES EXTRACTED FROM EMAILS (${deadlineList.length}):\n`;
							for (const d of deadlineList as any[]) {
								pipelineContextInjection += `  📅 ${d.due_date} — ${d.deadline_type}: ${(d.description || '').substring(0, 100)} [${d.client_name}]\n`;
							}
						}

						if (pipeResult.totalUnmatched > 0) {
							const unmatchedEmails = processedList.filter((pe: any) => pe.processing_status === 'unmatched');
							if (unmatchedEmails.length > 0) {
								pipelineContextInjection += `\n⚠️ UNMATCHED EMAILS (${unmatchedEmails.length}) — could not match to a case:\n`;
								for (const ue of unmatchedEmails as any[]) {
									pipelineContextInjection += `  • "${ue.subject}" from ${ue.from_name || ue.from_email}\n`;
								}
							}
						}

						pipelineContextInjection += `\nINSTRUCTION: Report ONLY the above verified data. Do NOT add, embellish, or omit any items. If 0 attachments were filed, say 0. If 3 were filed, list all 3 by name and destination path.\n═══════════════════════════════════════════════════\n`;

						emailActionResult = {
							type: 'pipeline', status: 'complete',
							note: `Processed ${pipeResult.totalProcessed} emails (last ${Math.round(pipelineHoursBack / 24)} days): ${pipeResult.totalFiled} attachments filed to OneDrive, ${pipeResult.totalDeadlines} deadlines extracted, ${pipeResult.totalUnmatched} unmatched.`,
							data: pipeResult,
							verified: { emails: processedList.length, attachments: filedList.length, deadlines: deadlineList.length }
						};

						console.log(`[pipeline-pre] Verified: ${processedList.length} emails, ${filedList.length} attachments, ${deadlineList.length} deadlines`);
					} catch (pErr: any) {
						console.error('[pipeline-pre] Pipeline error:', pErr.message);
						emailActionResult = { type: 'pipeline', status: 'error', note: `Pipeline error: ${pErr.message}` };
						pipelineContextInjection = `\n\n[EMAIL PIPELINE ERROR: ${pErr.message}. Report this error honestly to the user.]\n`;
					}
				}

				// Role anchor — prepended to every user message to prevent mid-conversation drift
				const roleAnchor = `[SYNTHIA: You have mastered the desktop data. Identify your hat (Secretary/Paralegal/Attorney) for this task. Execute using context — do NOT ask questions you can answer yourself. "C"/"Cs"=Client's. One match=use it.${inferredClient ? ` Active client context: ${inferredClient}.` : ''}]\n\n`;

				// --- PIPELINE FAST PATH: Skip research entirely for action commands with verified results ---
				if (emailAction === 'pipeline' && pipelineContextInjection && emailActionResult?.status === 'complete') {
					// No consensus needed — AI just needs to report verified pipeline data
					console.log('Pipeline fast path → Claude reports verified results (no research needed)');
					try {
						consensus = await queryClaudeDirect(roleAnchor + message + pipelineContextInjection);
						totalSources = 1;
					} catch (fastErr: any) {
						console.error('Pipeline fast path Claude error:', fastErr.message);
						// Fallback: generate a structured report directly from the data
						consensus = emailActionResult.note;
					}
				} else if (needsResearch) {
					// WIDE END: Fan out to research AIs + CourtListener, Claude synthesizes
					console.log('Funnel wide → dispatching to research AIs + CourtListener');

					// CourtListener search — find REAL case law in parallel with AI models
					const clSearchPromise = (async (): Promise<{ case_name: string; citation: string; court: string; date_filed: string; snippet: string }[]> => {
						if (!env.COURTLISTENER_API_TOKEN) return [];
						const abort = new AbortController();
						const timer = setTimeout(() => abort.abort(), 10000);
						try {
							const legalQuery = extractLegalQuery(message);
							if (!legalQuery) return [];
							let searchUrl = `https://www.courtlistener.com/api/rest/v4/search/?q=${encodeURIComponent(legalQuery)}&type=o&page_size=5`;
							// Add Utah court filter when context suggests Utah jurisdiction
							const caseRow = inferredClient ? await env.MEMORY_DB.prepare(`SELECT case_type, court FROM party_cache WHERE client_name LIKE ? LIMIT 1`).bind(`%${inferredClient}%`).first() as any : null;
							if (/\butah\b/i.test(message) || caseRow?.court?.toLowerCase().includes('utah') || caseRow?.court?.toLowerCase().includes('district'))
								searchUrl += '&court=utah,utahctapp';
							const res = await fetch(searchUrl, {
								headers: { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` },
								signal: abort.signal
							});
							if (!res.ok) return [];
							const data = await res.json() as any;
							return (data.results || []).slice(0, 5).map((r: any) => ({
								case_name: r.caseName || r.case_name || '',
								citation: Array.isArray(r.citation) ? (r.citation[0] || '') : (r.citation || ''),
								court: r.court || '',
								date_filed: r.dateFiled || r.date_filed || '',
								snippet: (r.text || r.snippet || '').substring(0, 300).replace(/<[^>]+>/g, '')
							}));
						} catch (e: any) {
							if (e.name !== 'AbortError') console.warn('CourtListener search error:', e.message);
							return [];
						} finally {
							clearTimeout(timer);
						}
					})();

					const researchPromises = researchModels.map(async (model) => {
						try {
							const content = await Promise.race([model.fn(), timeoutFn(25000)]) as string;
							return { id: model.id, name: model.name, content, success: true };
						} catch (e: any) {
							console.error(`Research model ${model.id} failed:`, e.message);
							return { id: model.id, name: model.name, content: '', success: false };
						}
					});

					// Wait for both CourtListener and AI models in parallel
					const [clResults, ...allResults] = await Promise.all([clSearchPromise, ...researchPromises]);
					validResults = allResults.filter(r => r.success && r.content.length > 10);

					if (!env.ANTHROPIC_API_KEY) {
						consensus = validResults[0]?.content || 'No AI services available.';
						totalSources = validResults.length;
					} else {
						let synthesisInput = '';
						// Inject verified CourtListener case law FIRST so Claude prioritizes real citations
						if (clResults.length > 0) {
							synthesisInput += '\n\n## VERIFIED Case Law from CourtListener (REAL — prioritize these over AI-generated citations):\n';
							for (const c of clResults) {
								synthesisInput += `- ${c.case_name}, ${c.citation} (${c.court}, ${c.date_filed})\n  ${c.snippet}\n`;
							}
						}
						if (validResults.length > 0) {
							synthesisInput += '\n\n## Research from other AI models (synthesize these into one answer):\n';
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
				// Note: emailActionResult already declared in STEP 4-PRE for pipeline actions
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
				// NOTE: emailAction === 'pipeline' is handled in STEP 4-PRE (before AI call) with DB verification

				// --- STEP 5c: CITATION VERIFICATION (CourtListener) ---
				// Intercept response BEFORE delivery — verify all case law citations
				let citationVerification: CitationResult | null = null;
				if (env.COURTLISTENER_API_TOKEN && consensus && containsCitations(consensus)) {
					try {
						console.log('Citation verification triggered — scanning response for case law references');
						citationVerification = await verifyCitationsCourtListener(consensus, env.COURTLISTENER_API_TOKEN, env);

						// Log to eval_log table (audit trail)
						ctx.waitUntil(logEval(env.MEMORY_DB, 'citation_check', 'chat_response', citationVerification, consensus));

						// Annotate response if any citations are invalid or ambiguous
						if (citationVerification.overallResult !== 'pass') {
							consensus = annotateResponse(consensus, citationVerification);
							console.log(`Citation verification: ${citationVerification.valid} valid, ${citationVerification.invalid} INVALID, ${citationVerification.ambiguous} ambiguous`);
						} else {
							console.log(`Citation verification PASSED: ${citationVerification.valid}/${citationVerification.found} verified`);
						}

						// --- STEP 5d: SHEPARDIZE valid citations (non-blocking, best effort) ---
						if (citationVerification.validCitations.length > 0 && citationVerification.validCitations.length <= 5) {
							try {
								const shepResults: ShepardizeResult[] = [];
								for (const vc of citationVerification.validCitations) {
									try {
										const sr = await shepardize(vc.citation, env.COURTLISTENER_API_TOKEN!);
										shepResults.push(sr);
									} catch { /* skip individual failures */ }
								}

								if (shepResults.length > 0) {
									const treatmentLines = shepResults.map(sr => {
										const negCount = sr.negative_treatments.length;
										const detail = negCount > 0
											? ` — ${sr.negative_treatments.map(n => n.type).join(', ')}`
											: sr.total_citing > 0 ? ` — cited ${sr.total_citing}×` : '';
										return `${sr.signal} **${sr.case_name}** (${sr.citation})${detail}`;
									});

									const anyBad = shepResults.some(s => s.signal === '🔴');
									const anyCaution = shepResults.some(s => s.signal === '🟡');
									const header = anyBad
										? '\n\n---\n🔴 **Treatment Alert — Bad Law Detected**'
										: anyCaution
											? '\n\n---\n🟡 **Treatment Check — Caution**'
											: '\n\n---\n🟢 **Treatment Check**';

									consensus += `${header}\n${treatmentLines.join('\n')}`;

									(citationVerification as any).shepardize = shepResults.map(sr => ({
										citation: sr.citation,
										signal: sr.signal,
										signal_label: sr.signal_label,
										total_citing: sr.total_citing,
										negative_count: sr.negative_treatments.length,
										positive_count: sr.positive_treatments.length,
										precedential_status: sr.precedential_status
									}));

									console.log(`Shepardize: ${shepResults.map(s => `${s.citation}=${s.signal}`).join(', ')}`);
								}
							} catch (shepErr: any) {
								console.error('Shepardize error (non-blocking):', shepErr.message);
							}
						}
					} catch (cvErr: any) {
						console.error('Citation verification error (non-blocking):', cvErr.message);
					}
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
					...(inferredClient && { activeClient: inferredClient }),
					...(citationVerification && { citationVerification: {
						result: citationVerification.overallResult,
						found: citationVerification.found,
						valid: citationVerification.valid,
						invalid: citationVerification.invalid,
						ambiguous: citationVerification.ambiguous,
						invalidCitations: citationVerification.invalidCitations,
						validCitations: citationVerification.validCitations.map(v => ({ citation: v.citation, case_name: v.case_name, url: v.url })),
					}})
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
			// CITATION VERIFICATION API
			// ═══════════════════════════════════════════════════════════════

			// Manual citation verification — submit text, get back verification results
			if (path === '/api/verify/citations' && request.method === 'POST') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const { text } = await request.json() as any;
					if (!text) return err('text is required', 400);
					const result = await verifyCitationsCourtListener(text, env.COURTLISTENER_API_TOKEN, env);
					await logEval(env.MEMORY_DB, 'manual_verify', 'api', result, text.substring(0, 500));
					return json({ success: true, verification: result });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Verify a single citation
			if (path === '/api/verify/citation' && request.method === 'POST') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const { citation } = await request.json() as any;
					if (!citation) return err('citation is required', 400);
					const clHeaders = { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}`, 'Content-Type': 'application/json' };
					// Use citation-lookup API for precise matching
					const lookupRes = await fetch('https://www.courtlistener.com/api/rest/v4/citation-lookup/', {
						method: 'POST', headers: clHeaders,
						body: JSON.stringify({ text: citation })
					});
					if (!lookupRes.ok) return err('CourtListener lookup failed', 502);
					const lookupData = await lookupRes.json() as any;
					const matches = Array.isArray(lookupData) ? lookupData : [];
					if (matches.length > 0 && matches[0].status === 200 && matches[0].clusters?.length > 0) {
						const cluster = matches[0].clusters[0];
						return json({
							success: true, verified: true, citation,
							case_name: cluster.case_name || cluster.case_name_short || '',
							court: cluster.court_id || '',
							date_filed: cluster.date_filed || '',
							url: cluster.absolute_url ? `https://www.courtlistener.com${cluster.absolute_url}` : '',
							judges: cluster.judges || '',
							citation_count: cluster.citation_count || 0,
							parallel_citations: (cluster.citations || []).map((c: any) => `${c.volume} ${c.reporter} ${c.page}`)
						});
					}
					return json({ success: true, verified: false, citation, message: 'Citation not found in CourtListener database of 9M+ decisions' });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Search CourtListener for case law (general search)
			if (path === '/api/verify/search' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const q = url.searchParams.get('q');
					const court = url.searchParams.get('court') || ''; // e.g., 'utah' for UT Supreme Court
					if (!q) return err('q query parameter required', 400);
					let searchUrl = `https://www.courtlistener.com/api/rest/v4/search/?q=${encodeURIComponent(q)}&type=o`;
					if (court) searchUrl += `&court=${encodeURIComponent(court)}`;
					const res = await fetch(searchUrl, {
						headers: { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` }
					});
					if (!res.ok) return err('CourtListener search failed', 502);
					const data = await res.json() as any;
					const results = (data.results || []).slice(0, 10).map((r: any) => ({
						case_name: r.caseName || r.case_name || '',
						citation: r.citation?.[0] || '',
						court: r.court || '',
						date_filed: r.dateFiled || '',
						url: r.absolute_url ? `https://www.courtlistener.com${r.absolute_url}` : '',
						snippet: (r.text || '').substring(0, 200)
					}));
					return json({ success: true, query: q, count: data.count || 0, results });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Verify a judge name via CourtListener People API
			if (path === '/api/verify/judge' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const name = url.searchParams.get('name');
					if (!name) return err('name query parameter required', 400);
					const result = await verifyJudgeName(name, env.COURTLISTENER_API_TOKEN);
					return json({ success: true, ...result });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Get eval log history
			if (path === '/api/verify/log' && request.method === 'GET') {
				try {
					const limit = parseInt(url.searchParams.get('limit') || '50');
					const failsOnly = url.searchParams.get('fails_only') === 'true';
					let q = `SELECT * FROM eval_log`;
					if (failsOnly) q += ` WHERE overall_result IN ('fail', 'flag')`;
					q += ` ORDER BY timestamp DESC LIMIT ?`;
					const { results } = await env.MEMORY_DB.prepare(q).bind(limit).all();
					return json({ success: true, count: results?.length || 0, logs: results || [] });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Docket lookup — find a case by docket number and court
			if (path === '/api/verify/docket' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const docketNumber = url.searchParams.get('docket_number');
					const court = url.searchParams.get('court') || ''; // e.g., 'utd' for Utah District, 'scotus', 'ca10' for 10th Circuit
					if (!docketNumber) return err('docket_number query parameter required', 400);
					let docketUrl = `https://www.courtlistener.com/api/rest/v4/dockets/?docket_number=${encodeURIComponent(docketNumber)}`;
					if (court) docketUrl += `&court=${encodeURIComponent(court)}`;
					const res = await fetch(docketUrl, {
						headers: { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` }
					});
					if (!res.ok) return err('CourtListener docket lookup failed', 502);
					const data = await res.json() as any;
					const results = (data.results || []).slice(0, 10).map((d: any) => ({
						id: d.id,
						case_name: d.case_name || '',
						docket_number: d.docket_number || '',
						court_id: d.court_id || '',
						date_filed: d.date_filed || '',
						date_terminated: d.date_terminated || '',
						assigned_to: d.assigned_to_str || '',
						referred_to: d.referred_to_str || '',
						cause: d.cause || '',
						nature_of_suit: d.nature_of_suit || '',
						jury_demand: d.jury_demand || '',
						jurisdiction_type: d.jurisdiction_type || '',
						clusters: d.clusters || [],
						url: d.absolute_url ? `https://www.courtlistener.com${d.absolute_url}` : '',
						pacer_case_id: d.pacer_case_id || ''
					}));
					return json({ success: true, query: docketNumber, court, count: data.count || results.length, results });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Opinion text — get the actual text of a decision by cluster ID
			if (path === '/api/verify/opinion' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const clusterId = url.searchParams.get('cluster_id');
					const opinionId = url.searchParams.get('opinion_id');
					if (!clusterId && !opinionId) return err('cluster_id or opinion_id required', 400);
					const clHeaders = { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` };

					if (clusterId) {
						// Get cluster first for metadata, then get opinions
						const clusterRes = await fetch(
							`https://www.courtlistener.com/api/rest/v4/clusters/${clusterId}/`,
							{ headers: clHeaders }
						);
						if (!clusterRes.ok) return err('Cluster not found', 404);
						const cluster = await clusterRes.json() as any;

						// Get first opinion text
						const opUrl = cluster.sub_opinions?.[0];
						let opinionText = '';
						if (opUrl) {
							const opRes = await fetch(`${opUrl}?fields=html_with_citations,plain_text,type,author_str`, { headers: clHeaders });
							if (opRes.ok) {
								const op = await opRes.json() as any;
								opinionText = op.plain_text || op.html_with_citations || '';
							}
						}

						return json({
							success: true,
							cluster_id: cluster.id,
							case_name: cluster.case_name || '',
							date_filed: cluster.date_filed || '',
							judges: cluster.judges || '',
							citations: (cluster.citations || []).map((c: any) => `${c.volume} ${c.reporter} ${c.page}`),
							citation_count: cluster.citation_count || 0,
							precedential_status: cluster.precedential_status || '',
							syllabus: (cluster.syllabus || '').substring(0, 2000),
							opinion_text: opinionText.substring(0, 5000), // Trim for response size
							opinion_count: cluster.sub_opinions?.length || 0,
							url: cluster.absolute_url ? `https://www.courtlistener.com${cluster.absolute_url}` : ''
						});
					}

					// Direct opinion lookup
					const opRes = await fetch(
						`https://www.courtlistener.com/api/rest/v4/opinions/${opinionId}/?fields=html_with_citations,plain_text,type,author_str,cluster`,
						{ headers: clHeaders }
					);
					if (!opRes.ok) return err('Opinion not found', 404);
					const op = await opRes.json() as any;
					return json({
						success: true,
						opinion_id: opinionId,
						type: op.type || '',
						author: op.author_str || '',
						text: (op.plain_text || op.html_with_citations || '').substring(0, 5000),
						cluster: op.cluster || ''
					});
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Courts — list or lookup court metadata
			if (path === '/api/verify/courts' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const courtId = url.searchParams.get('id'); // e.g., 'utah', 'utahctapp', 'scotus', 'ca10'
					const clHeaders = { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` };

					if (courtId) {
						// Specific court lookup
						const res = await fetch(`https://www.courtlistener.com/api/rest/v4/courts/${courtId}/`, { headers: clHeaders });
						if (!res.ok) return err('Court not found', 404);
						const court = await res.json() as any;
						return json({
							success: true,
							id: court.id || courtId,
							full_name: court.full_name || '',
							short_name: court.short_name || '',
							citation_string: court.citation_string || '',
							url: court.url || '',
							start_date: court.start_date || '',
							end_date: court.end_date || '',
							jurisdiction: court.jurisdiction || ''
						});
					}

					// List Utah-relevant courts
					const utahCourts = ['utah', 'utahctapp', 'ca10', 'utd', 'scotus'];
					const courtData = [];
					for (const cid of utahCourts) {
						try {
							const res = await fetch(`https://www.courtlistener.com/api/rest/v4/courts/${cid}/`, { headers: clHeaders });
							if (res.ok) {
								const c = await res.json() as any;
								courtData.push({
									id: c.id || cid,
									full_name: c.full_name || '',
									short_name: c.short_name || '',
									citation_string: c.citation_string || '',
									jurisdiction: c.jurisdiction || ''
								});
							}
						} catch { /* skip */ }
					}
					return json({ success: true, courts: courtData });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Judge positions — verify judge sits on a specific court
			if (path === '/api/verify/judge-positions' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const personId = url.searchParams.get('person_id');
					const name = url.searchParams.get('name');
					if (!personId && !name) return err('person_id or name required', 400);
					const clHeaders = { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` };

					let pid = personId;
					let personName = name || '';
					if (!pid && name) {
						// Look up person first
						const lastName = name.replace(/^(judge|justice|hon\.?|the honorable)\s+/i, '').trim().split(/\s+/).pop() || name;
						const pRes = await fetch(`https://www.courtlistener.com/api/rest/v4/people/?name_last=${encodeURIComponent(lastName)}`, { headers: clHeaders });
						if (pRes.ok) {
							const pData = await pRes.json() as any;
							if (pData.results?.length > 0) {
								pid = pData.results[0].id.toString();
								personName = `${pData.results[0].name_first || ''} ${pData.results[0].name_last || ''}`.trim();
							} else {
								return json({ success: true, verified: false, message: `No judge found with name "${name}"` });
							}
						}
					}

					// Get positions for this person
					const posRes = await fetch(`https://www.courtlistener.com/api/rest/v4/positions/?person=${pid}`, { headers: clHeaders });
					if (!posRes.ok) return err('Positions lookup failed', 502);
					const posData = await posRes.json() as any;
					const positions = (posData.results || []).map((p: any) => ({
						position_type: p.position_type || '',
						court: p.court || '',
						court_exact: p.court_exact || '',
						date_start: p.date_start || '',
						date_termination: p.date_termination || '',
						appointer: p.appointer_display || '',
						how_selected: p.how_selected || '',
						nomination_process: p.nomination_process || ''
					}));

					return json({
						success: true,
						person_id: pid,
						name: personName,
						positions_count: positions.length,
						positions
					});
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Oral arguments — search recordings
			if (path === '/api/verify/oral-arguments' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const q = url.searchParams.get('q');
					const court = url.searchParams.get('court') || '';
					if (!q) return err('q query parameter required', 400);
					let searchUrl = `https://www.courtlistener.com/api/rest/v4/search/?q=${encodeURIComponent(q)}&type=oa`;
					if (court) searchUrl += `&court=${encodeURIComponent(court)}`;
					const res = await fetch(searchUrl, {
						headers: { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` }
					});
					if (!res.ok) return err('Oral argument search failed', 502);
					const data = await res.json() as any;
					const results = (data.results || []).slice(0, 10).map((r: any) => ({
						case_name: r.caseName || r.case_name || '',
						court: r.court || '',
						date_argued: r.dateArgued || r.date_argued || '',
						docket_number: r.docketNumber || r.docket_number || '',
						duration: r.duration || 0,
						download_url: r.download_url || '',
						local_path: r.local_path_mp3 || '',
						url: r.absolute_url ? `https://www.courtlistener.com${r.absolute_url}` : ''
					}));
					return json({ success: true, query: q, count: data.count || 0, results });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Financial disclosures — judge conflicts of interest
			if (path === '/api/verify/disclosures' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const personId = url.searchParams.get('person_id');
					const name = url.searchParams.get('name');
					if (!personId && !name) return err('person_id or name required', 400);
					const clHeaders = { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` };

					let pid = personId;
					if (!pid && name) {
						const lastName = name.replace(/^(judge|justice|hon\.?)\s+/i, '').trim().split(/\s+/).pop() || name;
						const pRes = await fetch(`https://www.courtlistener.com/api/rest/v4/people/?name_last=${encodeURIComponent(lastName)}`, { headers: clHeaders });
						if (pRes.ok) {
							const pData = await pRes.json() as any;
							if (pData.results?.length > 0) pid = pData.results[0].id.toString();
							else return json({ success: true, disclosures: [], message: `No judge found with name "${name}"` });
						}
					}

					const res = await fetch(
						`https://www.courtlistener.com/api/rest/v4/financial-disclosures/?person=${pid}`,
						{ headers: clHeaders }
					);
					if (!res.ok) return err('Disclosure lookup failed', 502);
					const data = await res.json() as any;
					const disclosures = (data.results || []).map((d: any) => ({
						id: d.id,
						year: d.year || '',
						url: d.filepath ? `https://www.courtlistener.com${d.filepath}` : '',
						thumbnail: d.thumbnail || '',
						page_count: d.page_count || 0,
						investments: d.investments || [],
						gifts: d.gifts || [],
						reimbursements: d.reimbursements || [],
						debts: d.debts || [],
						non_investment_incomes: d.non_investment_incomes || []
					}));
					return json({ success: true, person_id: pid, disclosures_count: disclosures.length, disclosures });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Parties & attorneys — lookup from federal PACER dockets
			if (path === '/api/verify/parties' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const docketId = url.searchParams.get('docket_id');
					if (!docketId) return err('docket_id required', 400);
					const clHeaders = { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` };
					const res = await fetch(
						`https://www.courtlistener.com/api/rest/v4/parties/?docket=${docketId}`,
						{ headers: clHeaders }
					);
					if (!res.ok) return err('Party lookup failed', 502);
					const data = await res.json() as any;
					const parties = (data.results || []).map((p: any) => ({
						id: p.id,
						name: p.name || '',
						party_types: (p.party_types || []).map((pt: any) => ({
							role: pt.name || '',
							date_terminated: pt.date_terminated || null
						})),
						attorneys: (p.attorneys || []).slice(0, 5).map((a: any) => ({
							attorney_id: a.attorney_id,
							role: a.role
						}))
					}));
					return json({ success: true, docket_id: docketId, parties_count: parties.length, parties });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Attorney lookup
			if (path === '/api/verify/attorney' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const docketId = url.searchParams.get('docket_id');
					const attorneyId = url.searchParams.get('id');
					if (!docketId && !attorneyId) return err('docket_id or id required', 400);
					const clHeaders = { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` };

					let attUrl = 'https://www.courtlistener.com/api/rest/v4/attorneys/';
					if (attorneyId) attUrl += `${attorneyId}/`;
					else attUrl += `?docket=${docketId}`;

					const res = await fetch(attUrl, { headers: clHeaders });
					if (!res.ok) return err('Attorney lookup failed', 502);
					const data = await res.json() as any;

					if (attorneyId) {
						return json({
							success: true,
							id: data.id,
							name: data.name || '',
							phone: data.phone || '',
							fax: data.fax || '',
							email: data.email || '',
							contact: data.contact_raw || '',
							parties_represented: (data.parties_represented || []).map((p: any) => ({
								party: p.party,
								role: p.role,
								docket: p.docket
							}))
						});
					}

					const attorneys = (data.results || []).map((a: any) => ({
						id: a.id,
						name: a.name || '',
						phone: a.phone || '',
						email: a.email || '',
						contact: (a.contact_raw || '').substring(0, 200)
					}));
					return json({ success: true, docket_id: docketId, attorneys_count: attorneys.length, attorneys });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Cited-by — what cases cite a given opinion
			if (path === '/api/verify/cited-by' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				try {
					const citation = url.searchParams.get('citation');
					const clusterId = url.searchParams.get('cluster_id');
					if (!citation && !clusterId) return err('citation or cluster_id required', 400);
					const clHeaders = { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}`, 'Content-Type': 'application/json' };

					let cid = clusterId;
					if (!cid && citation) {
						// Resolve citation to cluster ID
						const lookupRes = await fetch('https://www.courtlistener.com/api/rest/v4/citation-lookup/', {
							method: 'POST', headers: clHeaders,
							body: JSON.stringify({ text: citation })
						});
						if (lookupRes.ok) {
							const lookupData = await lookupRes.json() as any;
							const matches = Array.isArray(lookupData) ? lookupData : [];
							if (matches.length > 0 && matches[0].clusters?.length > 0) {
								cid = matches[0].clusters[0].id.toString();
							}
						}
						if (!cid) return json({ success: true, verified: false, message: 'Citation not found — cannot look up citing cases' });
					}

					// Search for opinions that cite this cluster
					const searchRes = await fetch(
						`https://www.courtlistener.com/api/rest/v4/search/?q=cites%3A(${cid})&type=o&order_by=dateFiled+desc`,
						{ headers: { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` } }
					);
					if (!searchRes.ok) return err('Cited-by search failed', 502);
					const data = await searchRes.json() as any;
					const citing = (data.results || []).slice(0, 20).map((r: any) => ({
						case_name: r.caseName || r.case_name || '',
						citation: r.citation?.[0] || '',
						court: r.court || '',
						date_filed: r.dateFiled || '',
						url: r.absolute_url ? `https://www.courtlistener.com${r.absolute_url}` : ''
					}));
					return json({ success: true, cluster_id: cid, total_citing: data.count || 0, results: citing });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// Shepardize — negative treatment detection for a citation
			if (path === '/api/verify/shepardize' && request.method === 'GET') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				const citation = url.searchParams.get('citation');
				if (!citation) return err('citation required (e.g. ?citation=466+U.S.+668)', 400);
				try {
					const result = await shepardize(citation, env.COURTLISTENER_API_TOKEN);
					return json({ success: true, ...result });
				} catch (e: any) {
					return json({ success: false, error: e.message }, e.message.includes('not found') ? 404 : 500);
				}
			}

			// Bulk shepardize — check multiple citations at once
			if (path === '/api/verify/shepardize' && request.method === 'POST') {
				if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener API token not configured', 500);
				const { citations } = await request.json() as any;
				if (!citations || !Array.isArray(citations) || citations.length === 0) return err('citations array required', 400);
				if (citations.length > 10) return err('Max 10 citations per request', 400);

				const results: any[] = [];
				for (const cite of citations) {
					try {
						const result = await shepardize(cite, env.COURTLISTENER_API_TOKEN);
						results.push({ success: true, ...result });
					} catch (e: any) {
						results.push({ success: false, citation: cite, error: e.message });
					}
				}

				// Overall signal: worst of all citations
				const signals = results.filter(r => r.success).map(r => r.signal);
				const overallSignal = signals.includes('🔴') ? '🔴' : signals.includes('🟡') ? '🟡' : signals.includes('🔵') ? '🔵' : '🟢';

				return json({ success: true, overall_signal: overallSignal, results });
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
					scope: 'https://www.googleapis.com/auth/gmail.readonly https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.modify https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/calendar https://www.googleapis.com/auth/calendar.events',
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
					// Cache the access token immediately so Calendar/Gmail work right away
					if (tokenData.access_token) {
						await env.CACHE.put('gmail_access_token', tokenData.access_token, { expirationTtl: 3000 });
					}
					// Store refresh token in KV as backup (Wrangler secret is primary)
					await env.CACHE.put('google_refresh_token_backup', tokenData.refresh_token);
					return json({
						success: true,
						message: 'Got refresh token! Access token cached. Run the command below to save refresh token as a Worker secret.',
						command: `echo ${tokenData.refresh_token} | npx wrangler secret put GOOGLE_REFRESH_TOKEN`,
						refresh_token: tokenData.refresh_token,
						access_token_cached: !!tokenData.access_token,
						scopes: tokenData.scope,
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
					let q = `SELECT id, client_name, case_number, case_type, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, case_url, virtual_link, court_address, court_phone, status, source FROM deadlines WHERE status IN ('active', 'pending')`;
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
						'case_type', 'court', 'district', 'judge', 'client_role', 'folder_url', 'status', 'assigned_attorney'
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
									temperature: 0,
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
				// Also loose on spelling: if strict match fails, falls back to first+last name (skip middle)
				// Misspelling tolerance: if all else fails, Levenshtein distance match against all names
				const nameParts = clientName.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/).filter(p => p.length >= 2);
				const buildFuzzyWhere = (col: string, loose = false) => {
					if (nameParts.length === 0) return { clause: `${col} LIKE ?`, binds: [`%${clientName}%`] };
					if (loose) {
						// Use first+last name (skip middle names) to avoid "ALEJANDRO" matching wrong client
						if (nameParts.length >= 3) {
							const firstLast = [nameParts[0], nameParts[nameParts.length - 1]];
							const conditions = firstLast.map(() => `LOWER(${col}) LIKE ?`);
							return { clause: conditions.join(' AND '), binds: firstLast.map(p => `%${p.toLowerCase()}%`) };
						}
						// For 2-part names, use first part (usually last name in "Last, First" format)
						return { clause: `LOWER(${col}) LIKE ?`, binds: [`%${nameParts[0].toLowerCase()}%`] };
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
					// Searches Associate's drive first, then Diane's drive as fallback
					let oneDriveFiles: any[] = [];
					try {
						if (env.MICROSOFT_CLIENT_ID && env.ONEDRIVE_FOLDER_ID) {
							const token = await getGraphToken();
							const lcParts = nameParts.map(p => p.toLowerCase());
							const firstLastParts = lcParts.length >= 3 ? [lcParts[0], lcParts[lcParts.length - 1]] : lcParts;
							const findFolder = (items: any[]) => {
								return items.find((f: any) => lcParts.every(p => f.name.toLowerCase().includes(p)))
									|| (lcParts.length > 1 && firstLastParts[0] !== firstLastParts[firstLastParts.length - 1] ? items.find((f: any) => firstLastParts.every(p => f.name.toLowerCase().includes(p))) : null)
									|| items.find((f: any) => { const fn = f.name.toLowerCase(); const p = lcParts[0]; const idx = fn.indexOf(p); return idx >= 0 && (idx === 0 || /[^a-z]/.test(fn[idx - 1])) && (idx + p.length >= fn.length || /[^a-z]/.test(fn[idx + p.length])); });
							};
							// Search BOTH drives and merge files (dedup by name, Associate priority)
							const seenNames = new Set<string>();
							const addFiles = (items: any[], driveLabel: string) => {
								for (const f of items) {
									if (seenNames.has(f.name)) continue;
									seenNames.add(f.name);
									oneDriveFiles.push({
										file_name: f.name,
										file_size: f.size,
										last_modified: f.lastModifiedDateTime,
										file_type: f.name.split('.').pop()?.toLowerCase() || '',
										download_url: `https://api.esqs-law.com/api/onedrive/file?id=${f.id}&drive=${driveLabel}`,
										web_url: `https://api.esqs-law.com/api/onedrive/file?id=${f.id}&drive=${driveLabel}`,
										source: 'onedrive'
									});
								}
							};
							// Associate's drive
							const folderRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${env.ONEDRIVE_FOLDER_ID}/children?$top=200`, { headers: { 'Authorization': `Bearer ${token}` } });
							const folderData = await folderRes.json() as any;
							const assocFolder = findFolder(folderData.value || []);
							if (assocFolder) {
								const aFilesRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${assocFolder.id}/children?$top=50&$orderby=lastModifiedDateTime desc`, { headers: { 'Authorization': `Bearer ${token}` } });
								const aFilesData = await aFilesRes.json() as any;
								addFiles(aFilesData.value || [], 'associate');
							}
							// Diane's drive — always check for additional files
							try {
								const dianeRes = await fetch('https://graph.microsoft.com/v1.0/users/diane@dianepitcher.com/drive/items/01U5K3O7VWI7BU54HJQJBLWG76HRSFVGF7/children?$top=200', { headers: { 'Authorization': `Bearer ${token}` } });
								const dianeData = await dianeRes.json() as any;
								const dianeFolder = findFolder(dianeData.value || []);
								if (dianeFolder) {
									const dFilesRes = await fetch(`https://graph.microsoft.com/v1.0/users/diane@dianepitcher.com/drive/items/${dianeFolder.id}/children?$top=50&$orderby=lastModifiedDateTime desc`, { headers: { 'Authorization': `Bearer ${token}` } });
									const dFilesData = await dFilesRes.json() as any;
									addFiles(dFilesData.value || [], 'diane');
								}
							} catch (_) {}
							// Sort merged results by last_modified desc
							oneDriveFiles.sort((a: any, b: any) => (b.last_modified || '').localeCompare(a.last_modified || ''));
						}
					} catch (odErr: any) {
						console.error('OneDrive facesheet lookup (non-fatal):', odErr.message);
					}

					// Google Drive files (esqslaw@gmail.com — ESQs case files)
					let gdriveFiles: any[] = [];
					try {
						if (env.GOOGLE_OAUTH_CLIENT_ID && env.GOOGLE_REFRESH_TOKEN) {
							const gToken = await getGmailToken();
							// Search for folders matching client name — use first part (last name in "Last, First") to avoid middle-name collisions
							const searchPart = nameParts.length >= 3 ? nameParts[nameParts.length - 1] : nameParts[0] || '';
							if (searchPart.length >= 2) {
								const folderRes = await fetch(`https://www.googleapis.com/drive/v3/files?${new URLSearchParams({
									q: `mimeType = 'application/vnd.google-apps.folder' and trashed = false and name contains '${searchPart.replace(/'/g, "\\'")}'`,
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
				const clientName = url.searchParams.get('client');
				let q = 'SELECT id, case_id, client_name, doc_name, doc_type, template_id, file_size, mime_type, source, created_by, created_at FROM documents WHERE 1=1';
				const p: any[] = [];
				if (caseId) { q += ' AND case_id = ?'; p.push(caseId); }
				if (clientName) { q += ' AND client_name LIKE ?'; p.push(`%${clientName}%`); }
				q += ' ORDER BY created_at DESC LIMIT 100';
				const { results } = await env.DB.prepare(q).bind(...p).all();
				const docs = (results || []).map((d: any) => ({ ...d, download_url: `https://api.esqs-law.com/api/documents/download/${d.id}` }));
				return json({ success: true, documents: docs });
			}

			if (path === '/api/documents/upload' && request.method === 'POST') {
				const formData = await request.formData();
				const file = formData.get('file') as File;
				const caseId = formData.get('case_id') as string || null;
				const clientName = formData.get('client_name') as string || null;
				const docType = formData.get('doc_type') as string || null;
				const templateId = formData.get('template_id') as string || null;
				if (!file) return err('No file provided', 400);

				const r2Key = `documents/${caseId || 'general'}/${Date.now()}-${file.name}`;
				await env.DOCUMENTS.put(r2Key, file.stream(), { customMetadata: { originalName: file.name } });
				const r = await env.DB.prepare(
					`INSERT INTO documents (case_id, client_name, doc_name, doc_type, template_id, r2_key, file_size, mime_type, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'upload', ?)`
				).bind(caseId, clientName, file.name, docType, templateId, r2Key, file.size, file.type || 'application/octet-stream', mtnISO()).run();
				const docId = r.meta.last_row_id;
				return json({ success: true, id: docId, r2_key: r2Key, download_url: `https://api.esqs-law.com/api/documents/download/${docId}` });
			}

			// POST /api/documents/store — JSON upload (base64 content) for generated documents
			if (path === '/api/documents/store' && request.method === 'POST') {
				const body = await request.json() as any;
				const { file_name, file_content, case_id, client_name, doc_type, template_id, mime_type } = body;
				if (!file_name || !file_content) return err('file_name and file_content (base64) required', 400);

				const buffer = Uint8Array.from(atob(file_content), c => c.charCodeAt(0));
				const r2Key = `documents/${case_id || 'general'}/${Date.now()}-${file_name}`;
				const mType = mime_type || (file_name.endsWith('.pdf') ? 'application/pdf' : 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
				await env.DOCUMENTS.put(r2Key, buffer, { customMetadata: { originalName: file_name } });
				const r = await env.DB.prepare(
					`INSERT INTO documents (case_id, client_name, doc_name, doc_type, template_id, r2_key, file_size, mime_type, source, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'generated', ?)`
				).bind(case_id || null, client_name || null, file_name, doc_type || null, template_id || null, r2Key, buffer.length, mType, mtnISO()).run();
				const docId = r.meta.last_row_id;
				return json({ success: true, id: docId, download_url: `https://api.esqs-law.com/api/documents/download/${docId}`, file_name, file_size: buffer.length });
			}

			const docMatch = path.match(/^\/api\/documents\/download\/(\d+)$/);
			if (docMatch && request.method === 'GET') {
				const doc = await env.DB.prepare('SELECT * FROM documents WHERE id = ?').bind(docMatch[1]).first() as any;
				if (!doc) return err('Not found', 404);
				const obj = await env.DOCUMENTS.get(doc.r2_key);
				if (!obj) return err('File not found in storage', 404);
				const inline = url.searchParams.get('inline') === '1';
				const disposition = inline ? `inline; filename="${doc.doc_name}"` : `attachment; filename="${doc.doc_name}"`;
				return new Response(obj.body, {
					headers: { 'Content-Disposition': disposition, 'Content-Type': doc.mime_type || 'application/octet-stream', ...corsHeaders }
				});
			}

			// GET /api/onedrive/file?id=xxx or ?path=xxx — proxy file view (inline preview + download option)
			// drive=diane → fetch from Diane's OneDrive; default = Associate's
			if (path === '/api/onedrive/file' && request.method === 'GET') {
				const filePath = url.searchParams.get('path');
				const itemId = url.searchParams.get('id');
				const forceDownload = url.searchParams.get('download') === '1';
				const driveParam = url.searchParams.get('drive');
				const graphDriveBase = driveParam === 'diane' ? 'users/diane@dianepitcher.com/' : 'me/';
				if (!filePath && !itemId) return err('path or id required', 400);
				try {
					const token = await getGraphToken();
					let item: any = null;

					if (itemId) {
						const res = await fetch(`https://graph.microsoft.com/v1.0/${graphDriveBase}drive/items/${itemId}`, {
							headers: { 'Authorization': `Bearer ${token}` }
						});
						item = await res.json() as any;
					} else if (filePath) {
						const encodedPath = encodeURIComponent(filePath).replace(/%2F/g, '/');
						const res = await fetch(`https://graph.microsoft.com/v1.0/${graphDriveBase}drive/items/${env.ONEDRIVE_FOLDER_ID}:/${encodedPath}`, {
							headers: { 'Authorization': `Bearer ${token}` }
						});
						item = await res.json() as any;
					}

					if (item?.error) return err(`OneDrive error: ${item.error.message || item.error.code || 'Unknown'}`, 404);

					// If it's a FOLDER, list its contents instead of trying to download
					if (item?.folder) {
						const token2 = await getGraphToken();
						const driveSuffix = driveParam === 'diane' ? '&drive=diane' : '';
						const childRes = await fetch(`https://graph.microsoft.com/v1.0/${graphDriveBase}drive/items/${item.id}/children?$top=100&$select=name,id,size,lastModifiedDateTime,file,folder,webUrl`, {
							headers: { 'Authorization': `Bearer ${token2}` }
						});
						const childData = await childRes.json() as any;
						const children = (childData.value || []).map((c: any) => ({
							name: c.name,
							id: c.id,
							type: c.folder ? 'folder' : 'file',
							size: c.size,
							modified: c.lastModifiedDateTime,
							webUrl: c.webUrl,
							mimeType: c.file?.mimeType || null,
							viewLink: `/api/onedrive/file?id=${c.id}${driveSuffix}`,
						}));
						return json({
							success: true,
							type: 'folder',
							name: item.name,
							id: item.id,
							webUrl: item.webUrl,
							childCount: item.folder.childCount,
							children,
						});
					}

					const downloadUrl = item?.['@microsoft.graph.downloadUrl'];
					if (!downloadUrl) return err('File not found or no download URL', 404);

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
					const downloadLink = `https://api.esqs-law.com/api/onedrive/file?${itemId ? 'id=' + encodeURIComponent(itemId) : 'path=' + encodeURIComponent(filePath || '')}&download=1${driveParam === 'diane' ? '&drive=diane' : ''}`;
					// Use Office Online preview if available
					const previewUrl = itemId ? `https://graph.microsoft.com/v1.0/${graphDriveBase}drive/items/${encodeURIComponent(itemId)}/preview` : null;
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

			// --- Personal Microsoft Graph token (personal OneDrive, same app registration) ---
			async function getPersonalGraphToken(): Promise<string> {
				const cached = await env.CACHE.get('personal_ms_graph_token');
				if (cached) return cached;
				let refreshToken = env.PERSONAL_MS_REFRESH_TOKEN;
				if (!refreshToken) {
					const kvBackup = await env.CACHE.get('personal_ms_refresh_token_backup');
					if (kvBackup) refreshToken = kvBackup;
				}
				if (!refreshToken) throw new Error('Personal OneDrive not configured. Auth at /api/personal-onedrive/oauth');
				const res = await fetch('https://login.microsoftonline.com/common/oauth2/v2.0/token', {
					method: 'POST',
					headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
					body: new URLSearchParams({
						client_id: env.MICROSOFT_CLIENT_ID,
						client_secret: env.MICROSOFT_CLIENT_SECRET,
						refresh_token: refreshToken,
						grant_type: 'refresh_token',
						scope: 'https://graph.microsoft.com/User.Read https://graph.microsoft.com/Files.Read https://graph.microsoft.com/Files.Read.All offline_access',
					})
				});
				const data = await res.json() as any;
				if (!data.access_token) {
					// Try KV backup if primary failed
					const kvBackup = await env.CACHE.get('personal_ms_refresh_token_backup');
					if (kvBackup && kvBackup !== refreshToken) {
						const res2 = await fetch('https://login.microsoftonline.com/common/oauth2/v2.0/token', {
							method: 'POST',
							headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
							body: new URLSearchParams({
								client_id: env.MICROSOFT_CLIENT_ID, client_secret: env.MICROSOFT_CLIENT_SECRET,
								refresh_token: kvBackup, grant_type: 'refresh_token',
								scope: 'https://graph.microsoft.com/User.Read https://graph.microsoft.com/Files.Read https://graph.microsoft.com/Files.Read.All offline_access',
							})
						});
						const data2 = await res2.json() as any;
						if (data2.access_token) {
							await env.CACHE.put('personal_ms_graph_token', data2.access_token, { expirationTtl: 3000 });
							if (data2.refresh_token) await env.CACHE.put('personal_ms_refresh_token_backup', data2.refresh_token);
							return data2.access_token;
						}
					}
					throw new Error(`Personal Graph token failed: ${data.error_description || data.error}. Re-auth at /api/personal-onedrive/oauth`);
				}
				await env.CACHE.put('personal_ms_graph_token', data.access_token, { expirationTtl: 3000 });
				if (data.refresh_token) await env.CACHE.put('personal_ms_refresh_token_backup', data.refresh_token);
				return data.access_token;
			}

			// --- Google OAuth2 token helper (esqslaw@gmail.com — Gmail + Drive) ---
			async function getGmailToken(): Promise<string> {
				const cached = await env.CACHE.get('gmail_access_token');
				if (cached) return cached;
				// Try primary (Wrangler secret), then KV backup
				let refreshToken = env.GOOGLE_REFRESH_TOKEN;
				const res = await fetch('https://oauth2.googleapis.com/token', {
					method: 'POST',
					headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
					body: new URLSearchParams({
						grant_type: 'refresh_token',
						client_id: env.GOOGLE_OAUTH_CLIENT_ID,
						client_secret: env.GOOGLE_OAUTH_CLIENT_SECRET,
						refresh_token: refreshToken,
					})
				});
				let data = await res.json() as any;
				// If primary token expired, try KV backup
				if (!data.access_token) {
					const kvBackup = await env.CACHE.get('google_refresh_token_backup');
					if (kvBackup && kvBackup !== refreshToken) {
						console.log('[getGmailToken] Primary refresh token failed, trying KV backup...');
						const res2 = await fetch('https://oauth2.googleapis.com/token', {
							method: 'POST',
							headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
							body: new URLSearchParams({
								grant_type: 'refresh_token',
								client_id: env.GOOGLE_OAUTH_CLIENT_ID,
								client_secret: env.GOOGLE_OAUTH_CLIENT_SECRET,
								refresh_token: kvBackup,
							})
						});
						data = await res2.json() as any;
					}
				}
				if (!data.access_token) {
					console.error('Google token error:', JSON.stringify(data));
					throw new Error(`Failed to get Google token: ${data.error_description || data.error || 'Unknown error'}. Re-auth at /api/gmail/oauth`);
				}
				if (data.scope) console.log('Google token scopes:', data.scope);
				await env.CACHE.put('gmail_access_token', data.access_token, { expirationTtl: 3000 });
				return data.access_token;
			}

			// --- Zoom S2S OAuth token helper (esqslaw@gmail.com — Server-to-Server OAuth) ---
			async function getZoomToken(): Promise<string> {
				const cached = await env.CACHE.get('zoom_access_token');
				if (cached) return cached;
				const res = await fetch(`https://zoom.us/oauth/token?grant_type=account_credentials&account_id=${env.ZOOM_ACCOUNT_ID}`, {
					method: 'POST',
					headers: {
						'Authorization': 'Basic ' + btoa(env.ZOOM_CLIENT_ID + ':' + env.ZOOM_CLIENT_SECRET),
						'Content-Type': 'application/x-www-form-urlencoded',
					}
				});
				const data = await res.json() as any;
				if (!data.access_token) {
					console.error('Zoom token error:', JSON.stringify(data));
					throw new Error(`Failed to get Zoom token: ${data.reason || data.error || 'Unknown error'}`);
				}
				await env.CACHE.put('zoom_access_token', data.access_token, { expirationTtl: 3500 });
				return data.access_token;
			}

			// --- Zoom meeting creator ---
			async function createZoomMeeting(topic: string, startTime: string, duration: number = 30): Promise<{ join_url: string; start_url: string; id: number; password: string }> {
				const token = await getZoomToken();
				const res = await fetch('https://api-us.zoom.us/v2/users/esqslaw@gmail.com/meetings', {
					method: 'POST',
					headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
					body: JSON.stringify({
						topic,
						type: 2, // scheduled
						start_time: startTime, // ISO 8601
						duration,
						timezone: 'America/Denver',
						settings: {
							waiting_room: true,
							mute_upon_entry: true,
							join_before_host: false,
							auto_recording: 'cloud',
							meeting_authentication: false,
						}
					})
				});
				const data = await res.json() as any;
				if (!data.id) {
					console.error('Zoom meeting error:', JSON.stringify(data));
					throw new Error(`Failed to create Zoom meeting: ${data.message || 'Unknown error'}`);
				}
				return { join_url: data.join_url, start_url: data.start_url, id: data.id, password: data.password || '' };
			}

			// --- Google Calendar helpers (reuses getGmailToken — same OAuth refresh token) ---
			async function listGoogleCalendarEvents(timeMin: string, timeMax: string): Promise<any[]> {
				const token = await getGmailToken();
				const params = new URLSearchParams({
					timeMin: timeMin.includes('T') ? timeMin : `${timeMin}T00:00:00-07:00`,
					timeMax: timeMax.includes('T') ? timeMax : `${timeMax}T23:59:59-07:00`,
					singleEvents: 'true',
					orderBy: 'startTime',
					maxResults: '100',
				});
				const res = await fetch(`https://www.googleapis.com/calendar/v3/calendars/primary/events?${params}`, {
					headers: { 'Authorization': `Bearer ${token}` }
				});
				const data = await res.json() as any;
				if (data.error) {
					console.error('Google Calendar list error:', JSON.stringify(data.error));
					throw new Error(`Calendar API error: ${data.error.message || data.error.code}`);
				}
				return data.items || [];
			}

			async function createGoogleCalendarEvent(event: { summary: string; start: string; end: string; description?: string; location?: string; colorId?: string }): Promise<any> {
				const token = await getGmailToken();
				const body: any = {
					summary: event.summary,
					start: event.start.includes('T')
						? { dateTime: event.start, timeZone: 'America/Denver' }
						: { date: event.start },
					end: event.end.includes('T')
						? { dateTime: event.end, timeZone: 'America/Denver' }
						: { date: event.end },
				};
				if (event.description) body.description = event.description;
				if (event.location) body.location = event.location;
				if (event.colorId) body.colorId = event.colorId; // Google Calendar color IDs: 1=Lavender, 2=Sage, 3=Grape, 4=Flamingo, 5=Banana, 6=Tangerine, 7=Peacock, 8=Graphite, 9=Blueberry, 10=Basil, 11=Tomato
				const res = await fetch('https://www.googleapis.com/calendar/v3/calendars/primary/events', {
					method: 'POST',
					headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
					body: JSON.stringify(body)
				});
				const data = await res.json() as any;
				if (data.error) {
					console.error('Google Calendar create error:', JSON.stringify(data.error));
					throw new Error(`Calendar API error: ${data.error.message || data.error.code}`);
				}
				return data;
			}

			async function updateGoogleCalendarEvent(eventId: string, updates: object): Promise<any> {
				const token = await getGmailToken();
				const res = await fetch(`https://www.googleapis.com/calendar/v3/calendars/primary/events/${eventId}`, {
					method: 'PATCH',
					headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
					body: JSON.stringify(updates)
				});
				const data = await res.json() as any;
				if (data.error) {
					console.error('Google Calendar update error:', JSON.stringify(data.error));
					throw new Error(`Calendar API error: ${data.error.message || data.error.code}`);
				}
				return data;
			}

			// Helper: check if event is a court hearing or intake (should NOT get Zoom link)
			function isCourtOrIntakeEvent(summary: string): boolean {
				return /\b(hearing|arraignment|sentencing|pretrial|pre-trial|conference|plea|trial|intake|consultation|OSC|order to show cause|status|review hearing|bench trial|jury trial)\b/i.test(summary || '');
			}

			// ─── Template Renderer ───────────────────────────────────────
			function renderTemplate(template: string, vars: Record<string, string>): string {
				return template.replace(/\{\{(\w+)\}\}/g, (_, key) => vars[key] || '');
			}

			function wrapHtmlEmail(bodyHtml: string): string {
				return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f9fafb;font-family:Georgia,'Times New Roman',serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;margin:0 auto;background:#fff;">
<tr><td style="background:#800020;padding:16px 24px;"><h1 style="margin:0;color:#fff;font-size:18px;font-weight:600;">Pitcher Law PLLC</h1></td></tr>
<tr><td style="padding:24px;font-size:14px;line-height:1.6;color:#1f2937;">${bodyHtml}</td></tr>
<tr><td style="padding:16px 24px;background:#f3f4f6;font-size:11px;color:#6b7280;border-top:1px solid #e5e7eb;">
<p style="margin:0;">Pitcher Law PLLC &bull; 3610 North University Avenue, Suite 375, Provo, Utah 84604</p>
<p style="margin:4px 0 0;">Phone: (801) 960-3366 &bull; <a href="mailto:esqslaw@gmail.com" style="color:#800020;">esqslaw@gmail.com</a></p>
<p style="margin:8px 0 0;font-style:italic;">CONFIDENTIALITY NOTICE: This email and any attachments are for the exclusive and confidential use of the intended recipient. If you are not the intended recipient, please do not read, distribute, or take action based on this message. If you have received this in error, please notify the sender immediately and delete this email.</p>
</td></tr></table></body></html>`;
			}

			// ─── Communication Logger ────────────────────────────────────
			async function logCommunication(data: {
				client_name: string; case_number?: string; direction: string; channel: string;
				subject?: string; body_preview?: string; from_address?: string; to_address?: string;
				external_id?: string; source?: string; template_id?: string; status?: string;
				sent_by?: string; notes?: string;
				duration_minutes?: number; billable?: boolean; attorney?: string;
				advice_given?: boolean; follow_up_required?: boolean; follow_up_date?: string;
				client_sentiment?: string; interaction_summary?: string;
			}): Promise<void> {
				try {
					await env.MEMORY_DB.prepare(
						`INSERT OR IGNORE INTO communication_log (client_name, case_number, direction, channel, subject, body_preview, from_address, to_address, external_id, source, template_id, status, sent_by, notes, duration_minutes, billable, attorney, advice_given, follow_up_required, follow_up_date, client_sentiment, interaction_summary, created_at)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(
						data.client_name, data.case_number || null, data.direction, data.channel,
						data.subject || null, data.body_preview ? data.body_preview.substring(0, 500) : null,
						data.from_address || null, data.to_address || null, data.external_id || null,
						data.source || 'manual', data.template_id || null, data.status || 'sent',
						data.sent_by || 'JWA3', data.notes || null,
						data.duration_minutes || 0, data.billable !== false ? 1 : 0,
						data.attorney || 'JWA3', data.advice_given ? 1 : 0,
						data.follow_up_required ? 1 : 0, data.follow_up_date || null,
						data.client_sentiment || null, data.interaction_summary || null,
						new Date().toISOString()
					).run();
					// Update client_profiles contact stats (best-effort)
					if (data.duration_minutes && data.duration_minutes > 0) {
						try {
							await env.MEMORY_DB.prepare(
								`UPDATE client_profiles SET last_contact_date = ?, contact_count = contact_count + 1, total_billable_minutes = total_billable_minutes + ?, updated_at = ? WHERE client_name = ?`
							).bind(new Date().toISOString(), data.billable !== false ? data.duration_minutes : 0, new Date().toISOString(), data.client_name).run();
						} catch { /* profile may not exist yet */ }
					}
				} catch (e) { console.error('logCommunication error:', e); }
			}

			async function sendViaGmail(to: string, subject: string, body: string, cc?: string): Promise<{ success: boolean; error?: string }> {
				try {
					const token = await getGmailToken();
					// Strip non-ASCII from subject to prevent mojibake in email clients
					const safeSubject = subject.replace(/[^\x20-\x7E]/g, '').replace(/\s+/g, ' ').trim();
					// ALL emails send from esqslaw@gmail.com directly (no alias — alias can't deliver externally)
					const messageParts = [
						'From: Pitcher Law PLLC <esqslaw@gmail.com>',
						`To: ${to}`,
						cc ? `Cc: ${cc}` : '',
						`Subject: ${safeSubject}`,
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
					const sent = sendRes.status === 202 || sendRes.status === 200;
					// Log outbound email to communication_log (best-effort, never blocks)
					if (sent) {
						try {
							const caseMatch = await matchEmailToCase(subject, body || '', to, env);
							await logCommunication({
								client_name: caseMatch?.client_name || to.split('@')[0],
								case_number: caseMatch?.case_number,
								direction: 'outbound', channel: 'email', subject,
								body_preview: body, from_address: 'Associate@dianepitcher.com',
								to_address: to, source: 'graph-send', status: 'sent'
							});
						} catch { /* logging never blocks send */ }
					}
					return json({ success: sent, type: 'new', from: 'Associate@dianepitcher.com' });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// ═══════════════════════════════════════════════════════════════
			// EMAIL PROCESSING PIPELINE — Auto-file attachments + extract deadlines
			// Scans Outlook + Gmail, matches to cases, files to OneDrive, extracts deadlines
			// ═══════════════════════════════════════════════════════════════

			// --- matchEmailToCase: Match an email to a client/case ---
			async function matchEmailToCase(
				subject: string, body: string, fromEmail: string, env: Env
			): Promise<{ client_name: string; case_number: string; case_type?: string } | null> {
				const text = `${subject} ${body.substring(0, 1000)}`;
				const textLower = text.toLowerCase();

				// 1. Case number regex in subject/body → look up in party_cache
				const caseNumMatch = text.match(/\b(\d{9,12})\b/);
				let foundCaseNumber = caseNumMatch ? caseNumMatch[1] : null;
				if (foundCaseNumber) {
					const row = await env.MEMORY_DB.prepare(
						'SELECT client_name, case_number FROM party_cache WHERE case_number = ? LIMIT 1'
					).bind(foundCaseNumber).first() as any;
					if (row) return { client_name: row.client_name, case_number: row.case_number };
					// Case number found but NOT in party_cache — DON'T bail early anymore.
					// Fall through to name matching so we can still catch forwarded emails
					// about known clients (e.g., "Fw: Kelly and Kelly (244403129)" from Office).
					console.log(`[match] Case# ${foundCaseNumber} not in party_cache — falling through to name match`);
				}

				// 1b. Internal sender heuristic — emails from firm accounts (Office@, pd@, Associate@)
				// forwarding exhibits/documents → check body + recent context for client
				const isFirmSender = /^(office|pd|associate|esqslaw)@/i.test(fromEmail || '');
				if (isFirmSender && /\b(exhibits?|forwards?)\b|(?:^|\s)(?:fw|fwd):/i.test(subject)) {
					// These are internal forwards — scan body for client names
					const bodyClients = await env.MEMORY_DB.prepare(
						`SELECT client_name, case_number FROM party_cache
						 UNION SELECT client_name, case_number FROM client_cache
						 ORDER BY client_name`
					).all() as any;
					for (const c of (bodyClients.results || [])) {
						const parts = (c.client_name || '').toLowerCase().split(/\s+/).filter((p: string) => p.length >= 4);
						if (parts.length === 0) continue;
						const lastName = parts[parts.length - 1];
						if (lastName.length >= 4 && new RegExp(`\\b${lastName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i').test(text)) {
							console.log(`[match] Internal forward matched to ${c.client_name} via body/subject name`);
							return { client_name: c.client_name, case_number: foundCaseNumber || c.case_number || 'PENDING' };
						}
					}
					// No name match — check if any email addresses in body match client_email_map
					// (e.g., sweetberriesk@gmail.com in body → TERRI LYNN KELLY)
					// Use full body (not just first 1000 chars) — forwarded-from headers can be deep in HTML
					const fullText = `${subject} ${body}`;
					const bodyEmails = fullText.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g) || [];
					for (const bodyAddr of bodyEmails) {
						if (/^(office|pd|associate|esqslaw|noreply)@/i.test(bodyAddr)) continue; // skip firm addresses
						const mapHit = await env.MEMORY_DB.prepare(
							'SELECT client_name, case_number FROM client_email_map WHERE LOWER(email) = ? LIMIT 1'
						).bind(bodyAddr.toLowerCase()).first() as any;
						if (mapHit) {
							console.log(`[match] Internal forward body email mapped: ${bodyAddr} → ${mapHit.client_name}`);
							return { client_name: mapHit.client_name, case_number: foundCaseNumber || mapHit.case_number || 'PENDING' };
						}
					}
					// Still no match — defer to AI classification
					console.log(`[match] Internal forward from ${fromEmail} — no client name or email found in body/subject. Deferring to AI.`);
				}

				// 1c. Client email map — known sender emails mapped to clients
				// Handles cases like sweetberriesk@gmail.com → TERRI LYNN KELLY
				// where the client's name never appears in the email content
				if (fromEmail) {
					const emailMap = await env.MEMORY_DB.prepare(
						'SELECT client_name, case_number FROM client_email_map WHERE LOWER(email) = ? LIMIT 1'
					).bind(fromEmail.toLowerCase()).first() as any;
					if (emailMap) {
						console.log(`[match] Sender email mapped: ${fromEmail} → ${emailMap.client_name}`);
						return { client_name: emailMap.client_name, case_number: foundCaseNumber || emailMap.case_number || 'PENDING' };
					}
				}

				// 2. Opposing counsel email match
				if (fromEmail) {
					const oc = await env.MEMORY_DB.prepare(
						'SELECT counsel_name FROM opposing_counsel_intel WHERE LOWER(email) = ?'
					).bind(fromEmail.toLowerCase()).first() as any;
					if (oc) {
						const pc = await env.MEMORY_DB.prepare(
							'SELECT client_name, case_number FROM party_cache WHERE LOWER(opposing_counsel) LIKE ? LIMIT 1'
						).bind(`%${oc.counsel_name.toLowerCase()}%`).first() as any;
						if (pc) return { client_name: pc.client_name, case_number: pc.case_number };
					}
				}

				// 3. Client name match — check ALL clients (party_cache + client_cache + OneDrive folders)
				const clients = await env.MEMORY_DB.prepare(
					`SELECT client_name, case_number FROM party_cache
					 UNION
					 SELECT client_name, case_number FROM client_cache
					 ORDER BY client_name`
				).all() as any;
				for (const c of (clients.results || [])) {
					const name = (c.client_name || '').toLowerCase();
					if (!name || name.length < 3) continue;
					const parts = name.split(/\s+/).filter((p: string) => p.length > 0);
					if (parts.length === 0) continue;
					const lastName = parts[parts.length - 1];

					// Full name match (strongest signal)
					if (textLower.includes(name)) {
						return { client_name: c.client_name, case_number: foundCaseNumber || c.case_number || 'PENDING' };
					}

					// Last name + first name word-boundary match (strong signal)
					if (parts.length >= 2 && lastName.length >= 4) {
						const firstNamePart = parts[0];
						const lastNameBound = new RegExp(`\\b${lastName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
						const firstNameBound = new RegExp(`\\b${firstNamePart.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
						if (lastNameBound.test(text) && firstNameBound.test(text)) {
							return { client_name: c.client_name, case_number: foundCaseNumber || c.case_number || 'PENDING' };
						}
					}

					// Last name only — requires 5+ chars AND word boundary (lowered from 7 to catch "Kelly", "Smith" etc.)
					if (lastName.length >= 5) {
						const lastNameBound = new RegExp(`\\b${lastName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
						if (lastNameBound.test(text)) {
							return { client_name: c.client_name, case_number: foundCaseNumber || c.case_number || 'PENDING' };
						}
					}
				}

				// 4. NEF / Judicialink court filing parser — structured subject lines
				// Format: "NEF: 250500829 CN: Order (Proposed)" or
				// "Utah State Court Notice of Electronic Filing via Judicialink  -- MARTIN, JOSEPH, et al.  vs.  LOWES COMPANIES INC..., 5th District St George 250500829"
				if (/judicialink|efiler@utcourts\.gov/i.test(fromEmail)) {
					// Extract case number from NEF subject (e.g., "NEF: 250500829 CN:")
					const nefCase = subject.match(/NEF:\s*(\d{9,12})/i);
					if (nefCase && !foundCaseNumber) foundCaseNumber = nefCase[1];

					// Extract party names from Judicialink format: "-- LASTNAME, FIRSTNAME  and  LASTNAME2, FIRSTNAME2, District Case#"
					const judMatch = subject.match(/--\s+([A-Z][A-Z\s,]+?)\s+(?:and|vs\.?)\s+([A-Z][A-Z\s,]+?),\s+\d/i) ||
						text.match(/--\s+([A-Z][A-Z\s,]+?)\s+(?:and|vs\.?)\s+([A-Z][A-Z\s,]+?),\s+\d/i);
					if (judMatch) {
						// Parse "LASTNAME, FIRSTNAME" format — first party is usually our client
						const party1 = judMatch[1].replace(/,\s*et al\.?/i, '').trim();
						const party2 = judMatch[2].replace(/,\s*et al\.?/i, '').trim();
						// Try to match either party to known clients
						for (const partyRaw of [party1, party2]) {
							const partyParts = partyRaw.split(',').map((p: string) => p.trim()).filter(Boolean);
							const partyName = partyParts.length >= 2
								? `${partyParts[1]} ${partyParts[0]}`.toUpperCase()  // "FIRSTNAME LASTNAME"
								: partyRaw.toUpperCase();
							const partyLower = partyName.toLowerCase();
							for (const c of (clients.results || [])) {
								const cLower = (c.client_name || '').toLowerCase();
								if (cLower === partyLower || cLower.includes(partyLower) || partyLower.includes(cLower)) {
									return { client_name: c.client_name, case_number: foundCaseNumber || c.case_number || 'PENDING' };
								}
								// Last name match for court parties
								const partyLastName = (partyParts[0] || '').toLowerCase();
								const clientParts = cLower.split(/\s+/);
								const clientLastName = clientParts[clientParts.length - 1];
								if (partyLastName.length >= 4 && clientLastName === partyLastName) {
									return { client_name: c.client_name, case_number: foundCaseNumber || c.case_number || 'PENDING' };
								}
							}
						}
						// If no existing client matched but we have a case number, return first party as new client
						// (AI classifier + auto-add will handle this)
						if (foundCaseNumber) {
							console.log(`[match] NEF parties "${party1}" / "${party2}" not in DB — will defer to AI classifier`);
						}
					}
				}

				// 5. Subject-line keyword heuristic for common patterns
				// e.g., "Fw: Kelly and Kelly" → extract repeated surname
				const fwMatch = subject.match(/(?:Fw:|Fwd:|Re:)\s*(.+?)(?:\s*\(\d+\))?$/i);
				if (fwMatch) {
					const fwSubject = fwMatch[1].toLowerCase();
					// Re-scan clients against just the forwarded subject
					for (const c of (clients.results || [])) {
						const parts = (c.client_name || '').toLowerCase().split(/\s+/).filter((p: string) => p.length >= 4);
						const lastName = parts[parts.length - 1];
						if (lastName && fwSubject.includes(lastName)) {
							return { client_name: c.client_name, case_number: foundCaseNumber || c.case_number || 'PENDING' };
						}
					}
				}

				return null;
			}

			// --- classifyEmailViaAI: When pattern matching fails, use AI to READ the email and classify it ---
			async function classifyEmailViaAI(
				subject: string, body: string, fromEmail: string, fromName: string, env: Env
			): Promise<{ client_name: string; case_number: string; case_type?: string; document_type?: string; email_category?: string; confidence: string } | null> {
				try {
					// Get all known clients/cases for context
					const clientRows = await env.MEMORY_DB.prepare(
						`SELECT DISTINCT client_name, case_number, case_type FROM party_cache
						 UNION
						 SELECT DISTINCT client_name, case_number, '' as case_type FROM client_cache
						 ORDER BY client_name`
					).all() as any;
					const clientList = (clientRows.results || []).map((c: any) =>
						`- ${c.client_name} | Case# ${c.case_number || 'PENDING'}${c.case_type ? ' | ' + c.case_type : ''}`
					).join('\n');

					// Also get OC intel for sender recognition
					const ocRows = await env.MEMORY_DB.prepare(
						`SELECT counsel_name, email, firm_name FROM opposing_counsel_intel LIMIT 50`
					).all() as any;
					const ocList = (ocRows.results || []).map((o: any) =>
						`- ${o.counsel_name} (${o.email || ''}) — ${o.firm_name || ''}`
					).join('\n');

					const plainBody = body.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
					const emailText = `From: ${fromName} <${fromEmail}>\nSubject: ${subject}\n\n${plainBody.substring(0, 3000)}`;

					const aiRes = await env.AI.run('@cf/meta/llama-3.1-8b-instruct' as any, {
						temperature: 0,
						messages: [
							{
								role: 'system',
								content: `You are a legal secretary AI for Pitcher Law PLLC, a Utah law firm. Your job is to classify incoming emails — determine which client and case they relate to, and what type of document or communication this is.

KNOWN CLIENTS AND CASES:
${clientList}

KNOWN OPPOSING COUNSEL:
${ocList}

CLASSIFICATION RULES:
- Match emails to the MOST LIKELY client based on names, case numbers, subject matter, sender identity.
- If the sender is opposing counsel, match to the client case they're involved in.
- If it's a court filing/notice, look for case numbers or party names.
- If it's a forwarded exhibit or document from an internal sender (Office@, Associate@, pd@, esqslaw@), analyze the content to determine which client it belongs to.
- document_type should be one of: exhibit, court_filing, correspondence, discovery, motion, order, notice, settlement, financial, intake, general
- email_category: What kind of email is this? One of: case_work, court_notice, opposing_counsel, client_communication, professional_development, vendor, legal_research, marketing, internal, general
- confidence should be: high (clear match), medium (likely but ambiguous), low (best guess)
- If this email is NOT related to any client/case (e.g., CLE ads, vendor emails, newsletters), set client_name to null but STILL set email_category.
- NEVER invent, fabricate, or guess client names or case numbers that are not in the KNOWN CLIENTS list. Only use EXACT names from the list above. If unsure, return null.

Return ONLY valid JSON (no other text):
{"client_name":"EXACT NAME FROM LIST or null","case_number":"from list or email or null","document_type":"type","email_category":"category","confidence":"high|medium|low","reasoning":"brief explanation"}`
							},
							{ role: 'user', content: emailText }
						]
					}) as any;

					const responseText = (aiRes.response || '').trim();
					try {
						const jsonMatch = responseText.match(/\{[\s\S]*\}/);
						if (!jsonMatch) return null;
						const parsed = JSON.parse(jsonMatch[0]);
						if (!parsed.client_name || parsed.client_name === 'null' || parsed.client_name === 'unknown') {
							// No client match, but return email_category if AI provided one
							if (parsed.email_category) {
								return { client_name: '', case_number: '', email_category: parsed.email_category, confidence: 'medium' } as any;
							}
							return null;
						}

						// Verify the AI's suggested client actually exists in our DB
						const verified = await env.MEMORY_DB.prepare(
							`SELECT client_name, case_number FROM party_cache WHERE LOWER(client_name) = LOWER(?)
							 UNION
							 SELECT client_name, case_number FROM client_cache WHERE LOWER(client_name) = LOWER(?)
							 LIMIT 1`
						).bind(parsed.client_name, parsed.client_name).first() as any;

						if (verified) {
							console.log(`[AI-classify] Matched: ${parsed.client_name} (${parsed.confidence}) — ${parsed.reasoning || ''}`);
							return {
								client_name: verified.client_name,
								case_number: parsed.case_number || verified.case_number || 'PENDING',
								document_type: parsed.document_type || 'general',
								email_category: parsed.email_category || 'case_work',
								confidence: parsed.confidence || 'medium'
							};
						}

						// AI suggested a client name that's close but not exact — try fuzzy match
						const fuzzyClients = await env.MEMORY_DB.prepare(
							`SELECT client_name, case_number FROM party_cache
							 UNION SELECT client_name, case_number FROM client_cache`
						).all() as any;
						for (const c of (fuzzyClients.results || [])) {
							if (levenshtein((c.client_name || '').toLowerCase(), (parsed.client_name || '').toLowerCase()) <= 3) {
								console.log(`[AI-classify] Fuzzy matched: ${parsed.client_name} → ${c.client_name} (${parsed.confidence})`);
								return {
									client_name: c.client_name,
									case_number: parsed.case_number || c.case_number || 'PENDING',
									document_type: parsed.document_type || 'general',
									email_category: parsed.email_category || 'case_work',
									confidence: parsed.confidence || 'low'
								};
							}
						}

						// NEW CLIENT DISCOVERY: If AI identified a clear client name + case number from a trusted court source,
						// auto-add to party_cache. This builds our knowledge base organically.
						const isTrustedSource = /(@utcourts\.gov|judicialink|@courts\.utah\.gov)/i.test(fromEmail || '');
						const hasValidCase = parsed.case_number && /^\d{9,12}$/.test(parsed.case_number);
						if (isTrustedSource && hasValidCase && parsed.client_name && parsed.confidence !== 'low') {
							const cleanName = (parsed.client_name || '').toUpperCase().trim();
							console.log(`[AI-classify] NEW CLIENT DISCOVERY from court source: ${cleanName} | Case# ${parsed.case_number}`);
							try {
								await env.MEMORY_DB.prepare(
									`INSERT OR IGNORE INTO party_cache (client_name, case_number, case_type, created_at) VALUES (?, ?, ?, ?)`
								).bind(cleanName, parsed.case_number, parsed.document_type === 'court_filing' ? 'Civil' : 'Unknown', mtnISO()).run();
								return {
									client_name: cleanName,
									case_number: parsed.case_number,
									document_type: parsed.document_type || 'court_filing',
									confidence: parsed.confidence || 'medium'
								};
							} catch (insertErr: any) {
								console.error('[AI-classify] Failed to auto-add client:', insertErr.message);
							}
						}

						console.log(`[AI-classify] AI suggested "${parsed.client_name}" but no match in DB (source not trusted enough for auto-add)`);
						return null;
					} catch (parseErr) {
						console.error('[AI-classify] Parse error:', responseText.substring(0, 200));
						return null;
					}
				} catch (e: any) {
					console.error('[AI-classify] AI classification error:', e.message);
					return null;
				}
			}

			// --- extractDeadlinesAndCalendarViaAI: Extract deadlines, follow-ups, NLT dates, and calendar events ---
			async function extractDeadlinesAndCalendarViaAI(
				subject: string, body: string,
				caseInfo: { client_name: string; case_number: string; case_type?: string },
				emailId: number,
				env: Env
			): Promise<number> {
				const plainBody = body.replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
				const emailText = `Subject: ${subject}\n\n${plainBody.substring(0, 3000)}`;
				const today = mtnToday();

				// Get case context for enriched calendar items
				let caseContext = '';
				try {
					const pc = await env.MEMORY_DB.prepare(
						`SELECT court, judge, opposing_counsel, opposing_party, case_type, district FROM party_cache WHERE case_number = ? LIMIT 1`
					).bind(caseInfo.case_number).first() as any;
					if (pc) {
						caseContext = `\nCASE CONTEXT: Court: ${pc.court || 'unknown'} | Judge: ${pc.judge || 'unknown'} | OC: ${pc.opposing_counsel || 'unknown'} | Opposing Party: ${pc.opposing_party || 'unknown'} | Type: ${pc.case_type || caseInfo.case_type || 'unknown'} | District: ${pc.district || ''}`;
					}
				} catch {}

				try {
					const aiRes = await env.AI.run('@cf/meta/llama-3.1-8b-instruct' as any, {
						temperature: 0,
						messages: [
							{
								role: 'system',
								content: `You are a legal secretary extracting deadlines, follow-ups, and calendar items from emails for a Utah law firm.

Client: ${caseInfo.client_name} | Case# ${caseInfo.case_number}${caseContext}

Return ONLY a JSON array. Each item:
{
  "type": "hearing|trial|answer_due|response_due|discovery_due|motion_deadline|status_conference|pretrial_conference|sentencing|arraignment|filing_deadline|follow_up|nlt_deadline|email_response_due|document_due|review_deadline|conference_call",
  "date": "YYYY-MM-DD",
  "time": "HH:MM (if specified)",
  "description": "detailed description of what needs to happen",
  "court": "court name if mentioned",
  "courtroom": "room/dept if mentioned",
  "judge": "judge name if mentioned",
  "location": "physical address or 'virtual' if Webex/Zoom",
  "virtual_link": "Webex/Zoom/Teams URL if present",
  "opposing_counsel": "OC name if relevant to this item",
  "referenced_files": "any documents mentioned that should be prepared/filed",
  "event_type": "court_appearance|deadline|follow_up|meeting|conference",
  "confidence": "high|medium|low",
  "calendar_title": "Short title for calendar: e.g. 'Hearing - Kelly (244403129)'"
}

EXTRACTION RULES:
- Extract ALL explicit dates/deadlines/events, including follow-ups.
- "No later than" / "NLT" / "by [date]" / "must be filed by" → nlt_deadline
- "Please respond by" / "response needed" / "reply by" → email_response_due
- "Follow up" / "check on" / "circle back" → follow_up
- Any date with a time → likely a court appearance → include court/judge/location
- Dates MUST be future (after ${today}). Return [] if none found.
- calendar_title format: "[Type] - [Client Last Name] ([Case#])"
- Include ALL context available — court, judge, OC, location, files referenced.
- NEVER invent or fabricate dates, deadlines, or events. Only extract what is EXPLICITLY stated in the email text. If no dates/deadlines exist, return [].
- Phone call follow-ups: If the email says to call someone, create a follow_up with the phone number, reason, and any deadline mentioned.`
							},
							{ role: 'user', content: emailText }
						]
					}) as any;

					const responseText = (aiRes.response || '').trim();
					let parsed: any[];
					try {
						const jsonMatch = responseText.match(/\[[\s\S]*\]/);
						parsed = jsonMatch ? JSON.parse(jsonMatch[0]) : [];
					} catch { parsed = []; }

					if (!Array.isArray(parsed) || parsed.length === 0) return 0;

					let inserted = 0;
					for (const dl of parsed) {
						if (!dl.date || !dl.type) continue;
						if (!/^\d{4}-\d{2}-\d{2}$/.test(dl.date)) continue;
						if (dl.date <= today) continue;

						// Dedup — check exact type AND similar types (e.g., hearing vs review_hearing)
						// --- Robust dedup: check both deadlines table AND email_extracted_deadlines ---
						const typeVariants = [dl.type];
						if (dl.type.includes('hearing')) typeVariants.push('hearing', 'review_hearing', 'status_conference', 'pretrial_conference');
						if (dl.type === 'hearing') typeVariants.push('review_hearing', 'status_conference', 'pretrial_conference');
						if (dl.type.includes('conference')) typeVariants.push('pretrial_conference', 'status_conference', 'hearing', 'review_hearing');
						if (dl.type.includes('trial')) typeVariants.push('trial', 'bench_trial', 'jury_trial');
						if (dl.type.includes('arraignment')) typeVariants.push('arraignment', 'initial_appearance');
						const uniqueVariants = [...new Set(typeVariants)];
						const placeholders = uniqueVariants.map(() => '?').join(',');
						// Check main deadlines table
						const existingDl = await env.MEMORY_DB.prepare(
							`SELECT id FROM deadlines WHERE case_number = ? AND due_date = ? AND deadline_type IN (${placeholders}) AND status IN ('active','pending') LIMIT 1`
						).bind(caseInfo.case_number, dl.date, ...uniqueVariants).first();
						if (existingDl) { console.log(`[dedup] Skipping ${dl.type} ${dl.date} for ${caseInfo.case_number} — exists in deadlines`); continue; }
						// Check email_extracted_deadlines (cross-email dedup)
						const existingExt = await env.MEMORY_DB.prepare(
							`SELECT d.id FROM email_extracted_deadlines d JOIN processed_emails pe ON d.processed_email_id = pe.id WHERE pe.matched_case_number = ? AND d.due_date = ? AND d.deadline_type IN (${placeholders}) LIMIT 1`
						).bind(caseInfo.case_number, dl.date, ...uniqueVariants).first();
						if (existingExt) { console.log(`[dedup] Skipping ${dl.type} ${dl.date} for ${caseInfo.case_number} — exists in email_extracted_deadlines`); continue; }

						// Build enriched description with all available context
						let enrichedDesc = dl.description || `${dl.type} (from email)`;
						if (dl.referenced_files) enrichedDesc += ` | Files: ${dl.referenced_files}`;
						if (dl.opposing_counsel) enrichedDesc += ` | OC: ${dl.opposing_counsel}`;

						// Build calendar-ready notes
						const calNotes = [
							`Case: ${caseInfo.client_name} (${caseInfo.case_number})`,
							dl.court ? `Court: ${dl.court}` : null,
							dl.courtroom ? `Courtroom: ${dl.courtroom}` : null,
							dl.judge ? `Judge: ${dl.judge}` : null,
							dl.location ? `Location: ${dl.location}` : null,
							dl.opposing_counsel ? `OC: ${dl.opposing_counsel}` : null,
							dl.referenced_files ? `Prepare: ${dl.referenced_files}` : null,
							dl.virtual_link ? `Virtual: ${dl.virtual_link}` : null,
							`Source: ${subject.substring(0, 100)}`
						].filter(Boolean).join('\n');

						const dlResult = await env.MEMORY_DB.prepare(
							`INSERT INTO deadlines (client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, status, source, notes, created_at, virtual_link, court_address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 'email-auto', ?, ?, ?, ?)`
						).bind(
							caseInfo.client_name,
							caseInfo.case_number,
							dl.type,
							enrichedDesc,
							dl.date,
							dl.time || '',
							dl.court || '',
							dl.courtroom || '',
							dl.judge || '',
							dl.virtual_link ? 'virtual' : (dl.location === 'virtual' ? 'virtual' : ''),
							calNotes,
							mtnISO(),
							dl.virtual_link || '',
							dl.location || ''
						).run();

						// Traceability record
						await env.MEMORY_DB.prepare(
							`INSERT INTO email_extracted_deadlines (processed_email_id, deadline_id, deadline_type, due_date, description, confidence, raw_text, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
						).bind(
							emailId, dlResult.meta.last_row_id || 0, dl.type, dl.date,
							dl.calendar_title || enrichedDesc, dl.confidence || 'medium',
							emailText.substring(0, 500), mtnISO()
						).run();

						// Create Google Calendar event from this deadline
						try {
							const isAppointment = /hearing|trial|arraignment|sentencing|conference|plea|status_conference|pretrial_conference|review_hearing/i.test(dl.type);
							const isFollowUp = /follow_up|email_response_due|document_due|review_deadline|conference_call|nlt_deadline/i.test(dl.type);
							const calTitle = dl.calendar_title || `${dl.type.replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase())} - ${caseInfo.client_name.split(' ').pop()} (${caseInfo.case_number})`;

							let startStr: string;
							let endStr: string;
							if (dl.time && /^\d{2}:\d{2}/.test(dl.time)) {
								// Timed event — appointment
								const cleanTime = dl.time.replace(/\s*(AM|PM)/i, (m: string) => m.trim().toUpperCase());
								let hours = parseInt(cleanTime.split(':')[0]);
								const mins = cleanTime.split(':')[1].substring(0, 2);
								if (/PM/i.test(dl.time) && hours < 12) hours += 12;
								if (/AM/i.test(dl.time) && hours === 12) hours = 0;
								startStr = `${dl.date}T${String(hours).padStart(2, '0')}:${mins}:00`;
								// Default 1-hour duration for hearings, 30 min for follow-ups
								const durationMs = isAppointment ? 60 * 60 * 1000 : 30 * 60 * 1000;
								const endDate = new Date(new Date(`${startStr}-07:00`).getTime() + durationMs);
								endStr = `${dl.date}T${String(endDate.getUTCHours() - 7 < 0 ? endDate.getUTCHours() - 7 + 24 : endDate.getUTCHours() - 7).padStart(2, '0')}:${String(endDate.getUTCMinutes()).padStart(2, '0')}:00`;
							} else {
								// All-day event (deadline / task)
								startStr = dl.date;
								endStr = dl.date;
							}

							// Color coding: 9=Blueberry for to-do/tasks, 7=Peacock for hearings, 11=Tomato for urgent deadlines
							let colorId: string | undefined;
							if (isFollowUp) colorId = '9'; // Blueberry (blue) for tasks/follow-ups
							else if (isAppointment) colorId = '7'; // Peacock (teal) for court appearances
							else if (/nlt_deadline|filing_deadline|answer_due|response_due/i.test(dl.type)) colorId = '11'; // Tomato (red) for hard deadlines

							const calDesc = calNotes + (dl.virtual_link ? `\n\nJoin: ${dl.virtual_link}` : '');
							const calLocation = dl.virtual_link ? dl.virtual_link : (dl.location || dl.court || '');

							await createGoogleCalendarEvent({
								summary: calTitle,
								start: startStr,
								end: endStr,
								description: calDesc,
								location: calLocation,
								colorId
							});
							console.log(`[email-pipeline] Created calendar event: ${calTitle} on ${dl.date}`);
						} catch (calErr: any) {
							console.error(`[email-pipeline] Calendar event creation failed for ${dl.date}:`, calErr.message);
						}

						inserted++;
					}
					return inserted;
				} catch (e: any) {
					console.error('[email-pipeline] AI deadline/calendar extraction error:', e.message);
					return 0;
				}
			}

			// --- findClientFolder: Find client folder in OneDrive ---
			async function findClientFolder(
				clientName: string, env: Env
			): Promise<{ folderId: string; folderName: string } | null> {
				// Check KV cache first
				const cacheKey = `od-folder-${clientName.toLowerCase().replace(/\s+/g, '-')}`;
				const cached = await env.CACHE.get(cacheKey);
				if (cached) {
					try { return JSON.parse(cached); } catch {}
				}

				const token = await getGraphToken();
				const searchUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${env.ONEDRIVE_FOLDER_ID}/children?$top=200`;
				const res = await fetch(searchUrl, { headers: { 'Authorization': `Bearer ${token}` } });
				const data = await res.json() as any;
				if (!data.value) return null;

				const nameLower = clientName.toLowerCase();
				const parts = nameLower.split(/\s+/);
				const lastName = parts[parts.length - 1];

				// Exact match first
				let match = (data.value as any[]).find((f: any) => f.folder && f.name.toLowerCase() === nameLower);
				// Then contains full name
				if (!match) match = (data.value as any[]).find((f: any) => f.folder && f.name.toLowerCase().includes(nameLower));
				// Then last name match
				if (!match && lastName.length >= 3) {
					match = (data.value as any[]).find((f: any) => f.folder && f.name.toLowerCase().includes(lastName));
				}

				if (match) {
					const result = { folderId: match.id, folderName: match.name };
					await env.CACHE.put(cacheKey, JSON.stringify(result), { expirationTtl: 3600 });
					return result;
				}
				return null;
			}

			// --- fileAttachmentToOneDrive: Upload attachment to client folder ---
			async function fileAttachmentToOneDrive(
				attachment: { name: string; contentBytes: string; size: number },
				clientFolder: { folderId: string; folderName: string },
				emailSubject: string, fromEmail: string,
				caseInfo: { client_name: string; case_number: string },
				emailId: number,
				env: Env,
				aiDocType?: string
			): Promise<{ itemId: string; path: string } | null> {
				try {
					const token = await getGraphToken();
					const subjectLower = (emailSubject || '').toLowerCase();
					const fromLower = (fromEmail || '').toLowerCase();
					const fileNameLower = (attachment.name || '').toLowerCase();

					// Determine subfolder — combining regex signals, AI classification, and filename analysis
					let subfolder = 'Correspondence';

					// AI document type provides strongest signal when available
					if (aiDocType === 'exhibit') {
						subfolder = 'Exhibits';
					} else if (aiDocType === 'court_filing' || aiDocType === 'order' || aiDocType === 'notice') {
						subfolder = 'Court Documents';
					} else if (aiDocType === 'discovery') {
						subfolder = 'Discovery';
					} else if (aiDocType === 'motion') {
						subfolder = 'Court Documents';
					} else if (aiDocType === 'settlement' || aiDocType === 'financial') {
						subfolder = 'Financial';
					}
					// Regex fallbacks for when AI didn't classify
					else if (fromLower.endsWith('@utcourts.gov') || /\b(order|minute|notice|ruling|judgment|subpoena)\b/i.test(subjectLower)) {
						subfolder = 'Court Documents';
					} else if (/\b(discover|interrogator|rfp|request.{0,10}production|admission|deposition|subpoena duces)\b/i.test(subjectLower)) {
						subfolder = 'Discovery';
					} else if (/\b(plea|offer|settlement|negotiat)\b/i.test(subjectLower)) {
						subfolder = 'Plea and Sentencing';
					}
					// Subject/filename exhibit detection (catches "Exhibit" emails from internal forwards)
					else if (/\bexhibits?\b/i.test(subjectLower) || /\bexhibits?\b/i.test(fileNameLower)) {
						subfolder = 'Exhibits';
					}

					// Ensure subfolder exists (create if needed)
					const subfolderUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${clientFolder.folderId}/children`;
					const subRes = await fetch(subfolderUrl, { headers: { 'Authorization': `Bearer ${token}` } });
					const subData = await subRes.json() as any;
					let targetFolderId = clientFolder.folderId;
					const existingSub = (subData.value || []).find((f: any) => f.folder && f.name.toLowerCase() === subfolder.toLowerCase());

					if (existingSub) {
						targetFolderId = existingSub.id;
					} else {
						// Create subfolder
						const createRes = await fetch(subfolderUrl, {
							method: 'POST',
							headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
							body: JSON.stringify({ name: subfolder, folder: {}, '@microsoft.graph.conflictBehavior': 'fail' })
						});
						if (createRes.ok) {
							const created = await createRes.json() as any;
							targetFolderId = created.id;
						}
					}

					// Check if file already exists in target folder (prevent duplicates)
					const existingCheck = await env.MEMORY_DB.prepare(
						'SELECT id FROM email_filed_attachments WHERE original_filename = ? AND client_name = ? AND filed_path LIKE ?'
					).bind(attachment.name, caseInfo.client_name, `%${subfolder}%`).first();
					if (existingCheck) {
						console.log(`[email-pipeline] Skipping duplicate: ${attachment.name} already filed for ${caseInfo.client_name}`);
						return null;
					}

					// Upload file (PUT for files ≤4MB — Graph API limit)
					const fileBytes = Uint8Array.from(atob(attachment.contentBytes), c => c.charCodeAt(0));
					if (fileBytes.length > 4 * 1024 * 1024) {
						console.warn(`[email-pipeline] Skipping large file (${(fileBytes.length / 1024 / 1024).toFixed(1)}MB): ${attachment.name} — exceeds 4MB PUT limit`);
						return null;
					}

					const uploadUrl = `https://graph.microsoft.com/v1.0/me/drive/items/${targetFolderId}:/${encodeURIComponent(attachment.name)}:/content`;
					const uploadRes = await fetch(uploadUrl, {
						method: 'PUT',
						headers: {
							'Authorization': `Bearer ${token}`,
							'Content-Type': 'application/octet-stream'
						},
						body: fileBytes
					});

					if (!uploadRes.ok) {
						console.error(`[email-pipeline] Upload failed for ${attachment.name}: ${uploadRes.status}`);
						return null;
					}

					const uploadData = await uploadRes.json() as any;
					const filePath = `${clientFolder.folderName}/${subfolder}/${attachment.name}`;

					// Record in email_filed_attachments
					await env.MEMORY_DB.prepare(
						`INSERT INTO email_filed_attachments (processed_email_id, original_filename, filed_path, onedrive_item_id, file_size, client_name, case_number, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(emailId, attachment.name, filePath, uploadData.id || '', attachment.size || 0, caseInfo.client_name, caseInfo.case_number, mtnISO()).run();

					// Also record in case_files if table exists
					try {
						await env.MEMORY_DB.prepare(
							`INSERT OR IGNORE INTO case_files (client_name, case_number, file_name, file_type, file_path, source, created_at) VALUES (?, ?, ?, ?, ?, 'email-auto', ?)`
						).bind(caseInfo.client_name, caseInfo.case_number, attachment.name, attachment.name.split('.').pop() || '', filePath, mtnISO()).run();
					} catch {}

					return { itemId: uploadData.id || '', path: filePath };
				} catch (e: any) {
					console.error(`[email-pipeline] File upload error for ${attachment.name}:`, e.message);
					return null;
				}
			}

			// --- fetchGmailMessages: Fetch recent Gmail messages ---
			async function fetchGmailMessages(
				sinceHours: number, env: Env
			): Promise<any[]> {
				try {
					const token = await getGmailToken();
					const sinceEpoch = Math.floor((Date.now() - sinceHours * 60 * 60 * 1000) / 1000);
					const query = encodeURIComponent(`after:${sinceEpoch}`);
					const listUrl = `https://www.googleapis.com/gmail/v1/users/me/messages?q=${query}&maxResults=25`;
					const listRes = await fetch(listUrl, { headers: { 'Authorization': `Bearer ${token}` } });
					const listData = await listRes.json() as any;

					if (!listData.messages || listData.messages.length === 0) return [];

					const messages: any[] = [];
					for (const msg of listData.messages.slice(0, 25)) {
						const msgUrl = `https://www.googleapis.com/gmail/v1/users/me/messages/${msg.id}?format=full`;
						const msgRes = await fetch(msgUrl, { headers: { 'Authorization': `Bearer ${token}` } });
						const msgData = await msgRes.json() as any;

						const headers = msgData.payload?.headers || [];
						const getHeader = (name: string) => (headers.find((h: any) => h.name.toLowerCase() === name.toLowerCase()) || {}).value || '';

						// Decode body — recursive to handle multipart/alternative nested inside multipart/mixed
						let bodyText = '';
						const extractBody = (payload: any): void => {
							if (!payload) return;
							if (payload.body?.data) {
								const decoded = atob(payload.body.data.replace(/-/g, '+').replace(/_/g, '/'));
								if (payload.mimeType === 'text/plain' && !bodyText) {
									bodyText = decoded;
								} else if (payload.mimeType === 'text/html' && !bodyText) {
									bodyText = decoded;
								}
							}
							if (payload.parts) {
								for (const part of payload.parts) {
									extractBody(part);
									if (bodyText && payload.mimeType !== 'multipart/alternative') break; // prefer text/plain
								}
							}
						};
						extractBody(msgData.payload);

						// Extract attachments info
						const attachments: any[] = [];
						const extractAttachments = (parts: any[]) => {
							for (const part of parts) {
								if (part.filename && part.body?.attachmentId) {
									attachments.push({
										name: part.filename,
										attachmentId: part.body.attachmentId,
										size: part.body.size || 0,
										mimeType: part.mimeType
									});
								}
								if (part.parts) extractAttachments(part.parts);
							}
						};
						if (msgData.payload?.parts) extractAttachments(msgData.payload.parts);

						messages.push({
							id: `gmail_${msg.id}`,
							graphId: msg.id,
							source: 'gmail',
							subject: getHeader('Subject'),
							from: getHeader('From').match(/<(.+?)>/)?.[1] || getHeader('From'),
							fromName: getHeader('From').replace(/<.+?>/, '').trim(),
							date: getHeader('Date'),
							receivedDateTime: new Date(getHeader('Date')).toISOString(),
							body: bodyText,
							bodyPreview: bodyText.substring(0, 200),
							hasAttachments: attachments.length > 0,
							attachments
						});
					}
					return messages;
				} catch (e: any) {
					console.error('[email-pipeline] Gmail fetch error:', e.message);
					return [];
				}
			}

			// --- processEmailChunk: Process a single email (no budget limits — each chunk gets its own CPU) ---
			async function processEmailChunk(
				msg: any, env: Env
			): Promise<any> {
				try {
					// Dedup check
					const already = await env.MEMORY_DB.prepare(
						'SELECT id FROM processed_emails WHERE message_id = ?'
					).bind(msg.id).first();
					if (already) return { id: msg.id, status: 'skipped-dup' };

					// STEP 1: Pattern matching (fast, no AI cost)
					let caseMatch = await matchEmailToCase(msg.subject, msg.body, msg.from, env);
					let matchSource = caseMatch ? 'pattern' : null;
					let aiDocType: string | undefined;
					let emailCategory: string | null = null;

					// STEP 2: AI classification (no budget limit — each chunk is a separate invocation)
					if (!caseMatch) {
						const aiMatch = await classifyEmailViaAI(msg.subject, msg.body, msg.from, msg.fromName, env);
						if (aiMatch && aiMatch.client_name) {
							caseMatch = { client_name: aiMatch.client_name, case_number: aiMatch.case_number, case_type: aiMatch.case_type };
							matchSource = `ai-${aiMatch.confidence}`;
							aiDocType = aiMatch.document_type;
							emailCategory = (aiMatch as any).email_category || null;

							// Auto-learn sender mapping for high-confidence non-firm senders
							const isFirmAddr = /^(office|pd|associate|esqslaw|noreply)@/i.test(msg.from || '');
							if (aiMatch.confidence === 'high' && msg.from && !isFirmAddr) {
								try {
									await env.MEMORY_DB.prepare(
										`INSERT OR IGNORE INTO client_email_map (email, client_name, case_number, relationship, notes, created_at) VALUES (?, ?, ?, 'auto-learned', ?, ?)`
									).bind(msg.from.toLowerCase(), aiMatch.client_name, aiMatch.case_number || '', `Auto-learned from AI classification of "${(msg.subject || '').substring(0, 80)}"`, mtnISO()).run();
								} catch {}
							}
						} else {
							// AI returned no client match (or category-only) — classify non-case
							emailCategory = (aiMatch as any)?.email_category || classifyNonCaseEmail(msg.subject, msg.from, msg.fromName);
						}
					}

					// Insert processed_emails record
					const peResult = await env.MEMORY_DB.prepare(
						`INSERT INTO processed_emails (message_id, source, from_email, from_name, subject, received_date, matched_client, matched_case_number, processing_status, email_category, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(
						msg.id, msg.source || 'outlook', msg.from, msg.fromName,
						(msg.subject || '').substring(0, 500),
						msg.receivedDateTime || '',
						caseMatch?.client_name || null,
						caseMatch?.case_number || null,
						caseMatch ? `processed-${matchSource}` : 'unmatched',
						emailCategory,
						mtnISO()
					).run();
					const peId = peResult.meta.last_row_id || 0;

					// Log inbound email to communication_log
					if (caseMatch) {
						try {
							await logCommunication({
								client_name: caseMatch.client_name, case_number: caseMatch.case_number,
								direction: 'inbound', channel: 'email', subject: msg.subject,
								body_preview: (msg.body || '').substring(0, 500),
								from_address: msg.from, to_address: 'Associate@dianepitcher.com',
								external_id: msg.id, source: 'email-pipeline', status: 'received'
							});
						} catch {}
					}

					if (!caseMatch) {
						// Still mark as read — Synthia processed it
						try {
							if (msg.source === 'outlook') {
								const token = await getGraphToken();
								await fetch(`https://graph.microsoft.com/v1.0/me/messages/${msg.id}`, {
									method: 'PATCH',
									headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
									body: JSON.stringify({ isRead: true })
								});
							} else if (msg.source === 'gmail' && msg.graphId) {
								const gToken = await getGmailToken();
								await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${msg.graphId}/modify`, {
									method: 'POST',
									headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
									body: JSON.stringify({ removeLabelIds: ['UNREAD'] })
								});
							}
						} catch {}
						return {
							id: msg.id, subject: msg.subject, from: msg.from,
							status: 'unmatched', category: emailCategory,
							note: emailCategory ? `Non-case: ${emailCategory}` : 'Pattern + AI both failed to classify'
						};
					}

					// STEP 3: Extract deadlines + follow-ups + calendar items
					const deadlinesFound = await extractDeadlinesAndCalendarViaAI(msg.subject, msg.body, caseMatch, peId, env);

					// Auto-cascade: detect filing events → downstream deadlines
					let cascadeCount = 0;
					try {
						const trigger = detectTriggerEventFromEmail(msg.subject, (msg.body || '').substring(0, 2000));
						if (trigger) {
							const cascade = await cascadeDeadlinesFromEvent(
								trigger.triggerEvent, mtnToday(),
								{ client_name: caseMatch.client_name, case_number: caseMatch.case_number, case_type: caseMatch.case_type || 'civil' },
								trigger.serviceType, env,
								{ emailId: msg.id, emailSubject: msg.subject }
							);
							cascadeCount = cascade.created;
						}
					} catch {}

					// STEP 4: File attachments (no limit — own CPU budget)
					let attachmentsFiled = 0;
					if (msg.hasAttachments) {
						const clientFolder = await findClientFolder(caseMatch.client_name, env);
						if (clientFolder) {
							let attachments: any[] = [];
							if (msg.source === 'gmail' && msg.attachments) {
								const gToken = await getGmailToken();
								for (const att of msg.attachments) {
									try {
										const attUrl = `https://www.googleapis.com/gmail/v1/users/me/messages/${msg.graphId}/attachments/${att.attachmentId}`;
										const attRes = await fetch(attUrl, { headers: { 'Authorization': `Bearer ${gToken}` } });
										const attData = await attRes.json() as any;
										if (attData.data) {
											attachments.push({ name: att.name, contentBytes: attData.data.replace(/-/g, '+').replace(/_/g, '/'), size: att.size });
										}
									} catch {}
								}
							} else {
								try {
									const token = await getGraphToken();
									const attUrl = `https://graph.microsoft.com/v1.0/me/messages/${msg.id}/attachments`;
									const attRes = await fetch(attUrl, { headers: { 'Authorization': `Bearer ${token}` } });
									const attData = await attRes.json() as any;
									attachments = (attData.value || [])
										.filter((a: any) => a['@odata.type'] === '#microsoft.graph.fileAttachment' && a.contentBytes)
										.map((a: any) => ({ name: a.name, contentBytes: a.contentBytes, size: a.size || 0 }));
								} catch {}
							}
							for (const att of attachments) {
								const filed = await fileAttachmentToOneDrive(att, clientFolder, msg.subject, msg.from, caseMatch, peId, env, aiDocType);
								if (filed) attachmentsFiled++;
							}
						}
					}

					// Update processed_emails with counts
					await env.MEMORY_DB.prepare(
						'UPDATE processed_emails SET attachments_filed = ?, deadlines_extracted = ? WHERE id = ?'
					).bind(attachmentsFiled, deadlinesFound + cascadeCount, peId).run();

					// STEP 5: Mark email as READ so attorney knows it's been processed
					try {
						if (msg.source === 'outlook') {
							const token = await getGraphToken();
							await fetch(`https://graph.microsoft.com/v1.0/me/messages/${msg.id}`, {
								method: 'PATCH',
								headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
								body: JSON.stringify({ isRead: true })
							});
						} else if (msg.source === 'gmail' && msg.graphId) {
							// Gmail: remove UNREAD label
							const gToken = await getGmailToken();
							await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${msg.graphId}/modify`, {
								method: 'POST',
								headers: { 'Authorization': `Bearer ${gToken}`, 'Content-Type': 'application/json' },
								body: JSON.stringify({ removeLabelIds: ['UNREAD'] })
							});
						}
					} catch (markErr: any) {
						console.error(`[email-chunk] Failed to mark ${msg.id} as read:`, markErr.message);
					}

					return {
						id: msg.id, subject: msg.subject,
						client: caseMatch.client_name, case_number: caseMatch.case_number,
						matchedBy: matchSource, documentType: aiDocType || undefined,
						category: emailCategory,
						attachmentsFiled, deadlinesFound: deadlinesFound + cascadeCount,
						status: 'processed'
					};
				} catch (e: any) {
					console.error(`[email-chunk] Error processing ${msg.id}:`, e.message);
					try {
						await env.MEMORY_DB.prepare(
							`INSERT OR IGNORE INTO processed_emails (message_id, source, from_email, from_name, subject, received_date, processing_status, error_message, created_at) VALUES (?, ?, ?, ?, ?, ?, 'error', ?, ?)`
						).bind(msg.id, msg.source || 'outlook', msg.from || '', msg.fromName || '', (msg.subject || '').substring(0, 500), msg.receivedDateTime || '', e.message, mtnISO()).run();
					} catch {}
					return { id: msg.id, status: 'error', error: e.message };
				}
			}

			// --- classifyNonCaseEmail: Quick pattern-based category for non-case emails ---
			function classifyNonCaseEmail(subject: string, from: string, fromName: string): string {
				const s = (subject || '').toLowerCase();
				const f = (from || '').toLowerCase();
				if (/cle|continuing\s+legal|legal\s+education|mcle/i.test(s) || /utahbar\.org|nacle\.com|lawpracticecle/i.test(f)) return 'professional-development';
				if (/vonage|zoom|teams|webex|ringcentral/i.test(f) || /vonage|phone\s+system|voip/i.test(s)) return 'vendor-telecom';
				if (/courtlistener|free\s+law\s+project|pacer/i.test(f)) return 'legal-research-tool';
				if (/westlaw|lexis|casetext|fastcase/i.test(f)) return 'legal-research-tool';
				if (/newsletter|digest|update/i.test(s) && /bar\.org|legal/i.test(f)) return 'professional-development';
				if (/invoice|billing|payment|subscription/i.test(s)) return 'vendor-billing';
				if (/office\s*365|microsoft|adobe|dropbox|google/i.test(f)) return 'vendor-software';
				if (/unsubscribe|marketing|promo/i.test(s)) return 'marketing';
				return 'general';
			}

			// --- processEmailPipeline: PARALLEL orchestrator — fans out to concurrent sub-workers ---
			async function processEmailPipeline(
				source: 'outlook' | 'gmail' | 'both',
				hoursBack: number,
				env: Env,
				batchLimit: number = 5
			): Promise<{ totalProcessed: number; totalFiled: number; totalDeadlines: number; totalUnmatched: number; remaining: number; details: any[]; diag: string }> {
				// Safe migrations: ensure required schema changes exist
				try { await env.MEMORY_DB.prepare('ALTER TABLE processed_emails ADD COLUMN email_category TEXT DEFAULT NULL').run(); } catch {}
				try { await env.MEMORY_DB.prepare(`CREATE TABLE IF NOT EXISTS client_email_map (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL COLLATE NOCASE, client_name TEXT NOT NULL, case_number TEXT, relationship TEXT DEFAULT 'client', notes TEXT, created_at TEXT NOT NULL, UNIQUE(email, client_name))`).run(); } catch {}

				let outlookFetchDiag = '';

				// --- Phase 1: Fetch all emails ---
				let outlookMsgs: any[] = [];
				if (source === 'outlook' || source === 'both') {
					try {
						const token = await getGraphToken();
						if (!token) throw new Error('No Graph token available');
						const since = new Date(Date.now() - hoursBack * 60 * 60 * 1000).toISOString();
						const filterParam = encodeURIComponent(`receivedDateTime ge '${since}'`);
						const selectParam = encodeURIComponent('subject,from,toRecipients,receivedDateTime,bodyPreview,body,id,hasAttachments');
						const graphUrl = `https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$top=25&$filter=${filterParam}&$orderby=receivedDateTime desc&$select=${selectParam}`;
						const res = await fetch(graphUrl, { headers: { 'Authorization': `Bearer ${token}` } });
						const data = await res.json() as any;
						if (data.error) {
							const fallbackUrl = `https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$top=25&$orderby=receivedDateTime desc&$select=subject,from,toRecipients,receivedDateTime,bodyPreview,body,id,hasAttachments`;
							const fbRes = await fetch(fallbackUrl, { headers: { 'Authorization': `Bearer ${token}` } });
							const fbData = await fbRes.json() as any;
							outlookMsgs = (fbData.value || []).filter((e: any) => new Date(e.receivedDateTime) >= new Date(since))
								.map((e: any) => ({
									id: e.id, source: 'outlook', subject: e.subject || '',
									from: e.from?.emailAddress?.address || '', fromName: e.from?.emailAddress?.name || '',
									receivedDateTime: e.receivedDateTime, body: e.body?.content || '',
									bodyPreview: e.bodyPreview || '', hasAttachments: e.hasAttachments
								}));
							outlookFetchDiag = `fallback fetched ${outlookMsgs.length}`;
						} else {
							outlookMsgs = (data.value || []).map((e: any) => ({
								id: e.id, source: 'outlook', subject: e.subject || '',
								from: e.from?.emailAddress?.address || '', fromName: e.from?.emailAddress?.name || '',
								receivedDateTime: e.receivedDateTime, body: e.body?.content || '',
								bodyPreview: e.bodyPreview || '', hasAttachments: e.hasAttachments
							}));
							outlookFetchDiag = `fetched ${outlookMsgs.length} from Graph`;
						}
					} catch (e: any) {
						outlookFetchDiag = `Outlook error: ${e.message}`;
					}
				}

				let gmailMsgs: any[] = [];
				if (source === 'gmail' || source === 'both') {
					gmailMsgs = await fetchGmailMessages(hoursBack, env);
				}

				const allMessages = [...outlookMsgs, ...gmailMsgs];

				// --- Phase 2: Quick dedup against DB (batch check) ---
				const newMessages: any[] = [];
				for (const msg of allMessages) {
					const exists = await env.MEMORY_DB.prepare(
						'SELECT id FROM processed_emails WHERE message_id = ?'
					).bind(msg.id).first();
					if (!exists) newMessages.push(msg);
				}

				if (newMessages.length === 0) {
					return { totalProcessed: 0, totalFiled: 0, totalDeadlines: 0, totalUnmatched: 0, remaining: 0, details: [], diag: `${outlookFetchDiag} | 0 new emails` };
				}

				// --- Phase 3: Sequential processing in series ---
				// Process one email at a time to stay within CPU budget.
				// batchLimit controls how many per invocation. Caller re-invokes for more.
				const emailsThisBatch = newMessages.slice(0, batchLimit);
				const remaining = newMessages.length - emailsThisBatch.length;

				const results: any[] = [];
				let totalProcessed = 0, totalFiled = 0, totalDeadlines = 0, totalUnmatched = 0;

				// Process emails one-by-one in series — prevents CPU spikes from parallel AI+Graph calls
				for (const msg of emailsThisBatch) {
					try {
						const v = await processEmailChunk(msg, env);
						if (!v || v.status === 'skipped-dup') continue;
						totalProcessed++;
						if (v.status === 'unmatched') totalUnmatched++;
						if (v.attachmentsFiled) totalFiled += v.attachmentsFiled;
						if (v.deadlinesFound) totalDeadlines += v.deadlinesFound;
						results.push(v);
					} catch (e: any) {
						results.push({ id: msg.id, status: 'error', error: e.message });
					}
				}

				return {
					totalProcessed, totalFiled, totalDeadlines, totalUnmatched, remaining,
					details: results,
					diag: `${outlookFetchDiag} | ${newMessages.length} new, processed ${emailsThisBatch.length} serial, ${remaining} remaining`
				};
			}

			// ═══════════════════════════════════════════════════
			// RAID CPU ARCHITECTURE — Parallel Sub-Worker Fan-Out
			// ═══════════════════════════════════════════════════
			// Orchestrator fetches all emails, dedup checks, then dispatches
			// N parallel sub-workers via concurrent fetch() calls.
			// Each sub-worker = separate Worker invocation = fresh 30s CPU budget.
			// Like RAID striping: work is split across independent CPU lanes.

			// POST /api/email/process-single — Sub-worker: processes exactly ONE email
			// Called by the orchestrator. Each call is a separate Worker invocation.
			if (path === '/api/email/process-single' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const msg = body.message;
					if (!msg || !msg.id) return json({ success: false, error: 'message required' }, 400);

					// Safe migrations (idempotent)
					try { await env.MEMORY_DB.prepare('ALTER TABLE processed_emails ADD COLUMN email_category TEXT DEFAULT NULL').run(); } catch {}

					// Double-check dedup (another sub-worker may have grabbed it)
					const already = await env.MEMORY_DB.prepare('SELECT id FROM processed_emails WHERE message_id = ?').bind(msg.id).first();
					if (already) return json({ success: true, status: 'skipped-dup', id: msg.id });

					const result = await processEmailChunk(msg, env);
					return json({
						success: true,
						id: msg.id,
						status: result?.status || 'processed',
						client: result?.matchedClient || null,
						case_number: result?.matchedCase || null,
						attachments: result?.attachmentsFiled || 0,
						deadlines: result?.deadlinesFound || 0,
						category: result?.emailCategory || null,
					});
				} catch (e: any) {
					return json({ success: false, error: e.message, id: (await request.json().catch(() => ({} as any)))?.message?.id }, 500);
				}
			}

			// POST /api/email/process — RAID Orchestrator: fan-out to parallel sub-workers
			if (path === '/api/email/process' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const source = body.source || 'both';
					const hoursBack = Math.min(body.hours_back || 24, 168);
					const concurrency = Math.min(body.concurrency || 4, 6); // parallel sub-workers per wave (CF limit ~6 concurrent subrequests)
					const mode = body.mode || 'raid'; // 'raid' (parallel) or 'serial' (legacy)

					// --- LEGACY SERIAL MODE (fallback) ---
					if (mode === 'serial') {
						const batchLimit = Math.min(body.limit || 2, 10);
						const result = await processEmailPipeline(source, hoursBack, env, batchLimit);
						if (result.remaining > 0) {
							ctx.waitUntil((async () => {
								try {
									await fetch(`https://api.esqs-law.com/api/email/process`, {
										method: 'POST',
										headers: { 'Content-Type': 'application/json' },
										body: JSON.stringify({ source, hours_back: hoursBack, limit: batchLimit, mode: 'serial' })
									});
								} catch (e: any) { console.error('[email-serial] Auto-continue failed:', e.message); }
							})());
						}
						return json({
							success: true, mode: 'serial',
							summary: { total_processed: result.totalProcessed, attachments_filed: result.totalFiled, deadlines_extracted: result.totalDeadlines, unmatched: result.totalUnmatched, remaining: result.remaining, auto_continuing: result.remaining > 0 },
							details: result.details, diag: result.diag
						});
					}

					// --- RAID MODE (parallel sub-workers via fire-and-forget) ---
					// Architecture: Orchestrator does MINIMAL work (fetch emails, dedup, dispatch).
					// Sub-workers run as independent Worker invocations with their own CPU + subrequest budgets.
					// Results written to D1 by each sub-worker. Check /api/email/raid-status for results.
					//
					// Why fire-and-forget: CF Workers have a ~50 subrequest limit per invocation.
					// Fetching emails + dedup uses most of that budget. Sub-worker dispatches via
					// ctx.waitUntil run AFTER response, giving the orchestrator a clean exit.

					// Safe migrations
					try { await env.MEMORY_DB.prepare('ALTER TABLE processed_emails ADD COLUMN email_category TEXT DEFAULT NULL').run(); } catch {}
					try { await env.MEMORY_DB.prepare(`CREATE TABLE IF NOT EXISTS client_email_map (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL COLLATE NOCASE, client_name TEXT NOT NULL, case_number TEXT, relationship TEXT DEFAULT 'client', notes TEXT, created_at TEXT NOT NULL, UNIQUE(email, client_name))`).run(); } catch {}

					// Phase 1: Fetch emails (uses ~2-4 subrequests for Graph/Gmail API)
					let outlookMsgs: any[] = [];
					if (source === 'outlook' || source === 'both') {
						try {
							const token = await getGraphToken();
							const since = new Date(Date.now() - hoursBack * 60 * 60 * 1000).toISOString();
							const filterParam = encodeURIComponent(`receivedDateTime ge '${since}'`);
							const selectParam = encodeURIComponent('subject,from,toRecipients,receivedDateTime,bodyPreview,body,id,hasAttachments');
							const graphUrl = `https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$top=50&$filter=${filterParam}&$orderby=receivedDateTime desc&$select=${selectParam}`;
							const res = await fetch(graphUrl, { headers: { 'Authorization': `Bearer ${token}` } });
							const data = await res.json() as any;
							if (data.error) {
								const fbRes = await fetch(`https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$top=50&$orderby=receivedDateTime desc&$select=subject,from,toRecipients,receivedDateTime,bodyPreview,body,id,hasAttachments`, { headers: { 'Authorization': `Bearer ${token}` } });
								const fbData = await fbRes.json() as any;
								outlookMsgs = (fbData.value || []).filter((e: any) => new Date(e.receivedDateTime) >= new Date(since))
									.map((e: any) => ({ id: e.id, source: 'outlook', subject: e.subject || '', from: e.from?.emailAddress?.address || '', fromName: e.from?.emailAddress?.name || '', receivedDateTime: e.receivedDateTime, body: e.body?.content || '', bodyPreview: e.bodyPreview || '', hasAttachments: e.hasAttachments }));
							} else {
								outlookMsgs = (data.value || []).map((e: any) => ({ id: e.id, source: 'outlook', subject: e.subject || '', from: e.from?.emailAddress?.address || '', fromName: e.from?.emailAddress?.name || '', receivedDateTime: e.receivedDateTime, body: e.body?.content || '', bodyPreview: e.bodyPreview || '', hasAttachments: e.hasAttachments }));
							}
						} catch (e: any) { console.error('[raid-orchestrator] Outlook fetch error:', e.message); }
					}

					let gmailMsgs: any[] = [];
					if (source === 'gmail' || source === 'both') {
						try { gmailMsgs = await fetchGmailMessages(hoursBack, env); } catch (e: any) { console.error('[raid-orchestrator] Gmail fetch error:', e.message); }
					}

					const allMessages = [...outlookMsgs, ...gmailMsgs];

					// Phase 2: Batch dedup via single SQL query (1 subrequest instead of N)
					const msgIds = allMessages.map(m => m.id);
					const existingIds = new Set<string>();
					// Check in batches of 20 to stay within SQL parameter limits
					for (let i = 0; i < msgIds.length; i += 20) {
						const batch = msgIds.slice(i, i + 20);
						const placeholders = batch.map(() => '?').join(',');
						const { results } = await env.MEMORY_DB.prepare(
							`SELECT message_id FROM processed_emails WHERE message_id IN (${placeholders})`
						).bind(...batch).all();
						for (const r of results as any[]) existingIds.add(r.message_id);
					}
					const newMessages = allMessages.filter(m => !existingIds.has(m.id));

					if (newMessages.length === 0) {
						return json({ success: true, mode: 'raid', summary: { total_fetched: allMessages.length, total_new: 0, total_processed: 0 }, diag: `${allMessages.length} fetched, 0 new` });
					}

					// Generate a raid_batch_id for tracking this run
					const raidBatchId = `raid-${Date.now()}`;
					await env.CACHE.put(`raid:${raidBatchId}`, JSON.stringify({
						started: new Date().toISOString(),
						total: newMessages.length,
						dispatched: newMessages.length,
						concurrency,
					}), { expirationTtl: 3600 });

					// Phase 3: RAID chain-invoke pattern
					// Each wave runs in its OWN Worker invocation (fresh CPU + subrequest budget).
					// Orchestrator saves message queue to KV, then fires wave 0.
					// Each wave processes N emails via sub-worker fetches, then chains to wave N+1.
					const waves = Math.ceil(newMessages.length / concurrency);

					// Save message queue to KV for sub-waves to consume
					await env.CACHE.put(`raid:${raidBatchId}:queue`, JSON.stringify(newMessages), { expirationTtl: 3600 });

					// Process wave 0 directly (inline) — no self-fetch needed
					// Wave 0 runs in THIS invocation. It chains via ctx.waitUntil to
					// an EXTERNAL trigger that re-invokes us. But since CF blocks self-fetch,
					// we process as many as we can in this invocation, then return the batch_id
					// for the client to poll/continue.
					const wave0Emails = newMessages.slice(0, concurrency);
					const wave0Results: any[] = [];
					try { await env.MEMORY_DB.prepare('ALTER TABLE processed_emails ADD COLUMN email_category TEXT DEFAULT NULL').run(); } catch {}
					for (const msg of wave0Emails) {
						try {
							const already = await env.MEMORY_DB.prepare('SELECT id FROM processed_emails WHERE message_id = ?').bind(msg.id).first();
							if (already) { wave0Results.push({ id: msg.id, status: 'skipped-dup' }); continue; }
							const result = await processEmailChunk(msg, env);
							wave0Results.push({ id: msg.id, subject: (msg.subject || '').substring(0, 40), success: true, status: result?.status || 'processed', client: result?.matchedClient || null, attachments: result?.attachmentsFiled || 0, deadlines: result?.deadlinesFound || 0 });
						} catch (e: any) {
							wave0Results.push({ id: msg.id, success: false, error: e.message });
						}
					}
					// Update queue: remove processed emails
					const remaining0 = newMessages.slice(concurrency);
					if (remaining0.length > 0) {
						await env.CACHE.put(`raid:${raidBatchId}:queue`, JSON.stringify(remaining0), { expirationTtl: 3600 });
					} else {
						await env.CACHE.put(`raid:${raidBatchId}:complete`, 'true', { expirationTtl: 3600 });
					}

					const remainingCount = remaining0.length;
					return json({
						success: true,
						mode: 'raid',
						raid_batch_id: raidBatchId,
						summary: {
							total_fetched: allMessages.length,
							total_new: newMessages.length,
							wave0_processed: wave0Results.filter(r => r.success).length,
							remaining: remainingCount,
							waves_needed: Math.ceil(remainingCount / concurrency),
							concurrency,
							status: remainingCount > 0 ? 'processing' : 'complete',
						},
						wave0_results: wave0Results,
						message: remainingCount > 0
							? `Processed ${wave0Results.length} in wave 0. ${remainingCount} remaining. Call /api/email/raid-wave with batch_id=${raidBatchId} to continue.`
							: `All ${newMessages.length} emails processed in single wave.`,
					});
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// POST /api/email/raid-wave — RAID sub-wave: processes N emails in parallel, chains to next wave
			// Each call is a SEPARATE Worker invocation = fresh 30s CPU + 50 subrequest budget.
			// This is the RAID "stripe" — independent CPU lane.
			if (path === '/api/email/raid-wave' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const batchId = body.batch_id;
					const waveNum = body.wave || 0;
					const waveConcurrency = body.concurrency || 4;

					if (!batchId) return json({ success: false, error: 'batch_id required' }, 400);

					// Load message queue from KV
					const queueJson = await env.CACHE.get(`raid:${batchId}:queue`);
					if (!queueJson) return json({ success: false, error: 'Queue expired or not found' }, 404);

					const allMessages = JSON.parse(queueJson) as any[];
					const waveStart = waveNum * waveConcurrency;
					const waveEmails = allMessages.slice(waveStart, waveStart + waveConcurrency);
					const remainingWaves = Math.ceil((allMessages.length - waveStart - waveConcurrency) / waveConcurrency);

					if (waveEmails.length === 0) {
						await env.CACHE.put(`raid:${batchId}:complete`, 'true', { expirationTtl: 3600 });
						return json({ success: true, status: 'all-waves-complete', batch_id: batchId });
					}

					console.log(`[RAID-WAVE ${batchId}] Wave ${waveNum}: processing ${waveEmails.length} emails sequentially (${remainingWaves} waves remaining)`);

					// Safe migrations
					try { await env.MEMORY_DB.prepare('ALTER TABLE processed_emails ADD COLUMN email_category TEXT DEFAULT NULL').run(); } catch {}

					// Process emails in this wave sequentially (each wave = 1 Worker invocation)
					// CF blocks self-fetch (error 1033), so we call processEmailChunk directly.
					// Each WAVE is a separate invocation (chained via fetch to raid-wave).
					// Within a wave, we process N emails sequentially — fast because each email
					// gets full CPU and the wave chains give us unlimited total processing time.
					const waveResults: any[] = [];
					for (const msg of waveEmails) {
						try {
							const already = await env.MEMORY_DB.prepare('SELECT id FROM processed_emails WHERE message_id = ?').bind(msg.id).first();
							if (already) { waveResults.push({ id: msg.id, status: 'skipped-dup' }); continue; }

							const result = await processEmailChunk(msg, env);
							waveResults.push({
								id: msg.id,
								subject: (msg.subject || '').substring(0, 40),
								success: true,
								status: result?.status || 'processed',
								client: result?.matchedClient || null,
								attachments: result?.attachmentsFiled || 0,
								deadlines: result?.deadlinesFound || 0,
							});
						} catch (e: any) {
							waveResults.push({ id: msg.id, success: false, error: e.message });
						}
					}

					// Update queue: remove processed emails
					const queueRemaining = allMessages.slice(waveStart + waveConcurrency);
					if (queueRemaining.length > 0) {
						await env.CACHE.put(`raid:${batchId}:queue`, JSON.stringify(queueRemaining), { expirationTtl: 3600 });
					}

					if (remainingWaves <= 0) {
						await env.CACHE.put(`raid:${batchId}:complete`, 'true', { expirationTtl: 3600 });
						console.log(`[RAID ${batchId}] All waves complete.`);
					}

					return json({
						success: true,
						batch_id: batchId,
						wave: waveNum,
						processed_this_wave: waveEmails.length,
						remaining_waves: remainingWaves,
						results: waveResults,
					});
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/email/raid-status — Check status of a RAID batch
			if (path === '/api/email/raid-status' && request.method === 'GET') {
				try {
					const batchId = url.searchParams.get('batch_id') || '';
					if (!batchId) return json({ success: false, error: 'batch_id required' }, 400);

					const meta = await env.CACHE.get(`raid:${batchId}`);
					const complete = await env.CACHE.get(`raid:${batchId}:complete`);

					if (!meta) return json({ success: false, error: 'Batch not found or expired' }, 404);

					const batchMeta = JSON.parse(meta);

					// Count processed emails since raid started
					const { results } = await env.MEMORY_DB.prepare(
						`SELECT COUNT(*) as cnt, SUM(CASE WHEN processing_status = 'unmatched' THEN 1 ELSE 0 END) as unmatched, SUM(attachments_filed) as filed, SUM(deadlines_extracted) as deadlines FROM processed_emails WHERE created_at >= ?`
					).bind(batchMeta.started).all();
					const stats = (results as any[])[0] || {};

					return json({
						success: true,
						batch_id: batchId,
						status: complete ? 'complete' : 'processing',
						dispatched: batchMeta.total,
						concurrency: batchMeta.concurrency,
						started: batchMeta.started,
						processed: stats.cnt || 0,
						unmatched: stats.unmatched || 0,
						attachments_filed: stats.filed || 0,
						deadlines_extracted: stats.deadlines || 0,
						progress: `${stats.cnt || 0}/${batchMeta.total}`,
					});
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/email/debug-gmail/:messageId — Fetch raw Gmail message for debugging
			if (path.startsWith('/api/email/debug-gmail/') && request.method === 'GET') {
				try {
					const gmailId = path.split('/').pop();
					const token = await getGmailToken();
					const msgRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${gmailId}?format=full`, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const msgData = await msgRes.json() as any;
					const headers = msgData.payload?.headers || [];
					const getHeader = (name: string) => (headers.find((h: any) => h.name.toLowerCase() === name.toLowerCase()) || {}).value || '';
					let bodyText = '';
					const extractDbgBody = (payload: any): void => {
						if (!payload) return;
						if (payload.body?.data) {
							const decoded = atob(payload.body.data.replace(/-/g, '+').replace(/_/g, '/'));
							if (payload.mimeType === 'text/plain' && !bodyText) bodyText = decoded;
							else if (payload.mimeType === 'text/html' && !bodyText) bodyText = decoded;
						}
						if (payload.parts) {
							for (const part of payload.parts) {
								extractDbgBody(part);
								if (bodyText && payload.mimeType !== 'multipart/alternative') break;
							}
						}
					};
					extractDbgBody(msgData.payload);
					const emailsInBody = bodyText.match(/[\w.+-]+@[\w.-]+\.\w{2,}/g) || [];
					return json({
						subject: getHeader('Subject'),
						from: getHeader('From'),
						bodyLength: bodyText.length,
						bodyPreview: bodyText.substring(0, 2000),
						emailsFound: emailsInBody,
						partTypes: (msgData.payload?.parts || []).map((p: any) => p.mimeType)
					});
				} catch (e: any) {
					return err('Gmail debug error: ' + e.message);
				}
			}

			// POST /api/email/process-targeted — Process specific emails by search query, route to specified client
			if (path === '/api/email/process-targeted' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const searchQuery = body.search || '';
					const targetClient = body.client_name || '';
					const targetCase = body.case_number || 'PENDING';
					const maxResults = Math.min(body.limit || 30, 50);
					if (!searchQuery) return err('search query required', 400);
					if (!targetClient) return err('client_name required', 400);

					const token = await getGraphToken();
					// Paginate through all search results to collect enough emails
					const filterSender = (body.filter_sender || '').toLowerCase();
					let allFetched: any[] = [];
					const searchRes = await fetch(`https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$top=${Math.min(maxResults * 2, 50)}&$search="${encodeURIComponent(searchQuery)}"&$select=subject,from,toRecipients,receivedDateTime,body,id,hasAttachments`, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const searchData = await searchRes.json() as any;
					allFetched = searchData.value || [];
					let emails = allFetched;

					// Optional: filter by sender after fetch
					if (filterSender) {
						emails = emails.filter((e: any) => (e.from?.emailAddress?.address || '').toLowerCase() === filterSender);
					}

					// Optional: filter by subject keyword after fetch
					if (body.subject_contains) {
						const kw = body.subject_contains.toLowerCase();
						emails = emails.filter((e: any) => (e.subject || '').toLowerCase().includes(kw));
					}

					let totalProcessed = 0, totalFiled = 0, totalDeadlines = 0;
					const details: any[] = [];

					// Find client folder
					const clientFolder = await findClientFolder(targetClient, env);
					if (!clientFolder) return json({ success: false, error: `OneDrive folder not found for ${targetClient}` });

					for (const e of emails) {
						// Dedup check
						const already = await env.MEMORY_DB.prepare('SELECT id FROM processed_emails WHERE message_id = ?').bind(e.id).first();
						if (already) { details.push({ subject: e.subject, status: 'already_processed' }); continue; }

						totalProcessed++;
						const peResult = await env.MEMORY_DB.prepare(
							`INSERT INTO processed_emails (message_id, source, from_email, from_name, subject, received_date, matched_client, matched_case_number, processing_status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'processed', ?)`
						).bind(e.id, 'outlook', e.from?.emailAddress?.address || '', e.from?.emailAddress?.name || '', (e.subject || '').substring(0, 500), e.receivedDateTime || '', targetClient, targetCase, mtnISO()).run();
						const peId = peResult.meta.last_row_id || 0;

						// Extract deadlines
						const bodyText = e.body?.content || '';
						const deadlinesFound = await extractDeadlinesViaAI(e.subject, bodyText, { client_name: targetClient, case_number: targetCase }, peId, env);
						totalDeadlines += deadlinesFound;

						// File attachments
						let attachmentsFiled = 0;
						if (e.hasAttachments) {
							try {
								const attUrl = `https://graph.microsoft.com/v1.0/me/messages/${e.id}/attachments`;
								const attRes = await fetch(attUrl, { headers: { 'Authorization': `Bearer ${token}` } });
								const attData = await attRes.json() as any;
								const attachments = (attData.value || [])
									.filter((a: any) => a['@odata.type'] === '#microsoft.graph.fileAttachment' && a.contentBytes)
									.map((a: any) => ({ name: a.name, contentBytes: a.contentBytes, size: a.size || 0 }));

								for (const att of attachments) {
									const filed = await fileAttachmentToOneDrive(att, clientFolder, e.subject, e.from?.emailAddress?.address || '', { client_name: targetClient, case_number: targetCase }, peId, env);
									if (filed) attachmentsFiled++;
								}
							} catch (attErr: any) {
								console.error(`[targeted] Attachment error for ${e.id}:`, attErr.message);
							}
						}
						totalFiled += attachmentsFiled;

						await env.MEMORY_DB.prepare('UPDATE processed_emails SET attachments_filed = ?, deadlines_extracted = ? WHERE id = ?').bind(attachmentsFiled, deadlinesFound, peId).run();
						details.push({ subject: e.subject, client: targetClient, attachmentsFiled, deadlinesFound, status: 'processed' });
					}

					return json({
						success: true,
						summary: { total_found: emails.length, total_processed: totalProcessed, attachments_filed: totalFiled, deadlines_extracted: totalDeadlines },
						details
					});
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// GET /api/email/processed — View email processing history
			if (path === '/api/email/processed' && request.method === 'GET') {
				try {
					const url = new URL(request.url);
					const client = url.searchParams.get('client');
					const status = url.searchParams.get('status');
					const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);

					let q = 'SELECT * FROM processed_emails WHERE 1=1';
					const params: any[] = [];
					if (client) { q += ' AND LOWER(matched_client) LIKE ?'; params.push(`%${client.toLowerCase()}%`); }
					if (status) { q += ' AND processing_status = ?'; params.push(status); }
					q += ' ORDER BY created_at DESC LIMIT ?';
					params.push(limit);

					const stmt = env.MEMORY_DB.prepare(q);
					const rows = await stmt.bind(...params).all();

					return json({ success: true, emails: rows.results || [], count: (rows.results || []).length });
				} catch (e: any) {
					return json({ success: false, error: e.message }, 500);
				}
			}

			// POST /api/email/reprocess/:id — Manually assign unmatched email to client/case
			const reprocessMatch = path.match(/^\/api\/email\/reprocess\/(\d+)$/);
			if (reprocessMatch && request.method === 'POST') {
				try {
					const peId = parseInt(reprocessMatch[1]);
					const { client_name, case_number } = await request.json() as any;
					if (!client_name) return json({ success: false, error: 'client_name required' }, 400);

					// Get original email record
					const pe = await env.MEMORY_DB.prepare(
						'SELECT * FROM processed_emails WHERE id = ?'
					).bind(peId).first() as any;
					if (!pe) return json({ success: false, error: 'Email not found' }, 404);

					// Update the record
					await env.MEMORY_DB.prepare(
						'UPDATE processed_emails SET matched_client = ?, matched_case_number = ?, processing_status = ? WHERE id = ?'
					).bind(client_name, case_number || '', 'reprocessed', peId).run();

					const caseInfo = { client_name, case_number: case_number || '' };

					// Re-run deadline extraction + attachment filing (supports both Outlook and Gmail)
					let deadlinesFound = 0;
					let attachmentsFiled = 0;
					try {
						let emailBody = '';
						let hasAttachments = false;
						let attachments: any[] = [];

						if (pe.source === 'outlook') {
							const token = await getGraphToken();
							const msgRes = await fetch(`https://graph.microsoft.com/v1.0/me/messages/${pe.message_id}?$select=body,subject,hasAttachments,from`, {
								headers: { 'Authorization': `Bearer ${token}` }
							});
							const msgData = await msgRes.json() as any;
							emailBody = msgData.body?.content || '';
							hasAttachments = msgData.hasAttachments;

							if (hasAttachments) {
								const attUrl = `https://graph.microsoft.com/v1.0/me/messages/${pe.message_id}/attachments`;
								const attRes = await fetch(attUrl, { headers: { 'Authorization': `Bearer ${token}` } });
								const attData = await attRes.json() as any;
								attachments = (attData.value || [])
									.filter((a: any) => a['@odata.type'] === '#microsoft.graph.fileAttachment' && a.contentBytes)
									.map((a: any) => ({ name: a.name, contentBytes: a.contentBytes, size: a.size || 0 }));
							}
						} else if (pe.source === 'gmail') {
							// Gmail: strip 'gmail_' prefix to get original ID
							const gmailId = pe.message_id.replace(/^gmail_/, '');
							const gToken = await getGmailToken();
							const msgRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${gmailId}?format=full`, {
								headers: { 'Authorization': `Bearer ${gToken}` }
							});
							const msgData = await msgRes.json() as any;
							// Decode body
							if (msgData.payload?.body?.data) {
								emailBody = atob(msgData.payload.body.data.replace(/-/g, '+').replace(/_/g, '/'));
							} else if (msgData.payload?.parts) {
								for (const part of msgData.payload.parts) {
									if (part.mimeType === 'text/plain' && part.body?.data) {
										emailBody = atob(part.body.data.replace(/-/g, '+').replace(/_/g, '/'));
										break;
									}
									if (part.mimeType === 'text/html' && part.body?.data && !emailBody) {
										emailBody = atob(part.body.data.replace(/-/g, '+').replace(/_/g, '/'));
									}
								}
							}
							// Extract Gmail attachments
							const extractParts = (parts: any[]) => {
								for (const part of parts) {
									if (part.filename && part.body?.attachmentId) {
										attachments.push({ attachmentId: part.body.attachmentId, name: part.filename, size: part.body.size || 0 });
									}
									if (part.parts) extractParts(part.parts);
								}
							};
							if (msgData.payload?.parts) extractParts(msgData.payload.parts);
							hasAttachments = attachments.length > 0;
							// Fetch actual attachment bytes
							const resolvedAtts: any[] = [];
							for (const att of attachments) {
								try {
									const attRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${gmailId}/attachments/${att.attachmentId}`, { headers: { 'Authorization': `Bearer ${gToken}` } });
									const attData = await attRes.json() as any;
									if (attData.data) {
										resolvedAtts.push({ name: att.name, contentBytes: attData.data.replace(/-/g, '+').replace(/_/g, '/'), size: att.size });
									}
								} catch {}
							}
							attachments = resolvedAtts;
						}

						// Extract deadlines
						if (emailBody) {
							deadlinesFound = await extractDeadlinesViaAI(pe.subject, emailBody, caseInfo, peId, env);
						}

						// File attachments
						if (hasAttachments && attachments.length > 0) {
							const clientFolder = await findClientFolder(client_name, env);
							if (clientFolder) {
								for (const a of attachments) {
									const result = await fileAttachmentToOneDrive(a, clientFolder, pe.subject, pe.from_email || '', caseInfo, peId, env);
									if (result) attachmentsFiled++;
								}
							}
						}

						await env.MEMORY_DB.prepare('UPDATE processed_emails SET attachments_filed = attachments_filed + ?, deadlines_extracted = deadlines_extracted + ? WHERE id = ?').bind(attachmentsFiled, deadlinesFound, peId).run();
					} catch (e: any) {
						console.error('[reprocess] Error:', e.message);
					}

					return json({ success: true, message: `Email reassigned to ${client_name}`, deadlines_extracted: deadlinesFound, attachments_filed: attachmentsFiled });
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
					// Search for folders containing client name — use last name (not middle) to avoid collisions
					let folderQ = `mimeType = 'application/vnd.google-apps.folder' and trashed = false`;
					if (nameParts.length > 0) {
						// Use last part for 3+ word names (FIRST MIDDLE LAST), first part otherwise (Last, First)
						const searchPart = nameParts.length >= 3 ? nameParts[nameParts.length - 1] : nameParts[0];
						folderQ += ` and name contains '${searchPart.replace(/'/g, "\\'")}'`;
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
			// DEADLINE TIMELINE ENGINE — Cascade, backward timeline, reminders
			// ═══════════════════════════════════════════════════════════════

			// POST /api/deadlines/cascade — Manual cascade trigger
			if (path === '/api/deadlines/cascade' && request.method === 'POST') {
				try {
					const { trigger_event, trigger_date, client_name, case_number, case_type, service_type } = await request.json() as any;
					if (!trigger_event || !trigger_date || !client_name) return json({ success: false, error: 'trigger_event, trigger_date, client_name required' }, 400);
					const result = await cascadeDeadlinesFromEvent(
						trigger_event, trigger_date,
						{ client_name, case_number: case_number || '', case_type: case_type || '' },
						service_type || 'electronic', env
					);
					return json({ success: true, created: result.created, deadlines: result.deadlines });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/deadlines/timeline — Build backward timeline from anchor date
			if (path === '/api/deadlines/timeline' && request.method === 'POST') {
				try {
					const { anchor_date, anchor_event, client_name, case_number, case_type, service_type, save } = await request.json() as any;
					if (!anchor_date || !anchor_event) return json({ success: false, error: 'anchor_date, anchor_event required' }, 400);
					const timeline = await buildBackwardTimeline(
						anchor_date, anchor_event,
						{ client_name: client_name || '', case_number: case_number || '', case_type: case_type || '' },
						service_type || 'electronic', env
					);
					let saved = 0;
					if (save && client_name && case_number) {
						const cascadeGroup = crypto.randomUUID();
						for (const item of timeline) {
							if (item.direction === 'anchor') continue;
							// Dedup
							const dup = await env.MEMORY_DB.prepare(
								`SELECT id FROM deadlines WHERE case_number = ? AND due_date = ? AND description LIKE ? AND status IN ('active','pending') LIMIT 1`
							).bind(case_number, item.due_date, `%${item.deadline_name}%`).first();
							if (dup) continue;
							let reminderDays = '7,3,1,0';
							if (item.days_offset >= 28) reminderDays = '14,7,3,1,0';
							await env.MEMORY_DB.prepare(
								`INSERT INTO deadlines (client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, status, source, notes, created_at, cascade_group, trigger_event, service_type, rule_source, rule_number, reminder_days) VALUES (?, ?, ?, ?, ?, '', '', '', '', '', 'active', 'auto-cascade', ?, ?, ?, ?, ?, ?, ?, ?)`
							).bind(
								client_name, case_number, item.deadline_name,
								`${item.deadline_name}${item.extended ? ` (extended: ${item.reason})` : ''}`,
								item.due_date,
								`Backward timeline from ${anchor_event} on ${anchor_date}. ${item.rule}`,
								mtnISO(), cascadeGroup, anchor_event, service_type || 'electronic',
								item.rule.split(' ')[0] || '', item.rule.split(' ').slice(1).join(' ') || '', reminderDays
							).run();
							saved++;
						}
					}
					return json({ success: true, timeline, count: timeline.length, saved });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/deadlines/reminders — View reminder send history
			if (path === '/api/deadlines/reminders' && request.method === 'GET') {
				try {
					const deadlineId = url.searchParams.get('deadline_id');
					const limit = Math.min(parseInt(url.searchParams.get('limit') || '100'), 500);
					let q = 'SELECT * FROM deadline_reminders_sent';
					const params: any[] = [];
					if (deadlineId) { q += ' WHERE deadline_id = ?'; params.push(parseInt(deadlineId)); }
					q += ' ORDER BY sent_at DESC LIMIT ?';
					params.push(limit);
					const { results } = await env.MEMORY_DB.prepare(q).bind(...params).all();
					return json({ success: true, reminders: results, count: results?.length || 0 });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// PUT /api/deadlines/:id/reminders — Update reminder preferences for a deadline
			const reminderPrefMatch = path.match(/^\/api\/deadlines\/(\d+)\/reminders$/);
			if (reminderPrefMatch && request.method === 'PUT') {
				try {
					const dlId = parseInt(reminderPrefMatch[1]);
					const { reminder_days } = await request.json() as any;
					if (!reminder_days) return json({ success: false, error: 'reminder_days required (e.g. "14,7,3,1,0")' }, 400);
					// Validate format
					if (!/^[\d,]+$/.test(reminder_days)) return json({ success: false, error: 'reminder_days must be comma-separated numbers' }, 400);
					await env.MEMORY_DB.prepare('UPDATE deadlines SET reminder_days = ? WHERE id = ?').bind(reminder_days, dlId).run();
					return json({ success: true, message: `Reminder days updated to: ${reminder_days}` });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/deadlines/cascade-log — View cascade audit trail
			if (path === '/api/deadlines/cascade-log' && request.method === 'GET') {
				try {
					const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);
					const caseNum = url.searchParams.get('case_number');
					let q = 'SELECT * FROM deadline_cascade_log';
					const params: any[] = [];
					if (caseNum) { q += ' WHERE case_number = ?'; params.push(caseNum); }
					q += ' ORDER BY created_at DESC LIMIT ?';
					params.push(limit);
					const { results } = await env.MEMORY_DB.prepare(q).bind(...params).all();
					return json({ success: true, log: results, count: results?.length || 0 });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ═══════════════════════════════════════════════════════════════
			// ZOOM + GOOGLE CALENDAR API
			// ═══════════════════════════════════════════════════════════════

			// POST /api/zoom/meeting — Create a Zoom meeting
			if (path === '/api/zoom/meeting' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const topic = body.topic || (body.clientName ? `Meeting — ${body.clientName}` : 'Pitcher Law Meeting');
					const startTime = body.startTime || new Date().toISOString();
					const duration = body.duration || 30;
					const meeting = await createZoomMeeting(topic, startTime, duration);
					return json({ success: true, meeting });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/calendar/google — List Google Calendar events
			if (path === '/api/calendar/google' && request.method === 'GET') {
				try {
					const timeMin = url.searchParams.get('timeMin') || new Date().toISOString().split('T')[0];
					const dMax = new Date(); dMax.setDate(dMax.getDate() + 14);
					const timeMax = url.searchParams.get('timeMax') || dMax.toISOString().split('T')[0];
					const events = await listGoogleCalendarEvents(timeMin, timeMax);
					return json({ success: true, events, count: events.length });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/calendar/google — Create Google Calendar event (with optional auto-Zoom)
			if (path === '/api/calendar/google' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					if (!body.summary || !body.start || !body.end) {
						return json({ success: false, error: 'Required: summary, start, end' }, 400);
					}
					const calEvent = await createGoogleCalendarEvent({
						summary: body.summary,
						start: body.start,
						end: body.end,
						description: body.description || '',
						location: body.location || '',
					});

					// Auto-attach Zoom if not a hearing/intake AND autoZoom not explicitly false
					let zoomMeeting = null;
					if (body.autoZoom !== false && !isCourtOrIntakeEvent(body.summary)) {
						try {
							zoomMeeting = await createZoomMeeting(body.summary, body.start, body.duration || 30);
							// Update the calendar event with Zoom link
							await updateGoogleCalendarEvent(calEvent.id, {
								location: zoomMeeting.join_url,
								description: (body.description || '') + `\n\n🔗 Zoom Meeting\nJoin: ${zoomMeeting.join_url}\nPassword: ${zoomMeeting.password}`,
							});
						} catch (zErr: any) {
							console.error('Auto-Zoom attach failed:', zErr.message);
						}
					}
					return json({ success: true, event: calEvent, zoom: zoomMeeting });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/calendar/backfill-deadlines — Push existing D1 deadlines to Google Calendar
			if (path === '/api/calendar/backfill-deadlines' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					const minDate = body.min_date || new Date().toISOString().split('T')[0];
					const maxDate = body.max_date || (() => { const d = new Date(); d.setMonth(d.getMonth() + 6); return d.toISOString().split('T')[0]; })();
					const { results: deadlines } = await env.MEMORY_DB.prepare(
						`SELECT d.*, pe.matched_client, pe.matched_case_number
						 FROM email_extracted_deadlines d
						 LEFT JOIN processed_emails pe ON d.processed_email_id = pe.id
						 WHERE d.due_date >= ? AND d.due_date <= ?
						 ORDER BY d.due_date`
					).bind(minDate, maxDate).all();

					const created: any[] = [];
					const errors: any[] = [];
					for (const dl of deadlines as any[]) {
						try {
							const clientName = dl.matched_client || dl.client_name || 'Unknown';
							const caseNum = dl.matched_case_number || dl.case_number || '';
							const isAppointment = /hearing|trial|arraignment|sentencing|conference|plea/i.test(dl.deadline_type || '');
							const isFollowUp = /follow_up|email_response|document_due|review_deadline|conference_call|nlt_deadline/i.test(dl.deadline_type || '');
							const calTitle = dl.calendar_title || `${(dl.deadline_type || 'deadline').replace(/_/g, ' ').replace(/\b\w/g, (c: string) => c.toUpperCase())} - ${clientName.split(' ').pop()} (${caseNum})`;

							let startStr = dl.due_date;
							let endStr = dl.due_date;
							if (dl.due_time && /^\d{2}:\d{2}/.test(dl.due_time)) {
								startStr = `${dl.due_date}T${dl.due_time}:00`;
								const dur = isAppointment ? 60 * 60 * 1000 : 30 * 60 * 1000;
								const endDate = new Date(new Date(`${startStr}-07:00`).getTime() + dur);
								const ehrs = endDate.getUTCHours() - 7;
								endStr = `${dl.due_date}T${String(ehrs < 0 ? ehrs + 24 : ehrs).padStart(2, '0')}:${String(endDate.getUTCMinutes()).padStart(2, '0')}:00`;
							}

							let colorId: string | undefined;
							if (isFollowUp) colorId = '9';
							else if (isAppointment) colorId = '7';
							else if (/nlt_deadline|filing_deadline|answer_due|response_due/i.test(dl.deadline_type || '')) colorId = '11';

							const desc = [
								`Case: ${caseNum}`,
								`Client: ${clientName}`,
								dl.deadline_type ? `Type: ${dl.deadline_type}` : '',
								dl.notes ? `Notes: ${dl.notes}` : '',
								dl.court ? `Court: ${dl.court}` : '',
								dl.judge ? `Judge: ${dl.judge}` : '',
								dl.virtual_link ? `Join: ${dl.virtual_link}` : '',
							].filter(Boolean).join('\n');

							const calEvent = await createGoogleCalendarEvent({
								summary: calTitle, start: startStr, end: endStr,
								description: desc, location: dl.virtual_link || dl.court || '', colorId,
							});
							created.push({ id: dl.id, title: calTitle, date: dl.due_date, calEventId: calEvent?.id });
						} catch (err: any) {
							errors.push({ id: dl.id, error: err.message });
						}
					}
					return json({ success: true, total_deadlines: deadlines.length, created: created.length, errors: errors.length, created_events: created, error_details: errors });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// PUT /api/calendar/google/:eventId — Update a Google Calendar event
			if (path.startsWith('/api/calendar/google/') && request.method === 'PUT') {
				try {
					const eventId = path.split('/api/calendar/google/')[1];
					if (!eventId) return json({ success: false, error: 'Missing eventId' }, 400);
					const body = await request.json() as any;

					// If addZoom=true, create a Zoom meeting and attach it
					if (body.addZoom) {
						const summary = body.summary || 'Pitcher Law Meeting';
						const startTime = body.start || new Date().toISOString();
						const zoomMeeting = await createZoomMeeting(summary, startTime, body.duration || 30);
						body.location = zoomMeeting.join_url;
						body.description = (body.description || '') + `\n\n🔗 Zoom Meeting\nJoin: ${zoomMeeting.join_url}\nPassword: ${zoomMeeting.password}`;
						delete body.addZoom;
						delete body.duration;
						const updated = await updateGoogleCalendarEvent(eventId, body);
						return json({ success: true, event: updated, zoom: zoomMeeting });
					}

					const updated = await updateGoogleCalendarEvent(eventId, body);
					return json({ success: true, event: updated });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ═══════════════════════════════════════════════════════════════
			// WORD ADD-IN ENDPOINTS — Synthia in Word
			// ═══════════════════════════════════════════════════════════════

			if (path.startsWith('/api/word-addin/')) {
				// Auth check for all word-addin endpoints
				const wAuth = request.headers.get('Authorization');
				if (!wAuth?.startsWith('Bearer ')) return err('Unauthorized', 401);
				const wSession = await env.SESSIONS.get(wAuth.substring(7));
				if (!wSession) return err('Invalid session', 401);
				const wUser = JSON.parse(wSession);

				// --- Auth validation ---
				if (path === '/api/word-addin/auth' && request.method === 'POST') {
					return json({ authenticated: true, user: wUser.email || wUser.name || 'staff' });
				}

				// --- Research: CourtListener search + AI fan-out ---
				if (path === '/api/word-addin/research' && request.method === 'POST') {
					try {
						const body = await request.json() as any;
						const query = (body.query || '').trim().substring(0, 500);
						const jurisdiction = (body.jurisdiction || '').trim();
						if (!query) return err('query required', 400);

						// CourtListener search with timeout
						let clResults: any[] = [];
						if (env.COURTLISTENER_API_TOKEN) {
							const abort = new AbortController();
							const timer = setTimeout(() => abort.abort(), 12000);
							let searchUrl = `https://www.courtlistener.com/api/rest/v4/search/?q=${encodeURIComponent(query)}&type=o&page_size=8`;
							if (jurisdiction) searchUrl += `&court=${encodeURIComponent(jurisdiction)}`;
							try {
								const res = await fetch(searchUrl, {
									headers: { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` },
									signal: abort.signal
								});
								if (res.ok) {
									const data = await res.json() as any;
									clResults = (data.results || []).slice(0, 8).map((r: any) => ({
										case_name: r.caseName || r.case_name || '',
										citation: Array.isArray(r.citation) ? (r.citation[0] || '') : (r.citation || ''),
										court: r.court || '',
										date_filed: r.dateFiled || r.date_filed || '',
										url: r.absolute_url ? `https://www.courtlistener.com${r.absolute_url}` : '',
										snippet: (r.text || r.snippet || '').substring(0, 300).replace(/<[^>]+>/g, '')
									}));
								}
							} catch (e: any) { if (e.name !== 'AbortError') console.warn('CL search error:', e.message); }
							finally { clearTimeout(timer); }
						}

						// Quick AI synthesis via Claude
						let aiSynthesis = '';
						if (env.ANTHROPIC_API_KEY) {
							try {
								let clContext = '';
								if (clResults.length > 0) {
									clContext = '\n\nVerified case law from CourtListener:\n' + clResults.map(c => `- ${c.case_name}, ${c.citation} (${c.court}, ${c.date_filed}): ${c.snippet}`).join('\n');
								}
								const r = await fetch('https://api.anthropic.com/v1/messages', {
									method: 'POST',
									headers: { 'Content-Type': 'application/json', 'x-api-key': env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
									body: JSON.stringify({
										model: 'claude-sonnet-4-20250514', max_tokens: 2000, temperature: 0,
										system: 'You are a legal research assistant for a Utah law firm. Provide concise, citation-backed analysis. Use the CourtListener results as verified authority.',
										messages: [{ role: 'user', content: query + clContext }]
									})
								});
								const d = await r.json() as any;
								aiSynthesis = d.content?.[0]?.text || '';
							} catch { /* non-critical */ }
						}

						return json({ success: true, courtlistener: clResults, aiSynthesis, sources: clResults.length + (aiSynthesis ? 1 : 0) });
					} catch (e: any) { return json({ success: false, error: e.message }, 500); }
				}

				// --- Verify all citations in document text ---
				if (path === '/api/word-addin/verify-document' && request.method === 'POST') {
					try {
						const body = await request.json() as any;
						const text = body.text || '';
						if (!text) return err('text required', 400);
						if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener not configured', 500);

						const report = await verifyCitationsCourtListener(text, env.COURTLISTENER_API_TOKEN, env);

						// Shepardize valid citations (up to 5)
						const shepardized: ShepardizeResult[] = [];
						for (const vc of report.validCitations.slice(0, 5)) {
							try {
								const sr = await shepardize(vc.citation, env.COURTLISTENER_API_TOKEN);
								shepardized.push(sr);
							} catch { /* skip individual failures */ }
						}

						return json({ success: true, report, shepardized });
					} catch (e: any) { return json({ success: false, error: e.message }, 500); }
				}

				// --- Suggest citations for a legal argument ---
				if (path === '/api/word-addin/suggest-citations' && request.method === 'POST') {
					try {
						const body = await request.json() as any;
						const argument = body.argument || '';
						const jurisdiction = body.jurisdiction || '';
						if (!argument) return err('argument required', 400);
						if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener not configured', 500);

						// Use Workers AI to extract search queries from the argument
						const topicsRaw = await env.AI.run('@cf/meta/llama-3.1-8b-instruct', {
							messages: [
								{ role: 'system', content: 'Extract 2-3 concise legal search queries from this argument. Return ONLY the queries, one per line. No numbering, no explanation.' },
								{ role: 'user', content: argument }
							], max_tokens: 150
						}) as any;
						const topics = (topicsRaw.response || '').split('\n').filter((t: string) => t.trim().length > 5).slice(0, 3);

						// Search all topics in parallel with timeout
						const topicResults = await Promise.all(topics.map(async (topic: string) => {
							const abort = new AbortController();
							const timer = setTimeout(() => abort.abort(), 10000);
							try {
								let searchUrl = `https://www.courtlistener.com/api/rest/v4/search/?q=${encodeURIComponent(topic.trim())}&type=o&page_size=5`;
								if (jurisdiction) searchUrl += `&court=${encodeURIComponent(jurisdiction)}`;
								const res = await fetch(searchUrl, {
									headers: { 'Authorization': `Token ${env.COURTLISTENER_API_TOKEN}` },
									signal: abort.signal
								});
								if (!res.ok) return [];
								const data = await res.json() as any;
								return (data.results || []).slice(0, 5).map((r: any) => ({
									case_name: r.caseName || r.case_name || '',
									citation: Array.isArray(r.citation) ? (r.citation[0] || '') : (r.citation || ''),
									court: r.court || '',
									date_filed: r.dateFiled || r.date_filed || '',
									url: r.absolute_url ? `https://www.courtlistener.com${r.absolute_url}` : '',
									snippet: (r.text || r.snippet || '').substring(0, 300).replace(/<[^>]+>/g, ''),
									search_topic: topic.trim()
								}));
							} catch { return []; }
							finally { clearTimeout(timer); }
						}));

						// Deduplicate by citation
						const seen = new Set<string>();
						const allResults: any[] = [];
						for (const batch of topicResults) {
							for (const r of batch) {
								if (r.citation && !seen.has(r.citation)) {
									seen.add(r.citation);
									allResults.push(r);
								}
							}
						}

						return json({ success: true, suggestions: allResults.slice(0, 10), topics });
					} catch (e: any) { return json({ success: false, error: e.message }, 500); }
				}

				// --- Chat with Synthia in document context ---
				if (path === '/api/word-addin/chat' && request.method === 'POST') {
					try {
						const body = await request.json() as any;
						const message = body.message || '';
						const documentContext = body.documentContext || '';
						const selectedText = body.selectedText || '';
						if (!message) return err('message required', 400);

						const docContextBlock = documentContext
							? `\n\n## Current Document (user is editing in Word):\n${documentContext.substring(0, 6000)}`
							: '';
						const selectionBlock = selectedText
							? `\n\n## User's Selected Text:\n${selectedText}`
							: '';

						const systemPrompt = `You are Synthia, an AI legal assistant for Pitcher Law PLLC (Utah). The user is drafting a document in Microsoft Word and asking for help via the Synthia Word Add-in.

Your role: Provide concise, citation-backed legal assistance. When suggesting case law, prefer verified citations. When asked to draft text, format it for direct insertion into a legal document (numbered paragraphs, Bluebook citations, formal tone).

RULES:
- ZERO TOLERANCE FOR HALLUCINATION — if unsure about a citation, say so
- Utah jurisdiction primary (URCP, URCrimP, URAP)
- Bluebook citation format
- Be direct and actionable — the user is mid-draft${docContextBlock}${selectionBlock}`;

						let response = '';
						if (env.ANTHROPIC_API_KEY) {
							try {
								const r = await fetch('https://api.anthropic.com/v1/messages', {
									method: 'POST',
									headers: { 'Content-Type': 'application/json', 'x-api-key': env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
									body: JSON.stringify({
										model: 'claude-sonnet-4-20250514', max_tokens: 3000, temperature: 0,
										system: systemPrompt,
										messages: [{ role: 'user', content: message }]
									})
								});
								const d = await r.json() as any;
								response = d.content?.[0]?.text || '';
							} catch { /* fallback below */ }
						}
						if (!response && env.XAI_API_KEY) {
							try {
								const r = await fetch('https://api.x.ai/v1/chat/completions', {
									method: 'POST',
									headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.XAI_API_KEY}` },
									body: JSON.stringify({
										model: 'grok-3', temperature: 0, max_tokens: 3000,
										messages: [{ role: 'system', content: systemPrompt }, { role: 'user', content: message }]
									})
								});
								const d = await r.json() as any;
								response = d.choices?.[0]?.message?.content || '';
							} catch { /* */ }
						}
						if (!response) return err('AI services unavailable', 503);

						// Verify citations in response
						let citations: CitationResult | null = null;
						if (env.COURTLISTENER_API_TOKEN && containsCitations(response)) {
							try {
								citations = await verifyCitationsCourtListener(response, env.COURTLISTENER_API_TOKEN, env);
								if (citations.overallResult !== 'pass') {
									response = annotateResponse(response, citations);
								}
							} catch { /* non-critical */ }
						}

						return json({ success: true, response, citations });
					} catch (e: any) { return json({ success: false, error: e.message }, 500); }
				}

				// --- Shepardize a specific citation ---
				if (path === '/api/word-addin/shepardize' && request.method === 'POST') {
					try {
						const body = await request.json() as any;
						const citation = body.citation || '';
						if (!citation) return err('citation required', 400);
						if (!env.COURTLISTENER_API_TOKEN) return err('CourtListener not configured', 500);
						const result = await shepardize(citation, env.COURTLISTENER_API_TOKEN);
						return json({ success: true, ...result });
					} catch (e: any) { return json({ success: false, error: e.message }, 500); }
				}

				return err(`Word add-in endpoint not found: ${path}`, 404);
			}

			// ═══════════════════════════════════════════════════════════════
			// CLIENT ENGAGEMENT & COMMUNICATION SYSTEM
			// Templates, email queue, communication log, preferences, intake
			// ═══════════════════════════════════════════════════════════════

			// ─── Email Templates ─────────────────────────────────────────

			// GET /api/email-templates — List all active templates
			if (path === '/api/email-templates' && request.method === 'GET') {
				try {
					const category = url.searchParams.get('category');
					let q = 'SELECT * FROM email_templates WHERE is_active = 1';
					const binds: any[] = [];
					if (category) { q += ' AND category = ?'; binds.push(category); }
					q += ' ORDER BY category, name';
					const rows = binds.length > 0
						? await env.MEMORY_DB.prepare(q).bind(...binds).all()
						: await env.MEMORY_DB.prepare(q).all();
					return json({ success: true, templates: rows.results || [] });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/email-templates/:id — Single template
			if (path.startsWith('/api/email-templates/') && !path.includes('/preview') && request.method === 'GET') {
				try {
					const id = path.split('/api/email-templates/')[1];
					const row = await env.MEMORY_DB.prepare('SELECT * FROM email_templates WHERE id = ?').bind(id).first();
					if (!row) return err('Template not found', 404);
					return json({ success: true, template: row });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/email-templates — Create template
			if (path === '/api/email-templates' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					if (!body.id || !body.name || !body.category || !body.subject_template || !body.body_template) {
						return json({ success: false, error: 'id, name, category, subject_template, body_template required' }, 400);
					}
					await env.MEMORY_DB.prepare(
						`INSERT INTO email_templates (id, name, category, subject_template, body_template, description, variables, is_active, created_by, created_at)
						VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`
					).bind(body.id, body.name, body.category, body.subject_template, body.body_template,
						body.description || null, body.variables || null, body.created_by || 'JWA3', new Date().toISOString()
					).run();
					return json({ success: true, id: body.id });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// PUT /api/email-templates/:id — Update template
			if (path.startsWith('/api/email-templates/') && !path.includes('/preview') && request.method === 'PUT') {
				try {
					const id = path.split('/api/email-templates/')[1];
					const body = await request.json() as any;
					const sets: string[] = [];
					const binds: any[] = [];
					for (const field of ['name', 'category', 'subject_template', 'body_template', 'description', 'variables', 'is_active']) {
						if (body[field] !== undefined) { sets.push(`${field} = ?`); binds.push(body[field]); }
					}
					if (sets.length === 0) return json({ success: false, error: 'No fields to update' }, 400);
					sets.push('updated_at = ?'); binds.push(new Date().toISOString());
					binds.push(id);
					await env.MEMORY_DB.prepare(`UPDATE email_templates SET ${sets.join(', ')} WHERE id = ?`).bind(...binds).run();
					return json({ success: true });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/email-templates/:id/preview — Render with variables
			if (path.match(/^\/api\/email-templates\/[^/]+\/preview$/) && request.method === 'POST') {
				try {
					const id = path.split('/api/email-templates/')[1].replace('/preview', '');
					const template = await env.MEMORY_DB.prepare('SELECT * FROM email_templates WHERE id = ?').bind(id).first() as any;
					if (!template) return err('Template not found', 404);
					const { variables } = await request.json() as any;
					const vars = variables || {};
					const renderedSubject = renderTemplate(template.subject_template, vars);
					const renderedBody = renderTemplate(template.body_template, vars);
					const html = wrapHtmlEmail(renderedBody.replace(/\n/g, '<br>'));
					return json({ success: true, subject: renderedSubject, body: renderedBody, html });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ─── Email Queue ─────────────────────────────────────────────

			// GET /api/email-queue — List drafts (filterable by status)
			if (path === '/api/email-queue' && request.method === 'GET') {
				try {
					const status = url.searchParams.get('status') || 'draft';
					const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);
					const rows = await env.MEMORY_DB.prepare(
						'SELECT * FROM email_queue WHERE status = ? ORDER BY created_at DESC LIMIT ?'
					).bind(status, limit).all();
					return json({ success: true, queue: rows.results || [] });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/email-queue — Queue email from template + variables
			if (path === '/api/email-queue' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					if (!body.client_name || !body.to_address) {
						return json({ success: false, error: 'client_name and to_address required' }, 400);
					}
					let subject = body.subject || '';
					let emailBody = body.body || '';

					// If template_id provided, render it
					if (body.template_id) {
						const template = await env.MEMORY_DB.prepare('SELECT * FROM email_templates WHERE id = ?').bind(body.template_id).first() as any;
						if (template) {
							const vars = body.variables || {};
							vars.client_name = vars.client_name || body.client_name;
							subject = renderTemplate(template.subject_template, vars);
							emailBody = wrapHtmlEmail(renderTemplate(template.body_template, vars).replace(/\n/g, '<br>'));
						}
					}

					if (!subject) return json({ success: false, error: 'subject required (directly or via template)' }, 400);

					const result = await env.MEMORY_DB.prepare(
						`INSERT INTO email_queue (client_name, case_number, to_address, cc_address, subject, body, template_id, trigger_type, trigger_id, status, send_via, created_at)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?)`
					).bind(
						body.client_name, body.case_number || null, body.to_address, body.cc_address || null,
						subject, emailBody, body.template_id || null, body.trigger_type || null,
						body.trigger_id || null, body.send_via || 'graph', new Date().toISOString()
					).run();
					return json({ success: true, id: result.meta?.last_row_id });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/email-queue/:id/approve — Attorney approves → sends + logs
			if (path.match(/^\/api\/email-queue\/\d+\/approve$/) && request.method === 'POST') {
				try {
					const queueId = parseInt(path.split('/api/email-queue/')[1].split('/')[0]);
					const item = await env.MEMORY_DB.prepare('SELECT * FROM email_queue WHERE id = ? AND status = ?').bind(queueId, 'draft').first() as any;
					if (!item) return err('Queue item not found or already processed', 404);

					const body = await request.json().catch(() => ({})) as any;
					const approver = body?.approved_by || 'JWA3';

					// Send the email
					let sendResult: any;
					if (item.send_via === 'gmail') {
						sendResult = await sendViaGmail(item.to_address, item.subject, item.body, item.cc_address || undefined);
					} else {
						// Graph API
						const token = await getGraphToken();
						const toRecipients = item.to_address.split(',').map((e: string) => ({ emailAddress: { address: e.trim() } }));
						const ccRecipients = item.cc_address ? item.cc_address.split(',').map((e: string) => ({ emailAddress: { address: e.trim() } })) : [];
						const mailBody = {
							message: {
								subject: item.subject,
								body: { contentType: 'HTML', content: item.body },
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
						sendResult = { success: sendRes.status === 202 || sendRes.status === 200 };
					}

					if (sendResult?.success) {
						const now = new Date().toISOString();
						await env.MEMORY_DB.prepare(
							`UPDATE email_queue SET status = 'sent', approved_by = ?, approved_at = ?, sent_at = ? WHERE id = ?`
						).bind(approver, now, now, queueId).run();

						// Log to communication_log
						await logCommunication({
							client_name: item.client_name, case_number: item.case_number,
							direction: 'outbound', channel: 'email', subject: item.subject,
							body_preview: item.body, from_address: item.send_via === 'gmail' ? 'esqslaw@gmail.com' : 'Associate@dianepitcher.com',
							to_address: item.to_address, source: 'queue', template_id: item.template_id,
							status: 'sent', sent_by: approver
						});
						return json({ success: true, sent: true });
					} else {
						await env.MEMORY_DB.prepare(`UPDATE email_queue SET status = 'failed' WHERE id = ?`).bind(queueId).run();
						return json({ success: false, error: sendResult?.error || 'Send failed' }, 500);
					}
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/email-queue/:id/edit — Modify draft before sending
			if (path.match(/^\/api\/email-queue\/\d+\/edit$/) && request.method === 'POST') {
				try {
					const queueId = parseInt(path.split('/api/email-queue/')[1].split('/')[0]);
					const body = await request.json() as any;
					const sets: string[] = [];
					const binds: any[] = [];
					for (const field of ['subject', 'body', 'to_address', 'cc_address', 'send_via']) {
						if (body[field] !== undefined) { sets.push(`${field} = ?`); binds.push(body[field]); }
					}
					if (sets.length === 0) return json({ success: false, error: 'No fields to update' }, 400);
					binds.push(queueId);
					await env.MEMORY_DB.prepare(`UPDATE email_queue SET ${sets.join(', ')} WHERE id = ? AND status = 'draft'`).bind(...binds).run();
					return json({ success: true });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// DELETE /api/email-queue/:id — Cancel/discard
			if (path.match(/^\/api\/email-queue\/\d+$/) && request.method === 'DELETE') {
				try {
					const queueId = parseInt(path.split('/api/email-queue/')[1]);
					await env.MEMORY_DB.prepare(`UPDATE email_queue SET status = 'cancelled' WHERE id = ? AND status = 'draft'`).bind(queueId).run();
					return json({ success: true });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/email-queue/bulk-approve — Approve + send multiple
			if (path === '/api/email-queue/bulk-approve' && request.method === 'POST') {
				try {
					const { ids, approved_by } = await request.json() as any;
					if (!ids || !Array.isArray(ids) || ids.length === 0) return json({ success: false, error: 'ids array required' }, 400);
					const results: any[] = [];
					for (const id of ids.slice(0, 20)) {
						try {
							const item = await env.MEMORY_DB.prepare('SELECT * FROM email_queue WHERE id = ? AND status = ?').bind(id, 'draft').first() as any;
							if (!item) { results.push({ id, success: false, error: 'not found' }); continue; }

							let sendResult: any;
							if (item.send_via === 'gmail') {
								sendResult = await sendViaGmail(item.to_address, item.subject, item.body, item.cc_address || undefined);
							} else {
								const token = await getGraphToken();
								const toRecipients = item.to_address.split(',').map((e: string) => ({ emailAddress: { address: e.trim() } }));
								const sendRes = await fetch('https://graph.microsoft.com/v1.0/me/sendMail', {
									method: 'POST',
									headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
									body: JSON.stringify({
										message: {
											subject: item.subject,
											body: { contentType: 'HTML', content: item.body },
											toRecipients,
											from: { emailAddress: { address: 'Associate@dianepitcher.com', name: 'Pitcher Law PLLC' } }
										}, saveToSentItems: true
									})
								});
								sendResult = { success: sendRes.status === 202 || sendRes.status === 200 };
							}

							const now = new Date().toISOString();
							if (sendResult?.success) {
								await env.MEMORY_DB.prepare(`UPDATE email_queue SET status = 'sent', approved_by = ?, approved_at = ?, sent_at = ? WHERE id = ?`)
									.bind(approved_by || 'JWA3', now, now, id).run();
								await logCommunication({
									client_name: item.client_name, case_number: item.case_number,
									direction: 'outbound', channel: 'email', subject: item.subject,
									body_preview: item.body, to_address: item.to_address,
									source: 'queue', template_id: item.template_id,
									status: 'sent', sent_by: approved_by || 'JWA3'
								});
								results.push({ id, success: true });
							} else {
								await env.MEMORY_DB.prepare(`UPDATE email_queue SET status = 'failed' WHERE id = ?`).bind(id).run();
								results.push({ id, success: false, error: sendResult?.error });
							}
						} catch (e: any) { results.push({ id, success: false, error: e.message }); }
					}
					return json({ success: true, results, sent: results.filter(r => r.success).length, failed: results.filter(r => !r.success).length });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ─── Communication Log ───────────────────────────────────────

			// GET /api/communications — List by client/case
			if (path === '/api/communications' && request.method === 'GET') {
				try {
					const client = url.searchParams.get('client');
					const caseNum = url.searchParams.get('case_number');
					const channel = url.searchParams.get('channel');
					const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);
					let q = 'SELECT * FROM communication_log WHERE 1=1';
					const binds: any[] = [];
					if (client) { q += ' AND client_name LIKE ?'; binds.push(`%${client}%`); }
					if (caseNum) { q += ' AND case_number = ?'; binds.push(caseNum); }
					if (channel) { q += ' AND channel = ?'; binds.push(channel); }
					q += ' ORDER BY created_at DESC LIMIT ?';
					binds.push(limit);
					const rows = await env.MEMORY_DB.prepare(q).bind(...binds).all();
					return json({ success: true, communications: rows.results || [] });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/communications — Log client contact (call, meeting, email, text)
			// Serves 3 purposes: (1) timesheet billing, (2) malpractice defense, (3) client intelligence
			if (path === '/api/communications' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					if (!body.client_name || !body.channel) return json({ success: false, error: 'client_name and channel required' }, 400);
					await logCommunication({
						client_name: body.client_name, case_number: body.case_number,
						direction: body.direction || 'outbound', channel: body.channel,
						subject: body.subject, body_preview: body.body_preview || body.notes,
						from_address: body.from_address, to_address: body.to_address,
						source: 'manual', sent_by: body.sent_by || 'JWA3', notes: body.notes,
						// Timesheet fields
						duration_minutes: body.duration_minutes || body.duration || 0,
						billable: body.billable !== false,
						attorney: body.attorney || 'JWA3',
						// Malpractice defense fields
						advice_given: !!body.advice_given,
						follow_up_required: !!body.follow_up_required,
						follow_up_date: body.follow_up_date,
						interaction_summary: body.interaction_summary || body.summary,
						// Client intelligence
						client_sentiment: body.client_sentiment || body.sentiment
					});
					return json({ success: true });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/communications/timeline/:clientName — Full timeline
			if (path.startsWith('/api/communications/timeline/') && request.method === 'GET') {
				try {
					const clientName = decodeURIComponent(path.split('/api/communications/timeline/')[1]);
					const rows = await env.MEMORY_DB.prepare(
						'SELECT * FROM communication_log WHERE client_name LIKE ? ORDER BY created_at DESC LIMIT 100'
					).bind(`%${clientName}%`).all();
					return json({ success: true, timeline: rows.results || [], client: clientName });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ─── Client Profiles (Personality + Intelligence) ────────────

			// GET /api/client-profiles — List all profiles
			if (path === '/api/client-profiles' && request.method === 'GET') {
				try {
					const rows = await env.MEMORY_DB.prepare('SELECT * FROM client_profiles ORDER BY last_contact_date DESC').all();
					return json({ success: true, profiles: rows.results || [] });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/client-profiles/:clientName — Get profile
			if (path.startsWith('/api/client-profiles/') && request.method === 'GET') {
				try {
					const clientName = decodeURIComponent(path.split('/api/client-profiles/')[1]);
					const row = await env.MEMORY_DB.prepare('SELECT * FROM client_profiles WHERE client_name LIKE ?').bind(`%${clientName}%`).first();
					if (!row) return json({ success: true, profile: null, message: 'No profile yet — create one with PUT' });
					return json({ success: true, profile: row });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// PUT /api/client-profiles/:clientName — Create/update profile
			if (path.startsWith('/api/client-profiles/') && request.method === 'PUT') {
				try {
					const clientName = decodeURIComponent(path.split('/api/client-profiles/')[1]);
					const body = await request.json() as any;
					const now = new Date().toISOString();
					await env.MEMORY_DB.prepare(
						`INSERT INTO client_profiles (client_name, preferred_name, communication_style, personality_notes, key_concerns, risk_factors, language, special_needs, relationship_quality, emotional_tendencies, decision_style, trust_level, family_context, occupation, important_dates, created_by, created_at, updated_at)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
						ON CONFLICT(client_name) DO UPDATE SET
						preferred_name = COALESCE(excluded.preferred_name, preferred_name),
						communication_style = COALESCE(excluded.communication_style, communication_style),
						personality_notes = CASE WHEN excluded.personality_notes IS NOT NULL THEN COALESCE(personality_notes || '\n---\n', '') || excluded.personality_notes ELSE personality_notes END,
						key_concerns = COALESCE(excluded.key_concerns, key_concerns),
						risk_factors = COALESCE(excluded.risk_factors, risk_factors),
						language = COALESCE(excluded.language, language),
						special_needs = COALESCE(excluded.special_needs, special_needs),
						relationship_quality = COALESCE(excluded.relationship_quality, relationship_quality),
						emotional_tendencies = COALESCE(excluded.emotional_tendencies, emotional_tendencies),
						decision_style = COALESCE(excluded.decision_style, decision_style),
						trust_level = COALESCE(excluded.trust_level, trust_level),
						family_context = COALESCE(excluded.family_context, family_context),
						occupation = COALESCE(excluded.occupation, occupation),
						important_dates = COALESCE(excluded.important_dates, important_dates),
						updated_at = excluded.updated_at`
					).bind(
						clientName, body.preferred_name || null, body.communication_style || null,
						body.personality_notes || null, body.key_concerns || null, body.risk_factors || null,
						body.language || 'English', body.special_needs || null,
						body.relationship_quality || 'good', body.emotional_tendencies || null,
						body.decision_style || null, body.trust_level || 'developing',
						body.family_context || null, body.occupation || null,
						body.important_dates || null, body.created_by || 'JWA3', now, now
					).run();
					return json({ success: true });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ─── Timesheet Export ────────────────────────────────────────

			// GET /api/timesheet — Billable contacts for date range
			if (path === '/api/timesheet' && request.method === 'GET') {
				try {
					const from = url.searchParams.get('from') || new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];
					const to = url.searchParams.get('to') || new Date().toISOString().split('T')[0];
					const client = url.searchParams.get('client');
					const attorney = url.searchParams.get('attorney');

					let q = `SELECT client_name, case_number, channel, subject, duration_minutes, billable, attorney, advice_given, interaction_summary, client_sentiment, created_at FROM communication_log WHERE billable = 1 AND duration_minutes > 0 AND created_at >= ? AND created_at <= ?`;
					const binds: any[] = [from, to + 'T23:59:59Z'];
					if (client) { q += ' AND client_name LIKE ?'; binds.push(`%${client}%`); }
					if (attorney) { q += ' AND attorney = ?'; binds.push(attorney); }
					q += ' ORDER BY created_at DESC';

					const rows = await env.MEMORY_DB.prepare(q).bind(...binds).all();
					const entries = (rows.results || []) as any[];
					const totalMinutes = entries.reduce((sum: number, e: any) => sum + (e.duration_minutes || 0), 0);
					const totalHours = Math.round(totalMinutes / 6) / 10; // Round to nearest 0.1

					return json({
						success: true,
						entries,
						summary: {
							total_entries: entries.length,
							total_minutes: totalMinutes,
							total_hours: totalHours,
							date_range: { from, to },
							by_client: entries.reduce((acc: any, e: any) => {
								const key = e.client_name;
								if (!acc[key]) acc[key] = { minutes: 0, entries: 0 };
								acc[key].minutes += e.duration_minutes || 0;
								acc[key].entries++;
								return acc;
							}, {})
						}
					});
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ─── Follow-Up Tracker ───────────────────────────────────────

			// GET /api/follow-ups — Outstanding follow-ups (malpractice defense)
			if (path === '/api/follow-ups' && request.method === 'GET') {
				try {
					const rows = await env.MEMORY_DB.prepare(
						`SELECT id, client_name, case_number, channel, subject, interaction_summary, follow_up_date, attorney, advice_given, created_at
						FROM communication_log WHERE follow_up_required = 1
						AND id NOT IN (SELECT CAST(notes AS INTEGER) FROM communication_log WHERE source = 'follow-up-resolved' AND notes IS NOT NULL)
						ORDER BY COALESCE(follow_up_date, '9999') ASC, created_at DESC LIMIT 100`
					).all();
					return json({ success: true, follow_ups: rows.results || [] });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/follow-ups/:id/resolve — Mark follow-up as completed
			if (path.match(/^\/api\/follow-ups\/\d+\/resolve$/) && request.method === 'POST') {
				try {
					const id = parseInt(path.split('/api/follow-ups/')[1].split('/')[0]);
					const body = await request.json().catch(() => ({})) as any;
					// Log the resolution as a new comm entry referencing the original
					await logCommunication({
						client_name: body.client_name || 'unknown',
						direction: 'outbound', channel: body.channel || 'note',
						subject: `Follow-up resolved: ${body.subject || ''}`,
						interaction_summary: body.resolution_notes || 'Follow-up completed',
						source: 'follow-up-resolved', notes: String(id),
						attorney: body.attorney || 'JWA3'
					});
					return json({ success: true });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ─── Malpractice Defense Export ───────────────────────────────

			// GET /api/malpractice-log/:clientName — Full defense-grade contact history
			if (path.startsWith('/api/malpractice-log/') && request.method === 'GET') {
				try {
					const clientName = decodeURIComponent(path.split('/api/malpractice-log/')[1]);
					const comms = await env.MEMORY_DB.prepare(
						`SELECT id, client_name, case_number, direction, channel, subject, body_preview, interaction_summary, duration_minutes, attorney, advice_given, follow_up_required, follow_up_date, client_sentiment, created_at
						FROM communication_log WHERE client_name LIKE ? ORDER BY created_at ASC`
					).bind(`%${clientName}%`).all();
					const profile = await env.MEMORY_DB.prepare(
						'SELECT * FROM client_profiles WHERE client_name LIKE ?'
					).bind(`%${clientName}%`).first();

					const entries = (comms.results || []) as any[];
					const adviceCount = entries.filter((e: any) => e.advice_given).length;
					const followUps = entries.filter((e: any) => e.follow_up_required).length;
					const totalMinutes = entries.reduce((sum: number, e: any) => sum + (e.duration_minutes || 0), 0);

					return json({
						success: true,
						client: clientName,
						profile: profile || null,
						communications: entries,
						defense_summary: {
							total_contacts: entries.length,
							total_time_minutes: totalMinutes,
							advice_documented: adviceCount,
							follow_ups_created: followUps,
							channels_used: [...new Set(entries.map((e: any) => e.channel))],
							date_range: entries.length > 0 ? { first: entries[0].created_at, last: entries[entries.length - 1].created_at } : null,
							sentiments_recorded: entries.filter((e: any) => e.client_sentiment).map((e: any) => ({ date: e.created_at, sentiment: e.client_sentiment }))
						}
					});
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ─── Client Preferences ──────────────────────────────────────

			// GET /api/client-preferences — List all
			if (path === '/api/client-preferences' && request.method === 'GET') {
				try {
					const rows = await env.MEMORY_DB.prepare('SELECT * FROM client_comm_preferences ORDER BY client_name').all();
					return json({ success: true, preferences: rows.results || [] });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/client-preferences/:clientName — Get prefs for client
			if (path.startsWith('/api/client-preferences/') && request.method === 'GET') {
				try {
					const clientName = decodeURIComponent(path.split('/api/client-preferences/')[1]);
					const rows = await env.MEMORY_DB.prepare(
						'SELECT * FROM client_comm_preferences WHERE client_name LIKE ?'
					).bind(`%${clientName}%`).all();
					return json({ success: true, preferences: rows.results || [] });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// PUT /api/client-preferences/:clientName — Set/update prefs
			if (path.startsWith('/api/client-preferences/') && request.method === 'PUT') {
				try {
					const clientName = decodeURIComponent(path.split('/api/client-preferences/')[1]);
					const body = await request.json() as any;
					const now = new Date().toISOString();
					await env.MEMORY_DB.prepare(
						`INSERT INTO client_comm_preferences (client_name, case_number, preferred_email, email_frequency, send_hearing_reminders, send_filing_updates, send_deadline_alerts, send_case_resolution, notes, updated_by, created_at, updated_at)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
						ON CONFLICT(client_name, case_number) DO UPDATE SET
						preferred_email = COALESCE(excluded.preferred_email, preferred_email),
						email_frequency = COALESCE(excluded.email_frequency, email_frequency),
						send_hearing_reminders = COALESCE(excluded.send_hearing_reminders, send_hearing_reminders),
						send_filing_updates = COALESCE(excluded.send_filing_updates, send_filing_updates),
						send_deadline_alerts = COALESCE(excluded.send_deadline_alerts, send_deadline_alerts),
						send_case_resolution = COALESCE(excluded.send_case_resolution, send_case_resolution),
						notes = COALESCE(excluded.notes, notes),
						updated_by = excluded.updated_by, updated_at = excluded.updated_at`
					).bind(
						clientName, body.case_number || '', body.preferred_email || null,
						body.email_frequency || 'normal',
						body.send_hearing_reminders !== undefined ? (body.send_hearing_reminders ? 1 : 0) : 1,
						body.send_filing_updates !== undefined ? (body.send_filing_updates ? 1 : 0) : 1,
						body.send_deadline_alerts !== undefined ? (body.send_deadline_alerts ? 1 : 0) : 0,
						body.send_case_resolution !== undefined ? (body.send_case_resolution ? 1 : 0) : 1,
						body.notes || null, body.updated_by || 'JWA3', now, now
					).run();
					return json({ success: true });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ─── Intake ──────────────────────────────────────────────────

			// POST /api/intake — Submit new intake
			if (path === '/api/intake' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					if (!body.first_name || !body.last_name) return json({ success: false, error: 'first_name and last_name required' }, 400);
					const result = await env.MEMORY_DB.prepare(
						`INSERT INTO intake_submissions (first_name, last_name, email, phone, address, city, state, zip, case_type, description, referral_source, preferred_contact, status, assigned_attorney, notes, created_at)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)`
					).bind(
						body.first_name.trim(), body.last_name.trim(), body.email || null, body.phone || null,
						body.address || null, body.city || null, body.state || 'UT', body.zip || null,
						body.case_type || null, body.description || null, body.referral_source || null,
						body.preferred_contact || 'email', body.assigned_attorney || null, body.notes || null,
						new Date().toISOString()
					).run();
					return json({ success: true, id: result.meta?.last_row_id });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intake — List submissions
			if (path === '/api/intake' && request.method === 'GET') {
				try {
					const status = url.searchParams.get('status');
					let q = 'SELECT * FROM intake_submissions';
					const binds: any[] = [];
					if (status) { q += ' WHERE status = ?'; binds.push(status); }
					q += ' ORDER BY created_at DESC LIMIT 100';
					const rows = binds.length > 0
						? await env.MEMORY_DB.prepare(q).bind(...binds).all()
						: await env.MEMORY_DB.prepare(q).all();
					return json({ success: true, submissions: rows.results || [] });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/intake/:id — Single submission
			if (path.match(/^\/api\/intake\/\d+$/) && request.method === 'GET') {
				try {
					const id = parseInt(path.split('/api/intake/')[1]);
					const row = await env.MEMORY_DB.prepare('SELECT * FROM intake_submissions WHERE id = ?').bind(id).first();
					if (!row) return err('Intake submission not found', 404);
					return json({ success: true, submission: row });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/intake/:id/convert — Convert → client + welcome email queued
			if (path.match(/^\/api\/intake\/\d+\/convert$/) && request.method === 'POST') {
				try {
					const id = parseInt(path.split('/api/intake/')[1].split('/')[0]);
					const submission = await env.MEMORY_DB.prepare('SELECT * FROM intake_submissions WHERE id = ? AND status = ?').bind(id, 'pending').first() as any;
					if (!submission) return err('Intake not found or already processed', 404);

					const body = await request.json().catch(() => ({})) as any;
					const clientName = `${submission.last_name}, ${submission.first_name}`;
					const now = new Date().toISOString();

					// 1. Create client record
					await env.MEMORY_DB.prepare(
						`INSERT OR IGNORE INTO clients (name, email, phone, address, city, state, zip, notes, status, created_at)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)`
					).bind(
						clientName, submission.email, submission.phone, submission.address,
						submission.city, submission.state, submission.zip,
						`Intake: ${submission.case_type || 'General'}. ${submission.description || ''}`.trim(), now
					).run();

					// 2. Set default comm preferences
					if (submission.email) {
						await env.MEMORY_DB.prepare(
							`INSERT OR IGNORE INTO client_comm_preferences (client_name, case_number, preferred_email, email_frequency, send_hearing_reminders, send_filing_updates, send_deadline_alerts, send_case_resolution, updated_by, created_at, updated_at)
							VALUES (?, '', ?, 'normal', 1, 1, 0, 1, 'JWA3', ?, ?)`
						).bind(clientName, submission.email, now, now).run();
					}

					// 3. Queue welcome email if email exists
					if (submission.email) {
						const welcomeTemplate = await env.MEMORY_DB.prepare('SELECT * FROM email_templates WHERE id = ?').bind('welcome').first() as any;
						if (welcomeTemplate) {
							const vars: Record<string, string> = {
								client_name: submission.first_name,
								attorney_name: body.assigned_attorney || 'our team',
								case_type: submission.case_type || 'your matter'
							};
							const subject = renderTemplate(welcomeTemplate.subject_template, vars);
							const emailBody = wrapHtmlEmail(renderTemplate(welcomeTemplate.body_template, vars).replace(/\n/g, '<br>'));
							await env.MEMORY_DB.prepare(
								`INSERT INTO email_queue (client_name, case_number, to_address, subject, body, template_id, trigger_type, trigger_id, status, send_via, created_at)
								VALUES (?, '', ?, ?, ?, 'welcome', 'intake_convert', ?, 'draft', 'graph', ?)`
							).bind(clientName, submission.email, subject, emailBody, String(id), now).run();
						}
					}

					// 4. Update intake status
					await env.MEMORY_DB.prepare(
						`UPDATE intake_submissions SET status = 'converted', assigned_attorney = ?, reviewed_by = ?, reviewed_at = ? WHERE id = ?`
					).bind(body.assigned_attorney || 'JWA3', body.reviewed_by || 'JWA3', now, id).run();

					return json({ success: true, client_name: clientName, welcome_queued: !!submission.email });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/intake/:id/decline — Decline with optional note
			if (path.match(/^\/api\/intake\/\d+\/decline$/) && request.method === 'POST') {
				try {
					const id = parseInt(path.split('/api/intake/')[1].split('/')[0]);
					const body = await request.json().catch(() => ({})) as any;
					const now = new Date().toISOString();
					await env.MEMORY_DB.prepare(
						`UPDATE intake_submissions SET status = 'declined', notes = COALESCE(notes || ' | ', '') || ?, reviewed_by = ?, reviewed_at = ? WHERE id = ? AND status = 'pending'`
					).bind(body.reason || 'Declined', body.reviewed_by || 'JWA3', now, id).run();
					return json({ success: true });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ─── Client Status Updates ───────────────────────────────────

			// POST /api/client-updates/hearing-reminders — Batch-queue reminders for hearings N days out
			if (path === '/api/client-updates/hearing-reminders' && request.method === 'POST') {
				try {
					const { days_out } = await request.json().catch(() => ({ days_out: 3 })) as any;
					const targetDate = new Date();
					targetDate.setDate(targetDate.getDate() + (days_out || 3));
					const targetStr = targetDate.toISOString().split('T')[0];

					// Find hearings on target date
					const hearings = await env.MEMORY_DB.prepare(
						`SELECT d.*, cs.client_email FROM deadlines d
						LEFT JOIN case_summaries cs ON d.case_number = cs.case_number
						WHERE d.due_date = ? AND d.status IN ('active','pending')
						AND d.deadline_type IN ('hearing','Hearing','evidentiary hearing','review hearing','OSC','status conference','pretrial','trial')
						ORDER BY d.due_date`
					).bind(targetStr).all();

					let queued = 0;
					let skipped = 0;
					const template = await env.MEMORY_DB.prepare('SELECT * FROM email_templates WHERE id = ?').bind('hearing-reminder').first() as any;
					if (!template) return json({ success: false, error: 'hearing-reminder template not found' }, 400);

					for (const h of (hearings.results || []) as any[]) {
						if (!h.client_email && !h.client_name) { skipped++; continue; }

						// Check preferences
						const prefs = await env.MEMORY_DB.prepare(
							'SELECT send_hearing_reminders, preferred_email FROM client_comm_preferences WHERE client_name = ? LIMIT 1'
						).bind(h.client_name).first() as any;
						if (prefs && !prefs.send_hearing_reminders) { skipped++; continue; }

						const email = prefs?.preferred_email || h.client_email;
						if (!email) { skipped++; continue; }

						// Check if already queued
						const existing = await env.MEMORY_DB.prepare(
							`SELECT id FROM email_queue WHERE trigger_type = 'hearing_reminder' AND trigger_id = ? AND status IN ('draft','sent')`
						).bind(String(h.id)).first();
						if (existing) { skipped++; continue; }

						const vars: Record<string, string> = {
							client_name: (h.client_name || '').split(',')[0]?.trim() || 'Client',
							hearing_date: h.due_date,
							hearing_time: h.hearing_time || 'TBD',
							hearing_type: h.deadline_type || 'Hearing',
							court: h.court || '',
							courtroom: h.courtroom || '',
							judge: h.judge || ''
						};
						const subject = renderTemplate(template.subject_template, vars);
						const emailBody = wrapHtmlEmail(renderTemplate(template.body_template, vars).replace(/\n/g, '<br>'));

						await env.MEMORY_DB.prepare(
							`INSERT INTO email_queue (client_name, case_number, to_address, subject, body, template_id, trigger_type, trigger_id, status, send_via, created_at)
							VALUES (?, ?, ?, ?, ?, 'hearing-reminder', 'hearing_reminder', ?, 'draft', 'graph', ?)`
						).bind(h.client_name, h.case_number, email, subject, emailBody, String(h.id), new Date().toISOString()).run();
						queued++;
					}
					return json({ success: true, queued, skipped, target_date: targetStr });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// POST /api/client-updates/status-change — Queue status update for specific client
			if (path === '/api/client-updates/status-change' && request.method === 'POST') {
				try {
					const body = await request.json() as any;
					if (!body.client_name || !body.to_address) return json({ success: false, error: 'client_name and to_address required' }, 400);

					const template = await env.MEMORY_DB.prepare('SELECT * FROM email_templates WHERE id = ?').bind('status-update').first() as any;
					if (!template) return json({ success: false, error: 'status-update template not found' }, 400);

					const vars: Record<string, string> = {
						client_name: body.client_name.split(',')[0]?.trim() || body.client_name,
						case_number: body.case_number || '',
						status_summary: body.status_summary || body.message || ''
					};
					const subject = renderTemplate(template.subject_template, vars);
					const emailBody = wrapHtmlEmail(renderTemplate(template.body_template, vars).replace(/\n/g, '<br>'));

					const result = await env.MEMORY_DB.prepare(
						`INSERT INTO email_queue (client_name, case_number, to_address, subject, body, template_id, trigger_type, status, send_via, created_at)
						VALUES (?, ?, ?, ?, ?, 'status-update', 'status_change', 'draft', 'graph', ?)`
					).bind(body.client_name, body.case_number || null, body.to_address, subject, emailBody, new Date().toISOString()).run();
					return json({ success: true, id: result.meta?.last_row_id });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// ═══════════════════════════════════════════════════════════════
			// PERSONAL ONEDRIVE — Separate from work OneDrive (same app, different user)
			// ═══════════════════════════════════════════════════════════════

			// GET /api/personal-onedrive/oauth — Start OAuth flow for personal Microsoft account
			if (path === '/api/personal-onedrive/oauth' && request.method === 'GET') {
				const redirectUri = 'https://api.esqs-law.com/api/personal-onedrive/oauth/callback';
				const authUrl = `https://login.microsoftonline.com/common/oauth2/v2.0/authorize?${new URLSearchParams({
					client_id: env.MICROSOFT_CLIENT_ID,
					response_type: 'code',
					redirect_uri: redirectUri,
					scope: 'https://graph.microsoft.com/User.Read https://graph.microsoft.com/Files.Read https://graph.microsoft.com/Files.Read.All offline_access',
					access_type: 'offline',
					prompt: 'consent',
				})}`;
				return Response.redirect(authUrl, 302);
			}

			// GET /api/personal-onedrive/oauth/callback — Exchange code for tokens
			if (path === '/api/personal-onedrive/oauth/callback' && request.method === 'GET') {
				const code = url.searchParams.get('code');
				if (!code) return err('No code returned', 400);
				try {
					const tokenRes = await fetch('https://login.microsoftonline.com/common/oauth2/v2.0/token', {
						method: 'POST',
						headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
						body: new URLSearchParams({
							client_id: env.MICROSOFT_CLIENT_ID,
							client_secret: env.MICROSOFT_CLIENT_SECRET,
							code,
							redirect_uri: 'https://api.esqs-law.com/api/personal-onedrive/oauth/callback',
							grant_type: 'authorization_code',
						})
					});
					const tokenData = await tokenRes.json() as any;
					if (tokenData.refresh_token) {
						if (tokenData.access_token) {
							await env.CACHE.put('personal_ms_graph_token', tokenData.access_token, { expirationTtl: 3000 });
						}
						await env.CACHE.put('personal_ms_refresh_token_backup', tokenData.refresh_token);
						return json({
							success: true,
							message: 'Personal OneDrive connected! Access token cached. Save refresh token as secret.',
							command: `echo "${tokenData.refresh_token}" | npx wrangler secret put PERSONAL_MS_REFRESH_TOKEN`,
							refresh_token: tokenData.refresh_token,
							access_token_cached: !!tokenData.access_token,
							scopes: tokenData.scope,
						});
					}
					return json({ success: false, error: 'No refresh token returned', data: tokenData }, 400);
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/personal-onedrive/status — Check connection status
			if (path === '/api/personal-onedrive/status' && request.method === 'GET') {
				try {
					const token = await getPersonalGraphToken();
					const profileRes = await fetch('https://graph.microsoft.com/v1.0/me', {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const profile = await profileRes.json() as any;
					return json({
						success: true,
						connected: true,
						account: profile.displayName || profile.userPrincipalName || 'Unknown',
						email: profile.mail || profile.userPrincipalName || null,
					});
				} catch (e: any) {
					return json({ success: true, connected: false, error: e.message });
				}
			}

			// GET /api/personal-onedrive/list — Browse root or folder
			if (path === '/api/personal-onedrive/list' && request.method === 'GET') {
				try {
					const folderId = url.searchParams.get('folder_id');
					const token = await getPersonalGraphToken();
					const endpoint = folderId
						? `https://graph.microsoft.com/v1.0/me/drive/items/${folderId}/children?$top=100&$orderby=lastModifiedDateTime desc&$select=name,id,size,lastModifiedDateTime,file,folder,webUrl`
						: `https://graph.microsoft.com/v1.0/me/drive/root/children?$top=100&$orderby=lastModifiedDateTime desc&$select=name,id,size,lastModifiedDateTime,file,folder,webUrl`;
					const res = await fetch(endpoint, { headers: { 'Authorization': `Bearer ${token}` } });
					const data = await res.json() as any;
					const items = (data.value || []).map((c: any) => ({
						name: c.name,
						id: c.id,
						type: c.folder ? 'folder' : 'file',
						size: c.size,
						modified: c.lastModifiedDateTime,
						webUrl: c.webUrl,
						mimeType: c.file?.mimeType || null,
						viewLink: `/api/personal-onedrive/file?id=${c.id}`,
					}));
					return json({ success: true, items, count: items.length });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/personal-onedrive/search?q=xxx — Search files
			if (path === '/api/personal-onedrive/search' && request.method === 'GET') {
				try {
					const q = url.searchParams.get('q');
					if (!q) return err('q (search query) required', 400);
					const token = await getPersonalGraphToken();
					const res = await fetch(`https://graph.microsoft.com/v1.0/me/drive/root/search(q='${encodeURIComponent(q)}')?$top=50&$select=name,id,size,lastModifiedDateTime,file,folder,webUrl,parentReference`, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const data = await res.json() as any;
					const items = (data.value || []).map((c: any) => ({
						name: c.name,
						id: c.id,
						type: c.folder ? 'folder' : 'file',
						size: c.size,
						modified: c.lastModifiedDateTime,
						webUrl: c.webUrl,
						mimeType: c.file?.mimeType || null,
						parentPath: c.parentReference?.path?.replace('/drive/root:', '') || '',
						viewLink: `/api/personal-onedrive/file?id=${c.id}`,
					}));
					return json({ success: true, query: q, items, count: items.length });
				} catch (e: any) { return json({ success: false, error: e.message }, 500); }
			}

			// GET /api/personal-onedrive/file?id=xxx — View/download file (mirrors work OneDrive pattern)
			if (path === '/api/personal-onedrive/file' && request.method === 'GET') {
				const itemId = url.searchParams.get('id');
				const forceDownload = url.searchParams.get('download') === '1';
				if (!itemId) return err('id required', 400);
				try {
					const token = await getPersonalGraphToken();
					const res = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${itemId}`, {
						headers: { 'Authorization': `Bearer ${token}` }
					});
					const item = await res.json() as any;
					if (item?.error) return err(`OneDrive error: ${item.error.message || item.error.code}`, 404);

					// Folder → list children
					if (item?.folder) {
						const childRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${item.id}/children?$top=100&$select=name,id,size,lastModifiedDateTime,file,folder,webUrl`, {
							headers: { 'Authorization': `Bearer ${token}` }
						});
						const childData = await childRes.json() as any;
						const children = (childData.value || []).map((c: any) => ({
							name: c.name, id: c.id, type: c.folder ? 'folder' : 'file',
							size: c.size, modified: c.lastModifiedDateTime, webUrl: c.webUrl,
							mimeType: c.file?.mimeType || null,
							viewLink: `/api/personal-onedrive/file?id=${c.id}`,
						}));
						return json({ success: true, type: 'folder', name: item.name, id: item.id, webUrl: item.webUrl, childCount: item.folder.childCount, children });
					}

					const downloadUrl = item?.['@microsoft.graph.downloadUrl'];
					if (!downloadUrl) return err('File not found or no download URL', 404);

					const fileName = item.name || 'document';
					const ext = fileName.split('.').pop()?.toLowerCase() || '';

					if (forceDownload) return Response.redirect(downloadUrl, 302);

					// PDF → inline preview
					if (ext === 'pdf') {
						const fileRes = await fetch(downloadUrl);
						return new Response(fileRes.body, {
							headers: { 'Content-Type': 'application/pdf', 'Content-Disposition': `inline; filename="${fileName}"`, ...corsHeaders }
						});
					}

					// Other files → preview page with Office Online embed + download button
					const esc = (s: string) => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
					const safeName = esc(fileName);
					const sizeKB = ((item.size || 0) / 1024).toFixed(1);
					const modified = (item.lastModifiedDateTime || '').substring(0, 10);
					const downloadLink = `https://api.esqs-law.com/api/personal-onedrive/file?id=${encodeURIComponent(itemId)}&download=1`;
					let embedUrl = '';
					try {
						const prevRes = await fetch(`https://graph.microsoft.com/v1.0/me/drive/items/${encodeURIComponent(itemId)}/preview`, {
							method: 'POST', headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }, body: '{}'
						});
						const prevData = await prevRes.json() as any;
						embedUrl = prevData.getUrl || '';
					} catch (_) {}

					const html = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${safeName} — Personal</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#1a1a2e;color:#e0e0e0;height:100vh;display:flex;flex-direction:column}.toolbar{background:#16213e;padding:12px 20px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #0f3460}.toolbar h3{font-size:15px;color:#4ecdc4}.file-info{font-size:12px;color:#888;margin-left:12px}.btn{background:#4ecdc4;color:#1a1a2e;border:none;padding:8px 20px;border-radius:6px;cursor:pointer;font-size:14px;text-decoration:none;font-weight:600}.btn:hover{background:#45b7aa}.preview{flex:1;display:flex;align-items:center;justify-content:center}.preview iframe{width:100%;height:100%;border:none}.no-preview{text-align:center;padding:40px}.no-preview .icon{font-size:64px;margin-bottom:16px}.no-preview p{color:#888;margin:8px 0}</style></head><body>
<div class="toolbar"><div style="display:flex;align-items:center"><h3>📁 ${safeName}</h3><span class="file-info">${sizeKB} KB · ${modified} · Personal OneDrive</span></div><a href="${downloadLink}" class="btn">⬇ Download</a></div>
<div class="preview">${embedUrl ? `<iframe src="${embedUrl}"></iframe>` : `<div class="no-preview"><div class="icon">📄</div><p>${safeName}</p><p>${sizeKB} KB</p><a href="${downloadLink}" class="btn" style="margin-top:16px">⬇ Download</a></div>`}</div></body></html>`;
					return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8', ...corsHeaders } });
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
		const utcHour = new Date(event.scheduledTime).getUTCHours();
		const utcMinute = new Date(event.scheduledTime).getUTCMinutes();

		// --- Shared cron token helpers (accessible from all cron blocks) ---
		async function cronGetGmailTokenShared(): Promise<string> {
			const cached = await env.CACHE.get('gmail_access_token');
			if (cached) return cached;
			const res = await fetch('https://oauth2.googleapis.com/token', {
				method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
				body: new URLSearchParams({ grant_type: 'refresh_token', client_id: env.GOOGLE_OAUTH_CLIENT_ID, client_secret: env.GOOGLE_OAUTH_CLIENT_SECRET, refresh_token: env.GOOGLE_REFRESH_TOKEN })
			});
			const data = await res.json() as any;
			if (!data.access_token) throw new Error('Gmail token failed');
			await env.CACHE.put('gmail_access_token', data.access_token, { expirationTtl: 3000 });
			return data.access_token;
		}

		async function cronGetZoomTokenShared(): Promise<string> {
			const cached = await env.CACHE.get('zoom_access_token');
			if (cached) return cached;
			const res = await fetch(`https://zoom.us/oauth/token?grant_type=account_credentials&account_id=${env.ZOOM_ACCOUNT_ID}`, {
				method: 'POST',
				headers: { 'Authorization': 'Basic ' + btoa(env.ZOOM_CLIENT_ID + ':' + env.ZOOM_CLIENT_SECRET), 'Content-Type': 'application/x-www-form-urlencoded' }
			});
			const data = await res.json() as any;
			if (!data.access_token) throw new Error(`Zoom token failed: ${data.reason || data.error || 'Unknown'}`);
			await env.CACHE.put('zoom_access_token', data.access_token, { expirationTtl: 3500 });
			return data.access_token;
		}

		async function cronGetGraphTokenShared(): Promise<string> {
			const cached = await env.CACHE.get('ms_graph_token');
			if (cached) return cached;
			const res = await fetch('https://login.microsoftonline.com/common/oauth2/v2.0/token', {
				method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
				body: new URLSearchParams({ client_id: env.MICROSOFT_CLIENT_ID, client_secret: env.MICROSOFT_CLIENT_SECRET, refresh_token: env.MICROSOFT_REFRESH_TOKEN, grant_type: 'refresh_token', scope: 'https://graph.microsoft.com/User.Read https://graph.microsoft.com/Files.ReadWrite https://graph.microsoft.com/Files.ReadWrite.All https://graph.microsoft.com/Mail.Read https://graph.microsoft.com/Mail.Send https://graph.microsoft.com/Mail.ReadWrite offline_access' })
			});
			const data = await res.json() as any;
			if (!data.access_token) throw new Error('Graph token failed');
			await env.CACHE.put('ms_graph_token', data.access_token, { expirationTtl: 3000 });
			return data.access_token;
		}

		// ═══════════════════════════════════════════════════════════════
		// CALENDAR CHANGE DETECTION — runs every 2h (on the 6-hour cron)
		// Checks JudiciaLink + court emails for cancellations/changes
		// ═══════════════════════════════════════════════════════════════
		if (utcHour !== 6 || utcMinute < 55) {
			try {
				const graphToken = await cronGetGraphTokenShared();
				if (graphToken) {
					// Check JudiciaLink emails from last 3 hours for changes
					const since = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString();
					const jlUrl = `https://graph.microsoft.com/v1.0/me/messages?$top=25&$filter=receivedDateTime ge ${since}&$search="from:judicialink.com"&$select=subject,receivedDateTime,body,id`;
					const jlRes = await fetch(jlUrl, { headers: { 'Authorization': `Bearer ${graphToken}` } });
					const jlData = await jlRes.json() as any;

					// Also check court emails
					const courtUrl = `https://graph.microsoft.com/v1.0/me/messages?$top=15&$filter=receivedDateTime ge ${since}&$search="from:utcourts.gov"&$select=subject,receivedDateTime,body,id`;
					const courtRes = await fetch(courtUrl, { headers: { 'Authorization': `Bearer ${graphToken}` } });
					const courtData = await courtRes.json() as any;

					let changesDetected = 0;
					const allAlerts = [...(jlData.value || []), ...(courtData.value || [])];

					for (const msg of allAlerts) {
						const bodyText = (msg.body?.content || '').replace(/<[^>]+>/g, ' ').replace(/&amp;/g, '&').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
						const subject = (msg.subject || '').toLowerCase();

						// Detect cancellations / vacated
						const isCancelled = /\b(cancel|vacated|stricken|dismissed|withdrawn)\b/i.test(subject) || /\b(cancel|vacated|stricken|dismissed|withdrawn)\b/i.test(bodyText.substring(0, 500));
						// Detect rescheduled / continued
						const isRescheduled = /\b(continu|reschedul|reset|moved|new date|date change)\b/i.test(subject) || /\b(continu|reschedul|reset|moved to|new date)\b/i.test(bodyText.substring(0, 500));

						if (!isCancelled && !isRescheduled) continue;

						// Extract case number
						const caseMatch = bodyText.match(/(?:RE:|Case\s*(?:#|No\.?|Number)?:?\s*)(\d{9,12})/i) || subject.match(/(\d{9,12})/);
						if (!caseMatch) continue;
						const caseNum = caseMatch[1];

						if (isCancelled) {
							// Mark matching active deadlines as cancelled
							const upd = await env.MEMORY_DB.prepare(
								`UPDATE deadlines SET status = 'cancelled', description = description || ' [CANCELLED per court notice ' || ? || ']' WHERE case_number = ? AND status IN ('active', 'pending') AND due_date >= ?`
							).bind((msg.receivedDateTime || '').substring(0, 10), caseNum, mtnToday()).run();
							if (upd.meta.changes > 0) changesDetected += upd.meta.changes;
						}

						if (isRescheduled) {
							// Mark as rescheduled — removes from EOD timesheet entry and active deadlines
							await env.MEMORY_DB.prepare(
								`UPDATE deadlines SET status = 'rescheduled', description = description || ' [RESCHEDULED per court notice ' || ? || ']' WHERE case_number = ? AND status IN ('active', 'pending') AND due_date >= ?`
							).bind((msg.receivedDateTime || '').substring(0, 10), caseNum, mtnToday()).run();
							changesDetected++;
						}
					}
					if (changesDetected > 0) {
						console.log(`[CRON calendar-check] Detected ${changesDetected} schedule changes from ${allAlerts.length} recent alerts`);
					}
				}
			} catch (e: any) {
				console.error('[CRON calendar-check] Error:', e.message);
			}
		}

		// ═══════════════════════════════════════════════════════════════
		// EMAIL PROCESSING PIPELINE — runs every 2h
		// Scans Outlook + Gmail for new emails, files attachments, extracts deadlines
		// ═══════════════════════════════════════════════════════════════
		if (utcHour !== 6 || utcMinute < 55) {
			try {
				const hoursBack = 3;
				const since = new Date(Date.now() - hoursBack * 60 * 60 * 1000).toISOString();
				let totalProcessed = 0, totalFiled = 0, totalDeadlines = 0, totalUnmatched = 0;

				// Fetch Outlook messages
				const graphToken = await cronGetGraphTokenShared();
				const outlookUrl = `https://graph.microsoft.com/v1.0/me/mailFolders/inbox/messages?$top=30&$filter=receivedDateTime ge '${since}'&$orderby=receivedDateTime desc&$select=subject,from,receivedDateTime,bodyPreview,body,id,hasAttachments`;
				const olRes = await fetch(outlookUrl, { headers: { 'Authorization': `Bearer ${graphToken}` } });
				const olData = await olRes.json() as any;
				const outlookMsgs = (olData.value || []).map((e: any) => ({
					id: e.id, source: 'outlook', subject: e.subject || '', from: e.from?.emailAddress?.address || '',
					fromName: e.from?.emailAddress?.name || '', receivedDateTime: e.receivedDateTime,
					body: e.body?.content || '', hasAttachments: e.hasAttachments
				}));

				// Fetch Gmail messages
				let gmailMsgs: any[] = [];
				try {
					const gmailToken = await cronGetGmailTokenShared();
					const sinceEpoch = Math.floor((Date.now() - hoursBack * 60 * 60 * 1000) / 1000);
					const listUrl = `https://www.googleapis.com/gmail/v1/users/me/messages?q=${encodeURIComponent(`after:${sinceEpoch}`)}&maxResults=20`;
					const listRes = await fetch(listUrl, { headers: { 'Authorization': `Bearer ${gmailToken}` } });
					const listData = await listRes.json() as any;
					for (const msg of (listData.messages || []).slice(0, 20)) {
						const msgRes = await fetch(`https://www.googleapis.com/gmail/v1/users/me/messages/${msg.id}?format=full`, { headers: { 'Authorization': `Bearer ${gmailToken}` } });
						const msgData = await msgRes.json() as any;
						const headers = msgData.payload?.headers || [];
						const getH = (n: string) => (headers.find((h: any) => h.name.toLowerCase() === n.toLowerCase()) || {}).value || '';
						// Decode body from full message
						let gmBody = '';
						if (msgData.payload?.body?.data) {
							try { gmBody = atob(msgData.payload.body.data.replace(/-/g, '+').replace(/_/g, '/')); } catch {}
						} else if (msgData.payload?.parts) {
							for (const part of msgData.payload.parts) {
								if (part.mimeType === 'text/plain' && part.body?.data) {
									try { gmBody = atob(part.body.data.replace(/-/g, '+').replace(/_/g, '/')); } catch {}
									break;
								}
								if (part.mimeType === 'text/html' && part.body?.data && !gmBody) {
									try { gmBody = atob(part.body.data.replace(/-/g, '+').replace(/_/g, '/')); } catch {}
								}
							}
						}
						gmailMsgs.push({
							id: `gmail_${msg.id}`, source: 'gmail', subject: getH('Subject'),
							from: getH('From').match(/<(.+?)>/)?.[1] || getH('From'),
							fromName: getH('From').replace(/<.+?>/, '').trim(),
							receivedDateTime: new Date(getH('Date')).toISOString(),
							body: gmBody || msgData.snippet || '', hasAttachments: false
						});
					}
				} catch (gmErr: any) {
					console.error('[CRON email-pipeline] Gmail error:', gmErr.message);
				}

				for (const msg of [...outlookMsgs, ...gmailMsgs]) {
					try {
						// Dedup
						const already = await env.MEMORY_DB.prepare('SELECT id FROM processed_emails WHERE message_id = ?').bind(msg.id).first();
						if (already) continue;
						totalProcessed++;

						// Match to case — mirrors matchEmailToCase() logic
						const text = `${msg.subject} ${(msg.body || '').substring(0, 1000)}`;
						const textLower = text.toLowerCase();
						let matchedClient: string | null = null, matchedCase: string | null = null;

						// Case number regex
						const caseNumMatch = text.match(/\b(\d{9,12})\b/);
						if (caseNumMatch) {
							const row = await env.MEMORY_DB.prepare('SELECT client_name, case_number FROM party_cache WHERE case_number = ? LIMIT 1').bind(caseNumMatch[1]).first() as any;
							if (row) { matchedClient = row.client_name; matchedCase = row.case_number; }
							// If case number found but NOT ours, skip fuzzy matching (prevents false positives)
						}

						// OC email match (only if no case number was found)
						if (!matchedClient && !caseNumMatch && msg.from) {
							const oc = await env.MEMORY_DB.prepare('SELECT counsel_name FROM opposing_counsel_intel WHERE LOWER(email) = ?').bind(msg.from.toLowerCase()).first() as any;
							if (oc) {
								const pc = await env.MEMORY_DB.prepare('SELECT client_name, case_number FROM party_cache WHERE LOWER(opposing_counsel) LIKE ? LIMIT 1').bind(`%${oc.counsel_name.toLowerCase()}%`).first() as any;
								if (pc) { matchedClient = pc.client_name; matchedCase = pc.case_number; }
							}
						}

						// Client name match with word boundaries (only if no case number was found)
						if (!matchedClient && !caseNumMatch) {
							const clients = await env.MEMORY_DB.prepare('SELECT client_name, case_number FROM party_cache ORDER BY last_verified DESC LIMIT 50').all() as any;
							for (const c of (clients.results || [])) {
								const name = (c.client_name || '').toLowerCase();
								const parts = name.split(/\s+/).filter((p: string) => p.length > 0);
								if (parts.length === 0) continue;
								const lastName = parts[parts.length - 1];
								// Full name match
								if (textLower.includes(name)) { matchedClient = c.client_name; matchedCase = c.case_number; break; }
								// Last + first name word boundary
								if (parts.length >= 2 && lastName.length >= 4) {
									const lnRx = new RegExp(`\\b${lastName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
									const fnRx = new RegExp(`\\b${parts[0].replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
									if (lnRx.test(text) && fnRx.test(text)) { matchedClient = c.client_name; matchedCase = c.case_number; break; }
								}
								// Last name only — 7+ chars with word boundary
								if (lastName.length >= 7) {
									const lnRx = new RegExp(`\\b${lastName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
									if (lnRx.test(text)) { matchedClient = c.client_name; matchedCase = c.case_number; break; }
								}
							}
						}

						// Store
						await env.MEMORY_DB.prepare(
							`INSERT INTO processed_emails (message_id, source, from_email, from_name, subject, received_date, matched_client, matched_case_number, processing_status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
						).bind(msg.id, msg.source, msg.from, msg.fromName, (msg.subject || '').substring(0, 500), msg.receivedDateTime || '', matchedClient, matchedCase, matchedClient ? 'processed' : 'unmatched', mtnISO()).run();

						if (!matchedClient) { totalUnmatched++; continue; }

						// AI deadline extraction (cron version — lightweight, body available for Outlook)
						if (msg.body && msg.body.length > 20) {
							const plainBody = msg.body.replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
							const emailText = `Subject: ${msg.subject}\n\n${plainBody.substring(0, 2000)}`;
							try {
								const aiRes = await env.AI.run('@cf/meta/llama-3.1-8b-instruct' as any, {
									messages: [
										{ role: 'system', content: `Extract legal deadlines from this email. Return ONLY a JSON array. Each: {"type":"hearing|trial|answer_due|response_due|discovery_due|motion_deadline|status_conference|pretrial_conference|sentencing|arraignment|filing_deadline","date":"YYYY-MM-DD","description":"brief"}\nOnly explicit dates after ${mtnToday()}. If none: []` },
										{ role: 'user', content: emailText }
									]
								}) as any;
								const jsonMatch = (aiRes.response || '').match(/\[[\s\S]*\]/);
								const deadlines = jsonMatch ? JSON.parse(jsonMatch[0]) : [];
								for (const dl of deadlines) {
									if (!dl.date || !dl.type || !/^\d{4}-\d{2}-\d{2}$/.test(dl.date) || dl.date <= mtnToday()) continue;
									const dup = await env.MEMORY_DB.prepare(`SELECT id FROM deadlines WHERE case_number = ? AND due_date = ? AND deadline_type = ? AND status IN ('active','pending') LIMIT 1`).bind(matchedCase, dl.date, dl.type).first();
									if (dup) continue;
									await env.MEMORY_DB.prepare(
										`INSERT INTO deadlines (client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, hearing_mode, status, source, notes, created_at) VALUES (?, ?, ?, ?, ?, '', '', '', '', '', 'active', 'email-auto', ?, ?)`
									).bind(matchedClient, matchedCase, dl.type, dl.description || `${dl.type} (from email)`, dl.date, `Auto-extracted from: ${(msg.subject || '').substring(0, 80)}`, mtnISO()).run();
									totalDeadlines++;
								}
							} catch {}
						}

						// Auto-cascade: detect filing events and generate downstream deadlines
						try {
							const bodySnippet = msg.body ? msg.body.replace(/<[^>]+>/g, ' ').substring(0, 2000) : '';
							const trigger = detectTriggerEventFromEmail(msg.subject, bodySnippet);
							if (trigger && matchedClient && matchedCase) {
								// Determine case_type from party_cache
								const caseRow = await env.MEMORY_DB.prepare('SELECT case_type FROM party_cache WHERE case_number = ? LIMIT 1').bind(matchedCase).first() as any;
								const caseType = caseRow?.case_type || 'civil';
								const cascade = await cascadeDeadlinesFromEvent(
									trigger.triggerEvent,
									mtnToday(),
									{ client_name: matchedClient, case_number: matchedCase, case_type: caseType },
									trigger.serviceType,
									env,
									{ emailId: msg.id, emailSubject: msg.subject }
								);
								if (cascade.created > 0) {
									totalDeadlines += cascade.created;
									console.log(`[CRON cascade] ${trigger.triggerLabel} → ${cascade.created} deadlines for ${matchedClient}`);
								}
							}
						} catch (cascErr: any) {
							console.error(`[CRON cascade] Error:`, cascErr.message);
						}
					} catch (msgErr: any) {
						console.error(`[CRON email-pipeline] Error on ${msg.id}:`, msgErr.message);
					}
				}

				if (totalProcessed > 0) {
					console.log(`[CRON email-pipeline] Processed ${totalProcessed} emails: ${totalDeadlines} deadlines, ${totalUnmatched} unmatched`);
				}
			} catch (e: any) {
				console.error('[CRON email-pipeline] Error:', e.message);
			}
		}

		// ═══════════════════════════════════════════════════════════════
		// DEADLINE REMINDERS — runs every 2h (same as email pipeline)
		// Sends proactive email alerts for upcoming deadlines
		// ═══════════════════════════════════════════════════════════════
		if (utcHour !== 6 || utcMinute < 55) {
			try {
				// Cron-scoped Gmail send helper
				async function cronSendGmail(to: string, subject: string, body: string): Promise<boolean> {
					const token = await cronGetGmailTokenShared();
					const safeSubject = subject.replace(/[^\x20-\x7E]/g, '').replace(/\s+/g, ' ').trim();
					const messageParts = [
						'From: Pitcher Law PLLC <esqslaw@gmail.com>',
						`To: ${to}`,
						`Subject: ${safeSubject}`,
						'MIME-Version: 1.0',
						'Content-Type: text/html; charset=utf-8',
						'',
						body.replace(/\n/g, '<br>')
					];
					const raw = btoa(unescape(encodeURIComponent(messageParts.join('\r\n')))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
					const res = await fetch('https://www.googleapis.com/gmail/v1/users/me/messages/send', {
						method: 'POST', headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
						body: JSON.stringify({ raw })
					});
					return res.ok;
				}

				let remindersSent = 0;
				const today = mtnToday();
				const cutoffDate = new Date();
				cutoffDate.setDate(cutoffDate.getDate() + 8);
				const cutoffStr = cutoffDate.toISOString().split('T')[0];

				const dls = await env.MEMORY_DB.prepare(
					`SELECT id, client_name, case_number, deadline_type, description, due_date, reminder_days, hearing_time, court, courtroom, judge FROM deadlines WHERE status IN ('active','pending') AND due_date >= ? AND due_date <= ? ORDER BY due_date ASC`
				).bind(today, cutoffStr).all();

				for (const dl of (dls.results || []) as any[]) {
					const reminderDays = (dl.reminder_days || '7,3,1,0').split(',').map((d: string) => parseInt(d.trim())).filter((d: number) => !isNaN(d));
					const dueDate = new Date(dl.due_date + 'T12:00:00');
					const daysUntil = Math.floor((dueDate.getTime() - Date.now()) / (1000 * 60 * 60 * 24));

					for (const threshold of reminderDays) {
						if (daysUntil !== threshold) continue;
						const reminderType = threshold === 0 ? 'day_of' : `${threshold}d_before`;

						const already = await env.MEMORY_DB.prepare(`SELECT id FROM deadline_reminders_sent WHERE deadline_id = ? AND reminder_type = ? LIMIT 1`).bind(dl.id, reminderType).first();
						if (already) continue;

						const urgency = threshold === 0 ? '[TODAY]' : threshold === 1 ? '[TOMORROW]' : threshold <= 3 ? '[' + threshold + ' DAYS]' : '[' + threshold + ' DAYS]';
						const subject = `${urgency} ${dl.deadline_type.replace(/_/g, ' ').toUpperCase()} - ${dl.client_name}`;
						const details = [dl.description, dl.hearing_time ? `Time: ${dl.hearing_time}` : '', dl.court ? `Court: ${dl.court}` : '', dl.courtroom ? `Room: ${dl.courtroom}` : '', dl.judge ? `Judge: ${dl.judge}` : '', dl.case_number ? `Case: ${dl.case_number}` : ''].filter(Boolean).join('<br>');
						const body = `<div style="font-family:Georgia,serif;max-width:600px"><h2 style="color:#8B0000;margin-bottom:8px">${urgency}</h2><h3>${dl.deadline_type.replace(/_/g, ' ').toUpperCase()}</h3><p><strong>Client:</strong> ${dl.client_name}</p><p><strong>Due:</strong> ${dl.due_date}</p><p>${details}</p><hr style="border:1px solid #ddd"><p style="font-size:12px;color:#666">Pitcher Law PLLC — Automated Deadline Reminder</p></div>`;

						try {
							if (await cronSendGmail('esqslaw@gmail.com', subject, body)) {
								await env.MEMORY_DB.prepare(`INSERT INTO deadline_reminders_sent (deadline_id, reminder_type, sent_at, recipient, created_at) VALUES (?, ?, ?, ?, ?)`).bind(dl.id, reminderType, mtnISO(), 'esqslaw@gmail.com', mtnISO()).run();
								remindersSent++;
							}
						} catch {}
					}
				}

				if (remindersSent > 0) console.log(`[CRON reminders] Sent ${remindersSent} reminders`);
			} catch (e: any) {
				console.error('[CRON reminders] Error:', e.message);
			}
		}

		// ═══════════════════════════════════════════════════════════════
		// CLIENT HEARING REMINDERS — auto-queue 3 days out (cron every 2h)
		// Queues drafts in email_queue for attorney review before send
		// ═══════════════════════════════════════════════════════════════
		if (utcHour % 2 === 0 && utcMinute < 10) {
			try {
				let clientQueued = 0;
				const target3d = new Date(); target3d.setDate(target3d.getDate() + 3);
				const targetStr3d = target3d.toISOString().split('T')[0];

				const hearings = await env.MEMORY_DB.prepare(
					`SELECT d.id, d.client_name, d.case_number, d.deadline_type, d.description, d.due_date, d.hearing_time, d.court, d.courtroom, d.judge,
					cs.client_email
					FROM deadlines d
					LEFT JOIN case_summaries cs ON d.case_number = cs.case_number
					WHERE d.due_date = ? AND d.status IN ('active','pending')
					AND d.deadline_type IN ('hearing','Hearing','evidentiary hearing','review hearing','OSC','status conference','pretrial','trial','bench_trial','jury_trial','arraignment')
					ORDER BY d.due_date`
				).bind(targetStr3d).all();

				const tmpl = await env.MEMORY_DB.prepare('SELECT * FROM email_templates WHERE id = ?').bind('hearing-reminder').first() as any;
				if (tmpl) {
					for (const h of (hearings.results || []) as any[]) {
						if (!h.client_name) continue;

						// Check preferences
						const prefs = await env.MEMORY_DB.prepare(
							'SELECT send_hearing_reminders, preferred_email FROM client_comm_preferences WHERE client_name = ? LIMIT 1'
						).bind(h.client_name).first() as any;
						if (prefs && !prefs.send_hearing_reminders) continue;

						const email = prefs?.preferred_email || h.client_email;
						if (!email) continue;

						// Dedup — skip if already queued
						const existing = await env.MEMORY_DB.prepare(
							`SELECT id FROM email_queue WHERE trigger_type = 'hearing_reminder' AND trigger_id = ? AND status IN ('draft','sent')`
						).bind(String(h.id)).first();
						if (existing) continue;

						const vars: Record<string, string> = {
							client_name: (h.client_name || '').split(',')[0]?.trim() || 'Client',
							hearing_date: h.due_date,
							hearing_time: h.hearing_time || 'TBD',
							hearing_type: h.deadline_type || 'Hearing',
							court: h.court || '',
							courtroom: h.courtroom || '',
							judge: h.judge || ''
						};
						const subj = tmpl.subject_template.replace(/\{\{(\w+)\}\}/g, (_: string, k: string) => vars[k] || '');
						const bod = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f9fafb;font-family:Georgia,'Times New Roman',serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;margin:0 auto;background:#fff;">
<tr><td style="background:#800020;padding:16px 24px;"><h1 style="margin:0;color:#fff;font-size:18px;font-weight:600;">Pitcher Law PLLC</h1></td></tr>
<tr><td style="padding:24px;font-size:14px;line-height:1.6;color:#1f2937;">${tmpl.body_template.replace(/\{\{(\w+)\}\}/g, (_: string, k: string) => vars[k] || '').replace(/\n/g, '<br>')}</td></tr>
<tr><td style="padding:16px 24px;background:#f3f4f6;font-size:11px;color:#6b7280;border-top:1px solid #e5e7eb;">
<p style="margin:0;">Pitcher Law PLLC &bull; 3610 North University Avenue, Suite 375, Provo, Utah 84604</p>
<p style="margin:4px 0 0;">Phone: (801) 960-3366 &bull; <a href="mailto:esqslaw@gmail.com" style="color:#800020;">esqslaw@gmail.com</a></p>
<p style="margin:8px 0 0;font-style:italic;">CONFIDENTIALITY NOTICE: This email and any attachments are for the exclusive and confidential use of the intended recipient. If you are not the intended recipient, please do not read, distribute, or take action based on this message.</p>
</td></tr></table></body></html>`;

						await env.MEMORY_DB.prepare(
							`INSERT INTO email_queue (client_name, case_number, to_address, subject, body, template_id, trigger_type, trigger_id, status, send_via, created_at)
							VALUES (?, ?, ?, ?, ?, 'hearing-reminder', 'hearing_reminder', ?, 'draft', 'graph', ?)`
						).bind(h.client_name, h.case_number, email, subj, bod, String(h.id), new Date().toISOString()).run();
						clientQueued++;
					}
				}

				if (clientQueued > 0) console.log(`[CRON client-reminders] Queued ${clientQueued} client hearing reminders for ${targetStr3d}`);
			} catch (e: any) {
				console.error('[CRON client-reminders] Error:', e.message);
			}
		}

		// ═══════════════════════════════════════════════════════════════
		// MORNING BRIEFING — runs at 14:00 UTC (7AM MT)
		// Daily summary email with today's + this week's deadlines
		// ═══════════════════════════════════════════════════════════════
		if (utcHour === 14 && utcMinute < 5) {
			try {
				const today = mtnToday();

				// Dedup: one briefing per day
				const briefingSent = await env.MEMORY_DB.prepare(`SELECT id FROM deadline_reminders_sent WHERE reminder_type = 'morning_briefing' AND sent_at LIKE ? LIMIT 1`).bind(`${today}%`).first();
				if (!briefingSent) {
					const todayDL = await env.MEMORY_DB.prepare(`SELECT id, client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge FROM deadlines WHERE status IN ('active','pending') AND due_date = ? ORDER BY hearing_time ASC, client_name ASC`).bind(today).all();
					const weekEnd = new Date(); weekEnd.setDate(weekEnd.getDate() + 7);
					const weekDL = await env.MEMORY_DB.prepare(`SELECT id, client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge FROM deadlines WHERE status IN ('active','pending') AND due_date > ? AND due_date <= ? ORDER BY due_date ASC`).bind(today, weekEnd.toISOString().split('T')[0]).all();

					const todayItems = (todayDL.results || []) as any[];
					const weekItems = (weekDL.results || []) as any[];

					if (todayItems.length > 0 || weekItems.length > 0) {
						const formatDL = (dl: any, showDate = false) => {
							const parts = [`<strong>${dl.deadline_type.replace(/_/g, ' ').toUpperCase()}</strong>`, `${dl.client_name}${dl.case_number ? ' (' + dl.case_number + ')' : ''}`, showDate ? `Due: ${dl.due_date}` : '', dl.hearing_time ? `Time: ${dl.hearing_time}` : '', dl.court ? `${dl.court}${dl.courtroom ? ', ' + dl.courtroom : ''}` : '', dl.judge ? `Judge: ${dl.judge}` : ''].filter(Boolean);
							return `<li style="margin-bottom:10px">${parts.join(' — ')}</li>`;
						};

						let todaySection = todayItems.length > 0
							? `<h2 style="color:#8B0000;border-bottom:2px solid #8B0000;padding-bottom:4px">🔴 TODAY — ${today}</h2><ul style="list-style:none;padding-left:0">${todayItems.map(d => formatDL(d)).join('')}</ul>`
							: `<h2 style="color:#228B22;border-bottom:2px solid #228B22;padding-bottom:4px">✅ TODAY — ${today}</h2><p>No deadlines today.</p>`;
						let weekSection = weekItems.length > 0
							? `<h2 style="color:#B8860B;border-bottom:2px solid #B8860B;padding-bottom:4px">📅 THIS WEEK</h2><ul style="list-style:none;padding-left:0">${weekItems.map(d => formatDL(d, true)).join('')}</ul>`
							: '';

						const body = `<div style="font-family:Georgia,serif;max-width:650px"><h1 style="color:#333;margin-bottom:4px">☀️ Morning Briefing</h1><p style="color:#666;margin-top:0">${new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}</p>${todaySection}${weekSection}<hr style="border:1px solid #ddd;margin-top:20px"><p style="font-size:12px;color:#666">Pitcher Law PLLC — ${todayItems.length} today, ${weekItems.length} this week</p></div>`;
						const subject = `Morning Briefing: ${todayItems.length} today, ${weekItems.length} this week - ${today}`;

						// Send via Gmail (cron scope — using shared token helper)
						const token = await cronGetGmailTokenShared();
						const msgParts = ['From: Pitcher Law PLLC <esqslaw@gmail.com>', 'To: esqslaw@gmail.com', `Subject: ${subject}`, 'MIME-Version: 1.0', 'Content-Type: text/html; charset=utf-8', '', body.replace(/\n/g, '<br>')];
						const raw = btoa(unescape(encodeURIComponent(msgParts.join('\r\n')))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
						const sendRes = await fetch('https://www.googleapis.com/gmail/v1/users/me/messages/send', {
							method: 'POST', headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
							body: JSON.stringify({ raw })
						});

						if (sendRes.ok) {
							await env.MEMORY_DB.prepare(`INSERT INTO deadline_reminders_sent (deadline_id, reminder_type, sent_at, recipient, created_at) VALUES (0, 'morning_briefing', ?, ?, ?)`).bind(mtnISO(), 'esqslaw@gmail.com', mtnISO()).run();
							console.log(`[CRON briefing] Sent: ${todayItems.length} today, ${weekItems.length} this week`);
						}
					} else {
						console.log('[CRON briefing] No deadlines today or this week — skipped');
					}
				} else {
					console.log('[CRON briefing] Already sent today');
				}
			} catch (e: any) {
				console.error('[CRON briefing] Error:', e.message);
			}
		}

		// ═══════════════════════════════════════════════════════════════
		// AUTO-ZOOM: Attach Zoom links to upcoming Google Calendar events
		// Runs every 2h (same trigger as email pipeline). Skips hearings/intakes.
		// ═══════════════════════════════════════════════════════════════
		if (utcHour % 2 === 0 && utcMinute < 10) {
			try {
				const calToken = await cronGetGmailTokenShared();
				const now = new Date();
				const weekAhead = new Date(); weekAhead.setDate(weekAhead.getDate() + 7);
				const calParams = new URLSearchParams({ timeMin: now.toISOString(), timeMax: weekAhead.toISOString(), singleEvents: 'true', orderBy: 'startTime', maxResults: '50' });
				const calRes = await fetch(`https://www.googleapis.com/calendar/v3/calendars/primary/events?${calParams}`, {
					headers: { 'Authorization': `Bearer ${calToken}` }
				});
				const calData = await calRes.json() as any;
				const events = calData.items || [];
				const courtPattern = /\b(hearing|arraignment|sentencing|pretrial|pre-trial|conference|plea|trial|intake|consultation|OSC|order to show cause|status|review hearing|bench trial|jury trial)\b/i;
				let attached = 0;

				for (const evt of events) {
					const summary = evt.summary || '';
					const desc = evt.description || '';
					const loc = evt.location || '';
					if (courtPattern.test(summary)) continue;
					if (/zoom\.us/i.test(desc) || /zoom\.us/i.test(loc)) continue;
					const startDt = evt.start?.dateTime || evt.start?.date || '';
					if (!startDt) continue;

					try {
						const zoomToken = await cronGetZoomTokenShared();
						const meetRes = await fetch('https://api-us.zoom.us/v2/users/esqslaw@gmail.com/meetings', {
							method: 'POST',
							headers: { 'Authorization': `Bearer ${zoomToken}`, 'Content-Type': 'application/json' },
							body: JSON.stringify({
								topic: summary, type: 2, start_time: startDt, duration: 30, timezone: 'America/Denver',
								settings: { waiting_room: true, mute_upon_entry: true, join_before_host: false, auto_recording: 'cloud' }
							})
						});
						const meetData = await meetRes.json() as any;
						if (!meetData.join_url) continue;

						const patchRes = await fetch(`https://www.googleapis.com/calendar/v3/calendars/primary/events/${evt.id}`, {
							method: 'PATCH',
							headers: { 'Authorization': `Bearer ${calToken}`, 'Content-Type': 'application/json' },
							body: JSON.stringify({
								location: meetData.join_url,
								description: (desc ? desc + '\n\n' : '') + `🔗 Zoom Meeting\nJoin: ${meetData.join_url}\nPassword: ${meetData.password || ''}`
							})
						});
						if (patchRes.ok) attached++;
					} catch (meetErr: any) {
						console.error(`[CRON auto-zoom] Error for "${summary}":`, meetErr.message);
					}
				}

				console.log(`[CRON auto-zoom] ${attached > 0 ? `Attached Zoom links to ${attached} event(s)` : 'No events needed Zoom links'}`);
			} catch (e: any) {
				console.error('[CRON auto-zoom] Error:', e.message);
			}
		}

		// ═══════════════════════════════════════════════════════════════
		// EMAIL PIPELINE — Process new emails every 2 hours via self-fetch chain
		// Each fetch = new Worker invocation = fresh 30s CPU budget
		// ═══════════════════════════════════════════════════════════════
		if (utcHour % 2 === 0 && utcMinute < 10) {
			ctx.waitUntil((async () => {
				try {
					console.log('[CRON email-pipeline] Starting email processing (serial mode)...');
					// Use serial mode from cron — self-fetch for auto-continuation works within serial mode
					const result = await processEmailPipeline('both', 4, env, 2);
					console.log(`[CRON email-pipeline] Processed ${result.totalProcessed}, ${result.remaining} remaining`);
				} catch (e: any) {
					console.error('[CRON email-pipeline] Error:', e.message);
				}
			})());
		}

		// ═══════════════════════════════════════════════════════════════
		// END-OF-DAY: Auto-enter today's remaining deadlines into timesheet
		// Runs at 23:59 MT (06:59 UTC) — separate cron trigger
		// ═══════════════════════════════════════════════════════════════
		if (utcHour === 6 && utcMinute >= 55) {
			try {
				const today = mtnToday();
				// Get today's deadlines that are still active/pending (not yet completed)
				const { results: remaining } = await env.MEMORY_DB.prepare(
					`SELECT id, client_name, case_number, deadline_type, description, due_date, hearing_time, court FROM deadlines WHERE due_date = ? AND status IN ('active', 'pending')`
				).bind(today).all() as any;

				let entered = 0;
				for (const d of (remaining || [])) {
					// Check if a timecard already exists for this deadline (avoid duplicates)
					const existing = await env.MEMORY_DB.prepare(
						`SELECT id FROM timecards WHERE client = ? AND date = ? AND source IN ('deadline-complete', 'end-of-day') AND description LIKE ?`
					).bind(d.client_name, d.due_date, `%${d.deadline_type}%`).first();
					if (existing) continue;

					// Auto-map category
					const dt = (d.deadline_type || '').toLowerCase();
					let category = 'Appeared - Hearing';
					if (dt.includes('pretrial')) category = 'Appeared - Pretrial Conference';
					else if (dt.includes('arraign')) category = 'Appeared - Arraignment';
					else if (dt.includes('preliminary')) category = 'Appeared - Preliminary Hearing';
					else if (dt.includes('plea')) category = 'Appeared - Change of Plea';
					else if (dt.includes('evidentiary')) category = 'Appeared - Evidentiary Hearing';
					else if (dt.includes('motion')) category = 'Appeared - Motion Hearing';
					else if (dt.includes('review')) category = 'Appeared - Review Hearing';
					else if (dt.includes('protective')) category = 'Appeared - Protective Order Hearing';
					else if (dt.includes('sentenc')) category = 'Appeared - Sentencing';
					else if (dt.includes('trial')) category = 'Appeared - Trial';
					else if (dt.includes('discovery')) category = 'Case Prep - Discovery Review';

					const tcId = `tc-eod-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
					const desc = `${d.client_name} - ${d.deadline_type} ${d.due_date}`;
					const now = new Date().toISOString();

					await env.MEMORY_DB.prepare(
						`INSERT INTO timecards (id, client, case_number, case_type, description, category, date, hours, billed_hours, court, source, billed, notes, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
					).bind(tcId, d.client_name || '', d.case_number || '', '', desc, category, d.due_date, 1.0, 1.0, d.court || '', 'end-of-day', 1, '', 'approved', now).run();

					// Mark deadline as completed
					await env.MEMORY_DB.prepare(`UPDATE deadlines SET status = 'completed' WHERE id = ?`).bind(d.id).run();
					entered++;
				}
				console.log(`[CRON end-of-day] ${today}: ${entered} deadlines entered into timesheet (${(remaining || []).length} total remaining)`);
			} catch (e: any) {
				console.error('[CRON end-of-day] Error:', e.message);
			}
			return; // Don't run deep-scan on the EOD trigger
		}

		// ═══════════════════════════════════════════════════════════════
		// DEEP SCAN — only runs every 6 hours (0, 6, 12, 18 UTC)
		// ═══════════════════════════════════════════════════════════════
		if (utcHour % 6 !== 0) return; // Skip deep scan on 2h/4h marks

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

			// Get Graph token via shared helper
			let token: string;
			try {
				token = await cronGetGraphTokenShared();
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
							temperature: 0,
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
