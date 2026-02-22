#!/usr/bin/env node
/**
 * LOCAL BULK SEED SCRIPT — Case Summary Deep Scan
 *
 * Runs locally to avoid Worker AI token costs.
 * Gets Graph/Google tokens from the Worker's token-proxy endpoint.
 * Uses pdf-parse for PDFs, jszip for DOCX, Anthropic Haiku for AI extraction.
 * PATCHes results to the Worker API.
 *
 * Usage:
 *   node scripts/bulk-seed-cases.mjs                    # scan all cases
 *   node scripts/bulk-seed-cases.mjs --client "smith"   # scan one client
 *   node scripts/bulk-seed-cases.mjs --offset 50        # start at offset 50
 *   node scripts/bulk-seed-cases.mjs --dry-run          # preview only
 *   node scripts/bulk-seed-cases.mjs --gdrive           # also scan Google Drive
 *   node scripts/bulk-seed-cases.mjs --batch 20         # limit to 20 cases
 */

import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const pdfParse = require('pdf-parse');
const JSZip = require('jszip');

// ═══ CONFIG ═══
const API_BASE = 'https://api.esqs-law.com';
const MEMORY_DB_NAME = 'pitcher-law-memory';
const BATCH_DELAY = 600; // ms between cases to be gentle on APIs

// ═══ FIRM PERSONNEL (for party verification) ═══
const OWN_FIRM_PATTERNS = /pitcher\s*law|diane\s*pitcher|john\s*adams|dianepitcher\.com|esqslaw|marie@|associate@|^pitcher|^adams/i;

// ═══ PARSE ARGS ═══
const args = process.argv.slice(2);
const clientFilter = args.includes('--client') ? args[args.indexOf('--client') + 1] : null;
const offset = args.includes('--offset') ? parseInt(args[args.indexOf('--offset') + 1]) : 0;
const dryRun = args.includes('--dry-run');
const scanGDrive = args.includes('--gdrive');
const batchSize = args.includes('--batch') ? parseInt(args[args.indexOf('--batch') + 1]) : 999;

let graphToken = null;
let googleToken = null;
let onedriveFolderId = null;
let anthropicKey = null;
let stats = { scanned: 0, updated: 0, skipped: 0, errors: 0, fieldsUpdated: 0 };

// ═══ GET TOKENS FROM WORKER PROXY ═══
async function getTokens() {
  console.log('🔑 Getting tokens from Worker proxy...');
  const res = await fetch(`${API_BASE}/api/case-summaries/deep-scan/tokens`);
  if (!res.ok) throw new Error(`Token proxy failed: ${res.status} ${res.statusText}`);
  const data = await res.json();
  if (!data.success) throw new Error(`Token proxy error: ${data.error}`);
  graphToken = data.graph_token;
  googleToken = data.google_token || null;
  onedriveFolderId = data.onedrive_folder_id || null;
  if (data.anthropic_key && !anthropicKey) anthropicKey = data.anthropic_key;
  console.log('✅ Graph token acquired');
  if (googleToken) console.log('✅ Google token acquired');
  if (onedriveFolderId) console.log(`✅ OneDrive folder: ${onedriveFolderId}`);
}

// ═══ GET ANTHROPIC KEY ═══
function getAnthropicKey() {
  if (process.env.ANTHROPIC_API_KEY) return process.env.ANTHROPIC_API_KEY;
  // Check local .env-secrets if it exists
  try {
    const secretsFile = path.join(process.cwd(), 'scripts', '.env-secrets');
    if (fs.existsSync(secretsFile)) {
      const lines = fs.readFileSync(secretsFile, 'utf8').split('\n');
      for (const line of lines) {
        const [key, ...val] = line.split('=');
        if (key?.trim() === 'ANTHROPIC_API_KEY' && val.length) return val.join('=').trim();
      }
    }
  } catch {}
  // Check parent .env
  try {
    const envFile = fs.readFileSync(path.join(process.cwd(), '..', '.env'), 'utf8');
    const match = envFile.match(/ANTHROPIC_API_KEY=(.+)/);
    if (match) return match[1].trim();
  } catch {}
  return null;
}

// ═══ GET CASES FROM D1 ═══
async function getCases() {
  let sql = `SELECT * FROM case_summaries WHERE status = 'active'`;
  if (clientFilter) {
    const parts = clientFilter.split(/\s+/).filter(p => p.length >= 2);
    for (const p of parts) {
      sql += ` AND LOWER(client_name) LIKE '%${p.toLowerCase()}%'`;
    }
  }
  sql += ` ORDER BY client_name ASC LIMIT ${batchSize} OFFSET ${offset}`;

  try {
    const result = execSync(
      `npx wrangler d1 execute ${MEMORY_DB_NAME} --remote --json --command="${sql.replace(/"/g, '\\"')}"`,
      { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024 }
    );
    const parsed = JSON.parse(result);
    return parsed[0]?.results || [];
  } catch (e) {
    console.error('Failed to get cases from D1:', e.message);
    return [];
  }
}

// ═══ FUZZY NAME MATCHING ═══
function fuzzyMatch(a, b) {
  if (a === b) return true;
  if (a.length < 3 || b.length < 3) return false;
  // Substring containment (handles "brookly" in "brooklyn")
  if (a.includes(b) || b.includes(a)) return true;
  // Levenshtein distance for close matches (Shahraji vs Shaharji, Duplessis vs Duplesis)
  const len = Math.max(a.length, b.length);
  if (len === 0) return true;
  const dist = levenshtein(a, b);
  return dist <= Math.max(1, Math.floor(len / 4)); // allow ~25% edits
}

function levenshtein(a, b) {
  const m = a.length, n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const dp = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = a[i-1] === b[j-1]
        ? dp[i-1][j-1]
        : 1 + Math.min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]);
    }
  }
  return dp[m][n];
}

// ═══ FIND FILES IN ONEDRIVE VIA GRAPH ═══
async function findOneDriveFiles(clientName) {
  if (!graphToken) return null;

  const nameParts = clientName.replace(/[^a-zA-Z\s]/g, ' ').trim().split(/\s+/)
    .filter(p => p.length >= 2).map(p => p.toLowerCase());

  // Get top-level folders
  if (!onedriveFolderId) {
    const res = await fetch(
      `https://graph.microsoft.com/v1.0/me/drive/root/search(q='Open Cases')?$top=5&$select=name,id,folder`,
      { headers: { 'Authorization': `Bearer ${graphToken}` } }
    );
    const data = await res.json();
    const folder = (data.value || []).find(f => f.folder && f.name.includes('Open Cases'));
    if (folder) onedriveFolderId = folder.id;
    if (!onedriveFolderId) return null;
  }

  const folderRes = await fetch(
    `https://graph.microsoft.com/v1.0/me/drive/items/${onedriveFolderId}/children?$top=200`,
    { headers: { 'Authorization': `Bearer ${graphToken}` } }
  );
  const folderData = await folderRes.json();

  const last = nameParts[nameParts.length - 1];
  const first = nameParts[0];

  // Smart folder match — 3 tiers: exact > fuzzy > last-name-only
  const clientFolder = (folderData.value || []).find(f => {
    if (!f.folder) return false;
    const fn = f.name.toLowerCase().replace(/[^a-z\s]/g, ' ');
    const fParts = fn.trim().split(/\s+/).filter(p => p.length >= 2);

    // Tier 1: All name parts appear in folder (exact substring)
    if (nameParts.every(p => fn.includes(p))) return true;

    // Tier 2: First + last both appear (handles middle names)
    if (nameParts.length >= 2 && fn.includes(first) && fn.includes(last)) return true;

    // Tier 3: Fuzzy match — last name fuzzy-matches a folder part, AND first name fuzzy-matches too
    const lastFuzzy = fParts.some(fp => fuzzyMatch(last, fp));
    const firstFuzzy = fParts.some(fp => fuzzyMatch(first, fp));
    if (lastFuzzy && firstFuzzy) return true;

    // Tier 4: Last name exact starts-with (>4 chars) — e.g. "martinez" folder for "Martinez, Anthony"
    if (last && last.length > 4 && fn.startsWith(last)) return true;

    // Tier 5: Last name fuzzy only (>5 chars) — e.g. "Duplesis" for "DUPLESSIS"
    if (last && last.length > 5 && lastFuzzy) return true;

    return false;
  });

  if (!clientFolder) return null;

  // List files + 1 level subfolders
  const allFiles = [];
  const topFiles = await fetch(
    `https://graph.microsoft.com/v1.0/me/drive/items/${clientFolder.id}/children?$top=100&$orderby=lastModifiedDateTime desc`,
    { headers: { 'Authorization': `Bearer ${graphToken}` } }
  ).then(r => r.json());

  for (const f of (topFiles.value || [])) {
    if (f.folder) {
      try {
        const subFiles = await fetch(
          `https://graph.microsoft.com/v1.0/me/drive/items/${f.id}/children?$top=50`,
          { headers: { 'Authorization': `Bearer ${graphToken}` } }
        ).then(r => r.json());
        for (const sf of (subFiles.value || [])) allFiles.push({ ...sf, subfolder: f.name });
      } catch {}
    } else {
      allFiles.push(f);
    }
  }

  return { folder: clientFolder, files: allFiles };
}

// ═══ CATEGORIZE FILES ═══
function categorizeFiles(files) {
  const keyDocs = {};
  const prefer = (cat, f) => {
    const existing = keyDocs[cat];
    if (!existing) { keyDocs[cat] = f; return; }
    const newExt = (f.name || '').split('.').pop()?.toLowerCase();
    const oldExt = (existing.name || '').split('.').pop()?.toLowerCase();
    if (newExt === 'docx' && oldExt !== 'docx') keyDocs[cat] = f;
  };

  for (const f of files) {
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
  return keyDocs;
}

// ═══ EXTRACT TEXT FROM ONEDRIVE FILE ═══
async function extractText(file) {
  const ext = (file.name || '').split('.').pop()?.toLowerCase();

  // Download file through Worker proxy
  const fileRes = await fetch(
    `${API_BASE}/api/case-summaries/deep-scan/file-proxy?id=${file.id}&source=onedrive`
  );
  if (!fileRes.ok) return '';
  const buf = Buffer.from(await fileRes.arrayBuffer());

  if (ext === 'pdf') {
    try {
      const data = await pdfParse(buf);
      return data.text || '';
    } catch {
      return '';
    }
  } else if (ext === 'docx' || ext === 'doc') {
    try {
      const zip = await JSZip.loadAsync(buf);
      const docXml = await zip.file('word/document.xml')?.async('string');
      if (!docXml) return '';
      const matches = docXml.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
      return matches.map(m => m.replace(/<[^>]+>/g, '')).join(' ');
    } catch {
      return '';
    }
  } else if (ext === 'txt') {
    return buf.toString('utf-8');
  }
  return '';
}

// ═══ EXTRACT TEXT FROM GOOGLE DRIVE FILE ═══
async function extractGDriveFile(fileId, mimeType) {
  const fileRes = await fetch(
    `${API_BASE}/api/case-summaries/deep-scan/file-proxy?id=${fileId}&source=gdrive`
  );
  if (!fileRes.ok) return '';
  const buf = Buffer.from(await fileRes.arrayBuffer());

  if (mimeType === 'application/pdf') {
    try {
      const data = await pdfParse(buf);
      return data.text || '';
    } catch { return ''; }
  } else if (mimeType?.includes('wordprocessing')) {
    try {
      const zip = await JSZip.loadAsync(buf);
      const docXml = await zip.file('word/document.xml')?.async('string');
      if (!docXml) return '';
      const matches = docXml.match(/<w:t[^>]*>([^<]+)<\/w:t>/g) || [];
      return matches.map(m => m.replace(/<[^>]+>/g, '')).join(' ');
    } catch { return ''; }
  }
  return '';
}

// ═══ AI EXTRACTION (Anthropic Haiku — cheap) ═══
async function aiExtract(combinedText, clientName, caseNum, cs) {
  if (!anthropicKey) {
    console.log('    ⚠️  No Anthropic key — skipping AI extraction');
    return null;
  }

  const prompt = `You are a legal document analyzer. Extract CONCISE info.

CLIENT: ${clientName} (${cs.client_role || 'our client'})
OPPOSING PARTY: ${cs.opposing_party || 'unknown'}
OUR FIRM: Pitcher Law PLLC (Diane Pitcher, John Adams). Emails: @dianepitcher.com, @esqslaw
${cs.opposing_counsel ? `KNOWN OC: ${cs.opposing_counsel}` : ''}

CRITICAL: OC is the OTHER side's attorney. Pitcher Law/Diane Pitcher/John Adams/@dianepitcher.com = OUR firm, NOT OC.

RULES: Facts 2-3 sentences MAX. Charges: names + degrees only. Use "" for not found.

Respond with ONLY valid JSON:
{"facts":"","charges":"","oc_name":"","oc_phone":"","oc_email":"","oc_firm":"","discovery_deadline":"","trial_date":"","dispositive_deadline":"","statute_of_limitations":"","additional_parties":""}

Documents:
${combinedText.substring(0, 8000)}`;

  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': anthropicKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-3-haiku-20240307',
        max_tokens: 800,
        messages: [{ role: 'user', content: prompt }],
      }),
    });
    if (!res.ok) {
      const errText = await res.text();
      console.log(`    ⚠️  AI API error ${res.status}: ${errText.substring(0, 200)}`);
      return null;
    }
    const data = await res.json();
    const text = data.content?.[0]?.text || '';
    if (!text) {
      console.log(`    ⚠️  AI returned empty. Stop reason: ${data.stop_reason}. Type: ${data.type}`);
      if (data.error) console.log(`    ⚠️  API error: ${JSON.stringify(data.error)}`);
      return null;
    }
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (jsonMatch) return JSON.parse(jsonMatch[0]);
    console.log(`    ⚠️  AI response not JSON: ${text.substring(0, 100)}`);
    return null;
  } catch (e) {
    console.log(`    ⚠️  AI extraction failed: ${e.message}`);
    return null;
  }
}

// ═══ PARTY VERIFICATION ═══
function verifyParties(extracted, clientName, cs) {
  const isSelf = v => v && OWN_FIRM_PATTERNS.test(v);

  if (isSelf(extracted.oc_name || '') || isSelf(extracted.oc_firm || '') || isSelf(extracted.oc_email || '')) {
    extracted.oc_name = '';
    extracted.oc_phone = '';
    extracted.oc_email = '';
    extracted.oc_firm = '';
    console.log('    🚫 Party verification: REJECTED OC (matched our firm)');
  }

  if (extracted.oc_name && clientName.toLowerCase().includes(extracted.oc_name.toLowerCase().split(' ')[0])) {
    extracted.oc_name = '';
    extracted.oc_phone = '';
    extracted.oc_email = '';
    extracted.oc_firm = '';
    console.log('    🚫 Party verification: REJECTED OC (matched our client)');
  }

  const existingOC = cs.opposing_counsel || '';
  if (existingOC && extracted.oc_name && extracted.oc_name.toLowerCase() !== existingOC.toLowerCase()) {
    extracted.oc_name = '';
    console.log('    ⚠️  OC mismatch with court records — kept existing');
  }

  return extracted;
}

// ═══ PATCH CASE SUMMARY ═══
async function patchCase(caseNumber, clientName, extracted, cs) {
  const updates = {};

  if (extracted.facts && !cs.facts) updates.facts = extracted.facts;
  if (extracted.charges && !cs.charges && extracted.charges.toLowerCase() !== 'not found') updates.charges = extracted.charges;
  if (extracted.oc_name && !cs.opposing_counsel) updates.opposing_counsel = extracted.oc_name;
  if (extracted.oc_phone && !cs.opposing_counsel_phone && extracted.oc_phone.toLowerCase() !== 'not found') updates.opposing_counsel_phone = extracted.oc_phone;
  if (extracted.oc_email && !cs.opposing_counsel_email && extracted.oc_email.toLowerCase() !== 'not found') updates.opposing_counsel_email = extracted.oc_email;
  if (extracted.oc_firm && !cs.opposing_counsel_firm && extracted.oc_firm.toLowerCase() !== 'not found') updates.opposing_counsel_firm = extracted.oc_firm;
  if (extracted.discovery_deadline && !cs.discovery_deadline && extracted.discovery_deadline.toLowerCase() !== 'not found') updates.discovery_deadline = extracted.discovery_deadline;
  if (extracted.trial_date && !cs.trial_date && extracted.trial_date.toLowerCase() !== 'not found') updates.trial_date = extracted.trial_date;
  if (extracted.dispositive_deadline && !cs.dispositive_deadline && extracted.dispositive_deadline.toLowerCase() !== 'not found') updates.dispositive_deadline = extracted.dispositive_deadline;
  if (extracted.statute_of_limitations && !cs.statute_of_limitations && extracted.statute_of_limitations.toLowerCase() !== 'not found') updates.statute_of_limitations = extracted.statute_of_limitations;
  if (extracted.additional_parties && !cs.additional_parties && extracted.additional_parties.toLowerCase() !== 'none') updates.additional_parties = extracted.additional_parties;

  const fieldCount = Object.keys(updates).length;
  if (fieldCount === 0) {
    console.log('    → No new fields to update');
    return 0;
  }

  if (dryRun) {
    console.log(`    → DRY RUN: Would update ${fieldCount} fields:`, Object.keys(updates).join(', '));
    return fieldCount;
  }

  updates.client_name = clientName;
  const res = await fetch(`${API_BASE}/api/case-summaries/${encodeURIComponent(caseNumber)}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(updates),
  });
  const data = await res.json();
  if (data.success) {
    console.log(`    ✅ Updated ${fieldCount} fields: ${Object.keys(updates).filter(k => k !== 'client_name').join(', ')}`);
    return fieldCount;
  } else {
    console.log(`    ❌ PATCH failed: ${data.error}`);
    return 0;
  }
}

// ═══ GOOGLE DRIVE SCAN ═══
async function scanGoogleDriveFiles(cases) {
  if (!googleToken) {
    console.log('⚠️  No Google token available — skipping GDrive scan');
    return;
  }
  console.log('\n📂 Scanning Google Drive for ESQs cases...');

  for (const cs of cases) {
    const clientName = cs.client_name;
    if (cs.facts && cs.charges && cs.opposing_counsel) continue; // already populated

    const lastName = clientName.split(/\s+/).pop().toLowerCase();
    if (lastName.length < 3) continue;

    // Search GDrive for files with client last name
    try {
      const searchUrl = `https://www.googleapis.com/drive/v3/files?q=name contains '${lastName}' and (mimeType='application/pdf' or mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')&pageSize=10&fields=files(id,name,mimeType)`;
      const fileRes = await fetch(searchUrl, {
        headers: { 'Authorization': `Bearer ${googleToken}` }
      });
      const fileData = await fileRes.json();
      const gFiles = fileData.files || [];

      if (gFiles.length === 0) continue;
      console.log(`  📁 ${clientName}: ${gFiles.length} GDrive files found`);

      // Download and extract text (max 3 files)
      let combinedText = '';
      for (const gf of gFiles.slice(0, 3)) {
        try {
          const text = await extractGDriveFile(gf.id, gf.mimeType);
          if (text && text.trim().length > 20) {
            combinedText += `\n--- ${gf.name} ---\n${text.substring(0, 4000)}\n`;
            console.log(`    📝 ${gf.name}: ${text.trim().length} chars`);
          }
        } catch {}
      }

      if (combinedText.length < 50) continue;

      // AI extract
      const extracted = await aiExtract(combinedText, clientName, cs.case_number, cs);
      if (!extracted) continue;

      const verified = verifyParties(extracted, clientName, cs);
      const updated = await patchCase(cs.case_number, clientName, verified, cs);
      if (updated > 0) stats.fieldsUpdated += updated;

      await sleep(BATCH_DELAY);
    } catch (e) {
      console.log(`  ❌ GDrive error for ${clientName}: ${e.message}`);
    }
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ═══ MAIN ═══
async function main() {
  console.log('╔══════════════════════════════════════════════════════╗');
  console.log('║  CASE SUMMARY BULK SEED — Local Deep Scan          ║');
  console.log('╚══════════════════════════════════════════════════════╝');
  console.log(`  Mode: ${dryRun ? 'DRY RUN' : 'LIVE'}`);
  console.log(`  Client filter: ${clientFilter || 'all'}`);
  console.log(`  Offset: ${offset}`);
  console.log(`  Batch: ${batchSize}`);
  console.log(`  GDrive scan: ${scanGDrive ? 'yes' : 'no'}`);
  console.log('');

  // 1. Get tokens from Worker proxy
  await getTokens();

  // 2. Get Anthropic key (for local AI extraction)
  if (!anthropicKey) anthropicKey = getAnthropicKey();
  console.log(anthropicKey ? '✅ Anthropic key found' : '⚠️  No Anthropic key — AI extraction disabled');

  // 3. Get cases from D1
  console.log('\n📋 Loading cases from D1...');
  const cases = await getCases();
  console.log(`  Found ${cases.length} active cases`);

  if (cases.length === 0) {
    console.log('No cases to scan.');
    return;
  }

  // 4. Scan each case
  console.log(`\n🔍 Starting scan (${cases.length} cases)...\n`);

  for (let i = 0; i < cases.length; i++) {
    const cs = cases[i];
    const clientName = cs.client_name;
    const caseNum = cs.case_number;

    console.log(`[${i + 1}/${cases.length}] ${clientName} — ${caseNum}`);

    // Skip if all key fields populated
    if (cs.facts && cs.charges && cs.opposing_counsel) {
      console.log('    → Already populated, skipping');
      stats.skipped++;
      continue;
    }

    try {
      // Find OneDrive folder
      const result = await findOneDriveFiles(clientName);
      if (!result) {
        console.log('    → No OneDrive folder found');
        stats.skipped++;
        continue;
      }

      console.log(`    📂 ${result.files.length} files in "${result.folder.name}"`);

      // Categorize files
      const keyDocs = categorizeFiles(result.files);
      const docTypes = Object.keys(keyDocs);
      if (docTypes.length === 0) {
        console.log('    → No key documents identified');
        stats.skipped++;
        continue;
      }
      console.log(`    📄 Key docs: ${docTypes.map(t => `${t}(${keyDocs[t].name.split('.').pop()})`).join(', ')}`);

      // Extract text from top 3 docs
      const docsToRead = Object.entries(keyDocs).slice(0, 3);
      let combinedText = '';

      for (const [docType, file] of docsToRead) {
        const text = await extractText(file);
        if (text && text.trim().length > 20) {
          combinedText += `\n--- ${docType.toUpperCase()} (${file.name}) ---\n${text.substring(0, 4000)}\n`;
          console.log(`    📝 ${docType}: ${text.trim().length} chars extracted`);
        } else {
          console.log(`    ⚠️  ${docType}: no text extracted`);
        }
      }

      if (combinedText.length < 50) {
        console.log('    → Insufficient text extracted');
        stats.errors++;
        continue;
      }

      // AI extraction
      if (!anthropicKey) {
        console.log('    → Skipping AI (no key)');
        stats.scanned++;
        continue;
      }

      const extracted = await aiExtract(combinedText, clientName, caseNum, cs);
      if (!extracted) {
        console.log('    → AI extraction returned nothing');
        stats.errors++;
        continue;
      }

      // Party verification
      const verified = verifyParties(extracted, clientName, cs);

      // PATCH
      const updated = await patchCase(caseNum, clientName, verified, cs);
      stats.fieldsUpdated += updated;
      stats.scanned++;
      if (updated > 0) stats.updated++;

    } catch (e) {
      console.log(`    ❌ Error: ${e.message}`);
      stats.errors++;
    }

    // Rate limit
    await sleep(BATCH_DELAY);

    // Refresh tokens every 40 minutes (Graph token TTL is 50 min)
    if (i > 0 && i % 60 === 0) {
      console.log('\n🔄 Refreshing tokens...');
      await getTokens();
    }
  }

  // 5. Google Drive scan
  if (scanGDrive) {
    await scanGoogleDriveFiles(cases);
  }

  // 6. Summary
  console.log('\n╔══════════════════════════════════════════════════════╗');
  console.log('║  SCAN COMPLETE                                      ║');
  console.log('╚══════════════════════════════════════════════════════╝');
  console.log(`  Cases scanned:  ${stats.scanned}`);
  console.log(`  Cases updated:  ${stats.updated}`);
  console.log(`  Cases skipped:  ${stats.skipped}`);
  console.log(`  Errors:         ${stats.errors}`);
  console.log(`  Fields updated: ${stats.fieldsUpdated}`);
  console.log(`  Mode:           ${dryRun ? 'DRY RUN (no changes written)' : 'LIVE'}`);
}

main().catch(e => { console.error('Fatal error:', e); process.exit(1); });
