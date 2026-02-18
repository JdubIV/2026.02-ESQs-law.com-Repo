/**
 * procedures-loader.js
 *
 * Smart procedures injection for Synthia.
 * Instead of loading the entire 1200-line manual into every prompt,
 * this module loads only the sections relevant to the current task.
 *
 * Architecture:
 *   Tier 1 (always loaded): Core principles, common tasks, ethics, error prevention
 *   Tier 2 (by intent):     Hearing prep, motions, discovery, writing, research, etc.
 *   Tier 3 (on-demand):     Provisional remedies, ADR, evidence (rare tasks)
 *
 * Storage:
 *   - KV (proc:core, proc:s1, etc.) for sub-ms reads on hot sections
 *   - D1 (procedures table) for keyword-based lookups on cold sections
 *   - Local file fallback if both fail
 *
 * Usage:
 *   const { loadProcedures } = require('./procedures-loader');
 *   const procedures = await loadProcedures(userMessage, { activeClient, documentAction, ... });
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const DB_NAME = 'pitcher-law-memory';
const KV_NAMESPACE_ID = '8453e5c7ce484436afd7b84b0e06d9e2';
const MANUAL_PATH = path.join(__dirname, '..', '..', 'config', 'procedures-manual.md');

// In-memory cache (per-process, clears on restart)
const CACHE = {};
const CACHE_TTL = 10 * 60 * 1000; // 10 minutes

// --- Intent → Section mapping ---
// Maps detected intents (from bridges.js) and keyword patterns to section IDs

const INTENT_SECTIONS = {
  // Action intents (from bridges.js detection)
  // s_ = procedures, p_ = paralegal, a_ = secretary, t_ = attorney
  hearing:     ['s1', 's10', 'p3', 'p4', 'a1'],
  motion:      ['s2', 's16', 't4', 't5'],
  draft:       ['s2', 's3', 's16', 't4'],
  document:    ['s3', 's16', 'p5'],
  intake:      ['s4', 'p6', 'a6'],
  discovery:   ['s5', 's12', 'p2', 'p4'],
  filing:      ['s6', 's10', 'p5', 'a5'],
  email:       ['s7', 's13', 'p8', 'a2'],
  billing:     ['s8', 'p7'],
  calendar:    ['s1', 'p4', 'a1'],
  file_mgmt:   ['s11', 'p5', 'a5'],
  criminal:    ['s12', 'p9', 't5'],
  family:      ['s12', 'p10', 't5'],
  civil:       ['s12', 'p1', 't5'],
  research:    ['s17', 't1'],
  brief:       ['s16', 's17', 't4'],
  memo:        ['s16', 't4'],
  evidence:    ['s20', 't7'],
  settlement:  ['s19', 't2'],
  injunction:  ['s18', 't5'],
  enforcement: ['s18', 't5'],
  // Paralegal-specific intents
  case_status: ['p1', 'p11', 'a3'],
  case_prep:   ['p1', 'p3', 'p11', 't7'],
  proactive:   ['p11', 'p4', 'a3'],
  // Secretary-specific intents
  phone:       ['a4'],
  task_mgmt:   ['a3', 'p11'],
  triage:      ['a2', 'a5'],
  coordinate:  ['a6', 'a1'],
  // Attorney-specific intents
  strategy:    ['t2', 't3'],
  ethics:      ['t6'],
  trial:       ['t7', 'p3'],
  rules:       ['t5'],
};

// Keyword patterns for fallback detection
const KEYWORD_PATTERNS = [
  { pattern: /\b(hear|pretrial|sentenc|arraign|court\s*date)/i,        intent: 'hearing' },
  { pattern: /\b(motion|compel|suppress|dismiss|continue|summary\s*judgment)/i, intent: 'motion' },
  { pattern: /\b(draft|generate|create|prepare)\b.*\b(motion|document|brief|memo|noa|petition|order|declaration)/i, intent: 'draft' },
  { pattern: /\b(brief|appellate|appeal|standard\s*of\s*review)/i,    intent: 'brief' },
  { pattern: /\b(memo|memorandum|analyze|analysis|creac|question\s*presented)/i, intent: 'memo' },
  { pattern: /\b(new\s*client|intake|onboard)/i,                      intent: 'intake' },
  { pattern: /\b(discovery|interrogator|rfp|rfa|deposition|subpoena)/i, intent: 'discovery' },
  { pattern: /\b(court\s*notice|filing|minute\s*entry|docket|efil)/i, intent: 'filing' },
  { pattern: /\b(email|send|reply|forward|draft.*email)/i,            intent: 'email' },
  { pattern: /\b(time|hours|billing|invoice|timecard|log\s*time)/i,   intent: 'billing' },
  { pattern: /\b(calendar|schedule|what.*today|what.*tomorrow|what.*this\s*week)/i, intent: 'calendar' },
  { pattern: /\b(file|folder|onedrive|organiz|naming\s*convention)/i, intent: 'file_mgmt' },
  { pattern: /\b(criminal|felony|misdemeanor|plea|probation|expunge)/i, intent: 'criminal' },
  { pattern: /\b(family|divorce|custody|alimony|child\s*support|visitation)/i, intent: 'family' },
  { pattern: /\b(civil|complaint|answer|counterclaim|tort|damages)/i, intent: 'civil' },
  { pattern: /\b(research|case\s*law|find\s*cases|statute|authority)/i, intent: 'research' },
  { pattern: /\b(evidence|hearsay|relevance|authenticate|exhibit|expert\s*testimony|daubert)/i, intent: 'evidence' },
  { pattern: /\b(settle|mediat|arbitrat|adr|demand\s*letter)/i,       intent: 'settlement' },
  { pattern: /\b(tro|injunction|restraining\s*order|preliminary)/i,   intent: 'injunction' },
  { pattern: /\b(enforce|garnish|execution|judgment\s*lien|writ\s*of)\b/i, intent: 'enforcement' },
  // Paralegal-specific patterns
  { pattern: /\b(case\s*status|where\s*(is|are)\s*(the\s*)?case|lifecycle|what\s*phase)/i, intent: 'case_status' },
  { pattern: /\b(prep\s*for|prepare\s*for|get\s*ready|ready\s*for|what.*need.*(do|prepare|file))/i, intent: 'case_prep' },
  { pattern: /\b(what.*due|what.*overdue|anything\s*pending|status\s*update|check\s*on|review\s*cases)/i, intent: 'proactive' },
  { pattern: /\b(protective\s*order|cohabitant|domestic|modification|parent\s*time|parenting\s*plan)/i, intent: 'family' },
  { pattern: /\b(arraign|bail|preliminary\s*hear|speedy\s*trial|expung)/i, intent: 'criminal' },
  { pattern: /\b(conflict\s*check|new\s*matter|open\s*case|set\s*up\s*case|case\s*setup)/i, intent: 'intake' },
  // Secretary-specific patterns
  { pattern: /\b(who\s*called|phone|text|sms|voicemail|contact.*look\s*up|phone\s*number)/i, intent: 'phone' },
  { pattern: /\b(task|to\s*do|action\s*item|assign|follow\s*up|open\s*items|what.*pending)/i, intent: 'task_mgmt' },
  { pattern: /\b(triage|urgent|prioriti|sort.*email|process.*mail|incoming)/i, intent: 'triage' },
  { pattern: /\b(zoom|book|reserve|arrange|logistics|coordinate|set\s*up.*meeting)/i, intent: 'coordinate' },
  // Attorney-specific patterns
  { pattern: /\b(strateg|evaluate.*case|assess.*risk|chance|likelihood|odds|pros?\s*and\s*cons?|should\s*we)/i, intent: 'strategy' },
  { pattern: /\b(ethic|conflict\s*of\s*interest|privilege|confidential|malpractice|rpc|professional\s*conduct)/i, intent: 'ethics' },
  { pattern: /\b(trial|exhibit|witness|testimony|jury|bench\s*trial|voir\s*dire|opening\s*statement)/i, intent: 'trial' },
  { pattern: /\b(rule\s*\d|urcp|urcrp|urap|ure|utah\s*(code|rule)|local\s*rule)/i, intent: 'rules' },
];

/**
 * Detect intents from the user message
 * @param {string} message - User's message
 * @param {Object} context - Optional context from bridges.js (documentAction, emailAction, etc.)
 * @returns {string[]} Array of detected intents
 */
function detectIntents(message, context = {}) {
  const intents = new Set();

  // Check bridges.js action detections first (most reliable)
  if (context.documentAction === 'generate') intents.add('draft');
  if (context.emailAction === 'compose') intents.add('email');
  if (context.emailAction === 'process-notices') { intents.add('email'); intents.add('calendar'); intents.add('triage'); }
  if (context.deadlineAction) intents.add('calendar');
  if (context.courtLookupAction) intents.add('filing');
  if (context.fileSearchAction) intents.add('file_mgmt');
  if (context.zoomAction) intents.add('calendar');

  // Keyword pattern matching on the message
  if (message) {
    for (const { pattern, intent } of KEYWORD_PATTERNS) {
      if (pattern.test(message)) {
        intents.add(intent);
      }
    }
  }

  return [...intents];
}

/**
 * Get required section IDs for the detected intents
 * @param {string[]} intents
 * @returns {string[]} Unique section IDs to load
 */
function getSectionIds(intents) {
  const ids = new Set();

  for (const intent of intents) {
    const sections = INTENT_SECTIONS[intent];
    if (sections) {
      sections.forEach(id => ids.add(id));
    }
  }

  return [...ids];
}

/**
 * Read a section from KV cache
 */
function readKV(key) {
  try {
    const result = execSync(
      `npx wrangler kv key get --namespace-id=${KV_NAMESPACE_ID} "${key}" --remote --text`,
      { cwd: path.join(__dirname, '..', '..'), stdio: ['pipe', 'pipe', 'pipe'], timeout: 5000 }
    );
    return result.toString().trim();
  } catch {
    return null;
  }
}

/**
 * Read sections from D1
 */
function readD1(sectionIds) {
  if (!sectionIds.length) return {};
  const placeholders = sectionIds.map(id => `'${id}'`).join(',');
  try {
    const result = execSync(
      `npx wrangler d1 execute ${DB_NAME} --remote --command "SELECT section_id, content FROM procedures WHERE section_id IN (${placeholders});"`,
      { cwd: path.join(__dirname, '..', '..'), stdio: ['pipe', 'pipe', 'pipe'], timeout: 10000 }
    );
    const parsed = JSON.parse(result.toString());
    const rows = parsed?.[0]?.results || [];
    const map = {};
    for (const row of rows) {
      map[row.section_id] = row.content;
    }
    return map;
  } catch {
    return {};
  }
}

/**
 * Fallback: read sections from local file
 */
function readLocalSections(sectionIds) {
  try {
    const raw = fs.readFileSync(MANUAL_PATH, 'utf-8');
    const lines = raw.split('\n');
    const sections = {};
    let currentId = null;
    let currentLines = [];

    for (const line of lines) {
      const match = line.match(/^SECTION (\d+):/);
      if (match) {
        if (currentId !== null) {
          sections[`s${currentId}`] = currentLines.join('\n').trim();
        }
        currentId = match[1];
        currentLines = [line];
      } else if (currentId !== null) {
        if (/^={3,}$/.test(line.trim())) continue;
        currentLines.push(line);
      }
    }
    if (currentId !== null) {
      sections[`s${currentId}`] = currentLines.join('\n').trim();
    }

    // Return only requested sections
    const result = {};
    for (const id of sectionIds) {
      if (sections[id]) result[id] = sections[id];
    }
    return result;
  } catch {
    return {};
  }
}

/**
 * Main entry point: load relevant procedures for the current message
 *
 * @param {string} message - The user's message
 * @param {Object} context - Action context from bridges.js intent detection
 * @returns {Promise<string>} Procedures text to inject into the system prompt
 */
async function loadProcedures(message, context = {}) {
  const now = Date.now();

  // Tier-1 section IDs (always loaded)
  const TIER1_IDS = ['s0', 's9', 's14', 's15'];

  // 1. Always load tier-1 (core) from in-memory cache or D1
  let coreContent = CACHE['proc:core'];
  if (!coreContent || (now - (CACHE['proc:core:ts'] || 0)) > CACHE_TTL) {
    const d1Core = readD1(TIER1_IDS);
    const coreArr = TIER1_IDS.map(id => d1Core[id]).filter(Boolean);
    if (coreArr.length > 0) {
      coreContent = coreArr.join('\n\n');
    } else {
      // D1 failed — fall back to local file
      const localCore = readLocalSections(TIER1_IDS);
      coreContent = TIER1_IDS.map(id => localCore[id]).filter(Boolean).join('\n\n');
    }
    if (coreContent) {
      CACHE['proc:core'] = coreContent;
      CACHE['proc:core:ts'] = now;
    }
  }

  // 2. Detect intents and determine which tier-2/3 sections to load
  const intents = detectIntents(message, context);
  const sectionIds = getSectionIds(intents).filter(id => !TIER1_IDS.includes(id));

  // 3. Load requested sections (try in-memory cache → D1 → local file)
  const sectionContents = [];
  const uncachedIds = [];

  for (const id of sectionIds) {
    const cacheKey = `proc:${id}`;
    if (CACHE[cacheKey] && (now - (CACHE[`${cacheKey}:ts`] || 0)) < CACHE_TTL) {
      sectionContents.push(CACHE[cacheKey]);
    } else {
      uncachedIds.push(id);
    }
  }

  if (uncachedIds.length > 0) {
    // D1 lookup (single query for all needed sections)
    const d1Results = readD1(uncachedIds);
    for (const id of uncachedIds) {
      if (d1Results[id]) {
        CACHE[`proc:${id}`] = d1Results[id];
        CACHE[`proc:${id}:ts`] = now;
        sectionContents.push(d1Results[id]);
      }
    }

    // Local file fallback for anything D1 missed
    const missingIds = uncachedIds.filter(id => !d1Results[id]);
    if (missingIds.length > 0) {
      const localResults = readLocalSections(missingIds);
      for (const id of missingIds) {
        if (localResults[id]) {
          sectionContents.push(localResults[id]);
        }
      }
    }
  }

  // 4. Assemble final procedures text
  const parts = [];
  if (coreContent) parts.push(coreContent);
  parts.push(...sectionContents);

  if (parts.length === 0) {
    // Ultimate fallback — load the whole file (shouldn't happen)
    try {
      const raw = fs.readFileSync(MANUAL_PATH, 'utf-8');
      return raw.split('\n')
        .filter(l => !l.startsWith('#') || l.startsWith('##'))
        .join('\n')
        .replace(/={3,}/g, '---')
        .replace(/\n{3,}/g, '\n\n')
        .trim();
    } catch {
      return '';
    }
  }

  const result = parts.join('\n\n');

  // Log what we loaded
  const loadedIds = ['core', ...sectionIds].join(', ');
  console.log(`📋 Procedures loaded: [${loadedIds}] (${result.length} chars, ~${Math.round(result.length/4)} tokens, intents: ${intents.join(',')||'none'})`);

  return result;
}

/**
 * Force-load specific sections by ID (for testing or explicit requests)
 */
async function loadSpecificSections(sectionIds) {
  const results = readD1(sectionIds);
  const missing = sectionIds.filter(id => !results[id]);
  if (missing.length > 0) {
    const local = readLocalSections(missing);
    Object.assign(results, local);
  }
  return Object.values(results).join('\n\n');
}

module.exports = { loadProcedures, loadSpecificSections, detectIntents, getSectionIds };
