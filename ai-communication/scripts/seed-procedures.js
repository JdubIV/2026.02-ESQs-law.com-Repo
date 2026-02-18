#!/usr/bin/env node
/**
 * seed-procedures.js
 *
 * Splits procedures-manual.md into sections and seeds them into D1 `procedures` table.
 * Also caches tier-1 (always-loaded) sections in KV for fast reads.
 *
 * Usage: node scripts/seed-procedures.js
 * Re-run whenever procedures-manual.md is updated.
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const MANUAL_PATH = path.join(__dirname, '..', 'config', 'procedures-manual.md');
const DB_NAME = 'pitcher-law-memory';
const KV_NAMESPACE = 'pitcher-law-cache';

// Section metadata: section_id → { name, category, keywords, tier }
// Tier 1 = always loaded (~core), Tier 2 = loaded by intent, Tier 3 = on-demand only
const SECTION_META = {
  's0':  { name: 'Core Operating Principles', category: 'core', tier: 1,
           keywords: 'systems,lifecycle,workflow,proactive,ownership,checklist,naming,deadline,calendar' },
  's1':  { name: 'Hearing Preparation', category: 'court', tier: 2,
           keywords: 'hearing,prep,pretrial,sentencing,arraignment,motion hearing,evidentiary,court,judge' },
  's2':  { name: 'Motion Drafting', category: 'drafting', tier: 2,
           keywords: 'motion,draft,continue,compel,suppress,summary judgment,dismiss,citation,bluebook,indigo' },
  's3':  { name: 'Document Drafting General', category: 'drafting', tier: 2,
           keywords: 'draft,document,template,generate,auto-fill,noa,cover sheet' },
  's4':  { name: 'Client Intake', category: 'client', tier: 2,
           keywords: 'new client,intake,onboard,party_cache,cover sheet,case summary' },
  's5':  { name: 'Discovery', category: 'litigation', tier: 2,
           keywords: 'discovery,interrogatory,rfp,rfa,deposition,serve,respond,28 days,privilege' },
  's6':  { name: 'Court Filings and Notices', category: 'court', tier: 2,
           keywords: 'court notice,filing,minute entry,scheduling order,bench warrant,efiling,xchange' },
  's7':  { name: 'Email Handling', category: 'communication', tier: 2,
           keywords: 'email,send,reply,draft,court clerk,opposing counsel,client update' },
  's8':  { name: 'Timecard Billing', category: 'billing', tier: 2,
           keywords: 'time,billing,hours,rate,timecard,billable,cost,entry,increment' },
  's9':  { name: 'Common Tasks', category: 'core', tier: 1,
           keywords: 'calendar,prep,draft,check,file,send,log,common,just do it' },
  's10': { name: 'Utah Specific Legal Knowledge', category: 'court', tier: 2,
           keywords: 'utah,court,district,judge,statute,filing,efiling,xchange,justice court' },
  's11': { name: 'File Management', category: 'admin', tier: 2,
           keywords: 'file,folder,naming,onedrive,organize,retention,version' },
  's12': { name: 'Practice Area Procedures', category: 'litigation', tier: 2,
           keywords: 'criminal,family,civil,lifecycle,arraignment,plea,custody,complaint,trial' },
  's13': { name: 'Client Communication', category: 'communication', tier: 2,
           keywords: 'client,email,update,opposing counsel,court,communication,tone,professional' },
  's14': { name: 'Ethical Boundaries', category: 'core', tier: 1,
           keywords: 'ethics,confidential,conflict,upl,supervision,privilege' },
  's15': { name: 'Error Prevention', category: 'core', tier: 1,
           keywords: 'verify,error,check,party,case number,cross-case,contamination,missing' },
  's16': { name: 'Legal Writing and Analysis', category: 'legal_writing', tier: 2,
           keywords: 'creac,memo,brief,persuasive,writing,analysis,argument,conclusion,rule,explanation,application,counteranalysis,appellate,tone,voice,policy,transitions,synthesis,facts' },
  's17': { name: 'Legal Research and Case Analysis', category: 'research', tier: 2,
           keywords: 'research,case law,statute,standard of review,de novo,abuse of discretion,authority,hierarchy,evaluate,assess' },
  's18': { name: 'Provisional Remedies and Enforcement', category: 'litigation', tier: 3,
           keywords: 'tro,injunction,attachment,garnishment,enforcement,judgment,execution,lien' },
  's19': { name: 'Alternative Dispute Resolution', category: 'litigation', tier: 3,
           keywords: 'adr,mediation,arbitration,settlement,negotiation,demand letter' },
  's20': { name: 'Evidence Fundamentals', category: 'litigation', tier: 3,
           keywords: 'evidence,relevance,hearsay,authentication,privilege,expert,exhibit,foundation,daubert' },
};

function splitManual(filePath) {
  const raw = fs.readFileSync(filePath, 'utf-8');
  const lines = raw.split('\n');
  const sections = {};
  let currentSection = null;
  let currentLines = [];

  for (const line of lines) {
    const match = line.match(/^SECTION (\d+):/);
    if (match) {
      // Save previous section
      if (currentSection !== null) {
        sections[`s${currentSection}`] = currentLines.join('\n').trim();
      }
      currentSection = match[1];
      currentLines = [line];
    } else if (currentSection !== null) {
      // Skip separator lines
      if (/^={3,}$/.test(line.trim())) continue;
      currentLines.push(line);
    }
  }
  // Save last section
  if (currentSection !== null) {
    sections[`s${currentSection}`] = currentLines.join('\n').trim();
  }

  return sections;
}

function escapeSQL(str) {
  return str.replace(/'/g, "''");
}

async function main() {
  console.log('📋 Reading procedures manual...');
  const sections = splitManual(MANUAL_PATH);
  console.log(`   Found ${Object.keys(sections).length} sections`);

  // Clear existing rows
  console.log('🗑️  Clearing existing procedures...');
  execSync(`npx wrangler d1 execute ${DB_NAME} --remote --command "DELETE FROM procedures;"`, {
    cwd: path.join(__dirname, '..'),
    stdio: 'pipe'
  });

  // Insert each section
  for (const [sectionId, content] of Object.entries(sections)) {
    const meta = SECTION_META[sectionId];
    if (!meta) {
      console.warn(`⚠️  No metadata for ${sectionId}, skipping`);
      continue;
    }

    const sql = `INSERT INTO procedures (section_id, section_name, category, keywords, content, tier) VALUES ('${sectionId}', '${escapeSQL(meta.name)}', '${meta.category}', '${escapeSQL(meta.keywords)}', '${escapeSQL(content)}', ${meta.tier});`;

    // Write SQL to temp file to avoid shell escaping nightmares
    const tmpFile = path.join(__dirname, '..', '.tmp-proc-sql.txt');
    fs.writeFileSync(tmpFile, sql);

    try {
      execSync(`npx wrangler d1 execute ${DB_NAME} --remote --file="${tmpFile}"`, {
        cwd: path.join(__dirname, '..'),
        stdio: 'pipe'
      });
      console.log(`   ✅ ${sectionId}: ${meta.name} (${content.length} chars, tier ${meta.tier})`);
    } catch (err) {
      console.error(`   ❌ ${sectionId}: ${err.message}`);
    }

    // Clean up
    try { fs.unlinkSync(tmpFile); } catch {}
  }

  // Cache tier-1 sections in KV
  console.log('\n📦 Caching tier-1 sections in KV...');
  const tier1Content = Object.entries(sections)
    .filter(([id]) => SECTION_META[id]?.tier === 1)
    .map(([, content]) => content)
    .join('\n\n');

  const kvTmpFile = path.join(__dirname, '..', '.tmp-kv-value.txt');
  fs.writeFileSync(kvTmpFile, tier1Content);

  try {
    execSync(`npx wrangler kv key put --namespace-id=8453e5c7ce484436afd7b84b0e06d9e2 "proc:core" --path="${kvTmpFile}" --remote`, {
      cwd: path.join(__dirname, '..'),
      stdio: 'pipe'
    });
    console.log(`   ✅ KV proc:core (${tier1Content.length} chars — sections 0, 9, 14, 15)`);
  } catch (err) {
    console.error(`   ❌ KV write failed: ${err.message}`);
  }

  // Also cache individual high-use sections in KV
  const kvSections = ['s1', 's2', 's5', 's12', 's16', 's17'];
  for (const sid of kvSections) {
    if (!sections[sid]) continue;
    fs.writeFileSync(kvTmpFile, sections[sid]);
    try {
      execSync(`npx wrangler kv key put --namespace-id=8453e5c7ce484436afd7b84b0e06d9e2 "proc:${sid}" --path="${kvTmpFile}" --remote`, {
        cwd: path.join(__dirname, '..'),
        stdio: 'pipe'
      });
      console.log(`   ✅ KV proc:${sid} (${sections[sid].length} chars — ${SECTION_META[sid].name})`);
    } catch (err) {
      console.error(`   ❌ KV proc:${sid}: ${err.message}`);
    }
  }

  try { fs.unlinkSync(kvTmpFile); } catch {}

  // Summary
  const totalChars = Object.values(sections).reduce((sum, c) => sum + c.length, 0);
  const tier1Chars = Object.entries(sections)
    .filter(([id]) => SECTION_META[id]?.tier === 1)
    .reduce((sum, [, c]) => sum + c.length, 0);

  console.log('\n📊 Summary:');
  console.log(`   Total manual: ${totalChars} chars (~${Math.round(totalChars/4)} tokens)`);
  console.log(`   Tier 1 (always): ${tier1Chars} chars (~${Math.round(tier1Chars/4)} tokens)`);
  console.log(`   Savings per request: ~${Math.round((totalChars - tier1Chars)/4)} tokens when not drafting/researching`);
  console.log('✅ Done');
}

main().catch(console.error);
