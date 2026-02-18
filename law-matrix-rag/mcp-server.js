#!/usr/bin/env node

// LAW MATRIX RAG - MCP Server v2.2.0
// Embedding-based search (OpenAI text-embedding-3-small) with keyword fallback

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import fs from 'fs';
import path from 'path';
import https from 'https';
import { fileURLToPath } from 'url';
import { execFile } from 'child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─── Config ───
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
const EMBEDDING_MODEL = 'text-embedding-3-small';
const EMBED_DIMENSIONS = 1536;
const CACHE_DIR = path.join(__dirname, '.embed-cache');

// ─── State ───
let lawMatrixChunks = [];
let embeddings = []; // parallel array: embeddings[i] corresponds to lawMatrixChunks[i]
let isLoaded = false;
let embeddingsReady = false;

// ─── Document Loading ───
function findDocPath() {
  const possiblePaths = [
    path.join(__dirname, 'LAW-MATRIX-COMPLETE.txt'),
    path.join(__dirname, 'DOCUMENT INSTRUCTIONS.txt'),
    path.join(__dirname, '..', '# LAW MATRIX v6.0 - COMPLETE DEPLOY.txt'),
    path.join(__dirname, '..', 'ai-communication', 'LAW-Matrix-v4.5', 'Law Matrix', 'law-matrix-v4-5'),
  ];
  return possiblePaths.find(p => fs.existsSync(p));
}

function loadLawMatrix() {
  if (isLoaded) return true;
  const docPath = findDocPath();
  if (!docPath) return false;

  const content = fs.readFileSync(docPath, 'utf-8');
  const lines = content.split('\n');

  let currentChunk = { title: '', content: '', category: '', keywords: [] };
  let chunkIndex = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Detect section headers: "SECTION N: TITLE" preceded by ==== line
    const sectionMatch = line.match(/^SECTION\s+\d+:\s*(.+)/i);
    // Detect subsection headers: "4.N TITLE" (top-level doc type)
    const subsectionMatch = line.match(/^(\d+\.\d+)\s+(.+)/);
    // Detect sub-subsection headers: "--- 4.N.M TITLE ---"
    const subSubMatch = line.match(/^---\s+(\d+\.\d+\.\d+)\s+(.+?)\s*---/);
    // Also support markdown-style headers
    const h1Match = line.startsWith('# ') && !line.startsWith('# LAW MATRIX');
    const h2Match = line.startsWith('## ');

    if (sectionMatch || h1Match) {
      if (currentChunk.content.length > 100) {
        currentChunk.id = chunkIndex++;
        lawMatrixChunks.push({ ...currentChunk });
      }
      const title = sectionMatch ? sectionMatch[1].trim() : line.replace(/^#+\s*/, '').trim();
      currentChunk = {
        title, content: line + '\n', category: 'section',
        keywords: extractKeywords(title), lineStart: i + 1
      };
    } else if (subSubMatch) {
      // Sub-subsection (e.g., "--- 4.2.1 GENERIC MOTION TEMPLATE ---")
      if (currentChunk.content.length > 200) {
        currentChunk.id = chunkIndex++;
        lawMatrixChunks.push({ ...currentChunk });
        const title = `${subSubMatch[1]} ${subSubMatch[2].trim()}`;
        currentChunk = {
          title, content: line + '\n', category: 'document_type',
          keywords: extractKeywords(title), lineStart: i + 1, hasTemplate: true
        };
      } else {
        currentChunk.content += line + '\n';
        currentChunk.hasTemplate = true;
      }
    } else if (subsectionMatch || h2Match) {
      if (currentChunk.content.length > 200) {
        currentChunk.id = chunkIndex++;
        lawMatrixChunks.push({ ...currentChunk });
        const title = subsectionMatch ? `${subsectionMatch[1]} ${subsectionMatch[2].trim()}` : line.replace(/^#+\s*/, '').trim();
        currentChunk = {
          title, content: line + '\n', category: 'subsection',
          keywords: extractKeywords(title), lineStart: i + 1
        };
      } else {
        currentChunk.content += line + '\n';
      }
    } else {
      currentChunk.content += line + '\n';
      if (line.includes('DOCUMENT_TYPES') || line.match(/^\s+[A-Z_]+:/)) {
        currentChunk.category = 'document_type';
      }
      if (line.includes('```') || line.includes('STRUCTURE:') || line.includes('REQUIRED FIELDS:')) {
        currentChunk.hasTemplate = true;
      }
    }

    if (currentChunk.content.length > 3000) {
      currentChunk.id = chunkIndex++;
      currentChunk.keywords = [...currentChunk.keywords, ...extractKeywords(currentChunk.content)];
      lawMatrixChunks.push({ ...currentChunk });
      currentChunk = {
        title: currentChunk.title + ' (cont.)', content: '',
        category: currentChunk.category, keywords: [], lineStart: i + 1
      };
    }
  }

  if (currentChunk.content.length > 50) {
    currentChunk.id = chunkIndex++;
    currentChunk.keywords = extractKeywords(currentChunk.content);
    lawMatrixChunks.push(currentChunk);
  }

  isLoaded = true;
  return true;
}

function extractKeywords(text) {
  const words = text.toLowerCase().replace(/[^a-z0-9\s]/g, ' ').split(/\s+/).filter(w => w.length > 3);
  const legalTerms = [
    'motion', 'petition', 'complaint', 'answer', 'memorandum', 'order',
    'certificate', 'service', 'filing', 'court', 'appeals', 'commission',
    'labor', 'urcp', 'urap', 'rule', 'font', 'margin', 'spacing', 'citation',
    'bluebook', 'irac', 'template', 'format', 'deadline', 'discovery',
    'plaintiff', 'defendant', 'petitioner', 'respondent', 'judge',
    'caption', 'signature', 'dated', 'utah', 'workers', 'compensation',
    'demand', 'settlement', 'engagement', 'facesheet', 'interrogatory',
    'interrogatories', 'production', 'declaration', 'summary', 'dismiss',
    'compel', 'notice', 'appearance', 'criminal', 'family', 'civil',
    'objection', 'response', 'breach', 'injury', 'pleading', 'cover'
  ];
  return [...new Set(words.filter(w => legalTerms.includes(w) || w.length > 5))];
}

// ─── Embedding Functions ───
function getEmbedding(text) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: EMBEDDING_MODEL,
      input: text.substring(0, 8000), // trim to token limit
      dimensions: EMBED_DIMENSIONS
    });
    const req = https.request({
      hostname: 'api.openai.com',
      path: '/v1/embeddings',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${OPENAI_API_KEY}`,
        'Content-Length': Buffer.byteLength(body)
      }
    }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(d);
          if (parsed.data && parsed.data[0]) {
            resolve(parsed.data[0].embedding);
          } else {
            reject(new Error(parsed.error?.message || 'Embedding failed'));
          }
        } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function getBatchEmbeddings(texts) {
  // OpenAI supports batch embedding — send up to 100 at a time
  const results = [];
  for (let i = 0; i < texts.length; i += 50) {
    const batch = texts.slice(i, i + 50).map(t => t.substring(0, 8000));
    const body = JSON.stringify({ model: EMBEDDING_MODEL, input: batch, dimensions: EMBED_DIMENSIONS });
    const batchResult = await new Promise((resolve, reject) => {
      const req = https.request({
        hostname: 'api.openai.com',
        path: '/v1/embeddings',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${OPENAI_API_KEY}`,
          'Content-Length': Buffer.byteLength(body)
        }
      }, res => {
        let d = '';
        res.on('data', c => d += c);
        res.on('end', () => {
          try {
            const parsed = JSON.parse(d);
            if (parsed.data) {
              resolve(parsed.data.sort((a, b) => a.index - b.index).map(x => x.embedding));
            } else {
              reject(new Error(parsed.error?.message || 'Batch embedding failed'));
            }
          } catch (e) { reject(e); }
        });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
    results.push(...batchResult);
  }
  return results;
}

function cosineSimilarity(a, b) {
  let dot = 0, magA = 0, magB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    magA += a[i] * a[i];
    magB += b[i] * b[i];
  }
  return dot / (Math.sqrt(magA) * Math.sqrt(magB));
}

// ─── Embedding Cache ───
function getCachePath() {
  const docPath = findDocPath();
  if (!docPath) return null;
  const stat = fs.statSync(docPath);
  const hash = `${path.basename(docPath)}_${stat.size}_${stat.mtimeMs}`;
  return path.join(CACHE_DIR, hash.replace(/[^a-zA-Z0-9_.-]/g, '_') + '.json');
}

async function loadOrBuildEmbeddings() {
  if (!OPENAI_API_KEY || OPENAI_API_KEY === 'your-openai-api-key-here') {
    return false; // no API key, use keyword fallback
  }
  if (!isLoaded || lawMatrixChunks.length === 0) return false;

  // Check cache
  if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });
  const cachePath = getCachePath();
  if (cachePath && fs.existsSync(cachePath)) {
    try {
      embeddings = JSON.parse(fs.readFileSync(cachePath, 'utf-8'));
      if (embeddings.length === lawMatrixChunks.length) {
        embeddingsReady = true;
        return true;
      }
    } catch (e) { /* rebuild */ }
  }

  // Build embeddings
  try {
    const texts = lawMatrixChunks.map(c => `${c.title}\n${c.content}`);
    embeddings = await getBatchEmbeddings(texts);
    // Cache
    if (cachePath) {
      fs.writeFileSync(cachePath, JSON.stringify(embeddings));
    }
    embeddingsReady = true;
    return true;
  } catch (e) {
    return false; // fallback to keyword
  }
}

// ─── Search (hybrid: embedding + keyword boost) ───
async function searchSemantic(query, topK = 5) {
  if (!embeddingsReady) {
    await loadOrBuildEmbeddings();
  }

  if (embeddingsReady) {
    try {
      const queryEmbed = await getEmbedding(query);
      const scored = lawMatrixChunks.map((chunk, i) => {
        const semScore = cosineSimilarity(queryEmbed, embeddings[i]);
        // Keyword boost (small bonus for exact term matches)
        const kwBoost = keywordScore(query, chunk) * 0.05;
        return { ...chunk, score: semScore + kwBoost, semScore, method: 'embedding' };
      });
      return scored.sort((a, b) => b.score - a.score).slice(0, topK);
    } catch (e) {
      // Fall through to keyword
    }
  }

  // Keyword fallback
  return searchKeyword(query, topK);
}

function keywordScore(query, chunk) {
  const queryWords = query.toLowerCase().replace(/[^a-z0-9\s]/g, ' ').split(/\s+/).filter(w => w.length > 2);
  let score = 0;
  const contentLower = chunk.content.toLowerCase();
  const titleLower = chunk.title.toLowerCase();
  for (const word of queryWords) {
    if (titleLower.includes(word)) score += 10;
    if (chunk.keywords.some(k => k.includes(word) || word.includes(k))) score += 5;
    const matches = (contentLower.match(new RegExp(word, 'g')) || []).length;
    score += Math.min(matches * 2, 10);
    if (contentLower.includes(query.toLowerCase())) score += 20;
  }
  if (chunk.hasTemplate && queryWords.some(w => ['template', 'format', 'example'].includes(w))) score += 5;
  return score;
}

function searchKeyword(query, topK = 5) {
  if (!loadLawMatrix()) {
    return [{ title: 'Error', content: 'LAW MATRIX document not found. Place a Law Matrix source file in F:\\law-matrix-rag\\', score: 0 }];
  }
  const scored = lawMatrixChunks.map(chunk => ({
    ...chunk, score: keywordScore(query, chunk), method: 'keyword'
  }));
  return scored.filter(c => c.score > 0).sort((a, b) => b.score - a.score).slice(0, topK);
}

// ─── MCP Server ───
const server = new Server(
  { name: 'law-matrix', version: '2.2.0' },
  { capabilities: { tools: {} } }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'search_law_matrix',
      description: 'Search LAW MATRIX for Utah legal document templates, formatting rules, court procedures, workers compensation law, URCP/URAP rules, citation standards, and document types. Uses semantic (embedding) search with keyword fallback.',
      inputSchema: {
        type: 'object',
        properties: {
          query: {
            type: 'string',
            description: 'Search query - e.g., "Motion for Review template", "certificate of service", "font requirements Court of Appeals"'
          }
        },
        required: ['query']
      }
    },
    {
      name: 'get_template',
      description: 'Get a specific legal document template (Motion, Petition, Answer, Certificate of Service, etc.)',
      inputSchema: {
        type: 'object',
        properties: {
          documentType: {
            type: 'string',
            description: 'Document type - e.g., "Motion for Review", "Petition for Review", "Entry of Appearance", "Certificate of Service"'
          }
        },
        required: ['documentType']
      }
    },
    {
      name: 'get_formatting_rules',
      description: 'Get formatting requirements for Utah courts',
      inputSchema: {
        type: 'object',
        properties: {
          court: {
            type: 'string',
            description: 'Court name - "Labor Commission", "Court of Appeals", or "District Court"'
          }
        },
        required: ['court']
      }
    },
    {
      name: 'list_document_types',
      description: 'List all available document types in LAW MATRIX with their codes and requirements',
      inputSchema: {
        type: 'object',
        properties: {}
      }
    },
    {
      name: 'generate_document',
      description: 'Generate a filled legal document from a template. Supports .docx and .pdf output for ALL templates. Cover-sheet uses a dedicated HTML renderer; all others use DOCX→mammoth→Puppeteer pipeline. Templates: noa, motion-generic, motion-compel, motion-virtual, interrogatories, requests-production, cover-sheet, declaration, order, petition. Aliases: rfp, compel, virtual, rogs, appearance, facesheet, case-summary, summary, intake.',
      inputSchema: {
        type: 'object',
        properties: {
          template: {
            type: 'string',
            description: 'Template ID or alias (e.g., "noa", "motion-compel", "rfp", "cover-sheet", "case-summary")'
          },
          data: {
            type: 'object',
            description: 'Field values to populate (e.g., {"CLIENT_NAME": "Jane Smith", "CASE_NUMBER": "261100999", "JUDGE_NAME": "Judge Cannell"}). Firm defaults auto-fill if not provided.',
            additionalProperties: { type: 'string' }
          },
          format: {
            type: 'string',
            enum: ['docx', 'pdf', 'both'],
            description: 'Output format. "pdf" generates PDF only. "both" generates .docx + .pdf. Available for ALL templates. Default: "docx".'
          },
          outputPath: {
            type: 'string',
            description: 'Optional output file path. Defaults to Desktop with template name and timestamp.'
          }
        },
        required: ['template', 'data']
      }
    },
    {
      name: 'list_templates',
      description: 'List all available document templates with their IDs, descriptions, required fields, and categories.',
      inputSchema: {
        type: 'object',
        properties: {}
      }
    }
  ]
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  // Ensure document is loaded
  loadLawMatrix();

  try {
    switch (name) {
      case 'search_law_matrix': {
        const results = await searchSemantic(args.query, 5);
        const formatted = results.map((r, i) =>
          `## [${i + 1}] ${r.title}\n**Score:** ${r.score.toFixed(4)} | **Method:** ${r.method || 'keyword'} | **Category:** ${r.category}\n\n${r.content}`
        ).join('\n\n---\n\n');
        return { content: [{ type: 'text', text: formatted || 'No results found.' }] };
      }

      case 'get_template': {
        const results = await searchSemantic(`${args.documentType} template complete format`, 3);
        const content = results
          .filter(r => r.hasTemplate || r.content.includes('```') || r.content.includes('STRUCTURE:') || r.content.includes('REQUIRED FIELDS:'))
          .map(r => `## ${r.title}\n\n${r.content}`)
          .join('\n\n---\n\n');
        return { content: [{ type: 'text', text: content || `No template found for "${args.documentType}"` }] };
      }

      case 'get_formatting_rules': {
        const results = await searchSemantic(`${args.court} formatting font margin spacing typography requirements`, 5);
        const content = results.map(r => `## ${r.title}\n\n${r.content}`).join('\n\n---\n\n');
        return { content: [{ type: 'text', text: content || 'No formatting rules found.' }] };
      }

      case 'list_document_types': {
        const results = await searchSemantic('DOCUMENT_TYPES code title category', 10);
        const content = results.map(r => r.content).join('\n\n');
        return { content: [{ type: 'text', text: content || 'Document types not found.' }] };
      }

      case 'generate_document': {
        const generatorScript = path.resolve(__dirname, '..', 'ai-communication', 'scripts', 'generate-document.js');
        const pdfScript = path.resolve(__dirname, '..', 'ai-communication', 'scripts', 'generate-cover-sheet-pdf.js');
        const docxToPdfScript = path.resolve(__dirname, '..', 'ai-communication', 'scripts', 'docx-to-pdf.js');
        if (!fs.existsSync(generatorScript)) {
          return { content: [{ type: 'text', text: `Error: Template generator not found at ${generatorScript}` }], isError: true };
        }

        const templateId = args.template;
        const data = args.data || {};
        const format = args.format || 'docx';
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        const desktop = path.join(process.env.USERPROFILE || process.env.HOME || '.', 'Desktop');
        const isCoverSheet = ['cover-sheet', 'cover', 'facesheet', 'case-summary', 'summary', 'intake'].includes(templateId.toLowerCase());
        const wantPdf = format === 'pdf' || format === 'both';

        const results = [];

        // Step 1: Always generate DOCX first (needed as intermediate for PDF)
        const docxOutput = args.outputPath || path.join(desktop, `${templateId}-${timestamp}.docx`);
        const cliArgs = [generatorScript, '--template', templateId, '--data', JSON.stringify(data), '--output', docxOutput];

        const docxResult = await new Promise((resolve) => {
          execFile('node', cliArgs, { timeout: 30000, cwd: path.dirname(generatorScript) }, (error, stdout, stderr) => {
            if (error) {
              resolve({ ok: false, text: `DOCX generation failed:\n${stderr || error.message}` });
            } else {
              resolve({ ok: true, text: `DOCX: ${docxOutput}`, path: docxOutput });
            }
          });
        });

        if (!docxResult.ok) {
          return { content: [{ type: 'text', text: docxResult.text }], isError: true };
        }

        if (format !== 'pdf') {
          results.push(docxResult);
        }

        // Step 2: Generate PDF if requested
        if (wantPdf) {
          const pdfOutput = docxOutput.replace(/\.docx$/i, '.pdf');

          if (isCoverSheet && fs.existsSync(pdfScript)) {
            // Cover sheet: use dedicated HTML renderer for best quality
            const pdfArgs = [pdfScript, '--data', JSON.stringify(data), '--output', pdfOutput];
            const pdfResult = await new Promise((resolve) => {
              execFile('node', pdfArgs, { timeout: 60000, cwd: path.dirname(pdfScript) }, (error, stdout, stderr) => {
                if (error) {
                  resolve({ ok: false, text: `PDF generation failed:\n${stderr || error.message}` });
                } else {
                  resolve({ ok: true, text: `PDF: ${pdfOutput}`, path: pdfOutput });
                }
              });
            });
            results.push(pdfResult);
          } else if (fs.existsSync(docxToPdfScript)) {
            // All other templates: DOCX → mammoth → Puppeteer → PDF
            const pdfArgs = [docxToPdfScript, '--input', docxOutput, '--output', pdfOutput];
            const pdfResult = await new Promise((resolve) => {
              execFile('node', pdfArgs, { timeout: 60000, cwd: path.dirname(docxToPdfScript) }, (error, stdout, stderr) => {
                if (error) {
                  resolve({ ok: false, text: `PDF generation failed:\n${stderr || error.message}` });
                } else {
                  resolve({ ok: true, text: `PDF: ${pdfOutput}`, path: pdfOutput });
                }
              });
            });
            results.push(pdfResult);
          }

          // If pdf-only, remove the DOCX
          if (format === 'pdf' && fs.existsSync(docxOutput)) {
            try { fs.unlinkSync(docxOutput); } catch (e) { /* ignore */ }
          }
        }

        const output = results.map(r => r.text).join('\n');
        const hasError = results.some(r => !r.ok);
        return { content: [{ type: 'text', text: `Document generation complete.\n\n${output}` }], isError: hasError };
      }

      case 'list_templates': {
        const registryPath = path.resolve(__dirname, '..', 'ai-communication', 'config', 'template-registry.json');
        if (!fs.existsSync(registryPath)) {
          return { content: [{ type: 'text', text: 'Template registry not found.' }], isError: true };
        }
        const registry = JSON.parse(fs.readFileSync(registryPath, 'utf-8'));
        const lines = ['# Available Document Templates\n'];

        for (const [id, tmpl] of Object.entries(registry.templates)) {
          lines.push(`## ${id}`);
          lines.push(`- **File:** ${tmpl.file}`);
          lines.push(`- **Description:** ${tmpl.description}`);
          lines.push(`- **Category:** ${tmpl.category}`);
          lines.push(`- **Required Fields:** ${tmpl.requiredFields.join(', ')}`);
          if (tmpl.optionalFields?.length) lines.push(`- **Optional Fields:** ${tmpl.optionalFields.join(', ')}`);
          lines.push('');
        }

        if (registry.aliases) {
          lines.push('## Aliases');
          for (const [alias, target] of Object.entries(registry.aliases)) {
            lines.push(`- \`${alias}\` → ${target}`);
          }
        }

        return { content: [{ type: 'text', text: lines.join('\n') }] };
      }

      default:
        return { content: [{ type: 'text', text: `Unknown tool: ${name}` }], isError: true };
    }
  } catch (error) {
    return { content: [{ type: 'text', text: `Error: ${error.message}` }], isError: true };
  }
});

// Start
const transport = new StdioServerTransport();
await server.connect(transport);
