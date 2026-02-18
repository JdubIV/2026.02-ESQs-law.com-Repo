/**
 * THE BRIDGE - Multi-AI Chat Routes
 * Migrated from DEPLOY-CONSOLIDATED.js with new architecture
 */

const express = require('express');
const router = express.Router();
const { AIService } = require('../../services/ai-service');
const { cacheMiddleware } = require('../../middleware/cache');
const CaseQueryAgent = require('../../agents/case-query-agent');
const AgentRegistry = require('../../core/agent-registry');
const { getDailyContext } = require('../../lib/daily-context');
const emailClient = require('../../lib/email-client');
const emailArchiver = require('../../lib/email-archiver');
const axios = require('axios');
const path = require('path');
const fs = require('fs');

// Document generation
const GENERATE_DOC_PATH = path.join(__dirname, '../../../scripts/generate-document.js');
const GENERATE_PDF_PATH = path.join(__dirname, '../../../scripts/generate-cover-sheet-pdf.js');
const graphClient = require('../../lib/graph-client');
const { findClientFolder } = require('../../lib/email-archiver');
const outlookCal = require('../../lib/outlook-calendar');

// Smart procedures loader — loads only sections relevant to current task
const { loadProcedures } = require('../../lib/procedures-loader');
console.log('📋 Procedures loader initialized (smart section routing)');

/**
 * Parse structured email content from an AI consensus response.
 * Handles multiple formatting styles the AI might use:
 *   - "Subject: ...\n\nDear ...\n...\nSincerely,..."
 *   - "**To:** ...\n**Subject:** ...\n\n..."
 *   - Markdown-fenced email blocks
 * Returns { to, subject, body } or null if no email found.
 */
function parseEmailFromResponse(text) {
  if (!text) return null;

  let to = null, subject = null, body = null;

  // Strategy 1: Explicit "To:" and "Subject:" headers (Markdown bold or plain)
  const toMatch = text.match(/\*{0,2}To:\*{0,2}\s*([^\n]+)/i);
  const subjectMatch = text.match(/\*{0,2}Subject:\*{0,2}\s*([^\n]+)/i);

  if (subjectMatch) {
    subject = subjectMatch[1].replace(/\*+/g, '').trim();
  }
  if (toMatch) {
    // Extract email address from "Name <email>" or plain email
    const emailInTo = toMatch[1].match(/[\w.-]+@[\w.-]+\.\w+/);
    to = emailInTo ? emailInTo[0] : toMatch[1].replace(/\*+/g, '').trim();
  }

  // Strategy 2: Look for a salutation-based email body
  // "Dear ...,\n...\nSincerely," or "Hello ...,\n...\nBest regards,"
  const salutationPattern = /((?:Dear|Hello|Hi|Good (?:morning|afternoon|evening))[^\n]*,?\n)([\s\S]*?)((?:Sincerely|Best regards?|Regards|Thank you|Respectfully|Warm regards|Very truly yours)[,\s]*\n[\s\S]{0,200}(?:ESQs Law|Pitcher Law|John W\. Adams|Diane Pitcher|pd@dianepitcher\.com)[\s\S]{0,100})/i;
  const bodyMatch = text.match(salutationPattern);

  if (bodyMatch) {
    body = (bodyMatch[1] + bodyMatch[2] + bodyMatch[3]).trim();
    // Wrap in basic HTML
    body = '<div style="font-family: Calibri, Arial, sans-serif; font-size: 11pt;">'
      + body.replace(/\n/g, '<br>')
      + '</div>';
  }

  // Strategy 3: If we have a subject but no body, grab everything after "Subject:" until end or next section
  if (subject && !body) {
    const afterSubject = text.substring(text.indexOf(subjectMatch[0]) + subjectMatch[0].length).trim();
    if (afterSubject.length > 20) {
      body = '<div style="font-family: Calibri, Arial, sans-serif; font-size: 11pt;">'
        + afterSubject.replace(/\n/g, '<br>')
        + '</div>';
    }
  }

  // Only return if we have at minimum a subject and body
  if (subject && body) {
    return { to, subject, body };
  }

  return null;
}

/**
 * Parse structured document generation data from AI response.
 * Looks for ===DOCUMENT=== ... ===END_DOCUMENT=== blocks.
 * Returns { templateId, data } or null if no document block found.
 */
function parseDocumentFromResponse(text) {
  if (!text) return null;

  const docMatch = text.match(/===DOCUMENT===([\s\S]*?)===END_DOCUMENT===/);
  if (!docMatch) return null;

  const block = docMatch[1].trim();
  const lines = block.split('\n').map(l => l.trim()).filter(l => l);

  let templateId = null;
  const data = {};
  let inData = false;

  for (const line of lines) {
    const templateMatch = line.match(/^TEMPLATE:\s*(.+)/i);
    if (templateMatch) {
      templateId = templateMatch[1].trim().toLowerCase();
      continue;
    }
    if (/^DATA:\s*$/i.test(line)) {
      inData = true;
      continue;
    }
    // Any KEY: VALUE line after TEMPLATE or DATA
    const kvMatch = line.match(/^([A-Z][A-Z0-9_]+):\s*(.+)/);
    if (kvMatch) {
      inData = true;
      data[kvMatch[1]] = kvMatch[2].trim();
    }
  }

  if (templateId && Object.keys(data).length > 0) {
    return { templateId, data };
  }
  return null;
}

/**
 * Parse structured deadline management data from AI response.
 * Looks for ===DEADLINE=== ... ===END_DEADLINE=== blocks.
 * Returns { action, clientName, dueDate, hearingTime, deadlineType, court, judge, courtroom, notes } or null.
 */
function parseDeadlineFromResponse(text) {
  if (!text) return null;

  const dlMatch = text.match(/===DEADLINE===([\s\S]*?)===END_DEADLINE===/);
  if (!dlMatch) return null;

  const block = dlMatch[1].trim();
  const fields = {};

  for (const line of block.split('\n')) {
    const m = line.trim().match(/^([A-Z_]+):\s*(.+)/);
    if (m) fields[m[1]] = m[2].trim();
  }

  if (!fields.ACTION || !fields.CLIENT_NAME) return null;

  return {
    action: fields.ACTION.toLowerCase(),
    clientName: fields.CLIENT_NAME,
    dueDate: fields.DUE_DATE || null,
    hearingTime: fields.HEARING_TIME || null,
    deadlineType: fields.DEADLINE_TYPE || 'Calendar Event',
    court: fields.COURT || null,
    judge: fields.JUDGE || null,
    courtroom: fields.COURTROOM || null,
    notes: fields.NOTES || null
  };
}

// LAZY INITIALIZATION - Only create when first needed
let aiService, agentRegistry, caseQueryAgent;
function getServices() {
  if (!aiService) {
    aiService = new AIService();
    agentRegistry = new AgentRegistry();
    caseQueryAgent = new CaseQueryAgent({
      id: 'case-query',
      name: 'Case Query Agent'
    }, agentRegistry);
  }
  return { aiService, agentRegistry, caseQueryAgent };
}

/**
 * Synthesize consensus from multiple AI responses
 */
async function synthesizeConsensus(responses) {
  const validResponses = responses.filter(r => r.success);
  
  if (validResponses.length === 0) {
    return {
      consensus: 'No AI services responded successfully.',
      sources: 0,
      confidence: 0
    };
  }

  // Claude is the lead voice. Grok is second. GPT-4o is last resort.
  const { aiService: ai } = getServices();
  let synthesizerModel = 'claude';

  if (!ai.isAvailable('claude')) {
    if (ai.isAvailable('xai')) synthesizerModel = 'xai';       // Grok is second in line
    else if (ai.isAvailable('gpt4o')) synthesizerModel = 'gpt4o'; // GPT-4o last resort
    else {
      const synthesizer = validResponses.find(r => r.ai.includes('Grok') || r.ai.includes('GPT'));
      if (synthesizer) {
        if (synthesizer.ai.includes('Grok')) synthesizerModel = 'xai';
        else if (synthesizer.ai.includes('GPT')) synthesizerModel = 'gpt4o';
      }
    }
  }

  if (ai.isAvailable(synthesizerModel)) {
    try {
      const synthesisSystem = `You are Synthia, AI Legal Assistant for ESQs Law (Pitcher Law PLLC). You are synthesizing research from ${validResponses.length} AI sources into one authoritative answer.

YOUR VOICE:
- Sharp, direct, genuinely helpful. Think like a colleague on the legal team, not a search engine.
- Synthesize — don't just list what each source said. Find the answer, present it clearly, note disagreements only if material.
- Structure long answers with clear headers. Keep short answers short.
- Zero fabrication. If the research doesn't support a claim, don't make it. Say what's known and what isn't.

CONTEXT:
- Utah law practice — criminal defense and family law. 1st District primary.
- Attorneys: John W. Adams III (Bar #19429, primary), Diane Pitcher (Bar #12626).
- We represent client_name, NEVER the opposing party.
- Cite Utah Code / URCP when applicable. Flag risks and deadline issues proactively.`;

      const synthesisPrompt = `## Research Inputs
${validResponses.map((r, i) => `${i + 1}. ${r.ai}:\n${r.response.message || r.response.content}`).join('\n\n')}

Synthesize the above into one clear, actionable Synthia response.`;

      const consensus = await ai.query(synthesizerModel, synthesisPrompt, { maxTokens: 4096, temperature: 0, systemPrompt: synthesisSystem });
      
      return {
        consensus: consensus.content || consensus.message,
        sources: validResponses.length,
        confidence: validResponses.length >= 4 ? 0.9 : validResponses.length >= 2 ? 0.7 : 0.5
      };
    } catch (error) {
      console.error('Consensus synthesis error:', error);
    }
  }

  // Fallback: use first response
  return {
    consensus: validResponses[0].response.message || validResponses[0].response.content,
    sources: validResponses.length,
    confidence: validResponses.length >= 4 ? 0.9 : validResponses.length >= 2 ? 0.7 : 0.5
  };
}

/**
 * THE BRIDGE - Multi-AI Chat
 * POST /api/bridges/message
 */
router.post('/message', cacheMiddleware('aiResponse'), async (req, res) => {
  try {
    const { message, fileUrl, fileName, userId, sessionId, context, clientName, dashboardState } = req.body;

    if (!message && !fileUrl) {
      return res.status(400).json({ 
        success: false, 
        error: 'Either message or fileUrl required' 
      });
    }

    // ============================================
    // CORRECT WORKFLOW: AI → RAG → Memory Banks → Tokens
    // ============================================
    
    // Step 1: Get RAG Engine and Memory Bank from app locals
    const ragEngine = req.app.locals.ragEngine;
    const memoryBank = req.app.locals.memoryBank;
    const firmMemory = req.app.locals.firmMemory;
    
    // Step 2: Retrieve from Memory Banks FIRST (before AI queries)
    let ragContext = [];
    let memoryContext = [];
    
    // Extract client name — PRIORITY: 1) dashboardState.activeClient, 2) explicit clientName param, 3) message extraction
    let extractedClientName = dashboardState?.activeClient || clientName;
    if (!extractedClientName && message) {
      // Try to extract client name from message
      const patterns = [
        /(?:for|about|regarding)\s+(\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b)/i,
        /(?:report|status|update|info|data|case|client).*?(\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b)/i
      ];

      for (const pattern of patterns) {
        const match = message.match(pattern);
        if (match && match[1] && match[1].length > 2) {
          // Filter out non-name words
          const nonNames = ['Pre', 'Trial', 'Conference', 'Pretrial', 'Virtual', 'Court', 'Justice',
                            'Logan', 'Draft', 'Motion', 'Good', 'Morning', 'Open', 'Cases', 'Files',
                            'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday',
                            'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
                            'September', 'October', 'November', 'December', 'The', 'This', 'That',
                            'Ready', 'Need', 'Help', 'What', 'When', 'Where', 'How', 'Show'];
          if (!nonNames.includes(match[1].split(' ')[0])) {
            extractedClientName = match[1];
            break;
          }
        }
      }
    }
    console.log(`👤 Client resolution: dashboard="${dashboardState?.activeClient || 'none'}" param="${clientName || 'none'}" extracted="${extractedClientName || 'none'}"`);

    if (ragEngine && message) {
      try {
        // Build a more targeted query if we have a client name
        let ragQuery = message;
        if (extractedClientName) {
          ragQuery = `client ${extractedClientName} ${message}`;
        }
        
        const ragResults = await ragEngine.retrieve({
          query: ragQuery,
          sources: ['client_data', 'client_history', 'case_summaries', 'client_documents', 'knowledge_artifacts'],
          filters: extractedClientName ? { client_name: extractedClientName } : {},
          limit: 10, // Increase limit for better matching
          minScore: 0.4 // Lower threshold for partial matches like "alfua" -> "Alfau"
        });
        ragContext = ragResults.chunks || [];
        console.log(`📚 RAG retrieved ${ragContext.length} chunks for query "${ragQuery}"${extractedClientName ? ` (client: ${extractedClientName})` : ''}`);
      } catch (ragError) {
        console.error('RAG retrieval error:', ragError);
      }
    }
    
    // Step 3: Retrieve episodic/semantic memories
    if (firmMemory && message) {
      try {
        const memories = await firmMemory.retrieve('pitcher-law-pllc', {
          type: 'semantic',
          query: message,
          limit: 3
        });
        memoryContext = memories || [];
        console.log(`🧠 Retrieved ${memoryContext.length} memories from memory bank`);
      } catch (memError) {
        console.error('Memory retrieval error:', memError);
      }
    }

    // Step 4: Build enhanced system context with RAG data
    let contextData = '';
    
    if (ragContext.length > 0) {
      contextData += '\n\n📊 CLIENT DATA YOU HAVE ACCESS TO:\n';
      ragContext.forEach((chunk, i) => {
        contextData += `${i + 1}. ${chunk.content || chunk.text}\n`;
      });
    }
    
    if (memoryContext.length > 0) {
      contextData += '\n\n🧠 MEMORY BANK DATA:\n';
      memoryContext.forEach((mem, i) => {
        contextData += `${i + 1}. ${mem.content}\n`;
      });
    }
    
    let enhancedContext = '';

    // Inject ambient daily awareness (calendar, deadlines, active cases)
    let dailyContext = '';
    try {
      dailyContext = await getDailyContext();
    } catch (e) {
      console.warn('Daily context error:', e.message);
    }

    // Dashboard state — what the user is currently looking at
    let dashboardContextStr = '';
    if (dashboardState && Object.keys(dashboardState).length > 0) {
      const parts = [];
      if (dashboardState.activeClient) {
        parts.push(`User is viewing client: ${dashboardState.activeClient}`);
        if (dashboardState.activeCaseInfo) parts.push(`Case: ${dashboardState.activeCaseInfo}`);
      }
      if (dashboardState.sidebarTab) parts.push(`Sidebar showing: ${dashboardState.sidebarTab}`);
      if (dashboardState.deadlines?.length) {
        parts.push('Visible deadlines: ' + dashboardState.deadlines.map(d => `${d.client} (${d.due} - ${d.type})`).join(', '));
      }
      if (parts.length > 0) {
        dashboardContextStr = `\n🖥️ DASHBOARD STATE (what the user sees right now):\n${parts.join('\n')}\nWhen the user says "this client", "this case", or refers to something without naming it, they likely mean what's visible above.\n`;
      }
    }

    // --- Email context for active client ---
    let emailContextStr = '';
    const activeClient = dashboardState?.activeClient || extractedClientName;
    if (activeClient) {
      try {
        const clientEmails = await emailClient.getClientEmails(activeClient, 5);
        if (clientEmails.length > 0) {
          const enriched = await emailClient.enrichEmailsWithIdentity(clientEmails);
          emailContextStr = `\n📧 RECENT EMAILS FOR ${activeClient.toUpperCase()} (${enriched.length}):\n`;
          enriched.forEach((e, i) => {
            const who = e.senderIdentity ? `${e.senderIdentity.name} (${e.senderIdentity.role})` : e.fromName || e.from;
            emailContextStr += `  ${i + 1}. ${e.date?.substring(0, 10) || 'undated'} | FROM: ${who} | ${e.subject}\n     Preview: ${e.preview.substring(0, 120)}\n`;
          });
          emailContextStr += `When user says "reply to that email", "email them back", etc., use the most relevant email above.\n`;
        }
      } catch (emailErr) {
        console.warn('Email context error:', emailErr.message);
      }
    }

    // --- Phone Link context (unread texts, recent messages) ---
    let phoneContextStr = '';
    try {
      const phoneLink = require('../../lib/phone-link-client');
      const unread = phoneLink.getUnreadCount();
      const unreadConvos = phoneLink.getUnreadConversations();
      if (unread > 0) {
        phoneContextStr = `\n📱 PHONE (${unread} unread text${unread > 1 ? 's' : ''}):\n`;
        for (const c of unreadConvos.slice(0, 5)) {
          const contact = phoneLink.getContactByPhone(c.recipient);
          const name = contact ? contact.name : c.recipient;
          phoneContextStr += `  • ${name}: ${c.unreadCount} unread\n`;
        }
      }
      // If user is asking about texts/phone/sms, inject recent messages
      if (message && /\b(text|texts|sms|messages?|who\s*texted|phone\s*message)\b/i.test(message)) {
        const recent = phoneLink.getRecentMessages(10);
        if (recent.length > 0) {
          phoneContextStr += `\n📱 RECENT TEXT MESSAGES (${recent.length}):\n`;
          for (const m of recent) {
            const addr = m.direction === 'incoming' ? m.from : m.to;
            const contact = addr ? phoneLink.getContactByPhone(addr) : null;
            const name = contact ? contact.name : (addr || 'unknown');
            const dir = m.direction === 'incoming' ? '←' : '→';
            const time = m.timestamp ? m.timestamp.toISOString().substring(0, 16).replace('T', ' ') : '';
            phoneContextStr += `  ${dir} ${name} (${time}): ${(m.body || '').substring(0, 100)}\n`;
          }
        }
      }
    } catch (phoneErr) {
      // Phone Link not available or DB locked — non-fatal
      if (phoneErr.message && !phoneErr.message.includes('better-sqlite3')) {
        console.warn('Phone context error:', phoneErr.message);
      }
    }

    // --- Detect email action intents ---
    let emailAction = null;
    if (message) {
      if (/\b(send|email|write|draft|reply|respond|forward)\b.*\b(email|message|reply|response|them|back|her|him|court|client|counsel)\b/i.test(message) ||
          /\b(email|message|write to)\b/i.test(message)) {
        emailAction = 'compose';
      }
      if (/\b(archive|save|file|store|pdf)\b.*\b(email|emails|correspondence|communications)\b/i.test(message) ||
          /\b(email|emails)\b.*\b(to pdf|as pdf|archive|file|save)\b/i.test(message)) {
        emailAction = 'archive';
      }
    }

    // --- Detect phone/SMS action intents ---
    let phoneAction = null;
    if (message) {
      if (/\b(text|sms|send\s*(a\s*)?text|message)\b.*\b(to|them|him|her|client|counsel|court|back)\b/i.test(message) ||
          /\b(text|sms)\s+\d/i.test(message) ||
          /\b(reply|respond)\b.*\b(text|sms|message)\b/i.test(message)) {
        phoneAction = 'compose';
      }
      if (/\b(call|dial|phone)\b.*\b(client|counsel|court|them|him|her|back)\b/i.test(message) ||
          /\b(make\s*a\s*call|place\s*a\s*call)\b/i.test(message)) {
        phoneAction = 'dial';
      }
      if (/\b(check|show|read|any|unread|new)\b.*\b(text|texts|sms|messages?)\b/i.test(message) ||
          /\b(text|texts|sms|messages?)\b.*\b(from|check|show|read|new|unread)\b/i.test(message) ||
          /\b(who\s*texted|any\s*texts|any\s*messages)\b/i.test(message)) {
        phoneAction = 'read';
      }
      if (/\b(look\s*up|find|search|who\s*is)\b.*\b(phone|number|contact)\b/i.test(message) ||
          /\b(phone|number|contact)\b.*\b(look\s*up|find|search|who)\b/i.test(message)) {
        phoneAction = 'lookup';
      }
    }

    // --- Detect Zoom action intents ---
    let zoomAction = null;
    if (message && /\b(zoom|video\s*link|virtual\s*link|discovery\s*review\s*zoom|create\s*(a\s*)?zoom|schedule\s*(a\s*)?zoom|zoom\s*link\s*for|zoom\s*meeting)\b/i.test(message)) {
      zoomAction = 'create';
      if (/\b(list|show|upcoming)\b.*\b(zoom|meetings)\b/i.test(message)) zoomAction = 'list';
      if (/\b(cancel|delete|remove)\b.*\b(zoom|meeting)\b/i.test(message)) zoomAction = 'cancel';
      if (/\b(recording|recordings)\b/i.test(message)) zoomAction = 'recordings';
      if (/\b(check|verify)\b.*\b(discovery)\b/i.test(message)) zoomAction = 'discovery-check';
    }

    // --- Detect transcription / hearing notes intents ---
    let transcriptionAction = null;
    if (message) {
      if (/\b(transcribe|transcript)\b.*\b(last|latest|recent|my)\b.*\b(call|meeting|recording)\b/i.test(message) ||
          /\b(transcribe)\b.*\b(call|meeting|recording)\b/i.test(message)) {
        transcriptionAction = 'transcribe-latest';
      }
      if (/\b(hearing\s*notes?|court\s*notes?|just\s*got\s*out\s*of)\b/i.test(message) ||
          /\bnotes?\s*for\b/i.test(message)) {
        transcriptionAction = 'hearing-notes';
      }
      if (/\b(show|list|get)\b.*\b(recording|recordings)\b/i.test(message) && !zoomAction) {
        transcriptionAction = 'list-recordings';
      }
      if (/\b(show|list|get)\b.*\b(notes?|transcripts?)\b.*\b(for)\b/i.test(message)) {
        transcriptionAction = 'list-notes';
      }
      if (/\b(process)\b.*\b(all|new)\b.*\b(recording|recordings)\b/i.test(message)) {
        transcriptionAction = 'process-all';
      }
    }

    // --- Detect note-taking intents ---
    let noteAction = null;
    if (message) {
      // "note that...", "make a note", "jot down", "remember that", "add a note for Smith"
      if (/\b(note\s+that|make\s+a\s+note|jot\s+down|take\s+a\s+note|add\s+a?\s*note|save\s+a?\s*note|log\s+this|record\s+that)\b/i.test(message) ||
          /^\s*\/notes?\b/i.test(message)) {
        noteAction = 'save';
      }
      // "show notes for Smith", "what are my notes", "list notes"
      if (/\b(show|list|get|what\s+are|pull\s+up|find)\b.*\bnotes?\b/i.test(message) && !transcriptionAction) {
        noteAction = 'list';
      }
      // "meeting notes for Smith", "call notes", "client meeting summary"
      if (/\b(meeting|call|phone|conference|consultation)\s*notes?\b/i.test(message) && !transcriptionAction) {
        noteAction = 'meeting';
      }
      // "file notes to OneDrive", "save notes to folder"
      if (/\b(file|upload|save)\b.*\bnotes?\b.*\b(onedrive|folder|case\s*folder)\b/i.test(message)) {
        noteAction = 'file-notes';
      }
    }

    // --- Detect document generation intents ---
    let documentAction = null;
    if (message) {
      // "draft a motion", "generate NOA", "create cover sheet for Smith", "prepare interrogatories"
      if (/\b(draft|generate|create|prepare|make|build)\b.*\b(motion|noa|notice\s*of\s*appearance|cover\s*sheet|case\s*summary|facesheet|interrogator|request.*production|rfp|rogs|petition|declaration|order|document|pleading|brief)\b/i.test(message) ||
          /\b(motion|noa|cover\s*sheet|interrogator|rfp|rogs|petition|declaration)\b.*\b(for|draft|generate|create)\b/i.test(message)) {
        documentAction = 'generate';
      }
      // "file this to [client]'s folder", "save to OneDrive", "upload to case folder"
      if (/\b(file|save|upload|store)\b.*\b(to|in|into)\b.*\b(folder|onedrive|case|client)\b/i.test(message) ||
          /\b(case\s*folder|client\s*folder|onedrive)\b.*\b(file|save|upload|store)\b/i.test(message)) {
        if (!documentAction) documentAction = 'file';
      }
    }

    // --- Detect deadline/calendar management intents ---
    let deadlineAction = null;
    if (message) {
      const actionWords = /\b(move|reschedule|change|update|edit|push|postpone|continue|set)\b/i;
      const targetWords = /\b(hearing|deadline|court\s*date|event|appointment|sentencing|pretrial|arraignment|conference|trial)\b/i;
      const addWords = /\b(add|create|schedule|new)\b/i;
      const deleteWords = /\b(delete|remove|cancel)\b/i;
      const completeWords = /\b(complete|mark\s*done|finished|done\s*with)\b/i;

      if (actionWords.test(message) && targetWords.test(message)) {
        deadlineAction = 'update';
      } else if (addWords.test(message) && targetWords.test(message)) {
        deadlineAction = 'add';
      } else if (deleteWords.test(message) && targetWords.test(message)) {
        deadlineAction = 'delete';
      } else if (completeWords.test(message) && targetWords.test(message)) {
        deadlineAction = 'complete';
      }
    }

    // --- Detect court/case lookup intents ---
    // When user says "look up case X", "search JudicialLink", "check the docket", etc.
    let courtLookupAction = null;
    if (message) {
      if (/\b(look\s*up|search|check|find|pull|get|show)\b.*\b(case|docket|filing|judicialink|judicial\s*link|xchange|court\s*record|court\s*case)\b/i.test(message) ||
          /\b(judicialink|judicial\s*link|xchange)\b/i.test(message) ||
          /\b(case\s*(?:number|#|no\.?))\s*[:.]?\s*(\d{6,})/i.test(message) ||
          /\b(what.*filed|new.*filing|recent.*filing|any.*filing|status.*case)\b/i.test(message)) {
        courtLookupAction = 'search';
      }
    }

    // --- Execute court lookup if detected ---
    if (courtLookupAction === 'search') {
      // Extract case number from message
      const caseNumMatch = message.match(/\b(\d{9,12})\b/) || message.match(/case\s*(?:number|#|no\.?)?\s*[:.]?\s*(\d{6,})/i);
      const searchCaseNum = caseNumMatch ? (caseNumMatch[1] || caseNumMatch[2]) : null;

      // Extract name specifically for court lookups (strip "look up", "search", "JudicialLink", etc.)
      let courtSearchName = null;
      const nameExtract = message.replace(/\b(look\s*up|search|check|find|pull|get|show|in|on|for|from|the|case|docket|filing|judicialink|judicial\s*link|xchange|court\s*record|information|data|records?)\b/gi, '').trim();
      if (nameExtract && nameExtract.length > 1 && !/^\d+$/.test(nameExtract)) {
        courtSearchName = nameExtract;
      }

      // Search by case number, extracted name, or full query
      const searchQuery = searchCaseNum || courtSearchName || extractedClientName;

      try {
        // Query local JudicialLink cache — try both query (searches all fields) and clientName
        const jlResponse = await axios.get(`http://localhost:${process.env.PORT || 54112}/api/judiciallink-cases/search`, {
          params: {
            query: searchQuery || message,
            clientName: courtSearchName || extractedClientName || undefined,
            caseNumber: searchCaseNum || undefined,
            limit: 10
          }
        });

        if (jlResponse.data?.success && jlResponse.data?.results?.length > 0) {
          const cases = jlResponse.data.results;
          console.log(`🏛️ Court lookup found ${cases.length} cases for "${searchQuery}"`);

          let jlContext = `\n\n🏛️ JUDICIALINK/XCHANGE CASE DATA (${cases.length} results for "${searchQuery}"):\n`;
          cases.forEach((c, i) => {
            jlContext += `\n${i + 1}. ${c.caseName || 'Unknown'}\n`;
            jlContext += `   Case #: ${c.caseNumber || 'N/A'}\n`;
            jlContext += `   Court: ${c.court || 'N/A'}\n`;
            jlContext += `   Date: ${c.date || 'N/A'}\n`;
            jlContext += `   Status: ${c.status || 'N/A'}\n`;
            if (c.description) jlContext += `   Description: ${c.description}\n`;
            if (c.judge) jlContext += `   Judge: ${c.judge}\n`;
            if (c.nextHearing) jlContext += `   Next Hearing: ${c.nextHearing}\n`;
          });
          enhancedContext = `${enhancedContext || ''}${jlContext}`.trim();
        } else {
          console.log(`⚠️  Court lookup: no results for "${searchQuery}"`);
          enhancedContext = `${enhancedContext || ''}\n\n🏛️ JUDICIALINK SEARCH: No cases found matching "${searchQuery}" in the local cache. The cached data was last updated ${new Date().toISOString().split('T')[0]}. Data may need to be refreshed.`.trim();
        }
      } catch (jlError) {
        console.warn('Court lookup error:', jlError.message);
      }

      // Also query D1 party_cache for additional data
      try {
        const { queryD1 } = require('../../lib/graph-client');
        let d1Query;
        if (searchCaseNum) {
          d1Query = `SELECT * FROM party_cache WHERE case_number = '${searchCaseNum}'`;
        } else if (extractedClientName) {
          d1Query = `SELECT * FROM party_cache WHERE LOWER(client_name) LIKE LOWER('%${extractedClientName.replace(/'/g, "''")}%')`;
        }
        if (d1Query) {
          const d1Results = await queryD1(d1Query);
          if (d1Results && d1Results.length > 0) {
            let d1Context = `\n\n📋 PARTY CACHE DATA:\n`;
            d1Results.forEach((p, i) => {
              d1Context += `${i + 1}. ${p.client_name} (${p.client_role || 'N/A'}) — Case #${p.case_number}\n`;
              d1Context += `   Opposing: ${p.opposing_party || 'N/A'} (${p.opposing_role || 'N/A'})\n`;
              d1Context += `   Court: ${p.court || 'N/A'} | Judge: ${p.judge || 'N/A'}\n`;
              d1Context += `   Type: ${p.case_type || 'N/A'} | Status: ${p.status || 'N/A'}\n`;
              if (p.folder_url) d1Context += `   Folder: ${p.folder_url}\n`;
            });
            enhancedContext = `${enhancedContext || ''}${d1Context}`.trim();
          }
        }
      } catch (d1Error) {
        console.warn('D1 party_cache lookup error:', d1Error.message);
      }
    }

    let systemContext = `You are Synthia, AI Legal Assistant for ESQs Law (Pitcher Law PLLC). This platform is your world.

FIRM INFO:
- Firm: Pitcher Law PLLC (dba ESQs Law)
- Attorneys: John W. Adams III (Bar #19429, primary), Diane Pitcher (Bar #12626)
- Office Address: 40 W Cache Valley Blvd Ste 3B, Logan, UT 84321
- Phone: 435-752-3551
- Fax: 435-752-3554
- Email: pd@dianepitcher.com
- Courthouse (1st District Logan): 135 N 100 W, Logan, UT 84321
- Practice Areas: Criminal Defense, Family Law (1st District, Utah)

CORE DIRECTIVES:
- Be direct, concise, authoritative. You are the lead voice — no disclaimers, no "AI sources", no confidence percentages.
- Use common sense. If the user asks about hearings and you see schedule change alerts in your context, reference them. If they ask about a client and you have their data, use it. Don't give generic answers when you have specific data.
- You have UNRESTRICTED ability to research, review, browse, and analyze any legal content — case law, statutes, court rules, dockets, filings, legal commentary, news, and public records.
- ZERO TOLERANCE FOR HALLUCINATION. Never fabricate facts, law, case law, code, rules, or application of such. Every claim must trace to the client's case file, emails, court filings, or verifiable current law. If you don't have it, say so.
- NEVER FABRICATE DATA. If a piece of data is not in your context sections below, say "I don't have that in my current data." Do NOT invent placeholder values like "[Retrieving...]", fake attorney names, fake case details, or fake search results. This is the #1 rule.
- The system automatically queries JudicialLink, Utah Courts Calendar, Xchange, email, and OneDrive on your behalf before you respond. Any results appear in your context sections below (JUDICIALLINK, CALENDAR, EMAIL, etc.). If a section is present, use it confidently. If it's absent, that data wasn't found — say so.
- Legal analysis MUST BE OBJECTIVE and EVIDENCE-BASED. Base analysis ONLY on what can be proven from the case file and what current law says.
- NO SYMPATHY, NO PERSONAL INFERENCES, NO ACQUIRED BELIEFS OR TRAITS. Analysis is clinical and dispassionate.
- Proactively flag concerns, risks, procedural issues, or deadline problems.
- Utah state courts (1st District primary). Filter out 3rd/5th District unless explicitly asked.
- You DO have access to case files, JudicialLink, court calendars, client folders, email, and Zoom through the system bridge. Data from these sources is automatically queried and injected into your context below. Use it confidently. If specific data is NOT in your context sections, it wasn't found — say so honestly.

YOUR VOICE & PERSONALITY:
- You are sharp, direct, and genuinely helpful. Not robotic — you think through problems, anticipate follow-ups, and connect dots across conversations.
- You remember what was discussed earlier in the session. Reference previous messages naturally ("Earlier you asked about..." or "Building on the Smith case we were discussing...").
- When the user asks about a client, don't just dump data — interpret it. If a deadline is approaching, flag it. If a filing pattern suggests something, say it. Think like a colleague, not a search engine.
- Be concise but complete. Don't pad with disclaimers or repeat what the user already knows. If you need to give a long answer (case analysis, strategy memo), structure it clearly.
- Use your judgment. If the user says "what's going on with Smith?" and you see a hearing in 3 days plus new filings, lead with what matters most.
- You're part of the team. You know the attorneys, the practice areas (criminal defense + family law), the courts (1st District primarily), the workflow. Act like it.

YOUR ROLE — You wear three hats depending on what's needed:
1) Secretary/Legal Assistant: scheduling, calendar awareness, task lists, reminders, status updates, coordination. You know what's on the calendar, what alerts came in, what changed. Answer with common sense.
2) Paralegal: review files, prep for upcoming hearings, pre-draft required documents, organize facts, timelines, exhibits, summarize records. When a hearing is coming up, you should already be thinking about what needs to be filed.
3) Attorney: final review, case strategy, legal analysis, application of law to facts, nuances of Utah law. Research-oriented, precise, issue-spotting, cites Utah sources.
These roles are not exclusive — blend them as the situation requires.

CONFIDENCE RULE:
- For ESQs' own data — client files, calendar events, hearings, deadlines, case data, emails, party_cache — speak with FULL CONFIDENCE. This is your data, your platform, your world. No hedging.
- For external or uncertain information — legal research, case law interpretation, predictions about outcomes, opposing counsel's likely strategy — you may express appropriate uncertainty (e.g., "I'm about 70% sure on this" or "based on Judge Cannell's history, this is likely but not certain").
- NEVER express uncertainty about data you can see in your context. If the calendar says there's a hearing Tuesday, that's a fact — state it as one.

YOUR CAPABILITIES:
- Access client case data (party_cache, case files, calendar, alerts, deadlines) — all in context below
- Search JudicialLink / Xchange case data — the system queries these automatically when you mention a client or case number. Results appear in JUDICIALLINK section below.
- Search Utah Courts Calendar (public court hearing schedules) — queried automatically for relevant clients
- READ and SEND emails via Outlook (pd@dianepitcher.com) — the system handles the API call
- Archive client emails as PDFs to their OneDrive case folder
- Draft legal documents (motions, letters, pleadings) using templates
- Register new clients (generate cover sheets, update party_cache)
- Identify who is emailing (court clerks, opposing counsel, clients) via contact resolution
- Manage deadlines and calendar events (add, update, delete, complete)
- Create and schedule Zoom meetings
- Read SMS/text messages from the office phone (Samsung Galaxy S22 Ultra via Phone Link)
- Search phone contacts (734 synced contacts) and look up who called/texted
- Open SMS compose window or dialer for a phone number
- Cross-reference phone numbers with court contacts and opposing counsel
- Conversation history within this session
- Client data via RAG/Memory Bank

IMPORTANT — HOW YOUR DATA ACCESS WORKS:
- You do NOT directly browse the web. The server-side bridge queries JudicialLink, Xchange, Utah Courts, email, and OneDrive APIs automatically before building your response.
- If the user says "look up case 250100435" or "check JudicialLink for Smith", the system will search and inject results into your context. Just use whatever data appears in the sections below.
- If no data appears for a query, it means the system searched and found nothing — say so honestly.
- Never pretend to be "logging in" or "accessing" a website. Just reference the data that IS or ISN'T in your context.

DATA SOURCE PRIORITY:
1. CLIENT FILES FIRST - Documents reviewed by Claude Code
2. CALENDAR SECOND - Scheduled hearings and appointments
3. XCHANGE/JUDICIALLINK LAST - Court system data as fallback

${dailyContext}
${dashboardContextStr}
${emailContextStr}
${phoneContextStr}

${contextData}

${enhancedContext || ''}

RULES:
1. IF USER PROVIDES HEARING INFO IN THEIR MESSAGE, that IS authoritative data — use it directly.
2. ONLY use real data from: User's message, sections above (CLIENT DATA, CALENDAR, XCHANGE/JUDICIALLINK).
3. NEVER fabricate dates, case numbers, case names, or any detail not in the data.
4. For vague requests like "report" or "status", ask which client.
5. If user says "Draft X for [CLIENT]", generate complete document using data above.
6. Remember conversation history — maintain context.
7. Utah law firm — cite Utah Code when applicable.
8. If data is missing, say which sources you checked (context sections above) and ask for clarification. If the data simply isn't in your context, say so clearly.

EMAIL CAPABILITIES:
- You can READ emails from Outlook (pd@dianepitcher.com). Recent client emails are shown above when a client is active.
- You can SEND emails. When the user says "send", "email them", "reply", "thank them", draft the email and confirm you sent it. Routine/generic messages (thank you, acknowledgment, scheduling) are auto-approved.
- You can ARCHIVE client emails as PDFs to their OneDrive case folder under "Correspondence/".
- When referring to email senders, use their resolved identity (name, role, organization) rather than just email addresses.
- From address is always pd@dianepitcher.com. Sign emails as "ESQs Law / Pitcher Law PLLC" unless the user specifies otherwise.

DOCUMENT GENERATION:
- You CAN generate legal documents. Available templates: NOA, NOA-Master, Motion (generic), Motion to Compel, Motion for Virtual Hearing, Interrogatories, Requests for Production, Cover Sheet/Case Summary, Declaration, Order, Petition.
- When user says "draft", "generate", "create", or "prepare" a document, respond with the document data in this structured format so the system can generate it:
  ===DOCUMENT===
  TEMPLATE: <template-id> (e.g. noa, motion-generic, cover-sheet, interrogatories, etc.)
  DATA:
  CLIENT_NAME: <value>
  CASE_NUMBER: <value>
  (include all relevant fields from party_cache and context)
  ===END_DOCUMENT===
- The system will auto-fill firm defaults (attorney name, bar number, address, etc). You only need to provide case-specific fields.
- Pull data from party_cache, daily context, and case data to fill fields automatically. Ask only for data you cannot find.
- If user says "file this" or "save to folder", the document will be uploaded to the client's OneDrive case folder.

FILE OPERATIONS:
- You can SAVE/FILE generated documents to a client's OneDrive case folder.
- You can provide download links for generated documents.
- When a document is generated, tell the user the filename and offer to file it to the client's folder.

NOTE-TAKING:
- You can SAVE notes to the case file. When the user says "note that...", "make a note", "jot down", "remember that [client info]", save the note.
- You can LIST notes. When asked "show notes for Smith" or "what are my notes", retrieve and display them.
- You can save MEETING notes with structured fields (attendees, summary, action items, next steps).
- You can FILE notes to the client's OneDrive case folder under "Notes/".
- Notes are stored in the case_notes D1 table and associated with the active client.
- When you save a note during conversation, confirm it briefly (e.g., "Noted." or "Saved to Smith's file.").

DEADLINE/CALENDAR MANAGEMENT:
- You CAN move, reschedule, add, delete, and complete deadlines/hearings.
- When the user says "move [client]'s hearing to [date]", "reschedule the pretrial", "push the hearing to next week", etc., respond with a structured block:
  ===DEADLINE===
  ACTION: update|add|delete|complete
  CLIENT_NAME: <client name>
  DUE_DATE: <YYYY-MM-DD>
  HEARING_TIME: <HH:MM AM/PM> (optional)
  DEADLINE_TYPE: <type, e.g. Pretrial, Hearing, Sentencing, Motion Deadline>
  COURT: <court name> (optional)
  JUDGE: <judge name> (optional)
  COURTROOM: <courtroom> (optional)
  NOTES: <any notes> (optional)
  ===END_DEADLINE===
- For updates: DUE_DATE is the NEW date. Include only fields that are changing.
- For adds: include all known fields.
- For delete/complete: CLIENT_NAME and optionally DUE_DATE to identify the record.
- Pull client info from party_cache and daily context. Confirm the change in your response.`;

    // Step 5: Check if this is a case query (needs special handling)
    function isCaseQueryLocal(msg) {
      if (!msg) return false;
      const lower = msg.toLowerCase();
      // Only match phrases that clearly indicate a case/client data query
      // Single words like "email", "case", "request" are too broad — require context
      const strongSignals = [
        'opposing party', 'op filed', 'filed against', 'opposing counsel',
        'case number', 'case status', 'what is the status',
        'next hearing', 'court date', 'when is the hearing',
        'how many cases', 'how many clients', 'how many hearing',
        'all hearings', 'all cases', 'all clients', 'all the hearing',
        'back-schedul', 'back schedul', 'backschedul',
        'look up', 'pull up', 'search for',
        'loaded all', 'got all', 'have all', 'missing any',
        'total cases', 'total clients', 'total hearings',
        'show all', 'list all', 'every hearing', 'every case', 'every client'
      ];
      if (strongSignals.some(k => lower.includes(k))) return true;
      // Require at least 2 weak signals to trigger (e.g., "hearing" + a client name)
      const weakSignals = ['motion', 'filing', 'case', 'client', 'petition',
                           'hearing', 'scheduled', 'when was', 'when is'];
      const weakCount = weakSignals.filter(k => lower.includes(k)).length;
      return weakCount >= 2;
    }

    const isQuery = isCaseQueryLocal(message);
    let caseQueryResult = null;
    
    // Extract potential client names from the message (for queries like "tomorrow's cases")
    const clientNamesInMessage = [];
    if (message) {
      const namePattern = /([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})/g;
      const matches = message.match(namePattern);
      if (matches) {
        const nonNameWords = new Set(['Pre', 'Trial', 'Conference', 'Pretrial', 'Virtual', 'Court', 'Justice',
                                       'Logan', 'Draft', 'Motion', 'Good', 'Morning', 'Open', 'Cases', 'Files',
                                       'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday',
                                       'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
                                       'September', 'October', 'November', 'December', 'The', 'This', 'That',
                                       'Ready', 'Need', 'Help', 'What', 'When', 'Where', 'How', 'Show',
                                       'Get', 'Send', 'Email', 'Reply', 'Look', 'Search',
                                       'Check', 'Find', 'Pull', 'District', 'County', 'State', 'Utah',
                                       'Case', 'Number', 'Status', 'Report', 'Update', 'Schedule']);
        matches.forEach(name => {
          if (!nonNameWords.has(name.split(' ')[0]) && name.length > 5) {
            clientNamesInMessage.push(name);
          }
        });
      }
    }
    
    // Add extracted client name if not already in list
    if (extractedClientName && !clientNamesInMessage.includes(extractedClientName)) {
      clientNamesInMessage.push(extractedClientName);
    }

    // --- ALWAYS query party_cache for known client (core identity data) ---
    if (extractedClientName && !enhancedContext?.includes('PARTY CACHE')) {
      try {
        const { queryD1 } = require('../../lib/graph-client');
        const safeName = extractedClientName.replace(/'/g, "''");
        const partyResults = await queryD1(`SELECT * FROM party_cache WHERE LOWER(client_name) LIKE LOWER('%${safeName}%') LIMIT 5`);
        if (partyResults && partyResults.length > 0) {
          let pcContext = `\n\n📋 PARTY CACHE — OUR CLIENT DATA:\n`;
          partyResults.forEach((p, i) => {
            pcContext += `${i + 1}. Client: ${p.client_name} (${p.client_role || 'N/A'}) — Case #${p.case_number}\n`;
            pcContext += `   Opposing: ${p.opposing_party || 'N/A'} (${p.opposing_role || 'N/A'})\n`;
            pcContext += `   Court: ${p.court || 'N/A'} | Judge: ${p.judge || 'N/A'}\n`;
            pcContext += `   Type: ${p.case_type || 'N/A'} | Status: ${p.status || 'N/A'}\n`;
            if (p.folder_url) pcContext += `   Folder: ${p.folder_url}\n`;
          });
          enhancedContext = `${enhancedContext || ''}${pcContext}`.trim();
          console.log(`📋 Auto-loaded party_cache for "${extractedClientName}" (${partyResults.length} entries)`);
        }
      } catch (pcErr) {
        console.warn('Auto party_cache lookup error:', pcErr.message);
      }
    }

    // --- Auto-query deadlines for active client ---
    if (extractedClientName && !enhancedContext?.includes('CLIENT DEADLINES')) {
      try {
        const { queryD1 } = require('../../lib/graph-client');
        const safeName = extractedClientName.replace(/'/g, "''").toLowerCase().split(' ')[0];
        const deadlineResults = await queryD1(
          `SELECT client_name, case_number, due_date, hearing_time, deadline_type, court, judge, courtroom, status, notes FROM deadlines WHERE LOWER(client_name) LIKE '%${safeName}%' AND status IN ('active', 'pending') ORDER BY due_date ASC LIMIT 10`
        );
        if (deadlineResults && deadlineResults.length > 0) {
          let dlContext = `\n\n⏰ CLIENT DEADLINES FOR ${extractedClientName.toUpperCase()} (${deadlineResults.length}):\n`;
          deadlineResults.forEach((d, i) => {
            dlContext += `${i + 1}. ${d.deadline_type || 'Deadline'} — ${d.due_date}${d.hearing_time ? ' at ' + d.hearing_time : ''}\n`;
            dlContext += `   Case #${d.case_number || 'N/A'} | Court: ${d.court || 'N/A'} | Judge: ${d.judge || 'N/A'}\n`;
            if (d.courtroom) dlContext += `   Courtroom: ${d.courtroom}\n`;
            if (d.notes) dlContext += `   Notes: ${d.notes}\n`;
          });
          enhancedContext = `${enhancedContext || ''}${dlContext}`.trim();
          console.log(`⏰ Auto-loaded ${deadlineResults.length} deadlines for "${extractedClientName}"`);
        }
      } catch (dlErr) {
        console.warn('Auto deadline lookup error:', dlErr.message);
      }
    }

    // --- Aggregate system query: when user asks about ALL hearings/clients/cases without a specific name ---
    if (isQuery && clientNamesInMessage.length === 0 && !extractedClientName) {
      try {
        const { queryD1 } = require('../../lib/graph-client');

        // Detect aggregate intent — questions about "all hearings", "all clients", hearing counts, coverage, etc.
        const isAggregateQuery = /\b(all|every|total|how many|count|list all|show all|all the|back.?schedul|all hearings|all cases|all clients|coverage|missing|loaded|complete|got all)\b/i.test(message);

        if (isAggregateQuery) {
          // Pull aggregate stats
          const [deadlineStats, deadlinesByMonth, activeClientCount, pendingCases, recentDeadlines] = await Promise.all([
            queryD1(`SELECT COUNT(*) as total, COUNT(DISTINCT client_name) as unique_clients, MIN(due_date) as earliest, MAX(due_date) as latest, SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed, SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending_count FROM deadlines`),
            queryD1(`SELECT substr(due_date,1,7) as month, COUNT(*) as count, COUNT(DISTINCT client_name) as clients FROM deadlines WHERE due_date >= '2026-01-01' GROUP BY substr(due_date,1,7) ORDER BY month ASC`),
            queryD1(`SELECT COUNT(DISTINCT client_name) as active_count FROM party_cache WHERE status='active'`),
            queryD1(`SELECT COUNT(*) as pending_count FROM party_cache WHERE status='active' AND case_number='PENDING'`),
            queryD1(`SELECT client_name, case_number, deadline_type, due_date, hearing_time, court, judge, status FROM deadlines WHERE due_date >= '2026-01-01' ORDER BY due_date ASC LIMIT 60`)
          ]);

          let aggContext = `\n\n📊 SYSTEM DATA SUMMARY (aggregate query):\n`;

          if (deadlineStats?.[0]) {
            const s = deadlineStats[0];
            aggContext += `\nDEADLINES/HEARINGS TABLE:\n`;
            aggContext += `  Total entries: ${s.total}\n`;
            aggContext += `  Unique clients with hearings: ${s.unique_clients}\n`;
            aggContext += `  Date range: ${s.earliest} to ${s.latest}\n`;
            aggContext += `  Completed: ${s.completed} | Pending: ${s.pending_count}\n`;
          }

          if (deadlinesByMonth?.length) {
            aggContext += `\nHEARINGS BY MONTH:\n`;
            deadlinesByMonth.forEach(m => {
              aggContext += `  ${m.month}: ${m.count} hearings across ${m.clients} clients\n`;
            });
          }

          if (activeClientCount?.[0]) {
            aggContext += `\nACTIVE CLIENTS: ${activeClientCount[0].active_count} total`;
            if (pendingCases?.[0]?.pending_count > 0) {
              aggContext += ` (${pendingCases[0].pending_count} still missing case numbers)`;
            }
            aggContext += '\n';
          }

          if (recentDeadlines?.length) {
            aggContext += `\nALL HEARINGS (${recentDeadlines.length} entries from 2026-01-01 onward):\n`;
            recentDeadlines.forEach((d, i) => {
              aggContext += `  ${i + 1}. ${d.due_date}${d.hearing_time ? ' ' + d.hearing_time : ''} | ${d.client_name} | ${d.deadline_type || 'hearing'}`;
              if (d.case_number) aggContext += ` | #${d.case_number}`;
              if (d.court) aggContext += ` | ${d.court}`;
              if (d.judge) aggContext += ` | J. ${d.judge}`;
              aggContext += ` [${d.status}]\n`;
            });
          }

          enhancedContext = `${enhancedContext || ''}${aggContext}`.trim();
          console.log(`📊 Aggregate query: loaded ${recentDeadlines?.length || 0} hearings, ${activeClientCount?.[0]?.active_count || 0} active clients`);
        }
      } catch (aggErr) {
        console.warn('Aggregate query error:', aggErr.message);
      }
    }

    // --- Auto-run JudicialLink search for any client-related query (not just explicit lookups) ---
    if (!courtLookupAction && isQuery && clientNamesInMessage.length > 0) {
      for (const cn of clientNamesInMessage) {
        try {
          const jlRes = await axios.get(`http://localhost:${process.env.PORT || 54112}/api/judiciallink-cases/search`, {
            params: { clientName: cn, limit: 5 }
          });
          if (jlRes.data?.success && jlRes.data?.results?.length > 0 && !enhancedContext?.includes('JUDICIALINK')) {
            const cases = jlRes.data.results;
            console.log(`🏛️ Auto-lookup found ${cases.length} JudicialLink cases for "${cn}"`);
            let jlContext = `\n\n🏛️ JUDICIALINK CASES FOR ${cn.toUpperCase()} (${cases.length}):\n`;
            cases.forEach((c, i) => {
              jlContext += `${i + 1}. ${c.caseName} | #${c.caseNumber} | ${c.court} | ${c.status || 'active'}\n`;
            });
            enhancedContext = `${enhancedContext || ''}${jlContext}`.trim();
          }
        } catch (e) {
          console.warn(`Auto JudicialLink lookup error for ${cn}:`, e.message);
        }
      }
    }

    // PRIORITY ORDER: 1. Client Files (reviewed by Claude Code) 2. Calendar 3. Xchange/JudicialLink
    if (isQuery && clientNamesInMessage.length > 0) {
      try {
        // PRIORITY 1: Client folder documents (already handled by RAG above)
        // RAG has already searched client documents and added relevant data to ragContext
        
        // PRIORITY 2: Calendar events (scheduled hearings and appointments)
        for (const clientName of clientNamesInMessage) {
          try {
            const calendarEventsPath = path.join(__dirname, '../../../data/calendar-events.json');
            const legacyCalendarPath = path.join(__dirname, '../../../data/calendar.json');
            let events = [];
            if (fs.existsSync(calendarEventsPath)) {
              const calendarData = JSON.parse(fs.readFileSync(calendarEventsPath, 'utf-8'));
              events = Array.isArray(calendarData) ? calendarData : (calendarData.events || []);
            } else if (fs.existsSync(legacyCalendarPath)) {
              const calendarData = JSON.parse(fs.readFileSync(legacyCalendarPath, 'utf-8'));
              events = Array.isArray(calendarData) ? calendarData : (calendarData.events || []);
            }
            if (events.length > 0) {
              
              // Filter events for this client
              const clientEvents = events.filter(e => 
                (e.summary && e.summary.toLowerCase().includes(clientName.toLowerCase())) ||
                (e.description && e.description.toLowerCase().includes(clientName.toLowerCase())) ||
                (e.attendees && e.attendees.some(a => a.toLowerCase().includes(clientName.toLowerCase())))
              );
              
              if (clientEvents.length > 0) {
                console.log(`📅 Found ${clientEvents.length} calendar events for "${clientName}"`);
                enhancedContext = `${enhancedContext || ''}\n\n📅 CALENDAR EVENTS FOR ${clientName.toUpperCase()}:\n`;
                clientEvents.forEach((e, i) => {
                  enhancedContext += `\n${i + 1}. Event: ${e.summary}\n`;
                  enhancedContext += `   Date/Time: ${e.start}\n`;
                  if (e.location) enhancedContext += `   Location: ${e.location}\n`;
                  if (e.description) enhancedContext += `   Details: ${e.description}\n`;
                });
                enhancedContext = enhancedContext.trim();
              }
            }
          } catch (calendarError) {
            console.warn(`Calendar search error for ${clientName}:`, calendarError.message);
          }
        }
        
        // PRIORITY 3: Xchange/JudicialLink (court system data - always search, supplements calendar)
        if (!enhancedContext.includes('JUDICIALINK')) {
          for (const clientName of clientNamesInMessage) {
            try {
              const judicialLinkResponse = await axios.get(`http://localhost:${process.env.PORT || 54112}/api/judiciallink-cases/search?clientName=${encodeURIComponent(clientName)}&limit=10`);
              
              if (judicialLinkResponse.data && judicialLinkResponse.data.success && judicialLinkResponse.data.results && judicialLinkResponse.data.results.length > 0) {
                const cases = judicialLinkResponse.data.results;
                console.log(`🏛️ Found ${cases.length} Xchange/JudicialLink cases for "${clientName}"`);
                
                // Add all case information to context
                enhancedContext = `${enhancedContext || ''}\n\n🏛️ XCHANGE/JUDICIALLINK CASES FOR ${clientName.toUpperCase()}:\n`;
                cases.forEach((c, i) => {
                  enhancedContext += `\n${i + 1}. Case: ${c.caseName}\n`;
                  enhancedContext += `   Case Number: ${c.caseNumber}\n`;
                  enhancedContext += `   Court: ${c.court}\n`;
                  enhancedContext += `   Date: ${c.date}\n`;
                  enhancedContext += `   Description: ${c.description}\n`;
                  enhancedContext += `   Status: ${c.status}\n`;
                });
                enhancedContext = enhancedContext.trim();
              } else {
                console.log(`⚠️  No Xchange/JudicialLink cases found for "${clientName}"`);
              }
            } catch (judicialLinkError) {
              console.warn(`Xchange/JudicialLink search error for ${clientName}:`, judicialLinkError.message);
            }
          }
        }
        
        // FALLBACK: Query Utah State Courts calendar if no local data
        if (!enhancedContext.includes('LOCAL DOCKET HEARINGS') && extractedClientName) {
          try {
            const clientsPath = path.join(__dirname, '../../../data/memory-bank/pitcher-law-pllc/judicial-link-clients.json');
            if (fs.existsSync(clientsPath)) {
              const clientsData = JSON.parse(fs.readFileSync(clientsPath, 'utf8'));
              const extractedLower = extractedClientName.toLowerCase();
              const client = clientsData.clients.find(c => {
                const clientNameLower = c.clientName.toLowerCase();
                return clientNameLower.includes(extractedLower) || extractedLower.includes(clientNameLower.split(' ')[0]);
              });
              
              if (client && client.cases && client.cases.length > 0) {
                const activeCase = client.cases.find(c => c.status === 'active') || client.cases[0];
                
                // Try Utah State Courts calendar first (public, no auth needed)
                console.log(`📅 FALLBACK 1: Querying Utah State Courts calendar for "${extractedClientName}"...`);
                
                const calendarUrl = `https://legacy.utcourts.gov/cal/search.php?t=a&c=&p=&j=&f=&l=&b=12626&d=all&loc=all`;
                
                try {
                  const response = await axios.get(calendarUrl, {
                    headers: {
                      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    },
                    timeout: 10000
                  });
                  
                  // Parse HTML to extract hearing information
                  const html = response.data;
                  
                  // Look for the client's name and case number in the calendar HTML
                  const clientNamePattern = new RegExp(client.clientName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
                  const caseNumberPattern = new RegExp(activeCase.caseNumber.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
                  
                  if (clientNamePattern.test(html) || caseNumberPattern.test(html)) {
                    // Extract hearing information using regex patterns
                    const hearingMatches = html.match(/<tr[^>]*>[\s\S]*?(\d{1,2}\/\d{1,2}\/\d{4})[\s\S]*?(\d{1,2}:\d{2}\s*[AP]M)?[\s\S]*?(?:In Person|Virtual|Hybrid)[\s\S]*?(PRETRIAL|HEARING|TRIAL|CONFERENCE|APPEARANCE)[\s\S]*?Case\s*#\s*(\d+)/gi);
                    
                    if (hearingMatches && hearingMatches.length > 0) {
                      const relevantHearings = hearingMatches.filter(match => {
                        return caseNumberPattern.test(match) || clientNamePattern.test(match);
                      });
                      
                      if (relevantHearings.length > 0) {
                        const courtCalendarHearings = relevantHearings.slice(0, 3);
                        console.log(`✅ FALLBACK 1 SUCCESS: Found ${courtCalendarHearings.length} hearing(s) in Utah State Courts calendar for "${extractedClientName}"`);

                        const calendarContext = courtCalendarHearings.map((match, i) => {
                          // Extract date, time, type, and case number from match
                          const dateMatch = match.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
                          const timeMatch = match.match(/(\d{1,2}:\d{2}\s*[AP]M)/i);
                          const typeMatch = match.match(/(PRETRIAL|HEARING|TRIAL|CONFERENCE|APPEARANCE)/i);
                          const caseMatch = match.match(/Case\s*#\s*(\d+)/i);
                          
                          return `UTAH STATE COURTS CALENDAR HEARING ${i + 1}:\n- Date: ${dateMatch ? dateMatch[1] : 'Not specified'}\n- Time: ${timeMatch ? timeMatch[1] : 'Not specified'}\n- Type: ${typeMatch ? typeMatch[1] : 'Hearing'}\n- Case #: ${caseMatch ? caseMatch[1] : activeCase.caseNumber}\n- Court: ${activeCase.court || 'See calendar'}\n- Source: Official Utah State Courts Calendar`;
                        }).join('\n\n');
                        
                        enhancedContext = `${enhancedContext || ''}\n\n⚖️ UTAH STATE COURTS CALENDAR (Official Court Calendar - LIVE DATA):\n${calendarContext}\n\nThis is LIVE data from the official Utah State Courts calendar system. Use this information to answer about hearing dates.`.trim();
                      }
                    }
                  }
                } catch (calendarError) {
                  console.warn('Utah State Courts calendar query error:', calendarError.message);
                  
                  // FALLBACK 2: Try Utah Courts Xchange (authenticated, more detailed)
                  if (!enhancedContext.includes('UTAH STATE COURTS CALENDAR')) {
                    try {
                      const UtahXchangeService = require('../../integrations/utah-xchange-service');
                      const xchange = new UtahXchangeService();
                      
                      console.log(`📅 FALLBACK 2: Querying Utah Courts Xchange for "${extractedClientName}" (Case: ${activeCase.caseNumber})...`);
                      
                      const xchangeResult = await xchange.getHearingDates(activeCase.caseNumber);
                      
                      if (xchangeResult.success && xchangeResult.hearings && xchangeResult.hearings.length > 0) {
                        console.log(`✅ FALLBACK 2 SUCCESS: Found ${xchangeResult.hearings.length} hearing(s) in Utah Courts Xchange for "${extractedClientName}"`);
                        
                        const xchangeContext = xchangeResult.hearings.map((h, i) => {
                          return `UTAH COURTS XCHANGE HEARING ${i + 1}:\n- Date: ${h.date || 'Not specified'}\n- Time: ${h.time || 'Not specified'}\n- Type: ${h.type || 'Hearing'}\n- Case #: ${activeCase.caseNumber}\n- Court: ${activeCase.court || 'See Xchange'}\n- Source: Utah Courts Xchange (Authenticated)`;
                        }).join('\n\n');
                        
                        enhancedContext = `${enhancedContext || ''}\n\n⚖️ UTAH COURTS XCHANGE (Authenticated Case Search - DETAILED DATA):\n${xchangeContext}\n\nThis is detailed data from Utah Courts Xchange. Use this information to answer about hearing dates.`.trim();
                      }
                    } catch (xchangeError) {
                      console.warn('Utah Courts Xchange query error:', xchangeError.message);
                    }
                  }
                }
              }
            }
          } catch (fallbackError) {
            console.warn('Court calendar fallback error:', fallbackError.message);
          }
        }
      } catch (error) {
        console.warn('Hearing date lookup error:', error.message);
      }
    }
    
    // --- Inject procedures (smart section routing — loads only relevant sections) ---
    try {
      const proceduresContext = { documentAction, emailAction, deadlineAction, courtLookupAction, zoomAction };
      const proceduresText = await loadProcedures(message, proceduresContext);
      if (proceduresText) {
        systemContext += `\n\nOPERATIONAL PROCEDURES (follow these automatically — do NOT ask the user questions these procedures already answer):\n${proceduresText}`;
      }
    } catch (procErr) {
      console.warn('⚠️  Procedures loader error:', procErr.message);
    }

    // --- Late-inject enhanced context into system prompt ---
    // enhancedContext may have been populated AFTER systemContext was built (aggregate queries, party_cache, deadlines, JudicialLink)
    // Re-inject it now so the AI models see it
    if (enhancedContext && enhancedContext.trim().length > 0) {
      const lateContext = enhancedContext.trim();
      if (!systemContext.includes(lateContext.substring(0, 80))) {
        systemContext += `\n\n${lateContext}`;
        console.log(`📎 Late-injected ${lateContext.length} chars of enhanced context into system prompt`);
      }
    }

    if (isQuery && !courtLookupAction) {
      if (enhancedContext && enhancedContext.includes('SYSTEM DATA SUMMARY')) {
        // Skip caseQueryAgent — aggregate data already in context, go straight to AI synthesis
        console.log('📊 Skipping caseQueryAgent — aggregate data already in context, going straight to AI synthesis');
      } else {
        try {
          // Use Case Query Agent for case-specific questions (it uses RAG internally)
          // Skip this when court lookup was explicitly requested — we want Synthia to use the injected JudicialLink data
          const { caseQueryAgent: cqa } = getServices();
          caseQueryResult = await cqa.answerQuery(message);

          // If we got a good answer, use it directly
          if (caseQueryResult.success && caseQueryResult.answer && caseQueryResult.answer !== 'Unable to generate answer') {
            // Store in memory bank
            if (memoryBank) {
              await memoryBank.store({
                type: 'episodic',
                agent: 'the-bridge',
                event: 'case_query',
                content: {
                  query: message,
                  answer: caseQueryResult.answer,
                  clientName: caseQueryResult.clientName
                },
                metadata: {
                  firmId: 'pitcher-law-pllc',
                  sessionId: sessionId,
                  timestamp: new Date().toISOString()
                }
              });
            }

            return res.json({
              success: true,
              consensus: caseQueryResult.answer,
              sources: caseQueryResult.sources.aiModels || 1,
              confidence: caseQueryResult.confidence,
              operationalAIs: 1,
              totalAIs: 1,
              ragContext: ragContext.length,
              memoryContext: memoryContext.length,
              caseQuery: {
                used: true,
                clientName: caseQueryResult.clientName,
                sources: caseQueryResult.sources
              },
              queryType: 'case_query'
            });
          }

          // Otherwise, add case query result to enhanced context for synthesis
          if (caseQueryResult.success) {
            enhancedContext = `${enhancedContext || ''}\n\nCASE QUERY RESULT:\n${caseQueryResult.answer}`.trim();
          }
        } catch (error) {
          console.error('Case query error:', error);
          // Continue with normal processing
        }
      }
    }

    const hasContext =
      ragContext.length > 0 ||
      memoryContext.length > 0 ||
      (enhancedContext && enhancedContext.trim().length > 0) ||
      (caseQueryResult && caseQueryResult.success) ||
      !!courtLookupAction;

    // If it looks like a case query but no enhanced context was found,
    // don't block — let the AI respond using daily context + system prompt.
    // The AI knows what data it has and can ask for clarification.
    if (isQuery && !hasContext) {
      console.log('⚠️ Case query detected but no enhanced context found — letting AI handle with daily context');
    }

    // Step 6: Build user prompt
    // NOTE: RAG context, memory context, enhanced context (JudicialLink, calendar, party_cache, emails)
    // are ALL in the system prompt already (via contextData and enhancedContext in systemContext).
    // The user prompt should contain ONLY conversation history + current message.
    // DO NOT duplicate data into both system prompt and user prompt — wastes tokens.
    let prompt = '';
    if (context) {
      prompt = `CONVERSATION HISTORY (recent messages in this session):\n${context}\n\nCURRENT MESSAGE: ${message || `Analyze this document: ${fileName} (${fileUrl})`}`;
    } else {
      prompt = message || `Analyze this document: ${fileName} (${fileUrl})`;
    }

    // Step 7: THE FUNNEL — Claude is the narrow neck. Fan out to other AIs only when needed.
    const { aiService: ai } = getServices();
    const availableModels = ai.getAvailableModels();

    if (availableModels.length === 0) {
      return res.status(500).json({
        success: false,
        error: 'No AI services configured. Please set API keys.'
      });
    }

    // Decide: does this prompt need the wide end of the funnel (multi-AI research)?
    // Or can Claude handle it directly through the narrow neck?
    const msgLower = (message || '').toLowerCase();
    const needsResearch =
      // Legal research, case law, statute interpretation
      /\b(research|case\s*law|statute|precedent|urcp|utah\s*code|analyze|legal\s*analysis|strategy|argument|brief|motion\s*to|oppose|respond\s*to)\b/i.test(message) ||
      // Complex drafting that benefits from multiple perspectives
      /\b(draft|write|compose|prepare)\b.*\b(motion|brief|memorandum|petition|response|objection|argument)\b/i.test(message) ||
      // Opinion or analysis questions
      /\b(what\s*are\s*the\s*chances|likelihood|should\s*we|pros\s*and\s*cons|evaluate|assess|compare)\b/i.test(message) ||
      // Explicit multi-AI request
      /\b(research\s*this|look\s*into|dig\s*into|investigate)\b/i.test(message);

    let consensus, responses;

    // Temperature split: 0.0 for legal drafting/analysis (precision), 0.3 for conversational (personality)
    const legalTemp = needsResearch ? 0 : undefined; // undefined = use default (0.3)

    if (needsResearch) {
      // WIDE END: Fan out to all research AIs, Claude synthesizes
      const researchModels = availableModels.filter(model => model.id !== 'claude');
      console.log(`🌉 Funnel wide → Dispatching to ${researchModels.length} research AIs (temp=0). Claude will synthesize.`);

      const aiQueries = researchModels.map(model => {
        return ai.query(model.id, prompt, { maxTokens: 4096, temperature: 0, systemPrompt: systemContext })
          .then(response => ({
            success: true,
            ai: model.name,
            response: response
          }))
          .catch(error => {
            console.warn(`⚠️  ${model.name} error:`, error.message);
            return {
              success: false,
              ai: model.name,
              error: error.message
            };
          });
      });

      responses = await Promise.all(aiQueries);
      const validResponses = responses.filter(r => r.success);
      console.log(`✅ ${validResponses.length} research responses in. Claude synthesizing...`);

      // Claude reviews all research and delivers the final word
      consensus = await synthesizeConsensus(responses);
    } else {
      // NARROW NECK: Claude handles directly — calendar, status, emails, simple tasks
      console.log('🌉 Funnel narrow → Claude handling directly (temp=0.3).');
      try {
        const claudeResponse = await ai.query('claude', prompt, { maxTokens: 4096, systemPrompt: systemContext });
        consensus = {
          consensus: claudeResponse.content || claudeResponse.message,
          sources: 1,
          confidence: 1.0
        };
        responses = [{ success: true, ai: 'Claude (direct)' }];
      } catch (claudeErr) {
        console.error('Claude direct error:', claudeErr.message);
        // Fallback: Grok takes the voice, then GPT-4o, then full funnel
        let fallbackDone = false;
        if (ai.isAvailable('xai')) {
          try {
            console.log('⚠️  Claude down, Grok stepping up...');
            const grokResponse = await ai.query('xai', prompt, { maxTokens: 4096, systemPrompt: systemContext });
            consensus = { consensus: grokResponse.content || grokResponse.message, sources: 1, confidence: 0.9 };
            responses = [{ success: true, ai: 'Grok (backup)' }];
            fallbackDone = true;
          } catch (grokErr) { console.warn('Grok fallback also failed:', grokErr.message); }
        }
        if (!fallbackDone && ai.isAvailable('gpt4o')) {
          try {
            console.log('⚠️  Grok also down, GPT-4o stepping up...');
            const gptResponse = await ai.query('gpt4o', prompt, { maxTokens: 4096, systemPrompt: systemContext });
            consensus = { consensus: gptResponse.content || gptResponse.message, sources: 1, confidence: 0.8 };
            responses = [{ success: true, ai: 'GPT-4o (backup)' }];
            fallbackDone = true;
          } catch (gptErr) { console.warn('GPT-4o fallback also failed:', gptErr.message); }
        }
        if (!fallbackDone) {
          console.log('⚠️  All direct models failed, falling back to full funnel...');
          const researchModels = availableModels.filter(model => model.id !== 'claude');
          const aiQueries = researchModels.map(model => {
            return ai.query(model.id, prompt, { maxTokens: 4096, systemPrompt: systemContext })
              .then(response => ({ success: true, ai: model.name, response }))
              .catch(error => ({ success: false, ai: model.name, error: error.message }));
          });
          responses = await Promise.all(aiQueries);
          consensus = await synthesizeConsensus(responses);
        }
      }
    }
    
    // Step 8: Store response in Memory Bank
    if (memoryBank && sessionId) {
      try {
        await memoryBank.store({
          type: 'episodic',
          agent: 'the-bridge',
          event: 'chat_message',
          content: {
            message: message,
            consensus: consensus.consensus,
            ragContext: ragContext.length,
            memoryContext: memoryContext.length
          },
          metadata: {
            firmId: 'pitcher-law-pllc',
            sessionId: sessionId,
            userId: userId,
            timestamp: new Date().toISOString()
          }
        });
      } catch (error) {
        console.error('Memory storage error:', error);
      }
    }

    // --- Handle email compose action: parse AI response for email content ---
    if (emailAction === 'compose' && consensus?.consensus) {
      try {
        const parsed = parseEmailFromResponse(consensus.consensus);
        if (parsed) {
          console.log(`📧 Parsed email from AI response: to=${parsed.to}, subject="${parsed.subject}"`);
          const sendResult = await emailClient.sendEmail({
            to: parsed.to,
            subject: parsed.subject,
            body: parsed.body,
            isHtml: true,
            from: 'Associate@dianepitcher.com'
          });
          consensus.consensus += sendResult.success
            ? `\n\n✅ Email sent${sendResult.fallback ? ` (via ${sendResult.sentFrom})` : ''}.`
            : `\n\n⚠️ Email draft ready but failed to send: ${sendResult.error}`;
        }
      } catch (emailComposeErr) {
        console.warn('Email compose/send error:', emailComposeErr.message);
      }
    }

    // --- Handle email archive action if detected ---
    let emailActionResult = null;
    if (emailAction === 'archive' && activeClient) {
      try {
        console.log(`📁 Auto-archiving emails for ${activeClient}...`);
        emailActionResult = await emailArchiver.archiveClientEmails(activeClient, { limit: 20 });
        console.log(`📁 Archived ${emailActionResult.archived} emails for ${activeClient}`);
      } catch (archiveErr) {
        console.warn('Email archive error:', archiveErr.message);
        emailActionResult = { error: archiveErr.message };
      }
    }

    // --- Handle phone action if detected ---
    let phoneActionResult = null;
    if (phoneAction) {
      try {
        const phoneLink = require('../../lib/phone-link-client');
        if (phoneAction === 'compose') {
          // Extract phone number from message or active client context
          const numMatch = message.match(/(\+?1?\d[\d\s\-().]{8,}\d)/);
          if (numMatch) {
            phoneActionResult = phoneLink.composeSMS(numMatch[1].replace(/[\s\-().]/g, ''));
          } else {
            phoneActionResult = { type: 'compose', needsNumber: true, note: 'Ask user for phone number or contact name' };
          }
        } else if (phoneAction === 'dial') {
          const numMatch = message.match(/(\+?1?\d[\d\s\-().]{8,}\d)/);
          if (numMatch) {
            phoneActionResult = phoneLink.openDialer(numMatch[1].replace(/[\s\-().]/g, ''));
          } else {
            phoneActionResult = { type: 'dial', needsNumber: true, note: 'Ask user for phone number or contact name' };
          }
        } else if (phoneAction === 'read') {
          // Data is already in phoneContextStr via system prompt — just note the action
          phoneActionResult = { type: 'read', injected: true };
        } else if (phoneAction === 'lookup') {
          const numMatch = message.match(/(\+?1?\d[\d\s\-().]{8,}\d)/);
          if (numMatch) {
            const contact = phoneLink.getContactByPhone(numMatch[1]);
            const legal = await phoneLink.matchPhoneToClient(numMatch[1]);
            phoneActionResult = { type: 'lookup', contact, legalContext: legal };
          }
        }
      } catch (phoneErr) {
        console.warn('Phone action error:', phoneErr.message);
        phoneActionResult = { error: phoneErr.message };
      }
    }

    // --- Handle transcription / hearing notes action ---
    let transcriptionResult = null;
    if (transcriptionAction) {
      try {
        const ts = require('../../lib/transcription-service');
        if (transcriptionAction === 'transcribe-latest') {
          const zoom = require('../../lib/zoom-client');
          const meetings = await zoom.listRecordings();
          if (meetings.length > 0) {
            meetings.sort((a, b) => new Date(b.start_time) - new Date(a.start_time));
            const result = await ts.processRecording(meetings[0]);
            transcriptionResult = { type: 'transcribe-latest', meeting: meetings[0].topic, ...result };
          } else {
            transcriptionResult = { type: 'transcribe-latest', error: 'No recordings found' };
          }
        } else if (transcriptionAction === 'hearing-notes') {
          // Enter hearing notes mode — get context for auto-fill
          const context = await ts.getHearingContext();
          transcriptionResult = { type: 'hearing-notes-ready', ...context, client: activeClient };
          // If the message itself contains notes (not just the trigger phrase), process them
          const notesContent = message.replace(/^.*?(hearing\s*notes?\s*(?:for\s*\w+)?|just\s*got\s*out\s*of\s*\w+(?:\s+\w+)*\s*\w*)\s*[:\-]?\s*/i, '').trim();
          if (notesContent.length > 20) {
            const result = await ts.processTextNotes(notesContent, {
              clientName: activeClient || '',
              topic: 'Hearing Notes'
            });
            transcriptionResult = { type: 'hearing-notes', ...result };
          }
        } else if (transcriptionAction === 'list-recordings') {
          const zoom = require('../../lib/zoom-client');
          const recs = await zoom.listRecordings();
          const lastProcessed = ts.getLastProcessed();
          transcriptionResult = {
            type: 'list-recordings',
            recordings: recs.map(m => ({
              topic: m.topic, startTime: m.start_time, duration: m.duration,
              processed: lastProcessed.timestamp ? new Date(m.start_time) <= new Date(lastProcessed.timestamp) : false
            })),
            count: recs.length
          };
        } else if (transcriptionAction === 'list-notes' && activeClient) {
          const notes = await ts.getNotesForClient(activeClient);
          transcriptionResult = { type: 'list-notes', notes, count: notes.length, client: activeClient };
        } else if (transcriptionAction === 'process-all') {
          const result = await ts.processNewRecordings();
          transcriptionResult = { type: 'process-all', ...result };
        }
      } catch (tsErr) {
        console.warn('Transcription action error:', tsErr.message);
        transcriptionResult = { error: tsErr.message };
      }
    }

    // --- Handle Zoom action if detected ---
    let zoomActionResult = null;
    if (zoomAction && activeClient) {
      try {
        const zoom = require('../../lib/zoom-client');
        if (zoomAction === 'create') {
          // Discovery gate: check for docs first
          const check = await zoom.hasDiscoveryDocs(activeClient);
          if (check.hasDiscovery) {
            const mtg = await zoom.createMeeting({
              topic: `Discovery Review — ${activeClient}`,
              startTime: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString(),
              duration: 60
            });
            zoomActionResult = { type: 'created', meeting: mtg, discoveryFiles: check.files };
          } else {
            // Remedial actions
            const remedial = await zoom.handleMissingDiscovery(activeClient, {});
            zoomActionResult = { type: 'remedial', gated: true, ...remedial };
          }
        } else if (zoomAction === 'list') {
          const meetings = await zoom.listMeetings();
          zoomActionResult = { type: 'list', meetings, count: meetings.length };
        } else if (zoomAction === 'discovery-check') {
          const check = await zoom.hasDiscoveryDocs(activeClient);
          zoomActionResult = { type: 'discovery-check', ...check };
        } else if (zoomAction === 'recordings') {
          const recs = await zoom.listRecordings();
          zoomActionResult = { type: 'recordings', recordings: recs, count: recs.length };
        }
      } catch (zoomErr) {
        console.warn('Zoom action error:', zoomErr.message);
        zoomActionResult = { error: zoomErr.message };
      }
    }

    // --- Handle note-taking action ---
    let noteActionResult = null;
    if (noteAction) {
      try {
        const { queryD1: noteQueryD1 } = require('../../lib/graph-client');
        if (noteAction === 'save' && activeClient) {
          // Extract the note content from the message (strip the command prefix)
          let noteContent = message.replace(/^\s*\/notes?\s*/i, '').replace(/\b(note\s+that|make\s+a\s+note|jot\s+down|take\s+a\s+note|add\s+a?\s*note|save\s+a?\s*note|log\s+this|record\s+that)\s*[:\-]?\s*/i, '').trim();
          if (noteContent.length > 5) {
            // Look up case number from party_cache
            const pcRows = await noteQueryD1(`SELECT case_number FROM party_cache WHERE client_name LIKE '%${activeClient.replace(/'/g, "''")}%' LIMIT 1`);
            const caseNum = pcRows.length ? pcRows[0].case_number : '';
            const sql = `INSERT INTO case_notes (client_name, case_number, note_type, title, content, source, created_by) VALUES ('${activeClient.replace(/'/g, "''")}', '${caseNum.replace(/'/g, "''")}', 'general', '', '${noteContent.replace(/'/g, "''")}', 'synthia-chat', 'synthia')`;
            await noteQueryD1(sql);
            noteActionResult = { type: 'saved', client: activeClient, preview: noteContent.substring(0, 100) };
            if (consensus?.consensus) {
              consensus.consensus += `\n\n📝 Note saved for ${activeClient}.`;
            }
          }
        } else if (noteAction === 'list') {
          const client = activeClient || '';
          let sql = 'SELECT id, client_name, note_type, title, content, created_at FROM case_notes';
          if (client) sql += ` WHERE client_name LIKE '%${client.replace(/'/g, "''")}%'`;
          sql += ' ORDER BY created_at DESC LIMIT 10';
          const notes = await noteQueryD1(sql);
          noteActionResult = { type: 'list', notes, count: notes.length, client };
          // Inject notes into context for AI to discuss
          if (notes.length && consensus?.consensus) {
            const notesSummary = notes.map(n => `- [${n.created_at}] ${n.note_type}: ${(n.content || '').substring(0, 120)}`).join('\n');
            consensus.consensus += `\n\n📝 Recent notes${client ? ` for ${client}` : ''}:\n${notesSummary}`;
          }
        } else if (noteAction === 'meeting' && activeClient) {
          // AI response should contain structured meeting notes — save them
          if (consensus?.consensus) {
            const pcRows = await noteQueryD1(`SELECT case_number FROM party_cache WHERE client_name LIKE '%${activeClient.replace(/'/g, "''")}%' LIMIT 1`);
            const caseNum = pcRows.length ? pcRows[0].case_number : '';
            const title = `Meeting Notes - ${new Date().toLocaleDateString()}`;
            const sql = `INSERT INTO case_notes (client_name, case_number, note_type, title, content, source, created_by) VALUES ('${activeClient.replace(/'/g, "''")}', '${caseNum.replace(/'/g, "''")}', 'meeting', '${title.replace(/'/g, "''")}', '${message.replace(/'/g, "''")}', 'synthia-chat', 'user')`;
            await noteQueryD1(sql);
            noteActionResult = { type: 'meeting-saved', client: activeClient };
            consensus.consensus += `\n\n📝 Meeting notes saved for ${activeClient}.`;
          }
        } else if (noteAction === 'file-notes' && activeClient) {
          // File recent notes to OneDrive
          const notes = await noteQueryD1(`SELECT * FROM case_notes WHERE client_name LIKE '%${activeClient.replace(/'/g, "''")}%' AND onedrive_path IS NULL ORDER BY created_at DESC LIMIT 5`);
          if (notes.length) {
            const { findClientFolder: findFolder } = require('../../lib/email-archiver');
            const clientFolder = await findFolder(activeClient);
            if (clientFolder) {
              let filed = 0;
              for (const note of notes) {
                try {
                  const dateStr = new Date(note.created_at).toISOString().split('T')[0];
                  const fileName = `${dateStr}_${note.note_type}_${(note.title || 'note').replace(/[^a-zA-Z0-9 ]/g, '').substring(0, 30)}.txt`;
                  const uploadPath = `${clientFolder}/Notes/${fileName}`;
                  await graphClient.uploadFile(uploadPath, Buffer.from(note.content, 'utf8'));
                  await noteQueryD1(`UPDATE case_notes SET onedrive_path = '${uploadPath.replace(/'/g, "''")}', updated_at = datetime('now') WHERE id = ${note.id}`);
                  filed++;
                } catch (e) { console.warn('Note file error:', e.message); }
              }
              noteActionResult = { type: 'filed', count: filed, client: activeClient };
              if (consensus?.consensus) consensus.consensus += `\n\n📁 Filed ${filed} note(s) to ${activeClient}'s OneDrive folder.`;
            }
          }
        }
      } catch (noteErr) {
        console.warn('Note action error:', noteErr.message);
        noteActionResult = { error: noteErr.message };
      }
    }

    // --- Handle document generation action ---
    let documentActionResult = null;
    if (documentAction === 'generate' && consensus?.consensus) {
      try {
        const parsed = parseDocumentFromResponse(consensus.consensus);
        if (parsed) {
          console.log(`📄 Document generation: template=${parsed.templateId}, fields=${Object.keys(parsed.data).length}`);
          // Dynamically require generate-document.js for its generateDocument function
          // Clear require cache to get fresh registry each time
          delete require.cache[require.resolve(GENERATE_DOC_PATH)];
          const { generateDocument } = require(GENERATE_DOC_PATH);
          const result = generateDocument(parsed.templateId, parsed.data);

          if (result.success) {
            console.log(`📄 Document generated: ${result.outputPath}`);
            documentActionResult = {
              type: 'generated',
              templateId: result.templateId,
              outputPath: result.outputPath,
              fileName: path.basename(result.outputPath),
              missingRequired: result.missingRequired
            };

            // If user also said "file" or "save", upload to client OneDrive folder
            if (documentAction === 'file' || /\b(file|save|upload)\b.*\b(folder|onedrive)\b/i.test(message)) {
              const clientForUpload = parsed.data.CLIENT_NAME || activeClient;
              if (clientForUpload) {
                try {
                  const clientFolder = await findClientFolder(clientForUpload);
                  if (clientFolder) {
                    const fileBuffer = fs.readFileSync(result.outputPath);
                    const uploadPath = `${clientFolder}/${path.basename(result.outputPath)}`;
                    const uploadResult = await graphClient.uploadFile(uploadPath, fileBuffer);
                    documentActionResult.uploaded = true;
                    documentActionResult.uploadPath = uploadPath;
                    documentActionResult.webUrl = uploadResult?.webUrl || null;
                    consensus.consensus += `\n\n✅ Document filed to ${clientForUpload}'s case folder: ${path.basename(result.outputPath)}`;
                  }
                } catch (uploadErr) {
                  console.warn('Document upload error:', uploadErr.message);
                  documentActionResult.uploadError = uploadErr.message;
                }
              }
            }

            // Append status to consensus
            if (!documentActionResult.uploaded) {
              consensus.consensus += `\n\n📄 Document generated: **${path.basename(result.outputPath)}**`;
              if (result.missingRequired.length > 0) {
                consensus.consensus += `\n⚠️ Missing fields (left as placeholders): ${result.missingRequired.join(', ')}`;
              }
              consensus.consensus += `\nUse "file this to [client]'s folder" to upload it to OneDrive.`;
            }
          }
        }
      } catch (docErr) {
        console.warn('Document generation error:', docErr.message);
        documentActionResult = { type: 'error', error: docErr.message };
        consensus.consensus += `\n\n⚠️ Document generation failed: ${docErr.message}`;
      }
    }

    // --- Handle file-only action (save existing document to client folder) ---
    if (documentAction === 'file' && !documentActionResult && activeClient) {
      // Check if there's a recently generated document to file
      const outputDir = path.join(__dirname, '../../../output');
      if (fs.existsSync(outputDir)) {
        const files = fs.readdirSync(outputDir).filter(f => f.endsWith('.docx') || f.endsWith('.pdf'));
        if (files.length > 0) {
          // Get most recent file
          const sorted = files.map(f => ({ name: f, mtime: fs.statSync(path.join(outputDir, f)).mtime }))
            .sort((a, b) => b.mtime - a.mtime);
          const recentFile = sorted[0];
          // Only file documents generated in the last 10 minutes
          if (Date.now() - recentFile.mtime.getTime() < 10 * 60 * 1000) {
            try {
              const clientFolder = await findClientFolder(activeClient);
              if (clientFolder) {
                const filePath = path.join(outputDir, recentFile.name);
                const fileBuffer = fs.readFileSync(filePath);
                const uploadPath = `${clientFolder}/${recentFile.name}`;
                const uploadResult = await graphClient.uploadFile(uploadPath, fileBuffer);
                documentActionResult = {
                  type: 'filed',
                  fileName: recentFile.name,
                  uploadPath,
                  webUrl: uploadResult?.webUrl || null
                };
                consensus.consensus += `\n\n✅ Filed **${recentFile.name}** to ${activeClient}'s case folder.`;
              }
            } catch (fileErr) {
              console.warn('File upload error:', fileErr.message);
            }
          }
        }
      }
    }

    // --- Handle deadline management action ---
    let deadlineActionResult = null;
    if (deadlineAction && consensus?.consensus) {
      try {
        const parsed = parseDeadlineFromResponse(consensus.consensus);
        if (parsed) {
          console.log(`📅 Deadline action: ${parsed.action} for ${parsed.clientName}`);
          const { queryD1 } = require('../../lib/graph-client');

          if (parsed.action === 'update') {
            // Find the existing deadline for this client
            const existing = await queryD1(
              `SELECT id, client_name, case_number, due_date, hearing_time, deadline_type, court, courtroom, judge, court_address, court_phone, hearing_mode, virtual_link FROM deadlines WHERE LOWER(client_name) LIKE '%${parsed.clientName.toLowerCase().split(' ')[0]}%' AND status IN ('active', 'pending') ORDER BY due_date ASC LIMIT 5`
            );
            if (existing.length > 0) {
              const target = existing[0];
              const setClauses = [];
              if (parsed.dueDate) setClauses.push(`due_date = '${parsed.dueDate}'`);
              if (parsed.hearingTime) setClauses.push(`hearing_time = '${parsed.hearingTime}'`);
              if (parsed.deadlineType && parsed.deadlineType !== 'Calendar Event') setClauses.push(`deadline_type = '${parsed.deadlineType}'`);
              if (parsed.court) setClauses.push(`court = '${parsed.court}'`);
              if (parsed.judge) setClauses.push(`judge = '${parsed.judge}'`);
              if (parsed.courtroom) setClauses.push(`courtroom = '${parsed.courtroom}'`);
              if (parsed.notes) setClauses.push(`notes = '${parsed.notes.replace(/'/g, "''")}'`);

              if (setClauses.length > 0) {
                await queryD1(`UPDATE deadlines SET ${setClauses.join(', ')} WHERE id = ${target.id}`);
                deadlineActionResult = { type: 'updated', client: target.client_name, oldDate: target.due_date, newDate: parsed.dueDate || target.due_date };
                consensus.consensus += `\n\n✅ Updated ${target.client_name}'s deadline: ${target.due_date} → ${parsed.dueDate || target.due_date}${parsed.hearingTime ? ' at ' + parsed.hearingTime : ''}`;
                // Sync to Outlook calendar
                try {
                  const updatedDeadline = { ...target, due_date: parsed.dueDate || target.due_date, hearing_time: parsed.hearingTime || target.hearing_time, deadline_type: (parsed.deadlineType && parsed.deadlineType !== 'Calendar Event') ? parsed.deadlineType : target.deadline_type, court: parsed.court || target.court, judge: parsed.judge || target.judge, courtroom: parsed.courtroom || target.courtroom };
                  await outlookCal.syncDeadlineToOutlook(updatedDeadline, target);
                  console.log('📅 Outlook: synced updated deadline');
                } catch (olErr) { console.warn('📅 Outlook sync error:', olErr.message); }
              }
            } else {
              deadlineActionResult = { type: 'not_found', client: parsed.clientName };
              consensus.consensus += `\n\n⚠️ No pending deadline found for "${parsed.clientName}". Check the name and try again.`;
            }
          } else if (parsed.action === 'add' && parsed.dueDate) {
            await queryD1(
              `INSERT INTO deadlines (client_name, case_number, deadline_type, description, due_date, hearing_time, court, courtroom, judge, status, source, created_at) VALUES ('${parsed.clientName.replace(/'/g, "''")}', '', '${parsed.deadlineType}', '${(parsed.deadlineType + ' - ' + parsed.clientName).replace(/'/g, "''")}', '${parsed.dueDate}', '${parsed.hearingTime || ''}', '${parsed.court || ''}', '${parsed.courtroom || ''}', '${parsed.judge || ''}', 'pending', 'manual', datetime('now'))`
            );
            deadlineActionResult = { type: 'added', client: parsed.clientName, date: parsed.dueDate };
            consensus.consensus += `\n\n✅ Added ${parsed.deadlineType} for ${parsed.clientName} on ${parsed.dueDate}${parsed.hearingTime ? ' at ' + parsed.hearingTime : ''}`;
            // Sync to Outlook calendar
            try {
              await outlookCal.syncDeadlineToOutlook({ client_name: parsed.clientName, case_number: '', deadline_type: parsed.deadlineType, due_date: parsed.dueDate, hearing_time: parsed.hearingTime || '', court: parsed.court || '', courtroom: parsed.courtroom || '', judge: parsed.judge || '' });
              console.log('📅 Outlook: synced new deadline');
            } catch (olErr) { console.warn('📅 Outlook sync error:', olErr.message); }
          } else if (parsed.action === 'delete') {
            const existing = await queryD1(
              `SELECT id, client_name, case_number, due_date, hearing_time, deadline_type, court, courtroom, judge, court_address, court_phone, hearing_mode, virtual_link FROM deadlines WHERE LOWER(client_name) LIKE '%${parsed.clientName.toLowerCase().split(' ')[0]}%' AND status IN ('active', 'pending') ORDER BY due_date ASC LIMIT 1`
            );
            if (existing.length > 0) {
              // Remove from Outlook calendar first (need data before D1 delete)
              try {
                await outlookCal.deleteOutlookEvent(existing[0]);
                console.log('📅 Outlook: deleted event');
              } catch (olErr) { console.warn('📅 Outlook delete error:', olErr.message); }
              await queryD1(`DELETE FROM deadlines WHERE id = ${existing[0].id}`);
              deadlineActionResult = { type: 'deleted', client: existing[0].client_name, date: existing[0].due_date };
              consensus.consensus += `\n\n✅ Removed: ${existing[0].client_name} — ${existing[0].deadline_type} on ${existing[0].due_date}`;
            } else {
              deadlineActionResult = { type: 'not_found', client: parsed.clientName };
            }
          } else if (parsed.action === 'complete') {
            const existing = await queryD1(
              `SELECT id, client_name, case_number, due_date, hearing_time, deadline_type, court, courtroom, judge, court_address, court_phone, hearing_mode, virtual_link FROM deadlines WHERE LOWER(client_name) LIKE '%${parsed.clientName.toLowerCase().split(' ')[0]}%' AND status IN ('active', 'pending') ORDER BY due_date ASC LIMIT 1`
            );
            if (existing.length > 0) {
              await queryD1(`UPDATE deadlines SET status = 'completed', completed_at = datetime('now') WHERE id = ${existing[0].id}`);
              deadlineActionResult = { type: 'completed', client: existing[0].client_name, date: existing[0].due_date };
              consensus.consensus += `\n\n✅ Completed: ${existing[0].client_name} — ${existing[0].deadline_type} on ${existing[0].due_date}`;
              // Remove from Outlook calendar
              try {
                await outlookCal.completeOutlookEvent(existing[0]);
                console.log('📅 Outlook: removed completed event');
              } catch (olErr) { console.warn('📅 Outlook complete error:', olErr.message); }
            }
          }
        }
      } catch (dlErr) {
        console.warn('Deadline action error:', dlErr.message);
        deadlineActionResult = { type: 'error', error: dlErr.message };
      }
    }

    // Strip structured blocks from consensus before sending to frontend
    if (consensus?.consensus) {
      consensus.consensus = consensus.consensus.replace(/===DEADLINE===[\s\S]*?===END_DEADLINE===/g, '').trim();
    }

    res.json({
      success: true,
      consensus: consensus.consensus,
      sources: consensus.sources,
      confidence: consensus.confidence,
      operationalAIs: responses.filter(r => r.success).length,
      totalAIs: availableModels.length,
      emailAction: emailActionResult ? {
        type: emailAction,
        client: activeClient,
        ...emailActionResult
      } : null,
      documentAction: documentActionResult ? {
        client: activeClient,
        ...documentActionResult
      } : null,
      deadlineAction: deadlineActionResult ? {
        client: activeClient,
        ...deadlineActionResult
      } : null,
      zoomAction: zoomActionResult ? {
        client: activeClient,
        ...zoomActionResult
      } : null,
      phoneAction: phoneActionResult ? {
        type: phoneAction,
        ...phoneActionResult
      } : null,
      transcriptionAction: transcriptionResult ? {
        client: activeClient,
        ...transcriptionResult
      } : null,
      noteAction: noteActionResult ? {
        client: activeClient,
        ...noteActionResult
      } : null,
      caseQuery: caseQueryResult ? {
        used: true,
        clientName: caseQueryResult.clientName,
        sources: caseQueryResult.sources,
        answer: caseQueryResult.answer
      } : null
    });
  } catch (error) {
    console.error('THE BRIDGE error:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// ============================================
// EMAIL ACTION ENDPOINTS
// ============================================

/**
 * Send an email
 * POST /api/bridges/email/send
 */
router.post('/email/send', async (req, res) => {
  try {
    const { to, subject, body, cc, replyToId, isHtml, from } = req.body;

    if (!to || !subject) {
      return res.status(400).json({ success: false, error: 'to and subject are required' });
    }

    let result;
    if (replyToId) {
      result = await emailClient.replyToEmail(replyToId, body);
    } else {
      result = await emailClient.sendEmail({
        to, subject, body, cc,
        isHtml: isHtml !== false,
        from: from || 'Associate@dianepitcher.com'
      });
    }

    console.log(`📧 Email ${replyToId ? 'reply' : 'sent'}: from=${result.sentFrom || 'default'}, to=${to}, subject="${subject}", success=${result.success}${result.fallback ? ' (fallback)' : ''}`);
    res.json(result);
  } catch (error) {
    console.error('Email send error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * Get recent emails (with optional search/client filter)
 * GET /api/bridges/email/recent?search=term&limit=10&client=NAME
 */
router.get('/email/recent', async (req, res) => {
  try {
    const { search, limit = 10, client } = req.query;

    let emails;
    if (client) {
      emails = await emailClient.getClientEmails(client, parseInt(limit));
    } else {
      emails = await emailClient.getRecentEmails({ search, limit: parseInt(limit) });
    }

    const enriched = await emailClient.enrichEmailsWithIdentity(emails);
    res.json({ success: true, emails: enriched, count: enriched.length });
  } catch (error) {
    console.error('Email fetch error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * Archive emails for a client as PDFs to their OneDrive folder
 * POST /api/bridges/email/archive
 */
router.post('/email/archive', async (req, res) => {
  try {
    const { clientName, limit = 20, folderPath } = req.body;

    if (!clientName) {
      return res.status(400).json({ success: false, error: 'clientName is required' });
    }

    console.log(`📁 Archiving emails for ${clientName} (limit: ${limit})...`);
    const result = await emailArchiver.archiveClientEmails(clientName, { limit, folderPath });

    console.log(`📁 Archive complete: ${result.archived} archived, ${result.skipped} skipped, ${result.errors.length} errors`);
    res.json({ success: true, ...result });
  } catch (error) {
    console.error('Email archive error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * Resolve a contact identity from email address
 * GET /api/bridges/email/contact?email=address
 */
router.get('/email/contact', async (req, res) => {
  try {
    const { email } = req.query;
    if (!email) return res.status(400).json({ success: false, error: 'email parameter required' });

    const identity = await emailClient.resolveContact(email);
    res.json({ success: true, identity });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================
// DOCUMENT GENERATION ENDPOINTS
// ============================================

/**
 * Generate a legal document
 * POST /api/bridges/document/generate
 */
router.post('/document/generate', async (req, res) => {
  try {
    const { templateId, data, uploadTo } = req.body;

    if (!templateId) {
      return res.status(400).json({ success: false, error: 'templateId is required' });
    }

    // Clear require cache to get fresh registry
    delete require.cache[require.resolve(GENERATE_DOC_PATH)];
    const { generateDocument } = require(GENERATE_DOC_PATH);
    const result = generateDocument(templateId, data || {});

    if (!result.success) {
      return res.status(500).json({ success: false, error: 'Document generation failed' });
    }

    console.log(`📄 API: Document generated: ${result.outputPath}`);

    let uploadResult = null;
    if (uploadTo) {
      try {
        const fileBuffer = fs.readFileSync(result.outputPath);
        const uploadPath = `${uploadTo}/${path.basename(result.outputPath)}`;
        uploadResult = await graphClient.uploadFile(uploadPath, fileBuffer);
      } catch (uploadErr) {
        console.warn('Document upload error:', uploadErr.message);
      }
    }

    res.json({
      success: true,
      templateId: result.templateId,
      outputPath: result.outputPath,
      fileName: path.basename(result.outputPath),
      missingRequired: result.missingRequired,
      fieldsUsed: result.fieldsUsed,
      uploaded: uploadResult ? true : false,
      webUrl: uploadResult?.webUrl || null
    });
  } catch (error) {
    console.error('Document generate error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * List available templates
 * GET /api/bridges/document/templates
 */
router.get('/document/templates', (req, res) => {
  try {
    const registryPath = path.join(__dirname, '../../../config/template-registry.json');
    const registry = JSON.parse(fs.readFileSync(registryPath, 'utf8'));
    const templates = Object.entries(registry.templates).map(([id, t]) => ({
      id,
      description: t.description,
      category: t.category,
      requiredFields: t.requiredFields,
      optionalFields: t.optionalFields
    }));
    res.json({ success: true, templates, aliases: registry.aliases });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * Upload a file to a client's OneDrive case folder
 * POST /api/bridges/document/file
 */
router.post('/document/file', async (req, res) => {
  try {
    const { clientName, filePath: localPath, fileName } = req.body;

    if (!clientName || !localPath) {
      return res.status(400).json({ success: false, error: 'clientName and filePath are required' });
    }

    if (!fs.existsSync(localPath)) {
      return res.status(404).json({ success: false, error: 'File not found: ' + localPath });
    }

    const clientFolder = await findClientFolder(clientName);
    if (!clientFolder) {
      return res.status(404).json({ success: false, error: 'Client folder not found for: ' + clientName });
    }

    const name = fileName || path.basename(localPath);
    const fileBuffer = fs.readFileSync(localPath);
    const uploadPath = `${clientFolder}/${name}`;
    const uploadResult = await graphClient.uploadFile(uploadPath, fileBuffer);

    console.log(`📁 Filed: ${name} → ${uploadPath}`);
    res.json({
      success: true,
      fileName: name,
      uploadPath,
      webUrl: uploadResult?.webUrl || null
    });
  } catch (error) {
    console.error('Document file error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * Download a generated document
 * GET /api/bridges/document/download?file=filename
 */
router.get('/document/download', (req, res) => {
  try {
    const { file } = req.query;
    if (!file) return res.status(400).json({ success: false, error: 'file parameter required' });

    // Sanitize — only allow filenames, no path traversal
    const sanitized = path.basename(file);
    const filePath = path.join(__dirname, '../../../output', sanitized);

    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ success: false, error: 'File not found' });
    }

    res.download(filePath, sanitized);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ===== PHONE LINK API ENDPOINTS =====

/**
 * GET /api/bridges/phone/contacts/search?q=query&limit=20
 * Search phone contacts by name, number, or company
 */
router.get('/phone/contacts/search', (req, res) => {
  try {
    const phone = require('../../lib/phone-link-client');
    const { q, limit = 20 } = req.query;
    if (!q) return res.status(400).json({ success: false, error: 'q parameter required' });
    const contacts = phone.searchContacts(q, parseInt(limit));
    res.json({ success: true, count: contacts.length, contacts });
  } catch (error) {
    console.error('Phone contacts search error:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * GET /api/bridges/phone/contacts/lookup?number=+14355121809
 * Look up a contact by phone number
 */
router.get('/phone/contacts/lookup', (req, res) => {
  try {
    const phone = require('../../lib/phone-link-client');
    const { number } = req.query;
    if (!number) return res.status(400).json({ success: false, error: 'number parameter required' });
    const contact = phone.getContactByPhone(number);
    res.json({ success: true, found: !!contact, contact });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * GET /api/bridges/phone/messages/recent?limit=20
 * Get recent SMS messages across all conversations
 */
router.get('/phone/messages/recent', (req, res) => {
  try {
    const phone = require('../../lib/phone-link-client');
    const { limit = 20 } = req.query;
    const messages = phone.getRecentMessages(parseInt(limit));
    res.json({ success: true, count: messages.length, messages });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * GET /api/bridges/phone/messages/for?contact=name OR number=+1234567890&limit=20
 * Get messages for a specific contact or phone number
 */
router.get('/phone/messages/for', (req, res) => {
  try {
    const phone = require('../../lib/phone-link-client');
    const { contact, number, limit = 20 } = req.query;
    let messages;
    if (contact) {
      messages = phone.getMessagesForContact(contact, parseInt(limit));
    } else if (number) {
      messages = phone.getMessagesForNumber(number, parseInt(limit));
    } else {
      return res.status(400).json({ success: false, error: 'contact or number parameter required' });
    }
    res.json({ success: true, count: messages.length, messages });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * GET /api/bridges/phone/unread
 * Get unread message count and conversations with unread messages
 */
router.get('/phone/unread', (req, res) => {
  try {
    const phone = require('../../lib/phone-link-client');
    const unreadCount = phone.getUnreadCount();
    const unreadConversations = phone.getUnreadConversations();
    res.json({ success: true, unreadCount, conversations: unreadConversations });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * GET /api/bridges/phone/stats
 * Get phone link stats (contact count, message count, unread count)
 */
router.get('/phone/stats', (req, res) => {
  try {
    const phone = require('../../lib/phone-link-client');
    const contactCount = phone.getContactCount();
    const convStats = phone.getConversationStats();
    const unreadCount = phone.getUnreadCount();
    res.json({
      success: true,
      contactCount,
      ...convStats,
      unreadCount
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /api/bridges/phone/compose
 * Open SMS compose window for a phone number
 * Body: { number: "+1234567890", body: "optional pre-fill text" }
 */
router.post('/phone/compose', (req, res) => {
  try {
    const phone = require('../../lib/phone-link-client');
    const { number, body } = req.body;
    if (!number) return res.status(400).json({ success: false, error: 'number required' });
    const result = phone.composeSMS(number, body || '');
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * POST /api/bridges/phone/dial
 * Open phone dialer for a number
 * Body: { number: "+1234567890" }
 */
router.post('/phone/dial', (req, res) => {
  try {
    const phone = require('../../lib/phone-link-client');
    const { number } = req.body;
    if (!number) return res.status(400).json({ success: false, error: 'number required' });
    const result = phone.openDialer(number);
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

/**
 * GET /api/bridges/phone/identify?number=+1234567890
 * Cross-reference a phone number with D1 (court_contacts, opposing_counsel_intel, Phone Link contacts)
 */
router.get('/phone/identify', async (req, res) => {
  try {
    const phone = require('../../lib/phone-link-client');
    const { number } = req.query;
    if (!number) return res.status(400).json({ success: false, error: 'number parameter required' });

    // Check Phone Link contacts
    const contact = phone.getContactByPhone(number);
    // Check D1 for legal context
    const legalMatch = await phone.matchPhoneToClient(number);

    res.json({
      success: true,
      number,
      phoneContact: contact,
      legalContext: legalMatch,
      identified: !!(contact || legalMatch)
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;

