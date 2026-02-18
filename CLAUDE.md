# F:\ Project Instructions

Read this at the start of every conversation. Project-specific instructions, preferences, coding conventions.

## Behavior
- **Brief and direct.** Maximum 2-3 sentences unless complex analysis is required.
- **Answer only what was asked.** No tangents.
- **Consistency.** Do things the same way unless a variable is explicitly added or changed.

## Key Locations
- `C:\Users\EBPC\.claude\projects\F--\memory\MEMORY.md` - Persistent memory across conversations
- `C:\Users\EBPC\.claude\CLAUDE.md` - Global instructions for all projects

## ESQs Law / ai-communication
- Open Cases: `F:\Office Associate\Open Cases`
- Firm: PITCHER LAW PLLC (Diane Pitcher, esqslaw@gmail.com)
- Law Matrix: `src/core/knowledge-artifacts-pitcher-law.js`
- Document generator: `src/services/document-generator-service.js`

---

## Law Matrix (LM v6.0)

Framework for document generation and legal practice. NOT a firm name.

**Standards:**
- Structure: Numbered paragraphs only - NO BULLETS
- Tone: Brief, direct, to the point. No fluff.
- Jurisdiction: Utah (CUC, URCP, Bluebook)
- Citation: Bluebook
- Formatting: Return doc as clean, copy-pasteable for MS Word
- Artifact workflow: Identify project; find prior artifact; continue building on same artifact; do not invent facts/law; request clarification on ambiguities; timestamp updates
- Search priority: Open Cases folder first, then other sources
- Focus: No phantom law, cases, or facts. Use only verifiable sources.

**Personnel:**
- DP: Diane Pitcher, Owner and Managing Partner, esqslaw@gmail.com
- JWA3: John William Adams III, Attorney, bar #19429, $390/hr (primary user)
- DO NOT REFER: Travis R. Christiansen (TRC) - archived unless specifically asked

**Acronyms:** CUC, URCP, AOC, MTS, MTE, OSC, MTQ, TRO, EH, MSA, MTI, IntD, RFP, NC, CIO, Cal

---

## Document Generator

Main flow: `generateDocument(templateId, fieldValues, options)` in `src/services/document-generator-service.js`

**Options:** clientName, caseId, useAI, useRAG, userId (default JWA3)

**Flow:**
1. Validate required fields from template
2. Auto-fill from master sheet if caseId (PRIORITY)
3. Auto-fill from RAG/Memory Bank if clientName and no caseId
4. MANDATORY party verification (prevents attorney-as-party, swapped parties)
5. RAG context for client knowledge
6. AI enhancement if useAI + template.autoPrompt
7. Handlebars compile + Law Matrix formatting
8. Save to data/generated-documents/

**Handlebars helpers:** heading, signature, verification, juryDemand, date, court

**Key files:** legal-document-templates, party-verification-service, case-master-sheet-storage, utah-legal-knowledge-service
