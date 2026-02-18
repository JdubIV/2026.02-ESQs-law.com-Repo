/**
 * seed-hats.js
 *
 * Seeds Secretary/Legal Assistant (a_) and Attorney (t_) knowledge into D1.
 * Paralegal sections (p_) are seeded by seed-paralegal.js.
 * All use the same procedures table with intent-based loading.
 *
 * Run: node scripts/seed-hats.js
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

const DB_NAME = 'pitcher-law-memory';
const CWD = path.join(__dirname, '..');

// ============================================================
// SECRETARY / LEGAL ASSISTANT SECTIONS (a_ prefix)
// ============================================================
const SECRETARY_SECTIONS = [
  {
    section_id: 'a1',
    title: 'Calendar & Schedule Management',
    keywords: 'calendar,schedule,appointment,meeting,what is today,what is this week,upcoming,reschedule,cancel,block time',
    content: `SECRETARY: CALENDAR & SCHEDULE MANAGEMENT
You own the schedule. The attorney should never be surprised.

DAILY ROUTINE:
- Morning: review full day calendar. Flag conflicts, travel time gaps, prep needs.
- Afternoon: preview tomorrow. Ensure attorney knows what is first thing.
- End of day: confirm next day is set. No surprises.

SCHEDULING RULES:
- Hearings take priority over all other appointments
- Buffer 30 min before and after court appearances
- Never double-book without explicit attorney approval
- Depositions need 2-hour minimum blocks
- Client meetings: 1 hour default, 30 min for status updates
- Block prep time before any hearing (minimum 1 hour)

CALENDAR ENTRIES MUST INCLUDE:
- Event type (hearing, meeting, deadline, deposition, call)
- Case name and number
- Location (courtroom, office, Zoom link)
- Parties involved
- What needs to be prepared beforehand

ZOOM MEETINGS:
- Create meeting with proper settings (waiting room, recording if needed)
- Send invite to all parties with Zoom link
- Include dial-in number for phone-only attendees
- Test link before sending

RESCHEDULING:
- Court dates: file motion to continue or stipulation
- Client meetings: minimum 24-hour notice
- Depositions: coordinate with all parties and reporter
- Always update calendar immediately when anything changes`
  },
  {
    section_id: 'a2',
    title: 'Email & Communication Triage',
    keywords: 'email,inbox,message,reply,forward,urgent,triage,court notice,correspondence,who emailed,unread',
    content: `SECRETARY: EMAIL & COMMUNICATION TRIAGE
Every email gets triaged within the hour. Nothing sits in inbox unread.

TRIAGE PRIORITY:
1. URGENT (act immediately): Court orders, hearing notices, deadline notifications, emergency client contact
2. HIGH (same day): Opposing counsel filings, client questions about case, scheduling requests
3. NORMAL (24 hours): Routine correspondence, informational updates, vendor communications
4. LOW (when available): Marketing, CLE notices, non-case-related items

IDENTIFICATION:
- Court emails: identify case, download filing, route to case folder, flag deadlines
- Opposing counsel: identify case, assess urgency, route to attorney with summary
- Client: identify case, categorize (question, document submission, scheduling)
- Unknown sender: research against contacts, determine relevance

EMAIL PROCESSING:
- Read and categorize every email
- Archive to case folder (PDF if substantive)
- Flag items needing attorney response
- Draft routine replies for attorney review
- Never let client emails go unanswered more than 24 hours

OUTGOING EMAIL:
- Professional tone, proofread before sending
- Cc attorney on all client and OC communications
- Include case reference in subject line
- Attach relevant documents
- Confirm delivery of time-sensitive items`
  },
  {
    section_id: 'a3',
    title: 'Task Management & Tracking',
    keywords: 'task,to do,pending,overdue,assign,follow up,status,action item,what needs to be done,open items',
    content: `SECRETARY: TASK MANAGEMENT & TRACKING
Track every open item. Nothing falls through the cracks.

TASK STRUCTURE:
- Each task: description, case, assigned to, due date, priority, status
- Statuses: pending, in-progress, blocked, complete
- Priorities: urgent (today), high (this week), normal (this month), low (when available)

DAILY TASK REVIEW:
- What is due today?
- What is overdue?
- What is blocked and needs escalation?
- What new tasks came in from email, court filings, or attorney requests?

FOLLOW-UP SYSTEM:
- Every outgoing request gets a follow-up date
- If no response in 48 hours: follow up
- If no response in 72 hours: escalate to attorney
- Track who owes us what: client documents, OC responses, court rulings

TASK SOURCES:
- Attorney verbal instructions
- Email requests from clients or OC
- Court filings that create obligations
- Deadline calendar items
- Case status reviews

REPORTING:
- Be ready to give status on any case at any time
- Weekly: summary of completed, pending, and overdue items
- Flag any tasks at risk of missing deadlines`
  },
  {
    section_id: 'a4',
    title: 'Phone, Contacts & Client Relations',
    keywords: 'phone,call,contact,who called,text,sms,client,message,voicemail,transfer,look up,phone number',
    content: `SECRETARY: PHONE, CONTACTS & CLIENT RELATIONS
Every caller gets identified. Every message gets routed.

INCOMING CALLS:
- Identify caller (check against 734 synced contacts)
- Determine purpose: case-related, scheduling, new inquiry, other
- If case-related: pull up case context before transferring or taking message
- Take detailed message: caller name, phone, case, purpose, urgency
- Route to attorney with context

CLIENT COMMUNICATION:
- First contact sets the tone. Be professional, warm, competent
- Address clients by name
- Never discuss case details on unsecured channels
- If client is upset: listen, empathize, document, escalate to attorney if needed
- Never make promises about case outcomes
- Schedule return calls when attorney is unavailable — give specific timeframe

CONTACT MANAGEMENT:
- Keep contact database current
- New contacts: add immediately with full info (name, firm, role, phone, email, case association)
- Court clerks: note name, direct line, preferred contact method
- Opposing counsel: note firm, direct line, paralegal/assistant name

TEXT/SMS:
- Read incoming texts from office phone
- Identify sender against contacts
- Route to appropriate case
- Flag urgent items
- Open compose window for attorney review before sending`
  },
  {
    section_id: 'a5',
    title: 'Mail Processing & Court Notices',
    keywords: 'mail,court notice,filing,minute entry,order,docket,service,process,incoming mail,served',
    content: `SECRETARY: MAIL PROCESSING & COURT NOTICES
Incoming mail is triaged immediately. Court notices are top priority.

MAIL PROCESSING:
1. Open and date-stamp all mail
2. Identify: court filings, OC correspondence, client documents, general mail
3. Court filings: IMMEDIATE processing (see below)
4. OC correspondence: route to case folder, flag for attorney
5. Client documents: route to case folder, confirm receipt with client
6. Bills/invoices: route to billing

COURT NOTICE PROCESSING:
1. Identify case (case number, parties)
2. Download/scan the filing
3. Save to case folder with proper naming
4. Read for deadlines and required actions
5. Calendar any new deadlines
6. Notify attorney immediately if:
   - New hearing date set
   - Motion filed against us
   - Order entered
   - Deadline imposed
   - Case reassigned to new judge

SERVICE TRACKING:
- When we serve documents: record date, method (email, mail, hand delivery), who was served
- When we are served: record date received, calculate response deadline
- Keep certificate of service on file for everything

EFILING MONITORING:
- Check for eFiling acceptance/rejection notifications
- If rejected: fix and refile immediately, note any deadline implications`
  },
  {
    section_id: 'a6',
    title: 'Office Coordination & Logistics',
    keywords: 'office,coordinate,schedule,logistics,zoom,deposition,room,travel,supplies,vendor',
    content: `SECRETARY: OFFICE COORDINATION & LOGISTICS
Handle logistics so the attorney handles law.

MEETING COORDINATION:
- Reserve conference room or set up Zoom
- Send calendar invites with all details
- Confirm attendance day before
- Prepare any needed documents or exhibits
- Set up technology (screen sharing, recording) in advance

DEPOSITION LOGISTICS:
- Book court reporter
- Reserve space (our office, OC office, or neutral)
- Send notice of deposition to all parties
- Confirm reporter, space, and all attendees day before
- Prepare exhibit copies

COURT APPEARANCE LOGISTICS:
- Confirm courtroom and time
- Prepare hearing binder/materials
- Check for parking/travel time needs
- If remote: confirm Webex/Zoom link from court

VENDOR MANAGEMENT:
- Court reporters: maintain preferred list with contact info
- Process servers: coordinate service, track status
- Expert witnesses: schedule, coordinate document review
- Translators: arrange when needed for client communications

FILE MAINTENANCE:
- Opening: create folder structure, cover sheet, party_cache entry
- Ongoing: ensure documents are filed correctly, naming conventions followed
- Closing: final billing review, archive documents, close in PracticePanther`
  }
];

// ============================================================
// ATTORNEY SECTIONS (t_ prefix)
// ============================================================
const ATTORNEY_SECTIONS = [
  {
    section_id: 't1',
    title: 'Legal Research Methodology',
    keywords: 'research,case law,statute,authority,precedent,find cases,legal research,westlaw,casetext,search,cite',
    content: `ATTORNEY: LEGAL RESEARCH METHODOLOGY
Research must be thorough, current, and honest. Zero fabrication.

RESEARCH PROCESS:
1. Identify the legal issue precisely (narrow the question)
2. Start with controlling authority (Utah statutes, Utah appellate decisions)
3. Expand to persuasive authority if Utah is silent (10th Circuit, Restatements, other states)
4. Check for recent amendments or overrulings
5. Find adverse authority — you must know what the other side will cite

SOURCE HIERARCHY (Utah):
1. Utah Constitution
2. Utah Code (statutes)
3. Utah Supreme Court decisions
4. Utah Court of Appeals decisions
5. Utah Administrative Code / agency rules
6. 10th Circuit (federal issues)
7. Restatements and treatises
8. Other state courts (persuasive only)

CITATION FORMAT (Bluebook):
- Utah cases: Party v. Party, Year UT volume (e.g., 2024 UT 15)
- Utah Code: Utah Code Ann. section XX-XX-XXX
- Federal: F.3d, F.Supp.3d with circuit/district
- Always pin cite to specific page/paragraph

VERIFICATION:
- Every citation must be real and verifiable
- Check that holdings are accurately stated
- Verify cases have not been overruled or distinguished
- If you cannot verify a citation, do not use it — say you need to confirm
- ZERO tolerance for phantom citations`
  },
  {
    section_id: 't2',
    title: 'Case Strategy & Analysis',
    keywords: 'strategy,analysis,strengths,weaknesses,evaluate,assess,chances,likelihood,pros and cons,risk,outcome',
    content: `ATTORNEY: CASE STRATEGY & ANALYSIS
Objective analysis only. No wishful thinking. Present reality.

FRAMEWORK:
1. FACTS: What can we prove? What evidence exists? What is disputed?
2. LAW: What legal standards apply? What elements must be met?
3. APPLICATION: How do our facts meet (or fail to meet) the legal standards?
4. STRENGTHS: What works in our favor? Strong evidence, favorable law, procedural advantages
5. WEAKNESSES: What hurts us? Bad facts, adverse authority, evidentiary gaps, credibility issues
6. RISKS: What could go wrong? Sanctions, adverse rulings, costs, precedent
7. RECOMMENDATION: Based on honest assessment, what is the best course of action?

OBJECTIVITY RULES:
- Never minimize bad facts. The attorney needs the real picture.
- Present adverse authority. Hiding it does not make it go away.
- Quantify uncertainty: "Strong position" vs "50/50" vs "Uphill battle"
- Distinguish between what we want to be true and what the evidence shows
- Consider the judge. Different judges have different tendencies.

STRATEGIC OPTIONS:
- Always present at least 2 options (usually: aggressive, conservative, middle)
- Include cost/benefit for each option
- Note timing considerations (is delay advantageous or harmful?)
- Consider settlement value vs. trial risk
- Factor in client goals and resources`
  },
  {
    section_id: 't3',
    title: 'Issue Spotting & Risk Assessment',
    keywords: 'issue,risk,problem,flag,concern,procedural,jurisdiction,standing,statute of limitations,preservation,waiver',
    content: `ATTORNEY: ISSUE SPOTTING & RISK ASSESSMENT
See problems before they become crises. Flag everything.

PROCEDURAL ISSUES:
- Jurisdiction: personal and subject matter — verify both
- Standing: does our client have standing to bring/defend this claim?
- Statute of limitations: check every cause of action
- Service of process: was it proper? Can it be challenged?
- Venue: is this the right court?
- Timeliness: were all filings made within required deadlines?

EVIDENTIARY ISSUES:
- Authentication: can we authenticate every exhibit?
- Hearsay: identify potential hearsay issues in our evidence and theirs
- Relevance: will the judge admit it?
- Privilege: have we inadvertently waived anything?
- Expert testimony: do we need experts? Have we disclosed them timely?

PRESERVATION ISSUES:
- Did we send litigation hold letters?
- Is relevant evidence being preserved (texts, emails, social media)?
- ESI (electronically stored information) preservation obligations
- Spoliation risk from either side

ETHICAL ISSUES:
- Conflict of interest: check at case inception AND when new parties emerge
- Candor to tribunal: must disclose adverse authority if directly on point
- Communication with represented parties: only through counsel
- Confidentiality: protect at all times
- Frivolous positions: Rule 11 obligations

WHEN TO FLAG:
- Any issue that could result in dismissal, sanctions, or malpractice
- Anything the attorney may not be aware of
- Changes in law that affect pending cases
- New facts that change the analysis`
  },
  {
    section_id: 't4',
    title: 'Motion Practice & Brief Writing',
    keywords: 'motion,brief,memorandum,argument,standard,burden,summary judgment,dismiss,compel,suppress,creac,writing',
    content: `ATTORNEY: MOTION PRACTICE & BRIEF WRITING
Every motion must be legally sound, factually supported, and persuasive.

MOTION CHECKLIST:
1. Is this the right motion? (MTD vs MSJ vs MIL vs MTC)
2. What is the legal standard? (cite controlling authority)
3. Do we meet the standard? (apply facts to law)
4. What will the opposition argue? (anticipate and address)
5. What relief do we want? (be specific in the prayer)

BRIEF STRUCTURE (CREAC):
- Conclusion: State the result you want up front
- Rule: State the legal standard with citation
- Explanation: Show how courts have applied the rule
- Application: Apply the rule to our facts
- Conclusion: Restate why we win

URCP BRIEFING SCHEDULE:
- Motion + Memorandum filed together
- Opposition: 14 days after service (URCP 7(d))
- Reply: 7 days after opposition (URCP 7(d))
- Total pages: memo max 25 pages, reply max 15 pages (local rules may vary)
- Request for decision after reply period expires if no ruling

COMMON MOTIONS:
- Motion to Dismiss (URCP 12(b)): failure to state claim, jurisdiction, etc.
- Motion for Summary Judgment (URCP 56): no genuine dispute of material fact
- Motion to Compel (URCP 37): discovery enforcement
- Motion in Limine: exclude evidence before trial
- Motion to Continue: request hearing/trial postponement

PERSUASIVE WRITING:
- Lead with your strongest argument
- Use short, declarative sentences
- Cite authority for every legal proposition
- Use facts from the record — never argue facts not in evidence
- Anticipate counterarguments and address them
- Proofread everything — typos undermine credibility`
  },
  {
    section_id: 't5',
    title: 'Utah Law & Court Rules',
    keywords: 'utah,urcp,urcrp,urap,utah code,local rules,court rules,first district,judge,practice,procedure',
    content: `ATTORNEY: UTAH LAW & COURT RULES
Know the rules cold. Procedural missteps lose cases.

KEY RULE SETS:
- URCP (Utah Rules of Civil Procedure): governs civil litigation
- URCrP (Utah Rules of Criminal Procedure): governs criminal cases
- URAP (Utah Rules of Appellate Procedure): governs appeals
- URE (Utah Rules of Evidence): governs admissibility
- Utah RPC (Rules of Professional Conduct): ethical obligations

CRITICAL URCP RULES:
- Rule 4: Service of process
- Rule 6: Time computation (weekends, holidays, +3 for mail)
- Rule 7: Motions and memoranda format and timing
- Rule 12: Defenses and objections (12(b) motions)
- Rule 16: Pretrial conferences and scheduling orders
- Rule 26: Disclosures and discovery scope
- Rule 33/34/36: Interrogatories, RFPs, RFAs
- Rule 37: Discovery sanctions and motions to compel
- Rule 56: Summary judgment
- Rule 59/60: Post-judgment motions

1ST DISTRICT PRACTICE:
- Primary venue for Pitcher Law cases
- Know the judges and their preferences
- Local rules supplement URCP — check before filing
- eFiling required for most documents
- Ex parte motions: check local requirements

APPELLATE:
- Notice of appeal: 30 days from final judgment (URAP 4(a))
- Docketing statement: 21 days after notice
- Briefs: opening 40 days, response 30 days, reply 30 days (URAP 26)
- Standard of review matters: de novo (law), clearly erroneous (facts), abuse of discretion
- Preserve issues at trial or they are waived on appeal`
  },
  {
    section_id: 't6',
    title: 'Ethics & Professional Responsibility',
    keywords: 'ethics,conflict,confidentiality,privilege,upl,professional conduct,malpractice,duty,candor,competence,trust account',
    content: `ATTORNEY: ETHICS & PROFESSIONAL RESPONSIBILITY
Ethics violations end careers. Take this seriously.

CORE DUTIES:
- Competence (RPC 1.1): adequate knowledge and skill for the matter
- Diligence (RPC 1.3): reasonable promptness and attention
- Communication (RPC 1.4): keep clients informed, explain enough for informed decisions
- Confidentiality (RPC 1.6): do not reveal information relating to representation
- Conflicts (RPC 1.7-1.10): identify and resolve conflicts before they become problems

CONFLICT OF INTEREST:
- Check at intake AND whenever new parties or issues arise
- Current client conflicts: generally prohibited unless both consent after full disclosure
- Former client conflicts: prohibited if substantially related matter
- Imputed conflicts: one attorney disqualified = entire firm disqualified (absent screening)
- When in doubt: do not proceed, consult ethics resources

CONFIDENTIALITY:
- Extends to all information relating to representation
- Survives termination of representation
- AI systems (Synthia) bound by same confidentiality obligations
- Never discuss client matters in public or unsecured settings
- Email: use encryption for sensitive communications when possible

CANDOR TO TRIBUNAL (RPC 3.3):
- Must disclose directly adverse authority in the controlling jurisdiction
- Cannot make false statements of fact or law
- Must correct material misstatements
- Duty supersedes confidentiality in some circumstances

MALPRACTICE PREVENTION:
- Calendar all deadlines (the #1 cause of malpractice claims is missed deadlines)
- Document everything
- Supervise all work product
- Maintain competence through CLE
- Carry malpractice insurance
- When in doubt about an ethical question: stop and research before acting`
  },
  {
    section_id: 't7',
    title: 'Evidence & Trial Preparation',
    keywords: 'evidence,exhibit,witness,testimony,hearsay,relevance,foundation,authenticate,trial,trial prep,jury,bench trial',
    content: `ATTORNEY: EVIDENCE & TRIAL PREPARATION
Trial readiness starts months before trial date.

EVIDENCE RULES (URE):
- Relevance (Rules 401-403): must make a material fact more/less probable
- Hearsay (Rules 801-807): out-of-court statement offered for truth — know the exceptions
- Authentication (Rule 901): lay proper foundation for every exhibit
- Best evidence (Rule 1002): originals required for document content
- Privilege (Rule 501+): attorney-client, spousal, etc.

EXHIBIT PREPARATION:
- Number all exhibits sequentially
- Prepare exhibit list with description and foundation witness
- Authenticate each exhibit (who will testify to lay foundation?)
- Pre-mark and organize in binders (judge copy, OC copy, witness copy)
- Identify any exhibits that may face objection — prepare responses

WITNESS PREPARATION:
- Meet with each witness before testimony
- Review relevant documents and anticipated questions
- Prepare direct examination outline
- Anticipate cross-examination questions
- Remind: answer only what is asked, tell the truth, do not guess

TRIAL LOGISTICS:
- Subpoena all non-party witnesses
- Confirm party witnesses will appear voluntarily
- Prepare jury instructions (if jury trial)
- Prepare voir dire questions (if jury trial)
- Draft proposed findings of fact and conclusions of law (if bench trial)
- Prepare opening statement outline
- Prepare closing argument framework

30-DAY TRIAL COUNTDOWN:
- 30 days: finalize witness and exhibit lists
- 21 days: file pretrial disclosures per scheduling order
- 14 days: prepare all exhibits, witness outlines
- 7 days: final witness prep meetings, trial binder complete
- 3 days: review everything, prepare for contingencies
- 1 day: logistics confirmed, materials organized, get rest`
  }
];

async function seedAll() {
  const allSections = [...SECRETARY_SECTIONS, ...ATTORNEY_SECTIONS];
  console.log(`Seeding ${SECRETARY_SECTIONS.length} Secretary + ${ATTORNEY_SECTIONS.length} Attorney sections into D1...\n`);

  const esc = (s) => s.replace(/'/g, "''");
  const sqlStatements = allSections.map(section =>
    `INSERT OR REPLACE INTO procedures (section_id, section_name, category, keywords, content, tier) VALUES ('${esc(section.section_id)}', '${esc(section.title)}', '${section.section_id.startsWith('a') ? 'secretary' : 'attorney'}', '${esc(section.keywords)}', '${esc(section.content)}', 2);`
  );

  const sqlFile = path.join(os.tmpdir(), 'seed-hats.sql');
  fs.writeFileSync(sqlFile, sqlStatements.join('\n'), 'utf-8');
  console.log(`  SQL file written: ${sqlFile} (${sqlStatements.length} statements)`);

  try {
    const result = execSync(
      `npx wrangler d1 execute ${DB_NAME} --remote --file="${sqlFile}"`,
      { cwd: CWD, stdio: ['pipe', 'pipe', 'pipe'], timeout: 30000 }
    );
    console.log(`  Done. ${result.toString().substring(0, 300)}`);
  } catch (err) {
    console.error(`  Error: ${err.stderr?.toString().substring(0, 500) || err.message}`);
  }

  // Verify
  try {
    const verify = execSync(
      `npx wrangler d1 execute ${DB_NAME} --remote --command "SELECT section_id, section_name, category FROM procedures WHERE section_id LIKE 'a%' OR section_id LIKE 't%' ORDER BY section_id;"`,
      { cwd: CWD, stdio: ['pipe', 'pipe', 'pipe'], timeout: 10000 }
    );
    console.log('\nVerification:');
    console.log(verify.toString());
  } catch (err) {
    console.error('Verification failed:', err.message);
  }

  try { fs.unlinkSync(sqlFile); } catch {}
  console.log('Done.');
}

seedAll();
