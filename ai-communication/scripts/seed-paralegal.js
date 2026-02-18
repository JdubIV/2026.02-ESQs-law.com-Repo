/**
 * seed-paralegal.js
 *
 * Seeds paralegal training knowledge into D1 procedures table.
 * Uses p_ prefix for section IDs to distinguish from main procedures (s_).
 * Writes SQL to a temp file to avoid shell escaping issues with multiline content.
 *
 * Run: node scripts/seed-paralegal.js
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

const DB_NAME = 'pitcher-law-memory';
const CWD = path.join(__dirname, '..');

const PARALEGAL_SECTIONS = [
  {
    section_id: 'p1',
    title: 'Case Lifecycle & Litigation Roadmap',
    keywords: 'case lifecycle,litigation roadmap,complaint,answer,discovery,motion practice,trial prep,trial,post-trial,case phase,case status,where is the case',
    content: `PARALEGAL: CASE LIFECYCLE & LITIGATION ROADMAP
Every case follows this path. Know where each case is — that determines what needs doing NOW.

1. COMPLAINT/PETITION PHASE: Case filed. Verify service. Calendar answer deadline (21 days URCP, 30 days if waiver). Set up case file, cover sheet, and party_cache entry.
2. ANSWER/RESPONSIVE PLEADING: Draft answer or responsive motion. Calendar any counterclaim deadlines. Update case status.
3. INITIAL DISCLOSURES: Due within 14 days of first answer (URCP 26(a)). Prepare disclosure statement. Identify known witnesses, documents, damages computation, insurance.
4. DISCOVERY PHASE: Serve/respond to interrogatories, RFPs, RFAs. Track ALL deadlines. Discovery closes per scheduling order — calendar the cutoff and work backward.
5. MOTION PRACTICE: Dispositive motions (MSJ, MTD), discovery motions (MTC), motions in limine. Each has specific briefing schedules and hearing requirements.
6. TRIAL PREP (begins 30+ days before): Witness lists, exhibit lists, jury instructions, trial brief, subpoenas, exhibit binders. Verify all discovery complete.
7. TRIAL: Attend/support attorney. Manage exhibits. Track rulings. Prepare proposed findings if bench trial.
8. POST-TRIAL: Proposed findings/conclusions, judgment, post-trial motions, appeal deadlines (30 days for notice of appeal).

At each phase ask: What is due? What is overdue? What needs to start NOW to be ready on time?`
  },
  {
    section_id: 'p2',
    title: 'Discovery Management',
    keywords: 'discovery,interrogatories,rfp,rfa,request for production,request for admission,deposition,subpoena,discovery deadline,discovery response,propound,serve discovery',
    content: `PARALEGAL: DISCOVERY MANAGEMENT
Discovery is the biggest paralegal workload. Stay ahead or drown.

OUTGOING DISCOVERY (we serve):
- Draft interrogatories, RFPs, RFAs based on case needs
- Serve per URCP (email if stipulated, mail adds 3 days)
- Calendar 28-day response deadline from service date
- If no response: send deficiency letter at day 30, motion to compel at day 35
- Track what was asked and what was actually answered

INCOMING DISCOVERY (served on us):
- Calendar response deadline immediately upon receipt (28 days from service)
- Set reminder at 14 days (draft due) and 21 days (review due)
- Draft responses/objections, get attorney review
- Gather responsive documents, apply privilege review
- Prepare privilege log if withholding anything
- Serve responses before deadline — NEVER late

DEPOSITIONS:
- Schedule with all parties, secure reporter
- Prepare deposition outline for attorney
- Organize exhibits for deposition
- After: obtain transcript, summarize key testimony, update case file

DISCOVERY DISPUTES:
- Meet and confer BEFORE filing any discovery motion (URCP 37(a))
- Document all meet-and-confer attempts
- If motion to compel needed: prepare motion, memorandum, proposed order

TRACKING SYSTEM:
- Every discovery item gets: type, date served, deadline, status (pending/overdue/complete)
- Weekly review: what is due this week? What is overdue? What needs to go out?`
  },
  {
    section_id: 'p3',
    title: 'Hearing Preparation Checklist',
    keywords: 'hearing,hearing prep,pretrial,prepare for hearing,court date,hearing tomorrow,hearing this week,ready for court,exhibit,witness',
    content: `PARALEGAL: HEARING PREPARATION CHECKLIST
Start prep 3-5 business days before. Minimum 1-2 days for review.

PRE-HEARING (3-5 days out):
1. Pull and review entire case file — every filing, every order
2. Check docket for any new filings or minute entries
3. Verify all required documents have been filed (motion, memo, supporting declarations)
4. Check if opposition has filed response or any new motions
5. Confirm hearing date/time/courtroom on court calendar
6. Note judge name and any known preferences

CASE SUMMARY/CHEAT SHEET (prepare for attorney):
- Case caption and number
- Judge name
- Nature of hearing (what is being decided)
- Key facts (bullet format, cite to record)
- Our position and supporting law
- Anticipated opposing arguments and rebuttals
- Relief requested
- Any procedural issues

EXHIBITS:
- Organize in order of anticipated use
- Tab and label each exhibit
- Prepare exhibit list
- Have 3 copies minimum (judge, opposing counsel, witness)
- Verify all exhibits were properly disclosed in discovery

DAY BEFORE:
- Final docket check for any last-minute filings
- Confirm no scheduling changes
- Verify attorney has all materials
- Print/organize hearing binder

RED FLAGS — escalate immediately:
- Missing filings that should have been made
- Undisclosed witnesses or exhibits
- Deadline violations that could affect hearing
- New filings from opposing counsel that change the picture`
  },
  {
    section_id: 'p4',
    title: 'Deadline & Calendar Management',
    keywords: 'deadline,calendar,due date,filing deadline,response deadline,statute of limitations,scheduling order,when is it due,overdue,late',
    content: `PARALEGAL: DEADLINE & CALENDAR MANAGEMENT
Missed deadlines = malpractice. This is non-negotiable.

CALENDAR RULES:
- Every deadline gets calendared THE DAY it is identified
- Calendar the actual deadline AND working-backward reminders:
  * 30 days out: start prep for major deadlines (trial, MSJ)
  * 14 days out: draft should be in progress
  * 7 days out: draft should be in review
  * 3 days out: final review and filing
  * 1 day out: filed or emergency escalation
- Triple-check all date calculations against URCP
- Account for weekends/holidays (URCP 6(a): if deadline falls on weekend/holiday, extends to next business day)
- Service by mail adds 3 days (URCP 6(d))

COMMON UTAH DEADLINES:
- Answer: 21 days after service (URCP 12(a))
- Discovery responses: 28 days after service (URCP 33, 34, 36)
- Motion response: 14 days after service (URCP 7(d))
- Reply memo: 7 days after response (URCP 7(d))
- Notice of appeal: 30 days after final judgment (URAP 4(a))
- Initial disclosures: 14 days after first answer (URCP 26(a)(1))

WEEKLY ROUTINE:
- Monday: review all deadlines for the week across all cases
- Flag anything due this week that is not started
- Flag anything due next week that should be started
- Report status to attorney

OVERDUE ITEMS:
- If something is overdue, escalate IMMEDIATELY — do not wait to be asked
- Determine if stipulation for extension is possible
- If not, assess consequences and prepare any necessary motion for extension`
  },
  {
    section_id: 'p5',
    title: 'Document Management & Filing',
    keywords: 'document,filing,file management,naming convention,organize,folder,case file,case bible,document retention,efiling',
    content: `PARALEGAL: DOCUMENT MANAGEMENT & FILING
Good filing saves cases. Bad filing loses them.

NAMING CONVENTION:
- Format: YYYY-MM-DD_[DocType]_[Description]_[Party]
- Example: 2026-02-18_Motion_CompelDiscovery_Respondent
- No spaces in filenames — use underscores
- Consistent across all cases

FOLDER STRUCTURE (per case):
/[ClientName]_[CaseNumber]/
  /01_Pleadings/        (complaints, answers, counterclaims)
  /02_Discovery/        (interrogatories, RFPs, responses)
  /03_Motions/          (all motions filed and received)
  /04_Orders/           (court orders, minute entries)
  /05_Correspondence/   (letters, emails archived)
  /06_Evidence/         (exhibits, photos, records)
  /07_Research/         (memos, case law, statutes)
  /08_Billing/          (invoices, time entries)
  /09_Notes/            (case notes, strategy memos)
  /10_Trial/            (trial prep materials)

EFILING:
- Utah courts use OCAP/eFiling system
- Verify PDF formatting before filing (no scanned images if possible)
- Confirm filing acceptance (check for rejection notices)
- Download and save filed-stamped copies immediately

VERSION CONTROL:
- Drafts: append _v1, _v2, _v3
- Final versions: no version suffix
- Never overwrite a prior version — keep the history
- Track who reviewed and when`
  },
  {
    section_id: 'p6',
    title: 'Client Intake & New Case Setup',
    keywords: 'new client,intake,onboard,open case,new case,client intake,new matter,case setup,cover sheet,conflict check',
    content: `PARALEGAL: CLIENT INTAKE & NEW CASE SETUP
First impression matters. Get it right from the start.

INTAKE CHECKLIST:
1. Conflict check — run client name AND opposing party against all current/past clients
2. Collect: full legal name, DOB, address, phone, email, SSN (if criminal/family), employer
3. Identify: case type, opposing party, court (if filed), case number (if filed)
4. Fee agreement signed and filed
5. Retainer received and deposited to trust account
6. Client ID verification

CASE SETUP:
1. Create case folder (use standard folder structure)
2. Generate cover sheet / facesheet
3. Add to party_cache (client info, case number, court, judge, opposing counsel)
4. Calendar any known deadlines (answer date, hearing dates, statute of limitations)
5. Set up billing matter in PracticePanther
6. Send welcome letter/email to client
7. Request relevant documents from client (police reports, prior orders, financial records)
8. If case already filed: pull docket from JudicialLink, download all filings

CONFLICT CHECK IS MANDATORY:
- Check ALL parties, not just client name
- Check aliases, maiden names, business names
- If conflict found: stop intake, notify attorney immediately
- Document the conflict check (who was checked, when, result)`
  },
  {
    section_id: 'p7',
    title: 'Billing & Time Tracking',
    keywords: 'billing,time entry,timecard,invoice,hours,billable,non-billable,retainer,trust account,iolta,rate',
    content: `PARALEGAL: BILLING & TIME TRACKING
Track every minute. Bill accurately. Protect trust accounts.

TIME ENTRY RULES:
- Minimum increment: 0.1 hour (6 minutes)
- Capture: date, client, case, task description, time, billable/non-billable
- Descriptions must be specific: "Reviewed and analyzed discovery responses (RFP Set 1)" not "Worked on case"
- Log time same-day — do not reconstruct from memory

BILLABLE vs. NON-BILLABLE:
- Billable: client-specific work (research, drafting, court prep, communications about case)
- Non-billable: general admin, CLE, firm meetings, personal
- When in doubt: if it advances a specific client case, it is billable

ATTORNEY RATE: JWA3 at $390/hr
- All time entries default to JWA3 unless otherwise specified

TRUST ACCOUNTS (IOLTA):
- NEVER commingle client funds with operating funds
- Every deposit and withdrawal must be documented
- Client ledger must balance at all times
- Retainer draws require itemized billing statement first
- Utah Rule of Professional Conduct 1.15 governs trust accounts

INVOICING:
- Monthly billing cycle
- Include detailed time entries with descriptions
- Show trust account balance and any draws
- Flag any accounts approaching retainer depletion`
  },
  {
    section_id: 'p8',
    title: 'Communication & Professional Standards',
    keywords: 'communication,email,letter,opposing counsel,court clerk,client communication,professional,tone,follow up',
    content: `PARALEGAL: COMMUNICATION & PROFESSIONAL STANDARDS
Every communication reflects on the firm. Be professional, precise, documented.

WITH COURTS:
- Always respectful and formal
- Know the clerk name and preferred contact method
- Follow local rules for formatting and filing
- When in doubt, call the clerk — they are usually helpful
- Never argue with court staff

WITH OPPOSING COUNSEL:
- Professional, never personal
- Document everything in writing (follow up calls with confirming emails)
- Meet and confer in good faith before filing discovery motions
- Cc the attorney on all substantive communications
- Never make admissions or concessions without attorney approval

WITH CLIENTS:
- Respond within 24 hours (even if just to acknowledge receipt)
- Plain language — avoid unnecessary legal jargon
- Manage expectations — do not promise outcomes
- Document all advice given (in case notes)
- Never give legal advice — "The attorney will need to review this" for legal questions
- Paralegals/AI cannot give legal advice (UPL risk)

DOCUMENTATION:
- Every phone call: note in case file (date, time, who, what discussed)
- Every email: archived to case folder
- Every decision: documented with reasoning
- If it is not written down, it did not happen

FOLLOW-UP:
- If you are waiting on something, set a reminder
- If no response in 48 hours, follow up
- Track all open items per case`
  },
  {
    section_id: 'p9',
    title: 'Practice Area: Criminal Defense',
    keywords: 'criminal,felony,misdemeanor,plea,arraignment,sentencing,probation,expungement,bail,preliminary hearing,criminal defense',
    content: `PARALEGAL: CRIMINAL DEFENSE PROCEDURES
Criminal cases have strict constitutional timelines. Miss nothing.

CASE FLOW:
1. Arrest/Charge
2. Bail/Release
3. Initial Appearance
4. Preliminary Hearing (felony)
5. Arraignment
6. Discovery
7. Pretrial
8. Trial
9. Sentencing
10. Appeal

KEY DEADLINES:
- Initial appearance: within 48 hours of arrest (Rule 7, URCrP)
- Preliminary hearing: within 10 days if detained, 30 days if released (Rule 7(h))
- Speedy trial: defendant must demand; 120 days from arraignment if invoked

PARALEGAL TASKS:
- Obtain police reports, body cam footage, witness statements
- Calendar ALL court dates immediately
- Track discovery from prosecution (Brady material)
- Organize evidence by count/element of offense
- Prepare sentencing memorandum materials (character letters, treatment records, employment)
- Expungement: verify eligibility timeline, prepare petition, obtain BCI records

CRITICAL RULES:
- Attorney-client privilege is absolute in criminal cases
- Never discuss case facts with anyone outside the defense team
- Victim contact only through proper channels
- Body cam and evidence preservation requests must be timely`
  },
  {
    section_id: 'p10',
    title: 'Practice Area: Family Law',
    keywords: 'family,divorce,custody,parent time,alimony,child support,visitation,protective order,cohabitant,domestic,modification,decree',
    content: `PARALEGAL: FAMILY LAW PROCEDURES
High emotion, strict rules, children interests paramount.

CASE TYPES:
- Divorce (contested/uncontested)
- Custody/parent-time modification
- Protective orders (cohabitant abuse)
- Child support modification
- Paternity
- Adoption

KEY REQUIREMENTS:
- Financial declarations MANDATORY in all divorce/support cases
- 90-day waiting period for divorce (Utah Code 30-3-18)
- Custody evaluations: may be court-ordered
- Parent-time: URCP 26.1 requires mandatory disclosure of parenting plan

PARALEGAL TASKS:
- Prepare financial declarations (income, expenses, assets, debts)
- Gather 3 years of financial records (tax returns, pay stubs, bank statements)
- Draft proposed parenting plan
- Calculate child support using Utah child support calculator
- Organize custody evidence (school records, medical records, communications)
- Track temporary orders compliance
- Prepare decree/order based on settlement or court ruling

PROTECTIVE ORDERS:
- Ex parte petition can be filed without notice to respondent
- Hearing within 20 days of ex parte order
- Prepare client for hearing (what to expect, what evidence to bring)
- Safety planning with client`
  },
  {
    section_id: 'p11',
    title: 'Proactive Case Monitoring & Systems Thinking',
    keywords: 'proactive,case monitoring,systems,workflow,checklist,status,review,flag,bottleneck,process,downstream',
    content: `PARALEGAL: PROACTIVE CASE MONITORING & SYSTEMS THINKING
Do not wait to be told. Own the cases.

DAILY:
- Check for new court filings/minute entries across all active cases
- Check email for time-sensitive items
- Review today and tomorrow calendar
- Flag anything requiring immediate attention

WEEKLY:
- Review ALL active case deadlines for the next 2 weeks
- Check for overdue items across all cases
- Status update on any case in active litigation phase
- Discovery tracking review: what is outstanding?

MONTHLY:
- Full case status review: where is each case in the lifecycle?
- Identify stalled cases — anything without activity for 30+ days
- Review billing: any unbilled time? Any trust accounts running low?
- Clean up: close completed matters, archive old files

SYSTEMS THINKING:
- Every action has downstream effects — think about who uses your work product next
- Standardize everything: if you do it twice, make it a checklist
- When something goes wrong, fix the system, not just the symptom
- Build for handoff: if you were gone tomorrow, could someone pick up your cases?
- Efficiency compounds: small improvements in process save hours over time

ESCALATION:
- Missed deadline: immediate attorney notification
- New filing that changes case posture: immediate flag
- Client non-responsive on critical items: escalate at 48 hours
- Opposing counsel bad faith: document and flag for motion`
  }
];

async function seedParalegal() {
  console.log('Seeding paralegal knowledge into D1...\n');

  // Build a single SQL file with all inserts
  const sqlStatements = PARALEGAL_SECTIONS.map(section => {
    const esc = (s) => s.replace(/'/g, "''");
    return `INSERT OR REPLACE INTO procedures (section_id, section_name, category, keywords, content, tier) VALUES ('${esc(section.section_id)}', '${esc(section.title)}', 'paralegal', '${esc(section.keywords)}', '${esc(section.content)}', 2);`;
  });

  const sqlFile = path.join(os.tmpdir(), 'seed-paralegal.sql');
  fs.writeFileSync(sqlFile, sqlStatements.join('\n'), 'utf-8');
  console.log(`  SQL file written: ${sqlFile} (${sqlStatements.length} statements)`);

  try {
    const result = execSync(
      `npx wrangler d1 execute ${DB_NAME} --remote --file="${sqlFile}"`,
      { cwd: CWD, stdio: ['pipe', 'pipe', 'pipe'], timeout: 30000 }
    );
    console.log(`\n  Done. Output: ${result.toString().substring(0, 500)}`);
  } catch (err) {
    console.error(`  Error: ${err.stderr?.toString() || err.message}`);
  }

  // Verify
  try {
    const verify = execSync(
      `npx wrangler d1 execute ${DB_NAME} --remote --command "SELECT section_id, section_name FROM procedures WHERE section_id LIKE 'p%' ORDER BY section_id;"`,
      { cwd: CWD, stdio: ['pipe', 'pipe', 'pipe'], timeout: 10000 }
    );
    console.log('\nVerification:');
    console.log(verify.toString());
  } catch (err) {
    console.error('Verification failed:', err.message);
  }

  // Cleanup
  try { fs.unlinkSync(sqlFile); } catch {}
  console.log('\nDone.');
}

seedParalegal();
