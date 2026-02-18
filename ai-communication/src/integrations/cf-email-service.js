/**
 * CLOUDFLARE EMAIL SERVICE
 * Replaces Gmail API — sends email via Cloudflare Email Workers (MailChannels)
 * For receiving: uses IMAP (already in deps) or Cloudflare Email Routing
 * Drop-in replacement: same method signatures as GmailService
 */

const ImapSimple = require('imap-simple');
const { simpleParser } = require('mailparser');

class CloudflareEmailService {
  constructor(config = {}) {
    this.workerUrl = config.workerUrl || process.env.EMAIL_WORKER_URL || 'https://api.esqs-law.com/email/send';
    this.fromEmail = config.fromEmail || process.env.EMAIL_FROM || 'esqslaw@gmail.com';
    this.fromName = config.fromName || process.env.EMAIL_FROM_NAME || 'ESQs Law';
    this.apiToken = config.apiToken || process.env.CLOUDFLARE_API_TOKEN;

    // IMAP config for reading (keeps existing mailbox access)
    this.imapConfig = {
      imap: {
        user: config.imapUser || process.env.IMAP_USER || process.env.EMAIL_USER,
        password: config.imapPassword || process.env.IMAP_PASSWORD || process.env.EMAIL_PASSWORD,
        host: config.imapHost || process.env.IMAP_HOST || 'imap.gmail.com',
        port: config.imapPort || parseInt(process.env.IMAP_PORT) || 993,
        tls: true,
        authTimeout: 10000
      }
    };
  }

  async sendEmail({ to, subject, body, cc = null, bcc = null }) {
    try {
      const payload = {
        from: { email: this.fromEmail, name: this.fromName },
        to: Array.isArray(to) ? to.map(e => ({ email: e })) : [{ email: to }],
        subject,
        html: body
      };
      if (cc) payload.cc = Array.isArray(cc) ? cc.map(e => ({ email: e })) : [{ email: cc }];
      if (bcc) payload.bcc = Array.isArray(bcc) ? bcc.map(e => ({ email: e })) : [{ email: bcc }];

      const response = await fetch(this.workerUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.apiToken}`
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errText = await response.text();
        throw new Error(`Email send failed: ${response.status} ${errText}`);
      }

      const result = await response.json();
      console.log(`✅ Email sent: ${subject} → ${to}`);
      return { success: true, messageId: result.messageId || `cf-${Date.now()}` };
    } catch (error) {
      console.error('❌ Error sending email:', error.message);
      return { success: false, error: error.message };
    }
  }

  async getInboxMessages(maxResults = 10, query = '') {
    try {
      if (!this.imapConfig.imap.user || !this.imapConfig.imap.password) {
        return { success: false, error: 'IMAP not configured' };
      }

      const connection = await ImapSimple.connect(this.imapConfig);
      await connection.openBox('INBOX');

      const searchCriteria = query ? [['TEXT', query]] : ['ALL'];
      const fetchOptions = { bodies: '', struct: true, markSeen: false };
      const messages = await connection.search(searchCriteria, fetchOptions);

      const parsed = [];
      const subset = messages.slice(-maxResults).reverse();
      for (const msg of subset) {
        const rawBody = msg.parts.find(p => p.which === '')?.body || '';
        const mail = await simpleParser(rawBody);
        parsed.push({
          id: msg.attributes.uid.toString(),
          threadId: null,
          from: mail.from?.text || '',
          to: mail.to?.text || '',
          subject: mail.subject || '',
          date: mail.date?.toISOString() || '',
          body: mail.html || mail.text || '',
          snippet: (mail.text || '').slice(0, 200),
          labels: msg.attributes.flags || [],
          isUnread: !(msg.attributes.flags || []).includes('\\Seen')
        });
      }

      connection.end();
      return { success: true, messages: parsed, resultSizeEstimate: parsed.length };
    } catch (error) {
      console.error('❌ Error getting inbox:', error.message);
      return { success: false, error: error.message };
    }
  }

  async searchEmails(query, maxResults = 20) {
    return this.getInboxMessages(maxResults, query);
  }

  async getUnreadCount() {
    try {
      if (!this.imapConfig.imap.user) return { success: false, error: 'IMAP not configured' };
      const connection = await ImapSimple.connect(this.imapConfig);
      await connection.openBox('INBOX');
      const messages = await connection.search(['UNSEEN'], { bodies: '' });
      connection.end();
      return { success: true, unreadCount: messages.length };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async sendClientEmail(clientName, clientEmail, subject, body) {
    return this.sendEmail({
      to: clientEmail,
      subject: `[${clientName}] ${subject}`,
      body
    });
  }

  async getClientEmails(clientIdentifier, maxResults = 20) {
    return this.searchEmails(clientIdentifier, maxResults);
  }
}

module.exports = CloudflareEmailService;
