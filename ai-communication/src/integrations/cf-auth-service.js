/**
 * CLOUDFLARE AUTH SERVICE
 * Replaces Google OAuth + Azure AD — JWT-based auth with Cloudflare KV sessions
 * Drop-in replacement for auth flows
 */

const crypto = require('crypto');

class CloudflareAuthService {
  constructor(config = {}) {
    this.secret = config.secret || process.env.AUTH_SECRET || 'change-me-in-production';
    this.kvUrl = config.kvUrl || null; // Set by Worker env
    this.accountId = config.accountId || process.env.CLOUDFLARE_ACCOUNT_ID;
    this.apiToken = config.apiToken || process.env.CLOUDFLARE_API_TOKEN;
    this.kvNamespaceId = config.kvNamespaceId || process.env.CLOUDFLARE_KV_SESSIONS_ID || 'eaae2faf1d454416801304b92eb3cb34';
    this.tokenExpiry = config.tokenExpiry || 86400; // 24 hours
  }

  /**
   * Generate a session token (replaces OAuth token exchange)
   */
  generateToken(userId, email) {
    const payload = {
      sub: userId,
      email,
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + this.tokenExpiry,
      jti: crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(36).slice(2)}`
    };
    const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
    const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
    const sig = crypto.createHmac('sha256', this.secret).update(`${header}.${body}`).digest('base64url');
    return `${header}.${body}.${sig}`;
  }

  /**
   * Verify a session token
   */
  verifyToken(token) {
    try {
      const [header, body, sig] = token.split('.');
      const expectedSig = crypto.createHmac('sha256', this.secret).update(`${header}.${body}`).digest('base64url');
      if (sig !== expectedSig) return null;

      const payload = JSON.parse(Buffer.from(body, 'base64url').toString());
      if (payload.exp < Math.floor(Date.now() / 1000)) return null;
      return payload;
    } catch (_e) {
      return null;
    }
  }

  /**
   * Store session in Cloudflare KV (via API for Node.js, native in Worker)
   */
  async createSession(token, sessionData) {
    try {
      const url = `https://api.cloudflare.com/client/v4/accounts/${this.accountId}/storage/kv/namespaces/${this.kvNamespaceId}/values/${token}`;
      const response = await fetch(url, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${this.apiToken}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ...sessionData,
          createdAt: new Date().toISOString()
        })
      });
      return response.ok;
    } catch (error) {
      console.error('Session create error:', error.message);
      return false;
    }
  }

  /**
   * Validate session from KV
   */
  async getSession(token) {
    const payload = this.verifyToken(token);
    if (!payload) return null;

    try {
      const url = `https://api.cloudflare.com/client/v4/accounts/${this.accountId}/storage/kv/namespaces/${this.kvNamespaceId}/values/${token}`;
      const response = await fetch(url, {
        headers: { 'Authorization': `Bearer ${this.apiToken}` }
      });
      if (!response.ok) return payload; // Token valid but no KV session — still OK
      const session = await response.json();
      return { ...payload, ...session };
    } catch (_e) {
      return payload;
    }
  }

  /**
   * Destroy session
   */
  async destroySession(token) {
    try {
      const url = `https://api.cloudflare.com/client/v4/accounts/${this.accountId}/storage/kv/namespaces/${this.kvNamespaceId}/values/${token}`;
      await fetch(url, {
        method: 'DELETE',
        headers: { 'Authorization': `Bearer ${this.apiToken}` }
      });
      return true;
    } catch (_e) {
      return false;
    }
  }

  /**
   * Express middleware: authenticate requests
   */
  middleware() {
    return async (req, res, next) => {
      const authHeader = req.headers.authorization;
      if (!authHeader?.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Authorization required' });
      }
      const token = authHeader.slice(7);
      const session = await this.getSession(token);
      if (!session) {
        return res.status(401).json({ error: 'Invalid or expired token' });
      }
      req.user = session;
      req.token = token;
      next();
    };
  }

  // Compat stubs for code that checks isConfigured/hasToken
  isConfigured() { return true; }
  hasToken() { return true; }
}

module.exports = CloudflareAuthService;
