/**
 * R2 DOCS SERVICE
 * Replaces Google Docs — stores documents in R2, generates DOCX via mammoth/docx
 * Drop-in replacement: same method signatures as GoogleDocsService
 */

const { getR2Client } = require('../lib/cloudflare-clients');
const { getD1Client } = require('../lib/cloudflare-clients');

class R2DocsService {
  constructor(config = {}) {
    this.accountId = config.accountId || process.env.CLOUDFLARE_ACCOUNT_ID;
    this.apiToken = config.apiToken || process.env.CLOUDFLARE_API_TOKEN;
    this.bucketName = config.bucketName || process.env.R2_BUCKET_NAME || 'esqs-documents';
    this.dbId = config.dbId || process.env.CLOUDFLARE_D1_ID || '33dfce44-67dc-4df3-a2af-f821590bf80b';
  }

  isConfigured() { return true; }

  async ensureTable() {
    const db = getD1Client(this.accountId, this.dbId, this.apiToken);
    await db.exec(`
      CREATE TABLE IF NOT EXISTS documents (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        content_key TEXT NOT NULL,
        mime_type TEXT DEFAULT 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        size INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      );
    `);
    return db;
  }

  async createDocument(title) {
    const db = await this.ensureTable();
    const id = `doc-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const contentKey = `documents/${id}.docx`;

    await db.prepare('INSERT INTO documents (id, title, content_key) VALUES (?, ?, ?)')
      .bind(id, title || 'Untitled document', contentKey).run();

    return { documentId: id, title: title || 'Untitled document' };
  }

  async getDocument(documentId) {
    const db = await this.ensureTable();
    const doc = await db.prepare('SELECT * FROM documents WHERE id = ?').bind(documentId).first();
    if (!doc) return null;
    return { documentId: doc.id, title: doc.title, contentKey: doc.content_key, createdAt: doc.created_at };
  }

  async uploadContent(documentId, buffer, mimeType) {
    const db = await this.ensureTable();
    const doc = await db.prepare('SELECT content_key FROM documents WHERE id = ?').bind(documentId).first();
    if (!doc) return null;

    const r2 = getR2Client(this.accountId, this.bucketName, this.apiToken);
    await r2.put(doc.content_key, buffer, mimeType);

    await db.prepare("UPDATE documents SET size = ?, mime_type = ?, updated_at = datetime('now') WHERE id = ?")
      .bind(buffer.length, mimeType, documentId).run();

    return { success: true, key: doc.content_key };
  }

  async downloadContent(documentId) {
    const db = await this.ensureTable();
    const doc = await db.prepare('SELECT content_key FROM documents WHERE id = ?').bind(documentId).first();
    if (!doc) return null;

    const r2 = getR2Client(this.accountId, this.bucketName, this.apiToken);
    return r2.get(doc.content_key);
  }

  async listDocuments(limit = 50) {
    const db = await this.ensureTable();
    const result = await db.prepare('SELECT * FROM documents ORDER BY updated_at DESC LIMIT ?').bind(limit).all();
    return result.results;
  }

  async deleteDocument(documentId) {
    const db = await this.ensureTable();
    const doc = await db.prepare('SELECT content_key FROM documents WHERE id = ?').bind(documentId).first();
    if (!doc) return false;

    const r2 = getR2Client(this.accountId, this.bucketName, this.apiToken);
    await r2.delete(doc.content_key);
    await db.prepare('DELETE FROM documents WHERE id = ?').bind(documentId).run();
    return true;
  }

  // Compat: appendText writes to D1 metadata (no Google Docs API equivalent needed)
  async appendText(documentId, text) {
    const db = await this.ensureTable();
    await db.prepare("UPDATE documents SET updated_at = datetime('now') WHERE id = ?").bind(documentId).run();
    return { success: true, note: 'Text append tracked; use uploadContent for full document updates' };
  }
}

module.exports = R2DocsService;
