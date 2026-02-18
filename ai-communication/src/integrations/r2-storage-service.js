/**
 * R2 STORAGE SERVICE
 * Replaces OneDrive — file storage backed by Cloudflare R2
 * Drop-in replacement: same method signatures as OneDriveSimpleService
 */

const { getR2Client, getD1Client } = require('../lib/cloudflare-clients');
const fs = require('fs');
const path = require('path');

class R2StorageService {
  constructor(config = {}) {
    this.accountId = config.accountId || process.env.CLOUDFLARE_ACCOUNT_ID;
    this.apiToken = config.apiToken || process.env.CLOUDFLARE_API_TOKEN;
    this.bucketName = config.bucketName || process.env.R2_BUCKET_NAME || 'esqs-documents';
    this.dbId = config.dbId || process.env.CLOUDFLARE_D1_ID || '33dfce44-67dc-4df3-a2af-f821590bf80b';
    this.basePath = config.basePath || 'Open Cases';

    // Local sync path for case files
    this.localSyncPath = config.localSyncPath || process.env.CASE_FILES_PATH ||
      'F:/OneDrive - DianePitcher/Office/Open Cases';
  }

  async ensureTable() {
    const db = getD1Client(this.accountId, this.dbId, this.apiToken);
    await db.exec(`
      CREATE TABLE IF NOT EXISTS files (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        r2_key TEXT NOT NULL,
        folder TEXT DEFAULT '/',
        size INTEGER DEFAULT 0,
        mime_type TEXT,
        modified_at TEXT DEFAULT (datetime('now')),
        is_folder INTEGER DEFAULT 0,
        parent_folder TEXT DEFAULT '/'
      );
      CREATE INDEX IF NOT EXISTS idx_files_folder ON files(folder);
      CREATE INDEX IF NOT EXISTS idx_files_name ON files(name);
    `);
    return db;
  }

  async listFiles(folderPath) {
    // First try local filesystem (for case files that are on the machine)
    const localDir = path.join(this.localSyncPath, folderPath || '');
    if (fs.existsSync(localDir)) {
      try {
        const entries = fs.readdirSync(localDir, { withFileTypes: true });
        return entries.map(entry => ({
          id: `local-${Buffer.from(path.join(localDir, entry.name)).toString('base64url')}`,
          name: entry.name,
          path: path.join(localDir, entry.name),
          size: entry.isFile() ? fs.statSync(path.join(localDir, entry.name)).size : 0,
          modified: entry.isFile() ? fs.statSync(path.join(localDir, entry.name)).mtime.toISOString() : null,
          isFolder: entry.isDirectory(),
          source: 'local'
        }));
      } catch (_e) { /* fall through to R2 */ }
    }

    // Fallback: query D1 index for R2 files
    const db = await this.ensureTable();
    const folder = folderPath || '/';
    const result = await db.prepare('SELECT * FROM files WHERE parent_folder = ? ORDER BY is_folder DESC, name ASC')
      .bind(folder).all();

    return result.results.map(f => ({
      id: f.id,
      name: f.name,
      path: f.r2_key,
      size: f.size,
      modified: f.modified_at,
      isFolder: Boolean(f.is_folder),
      source: 'r2'
    }));
  }

  async getClientFiles(clientName) {
    // Local first
    const clientDir = path.join(this.localSyncPath, clientName);
    if (fs.existsSync(clientDir)) {
      try {
        const entries = fs.readdirSync(clientDir, { withFileTypes: true });
        return entries.map(entry => ({
          id: `local-${Buffer.from(path.join(clientDir, entry.name)).toString('base64url')}`,
          name: entry.name,
          path: path.join(clientDir, entry.name),
          size: entry.isFile() ? fs.statSync(path.join(clientDir, entry.name)).size : 0,
          modified: entry.isFile() ? fs.statSync(path.join(clientDir, entry.name)).mtime.toISOString() : null,
          isFolder: entry.isDirectory(),
          source: 'local'
        }));
      } catch (_e) { /* fall through */ }
    }

    // R2 fallback
    const db = await this.ensureTable();
    const result = await db.prepare("SELECT * FROM files WHERE folder LIKE ? ORDER BY name ASC")
      .bind(`%${clientName}%`).all();
    return result.results.map(f => ({
      id: f.id, name: f.name, path: f.r2_key, size: f.size, modified: f.modified_at,
      isFolder: Boolean(f.is_folder), source: 'r2'
    }));
  }

  async downloadFile(fileId) {
    // Local file
    if (fileId.startsWith('local-')) {
      const filePath = Buffer.from(fileId.replace('local-', ''), 'base64url').toString('utf-8');
      return fs.readFileSync(filePath);
    }

    // R2 file
    const db = await this.ensureTable();
    const file = await db.prepare('SELECT r2_key FROM files WHERE id = ?').bind(fileId).first();
    if (!file) throw new Error('File not found');

    const r2 = getR2Client(this.accountId, this.bucketName, this.apiToken);
    return r2.get(file.r2_key);
  }

  async uploadFile(filePath, content, contentType) {
    const db = await this.ensureTable();
    const r2 = getR2Client(this.accountId, this.bucketName, this.apiToken);

    const r2Key = `${this.basePath}${filePath}`;
    const id = `r2-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const name = path.basename(filePath);
    const folder = path.dirname(filePath);

    await r2.put(r2Key, content, contentType);
    await db.prepare('INSERT OR REPLACE INTO files (id, name, r2_key, folder, size, mime_type, parent_folder) VALUES (?, ?, ?, ?, ?, ?, ?)')
      .bind(id, name, r2Key, filePath, content.length, contentType, folder).run();

    return { success: true, path: r2Key, id };
  }

  // Email and calendar stubs removed — use dedicated services
}

module.exports = R2StorageService;
