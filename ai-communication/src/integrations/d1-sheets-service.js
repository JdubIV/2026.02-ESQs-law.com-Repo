/**
 * D1 SHEETS SERVICE
 * Replaces Google Sheets — stores case summaries and tabular data in D1
 * Drop-in replacement: same method signatures as GoogleSheetsService
 */

const { getD1Client } = require('../lib/cloudflare-clients');

class D1SheetsService {
  constructor(config = {}) {
    this.db = config.db || null;
    this.accountId = config.accountId || process.env.CLOUDFLARE_ACCOUNT_ID;
    this.dbId = config.dbId || process.env.CLOUDFLARE_D1_ID || '33dfce44-67dc-4df3-a2af-f821590bf80b';
    this.apiToken = config.apiToken || process.env.CLOUDFLARE_API_TOKEN;
  }

  async getDb() {
    if (this.db) return this.db;
    this.db = getD1Client(this.accountId, this.dbId, this.apiToken);
    return this.db;
  }

  async ensureTables() {
    const db = await this.getDb();
    await db.exec(`
      CREATE TABLE IF NOT EXISTS spreadsheets (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS sheets (
        id TEXT PRIMARY KEY,
        spreadsheet_id TEXT NOT NULL,
        title TEXT NOT NULL,
        FOREIGN KEY (spreadsheet_id) REFERENCES spreadsheets(id)
      );
      CREATE TABLE IF NOT EXISTS sheet_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sheet_id TEXT NOT NULL,
        row_index INTEGER NOT NULL,
        data TEXT NOT NULL,
        FOREIGN KEY (sheet_id) REFERENCES sheets(id)
      );
      CREATE INDEX IF NOT EXISTS idx_rows_sheet ON sheet_rows(sheet_id, row_index);
    `);
  }

  async createCaseSummarySpreadsheet(caseSummary, clientName, practiceArea) {
    try {
      await this.ensureTables();
      const db = await this.getDb();
      const spreadsheetId = crypto.randomUUID ? crypto.randomUUID() : `ss-${Date.now()}`;
      const sheetId1 = `${spreadsheetId}-info`;
      const sheetId2 = `${spreadsheetId}-history`;

      await db.exec(`
        INSERT INTO spreadsheets (id, title) VALUES ('${spreadsheetId}', '${clientName} - Case Summary');
        INSERT INTO sheets (id, spreadsheet_id, title) VALUES ('${sheetId1}', '${spreadsheetId}', 'Case Information');
        INSERT INTO sheets (id, spreadsheet_id, title) VALUES ('${sheetId2}', '${spreadsheetId}', 'Client History');
      `);

      // Populate case info
      const rows = [
        ['Field', 'Value', 'Sources', 'Last Updated'],
        ['Client Name', clientName, '', ''],
        ['Practice Area', practiceArea, '', ''],
        ['Generated', caseSummary.metadata?.generated || new Date().toISOString(), '', '']
      ];
      Object.entries(caseSummary.fields || {}).forEach(([fieldName, fieldData]) => {
        rows.push([
          fieldName.replace(/_/g, ' '),
          fieldData.value || '',
          Array.isArray(fieldData.sources) ? fieldData.sources.join('; ') : (fieldData.sources || ''),
          fieldData.lastUpdated || ''
        ]);
      });

      for (let i = 0; i < rows.length; i++) {
        await db.prepare('INSERT INTO sheet_rows (sheet_id, row_index, data) VALUES (?, ?, ?)')
          .bind(sheetId1, i, JSON.stringify(rows[i])).run();
      }

      // Populate history
      const historyRows = [['Date', 'Action', 'To Do', 'Initials']];
      (caseSummary.clientHistory || []).forEach(entry => {
        historyRows.push([entry.date || '', entry.action || '', entry.todo || '', entry.initials || '']);
      });
      for (let i = 0; i < historyRows.length; i++) {
        await db.prepare('INSERT INTO sheet_rows (sheet_id, row_index, data) VALUES (?, ?, ?)')
          .bind(sheetId2, i, JSON.stringify(historyRows[i])).run();
      }

      console.log(`✅ Created D1 spreadsheet: ${spreadsheetId}`);
      return { spreadsheetId, spreadsheetUrl: null, success: true };
    } catch (error) {
      console.error('❌ Error creating D1 spreadsheet:', error.message);
      return null;
    }
  }

  async createSpreadsheet(title, sheetTitle = 'Sheet1') {
    try {
      await this.ensureTables();
      const db = await this.getDb();
      const spreadsheetId = `ss-${Date.now()}`;
      const sheetId = `${spreadsheetId}-0`;

      await db.prepare('INSERT INTO spreadsheets (id, title) VALUES (?, ?)').bind(spreadsheetId, title).run();
      await db.prepare('INSERT INTO sheets (id, spreadsheet_id, title) VALUES (?, ?, ?)').bind(sheetId, spreadsheetId, sheetTitle).run();

      return { success: true, spreadsheetId, spreadsheetUrl: null };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async readSpreadsheet(spreadsheetId, _range = 'A1:Z1000') {
    try {
      const db = await this.getDb();
      const sheet = await db.prepare('SELECT id FROM sheets WHERE spreadsheet_id = ? LIMIT 1').bind(spreadsheetId).first();
      if (!sheet) return { success: false, error: 'Spreadsheet not found' };

      const rows = await db.prepare('SELECT data FROM sheet_rows WHERE sheet_id = ? ORDER BY row_index').bind(sheet.id).all();
      const values = rows.results.map(r => JSON.parse(r.data));
      return { success: true, values };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async writeSpreadsheet(spreadsheetId, _range, values) {
    try {
      const db = await this.getDb();
      const sheet = await db.prepare('SELECT id FROM sheets WHERE spreadsheet_id = ? LIMIT 1').bind(spreadsheetId).first();
      if (!sheet) return { success: false, error: 'Spreadsheet not found' };

      await db.prepare('DELETE FROM sheet_rows WHERE sheet_id = ?').bind(sheet.id).run();
      for (let i = 0; i < values.length; i++) {
        await db.prepare('INSERT INTO sheet_rows (sheet_id, row_index, data) VALUES (?, ?, ?)')
          .bind(sheet.id, i, JSON.stringify(values[i])).run();
      }
      return { success: true };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  async appendToSpreadsheet(spreadsheetId, _range, values) {
    try {
      const db = await this.getDb();
      const sheet = await db.prepare('SELECT id FROM sheets WHERE spreadsheet_id = ? LIMIT 1').bind(spreadsheetId).first();
      if (!sheet) return { success: false, error: 'Spreadsheet not found' };

      const maxRow = await db.prepare('SELECT MAX(row_index) as max_idx FROM sheet_rows WHERE sheet_id = ?').bind(sheet.id).first();
      let nextIdx = (maxRow?.max_idx ?? -1) + 1;

      for (const row of values) {
        await db.prepare('INSERT INTO sheet_rows (sheet_id, row_index, data) VALUES (?, ?, ?)')
          .bind(sheet.id, nextIdx++, JSON.stringify(row)).run();
      }
      return { success: true };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  // shareSpreadsheet is a no-op (D1 is private)
  async shareSpreadsheet(_id, _email) {
    return { success: true };
  }
}

module.exports = D1SheetsService;
