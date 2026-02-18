/**
 * D1 CALENDAR SERVICE
 * Replaces Google Calendar — stores events/deadlines in D1
 * Drop-in replacement: same method signatures as GoogleCalendarService
 */

const { getD1Client } = require('../lib/cloudflare-clients');

class D1CalendarService {
  constructor(config = {}) {
    this.db = config.db || null;
    this.calendarId = config.calendarId || 'primary';
    this.timeZone = config.timeZone || process.env.CALENDAR_TIMEZONE || 'America/Denver';
    this.accountId = config.accountId || process.env.CLOUDFLARE_ACCOUNT_ID;
    this.dbId = config.dbId || process.env.CLOUDFLARE_D1_ID || '33dfce44-67dc-4df3-a2af-f821590bf80b';
    this.apiToken = config.apiToken || process.env.CLOUDFLARE_API_TOKEN;
  }

  async getDb() {
    if (this.db) return this.db;
    this.db = getD1Client(this.accountId, this.dbId, this.apiToken);
    return this.db;
  }

  isConfigured() { return true; }

  async ensureTable() {
    const db = await this.getDb();
    await db.exec(`
      CREATE TABLE IF NOT EXISTS calendar_events (
        id TEXT PRIMARY KEY,
        calendar_id TEXT DEFAULT 'primary',
        summary TEXT NOT NULL,
        description TEXT,
        location TEXT,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        all_day INTEGER DEFAULT 0,
        attendees TEXT,
        event_type TEXT DEFAULT 'event',
        case_id TEXT,
        client_name TEXT,
        reminder_minutes INTEGER DEFAULT 30,
        recurrence TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_cal_start ON calendar_events(start_time);
      CREATE INDEX IF NOT EXISTS idx_cal_case ON calendar_events(case_id);
      CREATE INDEX IF NOT EXISTS idx_cal_type ON calendar_events(event_type);
    `);
  }

  async listEvents({ timeMin, timeMax, maxResults = 250 } = {}) {
    const db = await this.getDb();
    await this.ensureTable();

    let sql = 'SELECT * FROM calendar_events WHERE 1=1';
    const params = [];
    if (timeMin) { sql += ' AND end_time >= ?'; params.push(timeMin); }
    if (timeMax) { sql += ' AND start_time <= ?'; params.push(timeMax); }
    sql += ' ORDER BY start_time ASC LIMIT ?';
    params.push(maxResults);

    const stmt = db.prepare(sql);
    const result = await stmt.bind(...params).all();
    return result.results.map(this._mapRow);
  }

  async getEventsForRange(startDate, endDate) {
    const timeMin = `${startDate}T00:00:00`;
    const timeMax = `${endDate}T23:59:59`;
    return this.listEvents({ timeMin, timeMax });
  }

  async getEventsForDate(date) {
    return this.getEventsForRange(date, date);
  }

  async createEvent(event) {
    const db = await this.getDb();
    await this.ensureTable();
    const id = `evt-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    await db.prepare(`
      INSERT INTO calendar_events (id, calendar_id, summary, description, location, start_time, end_time, all_day, attendees, event_type, case_id, client_name, reminder_minutes)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      id,
      this.calendarId,
      event.summary || event.subject || '',
      event.description || event.body || '',
      event.location || '',
      event.start || event.startTime || event.start_time || '',
      event.end || event.endTime || event.end_time || '',
      event.allDay || event.all_day ? 1 : 0,
      JSON.stringify(event.attendees || []),
      event.eventType || event.event_type || 'event',
      event.caseId || event.case_id || null,
      event.clientName || event.client_name || null,
      event.reminderMinutes || 30
    ).run();

    return id;
  }

  async updateEvent(eventId, event) {
    const db = await this.getDb();
    const sets = [];
    const params = [];

    if (event.summary !== undefined) { sets.push('summary = ?'); params.push(event.summary); }
    if (event.description !== undefined) { sets.push('description = ?'); params.push(event.description); }
    if (event.location !== undefined) { sets.push('location = ?'); params.push(event.location); }
    if (event.start !== undefined) { sets.push('start_time = ?'); params.push(event.start); }
    if (event.end !== undefined) { sets.push('end_time = ?'); params.push(event.end); }
    if (event.eventType !== undefined) { sets.push('event_type = ?'); params.push(event.eventType); }

    if (sets.length === 0) return false;
    sets.push("updated_at = datetime('now')");
    params.push(eventId);

    await db.prepare(`UPDATE calendar_events SET ${sets.join(', ')} WHERE id = ?`).bind(...params).run();
    return true;
  }

  async deleteEvent(eventId) {
    const db = await this.getDb();
    await db.prepare('DELETE FROM calendar_events WHERE id = ?').bind(eventId).run();
    return true;
  }

  async getDeadlines(caseId) {
    const db = await this.getDb();
    await this.ensureTable();
    const result = await db.prepare(
      "SELECT * FROM calendar_events WHERE case_id = ? AND event_type IN ('deadline', 'hearing', 'filing') ORDER BY start_time ASC"
    ).bind(caseId).all();
    return result.results.map(this._mapRow);
  }

  async getUpcomingDeadlines(days = 7) {
    const db = await this.getDb();
    await this.ensureTable();
    const now = new Date().toISOString();
    const future = new Date(Date.now() + days * 86400000).toISOString();
    const result = await db.prepare(
      "SELECT * FROM calendar_events WHERE start_time >= ? AND start_time <= ? AND event_type IN ('deadline', 'hearing', 'filing') ORDER BY start_time ASC"
    ).bind(now, future).all();
    return result.results.map(this._mapRow);
  }

  _mapRow(row) {
    return {
      id: row.id,
      summary: row.summary,
      description: row.description,
      location: row.location,
      start: row.start_time,
      end: row.end_time,
      allDay: Boolean(row.all_day),
      attendees: row.attendees ? JSON.parse(row.attendees) : [],
      eventType: row.event_type,
      caseId: row.case_id,
      clientName: row.client_name,
      createdAt: row.created_at,
      updatedAt: row.updated_at
    };
  }
}

module.exports = D1CalendarService;
