/**
 * UTAH COURT CALENDAR SERVICE
 * Scrapes the Utah State Courts public calendar for hearing dates.
 *
 * Two calendars:
 *   1. Internal calendar (calendar-events.json / D1 deadlines) — already handled
 *   2. Utah State Courts public calendar — this service
 *
 * URL pattern:
 *   https://legacy.utcourts.gov/cal/search.php?t=a&c=&p=&j=&f=&l=&b={barNumber}&d=all&loc=all
 *
 * Bar numbers:
 *   JWA3 (John William Adams III): 19429
 *   DP (Diane Pitcher): 12626
 */

const axios = require('axios');

const BAR_NUMBERS = {
  JWA3: '19429',   // John William Adams III — primary
  DP: '12626',     // Diane Pitcher
};

const CALENDAR_BASE = 'https://legacy.utcourts.gov/cal/search.php';

class UtahCourtCalendarService {
  constructor(config = {}) {
    this.barNumbers = config.barNumbers || BAR_NUMBERS;
    this.timeout = config.timeout || 15000;
  }

  /**
   * Fetch all upcoming hearings for a bar number
   * @param {string} [attorney='JWA3'] - Attorney key or raw bar number
   * @returns {Object} { success, hearings[], attorney, barNumber, error }
   */
  async getHearings(attorney = 'JWA3') {
    const barNumber = this.barNumbers[attorney] || attorney;

    const url = `${CALENDAR_BASE}?t=a&c=&p=&j=&f=&l=&b=${barNumber}&d=all&loc=all`;
    console.log(`📅 Utah Courts Calendar: fetching ${url}`);

    try {
      const response = await axios.get(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml',
        },
        timeout: this.timeout,
      });

      const html = response.data;
      const hearings = this._parseCalendarHtml(html);

      console.log(`📅 Utah Courts Calendar: found ${hearings.length} hearing(s) for bar #${barNumber}`);

      return {
        success: true,
        hearings,
        attorney,
        barNumber,
        url,
        fetchedAt: new Date().toISOString(),
      };
    } catch (error) {
      console.warn(`📅 Utah Courts Calendar error: ${error.message}`);
      return {
        success: false,
        error: error.message,
        attorney,
        barNumber,
        url,
        hearings: [],
      };
    }
  }

  /**
   * Fetch hearings for ALL configured attorneys
   * @returns {Object} { success, allHearings[], errors[] }
   */
  async getAllHearings() {
    const allHearings = [];
    const errors = [];

    for (const [key, barNum] of Object.entries(this.barNumbers)) {
      const result = await this.getHearings(key);
      if (result.success) {
        // Tag each hearing with the attorney
        result.hearings.forEach(h => { h.attorney = key; h.barNumber = barNum; });
        allHearings.push(...result.hearings);
      } else {
        errors.push({ attorney: key, error: result.error });
      }
    }

    // Deduplicate by case number + date
    const seen = new Set();
    const unique = allHearings.filter(h => {
      const key = `${h.caseNumber || ''}-${h.date || ''}-${h.time || ''}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    // Sort by date ascending
    unique.sort((a, b) => {
      const da = this._parseDate(a.date);
      const db = this._parseDate(b.date);
      return da - db;
    });

    return {
      success: true,
      hearings: unique,
      totalCount: unique.length,
      errors,
      fetchedAt: new Date().toISOString(),
    };
  }

  /**
   * Search hearings for a specific client name or case number
   * @param {string} query - Client name or case number
   * @param {string} [attorney] - Specific attorney, or null for all
   * @returns {Object} { success, hearings[], query }
   */
  async searchHearings(query, attorney = null) {
    const result = attorney
      ? await this.getHearings(attorney)
      : await this.getAllHearings();

    if (!result.success && !result.hearings?.length) {
      return { success: false, error: result.error || 'No hearings found', hearings: [], query };
    }

    const queryLower = query.toLowerCase();
    const filtered = result.hearings.filter(h => {
      const text = [h.caseName, h.caseNumber, h.parties, h.type, h.location, h.judge]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
      return text.includes(queryLower);
    });

    return {
      success: true,
      hearings: filtered,
      totalBeforeFilter: result.hearings?.length || result.totalCount || 0,
      query,
    };
  }

  /**
   * Parse the Utah Courts calendar HTML into structured hearing data
   * The calendar uses a table-based layout.
   */
  _parseCalendarHtml(html) {
    const hearings = [];

    // The calendar renders hearings in table rows.
    // Each hearing block typically contains: date, time, case info, hearing type, location, judge

    // Strategy 1: Look for table rows with date patterns
    // The calendar uses <tr> elements with hearing data
    const rowPattern = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
    let currentDate = null;

    // First, try to find date headers — they appear as bold or in <th> or standalone date patterns
    const dateHeaderPattern = /(?:<(?:th|td|b|strong)[^>]*>)\s*(\w+day,\s+\w+\s+\d{1,2},\s+\d{4})\s*(?:<\/(?:th|td|b|strong)>)/gi;
    const dateMatches = [...html.matchAll(dateHeaderPattern)];

    // Also try simple date patterns like MM/DD/YYYY
    const simpleDatePattern = /(\d{1,2}\/\d{1,2}\/\d{4})/g;

    // Strategy 2: Extract individual hearing entries using common patterns
    // Look for case number patterns (e.g., 250100435, or formatted like 250-100-435)
    const casePattern = /(\d{9,}|\d{3}-?\d{3}-?\d{3})/g;

    // Strategy 3: Parse the full HTML more carefully
    // Split by <tr> and look for rows containing hearing data
    const rows = html.split(/<tr/i);

    for (const row of rows) {
      // Extract all text content from the row
      const text = row.replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
      if (text.length < 10) continue;

      // Look for date patterns in the text
      const dateMatch = text.match(/(\d{1,2}\/\d{1,2}\/\d{4})/);
      const longDateMatch = text.match(/(\w+day,?\s+\w+\s+\d{1,2},?\s+\d{4})/i);

      if (longDateMatch) {
        currentDate = longDateMatch[1];
      } else if (dateMatch) {
        currentDate = dateMatch[1];
      }

      // Look for time patterns
      const timeMatch = text.match(/(\d{1,2}:\d{2}\s*(?:AM|PM|a\.m\.|p\.m\.))/i);

      // Look for hearing type keywords
      const typeMatch = text.match(/\b(PRETRIAL|PRE-TRIAL|HEARING|TRIAL|CONFERENCE|APPEARANCE|ARRAIGNMENT|SENTENCING|REVIEW|STATUS|MOTION|SCHEDULING|EVIDENTIARY|ORAL\s+ARGUMENT|OSC|ORDER\s+TO\s+SHOW\s+CAUSE)\b/i);

      // Look for case numbers
      const caseMatch = text.match(/(?:Case\s*#?\s*|No\.?\s*)(\d{9,}|\d{3,}-\d{3,}-\d{3,})/i) ||
                         text.match(/\b(\d{9,})\b/);

      // Look for judge names
      const judgeMatch = text.match(/(?:Judge|Hon\.?|J\.)\s+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)/i);

      // Look for courtroom/location
      const locationMatch = text.match(/(?:Courtroom|Room|Rm\.?)\s+(\S+)/i) ||
                            text.match(/((?:First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth)\s+District)/i);

      // Look for case names (e.g., "State v. Smith" or "Smith v. Smith")
      const caseNameMatch = text.match(/([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\s+v\.?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)/i);

      // Look for hearing format
      const formatMatch = text.match(/\b(In\s*Person|Virtual|Hybrid|Zoom|Telephone|Phone|Video)\b/i);

      // Only create a hearing entry if we have at least a date or type
      if ((currentDate || dateMatch) && (typeMatch || caseMatch || timeMatch)) {
        const hearing = {
          date: dateMatch ? dateMatch[1] : currentDate,
          time: timeMatch ? timeMatch[1] : null,
          type: typeMatch ? typeMatch[1].toUpperCase() : 'HEARING',
          caseNumber: caseMatch ? caseMatch[1].replace(/-/g, '') : null,
          caseName: caseNameMatch ? caseNameMatch[1] : null,
          judge: judgeMatch ? judgeMatch[1] : null,
          location: locationMatch ? locationMatch[1] : null,
          format: formatMatch ? formatMatch[1] : null,
          parties: caseNameMatch ? caseNameMatch[1] : null,
          rawText: text.substring(0, 300),
        };

        // Avoid duplicates within same parse
        const isDupe = hearings.some(h =>
          h.caseNumber === hearing.caseNumber &&
          h.date === hearing.date &&
          h.time === hearing.time
        );

        if (!isDupe && (hearing.caseNumber || hearing.caseName)) {
          hearings.push(hearing);
        }
      }
    }

    return hearings;
  }

  _parseDate(dateStr) {
    if (!dateStr) return new Date(9999, 0);
    // Handle MM/DD/YYYY
    const slashMatch = dateStr.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (slashMatch) {
      return new Date(parseInt(slashMatch[3]), parseInt(slashMatch[1]) - 1, parseInt(slashMatch[2]));
    }
    // Try native parsing
    const d = new Date(dateStr);
    return isNaN(d.getTime()) ? new Date(9999, 0) : d;
  }

  /**
   * Format hearings as context string for injection into Synthia's prompt
   */
  formatForContext(hearings, label = 'UTAH STATE COURTS CALENDAR') {
    if (!hearings.length) return '';

    let ctx = `\n\n⚖️ ${label} (LIVE from utcourts.gov — ${hearings.length} hearing(s)):\n`;
    hearings.forEach((h, i) => {
      ctx += `\n${i + 1}. `;
      if (h.date) ctx += `Date: ${h.date}`;
      if (h.time) ctx += ` at ${h.time}`;
      ctx += '\n';
      if (h.type) ctx += `   Type: ${h.type}\n`;
      if (h.caseName) ctx += `   Case: ${h.caseName}\n`;
      if (h.caseNumber) ctx += `   Case #: ${h.caseNumber}\n`;
      if (h.judge) ctx += `   Judge: ${h.judge}\n`;
      if (h.location) ctx += `   Location: ${h.location}\n`;
      if (h.format) ctx += `   Format: ${h.format}\n`;
      if (h.attorney) ctx += `   Attorney: ${h.attorney}\n`;
    });
    ctx += `\nSource: Official Utah State Courts Calendar (utcourts.gov) — fetched ${new Date().toISOString().substring(0, 16).replace('T', ' ')}\n`;

    return ctx;
  }
}

module.exports = UtahCourtCalendarService;
