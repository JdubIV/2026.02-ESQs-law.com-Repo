/**
 * FILE SEARCH SERVICE
 * Enables Synthia to search arbitrary paths on the F: drive
 * (and any local filesystem path the user points to).
 *
 * This extends beyond OneDriveLocalService's fixed basePath
 * to handle requests like:
 *   - "look in F:\OneDrive - DianePitcher\Test 2026.02.02\Malouf files II"
 *   - "search F: for Malouf"
 *   - "what's in the Malouf folder"
 *
 * Security: Only allows paths under F:\ or the configured allowed roots.
 */

const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');

// Allowed root directories (prevent traversal outside these)
const ALLOWED_ROOTS = [
  'F:/',
  'F:\\',
  'C:/Users/EBPC/Documents',
  'C:\\Users\\EBPC\\Documents',
];

// Known base locations to search when user gives a partial path or client name
const SEARCH_ROOTS = [
  'F:/Office Associate',
  'F:/OneDrive - DianePitcher',
  'F:/Office Associate/Open Cases',
];

// File extensions we care about for legal work
const LEGAL_EXTENSIONS = new Set([
  '.pdf', '.docx', '.doc', '.xlsx', '.xls', '.txt', '.md',
  '.msg', '.eml', '.csv', '.rtf', '.odt', '.pptx', '.ppt',
  '.jpg', '.jpeg', '.png', '.tif', '.tiff', '.bmp',
]);

class FileSearchService {
  constructor(config = {}) {
    this.allowedRoots = config.allowedRoots || ALLOWED_ROOTS;
    this.searchRoots = config.searchRoots || SEARCH_ROOTS;
    this.maxResults = config.maxResults || 50;
    this.maxDepth = config.maxDepth || 5;
  }

  /**
   * Validate that a path is under an allowed root
   */
  isAllowedPath(targetPath) {
    const normalized = path.resolve(targetPath).toLowerCase().replace(/\\/g, '/');
    return this.allowedRoots.some(root => {
      const normalizedRoot = path.resolve(root).toLowerCase().replace(/\\/g, '/');
      return normalized.startsWith(normalizedRoot);
    });
  }

  /**
   * List contents of a specific directory
   * @param {string} dirPath - Absolute path to directory
   * @returns {Object} { success, files, folders, path, error }
   */
  async listDirectory(dirPath) {
    try {
      const resolved = path.resolve(dirPath);
      if (!this.isAllowedPath(resolved)) {
        return { success: false, error: `Path not allowed: ${dirPath}` };
      }

      const items = await fs.readdir(resolved, { withFileTypes: true });
      const files = [];
      const folders = [];

      for (const item of items) {
        const itemPath = path.join(resolved, item.name);
        try {
          const stats = await fs.stat(itemPath);
          const entry = {
            name: item.name,
            path: itemPath,
            size: stats.size,
            modified: stats.mtime.toISOString(),
            sizeHuman: this._humanSize(stats.size),
          };

          if (item.isDirectory()) {
            folders.push(entry);
          } else {
            entry.ext = path.extname(item.name).toLowerCase();
            files.push(entry);
          }
        } catch {
          // Skip items we can't stat (permissions, etc.)
        }
      }

      // Sort: folders first alphabetically, then files by modified desc
      folders.sort((a, b) => a.name.localeCompare(b.name));
      files.sort((a, b) => new Date(b.modified) - new Date(a.modified));

      return {
        success: true,
        path: resolved,
        folderCount: folders.length,
        fileCount: files.length,
        folders,
        files,
      };
    } catch (error) {
      return { success: false, error: error.message, path: dirPath };
    }
  }

  /**
   * Search for files by name across search roots or a specific path
   * @param {string} query - Search term (client name, file name, etc.)
   * @param {string} [startPath] - Optional starting directory (defaults to search roots)
   * @returns {Object} { success, results, searchedPaths, error }
   */
  async searchFiles(query, startPath = null) {
    const results = [];
    const searchedPaths = [];
    const queryLower = query.toLowerCase();

    const searchIn = startPath ? [startPath] : this.searchRoots;

    for (const root of searchIn) {
      const resolved = path.resolve(root);
      if (!this.isAllowedPath(resolved)) continue;
      if (!fsSync.existsSync(resolved)) continue;

      searchedPaths.push(resolved);
      await this._recursiveSearch(resolved, queryLower, results, 0);

      if (results.length >= this.maxResults) break;
    }

    // Sort by relevance: exact name match > contains in name > contains in path
    results.sort((a, b) => {
      const aName = a.name.toLowerCase();
      const bName = b.name.toLowerCase();
      const aExact = aName === queryLower ? 3 : aName.includes(queryLower) ? 2 : 1;
      const bExact = bName === queryLower ? 3 : bName.includes(queryLower) ? 2 : 1;
      if (aExact !== bExact) return bExact - aExact;
      return new Date(b.modified) - new Date(a.modified);
    });

    return {
      success: true,
      query,
      resultCount: results.length,
      results: results.slice(0, this.maxResults),
      searchedPaths,
    };
  }

  /**
   * Search for a client folder by name
   * @param {string} clientName - Client name to search for
   * @returns {Object} { success, clientFolder, files, error }
   */
  async findClientFolder(clientName) {
    const nameLower = clientName.toLowerCase();
    const nameParts = nameLower.split(/\s+/);

    for (const root of this.searchRoots) {
      const resolved = path.resolve(root);
      if (!fsSync.existsSync(resolved)) continue;

      try {
        const items = await fs.readdir(resolved, { withFileTypes: true });
        for (const item of items) {
          if (!item.isDirectory()) continue;
          const folderLower = item.name.toLowerCase();

          // Match: folder contains full name, or all name parts
          const fullMatch = folderLower.includes(nameLower);
          const partsMatch = nameParts.every(p => folderLower.includes(p));

          if (fullMatch || partsMatch) {
            const folderPath = path.join(resolved, item.name);
            const listing = await this.listDirectory(folderPath);
            return {
              success: true,
              clientFolder: folderPath,
              folderName: item.name,
              ...listing,
            };
          }
        }

        // Also search one level deeper (e.g., Open Cases/ClientName)
        for (const item of items) {
          if (!item.isDirectory()) continue;
          try {
            const subItems = await fs.readdir(path.join(resolved, item.name), { withFileTypes: true });
            for (const sub of subItems) {
              if (!sub.isDirectory()) continue;
              const subLower = sub.name.toLowerCase();
              if (subLower.includes(nameLower) || nameParts.every(p => subLower.includes(p))) {
                const folderPath = path.join(resolved, item.name, sub.name);
                const listing = await this.listDirectory(folderPath);
                return {
                  success: true,
                  clientFolder: folderPath,
                  folderName: sub.name,
                  ...listing,
                };
              }
            }
          } catch { /* skip unreadable subdirs */ }
        }
      } catch { /* skip unreadable roots */ }
    }

    return { success: false, error: `No folder found for client "${clientName}"` };
  }

  /**
   * Read a text file and return its content (for .txt, .md, .csv, etc.)
   */
  async readTextFile(filePath) {
    const resolved = path.resolve(filePath);
    if (!this.isAllowedPath(resolved)) {
      return { success: false, error: `Path not allowed: ${filePath}` };
    }
    try {
      const content = await fs.readFile(resolved, 'utf-8');
      return {
        success: true,
        path: resolved,
        name: path.basename(resolved),
        size: Buffer.byteLength(content),
        content: content.substring(0, 50000), // Cap at 50K chars for context injection
      };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }

  // --- Internal helpers ---

  async _recursiveSearch(dirPath, queryLower, results, depth) {
    if (depth > this.maxDepth || results.length >= this.maxResults) return;

    try {
      const items = await fs.readdir(dirPath, { withFileTypes: true });

      for (const item of items) {
        if (results.length >= this.maxResults) break;

        const itemPath = path.join(dirPath, item.name);
        const nameLower = item.name.toLowerCase();

        // Match if name contains query
        if (nameLower.includes(queryLower)) {
          try {
            const stats = await fs.stat(itemPath);
            results.push({
              name: item.name,
              path: itemPath,
              isFolder: item.isDirectory(),
              size: stats.size,
              sizeHuman: this._humanSize(stats.size),
              modified: stats.mtime.toISOString(),
              ext: item.isDirectory() ? null : path.extname(item.name).toLowerCase(),
            });
          } catch { /* skip */ }
        }

        // Recurse into directories
        if (item.isDirectory()) {
          await this._recursiveSearch(itemPath, queryLower, results, depth + 1);
        }
      }
    } catch {
      // Permission denied or other FS error — skip
    }
  }

  _humanSize(bytes) {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
  }
}

module.exports = FileSearchService;
