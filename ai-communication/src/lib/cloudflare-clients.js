/**
 * CLOUDFLARE API CLIENTS
 * Shared D1 and R2 client wrappers for Node.js (via REST API)
 * In Cloudflare Workers, use env bindings directly instead
 */

/**
 * D1 client — executes SQL via Cloudflare REST API
 */
function getD1Client(accountId, databaseId, apiToken) {
  accountId = accountId || process.env.CLOUDFLARE_ACCOUNT_ID;
  apiToken = apiToken || process.env.CLOUDFLARE_API_TOKEN;

  const baseUrl = `https://api.cloudflare.com/client/v4/accounts/${accountId}/d1/database/${databaseId}`;
  const headers = {
    'Authorization': `Bearer ${apiToken}`,
    'Content-Type': 'application/json'
  };

  return {
    prepare(sql) {
      let params = [];
      return {
        bind(...args) {
          params = args;
          return this;
        },
        async run() {
          const response = await fetch(`${baseUrl}/query`, {
            method: 'POST',
            headers,
            body: JSON.stringify({ sql, params })
          });
          const data = await response.json();
          if (!data.success) throw new Error(data.errors?.[0]?.message || 'D1 query failed');
          return data.result?.[0] || {};
        },
        async first() {
          const response = await fetch(`${baseUrl}/query`, {
            method: 'POST',
            headers,
            body: JSON.stringify({ sql, params })
          });
          const data = await response.json();
          if (!data.success) throw new Error(data.errors?.[0]?.message || 'D1 query failed');
          const results = data.result?.[0]?.results || [];
          return results[0] || null;
        },
        async all() {
          const response = await fetch(`${baseUrl}/query`, {
            method: 'POST',
            headers,
            body: JSON.stringify({ sql, params })
          });
          const data = await response.json();
          if (!data.success) throw new Error(data.errors?.[0]?.message || 'D1 query failed');
          return { results: data.result?.[0]?.results || [] };
        }
      };
    },
    async exec(sql) {
      const statements = sql.split(';').map(s => s.trim()).filter(Boolean);
      for (const stmt of statements) {
        await fetch(`${baseUrl}/query`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ sql: stmt })
        });
      }
    }
  };
}

/**
 * R2 client — stores/retrieves objects via Cloudflare REST API
 */
function getR2Client(accountId, bucketName, apiToken) {
  accountId = accountId || process.env.CLOUDFLARE_ACCOUNT_ID;
  apiToken = apiToken || process.env.CLOUDFLARE_API_TOKEN;

  const baseUrl = `https://api.cloudflare.com/client/v4/accounts/${accountId}/r2/buckets/${bucketName}/objects`;
  const headers = { 'Authorization': `Bearer ${apiToken}` };

  return {
    async put(key, body, contentType) {
      const response = await fetch(`${baseUrl}/${encodeURIComponent(key)}`, {
        method: 'PUT',
        headers: { ...headers, 'Content-Type': contentType || 'application/octet-stream' },
        body
      });
      if (!response.ok) throw new Error(`R2 PUT failed: ${response.status}`);
      return true;
    },
    async get(key) {
      const response = await fetch(`${baseUrl}/${encodeURIComponent(key)}`, {
        method: 'GET',
        headers
      });
      if (!response.ok) throw new Error(`R2 GET failed: ${response.status}`);
      return Buffer.from(await response.arrayBuffer());
    },
    async delete(key) {
      const response = await fetch(`${baseUrl}/${encodeURIComponent(key)}`, {
        method: 'DELETE',
        headers
      });
      return response.ok;
    },
    async list(prefix = '', limit = 100) {
      const url = new URL(`${baseUrl}`);
      if (prefix) url.searchParams.set('prefix', prefix);
      url.searchParams.set('limit', limit.toString());
      const response = await fetch(url.toString(), { headers });
      if (!response.ok) throw new Error(`R2 LIST failed: ${response.status}`);
      const data = await response.json();
      return data.result?.objects || [];
    }
  };
}

module.exports = { getD1Client, getR2Client };
