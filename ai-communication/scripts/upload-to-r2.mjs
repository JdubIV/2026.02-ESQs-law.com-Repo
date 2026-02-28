#!/usr/bin/env node
/**
 * Upload a local file to R2 via the documents store API.
 * Usage: node upload-to-r2.mjs <file_path> [case_id] [client_name] [template_id]
 */
import fs from 'fs';
import path from 'path';
import https from 'https';

const [,, filePath, caseId, clientName, templateId] = process.argv;
if (!filePath || !fs.existsSync(filePath)) {
  console.error('Usage: node upload-to-r2.mjs <file_path> [case_id] [client_name] [template_id]');
  process.exit(1);
}

const fileName = path.basename(filePath);
const b64 = fs.readFileSync(filePath).toString('base64');
const payload = JSON.stringify({
  file_name: fileName,
  file_content: b64,
  case_id: caseId || null,
  client_name: clientName || null,
  doc_type: templateId || fileName.split('-')[0] || null,
  template_id: templateId || null
});

const req = https.request('https://api.esqs-law.com/api/documents/store', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }
}, (res) => {
  let body = '';
  res.on('data', chunk => body += chunk);
  res.on('end', () => {
    try {
      const data = JSON.parse(body);
      if (data.success) {
        console.log(`✅ Uploaded: ${data.download_url}`);
        console.log(`   File: ${data.file_name} (${data.file_size} bytes)`);
        console.log(`   ID: ${data.id}`);
      } else {
        console.error('Upload failed:', body);
      }
    } catch { console.error('Response:', body); }
  });
});
req.on('error', (e) => console.error('Error:', e.message));
req.write(payload);
req.end();
