// export_wallet_csv.js
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const wallet = (process.argv[2] || '').trim();

if (!wallet) {
  console.error('Usage: node export_wallet_csv.js 0xYourWalletHere');
  process.exit(1);
}

if (!/^0x[0-9a-fA-F]{4,64}$/.test(wallet)) {
  console.error('Invalid wallet address:', wallet);
  process.exit(1);
}

async function main() {
  const url = `http://localhost:3000/api/query?wallet=${encodeURIComponent(wallet)}`;
  console.log('Requesting', url);

  const res = await fetch(url);
  if (!res.ok) {
    console.error('HTTP error', res.status, await res.text());
    process.exit(1);
  }

  const json = await res.json();
  if (!json.ok) {
    console.error('API error:', json.error || 'unknown');
    process.exit(1);
  }

  const rows = json.rows || [];
  console.log(`Got ${rows.length} rows`);

  if (!rows.length) {
    console.log('No moments found for this wallet.');
    return;
  }

  const headers = Object.keys(rows[0]);
  const escapeCell = (val) => {
    if (val === null || val === undefined) return '';
    const s = String(val);
    if (s.includes('"') || s.includes(',') || s.includes('\n')) {
      return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
  };

  const lines = [];
  lines.push(headers.join(','));
  for (const r of rows) {
    lines.push(headers.map((h) => escapeCell(r[h])).join(','));
  }

  const outName = path.join(__dirname, `wallet_${wallet.toLowerCase()}.csv`);
  fs.writeFileSync(outName, lines.join('\n'), 'utf8');
  console.log('Wrote', outName);
}

main().catch((err) => {
  console.error('Error:', err);
  process.exit(1);
});
