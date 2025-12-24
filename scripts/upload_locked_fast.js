/**
 * FAST locked NFT upload using PostgreSQL COPY
 * Uploads millions of rows in ~2-3 minutes instead of 30+
 */

import { pgQuery } from '../db.js';
import fs from 'fs';
import { pipeline } from 'stream/promises';
import { from as copyFrom } from 'pg-copy-streams';
import pg from 'pg';

const csvPath = process.argv[2];
if (!csvPath) {
  console.error('Usage: node scripts/upload_locked_fast.js <csv-file>');
  process.exit(1);
}

async function upload() {
  console.log('\n=== FAST LOCKED NFT UPLOAD ===\n');

  console.log('[1/6] Creating temporary table...');
  await pgQuery(`
    CREATE TEMP TABLE temp_locked_nfts (
      nft_id TEXT,
      wallet_address TEXT
    )
  `);
  console.log('      ✅ Created\n');

  console.log('[2/6] Uploading CSV to temp table (this is FAST)...');
  const client = new pg.Client({
    host: process.env.POSTGRES_HOST,
    database: process.env.POSTGRES_DATABASE,
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    ssl: { rejectUnauthorized: false }
  });

  await client.connect();

  const copyQuery = `COPY temp_locked_nfts (nft_id, wallet_address) FROM STDIN WITH CSV HEADER`;
  const stream = client.query(copyFrom(copyQuery));
  const fileStream = fs.createReadStream(csvPath);

  await pipeline(fileStream, stream);

  console.log('      ✅ Uploaded all rows\n');

  console.log('[3/6] Counting rows...');
  const count = await pgQuery('SELECT COUNT(*) as total FROM temp_locked_nfts');
  console.log(`      ✅ ${parseInt(count.rows[0].total).toLocaleString()} rows\n`);

  console.log('[4/6] Resetting all locked status to false...');
  await pgQuery('UPDATE holdings SET is_locked = false');
  console.log('      ✅ Reset complete\n');

  console.log('[5/6] Setting locked=true from temp table (single query)...');
  await pgQuery(`
    UPDATE holdings h
    SET is_locked = true
    FROM temp_locked_nfts t
    WHERE h.nft_id = t.nft_id
  `);
  console.log('      ✅ Updated\n');

  console.log('[6/6] Verification...');
  const check = await pgQuery('SELECT COUNT(*) FILTER (WHERE is_locked) as locked FROM holdings');
  console.log(`      ✅ ${parseInt(check.rows[0].locked).toLocaleString()} locked NFTs\n`);

  await client.end();
  console.log('=== COMPLETE ===\n');
  process.exit(0);
}

upload().catch(err => {
  console.error('\n❌ ERROR:', err.message);
  process.exit(1);
});
