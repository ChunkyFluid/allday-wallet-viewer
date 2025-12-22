/**
 * Chunked locked NFT upload - handles 8.9M rows by breaking into batches
 */

import fs from 'fs';
import { parse } from 'csv-parse/sync';
import { pgQuery } from '../db.js';

const csvPath = process.argv[2];
const CHUNK_SIZE = 50000; // 50k IDs per UPDATE query

if (!csvPath) {
    console.error('Usage: node scripts/upload_locked_chunked.js <csv-file>');
    process.exit(1);
}

async function upload() {
    console.log('\n=== CHUNKED LOCKED NFT UPLOAD ===\n');

    console.log('[1/5] Reading CSV...');
    const start = Date.now();
    const csv = fs.readFileSync(csvPath, 'utf-8');
    const records = parse(csv, { columns: true, skip_empty_lines: true });
    console.log(`      ✅ ${records.length.toLocaleString()} rows (${Math.round((Date.now() - start) / 1000)}s)\n`);

    console.log('[2/5] Extracting NFT IDs...');
    const nftIds = records.map(r => r.NFT_ID || r.nft_id || r.id).filter(Boolean);
    const chunks = Math.ceil(nftIds.length / CHUNK_SIZE);
    console.log(`      ✅ ${nftIds.length.toLocaleString()} IDs → ${chunks} chunks\n`);

    console.log('[3/5] Resetting all to false...');
    await pgQuery('UPDATE wallet_holdings SET is_locked = false');
    await pgQuery('UPDATE holdings SET is_locked = false');
    console.log('      ✅ Reset\n');

    console.log('[4/5] Setting locked=true in chunks...');
    for (let i = 0; i < nftIds.length; i += CHUNK_SIZE) {
        const chunk = nftIds.slice(i, i + CHUNK_SIZE);
        const chunkNum = Math.floor(i / CHUNK_SIZE) + 1;

        await pgQuery('UPDATE wallet_holdings SET is_locked = true WHERE nft_id = ANY($1)', [chunk]);
        await pgQuery('UPDATE holdings SET is_locked = true WHERE nft_id = ANY($1)', [chunk]);

        const pct = Math.round((i / nftIds.length) * 100);
        console.log(`      Chunk ${chunkNum}/${chunks} (${pct}%)`);
    }
    console.log('      ✅ All chunks complete\n');

    console.log('[5/5] Verification...');
    const check = await pgQuery('SELECT COUNT(*) FILTER (WHERE is_locked) as locked FROM wallet_holdings');
    console.log(`      ✅ ${parseInt(check.rows[0].locked).toLocaleString()} locked NFTs\n`);

    const elapsed = Math.round((Date.now() - start) / 1000);
    console.log(`=== COMPLETE in ${Math.floor(elapsed / 60)}m ${elapsed % 60}s ===\n`);
    process.exit(0);
}

upload().catch(err => {
    console.error('\n❌ ERROR:', err.message);
    console.error(err.stack);
    process.exit(1);
});
