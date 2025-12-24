/**
 * Optimized locked NFT upload - single query approach
 * Much faster than batching, no extra dependencies
 */

import fs from 'fs';
import { parse } from 'csv-parse/sync';
import { pgQuery } from '../db.js';

const csvPath = process.argv[2];
if (!csvPath) {
    console.error('Usage: node scripts/upload_locked_optimized.js <csv-file>');
    process.exit(1);
}

async function upload() {
    console.log('\n=== OPTIMIZED LOCKED NFT UPLOAD ===\n');

    console.log('[1/4] Reading CSV...');
    const csv = fs.readFileSync(csvPath, 'utf-8');
    const records = parse(csv, { columns: true, skip_empty_lines: true });
    console.log(`      ✅ ${records.length.toLocaleString()} rows\n`);

    console.log('[2/4] Extracting NFT IDs...');
    const nftIds = records.map(r => r.NFT_ID || r.nft_id || r.id).filter(Boolean);
    console.log(`      ✅ ${nftIds.length.toLocaleString()} unique IDs\n`);

    console.log('[3/4] Single UPDATE query (locked = nft_id IN list)...');
    console.log('      This replaces steps 2 & 3 - much faster!');

    // Single query that does BOTH reset AND set in one shot
    await pgQuery('UPDATE holdings SET is_locked = (nft_id = ANY($1))', [nftIds]);

    console.log('      ✅ Updated all rows\n');

    console.log('[4/4] Verification...');
    const check = await pgQuery('SELECT COUNT(*) FILTER (WHERE is_locked) as locked FROM holdings');
    console.log(`      ✅ ${parseInt(check.rows[0].locked).toLocaleString()} locked NFTs\n`);

    console.log('=== COMPLETE ===\n');
    process.exit(0);
}

upload().catch(err => {
    console.error('\n❌ ERROR:', err.message);
    console.error(err.stack);
    process.exit(1);
});
