/**
 * Upload locked NFTs from CSV - with optional skip reset
 * Usage: node scripts/upload_locked_csv.js <csv-file> [--skip-reset]
 */

import fs from 'fs';
import { parse } from 'csv-parse/sync';
import { pgQuery } from '../db.js';

const csvPath = process.argv[2];
const skipReset = process.argv.includes('--skip-reset');

if (!csvPath) {
    console.error('Usage: node scripts/upload_locked_csv.js <csv-file> [--skip-reset]');
    process.exit(1);
}

async function upload() {
    console.log('\n=== UPLOADING LOCKED NFTS FROM CSV ===\n');

    console.log('[1/4] Reading CSV...');
    const csv = fs.readFileSync(csvPath, 'utf-8');
    const records = parse(csv, { columns: true, skip_empty_lines: true });
    console.log(`      ✅ Found ${records.length.toLocaleString()} rows\n`);

    if (skipReset) {
        console.log('[2/4] Resetting all locked status to false...');
        console.log('      ⏭️  SKIPPED (--skip-reset flag)\n');
    } else {
        console.log('[2/4] Resetting all locked status to false...');
        await pgQuery('UPDATE wallet_holdings SET is_locked = false');
        await pgQuery('UPDATE holdings SET is_locked = false');
        console.log('      ✅ Reset complete\n');
    }

    console.log('[3/4] Updating locked NFTs in batches...');
    const BATCH_SIZE = 1000;
    let updated = 0;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);
        const nftIds = batch.map(r => r.NFT_ID || r.nft_id || r.id);

        await pgQuery('UPDATE wallet_holdings SET is_locked = true WHERE nft_id = ANY($1)', [nftIds]);
        await pgQuery('UPDATE holdings SET is_locked = true WHERE nft_id = ANY($1)', [nftIds]);

        updated += batch.length;
        if (updated % 100000 === 0 || updated === records.length) {
            console.log(`      Progress: ${updated.toLocaleString()}/${records.length.toLocaleString()} (${Math.round(updated / records.length * 100)}%)`);
        }
    }
    console.log('      ✅ Complete\n');

    console.log('[4/4] Verification...');
    const check = await pgQuery('SELECT COUNT(*) FILTER (WHERE is_locked) as locked FROM wallet_holdings');
    console.log(`      ✅ Database has ${parseInt(check.rows[0].locked).toLocaleString()} locked NFTs\n`);

    console.log('=== UPLOAD COMPLETE ===\n');
    process.exit(0);
}

upload().catch(err => {
    console.error('\n❌ ERROR:', err.message);
    console.error(err.stack);
    process.exit(1);
});
