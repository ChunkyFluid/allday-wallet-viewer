/**
 * Upload locked NFTs from CSV - FOR SPECIFIC WALLET ONLY
 * Usage: node scripts/upload_locked_for_wallet.js <csv-path> <wallet-address>
 */

import fs from 'fs';
import { parse } from 'csv-parse/sync';
import { pgQuery } from '../db.js';

const csvPath = process.argv[2];
const wallet = process.argv[3];

if (!csvPath || !wallet) {
    console.error('Usage: node scripts/upload_locked_for_wallet.js <csv-file> <wallet-address>');
    process.exit(1);
}

async function upload() {
    console.log('\n=== UPLOADING LOCKED NFTS FOR', wallet, '===\n');

    console.log('[1/5] Reading CSV...');
    const csv = fs.readFileSync(csvPath, 'utf-8');
    const records = parse(csv, { columns: true, skip_empty_lines: true });
    console.log(`      ✅ Found ${records.length.toLocaleString()} total rows\n`);

    console.log('[2/5] Filtering for your wallet...');
    const walletLower = wallet.toLowerCase();
    const myRecords = records.filter(r => {
        const w = (r.WALLET_ADDRESS || r.wallet_address || r.wallet || '').toLowerCase();
        return w === walletLower;
    });
    console.log(`      ✅ Found ${myRecords.length} locked NFTs for your wallet\n`);

    if (myRecords.length === 0) {
        console.log('      ⚠️  No records found for this wallet!');
        process.exit(0);
    }

    const nftIds = myRecords.map(r => r.NFT_ID || r.nft_id || r.id);

    console.log('[3/5] Resetting YOUR wallet locked status...');
    await pgQuery('UPDATE holdings SET is_locked = false WHERE wallet_address = $1', [walletLower]);
    console.log('      ✅ Reset complete\n');

    console.log('[4/5] Setting locked status for your NFTs...');
    await pgQuery('UPDATE holdings SET is_locked = true WHERE wallet_address = $1 AND nft_id = ANY($2)', [walletLower, nftIds]);
    console.log('      ✅ Updated\n');

    console.log('[5/5] Verification...');
    const check = await pgQuery('SELECT COUNT(*) FILTER (WHERE is_locked) as locked FROM holdings WHERE wallet_address = $1', [walletLower]);
    console.log(`      ✅ Your wallet has ${check.rows[0].locked} locked NFTs\n`);

    console.log('=== COMPLETE ===\n');
    process.exit(0);
}

upload().catch(err => {
    console.error('❌ ERROR:', err.message);
    process.exit(1);
});
