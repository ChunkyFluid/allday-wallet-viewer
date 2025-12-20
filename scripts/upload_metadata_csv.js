/**
 * Upload metadata from CSV to fix ??? teams
 * Usage: node scripts/upload_metadata_csv.js <path-to-metadata.csv>
 */

import fs from 'fs';
import { parse } from 'csv-parse/sync';
import { pgQuery } from '../db.js';

const csvPath = process.argv[2];
if (!csvPath) {
    console.error('Usage: node scripts/upload_metadata_csv.js <csv-file>');
    process.exit(1);
}

async function upload() {
    console.log('\n=== UPLOADING METADATA FROM CSV ===\n');
    console.log('[1/3] Reading CSV...');
    const csv = fs.readFileSync(csvPath, 'utf-8');
    const records = parse(csv, { columns: true, skip_empty_lines: true });
    console.log(`      ✅ Found ${records.length} rows\n`);

    console.log('[2/3] Updating metadata...');
    let updated = 0;

    for (const row of records) {
        const nftId = row.NFT_ID || row.nft_id || row.id;
        const teamName = row.TEAM_NAME || row.team_name || row.team;

        if (nftId && teamName) {
            await pgQuery(`
                INSERT INTO nft_core_metadata_v2 (nft_id, team_name)
                VALUES ($1, $2)
                ON CONFLICT (nft_id) DO UPDATE SET team_name = EXCLUDED.team_name
            `, [nftId, teamName]);
            updated++;

            if (updated % 5000 === 0) {
                console.log(`      Progress: ${updated.toLocaleString()}/${records.length.toLocaleString()}`);
            }
        }
    }
    console.log(`      ✅ Updated ${updated.toLocaleString()} rows\n`);

    console.log('[3/3] Verification...');
    const check = await pgQuery(`SELECT COUNT(*) as missing FROM nft_core_metadata_v2 WHERE team_name IS NULL OR team_name = ''`);
    console.log(`      ℹ️  ${check.rows[0].missing} records still missing team data\n`);

    console.log('=== UPLOAD COMPLETE ===\n');
    process.exit(0);
}

upload().catch(err => {
    console.error('❌ ERROR:', err.message);
    process.exit(1);
});
