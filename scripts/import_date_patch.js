/**
 * Import date patch from Snowflake CSVs
 * ONLY updates last_event_ts (Acquired Date) where currently missing or newer
 */
import fs from 'fs';
import { parse } from 'csv-parse/sync';
import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function main() {
    const csvFiles = process.argv.slice(2);

    if (csvFiles.length === 0) {
        console.error('Usage: node scripts/import_date_patch.js <csv1> <csv2> ...');
        process.exit(1);
    }

    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('  DATE PATCH: FILLING MISSING ACQUIRED DATES');
    console.log('═══════════════════════════════════════════════════════════\n');

    for (const csvPath of csvFiles) {
        if (!fs.existsSync(csvPath)) {
            console.warn(`⚠️ File not found, skipping: ${csvPath}`);
            continue;
        }

        console.log(`Processing: ${csvPath}`);
        const csvContent = fs.readFileSync(csvPath, 'utf-8');
        const records = parse(csvContent, {
            columns: true,
            skip_empty_lines: true,
            trim: true
        });

        console.log(`  Found ${records.length} records in this file.`);

        const BATCH_SIZE = 500;
        let totalUpdated = 0;

        for (let i = 0; i < records.length; i += BATCH_SIZE) {
            const batch = records.slice(i, i + BATCH_SIZE);

            // We use a temporary table/CTE approach to update efficiently in batch
            // Or just a series of updates. Given 26k rows, batching with a multi-row update is best.
            const values = batch.map((_, idx) =>
                `($${idx * 3 + 1}, $${idx * 3 + 2}, $${idx * 3 + 3}::timestamptz)`
            ).join(', ');

            const params = batch.flatMap(r => [
                r.WALLET_ADDRESS.toLowerCase(),
                r.NFT_ID,
                r.ACQUIRED_AT
            ]);

            const res = await pool.query(`
        UPDATE wallet_holdings h
        SET last_event_ts = v.acquired_at,
            last_synced_at = NOW()
        FROM (VALUES ${values}) AS v(wallet_address, nft_id, acquired_at)
        WHERE h.wallet_address = v.wallet_address 
          AND h.nft_id = v.nft_id
          AND (h.last_event_ts IS NULL OR h.last_event_ts != v.acquired_at)
      `, params);

            totalUpdated += res.rowCount;
        }

        console.log(`  ✅ Finished ${csvPath}: Updated ${totalUpdated} rows`);
    }

    // Final count of missing
    const nullCheck = await pool.query(`SELECT COUNT(*) FROM wallet_holdings WHERE last_event_ts IS NULL`);
    console.log(`\nFinal check: ${nullCheck.rows[0].count} moments still missing dates.`);

    await pool.end();
}

main().catch(err => {
    console.error('❌ Error processing patch:', err);
    process.exit(1);
});
