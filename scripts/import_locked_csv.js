/**
 * Import locked NFTs from Snowflake CSV - INSERT missing ones and UPDATE existing
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
    const csvPath = process.argv[2] || 'C:\\Users\\KyleM\\Downloads\\2025-12-20 9_23am.csv';

    console.log('');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('  IMPORT LOCKED NFTs FROM CSV (INSERT + UPDATE)');
    console.log('═══════════════════════════════════════════════════════════');
    console.log('');
    console.log(`Reading CSV: ${csvPath}`);

    // Read and parse CSV
    const csvContent = fs.readFileSync(csvPath, 'utf-8');
    const records = parse(csvContent, {
        columns: true,
        skip_empty_lines: true
    });

    console.log(`Found ${records.length} locked NFT records`);

    if (records.length === 0) {
        console.log('No records found, exiting.');
        return;
    }

    // Show sample
    console.log('\nSample records:');
    records.slice(0, 5).forEach(r => {
        console.log(`  Wallet: ${r.WALLET_ADDRESS}, NFT: ${r.NFT_ID}`);
    });

    console.log(`\nInserting/updating ${records.length} locked NFTs...`);

    // Insert in batches using UPSERT
    const BATCH_SIZE = 500;
    let totalInserted = 0;

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        // Build values string for batch insert
        // $1: wallet_address, $2: nft_id, $3: last_event_ts
        const values = batch.map((_, idx) =>
            `($${idx * 3 + 1}, $${idx * 3 + 2}, TRUE, $${idx * 3 + 3}, NOW())`
        ).join(', ');

        const params = batch.flatMap(r => [
            r.WALLET_ADDRESS.toLowerCase(),
            r.NFT_ID,
            r.ACQUIRED_AT // Use the acquired date from Snowflake
        ]);

        const result = await pool.query(
            `INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts, last_synced_at)
       VALUES ${values}
       ON CONFLICT (wallet_address, nft_id) 
       DO UPDATE SET 
        is_locked = TRUE, 
        last_event_ts = EXCLUDED.last_event_ts,
        last_synced_at = NOW()`,
            params
        );

        totalInserted += result.rowCount;

        if ((i + BATCH_SIZE) % 50000 === 0 || i + BATCH_SIZE >= records.length) {
            console.log(`  Progress: ${Math.min(i + BATCH_SIZE, records.length)}/${records.length} processed`);
        }
    }

    console.log(`\n✅ Inserted/updated ${totalInserted} wallet_holdings rows with is_locked = TRUE`);

    // Verify
    const verifyResult = await pool.query(
        `SELECT COUNT(*) as locked FROM wallet_holdings WHERE is_locked = TRUE`
    );
    console.log(`✅ Total locked holdings in database: ${verifyResult.rows[0].locked}`);

    await pool.end();
    console.log('\n✅ Complete!');
}

main().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
