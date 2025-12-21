/**
 * Patch recent wallet holdings from Snowflake catchup CSV
 * 
 * This script processes a CSV containing recent Deposit and NFTLocked events
 * to ensure the database is up-to-date with correct Acquired Dates for the last few days.
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
    const csvPath = process.argv[2] || "C:\\Users\\KyleM\\Downloads\\2025-12-20 10_36pm.csv";

    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('  CATCHUP PATCH: RECENT HOLDINGS');
    console.log('═══════════════════════════════════════════════════════════\n');

    if (!fs.existsSync(csvPath)) {
        console.error(`❌ File not found: ${csvPath}`);
        process.exit(1);
    }

    console.log(`Reading CSV: ${csvPath}`);
    const csvContent = fs.readFileSync(csvPath, 'utf-8');
    const records = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        trim: true
    });

    console.log(`Found ${records.length} recent activity records.`);

    if (records.length === 0) {
        console.log('No activity found. Database is likely already up to date.');
        return;
    }

    // Sample data check
    console.log('\nSample Patch Data:');
    records.slice(0, 3).forEach(r => {
        console.log(`  - Wallet: ${r.WALLET_ADDRESS.substring(0, 10)}... NFT: ${r.NFT_ID} Acquired: ${r.ACQUIRED_AT} Locked: ${r.IS_LOCKED}`);
    });

    const BATCH_SIZE = 500;
    let totalProcessed = 0;
    let totalChanges = 0;

    console.log(`\nApplying patches to wallet_holdings...`);

    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);

        // UPSERT logic:
        // If NFT exists for wallet: update locked status and date
        // If NFT doesn't exist: insert it
        const values = batch.map((_, idx) =>
            `($${idx * 4 + 1}, $${idx * 4 + 2}, $${idx * 4 + 3}, $${idx * 4 + 4}, NOW())`
        ).join(', ');

        const params = batch.flatMap(r => [
            r.WALLET_ADDRESS.toLowerCase(),
            r.NFT_ID,
            r.IS_LOCKED === 'true' || r.IS_LOCKED === true || r.IS_LOCKED === 'TRUE',
            r.ACQUIRED_AT
        ]);

        const res = await pool.query(`
      INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts, last_synced_at)
      VALUES ${values}
      ON CONFLICT (wallet_address, nft_id) 
      DO UPDATE SET 
        is_locked = EXCLUDED.is_locked,
        last_event_ts = EXCLUDED.last_event_ts,
        last_synced_at = NOW()
      WHERE 
        wallet_holdings.is_locked != EXCLUDED.is_locked OR 
        wallet_holdings.last_event_ts IS NULL OR
        wallet_holdings.last_event_ts != EXCLUDED.last_event_ts
    `, params);

        totalProcessed += batch.length;
        totalChanges += res.rowCount;

        if (i % 2000 === 0 || i + BATCH_SIZE >= records.length) {
            console.log(`  Progress: ${Math.min(i + BATCH_SIZE, records.length)}/${records.length} records processed...`);
        }
    }

    console.log(`\n✅ Catchup Complete!`);
    console.log(`   Processed: ${totalProcessed}`);
    console.log(`   Updated/Inserted: ${totalChanges}`);

    // One final check - count how many have NULL dates now
    const nullCheck = await pool.query(`SELECT COUNT(*) FROM wallet_holdings WHERE last_event_ts IS NULL`);
    if (parseInt(nullCheck.rows[0].count) > 0) {
        console.log(`   ⚠️ Warning: ${nullCheck.rows[0].count} moments still missing an acquired date.`);
    }

    await pool.end();
}

main().catch(err => {
    console.error('❌ Error applying patch:', err);
    process.exit(1);
});
