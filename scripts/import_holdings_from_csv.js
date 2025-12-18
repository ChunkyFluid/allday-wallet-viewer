// scripts/import_holdings_from_csv.js
// Import holdings from a CSV file exported from Snowflake
// Run: node scripts/import_holdings_from_csv.js <path-to-csv>

import * as dotenv from "dotenv";
import fs from "fs";
import { parse } from "csv-parse";
import { pgQuery } from "../db.js";

dotenv.config();

const BATCH_SIZE = 10000;

async function importHoldingsFromCSV(csvPath) {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  IMPORTING HOLDINGS FROM CSV");
    console.log("══════════════════════════════════════════════════════════════\n");

    if (!fs.existsSync(csvPath)) {
        console.error(`❌ File not found: ${csvPath}`);
        process.exit(1);
    }

    console.log(`Reading from: ${csvPath}`);

    // Get current count
    const countResult = await pgQuery(`SELECT COUNT(*) as count FROM holdings;`);
    console.log(`Current holdings in database: ${parseInt(countResult.rows[0].count).toLocaleString()}`);

    const records = [];
    let totalInserted = 0;
    let lineCount = 0;

    // Parse CSV
    const parser = fs
        .createReadStream(csvPath)
        .pipe(parse({
            columns: true,
            skip_empty_lines: true,
            trim: true
        }));

    for await (const record of parser) {
        lineCount++;

        // Normalize column names (Snowflake exports uppercase)
        const walletAddress = (record.WALLET_ADDRESS || record.wallet_address || '').toLowerCase();
        const nftId = record.NFT_ID || record.nft_id;
        const isLocked = (record.IS_LOCKED || record.is_locked || 'false').toString().toLowerCase() === 'true';
        const acquiredAt = record.ACQUIRED_AT || record.acquired_at || record.LAST_EVENT_TS || record.last_event_ts;

        if (!walletAddress || !nftId) continue;

        records.push({
            wallet_address: walletAddress,
            nft_id: nftId,
            is_locked: isLocked,
            acquired_at: acquiredAt ? new Date(acquiredAt) : null
        });

        // Insert in batches
        if (records.length >= BATCH_SIZE) {
            await insertBatch(records);
            totalInserted += records.length;
            console.log(`  Inserted ${totalInserted.toLocaleString()} holdings (line ${lineCount.toLocaleString()})`);
            records.length = 0;
        }
    }

    // Insert remaining records
    if (records.length > 0) {
        await insertBatch(records);
        totalInserted += records.length;
        console.log(`  Inserted final batch of ${records.length.toLocaleString()} holdings`);
    }

    console.log(`\n✓ Import complete!`);
    console.log(`  Total inserted: ${totalInserted.toLocaleString()}`);

    // Print final count
    const finalCount = await pgQuery(`SELECT COUNT(*) as count FROM holdings;`);
    console.log(`  Total holdings in database: ${parseInt(finalCount.rows[0].count).toLocaleString()}`);
}

async function insertBatch(records) {
    const values = records.map((r, idx) => {
        const offset = idx * 4;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4})`;
    }).join(',\n');

    const params = records.flatMap(r => [
        r.wallet_address,
        r.nft_id,
        r.is_locked,
        r.acquired_at
    ]);

    await pgQuery(`
    INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
    VALUES ${values}
    ON CONFLICT (wallet_address, nft_id) DO UPDATE SET
      is_locked = EXCLUDED.is_locked,
      acquired_at = COALESCE(holdings.acquired_at, EXCLUDED.acquired_at)
  `, params);
}

// Main
const csvPath = process.argv[2];
if (!csvPath) {
    console.log("Usage: node scripts/import_holdings_from_csv.js <path-to-csv>");
    console.log("\nExport from Snowflake Web UI with this query:");
    console.log(`
SELECT
  LOWER(wallet_address) AS WALLET_ADDRESS,
  nft_id AS NFT_ID,
  COALESCE(is_locked, FALSE) AS IS_LOCKED,
  last_event_ts AS ACQUIRED_AT
FROM ALLDAY_VIEWER.ALLDAY.ALLDAY_WALLET_HOLDINGS_CURRENT
ORDER BY wallet_address, nft_id;
  `);
    process.exit(1);
}

importHoldingsFromCSV(csvPath)
    .then(() => process.exit(0))
    .catch(err => {
        console.error("❌ Error:", err.message);
        process.exit(1);
    });
