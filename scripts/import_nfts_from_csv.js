// scripts/import_nfts_from_csv.js
// Import NFTs from a CSV file exported from Snowflake
// Run: node scripts/import_nfts_from_csv.js <path-to-csv>

import * as dotenv from "dotenv";
import fs from "fs";
import { parse } from "csv-parse";
import { pgQuery } from "../db.js";

dotenv.config();

const BATCH_SIZE = 10000;

async function importNFTsFromCSV(csvPath) {
    console.log("\n══════════════════════════════════════════════════════════════");
    console.log("  IMPORTING NFTs FROM CSV");
    console.log("══════════════════════════════════════════════════════════════\n");

    if (!fs.existsSync(csvPath)) {
        console.error(`❌ File not found: ${csvPath}`);
        process.exit(1);
    }

    console.log(`Reading from: ${csvPath}`);

    // Get current max NFT ID to track progress
    const maxResult = await pgQuery(`SELECT MAX(nft_id::bigint) as max_id FROM nfts WHERE nft_id ~ '^[0-9]+$';`);
    const existingMaxId = parseInt(maxResult.rows[0]?.max_id || 0);
    console.log(`Current max NFT ID in database: ${existingMaxId.toLocaleString()}`);

    const records = [];
    let totalProcessed = 0;
    let totalInserted = 0;
    let skipped = 0;

    // Parse CSV
    const parser = fs
        .createReadStream(csvPath)
        .pipe(parse({
            columns: true,
            skip_empty_lines: true,
            trim: true
        }));

    for await (const record of parser) {
        // Normalize column names (Snowflake exports uppercase)
        const nftId = record.NFT_ID || record.nft_id;
        const editionId = record.EDITION_ID || record.edition_id;
        const serialNumber = record.SERIAL_NUMBER || record.serial_number;
        const mintedAt = record.MINTED_AT || record.minted_at;

        if (!nftId) continue;

        // Skip if already in database (based on ID comparison)
        const nftIdNum = parseInt(nftId);
        if (nftIdNum <= existingMaxId) {
            skipped++;
            continue;
        }

        records.push({
            nft_id: nftId,
            edition_id: editionId || null,
            serial_number: serialNumber ? parseInt(serialNumber) : null,
            minted_at: mintedAt ? new Date(mintedAt) : null
        });

        // Insert in batches
        if (records.length >= BATCH_SIZE) {
            await insertBatch(records);
            totalInserted += records.length;
            totalProcessed += records.length + skipped;
            console.log(`  Inserted ${totalInserted.toLocaleString()} NFTs (skipped ${skipped.toLocaleString()} existing)`);
            records.length = 0;
            skipped = 0;
        }
    }

    // Insert remaining records
    if (records.length > 0) {
        await insertBatch(records);
        totalInserted += records.length;
        console.log(`  Inserted final batch of ${records.length.toLocaleString()} NFTs`);
    }

    console.log(`\n✓ Import complete!`);
    console.log(`  Total inserted: ${totalInserted.toLocaleString()}`);

    // Print final count
    const countResult = await pgQuery(`SELECT COUNT(*) as count FROM nfts;`);
    console.log(`  Total NFTs in database: ${parseInt(countResult.rows[0].count).toLocaleString()}`);
}

async function insertBatch(records) {
    const values = records.map((r, idx) => {
        const offset = idx * 4;
        return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4})`;
    }).join(',\n');

    const params = records.flatMap(r => [
        r.nft_id,
        r.edition_id,
        r.serial_number,
        r.minted_at
    ]);

    await pgQuery(`
    INSERT INTO nfts (nft_id, edition_id, serial_number, minted_at)
    VALUES ${values}
    ON CONFLICT (nft_id) DO UPDATE SET
      edition_id = EXCLUDED.edition_id,
      serial_number = EXCLUDED.serial_number,
      minted_at = COALESCE(nfts.minted_at, EXCLUDED.minted_at)
  `, params);
}

// Main
const csvPath = process.argv[2];
if (!csvPath) {
    console.log("Usage: node scripts/import_nfts_from_csv.js <path-to-csv>");
    console.log("\nExport from Snowflake Web UI with this query:");
    console.log(`
SELECT
  event_data:id::string AS NFT_ID,
  event_data:editionID::string AS EDITION_ID,
  TRY_TO_NUMBER(event_data:serialNumber::string) AS SERIAL_NUMBER,
  block_timestamp AS MINTED_AT
FROM FLOW_ONCHAIN_CORE_DATA.CORE.FACT_EVENTS
WHERE event_contract = 'A.e4cf4bdc1751c65d.AllDay'
  AND event_type = 'MomentNFTMinted'
  AND tx_succeeded = TRUE
  AND TRY_TO_NUMBER(event_data:id::string) > 3500000
ORDER BY TRY_TO_NUMBER(event_data:id::string);
  `);
    process.exit(1);
}

importNFTsFromCSV(csvPath)
    .then(() => process.exit(0))
    .catch(err => {
        console.error("❌ Error:", err.message);
        process.exit(1);
    });
