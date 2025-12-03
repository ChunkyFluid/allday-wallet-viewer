// scripts/cleanup_removed_holdings.js
// Removes wallet holdings that no longer exist in Snowflake (sold/transferred NFTs)
import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";
import { executeSnowflakeWithRetry, createSnowflakeConnection } from "./snowflake-utils.js";

dotenv.config();

async function cleanupRemovedHoldings() {
    const db = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;

    console.log("Starting cleanup of removed holdings...");
    console.log("Using Snowflake", `${db}.${schema}`);

    const connection = await createSnowflakeConnection();

    try {
        // Step 1: Get all current holdings from Snowflake (all unique wallet_address + nft_id pairs)
        console.log("Fetching current holdings from Snowflake...");
        const snowflakeHoldingsSql = `
            SELECT DISTINCT 
                LOWER(wallet_address) AS wallet_address,
                nft_id
            FROM ${db}.${schema}.ALLDAY_WALLET_HOLDINGS_CURRENT
        `;
        
        let snowflakeRows;
        try {
            snowflakeRows = await executeSnowflakeWithRetry(connection, snowflakeHoldingsSql, {
                maxRetries: 5, // More retries for cleanup since it's critical
                initialDelay: 2000
            });
        } catch (err) {
            console.error("❌ Failed to fetch holdings from Snowflake during cleanup");
            console.error("   This could mean Snowflake is unavailable or overloaded");
            console.error("   Skipping cleanup to prevent accidental data loss");
            console.error("   Error:", err.message || err.code || 'Unknown error');
            connection.destroy(() => {});
            return; // Exit without deleting anything
        }
        
        console.log(`Snowflake has ${snowflakeRows.length} current holdings`);

        // Create a Set for fast lookup
        const snowflakeHoldingsSet = new Set();
        for (const row of snowflakeRows) {
            const wa = (row.WALLET_ADDRESS ?? row.wallet_address || "").toLowerCase();
            const nftId = String(row.NFT_ID ?? row.nft_id);
            if (wa && nftId) {
                snowflakeHoldingsSet.add(`${wa}|${nftId}`);
            }
        }

        // Step 2: Get all local holdings from Postgres
        console.log("Fetching local holdings from Postgres...");
        const localHoldings = await pgQuery(`
            SELECT wallet_address, nft_id
            FROM wallet_holdings
        `);
        console.log(`Postgres has ${localHoldings.rows.length} holdings`);

        // Step 3: Find holdings that exist locally but not in Snowflake
        const toDelete = [];
        for (const row of localHoldings.rows) {
            const key = `${row.wallet_address.toLowerCase()}|${row.nft_id}`;
            if (!snowflakeHoldingsSet.has(key)) {
                toDelete.push({
                    wallet_address: row.wallet_address,
                    nft_id: row.nft_id
                });
            }
        }

        console.log(`Found ${toDelete.length} holdings to remove (sold/transferred)`);

        if (toDelete.length === 0) {
            console.log("✅ No cleanup needed - all holdings are current");
            connection.destroy(() => {
                console.log("Snowflake connection closed.");
            });
            return;
        }

        // Safety check: if we're about to delete more than 50% of holdings, something might be wrong
        const localCount = localHoldings.rows.length;
        const deletionPercentage = (toDelete.length / localCount) * 100;
        if (deletionPercentage > 50 && localCount > 1000) {
            console.error(`⚠️  WARNING: About to delete ${deletionPercentage.toFixed(1)}% of holdings (${toDelete.length} of ${localCount})`);
            console.error("   This seems unusual. Possible issues:");
            console.error("   - Snowflake query may have failed or returned incomplete data");
            console.error("   - Database might have been corrupted");
            console.error("   - Network issues with Snowflake");
            console.error("   ");
            console.error("   SKIPPING CLEANUP to prevent data loss!");
            console.error("   Please investigate manually before running cleanup again.");
            connection.destroy(() => {});
            return;
        }

        // Step 4: Delete in batches
        const DELETE_BATCH_SIZE = 5000;
        let deleted = 0;

        for (let i = 0; i < toDelete.length; i += DELETE_BATCH_SIZE) {
            const batch = toDelete.slice(i, i + DELETE_BATCH_SIZE);
            
            // Build DELETE query with WHERE IN clause
            const conditions = batch.map((item, idx) => {
                const wa = item.wallet_address.replace(/'/g, "''");
                const nft = item.nft_id.replace(/'/g, "''");
                return `(wallet_address = '${wa}' AND nft_id = '${nft}')`;
            }).join(' OR ');

            const deleteSql = `
                DELETE FROM wallet_holdings
                WHERE ${conditions}
            `;

            const result = await pgQuery(deleteSql);
            deleted += result.rowCount || 0;
            console.log(`Deleted ${deleted}/${toDelete.length} removed holdings...`);
        }

        console.log(`✅ Cleanup complete: Removed ${deleted} holdings that are no longer in Snowflake`);

        // Step 5: Optional - clean up orphaned metadata (NFTs not held by anyone)
        // This is optional and can be disabled if you want to keep metadata for all NFTs ever minted
        const cleanupMetadata = process.env.CLEANUP_ORPHANED_METADATA === 'true';
        
        if (cleanupMetadata) {
            console.log("Cleaning up orphaned metadata (NFTs not held by anyone)...");
            const orphanedResult = await pgQuery(`
                DELETE FROM nft_core_metadata
                WHERE nft_id NOT IN (
                    SELECT DISTINCT nft_id FROM wallet_holdings
                )
            `);
            console.log(`Removed ${orphanedResult.rowCount || 0} orphaned metadata records`);
        }

        connection.destroy(() => {
            console.log("Snowflake connection closed.");
        });

    } catch (err) {
        console.error("💥 Error during cleanup:", err);
        connection.destroy(() => {});
        throw err;
    }
}

cleanupRemovedHoldings()
    .then(() => {
        console.log("✅ Cleanup script complete.");
        process.exit(0);
    })
    .catch((err) => {
        console.error("💥 Fatal error during cleanup:", err);
        process.exit(1);
    });

