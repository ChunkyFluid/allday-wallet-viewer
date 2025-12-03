// scripts/diagnose_holdings.js
// Diagnostic script to check wallet_holdings table status

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function diagnose() {
    console.log("========================================");
    console.log("Wallet Holdings Diagnostic");
    console.log("========================================\n");

    try {
        // Check total count
        const totalRes = await pgQuery(`SELECT COUNT(*) as count FROM wallet_holdings`);
        const totalCount = parseInt(totalRes.rows[0].count, 10);
        console.log(`Total holdings in database: ${totalCount.toLocaleString()}`);

        if (totalCount === 0) {
            console.log("\n⚠️  WARNING: Database is EMPTY - no holdings found!");
            console.log("   This suggests the ETL may have failed or data was deleted.");
            console.log("   Recommendation: Run FULL REFRESH (option 2) to reload all data.\n");
            return;
        }

        // Check by wallet
        const walletStats = await pgQuery(`
            SELECT 
                wallet_address,
                COUNT(*) as nft_count,
                MIN(last_event_ts) as oldest_ts,
                MAX(last_event_ts) as newest_ts,
                MAX(last_synced_at) as last_synced
            FROM wallet_holdings
            GROUP BY wallet_address
            ORDER BY nft_count DESC
            LIMIT 10
        `);

        console.log(`\nTop 10 wallets by NFT count:`);
        console.log("Wallet Address".padEnd(20) + "NFTs".padStart(10) + "Oldest Event".padStart(20) + "Newest Event".padStart(20));
        console.log("-".repeat(70));

        for (const row of walletStats.rows) {
            const wallet = row.wallet_address.length > 18 
                ? row.wallet_address.slice(0, 15) + "..." 
                : row.wallet_address;
            console.log(
                wallet.padEnd(20) + 
                row.nft_count.toString().padStart(10) + 
                (row.oldest_ts ? new Date(row.oldest_ts).toISOString().slice(0, 10) : "NULL").padStart(20) +
                (row.newest_ts ? new Date(row.newest_ts).toISOString().slice(0, 10) : "NULL").padStart(20)
            );
        }

        // Check timestamp distribution
        const timestampStats = await pgQuery(`
            SELECT 
                COUNT(*) as total,
                COUNT(last_event_ts) as with_timestamp,
                COUNT(*) - COUNT(last_event_ts) as null_timestamps,
                MIN(last_event_ts) as min_ts,
                MAX(last_event_ts) as max_ts
            FROM wallet_holdings
        `);

        const stats = timestampStats.rows[0];
        console.log(`\nTimestamp Statistics:`);
        console.log(`  Total records: ${parseInt(stats.total, 10).toLocaleString()}`);
        console.log(`  With timestamp: ${parseInt(stats.with_timestamp, 10).toLocaleString()}`);
        console.log(`  NULL timestamps: ${parseInt(stats.null_timestamps, 10).toLocaleString()}`);
        if (stats.min_ts) {
            console.log(`  Oldest timestamp: ${stats.min_ts}`);
        }
        if (stats.max_ts) {
            console.log(`  Newest timestamp: ${stats.max_ts}`);
        }

        // Check last sync time
        const syncStats = await pgQuery(`
            SELECT 
                MIN(last_synced_at) as first_sync,
                MAX(last_synced_at) as last_sync,
                COUNT(DISTINCT DATE(last_synced_at)) as sync_days
            FROM wallet_holdings
        `);

        const sync = syncStats.rows[0];
        console.log(`\nSync Statistics:`);
        if (sync.first_sync) {
            console.log(`  First sync: ${sync.first_sync}`);
        }
        if (sync.last_sync) {
            console.log(`  Last sync: ${sync.last_sync}`);
            const lastSyncAge = (Date.now() - new Date(sync.last_sync).getTime()) / 1000 / 60 / 60;
            console.log(`  Age: ${lastSyncAge.toFixed(1)} hours ago`);
        }
        console.log(`  Unique sync days: ${parseInt(sync.sync_days, 10)}`);

        console.log("\n✅ Diagnostic complete");

    } catch (err) {
        console.error("❌ Diagnostic failed:", err);
        process.exit(1);
    }
}

diagnose()
    .then(() => {
        process.exit(0);
    })
    .catch((err) => {
        console.error("Fatal error:", err);
        process.exit(1);
    });

