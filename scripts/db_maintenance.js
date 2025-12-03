// scripts/db_maintenance.js
// Run database maintenance (VACUUM/ANALYZE) to optimize query performance

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function runMaintenance() {
    console.log("========================================");
    console.log("Database Maintenance");
    console.log("========================================\n");

    const tables = [
        'wallet_holdings',
        'nft_core_metadata',
        'wallet_summary_snapshot',
        'top_wallets_snapshot',
        'editions_snapshot',
        'edition_price_scrape',
        'wallet_profiles'
    ];

    try {
        console.log("Updating table statistics (ANALYZE)...\n");

        for (const table of tables) {
            try {
                console.log(`Analyzing ${table}...`);
                await pgQuery(`ANALYZE ${table};`);
                console.log(`  ✓ ${table} analyzed`);
            } catch (err) {
                console.warn(`  ⚠ Could not analyze ${table}: ${err.message}`);
            }
        }

        console.log("\nCleaning up dead tuples (VACUUM)...\n");
        console.log("Note: VACUUM can take a while on large tables\n");

        // VACUUM ANALYZE for tables that might have updates/deletes
        const vacuumTables = [
            'wallet_holdings',  // Has incremental updates
            'wallet_summary_snapshot',  // Has UPSERTs
            'wallet_profiles'   // Has updates
        ];

        for (const table of vacuumTables) {
            try {
                console.log(`Vacuuming ${table}...`);
                // Use VACUUM ANALYZE to both clean and update stats
                await pgQuery(`VACUUM ANALYZE ${table};`);
                console.log(`  ✓ ${table} vacuumed and analyzed`);
            } catch (err) {
                console.warn(`  ⚠ Could not vacuum ${table}: ${err.message}`);
            }
        }

        // Get table sizes
        console.log("\nTable sizes after maintenance:\n");
        const stats = await pgQuery(`
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
                pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
                pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) AS indexes_size
            FROM pg_tables
            WHERE schemaname = 'public'
                AND tablename = ANY($1)
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
        `, [tables]);

        console.log("Table".padEnd(30) + "Total Size".padStart(15) + "Table".padStart(15) + "Indexes".padStart(15));
        console.log("-".repeat(75));
        for (const row of stats.rows) {
            console.log(
                row.tablename.padEnd(30) + 
                row.size.padStart(15) +
                row.table_size.padStart(15) +
                row.indexes_size.padStart(15)
            );
        }

        console.log("\n✅ Database maintenance complete!");
        console.log("\nRecommendation: Run this script weekly or after major ETL runs.");

    } catch (err) {
        console.error("❌ Maintenance failed:", err);
        throw err;
    }
}

runMaintenance()
    .then(() => {
        process.exit(0);
    })
    .catch((err) => {
        console.error("Fatal error:", err);
        process.exit(1);
    });

