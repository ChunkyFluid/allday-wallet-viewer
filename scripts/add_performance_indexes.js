// scripts/add_performance_indexes.js
// Add critical indexes for query performance

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function addIndexes() {
    console.log("Adding performance indexes...\n");

    const indexes = [
        {
            name: "idx_wallet_holdings_wallet_address",
            sql: `CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet_address ON wallet_holdings(wallet_address);`,
            description: "Index for wallet lookups"
        },
        {
            name: "idx_wallet_holdings_nft_id",
            sql: `CREATE INDEX IF NOT EXISTS idx_wallet_holdings_nft_id ON wallet_holdings(nft_id);`,
            description: "Index for NFT lookups"
        },
        {
            name: "idx_wallet_holdings_last_event_ts",
            sql: `CREATE INDEX IF NOT EXISTS idx_wallet_holdings_last_event_ts ON wallet_holdings(last_event_ts);`,
            description: "Index for incremental sync"
        },
        {
            name: "idx_wallet_holdings_wallet_nft",
            sql: `CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet_nft ON wallet_holdings(wallet_address, nft_id);`,
            description: "Composite index for common lookups"
        },
        {
            name: "idx_wallet_holdings_wallet_locked",
            sql: `CREATE INDEX IF NOT EXISTS idx_wallet_holdings_wallet_locked ON wallet_holdings(wallet_address, is_locked);`,
            description: "Index for locked/unlocked queries"
        },
        {
            name: "idx_nft_metadata_edition_id",
            sql: `CREATE INDEX IF NOT EXISTS idx_nft_metadata_edition_id ON nft_core_metadata(edition_id);`,
            description: "Index for edition joins"
        },
        {
            name: "idx_nft_metadata_tier",
            sql: `CREATE INDEX IF NOT EXISTS idx_nft_metadata_tier ON nft_core_metadata(tier);`,
            description: "Index for tier filtering"
        },
        {
            name: "idx_nft_metadata_team",
            sql: `CREATE INDEX IF NOT EXISTS idx_nft_metadata_team ON nft_core_metadata(team_name);`,
            description: "Index for team filtering"
        },
        {
            name: "idx_price_scrape_edition_id",
            sql: `CREATE INDEX IF NOT EXISTS idx_price_scrape_edition_id ON edition_price_scrape(edition_id);`,
            description: "Index for price joins"
        },
        {
            name: "idx_wallet_profiles_address",
            sql: `CREATE INDEX IF NOT EXISTS idx_wallet_profiles_address ON wallet_profiles(wallet_address);`,
            description: "Index for profile lookups"
        }
    ];

    for (const idx of indexes) {
        try {
            console.log(`Creating ${idx.name}...`);
            await pgQuery(idx.sql);
            console.log(`  ✓ ${idx.description}`);
        } catch (err) {
            console.error(`  ✗ Failed: ${err.message}`);
            // Continue with other indexes even if one fails
        }
    }

    console.log("\n✅ Index creation complete!");
    console.log("\nAnalyzing table sizes...");
    
    try {
        const stats = await pgQuery(`
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
                pg_total_relation_size(schemaname||'.'||tablename) AS size_bytes
            FROM pg_tables
            WHERE schemaname = 'public'
                AND tablename IN ('wallet_holdings', 'nft_core_metadata', 'edition_price_scrape', 'wallet_profiles')
            ORDER BY size_bytes DESC;
        `);
        
        console.log("\nTable sizes:");
        for (const row of stats.rows) {
            console.log(`  ${row.tablename.padEnd(30)} ${row.size}`);
        }
    } catch (err) {
        console.warn("Could not get table stats:", err.message);
    }
}

addIndexes()
    .then(() => {
        console.log("\n✅ Done!");
        process.exit(0);
    })
    .catch((err) => {
        console.error("Fatal error:", err);
        process.exit(1);
    });

