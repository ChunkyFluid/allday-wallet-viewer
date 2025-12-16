// scripts/fix_trading_tables.js
// Fix missing columns and add missing data for trading functionality

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function fixTables() {
    console.log("Fixing trading tables...\n");

    // Fix bundles table - add missing columns
    const bundlesAlters = [
        "ALTER TABLE bundles ADD COLUMN IF NOT EXISTS title TEXT",
        "ALTER TABLE bundles ADD COLUMN IF NOT EXISTS description TEXT",
        "ALTER TABLE bundles ADD COLUMN IF NOT EXISTS nft_ids JSONB DEFAULT '[]'",
        "ALTER TABLE bundles ADD COLUMN IF NOT EXISTS price_usd DECIMAL(10,2)",
        "ALTER TABLE bundles ADD COLUMN IF NOT EXISTS buyer_wallet TEXT",
        "ALTER TABLE bundles ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'",
        "ALTER TABLE bundles ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE bundles ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()"
    ];

    console.log("=== Fixing bundles table ===");
    for (const sql of bundlesAlters) {
        try {
            await pgQuery(sql);
            console.log("✅", sql.substring(0, 50) + "...");
        } catch (err) {
            console.log("⚠️ ", err.message);
        }
    }

    // Fix listings table - ensure required columns exist
    const listingsAlters = [
        "ALTER TABLE listings ADD COLUMN IF NOT EXISTS id SERIAL",
        "ALTER TABLE listings ADD COLUMN IF NOT EXISTS seller_wallet TEXT",
        "ALTER TABLE listings ADD COLUMN IF NOT EXISTS nft_id TEXT",
        "ALTER TABLE listings ADD COLUMN IF NOT EXISTS price_usd DECIMAL(10,2)",
        "ALTER TABLE listings ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'",
        "ALTER TABLE listings ADD COLUMN IF NOT EXISTS buyer_wallet TEXT",
        "ALTER TABLE listings ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
        "ALTER TABLE listings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()"
    ];

    console.log("\n=== Fixing listings table ===");
    for (const sql of listingsAlters) {
        try {
            await pgQuery(sql);
            console.log("✅", sql.substring(0, 50) + "...");
        } catch (err) {
            console.log("⚠️ ", err.message);
        }
    }

    // Show table columns
    console.log("\n=== Current bundles columns ===");
    const bundlesCols = await pgQuery(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'bundles' 
        ORDER BY ordinal_position
    `);
    bundlesCols.rows.forEach(r => console.log(`  ${r.column_name}: ${r.data_type}`));

    console.log("\n=== Current listings columns ===");
    const listingsCols = await pgQuery(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'listings' 
        ORDER BY ordinal_position
    `);
    listingsCols.rows.forEach(r => console.log(`  ${r.column_name}: ${r.data_type}`));

    console.log("\n✅ Table fixes complete!");
}

fixTables()
    .then(() => process.exit(0))
    .catch(e => { console.error(e); process.exit(1); });
