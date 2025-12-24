// scripts/run_migration.js
// Run the trades table migration to add missing columns

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function runMigration() {
    console.log("Running trades table migration...\n");

    const alterStatements = [
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS initiator_child_wallet TEXT",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS target_child_wallet TEXT",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS initiator_signed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS target_signed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS initiator_nft_ids JSONB DEFAULT '[]'",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS target_nft_ids JSONB DEFAULT '[]'"
    ];

    for (const sql of alterStatements) {
        try {
            await pgQuery(sql);
            console.log("✅", sql.substring(0, 60) + "...");
        } catch (err) {
            console.log("⚠️ ", err.message);
        }
    }

    // Show current columns
    const result = await pgQuery(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'trades' 
        ORDER BY ordinal_position
    `);

    console.log("\nCurrent trades table columns:");
    for (const row of result.rows) {
        console.log(`  ${row.column_name}: ${row.data_type}`);
    }

    console.log("\n✅ Migration complete!");
}

runMigration()
    .then(() => process.exit(0))
    .catch(e => { console.error(e); process.exit(1); });
