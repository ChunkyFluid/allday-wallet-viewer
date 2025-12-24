// scripts/add_swap_tracking_columns.js
// Add columns to track per-party swap execution

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function addColumns() {
    console.log("Adding swap tracking columns to trades table...\n");

    const alters = [
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS initiator_executed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS target_executed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS initiator_tx_id TEXT",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS target_tx_id TEXT"
    ];

    for (const sql of alters) {
        try {
            await pgQuery(sql);
            console.log("✅", sql.substring(30, 80) + "...");
        } catch (e) {
            if (e.message.includes('already exists')) {
                console.log("⏭️  Column already exists");
            } else {
                console.log("❌", e.message);
            }
        }
    }

    // Verify columns
    console.log("\nVerifying trades table columns:");
    const cols = await pgQuery(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'trades' 
        ORDER BY ordinal_position
    `);
    cols.rows.forEach(r => console.log(`  ${r.column_name}: ${r.data_type}`));

    console.log("\n✅ Done!");
}

addColumns()
    .then(() => process.exit(0))
    .catch(e => { console.error(e); process.exit(1); });
