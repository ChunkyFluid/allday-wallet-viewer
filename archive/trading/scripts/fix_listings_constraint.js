// scripts/fix_listings_constraint.js
// Add unique constraint on nft_id to fix listings creation

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function fix() {
    console.log("Fixing listings table...\n");

    // First, remove any duplicate nft_ids
    console.log("1. Removing duplicate nft_ids (keeping latest)...");
    try {
        const result = await pgQuery(`
            DELETE FROM listings 
            WHERE id NOT IN (
                SELECT MAX(id) FROM listings GROUP BY nft_id
            )
        `);
        console.log(`   Removed ${result.rowCount || 0} duplicate entries`);
    } catch (e) {
        console.log("   Error:", e.message);
    }

    // Add unique constraint
    console.log("2. Adding unique constraint on nft_id...");
    try {
        await pgQuery(`ALTER TABLE listings ADD CONSTRAINT listings_nft_id_unique UNIQUE (nft_id)`);
        console.log("   ✅ Unique constraint added");
    } catch (e) {
        if (e.message.includes('already exists')) {
            console.log("   Already exists");
        } else {
            console.log("   Error:", e.message);
        }
    }

    // Verify
    console.log("\n3. Verifying constraints...");
    const constraints = await pgQuery(`
        SELECT conname, pg_get_constraintdef(oid) as def
        FROM pg_constraint 
        WHERE conrelid = 'listings'::regclass
    `);
    constraints.rows.forEach(r => console.log(`   ${r.conname}: ${r.def}`));

    // Test insert now
    console.log("\n4. Testing insert with ON CONFLICT...");
    try {
        const result = await pgQuery(`
            INSERT INTO listings (seller_wallet, nft_id, price_usd, status)
            VALUES ('0xtest123', 'test_nft_999', 5.00, 'active')
            ON CONFLICT (nft_id) DO UPDATE SET 
                price_usd = EXCLUDED.price_usd,
                status = 'active',
                updated_at = NOW()
            RETURNING *
        `);
        console.log("   ✅ Insert successful:", result.rows[0]);

        // Clean up
        await pgQuery(`DELETE FROM listings WHERE seller_wallet = '0xtest123'`);
        console.log("   Cleaned up test data");
    } catch (e) {
        console.log("   Error:", e.message);
    }

    console.log("\n✅ Fix complete!");
}

fix()
    .then(() => process.exit(0))
    .catch(e => { console.error(e); process.exit(1); });
