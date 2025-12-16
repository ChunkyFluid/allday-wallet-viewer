// scripts/debug_listings.js
// Debug listings table and test insert

import * as dotenv from "dotenv";
import { pgQuery } from "../db.js";

dotenv.config();

async function debug() {
    console.log("=== Listings Table Debug ===\n");

    // Check table structure
    try {
        const cols = await pgQuery(`
            SELECT column_name, data_type, is_nullable, column_default 
            FROM information_schema.columns 
            WHERE table_name = 'listings' 
            ORDER BY ordinal_position
        `);
        console.log("Columns:");
        cols.rows.forEach(r => console.log(`  ${r.column_name}: ${r.data_type} (${r.is_nullable === 'YES' ? 'nullable' : 'not null'})`));
    } catch (e) {
        console.log("Error getting columns:", e.message);
    }

    // Check constraints
    try {
        const constraints = await pgQuery(`
            SELECT conname, contype, pg_get_constraintdef(oid) as def
            FROM pg_constraint 
            WHERE conrelid = 'listings'::regclass
        `);
        console.log("\nConstraints:");
        constraints.rows.forEach(r => console.log(`  ${r.conname}: ${r.def}`));
    } catch (e) {
        console.log("Error getting constraints:", e.message);
    }

    // Check current listings
    try {
        const listings = await pgQuery(`SELECT * FROM listings LIMIT 5`);
        console.log("\nSample listings:", listings.rows.length);
        listings.rows.forEach(r => console.log(`  ${r.id}: ${r.nft_id} - $${r.price_usd} - ${r.status}`));
    } catch (e) {
        console.log("Error getting listings:", e.message);
    }

    // Test insert
    console.log("\n=== Test Insert ===");
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
        console.log("Insert result:", result.rows);

        // Clean up test
        await pgQuery(`DELETE FROM listings WHERE seller_wallet = '0xtest123'`);
        console.log("Test insert successful and cleaned up!");
    } catch (e) {
        console.log("Insert error:", e.message);
    }

    console.log("\n✅ Debug complete");
}

debug()
    .then(() => process.exit(0))
    .catch(e => { console.error(e); process.exit(1); });
