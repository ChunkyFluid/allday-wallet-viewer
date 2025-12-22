// scripts/sync_sniper_names.js
import * as dotenv from "dotenv";
import fetch from "node-fetch";
import { pgQuery } from "../db.js";

dotenv.config();

const REQUEST_DELAY_MS = 200; // 5 requests per second

async function main() {
    console.log("=== Updating Sniper Listings: Backfilling Seller Addresses and Names ===");

    // 1. Ensure columns exist
    try {
        await pgQuery(`ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS seller_addr TEXT`);
        await pgQuery(`ALTER TABLE sniper_listings ADD COLUMN IF NOT EXISTS seller_name TEXT`);
        console.log("Added columns to sniper_listings");
    } catch (e) {
        console.log("Columns might already exist");
    }

    // 2. Backfill seller_addr from JSON listing_data
    console.log("Backfilling seller_addr from JSON...");
    const backfillRes = await pgQuery(`
        UPDATE sniper_listings 
        SET seller_addr = (listing_data->>'sellerAddr')
        WHERE seller_addr IS NULL 
          AND listing_data->>'sellerAddr' IS NOT NULL;
    `);
    console.log(`Updated ${backfillRes.rowCount} rows with seller_addr from JSON.`);

    // 3. Find unique wallets from sniper_listings that need names
    console.log("Finding unique wallets from sniper_listings...");
    const walletRes = await pgQuery(`
        SELECT DISTINCT seller_addr as wallet FROM sniper_listings WHERE seller_addr IS NOT NULL
        UNION
        SELECT DISTINCT buyer_address as wallet FROM sniper_listings WHERE buyer_address IS NOT NULL
    `);
    const wallets = walletRes.rows.map(r => r.wallet.toLowerCase());
    console.log(`Found ${wallets.length} unique wallets in sniper listings.`);

    // 4. Fetch missing profiles from Dapper
    console.log("Checking who needs names...");
    let processed = 0;
    let found = 0;

    for (const wallet of wallets) {
        // Check if we already have it in wallet_profiles
        const profileCheck = await pgQuery(
            `SELECT display_name FROM wallet_profiles WHERE wallet_address = $1`,
            [wallet]
        );

        let name = null;
        if (profileCheck.rows.length > 0) {
            name = profileCheck.rows[0].display_name;
        }

        // If not in DB or name is NULL, try Dapper
        if (name === null) {
            try {
                const res = await fetch(`https://open.meetdapper.com/profile?address=${wallet}`, {
                    headers: { "user-agent": "allday-wallet-viewer/1.0" }
                });
                if (res.ok) {
                    const data = await res.json();
                    name = data?.displayName || null;
                    if (name) {
                        console.log(`Found name for ${wallet}: ${name}`);
                        found++;
                    }
                }
            } catch (err) {
                console.error(`Error fetching ${wallet}:`, err.message);
            }

            // Upsert into wallet_profiles
            await pgQuery(`
                INSERT INTO wallet_profiles (wallet_address, display_name, source, last_checked)
                VALUES ($1, $2, 'dapper', now())
                ON CONFLICT (wallet_address) DO UPDATE SET
                  display_name = EXCLUDED.display_name,
                  last_checked = now()
            `, [wallet, name]);
        }

        processed++;
        if (processed % 50 === 0) console.log(`Processed ${processed}/${wallets.length}...`);

        // Short delay to be nice
        if (name === null) await new Promise(r => setTimeout(r, REQUEST_DELAY_MS));
    }

    // 5. Finally, update seller_name column in sniper_listings from wallet_profiles
    console.log("Syncing seller_name column in sniper_listings...");
    const syncRes = await pgQuery(`
        UPDATE sniper_listings sl
        SET seller_name = wp.display_name
        FROM wallet_profiles wp
        WHERE sl.seller_addr = wp.wallet_address
          AND (sl.seller_name IS NULL OR sl.seller_name != wp.display_name)
          AND wp.display_name IS NOT NULL;
    `);
    console.log(`Updated ${syncRes.rowCount} rows in sniper_listings with names.`);

    console.log("✅ Done.");
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
