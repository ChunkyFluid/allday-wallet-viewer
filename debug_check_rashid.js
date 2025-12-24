import { pgQuery } from './db.js';

async function checkRashid() {
    console.log("Checking for Rashid Shaheed...");
    // Look for recent items or items matching name
    const res = await pgQuery(`
        SELECT h.nft_id, h.acquired_at, m.first_name, m.last_name, m.tier 
        FROM holdings h
        LEFT JOIN nft_core_metadata_v2 m ON h.nft_id = m.nft_id
        WHERE 
           (m.last_name ILIKE '%Shaheed%' OR m.first_name ILIKE '%Shaheed%')
           AND h.wallet_address = '0x93914b2bfb28d59d'
           AND h.acquired_at > NOW() - INTERVAL '1 DAY'
    `);

    if (res.rows.length === 0) {
        console.log("❌ Rashid Shaheed NOT found in recent holdings.");
    } else {
        console.table(res.rows);
        console.log("✅ Found him!");
    }
    process.exit();
}

checkRashid();
