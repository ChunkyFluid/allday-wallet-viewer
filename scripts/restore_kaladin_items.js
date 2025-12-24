import { pgQuery } from '../db.js';

async function restoreItems() {
    const wallet = '0x93914b2bfb28d59d'; // Kaladin49
    console.log(`Restoring missing items for ${wallet}...`);

    const items = [
        { id: '10576026', date: '2025-12-24T10:26:43-05:00' }, // Likely Drake Maye
        { id: '2629070', date: '2025-12-09T15:23:05-05:00' }
    ];

    for (const item of items) {
        try {
            await pgQuery(`
                INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
                VALUES ($1, $2, FALSE, $3)
                ON CONFLICT (wallet_address, nft_id) 
                DO UPDATE SET acquired_at = EXCLUDED.acquired_at
            `, [wallet, item.id, item.date]);
            console.log(`✅ Restored NFT ${item.id} (Date: ${item.date})`);
        } catch (err) {
            console.error(`❌ Failed to restore ${item.id}:`, err.message);
        }
    }
    process.exit();
}

restoreItems();
