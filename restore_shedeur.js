// Restore Shedeur Sanders #31 to wallet_holdings
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';
const SHEDEUR_ID = '10498837';

async function restore() {
    console.log('=== RESTORING SHEDEUR SANDERS #31 ===\n');

    // 1. Insert/Update in wallet_holdings
    // Use yesterday's date for last_event_ts as requested
    const yesterday = '2025-12-23 12:00:00';

    await pgQuery(`
        INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts, last_synced_at)
        VALUES ($1, $2, TRUE, $3::timestamp, NOW())
        ON CONFLICT (wallet_address, nft_id) 
        DO UPDATE SET is_locked = TRUE, last_event_ts = $3::timestamp, last_synced_at = NOW()
    `, [WALLET, SHEDEUR_ID, yesterday]);

    console.log(`✅ Restored Shedeur Sanders #31 (NFT ${SHEDEUR_ID}) to wallet_holdings`);

    // 2. Also ensure holdings table has the correct date
    await pgQuery(`
        UPDATE holdings 
        SET acquired_at = $3::timestamp
        WHERE wallet_address = $1 AND nft_id = $2
    `, [WALLET, SHEDEUR_ID, yesterday]);
    console.log(`✅ Updated date in holdings table to ${yesterday}`);

    // 3. Verification
    const res = await pgQuery(`
        SELECT h.nft_id, h.is_locked, h.last_event_ts, m.first_name, m.last_name, m.serial_number
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1 AND h.nft_id = $2
    `, [WALLET, SHEDEUR_ID]);

    console.log('\nRestoration Verification:');
    console.table(res.rows);

    const count = await pgQuery(`SELECT COUNT(*) as c FROM wallet_holdings WHERE wallet_address = $1`, [WALLET]);
    console.log(`\nFinal wallet_holdings count: ${count.rows[0].c}`);

    process.exit(0);
}
restore().catch(err => {
    console.error(err);
    process.exit(1);
});
