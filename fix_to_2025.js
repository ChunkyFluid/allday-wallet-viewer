// Fix dates to 2025 (not 2024!)
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function fix2025() {
    console.log('=== FIXING TO 2025 DATES ===\n');

    // Set Rashid #1096 to TODAY 2025
    await pgQuery(`
        UPDATE wallet_holdings 
        SET last_event_ts = '2025-12-24'::timestamp
        WHERE wallet_address = $1 AND nft_id = '4099792'
    `, [WALLET]);
    console.log('✅ Set Rashid #1096 to 2025-12-24');

    // Set Joe Mixon #2504 to TODAY 2025
    await pgQuery(`
        UPDATE wallet_holdings 
        SET last_event_ts = '2025-12-24'::timestamp
        WHERE wallet_address = $1 AND nft_id = '9872818'
    `, [WALLET]);
    console.log('✅ Set Joe Mixon #2504 to 2025-12-24');

    // Set all Shedeur Sanders to YESTERDAY 2025
    const shedeurIds = await pgQuery(`
        SELECT h.nft_id FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'shedeur'
          AND LOWER(m.last_name) = 'sanders'
    `, [WALLET]);

    for (const row of shedeurIds.rows) {
        await pgQuery(`
            UPDATE wallet_holdings 
            SET last_event_ts = '2025-12-23'::timestamp
            WHERE wallet_address = $1 AND nft_id = $2
        `, [WALLET, row.nft_id]);
    }
    console.log(`✅ Set ${shedeurIds.rowCount} Shedeur Sanders to 2025-12-23`);

    console.log('\n✅ All dates corrected to 2025!');
    process.exit(0);
}

fix2025();
