// Fix dates: Set TODAY for Rashid/Mixon, fix Ja'Tavion Sanders
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function fixDates() {
    console.log('=== FIXING ACQUISITION DATES ===\n');

    // 1. Set Rashid Shaheed #1096 to TODAY
    await pgQuery(`
        UPDATE holdings h
        SET acquired_at = CURRENT_DATE
        FROM nft_core_metadata_v2 m
        WHERE h.nft_id = m.nft_id
          AND h.wallet_address = $1
          AND LOWER(m.first_name) = 'rashid' 
          AND LOWER(m.last_name) = 'shaheed'
          AND m.serial_number = 1096
    `, [WALLET]);
    console.log('✅ Set Rashid Shaheed #1096 to today');

    // 2. Set Joe Mixon #2504 to TODAY
    await pgQuery(`
        UPDATE holdings h
        SET acquired_at = CURRENT_DATE
        FROM nft_core_metadata_v2 m
        WHERE h.nft_id = m.nft_id
          AND h.wallet_address = $1
          AND LOWER(m.first_name) = 'joe' 
          AND LOWER(m.last_name) = 'mixon'
          AND m.serial_number = 2504
    `, [WALLET]);
    console.log('✅ Set Joe Mixon #2504 to today');

    // 3. Fix Ja'Tavion Sanders - find and fix
    const sanders = await pgQuery(`
        SELECT h.nft_id, m.first_name, m.last_name, m.serial_number
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.last_name) = 'sanders'
          AND m.serial_number = 31
    `, [WALLET]);

    if (sanders.rows.length > 0) {
        const nftId = sanders.rows[0].nft_id;
        await pgQuery(`
            UPDATE holdings 
            SET acquired_at = '2024-12-01'
            WHERE wallet_address = $1 AND nft_id = $2
        `, [WALLET, nftId]);
        console.log(`✅ Set ${sanders.rows[0].first_name} Sanders #31 to past date (12/01/2024)`);
    }

    // 4. Verify - show newest 5
    const newest = await pgQuery(`
        SELECT m.first_name, m.last_name, m.serial_number, h.acquired_at
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
        ORDER BY h.acquired_at DESC NULLS LAST
        LIMIT 5
    `, [WALLET]);

    console.log('\nNewest 5 NFTs by acquisition date:');
    console.table(newest.rows);

    process.exit(0);
}

fixDates().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
