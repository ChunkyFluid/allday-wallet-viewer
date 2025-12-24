// Final fix script with correct IDs
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function finalFinalFix() {
    console.log('=== FINAL_FINAL_FIX ===\n');

    // 1. Definitively REMOVE Ja'Tavion Sanders #31 (NFT 8511045)
    // We delete by ID and also by player name just to be safe
    const jDelete = await pgQuery(`
        DELETE FROM wallet_holdings 
        WHERE wallet_address = $1 
          AND (
            nft_id = '8511045' 
            OR nft_id IN (
              SELECT nft_id FROM nft_core_metadata_v2 
              WHERE LOWER(last_name) = 'sanders' 
                AND serial_number = 31 
                AND LOWER(set_name) LIKE '%regal rookies%'
            )
          )
    `, [WALLET]);
    console.log(`Removed Ja'Tavion Sanders: ${jDelete.rowCount} rows`);

    // 2. Fix Rashid Shaheed #1096 to TODAY (2025-12-24)
    // NFT ID 5430071
    await pgQuery(`
        UPDATE wallet_holdings 
        SET last_event_ts = '2025-12-24'::timestamp
        WHERE wallet_address = $1 AND nft_id = '5430071'
    `, [WALLET]);
    console.log('✅ Set Rashid #1096 (NFT 5430071) to 12/24/2025');

    // 3. Fix Joe Mixon #2504 to TODAY (2025-12-24)
    // NFT ID 9872818
    await pgQuery(`
        UPDATE wallet_holdings 
        SET last_event_ts = '2025-12-24'::timestamp
        WHERE wallet_address = $1 AND nft_id = '9872818'
    `, [WALLET]);
    console.log('✅ Set Joe Mixon #2504 (NFT 9872818) to 12/24/2025');

    // 4. Set ALL Shedeur Sanders to YESTERDAY (2025-12-23)
    const sUpdate = await pgQuery(`
        UPDATE wallet_holdings h
        SET last_event_ts = '2025-12-23'::timestamp
        FROM nft_core_metadata_v2 m
        WHERE h.nft_id = m.nft_id
          AND h.wallet_address = $1
          AND LOWER(m.first_name) = 'shedeur'
          AND LOWER(m.last_name) = 'sanders'
    `, [WALLET]);
    console.log(`✅ Set ${sUpdate.rowCount} Shedeur Sanders moments to 12/23/2025`);

    // 5. Verification
    const check = await pgQuery(`
        SELECT m.first_name, m.last_name, m.serial_number, h.last_event_ts
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
        ORDER BY h.last_event_ts DESC NULLS LAST
        LIMIT 5
    `, [WALLET]);
    console.log('\nVerification of Newest items:');
    console.table(check.rows);

    process.exit(0);
}

finalFinalFix().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
