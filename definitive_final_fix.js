// DEFINITIVE FINAL FIX
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function definitiveFinalFix() {
    console.log('=== DEFINITIVE FINAL FIX ===\n');

    // 1. Remove Ja'Tavion Sanders #31 (NFT 8511045) - already done but re-verifying
    await pgQuery(`DELETE FROM wallet_holdings WHERE wallet_address = $1 AND nft_id = '8511045'`, [WALLET]);
    console.log('✅ Verified Ja\'Tavion #31 is GONE');

    // 2. Add/Update Rashid #1096 (NFT 4099792) to NOW()
    await pgQuery(`
        INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts, last_synced_at)
        VALUES ($1, '4099792', FALSE, NOW(), NOW())
        ON CONFLICT (wallet_address, nft_id) 
        DO UPDATE SET last_event_ts = NOW(), last_synced_at = NOW()
    `, [WALLET]);
    console.log('✅ Rashid #1096 (NFT 4099792) set to NOW()');

    // 3. Add/Update Joe Mixon #2504 (NFT 9872818) to NOW()
    await pgQuery(`
        INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts, last_synced_at)
        VALUES ($1, '9872818', FALSE, NOW(), NOW())
        ON CONFLICT (wallet_address, nft_id) 
        DO UPDATE SET last_event_ts = NOW(), last_synced_at = NOW()
    `, [WALLET]);
    console.log('✅ Joe Mixon #2504 (NFT 9872818) set to NOW()');

    // 4. Update Shedeur Sanders (5 moments) to Yesterday (2025-12-23)
    const sUpdate = await pgQuery(`
        UPDATE wallet_holdings h
        SET last_event_ts = '2025-12-23 12:00:00'::timestamp
        FROM nft_core_metadata_v2 m
        WHERE h.nft_id = m.nft_id
          AND h.wallet_address = $1
          AND LOWER(m.first_name) = 'shedeur'
          AND LOWER(m.last_name) = 'sanders'
    `, [WALLET]);
    console.log(`✅ Set ${sUpdate.rowCount} Shedeur Sanders moments to 12/23/2025`);

    // 5. Verification Check
    const top = await pgQuery(`
        SELECT m.first_name, m.last_name, m.serial_number, h.last_event_ts
        FROM wallet_holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
        ORDER BY h.last_event_ts DESC NULLS LAST
        LIMIT 5
    `, [WALLET]);
    console.log('\nTop 5 Newest Items:');
    console.table(top.rows);

    process.exit(0);
}

definitiveFinalFix().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
