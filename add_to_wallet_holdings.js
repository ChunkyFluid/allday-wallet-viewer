// Add NFTs to wallet_holdings (the table the frontend actually uses!)
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function addToWalletHoldings() {
    console.log('=== ADDING TO WALLET_HOLDINGS (FRONTEND TABLE) ===\n');

    // Add Rashid #1096
    await pgQuery(`
        INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at, last_synced_at)
        VALUES ($1, '4099792', FALSE, CURRENT_TIMESTAMP, NOW())
        ON CONFLICT (wallet_address, nft_id) 
        DO UPDATE SET acquired_at = CURRENT_TIMESTAMP, last_synced_at = NOW()
    `, [WALLET]);
    console.log('✅ Added Rashid Shaheed #1096 to holdings');

    // Add Joe Mixon #2504  
    await pgQuery(`
        INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at, last_synced_at)
        VALUES ($1, '9872818', FALSE, CURRENT_TIMESTAMP, NOW())
        ON CONFLICT (wallet_address, nft_id)
        DO UPDATE SET acquired_at = CURRENT_TIMESTAMP, last_synced_at = NOW()
    `, [WALLET]);
    console.log('✅ Added Joe Mixon #2504 to holdings');

    // Verify
    const count = await pgQuery(`SELECT COUNT(*) as c FROM holdings WHERE wallet_address = $1`, [WALLET]);
    console.log(`\nTotal in holdings: ${count.rows[0].c}`);

    // Check they're there
    const check = await pgQuery(`
        SELECT m.first_name, m.last_name, m.serial_number
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND h.nft_id IN ('4099792', '9872818')
    `, [WALLET]);
    console.log('\nVerification:');
    console.table(check.rows);

    process.exit(0);
}

addToWalletHoldings().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
