import { pgQuery } from '../db.js';

async function debugPhantomMoment() {
    const userWallet = '0x7541bafd155b683e';

    // Find the Troy Polamalu moment
    console.log('\n=== Finding Troy Polamalu 1080/9000 ===');

    const polamalu = await pgQuery(`
        SELECT h.nft_id, h.wallet_address, h.is_locked, h.last_event_ts, h.last_synced_at,
               m.first_name, m.last_name, m.serial_number, m.max_mint_size, m.tier
        FROM wallet_holdings h
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE m.last_name ILIKE 'Polamalu' 
          AND m.serial_number = 1080
    `);
    console.log('Polamalu moments in ALL wallets:', polamalu.rows);

    // Check if this NFT is in user's wallet_holdings
    const inUserWallet = await pgQuery(`
        SELECT h.nft_id, h.wallet_address, h.is_locked, h.last_synced_at
        FROM wallet_holdings h
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND m.last_name ILIKE 'Polamalu' 
          AND m.serial_number = 1080
    `, [userWallet]);
    console.log('\nPolamalu in USER wallet:', inUserWallet.rows);

    // Check the holdings table too
    const holdings = await pgQuery(`
        SELECT h.nft_id, h.wallet_address, h.is_locked
        FROM holdings h
        LEFT JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE m.last_name ILIKE 'Polamalu' 
          AND m.serial_number = 1080
    `);
    console.log('\nPolamalu in holdings table:', holdings.rows);

    // Check how many locked moments are in user's wallet_holdings
    const lockedCount = await pgQuery(`
        SELECT COUNT(*) as count FROM wallet_holdings 
        WHERE wallet_address = $1 AND is_locked = true
    `, [userWallet]);
    console.log('\nTotal locked in user wallet_holdings:', lockedCount.rows[0]);

    // Look for any duplicate NFT IDs across wallets
    const duplicates = await pgQuery(`
        SELECT nft_id, COUNT(*) as wallet_count, 
               ARRAY_AGG(wallet_address) as wallets,
               ARRAY_AGG(is_locked::text) as locked_status
        FROM wallet_holdings
        WHERE nft_id IN (
            SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1
        )
        GROUP BY nft_id
        HAVING COUNT(*) > 1
        LIMIT 10
    `, [userWallet]);
    console.log('\nNFTs in multiple wallets (duplicates):', duplicates.rows);

    process.exit(0);
}

debugPhantomMoment().catch(e => { console.error(e); process.exit(1); });
