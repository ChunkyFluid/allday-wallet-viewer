// Fix dates and add missing Rashid Shaheed #1096
import { pgQuery } from './db.js';
import * as flowService from './services/flow-blockchain.js';

const WALLET = '0x7541bafd155b683e';

async function fix() {
    console.log('=== FIXING DATES AND ADDING MISSING NFT ===\n');

    // Step 1: Add Rashid Shaheed #1096 (NFT ID 4099792)
    console.log('Adding Rashid Shaheed #1096...');
    const rashidNftId = '4099792';

    // Verify from blockchain
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    if (blockchainNfts.map(id => id.toString()).includes(rashidNftId)) {
        await pgQuery(`
            INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
            VALUES ($1, $2, FALSE, '2024-12-24')
            ON CONFLICT (wallet_address, nft_id) DO UPDATE SET is_locked = FALSE
        `, [WALLET, rashidNftId]);
        console.log('✅ Added Rashid Shaheed #1096');
    } else {
        console.log('⚠️ Rashid not found on blockchain - skipping');
    }

    // Also check for any other missing NFTs
    const currentHoldings = await pgQuery(`SELECT nft_id FROM holdings WHERE wallet_address = $1`, [WALLET]);
    const currentIds = new Set(currentHoldings.rows.map(r => r.nft_id));
    const missingIds = blockchainNfts.filter(id => !currentIds.has(id.toString()));
    console.log(`Found ${missingIds.length} missing NFTs on blockchain`);

    for (const nftId of missingIds) {
        await pgQuery(`
            INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
            VALUES ($1, $2, FALSE, NOW())
            ON CONFLICT (wallet_address, nft_id) DO NOTHING
        `, [WALLET, nftId.toString()]);
    }
    if (missingIds.length > 0) console.log(`✅ Added ${missingIds.length} missing NFTs`);

    // Step 2: Fix dates showing as 12/24/2025 (which is future date - wrong!)
    // These should have real historical dates from Snowflake, but for now set to reasonable past dates
    console.log('\nFixing corrupted future dates...');
    const fixedDates = await pgQuery(`
        UPDATE holdings 
        SET acquired_at = acquired_at - INTERVAL '1 year'
        WHERE wallet_address = $1 
          AND acquired_at > NOW()
        RETURNING nft_id
    `, [WALLET]);
    console.log(`Fixed ${fixedDates.rowCount} NFTs with future dates`);

    // Step 3: Final count
    const final = await pgQuery(`
        SELECT COUNT(*) as total,
               COUNT(*) FILTER (WHERE is_locked) as locked
        FROM holdings WHERE wallet_address = $1
    `, [WALLET]);
    console.log('\nFinal wallet state:', final.rows[0]);

    process.exit(0);
}

fix().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
