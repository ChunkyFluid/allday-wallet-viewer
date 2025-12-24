// Find and add the CORRECT Rashid Shaheed #1096 NFT
import { pgQuery } from './db.js';
import * as flowService from './services/flow-blockchain.js';

const WALLET = '0x7541bafd155b683e';

async function fix() {
    console.log('=== FIXING RASHID SHAHEED #1096 ===\n');

    // Step 1: Find the NFT ID for Rashid Shaheed #1096 from Edition 1228 (Series 2, Dec 2022)
    const rashidMeta = await pgQuery(`
        SELECT nft_id, serial_number, edition_id, first_name, last_name
        FROM nft_core_metadata_v2
        WHERE LOWER(first_name) = 'rashid' AND LOWER(last_name) = 'shaheed'
          AND serial_number = 1096
        ORDER BY edition_id
    `);
    console.log('Rashid Shaheed #1096 NFTs in metadata:');
    console.table(rashidMeta.rows);

    // Step 2: Fetch user's actual NFTs from blockchain
    console.log('\nFetching user wallet from blockchain...');
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    console.log(`Blockchain returned ${blockchainNfts.length} NFTs`);

    // Step 3: Find which Rashid #1096 the user owns
    const rashidNftIds = rashidMeta.rows.map(r => r.nft_id);
    const ownedRashid = blockchainNfts.filter(id => rashidNftIds.includes(id.toString()));
    console.log('\nUser owns these Rashid #1096 NFT IDs:', ownedRashid);

    // Step 4: Add them to holdings if not already there
    for (const nftId of ownedRashid) {
        const existing = await pgQuery(`SELECT 1 FROM holdings WHERE wallet_address = $1 AND nft_id = $2`, [WALLET, nftId.toString()]);
        if (existing.rowCount === 0) {
            await pgQuery(`
                INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
                VALUES ($1, $2, false, NOW())
            `, [WALLET, nftId.toString()]);
            console.log(`✅ Added NFT ${nftId} to holdings!`);
        } else {
            console.log(`NFT ${nftId} already in holdings`);
        }
    }

    // Step 5: Verify
    console.log('\n=== VERIFICATION ===');
    const final = await pgQuery(`
        SELECT h.nft_id, m.first_name, m.last_name, m.serial_number
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'rashid' AND LOWER(m.last_name) = 'shaheed'
    `, [WALLET]);
    console.log('Rashid Shaheed NFTs now owned by user:');
    console.table(final.rows);

    process.exit(0);
}
fix().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
