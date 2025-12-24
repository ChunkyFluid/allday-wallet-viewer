// Find the EXACT NFT ID for Rashid Shaheed #1096 from Edition 1228 (Series 2)
import { pgQuery } from './db.js';
import * as flowService from './services/flow-blockchain.js';

const WALLET = '0x7541bafd155b683e';

async function find() {
    console.log('=== FINDING RASHID SHAHEED #1096 EDITION 1228 ===\n');

    // The user's screenshot shows: Series 2, Dec 18, 2022, which is Edition 1228
    const meta = await pgQuery(`
        SELECT nft_id, serial_number, edition_id
        FROM nft_core_metadata_v2
        WHERE LOWER(first_name) = 'rashid' AND LOWER(last_name) = 'shaheed'
          AND serial_number = 1096
          AND edition_id = '1228'
    `);
    console.log('Rashid #1096 from Edition 1228:');
    console.table(meta.rows);

    if (meta.rows.length > 0) {
        const targetNftId = meta.rows[0].nft_id;
        console.log(`\nTarget NFT ID: ${targetNftId}`);

        // Check if user owns this on blockchain
        console.log('\nFetching blockchain data...');
        const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
        const ownsIt = blockchainNfts.map(id => id.toString()).includes(targetNftId);
        console.log(`User owns NFT ${targetNftId} on blockchain: ${ownsIt ? 'YES' : 'NO'}`);

        if (ownsIt) {
            // Add to holdings
            await pgQuery(`
                INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
                VALUES ($1, $2, false, NOW())
                ON CONFLICT (wallet_address, nft_id) DO NOTHING
            `, [WALLET, targetNftId]);
            console.log(`✅ Added NFT ${targetNftId} to holdings!`);
        } else {
            console.log('User does NOT own this NFT on blockchain.');

            // Check what Rashid NFTs user DOES own
            const rashidMeta = await pgQuery(`
                SELECT nft_id FROM nft_core_metadata_v2
                WHERE LOWER(first_name) = 'rashid' AND LOWER(last_name) = 'shaheed'
            `);
            const allRashidIds = rashidMeta.rows.map(r => r.nft_id);
            const ownedRashids = blockchainNfts.filter(id => allRashidIds.includes(id.toString()));
            console.log('\nRashid NFT IDs user owns on blockchain:', ownedRashids);

            for (const nftId of ownedRashids) {
                const info = await pgQuery(`SELECT serial_number, edition_id FROM nft_core_metadata_v2 WHERE nft_id = $1`, [nftId.toString()]);
                console.log(`  NFT ${nftId}: Serial #${info.rows[0]?.serial_number}, Edition ${info.rows[0]?.edition_id}`);
            }
        }
    }

    process.exit(0);
}
find().catch(err => { console.error(err); process.exit(1); });
