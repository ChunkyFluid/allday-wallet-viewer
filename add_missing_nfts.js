// Add both missing NFTs: Rashid Shaheed #1096 and Joe Mixon #2504
import { pgQuery } from './db.js';
import * as flowService from './services/flow-blockchain.js';

const WALLET = '0x7541bafd155b683e';

async function addMissing() {
    console.log('=== ADDING MISSING NFTS ===\n');

    // Get blockchain NFTs
    console.log('Fetching wallet from blockchain...');
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    console.log(`Blockchain has ${blockchainNfts.length} NFTs`);

    // Find Rashid Shaheed #1096 (Edition 1228)
    const rashidResult = await pgQuery(`
        SELECT nft_id FROM nft_core_metadata_v2
        WHERE LOWER(first_name) = 'rashid' AND LOWER(last_name) = 'shaheed'
          AND serial_number = 1096 AND edition_id = '1228'
    `);

    // Find Joe Mixon #2504
    const mixonResult = await pgQuery(`
        SELECT nft_id FROM nft_core_metadata_v2
        WHERE LOWER(first_name) = 'joe' AND LOWER(last_name) = 'mixon'
          AND serial_number = 2504
    `);

    const toAdd = [];

    if (rashidResult.rows.length > 0) {
        const nftId = rashidResult.rows[0].nft_id;
        if (blockchainNfts.map(id => id.toString()).includes(nftId)) {
            toAdd.push({ name: 'Rashid Shaheed #1096', nftId });
        } else {
            console.log('⚠️ Rashid #1096 NOT on blockchain');
        }
    }

    if (mixonResult.rows.length > 0) {
        const nftId = mixonResult.rows[0].nft_id;
        if (blockchainNfts.map(id => id.toString()).includes(nftId)) {
            toAdd.push({ name: 'Joe Mixon #2504', nftId });
        } else {
            console.log('⚠️ Joe Mixon #2504 NOT on blockchain');
        }
    }

    console.log(`\nAdding ${toAdd.length} NFTs...`);

    for (const nft of toAdd) {
        await pgQuery(`
            INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
            VALUES ($1, $2, FALSE, '2024-12-24')
            ON CONFLICT (wallet_address, nft_id) 
            DO UPDATE SET is_locked = EXCLUDED.is_locked
        `, [WALLET, nft.nftId]);
        console.log(`✅ Added ${nft.name} (NFT ${nft.nftId})`);
    }

    // Verify
    const final = await pgQuery(`
        SELECT COUNT(*) as total FROM holdings WHERE wallet_address = $1
    `, [WALLET]);
    console.log(`\nFinal count: ${final.rows[0].total} NFTs`);

    process.exit(0);
}

addMissing().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
