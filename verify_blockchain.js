// Check what's ACTUALLY on blockchain vs what we added to database
import { pgQuery } from './db.js';
import * as flowService from './services/flow-blockchain.js';

const WALLET = '0x7541bafd155b683e';

async function verify() {
    console.log('=== VERIFYING BLOCKCHAIN VS DATABASE ===\n');

    // Get blockchain NFTs
    console.log('Fetching from blockchain...');
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    const blockchainSet = new Set(blockchainNfts.map(id => id.toString()));
    console.log(`Blockchain: ${blockchainNfts.length} NFTs`);

    // Check Rashid #1096 (NFT 4099792)
    const rashid1096 = '4099792';
    console.log(`\nRashid #1096 (NFT ${rashid1096}) on blockchain: ${blockchainSet.has(rashid1096) ? 'YES' : 'NO'}`);

    // Check what Rashid NFTs ARE on blockchain
    const allRashid = await pgQuery(`
        SELECT nft_id, serial_number FROM nft_core_metadata_v2
        WHERE LOWER(first_name) = 'rashid' AND LOWER(last_name) = 'shaheed'
    `);
    const ownedRashid = allRashid.rows.filter(r => blockchainSet.has(r.nft_id));
    console.log(`\nRashid Shaheed NFTs you ACTUALLY own on blockchain:`);
    console.table(ownedRashid);

    // Check Ja'Tavion Sanders
    const sanders = await pgQuery(`
        SELECT nft_id, serial_number FROM nft_core_metadata_v2
        WHERE LOWER(m.last_name) = 'sanders' AND serial_number = 31
    `);
    if (sanders.rows.length > 0) {
        const sandersId = sanders.rows[0].nft_id;
        console.log(`\nJa'Tavion Sanders #31 on blockchain: ${blockchainSet.has(sandersId) ? 'YES' : 'NO'}`);
    }

    // Check what's in holdings that SHOULDN'T be
    const holdings = await pgQuery(`SELECT nft_id FROM holdings WHERE wallet_address = $1`, [WALLET]);
    const holdingsSet = new Set(holdings.rows.map(r => r.nft_id));

    const shouldntHave = Array.from(holdingsSet).filter(id => !blockchainSet.has(id));
    console.log(`\n⚠️ NFTs in holdings but NOT on blockchain: ${shouldntHave.length}`);
    if (shouldntHave.length > 0 && shouldntHave.length < 20) {
        for (const nftId of shouldntHave) {
            const meta = await pgQuery(`SELECT first_name, last_name, serial_number FROM nft_core_metadata_v2 WHERE nft_id = $1`, [nftId]);
            if (meta.rows.length > 0) {
                console.log(`  - NFT ${nftId}: ${meta.rows[0].first_name} ${meta.rows[0].last_name} #${meta.rows[0].serial_number}`);
            }
        }
    }

    process.exit(0);
}

verify().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
