// Search for ANY NFT with serial 31 owned by the user
import { pgQuery } from './db.js';

async function find() {
    console.log('=== GLOBAL SERIAL #31 OWNERSHIP SEARCH ===\n');

    // Get ALL blockchain IDs for this wallet
    const flowService = await import('./services/flow-blockchain.js');
    const WALLET = '0x7541bafd155b683e';
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    console.log(`Blockchain has ${blockchainNfts.length} NFTs`);
    const idList = blockchainNfts.map(id => `'${id}'`).join(',');

    if (blockchainNfts.length === 0) {
        console.log('No NFTs found on blockchain for this wallet.');
        return;
    }

    // Find any of THESE IDs that have serial number 31
    const res = await pgQuery(`
        SELECT nft_id, first_name, last_name, serial_number, edition_id, set_name, tier, series_name
        FROM nft_core_metadata_v2
        WHERE nft_id IN (${idList})
          AND serial_number = 31
    `);

    console.log(`\nOwned NFTs with serial #31:`);
    console.table(res.rows);

    process.exit(0);
}
find().catch(err => {
    console.error(err);
    process.exit(1);
});
