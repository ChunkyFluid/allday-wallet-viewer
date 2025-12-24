// Search for ANY NFT with serial 31 owned by the user - VERIFY RESULT COUNT
import { pgQuery } from './db.js';

async function find() {
    console.log('=== GLOBAL SERIAL #31 OWNERSHIP SEARCH ===\n');

    // Get ALL blockchain IDs for this wallet
    const flowService = await import('./services/flow-blockchain.js');
    const WALLET = '0x7541bafd155b683e';
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    console.log(`Blockchain has ${blockchainNfts.length} IDs`);

    if (blockchainNfts.length === 0) {
        console.log('No NFTs found on blockchain.');
        return;
    }

    const idList = blockchainNfts.map(id => `'${id}'`).join(',');

    // Find any of THESE IDs that have serial number 31
    const res = await pgQuery(`
        SELECT nft_id, first_name, last_name, serial_number, set_name, tier
        FROM nft_core_metadata_v2
        WHERE nft_id IN (${idList})
          AND serial_number = 31
    `);

    console.log(`\nFound ${res.rowCount} owned NFTs with serial #31:`);
    if (res.rowCount > 0) {
        console.table(res.rows);
    } else {
        console.log('No owned NFTs with serial #31 found on blockchain.');
    }

    process.exit(0);
}
find().catch(err => {
    console.error(err);
    process.exit(1);
});
