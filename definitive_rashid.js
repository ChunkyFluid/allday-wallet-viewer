// Definitive search for Rashid Shaheed #1096
import { pgQuery } from './db.js';

async function definitive() {
    console.log('=== DEFINITIVE RASHID SEARCH ===\n');

    const res = await pgQuery(`
        SELECT nft_id, first_name, last_name, serial_number, edition_id, set_name, tier
        FROM nft_core_metadata_v2
        WHERE LOWER(last_name) = 'shaheed' AND serial_number = 1096
    `);
    console.table(res.rows);

    // Also check what Rashid IDs the user owns on blockchain
    import * as flowService from './services/flow-blockchain.js';
    const WALLET = '0x7541bafd155b683e';
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    const blockchainSet = new Set(blockchainNfts.map(id => id.toString()));

    console.log(`\nUser owns these Rashid IDs on blockchain:`);
    const owned = res.rows.filter(r => blockchainSet.has(r.nft_id));
    console.table(owned);

    process.exit(0);
}
definitive();
