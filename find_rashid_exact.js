// Find specific NFT ID for Rashid Shaheed #1096 Base Series 2
import { pgQuery } from './db.js';

async function find() {
    const res = await pgQuery(`
        SELECT nft_id, first_name, last_name, serial_number, edition_id, set_name, tier, series_name
        FROM nft_core_metadata_v2
        WHERE LOWER(last_name) = 'shaheed' 
          AND serial_number = 1096
    `);
    console.log('Rashid #1096 variants:');
    console.table(res.rows);

    // Check which one user owns
    const WALLET = '0x7541bafd155b683e';
    import * as flowService from './services/flow-blockchain.js';
    // Use dynamic import to avoid the top-level syntax error if not in ESM (though we are)
    const fs = await import('./services/flow-blockchain.js');
    const blockchainNfts = await fs.getWalletNFTIds(WALLET);
    const blockchainSet = new Set(blockchainNfts.map(id => id.toString()));

    const owned = res.rows.filter(r => blockchainSet.has(r.nft_id));
    console.log(`\nUser owns:`);
    console.table(owned);

    process.exit(0);
}
find();
