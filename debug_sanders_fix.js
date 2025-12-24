// Find specific NFT IDs for Shedeur Sanders #31 vs Ja'Tavion Sanders #31
import { pgQuery } from './db.js';

async function find() {
    console.log('=== SANDERS #31 SEARCH ===\n');

    const res = await pgQuery(`
        SELECT nft_id, first_name, last_name, serial_number, edition_id, set_name, tier, series_name
        FROM nft_core_metadata_v2
        WHERE LOWER(last_name) = 'sanders' 
          AND serial_number = 31 
          AND LOWER(set_name) LIKE '%regal rookies%'
    `);
    console.table(res.rows);

    // Check which ones are on blockchain
    const flowService = await import('./services/flow-blockchain.js');
    const WALLET = '0x7541bafd155b683e';
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    const blockchainSet = new Set(blockchainNfts.map(id => id.toString()));

    console.log(`\nOwnership on Blockchain for these IDs:`);
    for (const r of res.rows) {
        console.log(`${r.first_name} ${r.last_name} (${r.nft_id}): ${blockchainSet.has(r.nft_id) ? 'ALIVE ✅' : 'DEAD ❌'}`);
    }

    process.exit(0);
}
find().catch(err => {
    console.error(err);
    process.exit(1);
});
