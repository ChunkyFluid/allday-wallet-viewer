// Simple check: what Rashid NFTs does user actually own on blockchain
import * as flowService from './services/flow-blockchain.js';
import { pgQuery } from './db.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    // Get blockchain NFTs
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    const blockchainSet = new Set(blockchainNfts.map(id => id.toString()));

    // Check specific NFT IDs
    console.log('Rashid #1096 (NFT 4099792) on blockchain:', blockchainSet.has('4099792') ? 'YES ✅' : 'NO ❌');
    console.log('Joe Mixon #2504 (NFT 9872818) on blockchain:', blockchainSet.has('9872818') ? 'YES ✅' : 'NO ❌');

    // Get all Rashid NFTs from metadata
    const allRashid = await pgQuery(`
        SELECT nft_id, serial_number 
        FROM nft_core_metadata_v2
        WHERE LOWER(first_name) = 'rashid' AND LOWER(last_name) = 'shaheed'
    `);

    // Filter to ones user owns
    const ownedRashid = allRashid.rows.filter(r => blockchainSet.has(r.nft_id));
    console.log(`\nRashid Shaheed NFTs you own (${ownedRashid.length} total):`);
    ownedRashid.forEach(r => console.log(`  - Serial #${r.serial_number} (NFT ${r.nft_id})`));

    process.exit(0);
}

check().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
