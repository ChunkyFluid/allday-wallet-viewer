// Check what Joe Mixon NFTs the user actually owns
import { pgQuery } from './db.js';
import * as flowService from './services/flow-blockchain.js';

const WALLET = '0x7541bafd155b683e';

async function check() {
    console.log('=== CHECKING JOE MIXON OWNERSHIP ===\n');

    // Get blockchain NFTs
    const blockchainNfts = await flowService.getWalletNFTIds(WALLET);
    const blockchainSet = new Set(blockchainNfts.map(id => id.toString()));

    // Find ALL Joe Mixon NFTs in metadata
    const allMixon = await pgQuery(`
        SELECT nft_id, serial_number, edition_id
        FROM nft_core_metadata_v2
        WHERE LOWER(first_name) = 'joe' AND LOWER(last_name) = 'mixon'
        ORDER BY serial_number
    `);

    console.log(`Total Joe Mixon NFTs in metadata: ${allMixon.rowCount}`);

    // Check which ones user owns on blockchain
    const ownedMixon = allMixon.rows.filter(r => blockchainSet.has(r.nft_id));
    console.log(`\nJoe Mixon NFTs you own on blockchain: ${ownedMixon.length}`);
    console.table(ownedMixon);

    // Check which are in holdings table
    const inHoldings = await pgQuery(`
        SELECT h.nft_id, m.serial_number, h.is_locked
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'joe' AND LOWER(m.last_name) = 'mixon'
    `, [WALLET]);
    console.log(`\nJoe Mixon in holdings table: ${inHoldings.rowCount}`);
    console.table(inHoldings.rows);

    // Find missing ones
    const holdingsSet = new Set(inHoldings.rows.map(r => r.nft_id));
    const missing = ownedMixon.filter(m => !holdingsSet.has(m.nft_id));
    if (missing.length > 0) {
        console.log(`\n⚠️ MISSING from holdings: ${missing.length}`);
        console.table(missing);
    }

    process.exit(0);
}

check().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
