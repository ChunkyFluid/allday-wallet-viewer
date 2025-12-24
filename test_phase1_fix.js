// Test the Phase 1 fix: Force refresh a wallet and check for Rashid Shaheed
import { pgQuery } from './db.js';
import * as flowService from './services/flow-blockchain.js';

const WALLET = '0x7541bafd155b683e';

async function test() {
    console.log('=== TESTING PHASE 1 FIX ===\n');

    // Step 1: Get current count
    const beforeCount = await pgQuery(`SELECT COUNT(*) as c FROM holdings WHERE wallet_address = $1`, [WALLET]);
    console.log(`BEFORE: ${beforeCount.rows[0].c} NFTs in holdings`);

    // Step 2: Fetch from blockchain (same as the fixed /api/query does)
    console.log('\nFetching from Flow blockchain...');
    const nftIds = await flowService.getWalletNFTIds(WALLET);
    console.log(`Blockchain returned ${nftIds.length} NFTs`);

    // Step 3: Find new NFTs to add
    const currentResult = await pgQuery(`SELECT nft_id FROM holdings WHERE wallet_address = $1`, [WALLET]);
    const currentNftIds = new Set(currentResult.rows.map(r => r.nft_id));

    const toAdd = nftIds.filter(id => !currentNftIds.has(id.toString()));
    console.log(`\nNEW NFTs to add: ${toAdd.length}`);
    if (toAdd.length > 0) {
        console.log('NFT IDs:', toAdd.slice(0, 10).join(', '), toAdd.length > 10 ? '...' : '');
    }

    // Step 4: Add them
    if (toAdd.length > 0) {
        const now = new Date();
        for (const nftId of toAdd) {
            await pgQuery(
                `INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (wallet_address, nft_id) DO NOTHING`,
                [WALLET, nftId.toString(), false, now]
            );
        }
        console.log(`\n✅ Added ${toAdd.length} new NFTs to holdings`);
    }

    // Step 5: Check for Rashid Shaheed now
    const rashid = await pgQuery(`
        SELECT h.nft_id, m.first_name, m.last_name, m.serial_number
        FROM holdings h
        JOIN nft_core_metadata_v2 m ON m.nft_id = h.nft_id
        WHERE h.wallet_address = $1
          AND LOWER(m.first_name) = 'rashid' AND LOWER(m.last_name) = 'shaheed'
    `, [WALLET]);

    console.log('\n=== RASHID SHAHEED CHECK ===');
    if (rashid.rows.length > 0) {
        console.log('✅ SUCCESS! Rashid Shaheed found:');
        console.table(rashid.rows);
    } else {
        console.log('❌ Rashid Shaheed still not found.');
        console.log('This likely means the metadata is missing.');

        // Check if any of the new NFTs match Rashid pattern
        if (toAdd.length > 0) {
            console.log('\nChecking if new NFTs have Rashid metadata...');
            const newNftIdStrings = toAdd.map(id => id.toString());
            const metaCheck = await pgQuery(`
                SELECT nft_id, first_name, last_name, serial_number
                FROM nft_core_metadata_v2
                WHERE nft_id = ANY($1::text[])
                  AND LOWER(first_name) = 'rashid'
            `, [newNftIdStrings]);
            console.log('New NFTs with Rashid metadata:', metaCheck.rows);
        }
    }

    // Final count
    const afterCount = await pgQuery(`SELECT COUNT(*) as c FROM holdings WHERE wallet_address = $1`, [WALLET]);
    console.log(`\nAFTER: ${afterCount.rows[0].c} NFTs in holdings`);

    process.exit(0);
}

test().catch(err => {
    console.error('Test failed:', err);
    process.exit(1);
});
