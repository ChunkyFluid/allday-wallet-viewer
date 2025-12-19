/**
 * Sync a wallet's holdings from blockchain
 * This will properly set up both wallet_holdings and holdings tables
 */

import { pgQuery } from "../db.js";

const WALLET = process.argv[2] || '0xcfd9bad75352b43b'; // JungleRules

async function syncWallet() {
    console.log(`=== Syncing wallet ${WALLET} from blockchain ===\n`);

    const flowService = await import('../services/flow-blockchain.js');

    // Get UNLOCKED NFTs from blockchain
    console.log('1. Fetching unlocked NFTs from blockchain...');
    const unlockedIds = await flowService.getWalletNFTIds(WALLET);
    console.log(`   Found ${unlockedIds.length} unlocked NFTs`);

    // Get LOCKED NFTs - try blockchain first, fallback to database
    console.log('\n2. Checking for locked NFTs...');
    let lockedIds = [];

    try {
        if (flowService.getLockedNFTIds) {
            lockedIds = await flowService.getLockedNFTIds(WALLET);
        }
    } catch (err) {
        console.log(`   Blockchain locked query failed: ${err.message}`);
    }

    // If blockchain didn't return locked NFTs, check old wallet_holdings
    if (lockedIds.length === 0) {
        const lockedResult = await pgQuery(
            `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = true`,
            [WALLET]
        );
        lockedIds = lockedResult.rows.map(r => r.nft_id);
        console.log(`   Found ${lockedIds.length} locked NFTs from database`);
    } else {
        console.log(`   Found ${lockedIds.length} locked NFTs from blockchain`);
    }

    // Combine both
    const allNftIds = [
        ...unlockedIds.map(id => ({ nft_id: id.toString(), is_locked: false })),
        ...lockedIds.map(id => ({ nft_id: id.toString(), is_locked: true }))
    ];

    console.log(`\n3. Total NFTs: ${allNftIds.length} (${unlockedIds.length} unlocked + ${lockedIds.length} locked)`);

    // Clear existing wallet_holdings for this wallet
    console.log('\n4. Updating wallet_holdings...');
    await pgQuery(`DELETE FROM wallet_holdings WHERE wallet_address = $1`, [WALLET]);

    // Insert all NFTs
    if (allNftIds.length > 0) {
        const values = allNftIds.map((nft, idx) =>
            `($1, $${idx * 2 + 2}, $${idx * 2 + 3}, NOW())`
        ).join(', ');

        const params = [WALLET];
        allNftIds.forEach(nft => {
            params.push(nft.nft_id, nft.is_locked);
        });

        // Insert in batches of 500
        const BATCH_SIZE = 500;
        for (let i = 0; i < allNftIds.length; i += BATCH_SIZE) {
            const batch = allNftIds.slice(i, i + BATCH_SIZE);
            const batchValues = batch.map((_, idx) =>
                `($1, $${idx * 2 + 2}, $${idx * 2 + 3}, NOW())`
            ).join(', ');

            const batchParams = [WALLET];
            batch.forEach(nft => {
                batchParams.push(nft.nft_id, nft.is_locked);
            });

            await pgQuery(
                `INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
         VALUES ${batchValues}
         ON CONFLICT (wallet_address, nft_id) DO UPDATE SET 
           is_locked = EXCLUDED.is_locked,
           last_event_ts = NOW()`,
                batchParams
            );
        }
        console.log(`   ✅ Inserted ${allNftIds.length} NFTs`);
    }

    // Also update holdings table
    console.log('\n5. Updating holdings table...');
    await pgQuery(`DELETE FROM holdings WHERE wallet_address = $1`, [WALLET]);

    if (allNftIds.length > 0) {
        const BATCH_SIZE = 500;
        for (let i = 0; i < allNftIds.length; i += BATCH_SIZE) {
            const batch = allNftIds.slice(i, i + BATCH_SIZE);
            const batchValues = batch.map((_, idx) =>
                `($1, $${idx * 2 + 2}, $${idx * 2 + 3}, NOW())`
            ).join(', ');

            const batchParams = [WALLET];
            batch.forEach(nft => {
                batchParams.push(nft.nft_id, nft.is_locked);
            });

            await pgQuery(
                `INSERT INTO holdings (wallet_address, nft_id, is_locked, acquired_at)
         VALUES ${batchValues}
         ON CONFLICT (wallet_address, nft_id) DO UPDATE SET 
           is_locked = EXCLUDED.is_locked`,
                batchParams
            );
        }
        console.log(`   ✅ Inserted ${allNftIds.length} NFTs into holdings`);
    }

    // Verify
    console.log('\n6. Verification:');
    const finalWH = await pgQuery(
        `SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE is_locked) as locked 
     FROM wallet_holdings WHERE wallet_address = $1`,
        [WALLET]
    );
    console.log(`   wallet_holdings: ${finalWH.rows[0].total} total, ${finalWH.rows[0].locked} locked`);

    const finalH = await pgQuery(
        `SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE is_locked) as locked 
     FROM holdings WHERE wallet_address = $1`,
        [WALLET]
    );
    console.log(`   holdings: ${finalH.rows[0].total} total, ${finalH.rows[0].locked} locked`);

    // Check Trey McBride
    const treyCheck = await pgQuery(
        `SELECT * FROM wallet_holdings WHERE wallet_address = $1 AND nft_id = '10510461'`,
        [WALLET]
    );
    console.log(`\n   Trey McBride #11:`, treyCheck.rows.length > 0 ? 'FOUND ✅' : 'NOT FOUND ❌');

    console.log('\n=== SYNC COMPLETE ===');
    process.exit(0);
}

syncWallet().catch(err => {
    console.error('Sync failed:', err);
    process.exit(1);
});
