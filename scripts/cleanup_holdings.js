/**
 * Cleanup script to fix locked NFT tracking and remove stale data
 * OPTIMIZED: Uses batch SQL operations instead of one-by-one iteration
 * 
 * Problem 1: wallet_holdings shows NFTLocker contract as owner of locked NFTs
 * Problem 2: Stale NFTs remain in database after transfers
 */

import { pgQuery } from "../db.js";

const NFTLOCKER_CONTRACT = '0xb6f2481eba4df97b';

async function cleanup() {
    console.log('=== NFT Holdings Cleanup Script (Optimized) ===\n');

    // Step 1: Find how many NFTs are owned by NFTLocker
    console.log('1. Checking NFTs incorrectly attributed to NFTLocker contract...');
    const lockerCount = await pgQuery(
        `SELECT COUNT(*) as count FROM wallet_holdings WHERE wallet_address = $1`,
        [NFTLOCKER_CONTRACT]
    );
    console.log(`   Found ${lockerCount.rows[0].count} NFTs owned by NFTLocker contract`);

    // Step 2: Use batch SQL to reassign locked NFTs
    // The holdings table should have the correct owner (since we didn't update it with NFTLocker)
    console.log('\n2. Reassigning locked NFTs to original owners (batch operation)...');

    // For each NFT owned by NFTLocker in wallet_holdings, 
    // look up the owner in holdings table and insert/update wallet_holdings
    const reassignResult = await pgQuery(`
    WITH locked_nfts AS (
      -- Get all NFTs currently "owned" by NFTLocker in wallet_holdings
      SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1
    ),
    true_owners AS (
      -- Find the true owner from the holdings table
      SELECT h.wallet_address, h.nft_id, h.acquired_at
      FROM holdings h
      JOIN locked_nfts ln ON ln.nft_id = h.nft_id
      WHERE h.wallet_address != $1
    )
    -- Insert these with is_locked = true
    INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
    SELECT wallet_address, nft_id, TRUE, COALESCE(acquired_at, NOW())
    FROM true_owners
    ON CONFLICT (wallet_address, nft_id) DO UPDATE SET is_locked = TRUE
    RETURNING wallet_address, nft_id
  `, [NFTLOCKER_CONTRACT]);

    console.log(`   ✅ Reassigned ${reassignResult.rowCount} NFTs to their original owners`);

    // Step 3: Delete the NFTLocker entries
    console.log('\n3. Removing NFTLocker entries from wallet_holdings...');
    const deleteResult = await pgQuery(
        `DELETE FROM wallet_holdings WHERE wallet_address = $1 RETURNING nft_id`,
        [NFTLOCKER_CONTRACT]
    );
    console.log(`   ✅ Deleted ${deleteResult.rowCount} NFTLocker entries`);

    // Step 4: For specific wallet cleanup (JungleRules)
    const JUNGLE_RULES = '0xcfd9bad75352b43b';
    console.log(`\n4. Specific cleanup for JungleRules (${JUNGLE_RULES})...`);

    const flowService = await import('../services/flow-blockchain.js');

    try {
        // Get blockchain NFTs
        const blockchainIds = await flowService.getWalletNFTIds(JUNGLE_RULES);
        console.log(`   Blockchain unlocked NFTs: ${blockchainIds.length}`);

        // Get locked NFTs from holdings
        const lockedResult = await pgQuery(
            `SELECT nft_id FROM holdings WHERE wallet_address = $1 AND is_locked = true`,
            [JUNGLE_RULES]
        );
        console.log(`   Locked NFTs in holdings: ${lockedResult.rows.length}`);

        // Build set of valid NFTs
        const validNfts = new Set([
            ...blockchainIds.map(id => id.toString()),
            ...lockedResult.rows.map(r => r.nft_id)
        ]);

        // Get current wallet_holdings
        const whResult = await pgQuery(
            `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1`,
            [JUNGLE_RULES]
        );
        console.log(`   Current wallet_holdings: ${whResult.rows.length}`);

        // Find stale NFTs
        const staleNfts = whResult.rows.filter(r => !validNfts.has(r.nft_id));
        console.log(`   Stale NFTs to remove: ${staleNfts.length}`);

        if (staleNfts.length > 0) {
            const staleIds = staleNfts.map(r => r.nft_id);
            await pgQuery(
                `DELETE FROM wallet_holdings WHERE wallet_address = $1 AND nft_id = ANY($2)`,
                [JUNGLE_RULES, staleIds]
            );
            console.log(`   ✅ Removed ${staleNfts.length} stale NFTs`);
        }

        // Add any missing locked NFTs to wallet_holdings
        const lockedToAdd = lockedResult.rows.filter(r => {
            const inWH = whResult.rows.some(w => w.nft_id === r.nft_id);
            return !inWH;
        });

        if (lockedToAdd.length > 0) {
            console.log(`   Adding ${lockedToAdd.length} missing locked NFTs to wallet_holdings...`);
            for (const row of lockedToAdd) {
                await pgQuery(
                    `INSERT INTO wallet_holdings (wallet_address, nft_id, is_locked, last_event_ts)
           VALUES ($1, $2, TRUE, NOW())
           ON CONFLICT (wallet_address, nft_id) DO UPDATE SET is_locked = TRUE`,
                    [JUNGLE_RULES, row.nft_id]
                );
            }
            console.log(`   ✅ Added ${lockedToAdd.length} locked NFTs`);
        }

        // Final count
        const finalCount = await pgQuery(
            `SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE is_locked) as locked 
       FROM wallet_holdings WHERE wallet_address = $1`,
            [JUNGLE_RULES]
        );
        console.log(`\n   JungleRules final: ${finalCount.rows[0].total} total (${finalCount.rows[0].locked} locked)`);

    } catch (err) {
        console.error(`   Error: ${err.message}`);
    }

    console.log('\n=== CLEANUP COMPLETE ===');
    process.exit(0);
}

cleanup().catch(err => {
    console.error('Cleanup failed:', err);
    process.exit(1);
});
