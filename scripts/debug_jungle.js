/**
 * Investigate the 62 moment discrepancy between our count (2651) and NFL All Day (2589)
 * The difference is likely stale locked NFTs that were unlocked and transferred
 */

import { pgQuery } from "../db.js";

const JUNGLE_RULES = '0xcfd9bad75352b43b';

async function investigate() {
    console.log('=== Investigating JungleRules Discrepancy ===\n');
    console.log('Our count: 2651 (1522 unlocked + 1129 locked)');
    console.log('NFL All Day: 2589');
    console.log('Difference: 62 moments\n');

    const flowService = await import('../services/flow-blockchain.js');

    // Get current blockchain unlocked count
    const unlockedIds = await flowService.getWalletNFTIds(JUNGLE_RULES);
    console.log('1. Blockchain unlocked count:', unlockedIds.length);

    // Get our locked NFT IDs
    const lockedResult = await pgQuery(
        `SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1 AND is_locked = true`,
        [JUNGLE_RULES]
    );
    console.log('2. Our locked count:', lockedResult.rows.length);

    // Check if any of our "locked" NFTs are now unlocked (in someone's wallet on blockchain)
    // This would mean they were unlocked and transferred since our snapshot
    console.log('\n3. Checking if "locked" NFTs are actually on blockchain (stale locks)...');

    const staleLockedNfts = [];
    const unlockedSet = new Set(unlockedIds.map(id => id.toString()));

    let checked = 0;
    for (const row of lockedResult.rows) {
        // If this "locked" NFT is in the user's unlocked wallet, it's not actually locked
        if (unlockedSet.has(row.nft_id)) {
            staleLockedNfts.push(row.nft_id);
        }
        checked++;
    }

    console.log(`   Found ${staleLockedNfts.length} NFTs marked "locked" but actually unlocked in wallet`);

    // The remaining ~62 locked NFTs that are missing from NFL All Day are likely:
    // 1. Unlocked and transferred to OTHER wallets (no longer JungleRules')
    // 2. Or burned

    // Let's check if these locked NFTs exist on NFTLocker contract
    // We can't directly check NFTLocker without the function, so let's estimate

    console.log('\n4. Summary:');
    console.log('   - Blockchain unlocked:', unlockedIds.length);
    console.log('   - Our locked count:', lockedResult.rows.length);
    console.log('   - Stale locks (actually unlocked):', staleLockedNfts.length);
    console.log('   - True locked (estimated):', lockedResult.rows.length - staleLockedNfts.length);
    console.log('   - Expected total:', unlockedIds.length + (lockedResult.rows.length - staleLockedNfts.length));

    // The issue: Our locked data is from the last Snowflake sync
    // Some of these locked NFTs may have been:
    // - Unlocked and kept
    // - Unlocked and transferred
    // - Unlocked and sold
    // We need to verify each locked NFT is still actually locked

    console.log('\n5. The ~62 missing moments are likely locked NFTs that have been:');
    console.log('   - Unlocked and transferred to other wallets');
    console.log('   - These are still in our database as "locked" from old Snowflake sync');
    console.log('   - But NFL All Day shows current state from blockchain');

    process.exit(0);
}

investigate();
