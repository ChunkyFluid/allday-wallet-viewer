import { pgQuery } from './db.js';
import { getWalletNFTIds } from './services/flow-blockchain.js';
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';

async function investigateLockedUsage() {
    console.log(`Fetching 1131 'locked' IDs from local DB...`);
    const res = await pgQuery(`
        SELECT nft_id FROM wallet_holdings 
        WHERE wallet_address = $1 AND is_locked = true
    `, [ADDRESS]);
    const localLockedIds = res.rows.map(r => r.nft_id);
    console.log(`Found ${localLockedIds.length} locally locked IDs.`);

    console.log(`Fetching actual UNLOCKED IDs from Blockchain...`);
    const chainUnlockedIds = await getWalletNFTIds(ADDRESS);
    const chainUnlockedSet = new Set(chainUnlockedIds.map(String));
    console.log(`Found ${chainUnlockedIds.length} unlocked IDs on chain.`);

    const foundInUnlocked = [];
    const missingFromChain = [];

    for (const id of localLockedIds) {
        if (chainUnlockedSet.has(String(id))) {
            foundInUnlocked.push(id);
        } else {
            missingFromChain.push(id);
        }
    }

    console.log(`\nResults:`);
    console.log(`- ${foundInUnlocked.length} IDs are marked LOCKED in DB but are UNLOCKED on chain.`);
    console.log(`  -> ACTION: Update is_locked = false for these.`);
    console.log(`- ${missingFromChain.length} IDs are missing from chain entirely (Ghost candidates).`);
    console.log(`  -> ACTION: Delete these.`);

    if (missingFromChain.length === 41) {
        console.log(`\nSUCCESS: Found exactly 41 missing moments! This explains the discrepancy.`);
    }

    console.log(`\nSample Missing IDs:`, missingFromChain.slice(0, 10));

    // Also check if any un-locked ghosts (from previous step) are still pending deletion
    // But user asked about the 41 diff.

    process.exit(0);
}

investigateLockedUsage();
