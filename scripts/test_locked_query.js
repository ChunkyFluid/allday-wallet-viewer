import { pgQuery } from "../db.js";

const JUNGLE_RULES = '0xcfd9bad75352b43b';

async function testLockedQuery() {
    console.log('=== Testing getLockedNFTIds Function ===\n');

    const flowService = await import('../services/flow-blockchain.js');

    // Test with JungleRules
    console.log(`Testing with ${JUNGLE_RULES}...`);

    try {
        console.log('\n1. Getting unlocked NFTs from blockchain...');
        const unlockedIds = await flowService.getWalletNFTIds(JUNGLE_RULES);
        console.log(`   Unlocked NFTs: ${unlockedIds.length}`);

        console.log('\n2. Getting LOCKED NFTs from NFTLocker contract...');
        const lockedIds = await flowService.getLockedNFTIds(JUNGLE_RULES);
        console.log(`   Locked NFTs: ${lockedIds.length}`);

        console.log('\n3. Total from blockchain:', unlockedIds.length + lockedIds.length);
        console.log('   NFL All Day shows: 2589');

        if (lockedIds.length > 0) {
            console.log('\n4. Sample locked NFT IDs:', lockedIds.slice(0, 5));

            // Check if Trey McBride #11 is in locked list
            const TREY_MCBRIDE_11 = 10510461;
            const hasTreyMcbride = lockedIds.includes(TREY_MCBRIDE_11);
            console.log(`\n5. Trey McBride #11 (10510461) in locked list: ${hasTreyMcbride ? 'YES ✅' : 'NO ❌'}`);
        }

    } catch (err) {
        console.error('Error:', err.message);
    }

    process.exit(0);
}

testLockedQuery();
