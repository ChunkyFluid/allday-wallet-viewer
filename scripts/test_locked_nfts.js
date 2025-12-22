/**
 * Test getting locked NFTs from Flow blockchain for a single wallet
 */
import * as flowService from '../services/flow-blockchain.js';
import dotenv from 'dotenv';

dotenv.config();

// Your wallet
const testWallet = '0x7541bafd155b683e';

async function test() {
    console.log('Testing locked NFT query for wallet:', testWallet);
    console.log('');

    try {
        console.log('1. Getting unlocked NFTs from blockchain...');
        const unlockedIds = await flowService.getWalletNFTIds(testWallet);
        console.log(`   Found ${unlockedIds.length} unlocked NFTs`);

        console.log('');
        console.log('2. Getting locked NFTs from NFTLocker contract...');
        const lockedIds = await flowService.getLockedNFTIds(testWallet);
        console.log(`   Found ${lockedIds.length} locked NFTs`);

        if (lockedIds.length > 0) {
            console.log('   First 10:', lockedIds.slice(0, 10).join(', '));
        }

        console.log('');
        console.log(`TOTAL: ${unlockedIds.length + lockedIds.length} NFTs (${unlockedIds.length} unlocked, ${lockedIds.length} locked)`);

    } catch (err) {
        console.error('Error:', err.message);
    }

    process.exit(0);
}

test();
