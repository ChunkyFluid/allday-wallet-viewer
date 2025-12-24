import { pgQuery } from './db.js';
import { getWalletNFTIds } from './services/flow-blockchain.js';

async function findMissingMoments(address) {
    try {
        const walletAddr = address.toLowerCase();

        // 1. Get IDs from blockchain
        console.log("Fetching NFT IDs from Flow blockchain...");
        const blockchainIds = await getWalletNFTIds(walletAddr);

        // 2. Get IDs from local DB
        const localRes = await pgQuery(
            'SELECT nft_id FROM wallet_holdings WHERE wallet_address = $1',
            [walletAddr]
        );
        const localIds = new Set(localRes.rows.map(r => r.nft_id));

        const blockchainIdStrings = blockchainIds.map(id => id.toString());
        const missingInLocal = blockchainIdStrings.filter(id => !localIds.has(id));
        const extraInLocal = Array.from(localIds).filter(id => !blockchainIdStrings.includes(id));

        console.log('\n--- Discrepancy Summary ---');
        console.log(`Target Address:    ${walletAddr}`);
        console.log(`Blockchain Count:  ${blockchainIds.length}`);
        console.log(`Local DB Count:    ${localIds.size}`);
        console.log(`Missing locally:   ${missingInLocal.length}`);
        console.log(`Extra locally:     ${extraInLocal.length}`);

        if (missingInLocal.length > 0) {
            console.log(`\nIDs missing in local DB (found ${missingInLocal.length}):`);
            console.log(missingInLocal.slice(0, 20));
        }

        if (extraInLocal.length > 0) {
            console.log(`\nIDs extra in local DB (found ${extraInLocal.length}):`);
            console.log(extraInLocal.slice(0, 20));
        }

    } catch (err) {
        console.error('Error finding missing moments:', err);
    } finally {
        process.exit(0);
    }
}

const CHUNKY_ADDR = '0x7541bafd155b683e';
findMissingMoments(CHUNKY_ADDR);
