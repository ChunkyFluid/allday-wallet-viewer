import { getLockedNFTIds } from './services/flow-blockchain.js';
import dotenv from 'dotenv';
dotenv.config();

const ADDRESS = '0xcfd9bad75352b43b';

async function checkLockedViaService() {
    console.log(`Checking locked moments for ${ADDRESS} using flow-blockchain service...`);
    try {
        const lockedIds = await getLockedNFTIds(ADDRESS);
        console.log(`\nLocked moments found: ${lockedIds.length}`);
        if (lockedIds.length > 0) {
            console.log(`Sample IDs: ${lockedIds.slice(0, 10).join(', ')}...`);
        } else {
            console.log(`No locked moments found on blockchain.`);
            console.log(`This confirms that Junglerules has 0 locked moments.`);
        }
    } catch (err) {
        console.error('Error:', err.message);
    }
    process.exit(0);
}

checkLockedViaService();
