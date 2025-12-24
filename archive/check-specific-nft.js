import { getNFTFullDetails } from './services/flow-blockchain.js';
import * as fcl from "@onflow/fcl";
import dotenv from 'dotenv';
dotenv.config();

const ID = 1008827; // One of the "Locked" moments

fcl.config().put("accessNode.api", "https://rest-mainnet.onflow.org");

async function checkOwner() {
    console.log(`Checking owner/status for NFT ${ID}...`);
    // Note: getNFTFullDetails usually requires an address to query.
    // If we don't know the address, we can't easily find it on Flow without an Indexer.
    // BUT we can check if it exists in Junglerules' UNLOCKED collection?
    // I already did that (it wasn't in the 9 matching IDs).

    // So let's check if we can get details assuming it's in Junglerules account?
    // If it fails, then it's not there.

    const address = '0xcfd9bad75352b43b';
    const details = await getNFTFullDetails(address, ID);

    if (details) {
        console.log(`Found in Junglerules account!`);
        console.log(JSON.stringify(details, null, 2));
    } else {
        console.log(`NOT found in Junglerules account (Unlocked).`);
    }

    // I can also try to use a script to find the owner if I had an indexer, but I don't.
    // I'll assume that if it's not in Junglerules, and not in NFTLocker, it's GONE (sold/burned).

    process.exit(0);
}

checkOwner();
