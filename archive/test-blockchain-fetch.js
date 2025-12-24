import flowService from "./services/flow-blockchain.js";
import fs from "fs";

async function testBlockchainFetch() {
    try {
        const nftId = "938885";
        const walletAddress = "0xddfbe848a81b2236";

        console.log(`Fetching from blockchain: ID=${nftId} Wallet=${walletAddress}`);
        const details = await flowService.getNFTFullDetails(walletAddress, parseInt(nftId));

        console.log("BLOCKCHAIN_DETAILS:");
        console.log(JSON.stringify(details, null, 2));

        fs.writeFileSync("blockchain_test_results.txt", JSON.stringify(details, null, 2));
    } catch (err) {
        console.error(err);
    } finally {
        process.exit(0);
    }
}

testBlockchainFetch();
